#!/usr/bin/env python3
"""Compare the current StatLA live CSV with the stored latest export."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import poll_election_core as core


SUMMARY_LEVELS = {"LAND", "WAHLKREIS", "GEMEINDE"}
LEVEL_ORDER = {"LAND": 0, "WAHLKREIS": 1, "GEMEINDE": 2}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--election-key", default=core.DEFAULT_ELECTION_KEY, help="Election key from config/<key>.json")
    parser.add_argument(
        "--output-prefix",
        default="statla_live_vs_latest",
        help="Filename prefix for generated report files inside data/<key>/reports/",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="HTTP timeout for the live StatLA CSV download.",
    )
    return parser.parse_args()


def row_key_from_raw_row(index: int, raw_row: Dict[str, str]) -> str:
    return (
        f"{index:06d}:"
        f"{str(raw_row.get('Gebietsnummer', '')).strip() or '-'}:"
        f"{str(raw_row.get('Bezirksnummer', '')).strip() or '-'}:"
        f"{core.canonical_ags(raw_row.get('AGS')) or '-'}:"
        f"{str(raw_row.get('Gebietsart', '')).strip() or '-'}"
    )


def fetch_live_csv(config: core.Config, timeout_seconds: int) -> Tuple[str, core.HttpResult]:
    result = core.statla_http_get(config.statla_live_csv_url, timeout_seconds, show_progress=False)
    if result.status_code != 200 or not result.content:
        raise SystemExit(
            f"Failed to download StatLA CSV from {config.statla_live_csv_url}: "
            f"status={result.status_code} error={result.error_message}"
        )
    csv_text = core.decode_bytes(result.content)
    if not core.looks_like_statla_csv(csv_text):
        raise SystemExit(f"Live source did not return a Statistik BW CSV: {config.statla_live_csv_url}")
    return csv_text, result


def summary_row_name(snapshot: Dict[str, Any], raw_row: Optional[Dict[str, str]]) -> str:
    level, _identifier = core.statla_snapshot_match_key(snapshot)
    if level == "LAND":
        return str((raw_row or {}).get("Gebietsname") or "Land Baden-Württemberg").strip()
    if level == "WAHLKREIS":
        return str((raw_row or {}).get("Wahlkreisname") or (raw_row or {}).get("Gebietsname") or "").strip()
    return str(snapshot.get("municipality_name") or (raw_row or {}).get("Gemeindename") or "").strip()


def build_summary_identity_maps(
    snapshots: List[Dict[str, Any]],
    raw_rows: Iterable[Dict[str, str]],
) -> Tuple[Dict[Tuple[str, str], Dict[str, str]], Dict[str, Tuple[str, str]]]:
    identities: Dict[Tuple[str, str], Dict[str, str]] = {}
    row_key_to_match_key: Dict[str, Tuple[str, str]] = {}
    for index, (snapshot, raw_row) in enumerate(zip(snapshots, raw_rows)):
        row_key = str(snapshot.get("row_key") or row_key_from_raw_row(index, raw_row))
        match_key = core.statla_snapshot_match_key(snapshot)
        level, identifier = match_key
        if level not in SUMMARY_LEVELS:
            continue
        row_key_to_match_key[row_key] = match_key
        identities[match_key] = {
            "level": level,
            "identifier": identifier,
            "ags": core.canonical_ags(snapshot.get("ags")) if level == "GEMEINDE" else "",
            "wahlkreisnummer": core.normalize_wahlkreis_nummer(snapshot.get("gebietsnummer")) if level == "WAHLKREIS" else "",
            "name": summary_row_name(snapshot, raw_row),
        }
    return identities, row_key_to_match_key


def build_snapshot_map(snapshots: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    mapped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in snapshots:
        match_key = core.statla_snapshot_match_key(row)
        if match_key[0] in SUMMARY_LEVELS:
            mapped[match_key] = row
    return mapped


def build_party_map(
    party_rows: Iterable[Dict[str, Any]],
    row_key_to_match_key: Dict[str, Tuple[str, str]],
) -> Dict[Tuple[Tuple[str, str], str], Dict[str, int]]:
    mapped: Dict[Tuple[Tuple[str, str], str], Dict[str, int]] = defaultdict(dict)
    for row in party_rows:
        row_key = str(row.get("row_key") or "")
        match_key = row_key_to_match_key.get(row_key)
        if match_key is None:
            continue
        vote_type = str(row.get("vote_type") or "")
        party_name = str(row.get("party_name") or "")
        mapped[(match_key, vote_type)][party_name] = core.parse_int(row.get("votes")) or 0
    return mapped


def share_percent(votes: int, total_votes: Optional[int]) -> Optional[float]:
    if total_votes in (None, 0):
        return None
    return round((votes / total_votes) * 100, 4)


def valid_votes_field(vote_type: str) -> str:
    return "valid_votes_erst" if vote_type == "Erststimmen" else "valid_votes_zweit"


def sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    level = str(row.get("level") or "")
    identifier = str(row.get("identifier") or "")
    if level == "WAHLKREIS":
        return (LEVEL_ORDER[level], int(identifier))
    if level == "GEMEINDE":
        return (LEVEL_ORDER[level], str(row.get("ags") or identifier))
    return (LEVEL_ORDER.get(level, 99), identifier)


def build_totals_rows(
    old_snapshots: Dict[Tuple[str, str], Dict[str, Any]],
    new_snapshots: Dict[Tuple[str, str], Dict[str, Any]],
    identities: Dict[Tuple[str, str], Dict[str, str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    all_keys = set(old_snapshots) | set(new_snapshots)
    for match_key in all_keys:
        old_row = old_snapshots.get(match_key)
        new_row = new_snapshots.get(match_key)
        fields = {
            "reported_precincts": ((old_row or {}).get("reported_precincts"), (new_row or {}).get("reported_precincts")),
            "total_precincts": ((old_row or {}).get("total_precincts"), (new_row or {}).get("total_precincts")),
            "voters_total": ((old_row or {}).get("voters_total"), (new_row or {}).get("voters_total")),
            "valid_votes_erst": ((old_row or {}).get("valid_votes_erst"), (new_row or {}).get("valid_votes_erst")),
            "valid_votes_zweit": ((old_row or {}).get("valid_votes_zweit"), (new_row or {}).get("valid_votes_zweit")),
        }
        deltas = {name: (core.parse_int(new_value) or 0) - (core.parse_int(old_value) or 0) for name, (old_value, new_value) in fields.items()}
        if not any(deltas.values()):
            continue
        identity = identities.get(match_key, {"level": match_key[0], "identifier": match_key[1], "ags": "", "wahlkreisnummer": "", "name": ""})
        rows.append(
            {
                **identity,
                "old_reported_precincts": core.parse_int(fields["reported_precincts"][0]),
                "new_reported_precincts": core.parse_int(fields["reported_precincts"][1]),
                "delta_reported_precincts": deltas["reported_precincts"],
                "old_total_precincts": core.parse_int(fields["total_precincts"][0]),
                "new_total_precincts": core.parse_int(fields["total_precincts"][1]),
                "delta_total_precincts": deltas["total_precincts"],
                "old_voters_total": core.parse_int(fields["voters_total"][0]),
                "new_voters_total": core.parse_int(fields["voters_total"][1]),
                "delta_voters_total": deltas["voters_total"],
                "old_valid_votes_erst": core.parse_int(fields["valid_votes_erst"][0]),
                "new_valid_votes_erst": core.parse_int(fields["valid_votes_erst"][1]),
                "delta_valid_votes_erst": deltas["valid_votes_erst"],
                "old_valid_votes_zweit": core.parse_int(fields["valid_votes_zweit"][0]),
                "new_valid_votes_zweit": core.parse_int(fields["valid_votes_zweit"][1]),
                "delta_valid_votes_zweit": deltas["valid_votes_zweit"],
            }
        )
    rows.sort(key=sort_key)
    return rows


def build_party_rows(
    old_snapshots: Dict[Tuple[str, str], Dict[str, Any]],
    new_snapshots: Dict[Tuple[str, str], Dict[str, Any]],
    old_party_map: Dict[Tuple[Tuple[str, str], str], Dict[str, int]],
    new_party_map: Dict[Tuple[Tuple[str, str], str], Dict[str, int]],
    identities: Dict[Tuple[str, str], Dict[str, str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    summary_keys = set(old_snapshots) | set(new_snapshots)
    for match_key in summary_keys:
        identity = identities.get(match_key, {"level": match_key[0], "identifier": match_key[1], "ags": "", "wahlkreisnummer": "", "name": ""})
        old_snapshot = old_snapshots.get(match_key, {})
        new_snapshot = new_snapshots.get(match_key, {})
        for vote_type in ("Erststimmen", "Zweitstimmen"):
            old_votes_by_party = old_party_map.get((match_key, vote_type), {})
            new_votes_by_party = new_party_map.get((match_key, vote_type), {})
            all_parties = set(old_votes_by_party) | set(new_votes_by_party)
            total_field = valid_votes_field(vote_type)
            old_total = core.parse_int(old_snapshot.get(total_field))
            new_total = core.parse_int(new_snapshot.get(total_field))
            for party_name in sorted(all_parties):
                old_votes = old_votes_by_party.get(party_name, 0)
                new_votes = new_votes_by_party.get(party_name, 0)
                delta_votes = new_votes - old_votes
                if delta_votes == 0:
                    continue
                old_share = share_percent(old_votes, old_total)
                new_share = share_percent(new_votes, new_total)
                rows.append(
                    {
                        **identity,
                        "vote_type": vote_type,
                        "party_name": party_name,
                        "old_votes": old_votes,
                        "new_votes": new_votes,
                        "delta_votes": delta_votes,
                        "old_share_percent": old_share,
                        "new_share_percent": new_share,
                        "delta_share_percent": None if old_share is None or new_share is None else round(new_share - old_share, 4),
                    }
                )
    rows.sort(key=lambda row: (*sort_key(row), str(row.get("vote_type") or ""), str(row.get("party_name") or "")))
    return rows


def top_total_changes(rows: Iterable[Dict[str, Any]], *, level: str, limit: int = 10) -> List[Dict[str, Any]]:
    filtered = [row for row in rows if row.get("level") == level and row.get("delta_valid_votes_zweit")]
    filtered.sort(key=lambda row: (abs(int(row.get("delta_valid_votes_zweit") or 0)), str(row.get("identifier") or "")), reverse=True)
    return [
        {
            "identifier": row["identifier"],
            "name": row["name"],
            "old_valid_votes_zweit": row["old_valid_votes_zweit"],
            "new_valid_votes_zweit": row["new_valid_votes_zweit"],
            "delta_valid_votes_zweit": row["delta_valid_votes_zweit"],
        }
        for row in filtered[:limit]
    ]


def build_summary_payload(
    config: core.Config,
    live_result: core.HttpResult,
    baseline_metadata: Dict[str, Any],
    totals_rows: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    changed_rows_by_level = Counter(str(row.get("level") or "") for row in totals_rows)
    land_row = next(row for row in totals_rows if row["level"] == "LAND")
    land_party_rows = [row for row in party_rows if row.get("level") == "LAND"]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "election_key": config.election_key,
        "baseline_run_label": baseline_metadata.get("run_label"),
        "baseline_generated_at_utc": baseline_metadata.get("generated_at_utc"),
        "live_source_url": live_result.url,
        "live_status_code": live_result.status_code,
        "changed_total_rows": len(totals_rows),
        "changed_party_rows": len(party_rows),
        "changed_rows_by_level": dict(sorted(changed_rows_by_level.items())),
        "land_totals": {
            "old_voters_total": land_row["old_voters_total"],
            "new_voters_total": land_row["new_voters_total"],
            "delta_voters_total": land_row["delta_voters_total"],
            "old_valid_votes_erst": land_row["old_valid_votes_erst"],
            "new_valid_votes_erst": land_row["new_valid_votes_erst"],
            "delta_valid_votes_erst": land_row["delta_valid_votes_erst"],
            "old_valid_votes_zweit": land_row["old_valid_votes_zweit"],
            "new_valid_votes_zweit": land_row["new_valid_votes_zweit"],
            "delta_valid_votes_zweit": land_row["delta_valid_votes_zweit"],
        },
        "land_party_deltas": {
            "Erststimmen": [row for row in land_party_rows if row.get("vote_type") == "Erststimmen"],
            "Zweitstimmen": [row for row in land_party_rows if row.get("vote_type") == "Zweitstimmen"],
        },
        "top_wahlkreis_changes_by_valid_votes_zweit": top_total_changes(totals_rows, level="WAHLKREIS"),
        "top_municipality_changes_by_valid_votes_zweit": top_total_changes(totals_rows, level="GEMEINDE"),
    }


def load_baseline_metadata() -> Dict[str, Any]:
    path = core.LATEST_DIR / "run_metadata.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    core.set_active_election(election_key=args.election_key)
    core.ensure_directories()
    config = core.load_config()

    csv_text, live_result = fetch_live_csv(config, args.timeout_seconds)
    raw_rows = core.csv_rows_from_text(csv_text, delimiter=";")
    new_snapshots, new_party_rows = core.parse_statla_csv_rows(csv_text)
    old_exports = core.load_latest_statla_exports()

    identities, new_row_key_to_match_key = build_summary_identity_maps(new_snapshots, raw_rows)
    old_snapshots = build_snapshot_map(old_exports["snapshots"])
    new_snapshots_map = build_snapshot_map(new_snapshots)
    old_row_key_to_match_key = {
        str(row.get("row_key") or ""): core.statla_snapshot_match_key(row)
        for row in old_exports["snapshots"]
        if core.statla_snapshot_match_key(row)[0] in SUMMARY_LEVELS
    }
    old_party_map = build_party_map(old_exports["party_rows"], old_row_key_to_match_key)
    new_party_map = build_party_map(new_party_rows, new_row_key_to_match_key)

    totals_rows = build_totals_rows(old_snapshots, new_snapshots_map, identities)
    party_diff_rows = build_party_rows(old_snapshots, new_snapshots_map, old_party_map, new_party_map, identities)
    baseline_metadata = load_baseline_metadata()
    summary_payload = build_summary_payload(config, live_result, baseline_metadata, totals_rows, party_diff_rows)

    output_dir = core.REPORT_DIR
    totals_path = output_dir / f"{args.output_prefix}_totals_diff.csv"
    party_path = output_dir / f"{args.output_prefix}_party_diff.csv"
    summary_path = output_dir / f"{args.output_prefix}_summary.json"

    core.write_csv(
        totals_path,
        [
            "level",
            "identifier",
            "name",
            "ags",
            "wahlkreisnummer",
            "old_reported_precincts",
            "new_reported_precincts",
            "delta_reported_precincts",
            "old_total_precincts",
            "new_total_precincts",
            "delta_total_precincts",
            "old_voters_total",
            "new_voters_total",
            "delta_voters_total",
            "old_valid_votes_erst",
            "new_valid_votes_erst",
            "delta_valid_votes_erst",
            "old_valid_votes_zweit",
            "new_valid_votes_zweit",
            "delta_valid_votes_zweit",
        ],
        totals_rows,
    )
    core.write_csv(
        party_path,
        [
            "level",
            "identifier",
            "name",
            "ags",
            "wahlkreisnummer",
            "vote_type",
            "party_name",
            "old_votes",
            "new_votes",
            "delta_votes",
            "old_share_percent",
            "new_share_percent",
            "delta_share_percent",
        ],
        party_diff_rows,
    )
    core.write_json(summary_path, summary_payload)

    print(f"Wrote {totals_path.relative_to(core.ROOT)}")
    print(f"Wrote {party_path.relative_to(core.ROOT)}")
    print(f"Wrote {summary_path.relative_to(core.ROOT)}")
    print(
        f"Changed rows: {summary_payload['changed_total_rows']} "
        f"(land={summary_payload['changed_rows_by_level'].get('LAND', 0)}, "
        f"wahlkreise={summary_payload['changed_rows_by_level'].get('WAHLKREIS', 0)}, "
        f"gemeinden={summary_payload['changed_rows_by_level'].get('GEMEINDE', 0)})"
    )
    land = summary_payload["land_totals"]
    print(
        "Land valid votes delta: "
        f"Erst {land['delta_valid_votes_erst']:+d}, Zweit {land['delta_valid_votes_zweit']:+d}, "
        f"voters_total {land['delta_voters_total']:+d}"
    )


if __name__ == "__main__":
    main()
