#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import poll_election_core as core


def load_latest_kommone_snapshots() -> List[Dict[str, Any]]:
    rows = core.read_csv_rows_from_file(core.LATEST_DIR / "kommone_snapshots.csv", delimiter=",")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "ags": core.canonical_ags(row.get("ags")),
                "municipality_name": core.canonical_municipality_name(row.get("municipality_name")),
                "status": str(row.get("status") or ""),
                "reported_precincts": core.parse_int(row.get("reported_precincts")),
                "total_precincts": core.parse_int(row.get("total_precincts")),
                "voters_total": core.parse_int(row.get("voters_total")),
                "valid_votes": core.parse_int(row.get("valid_votes")),
                "invalid_votes": core.parse_int(row.get("invalid_votes")),
                "source_timestamp": str(row.get("source_timestamp") or ""),
                "payload_hash": str(row.get("payload_hash") or ""),
                "error_message": str(row.get("error_message") or ""),
            }
        )
    return normalized


def load_latest_kommone_party_rows() -> List[Dict[str, Any]]:
    rows = core.read_csv_rows_from_file(core.LATEST_DIR / "kommone_party_results.csv", delimiter=",")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "ags": core.canonical_ags(row.get("ags")),
                "municipality_name": core.canonical_municipality_name(row.get("municipality_name")),
                "vote_type": core.canonical_vote_type(str(row.get("vote_type") or "")),
                "party": core.canonical_party_name(row.get("party"), row.get("vote_type")),
                "votes": core.parse_int(row.get("votes")),
                "percent": core.parse_float_percent(row.get("percent")),
            }
        )
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh StatLA latest exports from the official result presentation HTML.")
    parser.add_argument("--election-key", default=core.DEFAULT_ELECTION_KEY)
    parser.add_argument("--config-path", default=None)
    args = parser.parse_args()

    core.set_active_election(
        election_key=args.election_key,
        config_path=Path(args.config_path) if args.config_path else None,
    )
    core.ensure_directories()
    config = core.load_config()
    previous_latest = core.load_latest_statla_exports()
    statla = core.fetch_statla_presentation_fallback(
        config,
        config.request_timeout_seconds,
        previous_latest,
        base_error="Manual refresh from StatLA result presentation",
    )
    if statla is None:
        raise SystemExit("Could not build StatLA fallback from result presentation")

    kommone_snapshots = load_latest_kommone_snapshots()
    kommone_party_rows = load_latest_kommone_party_rows()
    diff_rows = (
        core.compute_source_diffs(0, kommone_snapshots, statla["snapshots"])
        if config.publish_source_comparison
        else []
    )
    events_rows = core.read_csv_rows_from_file(core.REPORT_DIR / "latest_events.csv", delimiter=",")
    label_file, _label_human = core.time_labels(config.timezone)

    core.generate_wahlkreis_map(
        kommone_snapshots=kommone_snapshots,
        statla_snapshots=statla["snapshots"],
        prestart=False,
    )
    core.persist_files(
        label_file=label_file,
        kommone_snapshots=kommone_snapshots,
        kommone_party_rows=kommone_party_rows,
        statla=statla,
        diff_rows=diff_rows,
        events_rows=events_rows,
    )

    land_row = next((row for row in statla["snapshots"] if row["row_key"] == "000000:BW:-:-:LAND"), None)
    print(f"mode={statla['mode']}")
    print(f"url={statla['url']}")
    print(f"snapshots={len(statla['snapshots'])}")
    print(f"party_rows={len(statla['party_rows'])}")
    print(f"land_reported={land_row.get('reported_precincts') if land_row else None}")
    print(f"land_total={land_row.get('total_precincts') if land_row else None}")


if __name__ == "__main__":
    main()
