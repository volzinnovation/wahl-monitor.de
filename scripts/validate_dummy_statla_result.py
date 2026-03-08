#!/usr/bin/env python3
"""Validate latest StatLA outputs against the official dummy CSV."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

import poll_election_core as core


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def normalize_snapshot_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "row_key": str(row["row_key"]),
                "ags": str(row.get("ags") or ""),
                "municipality_name": str(row.get("municipality_name") or ""),
                "gebietsart": str(row.get("gebietsart") or ""),
                "gebietsnummer": str(row.get("gebietsnummer") or ""),
                "reported_precincts": core.parse_int(row.get("reported_precincts")),
                "total_precincts": core.parse_int(row.get("total_precincts")),
                "voters_total": core.parse_int(row.get("voters_total")),
                "valid_votes_erst": core.parse_int(row.get("valid_votes_erst")),
                "valid_votes_zweit": core.parse_int(row.get("valid_votes_zweit")),
                "payload_hash": str(row.get("payload_hash") or ""),
                "is_municipality_summary": str(row.get("is_municipality_summary")).lower() == "true",
            }
        )
    return normalized


def normalize_party_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "row_key": str(row["row_key"]),
                "vote_type": str(row.get("vote_type") or ""),
                "party_key": str(row.get("party_key") or ""),
                "party_name": str(row.get("party_name") or ""),
                "votes": core.parse_int(row.get("votes")),
            }
        )
    return normalized


def assert_equal(name: str, expected: Any, actual: Any) -> None:
    if expected != actual:
        raise AssertionError(f"{name} mismatch")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate latest StatLA outputs against the official dummy CSV.")
    parser.add_argument(
        "--election-key",
        default=core.DEFAULT_ELECTION_KEY,
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    core.set_active_election(election_key=args.election_key)
    core.load_config()

    dummy_csv_path = core.LOCAL_DUMMY_STATLA_PATH
    run_metadata_path = core.LATEST_DIR / "run_metadata.json"
    statla_snapshots_path = core.LATEST_DIR / "statla_snapshots.csv"
    statla_party_results_path = core.LATEST_DIR / "statla_party_results.csv"

    metadata = json.loads(run_metadata_path.read_text(encoding="utf-8"))
    if metadata.get("statla_mode") != "DUMMY":
        raise SystemExit(f"Expected latest run to be DUMMY, got {metadata.get('statla_mode')!r}")

    dummy_text = core.decode_bytes(dummy_csv_path.read_bytes())
    expected_snapshots, expected_party_rows = core.parse_statla_csv_rows(dummy_text)
    actual_snapshots = read_csv(statla_snapshots_path)
    actual_party_rows = read_csv(statla_party_results_path)

    expected_snapshots_norm = normalize_snapshot_rows(expected_snapshots)
    actual_snapshots_norm = normalize_snapshot_rows(actual_snapshots)
    expected_party_rows_norm = normalize_party_rows(expected_party_rows)
    actual_party_rows_norm = normalize_party_rows(actual_party_rows)

    assert_equal("snapshot_row_count", len(expected_snapshots_norm), len(actual_snapshots_norm))
    assert_equal("party_row_count", len(expected_party_rows_norm), len(actual_party_rows_norm))
    assert_equal("snapshots", expected_snapshots_norm, actual_snapshots_norm)
    assert_equal("party_rows", expected_party_rows_norm, actual_party_rows_norm)

    # Aggregated party-key totals provide a compact sanity check in the output.
    party_key_totals: Dict[str, int] = {}
    for row in actual_party_rows_norm:
        key = row["party_key"]
        votes = row["votes"] or 0
        party_key_totals[key] = party_key_totals.get(key, 0) + votes

    print("Validation successful.")
    print(f"Dummy CSV: {dummy_csv_path}")
    print(f"Latest run mode: {metadata.get('statla_mode')}")
    print(f"Snapshot rows: {len(actual_snapshots_norm)}")
    print(f"Party rows: {len(actual_party_rows_norm)}")
    print(f"Distinct party keys: {len(party_key_totals)}")
    print(f"Total D/F votes: {sum(party_key_totals.values())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
