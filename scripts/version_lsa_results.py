#!/usr/bin/env python3
"""Verify captured LSA results; optionally commit and push without force."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

import poll_election_core as core
from lsa_source import parse_dynamic_results_page, validate_results

VERSION_PATHS = ["data/2026-lsa/latest", "data/2026-lsa/metadata/wahlkreis-status.csv",
                 "data/2026-lsa/metadata/wahlkreis-status.svg",
                 "data/2026-lsa/reports/latest_events.csv"]
IDENTITY = ["-c", "user.name=github-actions[bot]", "-c",
            "user.email=41898282+github-actions[bot]@users.noreply.github.com"]


def verify(root: Path) -> dict:
    latest = root / "data/2026-lsa/latest"
    metadata = json.loads((latest / "run_metadata.json").read_text())
    if metadata.get("statla_mode") not in {
        "LIVE_CSV_DOWNLOAD",
        "LIVE_OVERVIEW_HTML_WITH_CSV_DOWNLOAD",
    } or metadata.get("statla_error"):
        raise ValueError("Cannot version an unsuccessful LSA capture")
    source_dir = latest / "official_sources"
    manifest = json.loads((source_dir / "manifest.json").read_text())
    if manifest.get("election_key") != "2026-lsa" or len(manifest.get("fetches", [])) < 3:
        raise ValueError("Missing LSA source provenance")
    expected_snapshots, expected_parties = [], []
    dynamic_parties = []
    for source in manifest["fetches"]:
        name = source["filename"]
        if Path(name).name != name:
            raise ValueError("Invalid source filename")
        data = (source_dir / name).read_bytes()
        if source["status_code"] != 200 or source["error_message"]:
            raise ValueError("Cannot version a failed source response")
        if len(data) != source["byte_count"] or core.sha256_bytes(data) != source["content_hash"]:
            raise ValueError(f"Source checksum mismatch: {name}")
        text = core.decode_bytes(data)
        if core.looks_like_statla_csv(text) or core.looks_like_statla_wahlbezirk_csv(text):
            rows, parties = core.parse_statla_csv_rows(text)
            expected_snapshots.extend(rows)
            expected_parties.extend(parties)
        elif name == "erg_wkr.html":
            dynamic_parties = parse_dynamic_results_page(data)["party_rows"]
    if dynamic_parties:
        # The dynamic Wahlkreise page can advance before the downloadable CSV.
        # Only replace CSV Wahlkreis rows when both sources reconcile; otherwise
        # keep the CSV as the normalized result and preserve the reported delta.
        dynamic_keys = {row["row_key"] for row in dynamic_parties}
        expected_snapshots_by_key = {row["row_key"]: row for row in expected_snapshots}
        dynamic_totals = {}
        for row in dynamic_parties:
            key = (row["row_key"], row["vote_type"])
            dynamic_totals[key] = dynamic_totals.get(key, 0) + row["votes"]
        dynamic_matches_csv = all(
            key in expected_snapshots_by_key
            and dynamic_totals.get((key, "Erststimmen"), 0) == (expected_snapshots_by_key[key].get("valid_votes_erst") or 0)
            and dynamic_totals.get((key, "Zweitstimmen"), 0) == (expected_snapshots_by_key[key].get("valid_votes_zweit") or 0)
            for key in dynamic_keys
        ) and len(dynamic_keys) == 41
        if dynamic_matches_csv:
            expected_parties = [row for row in expected_parties if row["row_key"] not in dynamic_keys]
            expected_parties.extend(dynamic_parties)
    snapshots = core.normalize_latest_statla_snapshots(core.read_csv_rows_from_file(latest / "statla_snapshots.csv"))
    parties = core.normalize_latest_statla_party_rows(core.read_csv_rows_from_file(latest / "statla_party_results.csv"))
    validate_results(snapshots, parties, [])
    snapshot_key = lambda row: row["row_key"]
    party_key = lambda row: (row["row_key"], row["vote_type"], row["party_key"])
    if sorted(snapshots, key=snapshot_key) != sorted(core.normalize_latest_statla_snapshots(expected_snapshots), key=snapshot_key):
        raise ValueError("Normalized LSA snapshots do not match the archived source bytes")
    if sorted(parties, key=party_key) != sorted(core.normalize_latest_statla_party_rows(expected_parties), key=party_key):
        raise ValueError("Normalized LSA party results do not match the archived source bytes")
    return metadata


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, check=True, text=True, stdout=subprocess.PIPE).stdout.strip()


def commit_results(root: Path) -> bool:
    metadata = verify(root)
    if git(root, "diff", "--cached", "--name-only"):
        raise ValueError("Refusing to commit while unrelated changes are staged")
    git(root, "add", "--", *VERSION_PATHS)
    if not git(root, "diff", "--cached", "--name-only"):
        return False
    git(root, *IDENTITY, "commit", "-m", f"2026-lsa poll {metadata['generated_at_utc']}")
    return True


def push_results(root: Path, branch: str, retry_delay: float = 5) -> None:
    git(root, "check-ref-format", "--branch", branch)
    for attempt in range(3):
        try:
            git(root, "push", "origin", f"HEAD:refs/heads/{branch}")
            return
        except subprocess.CalledProcessError:
            if attempt == 2:
                raise
            time.sleep(retry_delay)
            git(root, "fetch", "origin", branch)
            # Conflicts fail visibly. Keep the pre-push patch; never force-push.
            git(root, *IDENTITY, "rebase", "FETCH_HEAD")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--branch", help="Required destination branch with --push")
    parser.add_argument("--recovery-patch", type=Path)
    args = parser.parse_args()
    if args.push and (not args.commit or not args.branch):
        parser.error("--push requires --commit and --branch")
    if not args.commit:
        print(json.dumps(verify(core.ROOT), indent=2))
        return
    changed = commit_results(core.ROOT)
    if changed and args.recovery_patch:
        args.recovery_patch.parent.mkdir(parents=True, exist_ok=True)
        with args.recovery_patch.open("wb") as handle:
            subprocess.run(["git", "format-patch", "-1", "--stdout", "--binary", "HEAD"], cwd=core.ROOT, stdout=handle, check=True)
    if args.push:
        push_results(core.ROOT, args.branch)
    print("LSA result versioned" if changed else "LSA result unchanged")


if __name__ == "__main__":
    main()
