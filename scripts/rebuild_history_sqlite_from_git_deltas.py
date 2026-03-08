#!/usr/bin/env python3
"""Rebuild an election history.sqlite from git-tracked poll delta files."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from zoneinfo import ZoneInfo

import poll_election_core as core


def run_git(args: List[str], *, text: bool = True) -> str | bytes:
    result = subprocess.run(["git", *args], check=True, capture_output=True, text=text)
    return result.stdout


def git_show_text(commit: str, path: str) -> Optional[str]:
    result = subprocess.run(["git", "show", f"{commit}:{path}"], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return result.stdout


def git_show_bytes(commit: str, path: str) -> Optional[bytes]:
    result = subprocess.run(["git", "show", f"{commit}:{path}"], capture_output=True)
    if result.returncode != 0:
        return None
    return result.stdout


def parse_csv_rows(text: str) -> List[Dict[str, str]]:
    return [dict(row) for row in csv.DictReader(text.splitlines(), delimiter=",")]


def commit_date_iso(commit: str) -> str:
    value = run_git(["show", "-s", "--format=%cI", commit], text=True).strip()
    if value:
        return value
    return datetime.now(timezone.utc).isoformat()


def list_poll_commits() -> List[str]:
    run_metadata_path = core.repo_relative_path(core.LATEST_DIR / "run_metadata.json")
    raw = run_git(["rev-list", "--reverse", "--all", "--", run_metadata_path], text=True)
    commits = [line.strip() for line in raw.splitlines() if line.strip()]
    return commits


def upsert_poll(conn: sqlite3.Connection, polled_at_utc: str, polled_at_local: str, created_at_utc: str) -> int:
    conn.execute(
        """
        INSERT INTO polls (polled_at_utc, polled_at_local, created_at_utc)
        VALUES (?, ?, ?)
        ON CONFLICT(polled_at_utc) DO NOTHING
        """,
        (polled_at_utc, polled_at_local, created_at_utc),
    )
    row = conn.execute("SELECT id FROM polls WHERE polled_at_utc = ?", (polled_at_utc,)).fetchone()
    if row is None:
        raise RuntimeError(f"Missing poll row for {polled_at_utc}")
    return int(row[0])


def local_label(utc_iso: str, tz_name: str) -> str:
    dt = core.parse_iso_datetime(utc_iso)
    if dt is None:
        return utc_iso
    return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S %Z")


def rebuild(db_path: Path, limit: Optional[int]) -> Dict[str, int]:
    config = core.load_config()
    run_metadata_path = core.repo_relative_path(core.LATEST_DIR / "run_metadata.json")
    latest_events_path = core.repo_relative_path(core.REPORT_DIR / "latest_events.csv")
    latest_diffs_path = core.repo_relative_path(core.REPORT_DIR / "latest_source_diff.csv")
    raw_statla_dir = core.repo_relative_path(core.RAW_STATLA_DIR)
    commits = list_poll_commits()
    if limit is not None and limit > 0:
        commits = commits[-limit:]

    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        core.init_db(conn)

        poll_count = 0
        event_count = 0
        diff_count = 0

        for commit in commits:
            metadata_text = git_show_text(commit, run_metadata_path)
            if not metadata_text:
                continue
            try:
                metadata = json.loads(metadata_text)
            except json.JSONDecodeError:
                continue

            polled_at_utc = str(metadata.get("generated_at_utc") or "").strip()
            if not polled_at_utc:
                polled_at_utc = commit_date_iso(commit)

            poll_id = upsert_poll(
                conn,
                polled_at_utc=polled_at_utc,
                polled_at_local=local_label(polled_at_utc, config.timezone),
                created_at_utc=commit_date_iso(commit),
            )
            poll_count += 1

            run_label = str(metadata.get("run_label") or "").strip()
            statla_url = str(metadata.get("statla_url") or "")
            statla_error = metadata.get("statla_error")
            statla_hash = None
            statla_bytes = 0
            if run_label:
                raw_statla_path = f"{raw_statla_dir}/{run_label}-statla.csv"
                raw_statla = git_show_bytes(commit, raw_statla_path)
                if raw_statla:
                    statla_hash = core.sha256_bytes(raw_statla)
                    statla_bytes = len(raw_statla)

            conn.execute(
                """
                INSERT INTO source_fetches (
                  poll_id, source, url, status_code, content_hash, byte_count, error_message, fetched_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    poll_id,
                    "statla",
                    statla_url,
                    200 if statla_hash else None,
                    statla_hash,
                    statla_bytes,
                    statla_error,
                    polled_at_utc,
                ),
            )

            events_text = git_show_text(commit, latest_events_path)
            if events_text:
                events = parse_csv_rows(events_text)
                conn.executemany(
                    """
                    INSERT INTO events (
                      poll_id, event_time_utc, source, ags, municipality_name, event_type, details_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            poll_id,
                            str(row.get("event_time_utc") or polled_at_utc),
                            str(row.get("source") or ""),
                            core.canonical_ags(row.get("ags")),
                            core.canonical_municipality_name(row.get("municipality_name")),
                            str(row.get("event_type") or ""),
                            str(row.get("details_json") or "{}"),
                        )
                        for row in events
                        if str(row.get("source") or "") and str(row.get("event_type") or "")
                    ],
                )
                event_count += len(events)

            diffs_text = git_show_text(commit, latest_diffs_path)
            if diffs_text:
                diffs = parse_csv_rows(diffs_text)
                conn.executemany(
                    """
                    INSERT INTO source_diffs (
                      poll_id, ags, municipality_name, metric, kommone_value, statla_value, delta
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(poll_id, ags, metric) DO UPDATE SET
                      municipality_name = excluded.municipality_name,
                      kommone_value = excluded.kommone_value,
                      statla_value = excluded.statla_value,
                      delta = excluded.delta
                    """,
                    [
                        (
                            poll_id,
                            core.canonical_ags(row.get("ags")),
                            core.canonical_municipality_name(row.get("municipality_name")),
                            str(row.get("metric") or ""),
                            core.parse_float_value(row.get("kommone_value")),
                            core.parse_float_value(row.get("statla_value")),
                            core.parse_float_value(row.get("delta")),
                        )
                        for row in diffs
                        if core.canonical_ags(row.get("ags")) and str(row.get("metric") or "")
                    ],
                )
                diff_count += len(diffs)

        conn.commit()
        return {
            "poll_commits": len(commits),
            "poll_rows": poll_count,
            "events_rows": event_count,
            "diff_rows": diff_count,
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild history.sqlite from git-tracked delta files.")
    parser.add_argument(
        "--election-key",
        default=core.DEFAULT_ELECTION_KEY,
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional explicit SQLite path. Defaults to data/<election-key>/history.sqlite.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only replay the last N poll commits.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    core.set_active_election(election_key=args.election_key)
    db_path = Path(args.db_path) if args.db_path else core.DB_PATH
    stats = rebuild(db_path=db_path, limit=args.limit)
    print(f"Rebuilt SQLite: {db_path}")
    print(f"Poll commits scanned: {stats['poll_commits']}")
    print(f"Poll rows inserted: {stats['poll_rows']}")
    print(f"Event rows inserted: {stats['events_rows']}")
    print(f"Diff rows inserted: {stats['diff_rows']}")


if __name__ == "__main__":
    main()
