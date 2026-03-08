#!/usr/bin/env python3
"""Run the election poller locally on a fixed interval."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Python 3.9+ is required") from exc


ROOT = Path(__file__).resolve().parents[1]
POLLER_PATH = ROOT / "scripts" / "poll_election.py"
LOCAL_TZ = ZoneInfo("Europe/Berlin")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the election poller locally every minute.")
    parser.add_argument(
        "--election-key",
        default="2026-bw",
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Polling interval in seconds. Defaults to 60.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Optional number of poll executions before exiting.",
    )
    parser.add_argument(
        "--start-at",
        default=None,
        help="Optional local Berlin start time as HH:MM or ISO datetime.",
    )
    parser.add_argument(
        "poller_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to poll_election.py. Prefix them with --.",
    )
    return parser.parse_args()


def normalize_poller_args(args: list[str]) -> list[str]:
    if args and args[0] == "--":
        return args[1:]
    return args


def parse_start_at(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    text = raw_value.strip()
    now_local = datetime.now(LOCAL_TZ)
    if len(text) == 5 and text[2] == ":":
        hour = int(text[:2])
        minute = int(text[3:])
        candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate < now_local:
            candidate += timedelta(days=1)
        return candidate
    candidate = datetime.fromisoformat(text)
    if candidate.tzinfo is None:
        return candidate.replace(tzinfo=LOCAL_TZ)
    return candidate.astimezone(LOCAL_TZ)


def sleep_until(target: datetime) -> None:
    while True:
        remaining = (target - datetime.now(LOCAL_TZ)).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 30))


def sleep_to_next_tick(interval_seconds: int, started_at: float) -> None:
    elapsed = time.time() - started_at
    remaining = max(0.0, interval_seconds - elapsed)
    time.sleep(remaining)


def main() -> int:
    args = parse_args()
    poller_args = normalize_poller_args(list(args.poller_args))
    start_at = parse_start_at(args.start_at)
    if start_at is not None:
        print(f"Waiting until {start_at.strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)
        sleep_until(start_at)

    iteration = 0
    while args.iterations is None or iteration < args.iterations:
        iteration += 1
        started_at = time.time()
        started_label = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        print(f"[{started_label}] Starting poll {iteration}", flush=True)
        completed = subprocess.run(
            [sys.executable, str(POLLER_PATH), "--election-key", args.election_key, *poller_args],
            cwd=ROOT,
            check=False,
        )
        if completed.returncode != 0:
            print(f"Poll {iteration} failed with exit code {completed.returncode}", flush=True)
            return completed.returncode
        sleep_to_next_tick(args.interval_seconds, started_at)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
