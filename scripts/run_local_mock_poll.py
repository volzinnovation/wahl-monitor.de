#!/usr/bin/env python3
"""Run the election poller locally with Statistik BW mock data."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOOP_PATH = ROOT / "scripts" / "run_local_poll_loop.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the election poller locally with mock Statistik BW data.")
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
        "--limit-ags",
        type=int,
        default=None,
        help="Optional municipality cap for faster local dry runs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    command = [
        sys.executable,
        str(LOOP_PATH),
        "--election-key",
        args.election_key,
        "--interval-seconds",
        str(args.interval_seconds),
    ]
    if args.iterations is not None:
        command.extend(["--iterations", str(args.iterations)])
    if args.start_at is not None:
        command.extend(["--start-at", args.start_at])
    command.extend([
        "--",
        "--force-run",
        "--use-dummy-statla",
        "--skip-kommone",
    ])
    if args.limit_ags is not None:
        command.extend(["--limit-ags", str(args.limit_ags)])
    return subprocess.run(command, cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
