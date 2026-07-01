#!/usr/bin/env python3
"""Read-only status snapshot for configured election data artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


LATEST_CSV_FILES = [
    "kommone_party_results.csv",
    "kommone_snapshots.csv",
    "statla_party_results.csv",
    "statla_snapshots.csv",
]
UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print a read-only status snapshot for election artifacts.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root. Defaults to this script's parent repository.",
    )
    parser.add_argument(
        "--election-key",
        action="append",
        default=[],
        help="Limit the report to one election key. May be supplied more than once.",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format.",
    )
    return parser.parse_args()


def file_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "bytes": stat.st_size,
        "modified_at_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
    }


def count_csv_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def count_raw_files(root: Path, election_key: str) -> dict[str, int]:
    raw_dir = root / "data" / election_key / "raw"
    counts: dict[str, int] = {}
    if not raw_dir.exists():
        return counts
    for child in sorted(raw_dir.iterdir()):
        if child.is_dir():
            counts[child.name] = sum(1 for path in child.rglob("*") if path.is_file())
    return counts


def summarize_election(root: Path, config_path: Path) -> dict[str, Any]:
    config = load_json(config_path)
    election_key = str(config.get("election_key") or config_path.stem)
    election_date = parse_iso_date(config.get("election_date"))
    data_dir = root / "data" / election_key
    latest_dir = data_dir / "latest"
    metadata_dir = data_dir / "metadata"
    reports_dir = data_dir / "reports"
    site_dir = root / "site" / election_key
    run_metadata_path = latest_dir / "run_metadata.json"
    run_metadata = load_json(run_metadata_path)

    latest_files = {
        filename: {
            **file_status(latest_dir / filename),
            "rows": count_csv_rows(latest_dir / filename),
        }
        for filename in LATEST_CSV_FILES
    }

    metadata_files = {
        "municipalities.csv": file_status(metadata_dir / "municipalities.csv"),
        "wahlkreis-status.csv": file_status(metadata_dir / "wahlkreis-status.csv"),
        str(config.get("local_wahlkreise_geojson_filename") or "wahlkreise.geojson"): file_status(
            metadata_dir / str(config.get("local_wahlkreise_geojson_filename") or "wahlkreise.geojson")
        ),
        str(config.get("local_wahlkreise_mapping_csv_filename") or "wahlkreis-mapping.csv"): file_status(
            metadata_dir
            / str(config.get("local_wahlkreise_mapping_csv_filename") or "wahlkreis-mapping.csv")
        ),
    }

    today = datetime.now(UTC).date()
    days_until_election = (election_date - today).days if election_date else None
    latest_csvs_present = all(info["exists"] for info in latest_files.values())
    metadata_present = all(info["exists"] for info in metadata_files.values())
    latest_events_path = reports_dir / "latest_events.csv"

    checks = [
        {
            "name": "latest_exports_present",
            "status": "ok" if latest_csvs_present else "warn",
            "detail": (
                f"{sum(1 for info in latest_files.values() if info['exists'])} "
                f"of {len(latest_files)} latest CSVs present"
            ),
        },
        {
            "name": "metadata_present",
            "status": "ok" if metadata_present else "warn",
            "detail": (
                f"{sum(1 for info in metadata_files.values() if info['exists'])} "
                f"of {len(metadata_files)} metadata files present"
            ),
        },
        {
            "name": "run_metadata_present",
            "status": "ok" if run_metadata_path.exists() else "warn",
            "detail": run_metadata.get("run_label") or "missing run_metadata.json",
        },
        {
            "name": "site_index_present",
            "status": "ok" if (site_dir / "index.html").exists() else "info",
            "detail": str(site_dir / "index.html"),
        },
    ]

    return {
        "election_key": election_key,
        "election_name": config.get("election_name"),
        "election_date": election_date.isoformat() if election_date else None,
        "days_until_election": days_until_election,
        "tracking_start_local": config.get("tracking_start_local"),
        "timezone": config.get("timezone"),
        "run_metadata": run_metadata,
        "latest_files": latest_files,
        "metadata_files": metadata_files,
        "reports": {
            "latest_events_csv": {
                **file_status(latest_events_path),
                "rows": count_csv_rows(latest_events_path),
            }
        },
        "raw_file_counts": count_raw_files(root, election_key),
        "site": {
            "index_html": file_status(site_dir / "index.html"),
        },
        "checks": checks,
    }


def build_status(root: Path, election_keys: list[str]) -> dict[str, Any]:
    root = root.resolve()
    requested = set(election_keys)
    config_paths = sorted((root / "config").glob("*.json"))
    elections = []
    for config_path in config_paths:
        if requested and config_path.stem not in requested:
            continue
        elections.append(summarize_election(root, config_path))

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "root": str(root),
        "election_count": len(elections),
        "elections": elections,
    }


def render_rows(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def render_markdown(status: dict[str, Any]) -> str:
    lines = [
        "# Election Artifact Status",
        "",
        f"- Generated at: `{status['generated_at_utc']}`",
        f"- Elections reported: `{status['election_count']}`",
    ]

    for election in status["elections"]:
        metadata = election["run_metadata"]
        lines.extend(
            [
                "",
                f"## {election['election_key']}",
                "",
                f"- Name: `{election.get('election_name') or 'n/a'}`",
                f"- Election date: `{election.get('election_date') or 'n/a'}`",
                f"- Days until election: `{election.get('days_until_election') if election.get('days_until_election') is not None else 'n/a'}`",
                f"- Latest run label: `{metadata.get('run_label') or 'n/a'}`",
                f"- Generated at UTC: `{metadata.get('generated_at_utc') or 'n/a'}`",
                f"- StatLA mode: `{metadata.get('statla_mode') or 'n/a'}`",
                "",
                "| Latest file | Rows | Present |",
                "| --- | ---: | --- |",
            ]
        )
        for filename, info in election["latest_files"].items():
            lines.append(f"| `{filename}` | {render_rows(info.get('rows'))} | {info['exists']} |")

        lines.extend(["", "### Checks", ""])
        for check in election["checks"]:
            lines.append(f"- `{check['status']}` {check['name']}: {check['detail']}")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    status = build_status(args.root, args.election_key)
    if args.format == "json":
        print(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_markdown(status), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
