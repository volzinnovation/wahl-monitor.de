#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
BERLIN = ZoneInfo("Europe/Berlin")
LAND_ROW_KEY = "000000:BW:-:-:LAND"


def git_text(commit: str, path: Path) -> str:
    return subprocess.check_output(
        ["git", "show", f"{commit}:{path.relative_to(ROOT).as_posix()}"],
        cwd=ROOT,
        text=True,
    )


def git_poll_commits(election_key: str) -> List[Tuple[str, str, str]]:
    paths = [
        (ROOT / "data" / election_key / "latest" / "run_metadata.json").relative_to(ROOT).as_posix(),
        (ROOT / "data" / election_key / "latest" / "statla_snapshots.csv").relative_to(ROOT).as_posix(),
    ]
    raw = subprocess.check_output(
        ["git", "log", "--reverse", "--format=%H\t%cI\t%s", "--", *paths],
        cwd=ROOT,
        text=True,
    )
    commits: List[Tuple[str, str, str]] = []
    seen = set()
    prefix = f"{election_key} poll "
    for line in raw.splitlines():
        if not line.strip():
            continue
        commit, committed_at, subject = line.split("\t", 2)
        if commit in seen or not subject.startswith(prefix):
            continue
        seen.add(commit)
        commits.append((commit, committed_at, subject))
    return commits


def local_label(dt_utc: datetime) -> str:
    local_dt = dt_utc.astimezone(BERLIN)
    suffix = "MEZ" if local_dt.utcoffset() == timedelta(hours=1) else "MESZ"
    return local_dt.strftime(f"%Y-%m-%d %H:%M {suffix}")


def load_land_snapshot(commit: str, election_key: str) -> Dict[str, str]:
    path = ROOT / "data" / election_key / "latest" / "statla_snapshots.csv"
    text = git_text(commit, path)
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("row_key") == LAND_ROW_KEY:
            return row
    raise RuntimeError(f"LAND row not found in {path} at commit {commit}")


def build_rows(election_key: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for commit, committed_at, subject in git_poll_commits(election_key):
        snapshot = load_land_snapshot(commit, election_key)
        dt_utc = datetime.fromisoformat(committed_at)
        voters_total = int(snapshot["voters_total"])
        valid_votes_erst = int(snapshot["valid_votes_erst"])
        valid_votes_zweit = int(snapshot["valid_votes_zweit"])
        valid_share_erst = (valid_votes_erst / voters_total * 100.0) if voters_total else 0.0
        valid_share_zweit = (valid_votes_zweit / voters_total * 100.0) if voters_total else 0.0
        rows.append(
            {
                "commit": commit,
                "commit_time_utc": dt_utc.isoformat(),
                "commit_time_local": local_label(dt_utc),
                "subject": subject,
                "time_local_dt": dt_utc.astimezone(BERLIN),
                "voters_total": voters_total,
                "valid_votes_erst": valid_votes_erst,
                "valid_votes_zweit": valid_votes_zweit,
                "valid_share_erst": round(valid_share_erst, 4),
                "valid_share_zweit": round(valid_share_zweit, 4),
            }
        )
    return rows


def trim_trailing_static_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    if len(rows) < 2:
        return rows

    metric_keys = ["voters_total", "valid_votes_erst", "valid_votes_zweit"]
    last_dynamic_index = 0
    for idx in range(1, len(rows)):
        if any(rows[idx][key] != rows[idx - 1][key] for key in metric_keys):
            last_dynamic_index = idx
    return rows[: last_dynamic_index + 1]


def write_csv_report(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "commit",
                "commit_time_utc",
                "commit_time_local",
                "subject",
                "voters_total",
                "valid_votes_erst",
                "valid_votes_zweit",
                "valid_share_erst",
                "valid_share_zweit",
            ],
        )
        writer.writeheader()
        for row in rows:
            serializable = dict(row)
            serializable.pop("time_local_dt", None)
            writer.writerow(serializable)


def format_int(value: float, _: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_pct(value: float, _: int) -> str:
    return f"{value:.2f}%".replace(".", ",")


def write_png_report(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    df = pd.DataFrame(rows)
    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        figsize=(14, 9),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 2]},
    )

    ax_top.plot(df["time_local_dt"], df["valid_votes_erst"], color="#0b7a75", linewidth=2.5, label="Gültige Erststimmen")
    ax_top.plot(df["time_local_dt"], df["valid_votes_zweit"], color="#c75146", linewidth=2.5, label="Gültige Zweitstimmen")
    ax_top.set_ylabel("Absolute Zahl")
    ax_top.yaxis.set_major_formatter(FuncFormatter(format_int))
    ax_top.set_title("Landtagswahl Baden-Württemberg 2026: Gültige Stimmen im Verlauf des Wahlabends")

    ax_bottom.plot(df["time_local_dt"], df["valid_share_erst"], color="#0b7a75", linewidth=2.5, label="Gültige Erststimmen (%)")
    ax_bottom.plot(df["time_local_dt"], df["valid_share_zweit"], color="#c75146", linewidth=2.5, label="Gültige Zweitstimmen (%)")
    ax_bottom.set_ylabel("Anteil an Wählenden")
    ax_bottom.yaxis.set_major_formatter(FuncFormatter(format_pct))

    for axis in (ax_top, ax_bottom):
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)

    ax_bottom.set_xlabel("Zeit")
    ax_bottom.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=12, tz=BERLIN))
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=BERLIN))

    handles, labels = ax_top.get_legend_handles_labels()
    handles.extend(ax_bottom.get_legend_handles_labels()[0])
    labels.extend(ax_bottom.get_legend_handles_labels()[1])
    labels.append("Analyse: @ProfVolz wahl-monitor.de (Ausfall CSV Abruf zwischen 21:00 und 22:00)")
    handles.append(Line2D([], [], color="none"))
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.01))
    fig.tight_layout(rect=(0, 0.08, 1, 0.93))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--election-key", default="2026-bw")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-png")
    args = parser.parse_args()

    report_dir = ROOT / "data" / args.election_key / "reports"
    output_csv = Path(args.output_csv) if args.output_csv else report_dir / "statla_total_vote_timeline.csv"
    output_png = Path(args.output_png) if args.output_png else report_dir / "statla_total_vote_timeline.png"

    rows = trim_trailing_static_rows(build_rows(args.election_key))
    if not rows:
        raise SystemExit("No poll commits found.")

    write_csv_report(output_csv, rows)
    write_png_report(output_png, rows)
    print(json.dumps({"csv": str(output_csv), "png": str(output_png), "rows": len(rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
