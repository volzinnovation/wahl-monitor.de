#!/usr/bin/env python3
"""Generate static detail pages for wahlkreise, municipalities, and booths."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import subprocess
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import poll_election_core as core


OUTPUT_ROOT = None
STRUCTURE_DATE = "20210314"
STRUCTURE_BASE_URL = "https://wahlergebnisse.komm.one/01/produktion/wahltermin-20210314"
REMOTE_TIMEOUT_SECONDS = 20

WAHL_PARTY_COLORS = {
    "GRÜNE": "#008939",
    "CDU": "#2d3c4b",
    "SPD": "#e3000f",
    "FDP": "#ffed00",
    "AfD": "#00ccff",
    "Die Linke": "#e6007b",
    "FREIE WÄHLER": "#F29204",
    "Die PARTEI": "#b91023",
    "dieBasis": "#00cdd8",
    "KlimalisteBW": "#e1ede0",
    "ÖDP": "#ffa338",
    "Volt": "#502379",
    "Bündnis C": "#00529c",
    "BSW": "#a21749",
    "Die Gerechtigkeitspartei": "#d4d0d3",
    "Tierschutzpartei": "#018787",
    "Werteunion": "#646464",
    "WerteUnion": "#646464",
    "PdH": "#ededed",
    "PDH": "#ededed",
    "Verjüngungsforschung": "#b5b2b4",
    "PDR": "#7e68b0",
    "PdF": "#FFB27F",
    "Anderer Kreiswahlvorschlag": "#eeeeee",
}

WOKAL_ROW_RE = re.compile(
    r'<td><a href="(?P<href>Strassenverzeichnis_[^"]+\.html)"[^>]*>(?P<location>.*?)</a></td>\s*'
    r'<td[^>]*>(?P<label>.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
UEBERSICHT_ROW_RE = re.compile(
    r'<td><a href="(?P<href>Landtagswahl_BW_2021_[^"]+\.html)"[^>]*>(?P<label>.*?)</a></td>',
    re.IGNORECASE | re.DOTALL,
)
LEADING_CODE_RE = re.compile(r"^\s*([0-9A-Za-z.\-]+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate static election detail pages.")
    parser.add_argument(
        "--election-key",
        default=core.DEFAULT_ELECTION_KEY,
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional output directory. Defaults to site/<election-key>.",
    )
    parser.add_argument("--limit-ags", type=int, default=None, help="Optional municipality cap for faster test runs.")
    parser.add_argument(
        "--refresh-structure",
        action="store_true",
        help="Refresh cached 2021 komm.one municipality structure for municipalities in scope.",
    )
    parser.add_argument(
        "--structure-workers",
        type=int,
        default=16,
        help="Number of parallel workers for fetching 2021 komm.one structure. Defaults to 16.",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_csv_rows_from_text(text: str) -> List[Dict[str, str]]:
    return list(csv.DictReader(text.splitlines()))


def parse_float(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def status_label(status: str) -> str:
    return {
        "complete": "vollständig",
        "pending": "ausstehend",
        "no_data": "keine Daten",
        "prestart": "vor Start",
    }.get(status, status)


def display_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(marker in text for marker in ("Ã", "Â")):
        try:
            text = text.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return text


def slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized or "item"


def html_text(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment)
    text = html.unescape(text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def leading_code(text: str) -> str:
    match = LEADING_CODE_RE.match(text)
    if not match:
        return ""
    return match.group(1).strip()


def run_curl(url: str) -> str:
    completed = subprocess.run(
        ["curl", "-L", "--fail", "--silent", "--show-error", "--max-time", str(REMOTE_TIMEOUT_SECONDS), url],
        check=True,
        capture_output=True,
    )
    return core.decode_bytes(completed.stdout)


def structure_cache_path() -> Path:
    return core.META_DIR / "kommone_2021_structure.json"


def load_structure_cache() -> Dict[str, Any]:
    cache_path = structure_cache_path()
    if not cache_path.exists():
        return {}
    return json.loads(cache_path.read_text(encoding="utf-8"))


def save_structure_cache(cache: Dict[str, Any]) -> None:
    structure_cache_path().write_text(json.dumps(cache, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def fetch_2021_structure_for_ags(ags: str) -> Dict[str, Any]:
    base = f"{STRUCTURE_BASE_URL}/{ags}/html5"
    booth_overview_url = f"{base}/Landtagswahl_BW_2021_Land_BW_172_Uebersicht_stbz.html"
    location_overview_url = f"{base}/Wahllokaluebersicht.html"

    booth_overview_html = run_curl(booth_overview_url)
    location_overview_html = ""
    try:
        location_overview_html = run_curl(location_overview_url)
    except subprocess.CalledProcessError:
        location_overview_html = ""

    location_rows: Dict[str, Dict[str, str]] = {}
    for match in WOKAL_ROW_RE.finditer(location_overview_html):
        label = html_text(match.group("label"))
        location = html_text(match.group("location"))
        location_rows[label] = {
            "label": label,
            "location_name": location,
            "location_url": f"{base}/{match.group('href')}",
        }

    booths: List[Dict[str, str]] = []
    for match in UEBERSICHT_ROW_RE.finditer(booth_overview_html):
        label = html_text(match.group("label"))
        location_info = location_rows.get(label, {})
        booths.append(
            {
                "label": label,
                "detail_url": f"{base}/{match.group('href')}",
                "location_name": location_info.get("location_name", ""),
                "location_url": location_info.get("location_url", ""),
            }
        )

    return {
        "booth_overview_url": booth_overview_url,
        "location_overview_url": location_overview_url,
        "booths": booths,
    }


def maybe_refresh_structure_cache(
    cache: Dict[str, Any],
    ags_list: List[str],
    refresh: bool,
    workers: int,
) -> Dict[str, Any]:
    ags_to_fetch = [ags for ags in ags_list if refresh or ags not in cache]
    if not ags_to_fetch:
        return cache

    changed = False
    completed_count = 0
    total_count = len(ags_to_fetch)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_map = {executor.submit(fetch_2021_structure_for_ags, ags): ags for ags in ags_to_fetch}
        for future in as_completed(future_map):
            ags = future_map[future]
            try:
                cache[ags] = future.result()
                changed = True
            except subprocess.CalledProcessError:
                cache.setdefault(ags, {"booth_overview_url": "", "location_overview_url": "", "booths": []})
            completed_count += 1
            if completed_count % 25 == 0 or completed_count == total_count:
                print(f"Fetched 2021 structure: {completed_count}/{total_count}", flush=True)
    if changed:
        save_structure_cache(cache)
    return cache


def current_raw_statla_csv_path() -> Optional[Path]:
    metadata = json.loads((core.LATEST_DIR / "run_metadata.json").read_text(encoding="utf-8"))
    run_label = str(metadata.get("run_label") or "").strip()
    candidate = core.RAW_STATLA_DIR / f"{run_label}-statla.csv"
    if candidate.exists():
        return candidate
    return None


def load_latest_statla_snapshots() -> List[Dict[str, Any]]:
    rows = read_csv_rows(core.LATEST_DIR / "statla_snapshots.csv")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "row_key": str(row.get("row_key") or ""),
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
                "is_municipality_summary": str(row.get("is_municipality_summary") or ""),
            }
        )
    return normalized


def load_statla_dataset() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, str]]]:
    snapshots = load_latest_statla_snapshots()
    raw_by_row_key: Dict[str, Dict[str, str]] = {}
    raw_path = current_raw_statla_csv_path()
    if raw_path is not None:
        raw_text = core.decode_bytes(raw_path.read_bytes())
        raw_rows = list(csv.DictReader(raw_text.splitlines(), delimiter=";"))
        for snapshot, raw_row in zip(snapshots, raw_rows):
            raw_by_row_key[snapshot["row_key"]] = raw_row
    return snapshots, raw_by_row_key


def load_latest_party_rows() -> List[Dict[str, Any]]:
    rows = read_csv_rows(core.LATEST_DIR / "statla_party_results.csv")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "row_key": str(row.get("row_key") or ""),
                "vote_type": str(row.get("vote_type") or ""),
                "party_key": str(row.get("party_key") or ""),
                "party_name": str(row.get("party_name") or ""),
                "votes": core.parse_int(row.get("votes")),
            }
        )
    return normalized


def load_git_vote_share_history(config: core.Config) -> List[Dict[str, Any]]:
    snapshots_rel = core.repo_relative_path(core.LATEST_DIR / "statla_snapshots.csv")
    metadata_rel = core.repo_relative_path(core.LATEST_DIR / "run_metadata.json")
    party_rel = core.repo_relative_path(core.LATEST_DIR / "statla_party_results.csv")
    try:
        result = subprocess.run(
            ["git", "log", "--reverse", "--format=%H\t%cI", "--", snapshots_rel],
            cwd=core.ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []

    history_by_timestamp: Dict[str, Dict[str, Any]] = {}
    timezone = core.ZoneInfo(config.timezone)
    for line in result.stdout.splitlines():
        parts = line.strip().split("\t", 1)
        if not parts or not parts[0]:
            continue
        commit = parts[0]
        commit_timestamp = parts[1] if len(parts) > 1 else ""

        metadata_result = subprocess.run(
            ["git", "show", f"{commit}:{metadata_rel}"],
            cwd=core.ROOT,
            capture_output=True,
            text=True,
        )
        snapshot_result = subprocess.run(
            ["git", "show", f"{commit}:{snapshots_rel}"],
            cwd=core.ROOT,
            capture_output=True,
            text=True,
        )
        party_result = subprocess.run(
            ["git", "show", f"{commit}:{party_rel}"],
            cwd=core.ROOT,
            capture_output=True,
            text=True,
        )
        if metadata_result.returncode != 0 or snapshot_result.returncode != 0 or party_result.returncode != 0:
            continue

        try:
            metadata = json.loads(metadata_result.stdout)
        except json.JSONDecodeError:
            metadata = {}

        generated_at_utc = str(metadata.get("generated_at_utc") or "").strip() or commit_timestamp
        snapshots = read_csv_rows_from_text(snapshot_result.stdout)
        party_rows = read_csv_rows_from_text(party_result.stdout)

        land_snapshot = next((row for row in snapshots if str(row.get("row_key") or "") == "000000:BW:-:-:LAND"), None)
        if land_snapshot is None:
            continue
        valid_votes = core.parse_int(land_snapshot.get("valid_votes_zweit")) or 0
        if valid_votes <= 0:
            continue

        party_votes: Dict[str, int] = {}
        for row in party_rows:
            if str(row.get("row_key") or "") != "000000:BW:-:-:LAND":
                continue
            if core.canonical_vote_type(row.get("vote_type")) != "Zweitstimmen":
                continue
            party_name = core.canonical_party_name(row.get("party_name"), "Zweitstimmen")
            party_votes[party_name] = core.parse_int(row.get("votes")) or 0

        parsed_timestamp = core.parse_iso_datetime(generated_at_utc)
        if parsed_timestamp is None:
            continue
        local_dt = parsed_timestamp.astimezone(timezone)

        history_by_timestamp[generated_at_utc] = {
            "timestamp_utc": generated_at_utc,
            "timestamp_local": local_dt,
            "label": local_dt.strftime("%H:%M"),
            "reported_precincts": core.parse_int(land_snapshot.get("reported_precincts")) or 0,
            "total_precincts": core.parse_int(land_snapshot.get("total_precincts")) or 0,
            "valid_votes": valid_votes,
            "shares": {
                "AfD": ((party_votes.get("AfD") or 0) / valid_votes) * 100.0,
                "CDU": ((party_votes.get("CDU") or 0) / valid_votes) * 100.0,
                "GRÜNE": ((party_votes.get("GRÜNE") or 0) / valid_votes) * 100.0,
            },
        }

    return sorted(history_by_timestamp.values(), key=lambda item: item["timestamp_local"])


def render_vote_share_history_panel(config: core.Config) -> str:
    history = load_git_vote_share_history(config)
    if len(history) < 2:
        return (
            "<div class='panel'><h2>Verlauf der Stimmanteile am Wahlabend</h2>"
            "<p class='muted'>Nicht genug Git-Historie mit landesweiten Zweitstimmen vorhanden.</p></div>"
        )

    width = 880.0
    height = 360.0
    margin_left = 58.0
    margin_right = 88.0
    margin_top = 24.0
    margin_bottom = 52.0
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    timestamps = [item["timestamp_local"].timestamp() for item in history]
    min_x = min(timestamps)
    max_x = max(timestamps)
    parties = ["AfD", "CDU", "GRÜNE"]
    all_values = [history_item["shares"][party] for history_item in history for party in parties]
    min_share = min(all_values)
    max_share = max(all_values)
    padded_min = math.floor((min_share - 1.0) / 2.0) * 2.0
    padded_max = math.ceil((max_share + 1.0) / 2.0) * 2.0
    if padded_max - padded_min < 8.0:
        midpoint = (padded_max + padded_min) / 2.0
        padded_min = math.floor((midpoint - 4.0) / 2.0) * 2.0
        padded_max = math.ceil((midpoint + 4.0) / 2.0) * 2.0

    def x_pos(ts_value: float) -> float:
        if max_x <= min_x:
            return margin_left + (plot_width / 2.0)
        return margin_left + ((ts_value - min_x) / (max_x - min_x)) * plot_width

    def y_pos(value: float) -> float:
        return margin_top + ((padded_max - value) / max(padded_max - padded_min, 1e-9)) * plot_height

    grid_lines: List[str] = []
    tick_count = 5
    for index in range(tick_count + 1):
        share_value = padded_min + ((padded_max - padded_min) / tick_count) * index
        y = y_pos(share_value)
        grid_lines.append(
            f"<line x1='{margin_left:.2f}' y1='{y:.2f}' x2='{width - margin_right:.2f}' y2='{y:.2f}' "
            "stroke='#d8e1ec' stroke-width='1'/>"
        )
        grid_lines.append(
            f"<text x='{margin_left - 10:.2f}' y='{y + 4:.2f}' text-anchor='end' class='history-axis-label'>"
            f"{share_value:.0f}%</text>"
        )

    x_ticks: List[str] = []
    for item, ts_value in zip(history, timestamps):
        x = x_pos(ts_value)
        x_ticks.append(
            f"<line x1='{x:.2f}' y1='{margin_top + plot_height:.2f}' x2='{x:.2f}' y2='{margin_top + plot_height + 6:.2f}' "
            "stroke='#7c8a9a' stroke-width='1'/>"
        )
        x_ticks.append(
            f"<text x='{x:.2f}' y='{height - 16:.2f}' text-anchor='middle' class='history-axis-label'>"
            f"{html.escape(str(item['label']))}</text>"
        )

    series_nodes: List[str] = []
    legend_nodes: List[str] = []
    end_labels: List[Dict[str, Any]] = []
    for series_index, party in enumerate(parties):
        color = WAHL_PARTY_COLORS[party]
        points = [
            (x_pos(ts_value), y_pos(float(history_item["shares"][party])))
            for history_item, ts_value in zip(history, timestamps)
        ]
        polyline_points = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
        series_nodes.append(
            f"<polyline fill='none' stroke='{color}' stroke-width='3.5' stroke-linecap='round' "
            f"stroke-linejoin='round' points='{polyline_points}'/>"
        )
        for x, y in points:
            series_nodes.append(
                f"<circle cx='{x:.2f}' cy='{y:.2f}' r='4.5' fill='{color}' stroke='#ffffff' stroke-width='1.5'/>"
            )
        end_x, end_y = points[-1]
        latest_value = float(history[-1]["shares"][party])
        end_labels.append(
            {
                "party": party,
                "color": color,
                "x": end_x + 10.0,
                "y": end_y + 4.0,
                "value": latest_value,
            }
        )
        legend_x = margin_left + (series_index * 118.0)
        legend_nodes.append(
            f"<g transform='translate({legend_x:.2f}, {margin_top - 4:.2f})'>"
            f"<line x1='0' y1='0' x2='20' y2='0' stroke='{color}' stroke-width='3.5' stroke-linecap='round'/>"
            f"<text x='28' y='4' class='history-legend-label'>{html.escape(party)}</text>"
            "</g>"
        )

    end_labels.sort(key=lambda item: float(item["y"]))
    min_gap = 16.0
    lower_bound = margin_top + 12.0
    upper_bound = margin_top + plot_height - 4.0
    for index in range(1, len(end_labels)):
        previous_y = float(end_labels[index - 1]["y"])
        current_y = float(end_labels[index]["y"])
        if current_y - previous_y < min_gap:
            end_labels[index]["y"] = previous_y + min_gap
    if end_labels and float(end_labels[-1]["y"]) > upper_bound:
        shift = float(end_labels[-1]["y"]) - upper_bound
        for item in end_labels:
            item["y"] = float(item["y"]) - shift
    if end_labels and float(end_labels[0]["y"]) < lower_bound:
        shift = lower_bound - float(end_labels[0]["y"])
        for item in end_labels:
            item["y"] = float(item["y"]) + shift
    for item in end_labels:
        series_nodes.append(
            f"<text x='{float(item['x']):.2f}' y='{float(item['y']):.2f}' class='history-end-label' fill='{item['color']}'>"
            f"{html.escape(str(item['party']))} {float(item['value']):.1f}%</text>"
        )

    latest = history[-1]
    reporting = "?"
    if latest["total_precincts"]:
        reporting = f"{latest['reported_precincts']:,}/{latest['total_precincts']:,}".replace(",", ".")
    subtitle = (
        "Git-Historie der landesweiten StatLA-Zweitstimmen. "
        f"Letzter Stand {latest['timestamp_local'].strftime('%H:%M %Z')}, gemeldete Bezirke {reporting}."
    )

    chart = (
        f"<svg class='history-chart' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {int(width)} {int(height)}' "
        f"role='img' aria-label='Verlauf der Zweitstimmenanteile von AfD, CDU und GRÜNE am Wahlabend'>"
        f"<rect x='0' y='0' width='{width:.2f}' height='{height:.2f}' rx='14' fill='#fbfdff'/>"
        f"{''.join(grid_lines)}"
        f"<line x1='{margin_left:.2f}' y1='{margin_top + plot_height:.2f}' x2='{width - margin_right:.2f}' y2='{margin_top + plot_height:.2f}' stroke='#7c8a9a' stroke-width='1.2'/>"
        f"{''.join(x_ticks)}"
        f"{''.join(legend_nodes)}"
        f"{''.join(series_nodes)}"
        "</svg>"
    )
    return (
        "<div class='panel'><h2>Verlauf der Stimmanteile am Wahlabend</h2>"
        f"<p class='small'>{html.escape(subtitle)}</p>"
        f"{chart}</div>"
    )


def load_latest_kommone_snapshots() -> List[Dict[str, Any]]:
    rows = read_csv_rows(core.LATEST_DIR / "kommone_snapshots.csv")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "ags": str(row.get("ags") or ""),
                "municipality_name": str(row.get("municipality_name") or ""),
                "status": str(row.get("status") or ""),
                "reported_precincts": core.parse_int(row.get("reported_precincts")),
                "total_precincts": core.parse_int(row.get("total_precincts")),
                "voters_total": core.parse_int(row.get("voters_total")),
                "valid_votes": core.parse_int(row.get("valid_votes")),
                "invalid_votes": core.parse_int(row.get("invalid_votes")),
                "source_timestamp": row.get("source_timestamp"),
                "payload_hash": row.get("payload_hash"),
                "error_message": row.get("error_message"),
            }
        )
    return normalized


def load_latest_kommone_party_rows() -> List[Dict[str, Any]]:
    rows = read_csv_rows(core.LATEST_DIR / "kommone_party_results.csv")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "ags": str(row.get("ags") or ""),
                "municipality_name": str(row.get("municipality_name") or ""),
                "vote_type": str(row.get("vote_type") or ""),
                "party": str(row.get("party") or ""),
                "votes": core.parse_int(row.get("votes")),
                "percent": parse_float(row.get("percent")),
            }
        )
    return normalized


def load_latest_source_diffs() -> List[Dict[str, Any]]:
    rows = read_csv_rows(core.REPORT_DIR / "latest_source_diff.csv")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "ags": str(row.get("ags") or ""),
                "municipality_name": str(row.get("municipality_name") or ""),
                "metric": str(row.get("metric") or ""),
                "kommone_value": parse_float(row.get("kommone_value")),
                "statla_value": parse_float(row.get("statla_value")),
                "delta": parse_float(row.get("delta")),
            }
        )
    return normalized


def build_party_votes_by_row_key(party_rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Dict[str, int]]]:
    out: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    for row in party_rows:
        row_key = row["row_key"]
        vote_type = core.canonical_vote_type(row["vote_type"])
        party = core.canonical_party_name(row["party_name"], vote_type)
        out[row_key][vote_type][party] = core.parse_int(row["votes"]) or 0
    return out


def group_rows_by_ags(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    rows_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in snapshots:
        ags = str(row.get("ags") or "")
        if ags:
            rows_by_ags[ags].append(row)
    return rows_by_ags


def build_wahlkreis_groups(
    municipality_rows: Dict[str, Dict[str, Any]],
    mapping: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for wk, item in mapping.items():
        for ags in sorted(item.get("ags_set", set())):
            row = municipality_rows.get(ags)
            if row is not None:
                out[wk].append(row)
    for wk in out:
        out[wk].sort(key=lambda row: str(row.get("municipality_name") or ""))
    return out


def vote_total_for_snapshot(snapshot: Dict[str, Any], vote_type: str) -> int:
    if vote_type == "Erststimmen":
        return core.parse_int(snapshot.get("valid_votes_erst")) or 0
    return core.parse_int(snapshot.get("valid_votes_zweit")) or 0


def reporting_counts(snapshot: Dict[str, Any]) -> Tuple[int, int]:
    reported = core.parse_int(snapshot.get("reported_precincts")) or 0
    total = core.parse_int(snapshot.get("total_precincts")) or 0
    return reported, total


def reporting_status_label(snapshot: Dict[str, Any]) -> str:
    reported, total = reporting_counts(snapshot)
    if total <= 0:
        return "keine Daten"
    if reported <= 0:
        return "offen"
    if reported >= total:
        return "vollständig"
    return "teilweise"


def pct(value: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{(value / total) * 100.0:.2f}%"


def format_votes_cell(votes: int, total: int) -> str:
    return f"<div class='vote-abs'>{votes:,}</div><div class='vote-rel'>{pct(votes, total)}</div>"


def party_header_cell(party: str) -> str:
    color = WAHL_PARTY_COLORS.get(party, "#cbd5e1")
    return (
        f"<th class='party-col'><span class='party-chip'>"
        f"<span class='party-dot' style='background:{color}'></span>{html.escape(party)}</span></th>"
    )


def booth_slug(ags: str, snapshot: Dict[str, Any], raw_row: Dict[str, str], wk: Optional[str] = None) -> str:
    prefix = f"{ags}-wk-{wk.zfill(2)}" if wk else ags
    gebietsart = str(raw_row.get("Gebietsart") or snapshot.get("gebietsart") or "").strip().upper()
    kind = {
        "URNENWAHLBEZIRK": "urn",
        "BRIEFWAHLBEZIRK": "brief",
    }.get(gebietsart, slugify(gebietsart) or "booth")
    code = str(raw_row.get("Gebietsnummer") or raw_row.get("Bezirksnummer") or "").strip()
    if code:
        return f"{prefix}-{kind}-{slugify(code)}"
    return f"{prefix}-{kind}-{slugify(str(snapshot.get('gebietsnummer') or snapshot['row_key']))}"


def municipality_slug(ags: str, name: str) -> str:
    return f"{ags}-{slugify(name)}"


def wahlkreis_slug(wk: str, name: str) -> str:
    return f"{wk.zfill(2)}-{slugify(name)}"


def municipality_detail_slug(ags: str, name: str, wk: Optional[str] = None) -> str:
    if wk:
        return f"{ags}-wk-{wk.zfill(2)}-{slugify(name)}"
    return municipality_slug(ags, name)


def wahlkreis_number_from_raw_row(raw_row: Dict[str, str]) -> str:
    return str(raw_row.get("Wahlkreisnummer") or "").strip()


def fallback_raw_row(snapshot: Dict[str, Any]) -> Dict[str, str]:
    gebietsart = str(snapshot.get("gebietsart") or "").strip()
    gebietsnummer = str(snapshot.get("gebietsnummer") or "").strip()
    municipality_name = str(snapshot.get("municipality_name") or "").strip()
    wahlkreisnummer = ""
    if gebietsart.upper() == "WAHLKREIS":
        wahlkreisnummer = core.normalize_wahlkreis_nummer(gebietsnummer)
    return {
        "Wahlkreisnummer": wahlkreisnummer,
        "Gemeindename": municipality_name,
        "Gebietsname": municipality_name or gebietsnummer or str(snapshot.get("row_key") or ""),
        "Gebietsnummer": gebietsnummer,
        "Bezirksnummer": gebietsnummer,
    }


def raw_row_for_snapshot(raw_by_row_key: Dict[str, Dict[str, str]], snapshot: Dict[str, Any]) -> Dict[str, str]:
    return raw_by_row_key.get(str(snapshot.get("row_key") or ""), fallback_raw_row(snapshot))


def municipality_name_for_snapshot(snapshot: Dict[str, Any], raw_row: Dict[str, str]) -> str:
    return str(snapshot.get("municipality_name") or raw_row.get("Gemeindename") or snapshot.get("ags") or "").strip()


def build_city_entities(
    snapshots: List[Dict[str, Any]],
    raw_by_row_key: Dict[str, Dict[str, str]],
    mapping: Dict[str, Dict[str, Any]],
    selected_ags: List[str],
) -> List[Dict[str, Any]]:
    ags_to_wahlkreise: Dict[str, set[str]] = defaultdict(set)
    for wk, item in mapping.items():
        for ags in item.get("ags_set", set()):
            ags_to_wahlkreise[str(ags)].add(str(wk))

    rows_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for snapshot in snapshots:
        ags = str(snapshot.get("ags") or "")
        if ags in selected_ags:
            rows_by_ags[ags].append(snapshot)

    entities: List[Dict[str, Any]] = []
    for ags in selected_ags:
        ags_rows = rows_by_ags.get(ags, [])
        municipality_rows = [row for row in ags_rows if str(row.get("is_municipality_summary")).lower() == "true"]
        split_wahlkreise = ags_to_wahlkreise.get(ags, set())

        if municipality_rows and len(split_wahlkreise) <= 1:
            snapshot = municipality_rows[0]
            raw_row = raw_row_for_snapshot(raw_by_row_key, snapshot)
            wk = next(iter(split_wahlkreise), wahlkreis_number_from_raw_row(raw_row))
            name = municipality_name_for_snapshot(snapshot, raw_row)
            entities.append(
                {
                    "entity_key": snapshot["row_key"],
                    "ags": ags,
                    "municipality_name": name,
                    "wahlkreisnummer": wk,
                    "snapshot": snapshot,
                    "raw_row": raw_row,
                    "is_split_city": False,
                }
            )
            continue

        for snapshot in ags_rows:
            if str(snapshot.get("gebietsart") or "").upper() != "WAHLKREIS":
                continue
            raw_row = raw_row_for_snapshot(raw_by_row_key, snapshot)
            wk = wahlkreis_number_from_raw_row(raw_row) or str(snapshot.get("gebietsnummer") or "")
            name = municipality_name_for_snapshot(snapshot, raw_row)
            entities.append(
                {
                    "entity_key": snapshot["row_key"],
                    "ags": ags,
                    "municipality_name": name,
                    "wahlkreisnummer": wk,
                    "snapshot": snapshot,
                    "raw_row": raw_row,
                    "is_split_city": True,
                }
            )

    entities.sort(
        key=lambda item: (
            int(item["wahlkreisnummer"] or 0),
            item["municipality_name"],
            item["entity_key"],
        )
    )
    return entities


def build_wahlkreis_groups_from_entities(
    entities: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entity in entities:
        wk = str(entity.get("wahlkreisnummer") or "")
        if wk:
            out[wk].append(entity)
    for wk in out:
        out[wk].sort(key=lambda item: item["municipality_name"])
    return out


def render_page(title: str, body: str, root_path: str = "../") -> str:
    header = (
        "<header class='site-header'>"
        "<div class='header-inner'>"
        "<a href='" + root_path + "index.html' class='header-brand'>"
        "<span class='header-icon' aria-hidden='true'>🗳️</span>"
        "<span class='header-title'>wahl-monitor.de</span>"
        "</a>"
        "<span class='header-label'>Wahlergebnisse Baden-Württemberg</span>"
        "</div>"
        "</header>"
    )
    footer = (
        "<footer class='site-footer'>"
        "<div class='footer-inner'>"
        "<div class='footer-grid'>"
        "<div>"
        "<strong>wahl-monitor.de</strong>"
        "<p>Unabhängiges Open-Source-Projekt zur transparenten Darstellung von Wahlergebnissen.</p>"
        "</div>"
        "<div>"
        "<strong>Impressum</strong>"
        "<p>Open Source &amp; Open Data</p>"
        "<p class='footer-links'>"
        "<a href='https://github.com/volzinnovation/wahl-monitor.de'>GitHub</a>"
        "</p>"
        "</div>"
        "</div>"
        "</div>"
        "</footer>"
    )
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="Wahlergebnisse Baden-Württemberg – Live-Tracking und Detailseiten für alle Wahlkreise, Gemeinden und Wahlbezirke.">
  <title>{html.escape(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #f0f2f5;
      --panel: #ffffff;
      --ink: #1a1a2e;
      --muted: #6b7280;
      --line: #e5e7eb;
      --accent: #003366;
      --accent-light: #0055a4;
      --accent-hover: #004080;
      --success: #16a34a;
      --warning: #d97706;
      --radius: 12px;
      --shadow-sm: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
      --shadow-md: 0 4px 14px rgba(0,0,0,0.06), 0 2px 6px rgba(0,0,0,0.04);
      --shadow-lg: 0 10px 30px rgba(0,0,0,0.08), 0 4px 10px rgba(0,0,0,0.04);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      color: var(--ink);
      background: var(--bg);
      line-height: 1.6;
      -webkit-font-smoothing: antialiased;
    }}
    /* ── Header ── */
    .site-header {{
      background: var(--accent);
      color: #fff;
      position: sticky;
      top: 0;
      z-index: 100;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }}
    .header-inner {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 0 24px;
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }}
    .header-brand {{
      display: flex;
      align-items: center;
      gap: 10px;
      color: #fff;
      text-decoration: none;
      font-weight: 700;
      font-size: 17px;
      letter-spacing: -0.01em;
    }}
    .header-brand:hover {{ opacity: 0.9; text-decoration: none; }}
    .header-icon {{ font-size: 22px; }}
    .header-label {{
      font-size: 13px;
      opacity: 0.8;
      font-weight: 400;
    }}
    /* ── Main ── */
    main {{ max-width: 1400px; margin: 0 auto; padding: 28px 24px 64px; }}
    a {{ color: var(--accent-light); text-decoration: none; transition: color 0.15s ease; }}
    a:hover {{ color: var(--accent-hover); text-decoration: underline; }}
    .topbar {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 18px;
      color: var(--muted);
      font-size: 13px;
      align-items: center;
    }}
    .topbar a {{
      padding: 4px 10px;
      border-radius: 6px;
      background: rgba(0,51,102,0.06);
      font-weight: 500;
      transition: background 0.15s ease;
    }}
    .topbar a:hover {{ background: rgba(0,51,102,0.12); text-decoration: none; }}
    /* ── Hero ── */
    .hero {{
      background: linear-gradient(135deg, rgba(0,51,102,0.07) 0%, rgba(255,255,255,0.95) 100%);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 28px 28px 24px;
      margin-bottom: 24px;
      box-shadow: var(--shadow-md);
    }}
    .hero h1 {{
      font-size: 26px;
      font-weight: 700;
      margin: 0 0 6px;
      letter-spacing: -0.02em;
      color: var(--accent);
    }}
    .hero p {{ margin: 0; }}
    /* ── Grid & Panels ── */
    .grid {{ display: grid; gap: 20px; }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 22px;
      overflow-x: auto;
      box-shadow: var(--shadow-sm);
      transition: box-shadow 0.2s ease;
      -webkit-overflow-scrolling: touch;
    }}
    .panel:hover {{ box-shadow: var(--shadow-md); }}
    .panel h2 {{
      font-size: 17px;
      font-weight: 600;
      margin: 0 0 14px;
      color: var(--accent);
      letter-spacing: -0.01em;
    }}
    /* ── Tables ── */
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 700px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 10px;
      vertical-align: top;
      text-align: right;
    }}
    th:first-child, td:first-child {{ text-align: left; position: sticky; left: 0; background: var(--panel); z-index: 1; }}
    thead th {{
      background: #f8f9fb;
      position: sticky;
      top: 0;
      z-index: 2;
      font-weight: 600;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      color: var(--muted);
    }}
    tbody tr {{ transition: background 0.12s ease; }}
    tbody tr:hover td {{ background: #f0f4ff; }}
    .party-chip {{ display: inline-flex; align-items: center; gap: 6px; }}
    .party-dot {{ width: 10px; height: 10px; border-radius: 999px; display: inline-block; border: 1px solid rgba(0,0,0,0.08); }}
    .vote-abs {{ font-variant-numeric: tabular-nums; font-weight: 600; }}
    .vote-rel {{ font-size: 11px; color: var(--muted); }}
    .muted {{ color: var(--muted); }}
    /* ── Stat Cards ── */
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .stat {{
      padding: 16px 16px 14px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(255,255,255,0.7);
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }}
    .stat:hover {{ transform: translateY(-1px); box-shadow: var(--shadow-md); }}
    .stat-label {{
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 500;
    }}
    .stat-value {{
      font-size: 22px;
      margin-top: 6px;
      font-weight: 700;
      color: var(--ink);
      font-variant-numeric: tabular-nums;
    }}
    /* ── Links & Lists ── */
    ul.linklist {{ list-style: none; padding: 0; margin: 0; display: grid; gap: 6px; }}
    ul.linklist li a {{
      display: block;
      padding: 10px 14px;
      border-radius: 8px;
      background: #f8f9fb;
      font-weight: 500;
      transition: background 0.15s ease, transform 0.1s ease;
      border: 1px solid transparent;
    }}
    ul.linklist li a:hover {{
      background: #eef2ff;
      border-color: rgba(0,85,164,0.15);
      text-decoration: none;
      transform: translateX(3px);
    }}
    .small {{ font-size: 12px; color: var(--muted); }}
    details {{ border-top: 1px solid var(--line); padding-top: 12px; }}
    details + details {{ margin-top: 10px; }}
    summary {{
      cursor: pointer;
      font-weight: 600;
      padding: 6px 0;
      transition: color 0.15s ease;
    }}
    summary:hover {{ color: var(--accent-light); }}
    /* ── Map ── */
    .dashboard-map svg {{ width: 100%; height: auto; display: block; border-radius: 8px; }}
    .dashboard-map a:hover path {{ stroke-width: 1.6; filter: brightness(0.97); }}
    .dashboard-map path {{ transition: stroke-width 120ms ease, filter 120ms ease; }}
    .history-chart {{ width: 100%; height: auto; display: block; margin-top: 14px; }}
    .history-axis-label {{
      fill: var(--muted);
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }}
    .history-end-label {{
      font-size: 12px;
      font-weight: 700;
      paint-order: stroke;
      stroke: #fbfdff;
      stroke-width: 4px;
      stroke-linejoin: round;
    }}
    .history-legend-label {{
      fill: var(--ink);
      font-size: 12px;
      font-weight: 600;
    }}
    .inline-list {{ margin: 0; padding-left: 18px; }}
    .inline-list li {{ margin-bottom: 4px; }}
    .compact td, .compact th {{ padding: 8px 7px; }}
    /* ── Footer ── */
    .site-footer {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 0 24px 40px;
    }}
    .footer-inner {{
      border-top: 2px solid var(--line);
      color: var(--muted);
      font-size: 13px;
      padding-top: 24px;
    }}
    .footer-grid {{
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 32px;
    }}
    .footer-inner strong {{
      color: var(--ink);
      display: block;
      margin-bottom: 4px;
      font-size: 14px;
    }}
    .footer-inner p {{
      margin: 4px 0 0;
      line-height: 1.5;
    }}
    .footer-links {{ display: flex; gap: 16px; }}
    .footer-links a {{ font-weight: 500; }}
    /* ── Responsive ── */
    @media (max-width: 900px) {{
      main {{ padding: 20px 16px 48px; }}
      .hero {{ padding: 20px; }}
      .hero h1 {{ font-size: 22px; }}
      .site-footer {{ padding: 0 16px 28px; }}
      .footer-grid {{ grid-template-columns: 1fr; gap: 20px; }}
      .header-label {{ display: none; }}
      .stats {{ grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); }}
      .stat-value {{ font-size: 18px; }}
    }}
    @media (max-width: 600px) {{
      .header-inner {{ padding: 0 14px; height: 50px; }}
      .header-brand {{ font-size: 15px; }}
      main {{ padding: 14px 12px 40px; }}
      .hero {{ padding: 16px; border-radius: 10px; }}
      .hero h1 {{ font-size: 19px; }}
      .panel {{ padding: 16px; border-radius: 10px; }}
      table {{ min-width: auto; font-size: 12px; }}
      th, td {{ padding: 8px 6px; }}
      .stats {{ grid-template-columns: 1fr 1fr; gap: 8px; }}
      .stat {{ padding: 12px; }}
      .stat-value {{ font-size: 16px; }}
      ul.linklist li a {{ padding: 10px 12px; font-size: 14px; }}
    }}
  </style>
</head>
<body>
  {header}
  <main>{body}</main>
  {footer}
</body>
</html>
"""


def tracking_start_hhmm(config: core.Config) -> str:
    return core.tracking_start_local_dt(config).strftime("%H:%M")


def write_page(path: Path, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_page(title, body), encoding="utf-8")


def prepare_output_dirs(output_root: Path) -> None:
    for subdir_name in ["wahlkreis", "municipality", "booth"]:
        subdir = output_root / subdir_name
        subdir.mkdir(parents=True, exist_ok=True)
        for path in subdir.glob("*.html"):
            path.unlink()


def render_vote_table(
    rows: List[Tuple[str, str, Dict[str, Any]]],
    party_votes_by_row_key: Dict[str, Dict[str, Dict[str, int]]],
    vote_type: str,
    parties: List[str],
    link_lookup: Dict[str, str],
) -> str:
    totals_by_party = {party: 0 for party in parties}
    grand_total = 0
    total_reported = 0
    total_precincts = 0
    body_rows: List[str] = []

    for label, row_key, snapshot in rows:
        total = vote_total_for_snapshot(snapshot, vote_type)
        grand_total += total
        reported, precinct_total = reporting_counts(snapshot)
        total_reported += reported
        total_precincts += precinct_total
        votes_for_row = party_votes_by_row_key.get(row_key, {}).get(vote_type, {})
        cells = [f"<td><a href='{html.escape(link_lookup[row_key])}'>{html.escape(label)}</a></td>"]
        cells.append(f"<td>{html.escape(reporting_status_label(snapshot))}</td>")
        cells.append(f"<td>{reported}/{precinct_total}</td>")
        for party in parties:
            votes = votes_for_row.get(party, 0)
            totals_by_party[party] += votes
            cells.append(f"<td>{format_votes_cell(votes, total)}</td>")
        cells.append(f"<td>{format_votes_cell(total, total or 1)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    total_cells = ["<td><strong>Gesamt</strong></td>"]
    overall_status = "vollständig" if total_precincts > 0 and total_reported >= total_precincts else ("teilweise" if total_reported > 0 else "offen")
    total_cells.append(f"<td><strong>{overall_status}</strong></td>")
    total_cells.append(f"<td><strong>{total_reported}/{total_precincts}</strong></td>")
    for party in parties:
        total_cells.append(f"<td><strong>{format_votes_cell(totals_by_party[party], grand_total or 1)}</strong></td>")
    total_cells.append(f"<td><strong>{format_votes_cell(grand_total, grand_total or 1)}</strong></td>")

    header = (
        "<tr><th>Gebiet</th><th>Status</th><th>Gemeldete Bezirke</th>"
        + "".join(party_header_cell(party) for party in parties)
        + "<th>Gültige Stimmen</th></tr>"
    )
    return (
        "<p class='small'>Anteile beziehen sich auf die bisher gemeldeten gültigen Stimmen des jeweiligen Gebiets.</p>"
        f"<table><thead>{header}</thead><tbody>{''.join(body_rows)}</tbody>"
        f"<tfoot><tr>{''.join(total_cells)}</tr></tfoot></table>"
    )


def render_booth_list(
    booths: List[Dict[str, Any]],
    booth_local_links: Dict[str, str],
) -> str:
    rows: List[str] = []
    for booth in booths:
        reported, total = reporting_counts(booth)
        location_link = ""
        if booth.get("structure_location_url"):
            location_link = (
                f"<a href='{html.escape(booth['structure_location_url'])}' target='_blank' rel='noopener'>"
                f"{html.escape(booth.get('structure_location_name') or 'Wahllokal 2021')}</a>"
            )
        rows.append(
            "<tr>"
            f"<td><a href='{html.escape(booth_local_links[booth['row_key']])}'>{html.escape(booth['display_name'])}</a></td>"
            f"<td>{html.escape(booth['gebietsart'])}</td>"
            f"<td>{html.escape(reporting_status_label(booth))}</td>"
            f"<td>{reported}/{total}</td>"
            f"<td>{booth['valid_votes_zweit']}</td>"
            f"<td>{location_link}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>Wahlbezirk</th><th>Typ</th><th>Status</th><th>Gemeldete Bezirke</th><th>Gültige Zweitstimmen</th><th>Wahllokal 2021</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def render_vote_type_summary_table(vote_type: str, rows: List[Dict[str, Any]]) -> str:
    body_rows: List[str] = []
    for row in rows:
        label = "<strong>Gesamt</strong>" if str(row.get("row_type") or "") == "TOTAL" else html.escape(str(row.get("party") or ""))
        body_rows.append(
            "<tr>"
            f"<td>{label}</td>"
            f"<td>{int(row.get('kommone_votes') or 0):,}</td>"
            f"<td>{float(row.get('kommone_share_percent') or 0.0):.2f}%</td>"
            f"<td>{int(row.get('statla_votes') or 0):,}</td>"
            f"<td>{float(row.get('statla_share_percent') or 0.0):.2f}%</td>"
            f"<td>{int(row.get('delta_votes') or 0):+d}</td>"
            f"<td>{float(row.get('delta_share_percent') or 0.0):+.2f}%</td>"
            "</tr>"
        )
    return (
        f"<div class='panel'><h2>{html.escape(vote_type)}</h2>"
        "<table class='compact'><thead><tr>"
        "<th>Partei</th><th>`komm.one` Stimmen</th><th>`komm.one` Anteil</th>"
        "<th>`statla` Stimmen</th><th>`statla` Anteil</th><th>Differenz Stimmen</th><th>Differenz Anteil</th>"
        f"</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"
    )


def render_party_dashboard(
    party_summary: List[Dict[str, Any]],
    party_details: Dict[str, List[Dict[str, Any]]],
    municipality_link_by_ags: Dict[str, str],
) -> str:
    if not party_summary:
        return "<div class='panel'><h2>Parteien</h2><p class='muted'>Noch keine Parteidaten verfügbar.</p></div>"

    summary_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row['party']))}</td>"
        f"<td>{int(row['votes']):,}</td>"
        f"<td>{float(row['share_percent']):.2f}%</td>"
        "</tr>"
        for row in party_summary
    )

    details_blocks: List[str] = []
    for row in party_summary:
        party = str(row["party"])
        detail_rows = party_details.get(party, [])
        table_rows: List[str] = []
        for item in detail_rows[:100]:
            municipality = html.escape(str(item["municipality_name"]))
            if item["ags"] in municipality_link_by_ags:
                municipality = f"<a href='{html.escape(municipality_link_by_ags[item['ags']])}'>{municipality}</a>"
            votes = "" if item.get("votes") is None else f"{int(item['votes']):,}"
            percent = "" if item.get("percent") is None else f"{float(item['percent']):.2f}%"
            table_rows.append(
                "<tr>"
                f"<td>{item['ags']}</td>"
                f"<td>{municipality}</td>"
                f"<td>{votes}</td>"
                f"<td>{percent}</td>"
                f"<td>{html.escape(status_label(str(item['status'])))}</td>"
                "</tr>"
            )
        details_blocks.append(
            f"<details><summary>{html.escape(party)}</summary>"
            "<table class='compact'><thead><tr><th>AGS</th><th>Gemeinde</th><th>Stimmen</th><th>Anteil</th><th>Status</th></tr></thead>"
            f"<tbody>{''.join(table_rows)}</tbody></table></details>"
        )

    return (
        "<div class='panel'><h2>Parteien</h2>"
        "<table class='compact'><thead><tr><th>Partei</th><th>Stimmen</th><th>Anteil</th></tr></thead>"
        f"<tbody>{summary_rows}</tbody></table>{''.join(details_blocks)}</div>"
    )


def render_pending_results(
    pending_rows: List[Dict[str, Any]],
    municipality_link_by_ags: Dict[str, str],
) -> str:
    if not pending_rows:
        return "<div class='panel'><h2>Ausstehende Ergebnisse</h2><p class='muted'>Keine ausstehenden Gemeinden.</p></div>"

    table_rows: List[str] = []
    for row in pending_rows[:200]:
        municipality_name = html.escape(str(row["municipality_name"]))
        if row["ags"] in municipality_link_by_ags:
            municipality_name = f"<a href='{html.escape(municipality_link_by_ags[row['ags']])}'>{municipality_name}</a>"
        reported = row.get("reported_precincts")
        total = row.get("total_precincts")
        rep_total = "" if reported is None or total is None else f"{reported}/{total}"
        table_rows.append(
            "<tr>"
            f"<td>{row['ags']}</td>"
            f"<td>{municipality_name}</td>"
            f"<td>{rep_total}</td>"
            f"<td>{html.escape(status_label(core.municipality_status(row)))}</td>"
            "</tr>"
        )
    return (
        f"<div class='panel'><h2>Ausstehende Ergebnisse</h2><p class='small'>Zeige {min(len(pending_rows), 200)} von {len(pending_rows)} Zeilen.</p>"
        "<table class='compact'><thead><tr><th>AGS</th><th>Gemeinde</th><th>`komm.one` gemeldet/gesamt</th><th>Status</th></tr></thead>"
        f"<tbody>{''.join(table_rows)}</tbody></table></div>"
    )


def render_source_diff_summary(diff_rows: List[Dict[str, Any]]) -> str:
    metrics = ["reported_precincts", "total_precincts", "voters_total", "valid_votes"]
    summary: Dict[str, Dict[str, float]] = {}
    for row in diff_rows:
        metric = str(row.get("metric") or "")
        bucket = summary.setdefault(metric, {"count_with_delta": 0, "abs_delta_sum": 0.0})
        if isinstance(row.get("delta"), (int, float)):
            bucket["count_with_delta"] += 1
            bucket["abs_delta_sum"] += abs(float(row["delta"]))
    rows_html = "".join(
        "<tr>"
        f"<td>{html.escape(metric)}</td>"
        f"<td>{int(summary.get(metric, {}).get('count_with_delta', 0))}</td>"
        f"<td>{float(summary.get(metric, {}).get('abs_delta_sum', 0.0)):.2f}</td>"
        "</tr>"
        for metric in metrics
    )
    return (
        "<div class='panel'><h2>Quellenvergleich</h2>"
        "<table class='compact'><thead><tr><th>Metrik</th><th>Zeilen mit Differenz</th><th>Summe(|delta|)</th></tr></thead>"
        f"<tbody>{rows_html}</tbody></table></div>"
    )


def statla_wahlkreis_winner_map(
    statla_snapshots: List[Dict[str, Any]],
    party_votes_by_row_key: Dict[str, Dict[str, Dict[str, int]]],
    vote_type: str,
) -> Dict[str, Dict[str, Any]]:
    winners: Dict[str, Dict[str, Any]] = {}
    for row in statla_snapshots:
        if str(row.get("gebietsart") or "").strip().upper() != "WAHLKREIS":
            continue
        wk = core.normalize_wahlkreis_nummer(row.get("gebietsnummer") or row.get("row_key"))
        if not wk:
            continue
        vote_totals = party_votes_by_row_key.get(str(row.get("row_key") or ""), {}).get(vote_type, {})
        if not vote_totals:
            continue
        winner_party, winner_votes = max(
            vote_totals.items(),
            key=lambda item: (int(item[1]), str(item[0])),
        )
        winners[wk] = {
            "winner_party": winner_party,
            "winner_votes": int(winner_votes or 0),
            "winner_total_votes": sum(int(v or 0) for v in vote_totals.values()),
        }
    return winners


def render_wahlkreis_overview_table(
    status_rows: List[Dict[str, Any]],
    link_by_wk: Dict[str, str],
) -> str:
    rows_html: List[str] = []
    for row in status_rows:
        wk = str(row["wahlkreisnummer"])
        label = f"{wk.zfill(2)} - {row['wahlkreisname']}"
        href = link_by_wk.get(wk)
        linked_label = f"<a href='{html.escape(href)}'>{html.escape(label)}</a>" if href else html.escape(label)
        reported = row.get("reported_precincts")
        total = row.get("total_precincts")
        rep_total = "" if reported is None or total is None else f"{reported}/{total}"
        rows_html.append(
            "<tr>"
            f"<td>{linked_label}</td>"
            f"<td>{html.escape(status_label(str(row['status'])))}</td>"
            f"<td>{html.escape(str(row.get('winner_party_erst') or ''))}</td>"
            f"<td>{html.escape(str(row.get('winner_party_zweit') or ''))}</td>"
            f"<td>{rep_total}</td>"
            "</tr>"
        )
    return (
        "<div class='panel'><h2>Wahlkreisstatus</h2>"
        "<table class='compact'><thead><tr><th>Wahlkreis</th><th>Status</th><th>Führend Erststimmen</th><th>Führend Zweitstimmen</th><th>Gemeldete Bezirke</th></tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody></table></div>"
    )


def render_clickable_wahlkreis_map(
    features: List[Dict[str, Any]],
    status_rows: List[Dict[str, Any]],
    link_by_wk: Dict[str, str],
) -> str:
    if not features:
        return "<p class='muted'>Keine Wahlkreis-Geometrie verfügbar.</p>"

    status_by_wk = {row["wahlkreisnummer"]: row for row in status_rows}
    colors = {
        "prestart": "#d1d5db",
        "no_data": "#e5e7eb",
        "pending": "#f59e0b",
        "complete": "#16a34a",
    }

    all_points: List[Tuple[float, float]] = []
    for feature in features:
        for ring in core.iter_exterior_rings(feature.get("geometry") or {}):
            for point in ring:
                if len(point) >= 2:
                    all_points.append((float(point[0]), float(point[1])))
    if not all_points:
        return "<p class='muted'>Keine Wahlkreis-Geometrie verfügbar.</p>"

    min_lon = min(p[0] for p in all_points)
    max_lon = max(p[0] for p in all_points)
    min_lat = min(p[1] for p in all_points)
    max_lat = max(p[1] for p in all_points)
    width = 1000.0
    height = 1300.0
    pad = 40.0
    scale_x = (width - 2 * pad) / max(max_lon - min_lon, 1e-9)
    scale_y = (height - 2 * pad) / max(max_lat - min_lat, 1e-9)
    scale = min(scale_x, scale_y)

    path_nodes: List[str] = []
    for feature in features:
        props = feature.get("properties") or {}
        wk = core.normalize_wahlkreis_nummer(props.get("Nummer"))
        if not wk:
            continue
        row = status_by_wk.get(wk, {})
        status = str(row.get("status") or "no_data")
        name = display_text(props.get("WK Name") or row.get("wahlkreisname") or f"Wahlkreis {wk}")
        winner_party = str(row.get("winner_party") or "").strip()
        fill = WAHL_PARTY_COLORS.get(winner_party, colors.get(status, colors["no_data"]))
        d_parts: List[str] = []
        for ring in core.iter_exterior_rings(feature.get("geometry") or {}):
            if len(ring) < 3:
                continue
            projected = [
                core.project_point(
                    float(pt[0]),
                    float(pt[1]),
                    min_lon=min_lon,
                    min_lat=min_lat,
                    scale=scale,
                    pad=pad,
                    height=height,
                )
                for pt in ring
            ]
            d_parts.append("M " + " L ".join(f"{x:.2f} {y:.2f}" for x, y in projected) + " Z")
        if not d_parts:
            continue
        title_text = f"{wk.zfill(2)} {name} ({status_label(status)})"
        if winner_party:
            title_text += f" - Zweitstimmen: {winner_party}"
        title = html.escape(title_text)
        path_markup = f"<path d=\"{' '.join(d_parts)}\" fill=\"{fill}\" stroke=\"#111827\" stroke-width=\"0.8\"><title>{title}</title></path>"
        href = link_by_wk.get(wk)
        if href:
            path_nodes.append(f"<a href='{html.escape(href)}'>{path_markup}</a>")
        else:
            path_nodes.append(path_markup)

    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {int(width)} {int(height)}'>"
        "<rect width='100%' height='100%' fill='#ffffff'/>"
        f"{''.join(path_nodes)}"
        "</svg>"
    )


def structure_booth_maps(
    structure: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    urn_by_label: Dict[str, Dict[str, str]] = {}
    urn_by_code: Dict[str, Dict[str, str]] = {}
    brief_rows: List[Dict[str, str]] = []
    for booth in structure.get("booths", []):
        label = str(booth.get("label") or "")
        if label.lower().startswith("briefwahlbezirk"):
            brief_rows.append(booth)
        else:
            urn_by_label[label] = booth
            code = leading_code(label)
            if code:
                urn_by_code[code] = booth
    return urn_by_label, urn_by_code, brief_rows


def enrich_booths_for_municipality(
    ags: str,
    booth_rows: List[Dict[str, Any]],
    raw_by_row_key: Dict[str, Dict[str, str]],
    structure: Dict[str, Any],
) -> List[Dict[str, Any]]:
    urn_by_label, urn_by_code, brief_rows = structure_booth_maps(structure)
    enriched: List[Dict[str, Any]] = []
    brief_index = 0
    for snapshot in sorted(booth_rows, key=lambda item: item["row_key"]):
        raw_row = raw_row_for_snapshot(raw_by_row_key, snapshot)
        display_name = str(raw_row.get("Gebietsname") or raw_row.get("Gebietsnummer") or snapshot["row_key"]).strip()
        booth_code = str(raw_row.get("Bezirksnummer") or "").strip()
        structure_detail_url = ""
        structure_location_url = ""
        structure_location_name = ""
        if str(snapshot.get("gebietsart") or "").upper() == "BRIEFWAHLBEZIRK":
            if brief_index < len(brief_rows):
                matched = brief_rows[brief_index]
                brief_index += 1
                display_name = matched.get("label") or display_name
                structure_detail_url = matched.get("detail_url", "")
                structure_location_url = matched.get("location_url", "")
                structure_location_name = matched.get("location_name", "")
        else:
            matched = (
                urn_by_code.get(booth_code)
                or urn_by_label.get(display_name)
                or urn_by_label.get(str(raw_row.get("Gebietsnummer") or ""))
            )
            if matched:
                structure_detail_url = matched.get("detail_url", "")
                structure_location_url = matched.get("location_url", "")
                structure_location_name = matched.get("location_name", "")
        enriched.append(
            {
                **snapshot,
                "display_name": display_name,
                "gebietsart": str(snapshot.get("gebietsart") or ""),
                "total_precincts": core.parse_int(snapshot.get("total_precincts")) or 0,
                "valid_votes_zweit": core.parse_int(snapshot.get("valid_votes_zweit")) or 0,
                "structure_detail_url": structure_detail_url,
                "structure_location_url": structure_location_url,
                "structure_location_name": structure_location_name,
            }
        )
    return enriched


def render_index_page(
    config: core.Config,
    output_root: Path,
    wahlkreis_pages: List[Tuple[str, str, str]],
    wahlkreis_status_rows: List[Dict[str, Any]],
    wahlkreis_link_by_wk: Dict[str, str],
    municipality_link_by_ags: Dict[str, str],
    latest_kommone_snapshots: List[Dict[str, Any]],
    latest_kommone_party_rows: List[Dict[str, Any]],
    statla_party_rows: List[Dict[str, Any]],
    diff_rows: List[Dict[str, Any]],
) -> None:
    run_metadata = json.loads((core.LATEST_DIR / "run_metadata.json").read_text(encoding="utf-8"))
    polled_at = core.parse_iso_datetime(str(run_metadata.get("generated_at_utc") or ""))
    if polled_at is not None:
        polled_at_local = polled_at.astimezone(core.ZoneInfo(config.timezone)).strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        polled_at_local = "-"

    tracking_start = core.format_local_dt(core.tracking_start_local_dt(config))
    statla_mode = str(run_metadata.get("statla_mode") or "-")
    statla_url = str(run_metadata.get("statla_url") or config.statla_live_csv_url)
    if statla_mode == "DUMMY" and Path(statla_url).is_absolute():
        statla_url = config.statla_dummy_csv_url

    wahlkreis_counts = {"prestart": 0, "no_data": 0, "pending": 0, "complete": 0}
    for row in wahlkreis_status_rows:
        wahlkreis_counts[str(row["status"])] = wahlkreis_counts.get(str(row["status"]), 0) + 1

    summary_by_vote_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if config.publish_source_comparison:
        vote_type_summary = core.party_summary_by_vote_type_sources(latest_kommone_party_rows, statla_party_rows)
        for row in vote_type_summary:
            summary_by_vote_type[str(row["vote_type"] or "Unbekannt")].append(row)

    features = core.load_wahlkreis_features()
    operations = [
        f"`python scripts/poll_election.py --election-key {config.election_key}`",
        f"`python scripts/run_local_poll_loop.py --election-key {config.election_key} --start-at {tracking_start_hhmm(config)}`",
        f"`python scripts/run_local_mock_poll.py --election-key {config.election_key} --iterations 1 --limit-ags 10`",
        f"`python scripts/validate_dummy_statla_result.py --election-key {config.election_key}`",
        f"`python scripts/generate_static_detail_pages.py --election-key {config.election_key}`",
    ]
    body = (
        "<div class='hero'><div class='topbar'><a href='../index.html'>Alle Wahlen</a></div>"
        f"<h1>{html.escape(config.election_name)} ({html.escape(config.election_key)})</h1>"
        "<p class='muted'>Statische Übersicht mit Drill-down von Wahlkreis zu Gemeinde und Wahlbezirk.</p>"
        f"<div class='stats'>"
        f"<div class='stat'><div class='stat-label'>Letzter Poll</div><div class='stat-value'>{html.escape(polled_at_local)}</div></div>"
        f"<div class='stat'><div class='stat-label'>Trackingstart</div><div class='stat-value'>{html.escape(tracking_start)}</div></div>"
        f"<div class='stat'><div class='stat-label'>Gemeinden</div><div class='stat-value'>{len(latest_kommone_snapshots):,}</div></div>"
        f"<div class='stat'><div class='stat-label'>Wahlkreise vollständig</div><div class='stat-value'>{wahlkreis_counts['complete']}</div></div>"
        "</div></div>"
        "<div class='grid'>"
        "<div class='panel dashboard-map'><h2>Klickbare Wahlkreiskarte</h2>"
        "<p class='small'>Jeder Wahlkreis führt direkt zur Detailseite.</p>"
        f"{render_clickable_wahlkreis_map(features, wahlkreis_status_rows, wahlkreis_link_by_wk)}</div>"
        f"{render_vote_share_history_panel(config)}"
        f"{render_wahlkreis_overview_table(wahlkreis_status_rows, wahlkreis_link_by_wk)}"
        + (
            "".join(
            render_vote_type_summary_table(vote_type, rows)
            for vote_type, rows in sorted(
                summary_by_vote_type.items(),
                key=lambda item: {"Erststimmen": 0, "Zweitstimmen": 1}.get(item[0], 99),
            )
        )
            if config.publish_source_comparison
            else ""
        )
        + (render_source_diff_summary(diff_rows) if config.publish_source_comparison else "")
        + "<div class='panel'><h2>Datenquellen</h2>"
        + "<ul class='inline-list'>"
        + "<li>`komm.one`-Gemeindeseiten in der aktuellen HTML-Struktur</li>"
        + f"<li>Statistik BW CSV (Modus: <strong>{html.escape(statla_mode)}</strong>): <a href='{html.escape(statla_url)}'>{html.escape(statla_url)}</a></li>"
        + "</ul></div>"
        + "<div class='panel'><h2>Betrieb</h2><ul class='inline-list'>"
        + "".join(f"<li>{item}</li>" for item in operations)
        + "</ul></div>"
        + "<div class='panel'><h2>Abdeckung</h2><ul class='inline-list'>"
        + f"<li>Wahlkreise vollständig: <strong>{wahlkreis_counts['complete']}</strong></li>"
        + f"<li>Wahlkreise ausstehend: <strong>{wahlkreis_counts['pending']}</strong></li>"
        + f"<li>Wahlkreise ohne Daten: <strong>{wahlkreis_counts['no_data']}</strong></li>"
        + "</ul></div>"
        + "</div>"
    )
    write_page(output_root / "index.html", f"{config.election_name} ({config.election_key})", body)


def render_site_root_index(site_root: Path, current_config: core.Config) -> None:
    entries_by_key: Dict[str, str] = {}
    for site_dir in sorted(path for path in site_root.iterdir() if path.is_dir()):
        election_key = site_dir.name
        election_index = site_dir / "index.html"
        if not election_index.exists():
            continue

        label = ""
        config_path = core.ROOT / "config" / f"{election_key}.json"
        if config_path.exists():
            try:
                config_data = json.loads(config_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                config_data = {}
            label = str(config_data.get("election_name") or "").strip()

        if not label:
            manifest_path = core.ROOT / "data" / election_key / "metadata" / "setup_manifest.json"
            if manifest_path.exists():
                try:
                    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    manifest_data = {}
                label = str(manifest_data.get("election_name") or "").strip()

        if label:
            entries_by_key[election_key] = label

    entries = sorted(entries_by_key.items())

    if not entries:
        entries.append((current_config.election_key, current_config.election_name))

    links = "".join(
        f"<li><a href='{html.escape(election_key)}/index.html'>{html.escape(label)} ({html.escape(election_key)})</a></li>"
        for election_key, label in entries
    )
    body = (
        "<div class='hero'>"
        "<h1>wahl-monitor.de</h1>"
        "<p class='muted'>Statische Wahldashboards gruppiert nach Wahlkennung.</p>"
        "</div>"
        f"<div class='panel'><h2>Verfügbare Wahlen</h2><ul class='linklist'>{links}</ul></div>"
    )
    write_page(site_root / "index.html", "wahl-monitor.de", body)


def main() -> int:
    args = parse_args()
    core.set_active_election(election_key=args.election_key)
    config = core.load_config()
    output_root = args.output_root or (core.ROOT / "site" / config.election_key)
    output_root.mkdir(parents=True, exist_ok=True)
    prepare_output_dirs(output_root)

    snapshots, raw_by_row_key = load_statla_dataset()
    statla_party_rows = load_latest_party_rows()
    party_votes = build_party_votes_by_row_key(statla_party_rows)
    party_order = core.fixed_party_order_by_vote_type()
    mapping = core.load_wahlkreis_mapping()
    site_root = output_root.parent

    ags_in_scope = sorted(
        {
            str(row.get("ags") or "")
            for row in snapshots
            if str(row.get("ags") or "") and str(row.get("ags") or "") in {ags for item in mapping.values() for ags in item.get("ags_set", set())}
        }
    )
    if args.limit_ags is not None:
        selected_ags = ags_in_scope[: args.limit_ags]
    else:
        selected_ags = ags_in_scope

    latest_kommone_snapshots = [row for row in load_latest_kommone_snapshots() if row["ags"] in selected_ags]
    latest_kommone_party_rows = [row for row in load_latest_kommone_party_rows() if row["ags"] in selected_ags]
    latest_source_diffs = (
        [row for row in load_latest_source_diffs() if row["ags"] in selected_ags]
        if config.publish_source_comparison
        else []
    )

    city_entities = build_city_entities(snapshots, raw_by_row_key, mapping, selected_ags)

    booth_rows_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in snapshots:
        if str(row.get("ags") or "") not in selected_ags:
            continue
        if str(row.get("gebietsart") or "").upper() in {"URNENWAHLBEZIRK", "BRIEFWAHLBEZIRK"}:
            booth_rows_by_ags[row["ags"]].append(row)

    structure_cache = maybe_refresh_structure_cache(
        load_structure_cache(),
        selected_ags,
        args.refresh_structure,
        args.structure_workers,
    )

    municipality_pages: Dict[str, str] = {}
    municipality_index_links: Dict[str, str] = {}
    booth_pages: Dict[str, str] = {}
    wahlkreis_pages: List[Tuple[str, str, str]] = []
    wahlkreis_link_by_wk: Dict[str, str] = {}
    entity_to_wahlkreis_filename: Dict[str, str] = {}

    for entity in city_entities:
        slug = municipality_detail_slug(
            entity["ags"],
            entity["municipality_name"],
            entity["wahlkreisnummer"] if entity["is_split_city"] else None,
        )
        municipality_pages[entity["entity_key"]] = f"../municipality/{slug}.html"
        municipality_index_links.setdefault(entity["ags"], f"municipality/{slug}.html")

    wahlkreis_groups = build_wahlkreis_groups_from_entities(city_entities)

    for wk, municipalities in sorted(wahlkreis_groups.items(), key=lambda item: int(item[0])):
        wk_name = mapping.get(wk, {}).get("wahlkreis_name", f"Wahlkreis {wk}")
        filename = f"{wahlkreis_slug(wk, wk_name)}.html"
        wahlkreis_pages.append((wk, wk_name, filename))
        wahlkreis_link_by_wk[wk] = f"wahlkreis/{filename}"
        for entity in municipalities:
            entity_to_wahlkreis_filename[entity["entity_key"]] = filename

        rows_for_table = [(entity["municipality_name"], entity["entity_key"], entity["snapshot"]) for entity in municipalities]
        link_lookup = {entity["entity_key"]: municipality_pages[entity["entity_key"]] for entity in municipalities}

        first_table = render_vote_table(rows_for_table, party_votes, "Erststimmen", party_order["Erststimmen"], link_lookup)
        second_table = render_vote_table(rows_for_table, party_votes, "Zweitstimmen", party_order["Zweitstimmen"], link_lookup)
        body = (
            f"<div class='hero'><div class='topbar'><a href='../index.html'>Startseite dieser Wahl</a><span>/</span>"
            f"<a href='../../index.html'>Alle Wahlen</a></div><h1>{html.escape(wk.zfill(2))} - {html.escape(wk_name)}</h1>"
            "<p class='muted'>Gemeinden als Zeilen, Parteien als Spalten. Jede Zelle zeigt absolute Stimmen und den Zeilenanteil.</p></div>"
            f"<div class='panel'><h2>Erststimmen</h2>{first_table}</div>"
            f"<div class='panel'><h2>Zweitstimmen</h2>{second_table}</div>"
        )
        write_page(output_root / "wahlkreis" / filename, f"{wk_name} - {config.election_key}", body)

    for entity in city_entities:
        ags = entity["ags"]
        municipality_row = entity["snapshot"]
        name = entity["municipality_name"]
        wk = entity["wahlkreisnummer"]
        filename = municipality_detail_slug(ags, name, wk if entity["is_split_city"] else None) + ".html"
        structure = structure_cache.get(ags, {"booths": []})
        candidate_booths = booth_rows_by_ags.get(ags, [])
        if entity["is_split_city"]:
            candidate_booths = [
                row
                for row in candidate_booths
                if not wahlkreis_number_from_raw_row(raw_row_for_snapshot(raw_by_row_key, row))
                or wahlkreis_number_from_raw_row(raw_row_for_snapshot(raw_by_row_key, row)) == wk
            ]
        booth_rows = enrich_booths_for_municipality(ags, candidate_booths, raw_by_row_key, structure)
        booth_local_links: Dict[str, str] = {}
        for booth in booth_rows:
            raw_row = raw_row_for_snapshot(raw_by_row_key, booth)
            booth_filename = booth_slug(ags, booth, raw_row, wk if entity["is_split_city"] else None) + ".html"
            booth_pages[booth["row_key"]] = f"../booth/{booth_filename}"
            booth_local_links[booth["row_key"]] = booth_pages[booth["row_key"]]

        row_links = {booth["row_key"]: booth_local_links[booth["row_key"]] for booth in booth_rows}
        rows_for_table = [(booth["display_name"], booth["row_key"], booth) for booth in booth_rows]
        first_table = render_vote_table(rows_for_table, party_votes, "Erststimmen", party_order["Erststimmen"], row_links)
        second_table = render_vote_table(rows_for_table, party_votes, "Zweitstimmen", party_order["Zweitstimmen"], row_links)
        wahlkreis_link = entity_to_wahlkreis_filename.get(entity["entity_key"], "../index.html")
        wk_stat = ""
        if wk:
            wk_stat = f"<div class='stat'><div class='stat-label'>Wahlkreis</div><div class='stat-value'>{html.escape(wk.zfill(2))}</div></div>"
        body = (
            f"<div class='hero'><div class='topbar'><a href='../index.html'>Startseite dieser Wahl</a><span>/</span>"
            f"<a href='../wahlkreis/{html.escape(wahlkreis_link)}'>Wahlkreis</a><span>/</span>"
            "<a href='../../index.html'>Alle Wahlen</a></div>"
            f"<h1>{html.escape(name)}</h1><p class='muted'>Gemeindedetail mit Drill-down zu Wahlbezirken und Verweisen auf die Struktur von 2021.</p>"
            "<div class='stats'>"
            f"<div class='stat'><div class='stat-label'>AGS</div><div class='stat-value'>{html.escape(ags)}</div></div>"
            f"{wk_stat}"
            f"<div class='stat'><div class='stat-label'>Gültige Erststimmen</div><div class='stat-value'>{vote_total_for_snapshot(municipality_row, 'Erststimmen'):,}</div></div>"
            f"<div class='stat'><div class='stat-label'>Gültige Zweitstimmen</div><div class='stat-value'>{vote_total_for_snapshot(municipality_row, 'Zweitstimmen'):,}</div></div>"
            "</div></div>"
            f"<div class='panel'><h2>Wahlbezirke</h2>{render_booth_list(booth_rows, booth_local_links)}</div>"
            f"<div class='panel'><h2>Wahlbezirkstabelle: Erststimmen</h2>{first_table}</div>"
            f"<div class='panel'><h2>Wahlbezirkstabelle: Zweitstimmen</h2>{second_table}</div>"
        )
        write_page(output_root / "municipality" / filename, f"{name} - {config.election_key}", body)

        for booth in booth_rows:
            raw_row = raw_row_for_snapshot(raw_by_row_key, booth)
            booth_filename = booth_slug(ags, booth, raw_row, wk if entity["is_split_city"] else None) + ".html"
            detail_link = ""
            if booth.get("structure_detail_url"):
                detail_link = (
                    f"<p><a href='{html.escape(booth['structure_detail_url'])}' target='_blank' rel='noopener'>"
                    "Detailseite 2021 bei komm.one öffnen</a></p>"
                )
            location_link = ""
            if booth.get("structure_location_url"):
                location_link = (
                    f"<p><a href='{html.escape(booth['structure_location_url'])}' target='_blank' rel='noopener'>"
                    f"Wahllokal 2021 öffnen: {html.escape(display_text(booth.get('structure_location_name') or 'Wahllokal'))}</a></p>"
                )
            first_votes = party_votes.get(booth["row_key"], {}).get("Erststimmen", {})
            second_votes = party_votes.get(booth["row_key"], {}).get("Zweitstimmen", {})
            def render_detail_list(votes: Dict[str, int], vote_type: str) -> str:
                total = vote_total_for_snapshot(booth, vote_type)
                ordered = party_order[vote_type]
                rows = "".join(
                    "<tr>"
                    f"<td>{html.escape(party)}</td>"
                    f"<td>{votes.get(party, 0):,}</td>"
                    f"<td>{pct(votes.get(party, 0), total)}</td>"
                    "</tr>"
                    for party in ordered
                )
                return f"<table><thead><tr><th>Partei</th><th>Stimmen</th><th>Anteil</th></tr></thead><tbody>{rows}</tbody></table>"

            body = (
                f"<div class='hero'><div class='topbar'><a href='../municipality/{html.escape(filename)}'>{html.escape(name)}</a>"
                "<span>/</span><a href='../index.html'>Startseite dieser Wahl</a><span>/</span>"
                "<a href='../../index.html'>Alle Wahlen</a></div>"
                f"<h1>{html.escape(booth['display_name'])}</h1>"
                f"<p class='muted'>{html.escape(booth['gebietsart'])} in {html.escape(name)}</p>"
                f"{detail_link}{location_link}</div>"
                f"<div class='panel'><h2>Erststimmen</h2>{render_detail_list(first_votes, 'Erststimmen')}</div>"
                f"<div class='panel'><h2>Zweitstimmen</h2>{render_detail_list(second_votes, 'Zweitstimmen')}</div>"
            )
            write_page(output_root / "booth" / booth_filename, f"{booth['display_name']} - {config.election_key}", body)

    wahlkreis_status_rows = core.compute_wahlkreis_status_rows(
        features=core.load_wahlkreis_features(),
        mapping=mapping,
        kommone_snapshots=latest_kommone_snapshots,
        statla_snapshots=snapshots,
        prestart=False,
    )
    statla_erst_winners = statla_wahlkreis_winner_map(snapshots, party_votes, "Erststimmen")
    statla_zweit_winners = statla_wahlkreis_winner_map(snapshots, party_votes, "Zweitstimmen")
    for row in wahlkreis_status_rows:
        wk = str(row.get("wahlkreisnummer") or "").strip()
        erst_winner = statla_erst_winners.get(wk)
        zweit_winner = statla_zweit_winners.get(wk)
        if erst_winner:
            row["winner_party_erst"] = erst_winner.get("winner_party")
        if zweit_winner:
            row["winner_party_zweit"] = zweit_winner.get("winner_party")
            row.update(zweit_winner)

    render_index_page(
        config,
        output_root,
        wahlkreis_pages,
        wahlkreis_status_rows,
        wahlkreis_link_by_wk,
        municipality_index_links,
        latest_kommone_snapshots,
        latest_kommone_party_rows,
        statla_party_rows,
        latest_source_diffs,
    )
    render_site_root_index(site_root, config)
    print(f"Generated static site at {output_root}")
    print(f"Wahlkreis pages: {len(wahlkreis_pages)}")
    print(f"Municipality pages: {len(city_entities)}")
    print(f"Booth pages: {len(booth_pages)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
