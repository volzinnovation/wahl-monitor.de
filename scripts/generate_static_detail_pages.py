#!/usr/bin/env python3
"""Generate static detail pages for wahlkreise, municipalities, and booths."""

from __future__ import annotations

import argparse
import csv
import html
import json
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
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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


def current_raw_statla_csv_path() -> Path:
    metadata = json.loads((core.LATEST_DIR / "run_metadata.json").read_text(encoding="utf-8"))
    run_label = str(metadata.get("run_label") or "").strip()
    candidate = core.RAW_STATLA_DIR / f"{run_label}-statla.csv"
    if candidate.exists():
        return candidate
    return core.LOCAL_DUMMY_STATLA_PATH


def load_statla_dataset() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, str]]]:
    raw_path = current_raw_statla_csv_path()
    raw_text = core.decode_bytes(raw_path.read_bytes())
    snapshots, _party_rows = core.parse_statla_csv_rows(raw_text)
    raw_rows = list(csv.DictReader(raw_text.splitlines(), delimiter=";"))
    raw_by_row_key: Dict[str, Dict[str, str]] = {}
    for snapshot, raw_row in zip(snapshots, raw_rows):
        raw_by_row_key[snapshot["row_key"]] = raw_row
    return snapshots, raw_by_row_key


def load_latest_party_rows() -> List[Dict[str, str]]:
    return read_csv_rows(core.LATEST_DIR / "statla_party_results.csv")


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
            raw_row = raw_by_row_key[snapshot["row_key"]]
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
            raw_row = raw_by_row_key[snapshot["row_key"]]
            wk = wahlkreis_number_from_raw_row(raw_row)
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
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f6f4ee;
      --panel: #fffdfa;
      --ink: #1d2733;
      --muted: #5b6875;
      --line: #d8d1c5;
      --accent: #b5542f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background: radial-gradient(circle at top, #fffef8 0, var(--bg) 58%);
    }}
    main {{ max-width: 1500px; margin: 0 auto; padding: 32px 24px 64px; }}
    a {{ color: #87421f; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .topbar {{ display:flex; gap:14px; flex-wrap:wrap; margin-bottom:20px; color:var(--muted); }}
    .hero {{
      background: linear-gradient(135deg, rgba(181,84,47,0.14), rgba(255,255,255,0.95));
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px 24px;
      margin-bottom: 24px;
      box-shadow: 0 12px 30px rgba(29,39,51,0.06);
    }}
    .grid {{ display:grid; gap:22px; }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      overflow-x: auto;
      box-shadow: 0 8px 18px rgba(29,39,51,0.04);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 980px;
    }}
    th, td {{
      border-bottom: 1px solid #e8e1d5;
      padding: 10px 8px;
      vertical-align: top;
      text-align: right;
    }}
    th:first-child, td:first-child {{ text-align: left; position: sticky; left: 0; background: var(--panel); }}
    thead th {{ background: #fbf7ef; position: sticky; top: 0; z-index: 1; }}
    tbody tr:hover td {{ background: #fff7ec; }}
    .party-chip {{ display:inline-flex; align-items:center; gap:6px; }}
    .party-dot {{ width:10px; height:10px; border-radius:999px; display:inline-block; border:1px solid rgba(0,0,0,0.08); }}
    .vote-abs {{ font-variant-numeric: tabular-nums; font-weight: 600; }}
    .vote-rel {{ font-size: 11px; color: var(--muted); }}
    .muted {{ color: var(--muted); }}
    .stats {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-top: 14px; }}
    .stat {{ padding: 12px 14px; border: 1px solid var(--line); border-radius: 14px; background: rgba(255,255,255,0.6); }}
    .stat-label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .stat-value {{ font-size: 24px; margin-top: 6px; }}
    ul.linklist {{ list-style: none; padding: 0; margin: 0; display:grid; gap:8px; }}
    .small {{ font-size: 12px; color: var(--muted); }}
    @media (max-width: 900px) {{
      main {{ padding: 20px 14px 48px; }}
      .hero {{ padding: 18px; }}
    }}
  </style>
</head>
<body>
  <main>{body}</main>
</body>
</html>
"""


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
    body_rows: List[str] = []

    for label, row_key, snapshot in rows:
        total = vote_total_for_snapshot(snapshot, vote_type)
        grand_total += total
        votes_for_row = party_votes_by_row_key.get(row_key, {}).get(vote_type, {})
        cells = [f"<td><a href='{html.escape(link_lookup[row_key])}'>{html.escape(label)}</a></td>"]
        for party in parties:
            votes = votes_for_row.get(party, 0)
            totals_by_party[party] += votes
            cells.append(f"<td>{format_votes_cell(votes, total)}</td>")
        cells.append(f"<td>{format_votes_cell(total, total or 1)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    total_cells = ["<td><strong>Total</strong></td>"]
    for party in parties:
        total_cells.append(f"<td><strong>{format_votes_cell(totals_by_party[party], grand_total or 1)}</strong></td>")
    total_cells.append(f"<td><strong>{format_votes_cell(grand_total, grand_total or 1)}</strong></td>")

    header = "<tr><th>Area</th>" + "".join(party_header_cell(party) for party in parties) + "<th>Valid votes</th></tr>"
    return (
        f"<table><thead>{header}</thead><tbody>{''.join(body_rows)}</tbody>"
        f"<tfoot><tr>{''.join(total_cells)}</tr></tfoot></table>"
    )


def render_booth_list(
    booths: List[Dict[str, Any]],
    booth_local_links: Dict[str, str],
) -> str:
    rows: List[str] = []
    for booth in booths:
        location_link = ""
        if booth.get("structure_location_url"):
            location_link = (
                f"<a href='{html.escape(booth['structure_location_url'])}' target='_blank' rel='noopener'>"
                f"{html.escape(booth.get('structure_location_name') or '2021 location')}</a>"
            )
        rows.append(
            "<tr>"
            f"<td><a href='{html.escape(booth_local_links[booth['row_key']])}'>{html.escape(booth['display_name'])}</a></td>"
            f"<td>{html.escape(booth['gebietsart'])}</td>"
            f"<td>{booth['total_precincts']}</td>"
            f"<td>{booth['valid_votes_zweit']}</td>"
            f"<td>{location_link}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>Booth</th><th>Type</th><th>Precincts</th><th>Valid zweit votes</th><th>2021 location</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
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
        raw_row = raw_by_row_key[snapshot["row_key"]]
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
) -> None:
    links = "".join(
        f"<li><a href='wahlkreis/{html.escape(filename)}'>{html.escape(wk.zfill(2))} - {html.escape(name)}</a></li>"
        for wk, name, filename in wahlkreis_pages
    )
    body = (
        "<div class='hero'><div class='topbar'><a href='../index.html'>All elections</a></div>"
        f"<h1>{html.escape(config.election_name)} ({html.escape(config.election_key)})</h1>"
        "<p class='muted'>Static file-based drill-down from Wahlkreis to municipality to booth.</p></div>"
        f"<div class='panel'><h2>Wahlkreise</h2><ul class='linklist'>{links}</ul></div>"
    )
    write_page(output_root / "index.html", f"{config.election_name} ({config.election_key})", body)


def render_site_root_index(site_root: Path, current_config: core.Config) -> None:
    entries: List[Tuple[str, str]] = []
    for config_path in sorted((core.ROOT / "config").glob("*.json")):
        election_key = config_path.stem
        election_index = site_root / election_key / "index.html"
        if not election_index.exists():
            continue
        try:
            config_data = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        label = str(config_data.get("election_name") or election_key)
        entries.append((election_key, label))

    if not entries:
        entries.append((current_config.election_key, current_config.election_name))

    links = "".join(
        f"<li><a href='{html.escape(election_key)}/index.html'>{html.escape(label)} ({html.escape(election_key)})</a></li>"
        for election_key, label in entries
    )
    body = (
        "<div class='hero'>"
        "<h1>wahl-monitor.de</h1>"
        "<p class='muted'>Static election dashboards grouped by election key.</p>"
        "</div>"
        f"<div class='panel'><h2>Available elections</h2><ul class='linklist'>{links}</ul></div>"
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
    party_rows = load_latest_party_rows()
    party_votes = build_party_votes_by_row_key(party_rows)
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
    booth_pages: Dict[str, str] = {}
    wahlkreis_pages: List[Tuple[str, str, str]] = []
    entity_to_wahlkreis_filename: Dict[str, str] = {}

    for entity in city_entities:
        municipality_pages[entity["entity_key"]] = (
            f"../municipality/{municipality_detail_slug(entity['ags'], entity['municipality_name'], entity['wahlkreisnummer'] if entity['is_split_city'] else None)}.html"
        )

    wahlkreis_groups = build_wahlkreis_groups_from_entities(city_entities)

    for wk, municipalities in sorted(wahlkreis_groups.items(), key=lambda item: int(item[0])):
        wk_name = mapping.get(wk, {}).get("wahlkreis_name", f"Wahlkreis {wk}")
        filename = f"{wahlkreis_slug(wk, wk_name)}.html"
        wahlkreis_pages.append((wk, wk_name, filename))
        for entity in municipalities:
            entity_to_wahlkreis_filename[entity["entity_key"]] = filename

        rows_for_table = [(entity["municipality_name"], entity["entity_key"], entity["snapshot"]) for entity in municipalities]
        link_lookup = {entity["entity_key"]: municipality_pages[entity["entity_key"]] for entity in municipalities}

        first_table = render_vote_table(rows_for_table, party_votes, "Erststimmen", party_order["Erststimmen"], link_lookup)
        second_table = render_vote_table(rows_for_table, party_votes, "Zweitstimmen", party_order["Zweitstimmen"], link_lookup)
        body = (
            f"<div class='hero'><div class='topbar'><a href='../index.html'>Site index</a><span>/</span>"
            f"<a href='../../index.html'>All elections</a></div><h1>{html.escape(wk.zfill(2))} - {html.escape(wk_name)}</h1>"
            "<p class='muted'>Municipalities as rows, parties as columns. Each cell shows absolute votes and row share.</p></div>"
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
                row for row in candidate_booths if wahlkreis_number_from_raw_row(raw_by_row_key[row["row_key"]]) == wk
            ]
        booth_rows = enrich_booths_for_municipality(ags, candidate_booths, raw_by_row_key, structure)
        booth_local_links: Dict[str, str] = {}
        for booth in booth_rows:
            raw_row = raw_by_row_key[booth["row_key"]]
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
            f"<div class='hero'><div class='topbar'><a href='../index.html'>Site index</a><span>/</span>"
            f"<a href='../wahlkreis/{html.escape(wahlkreis_link)}'>Wahlkreis</a><span>/</span>"
            "<a href='../../index.html'>All elections</a></div>"
            f"<h1>{html.escape(name)}</h1><p class='muted'>Municipality detail with booth drill-down and 2021 komm.one structure links.</p>"
            "<div class='stats'>"
            f"<div class='stat'><div class='stat-label'>AGS</div><div class='stat-value'>{html.escape(ags)}</div></div>"
            f"{wk_stat}"
            f"<div class='stat'><div class='stat-label'>Valid erst votes</div><div class='stat-value'>{vote_total_for_snapshot(municipality_row, 'Erststimmen'):,}</div></div>"
            f"<div class='stat'><div class='stat-label'>Valid zweit votes</div><div class='stat-value'>{vote_total_for_snapshot(municipality_row, 'Zweitstimmen'):,}</div></div>"
            "</div></div>"
            f"<div class='panel'><h2>Booths</h2>{render_booth_list(booth_rows, booth_local_links)}</div>"
            f"<div class='panel'><h2>Booth table: Erststimmen</h2>{first_table}</div>"
            f"<div class='panel'><h2>Booth table: Zweitstimmen</h2>{second_table}</div>"
        )
        write_page(output_root / "municipality" / filename, f"{name} - {config.election_key}", body)

        for booth in booth_rows:
            raw_row = raw_by_row_key[booth["row_key"]]
            booth_filename = booth_slug(ags, booth, raw_row, wk if entity["is_split_city"] else None) + ".html"
            detail_link = ""
            if booth.get("structure_detail_url"):
                detail_link = (
                    f"<p><a href='{html.escape(booth['structure_detail_url'])}' target='_blank' rel='noopener'>"
                    "Open 2021 komm.one booth detail</a></p>"
                )
            location_link = ""
            if booth.get("structure_location_url"):
                location_link = (
                    f"<p><a href='{html.escape(booth['structure_location_url'])}' target='_blank' rel='noopener'>"
                    f"Open 2021 polling-place location: {html.escape(booth.get('structure_location_name') or 'location')}</a></p>"
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
                return f"<table><thead><tr><th>Party</th><th>Votes</th><th>Share</th></tr></thead><tbody>{rows}</tbody></table>"

            body = (
                f"<div class='hero'><div class='topbar'><a href='../municipality/{html.escape(filename)}'>{html.escape(name)}</a>"
                "<span>/</span><a href='../index.html'>Site index</a><span>/</span>"
                "<a href='../../index.html'>All elections</a></div>"
                f"<h1>{html.escape(booth['display_name'])}</h1>"
                f"<p class='muted'>{html.escape(booth['gebietsart'])} in {html.escape(name)}</p>"
                f"{detail_link}{location_link}</div>"
                f"<div class='panel'><h2>Erststimmen</h2>{render_detail_list(first_votes, 'Erststimmen')}</div>"
                f"<div class='panel'><h2>Zweitstimmen</h2>{render_detail_list(second_votes, 'Zweitstimmen')}</div>"
            )
            write_page(output_root / "booth" / booth_filename, f"{booth['display_name']} - {config.election_key}", body)

    render_index_page(config, output_root, wahlkreis_pages)
    render_site_root_index(site_root, config)
    print(f"Generated static site at {output_root}")
    print(f"Wahlkreis pages: {len(wahlkreis_pages)}")
    print(f"Municipality pages: {len(city_entities)}")
    print(f"Booth pages: {len(booth_pages)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
