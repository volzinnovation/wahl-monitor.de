#!/usr/bin/env python3
"""Build Rheinland-Pfalz 2026 election metadata from official published sources."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "2026-rlp" / "metadata"

ELECTION_INFO_URL = "https://landtag-rlp.de/de/wahl-2026.htm"
RESULTS_PAGE_URL = "https://www.wahlen.rlp.de/landtagswahl/ergebnisse"
RESULT_PORTAL_URL_2026 = "https://wahlen.rlp-ltw-2026.23degrees.eu/wk/0000000000000/overview"
TREE_URL_2021 = "https://wahlen.rlp-ltw-2021.23degrees.eu/assets/wk-vec-tree.json"
GLOBAL_URL_2021 = "https://wahlen.rlp-ltw-2021.23degrees.eu/assets/json/global.json"
RESULTS_XLSX_URL_2021 = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/btw/csv/2021/LW_2021_GESAMT.xlsx"
)
GEODATA_URL_2026 = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Shapefiles/Geodaten_LW2026_RP.zip"
)

CITY_NAME_OVERRIDES = {
    "11100000": "Koblenz",
    "21100000": "Trier",
    "31100000": "Frankenthal (Pfalz)",
    "31200000": "Kaiserslautern",
    "31300000": "Landau in der Pfalz",
    "31400000": "Ludwigshafen am Rhein",
    "31500000": "Mainz",
    "31600000": "Neustadt an der Weinstrasse",
    "31700000": "Pirmasens",
    "31800000": "Speyer",
    "31900000": "Worms",
    "32000000": "Zweibruecken",
}


@dataclass(frozen=True)
class CitySourceRecord:
    ags: str
    verification_status: str
    source_type: str
    source_scope: str
    url: str
    verified_from: str
    notes: str


CITY_SOURCE_RECORDS = [
    CitySourceRecord(
        ags="11100000",
        verification_status="partial",
        source_type="municipal-landing-page",
        source_scope="city archive and PDF reports",
        url="https://wahlen.koblenz.de/wahlpraesentation/landtagswahlen/",
        verified_from="city elections page",
        notes=(
            "Verified archive landing page and 2021 PDF reports. "
            "No separate 2021 live HTML result app confirmed in this pass."
        ),
    ),
    CitySourceRecord(
        ags="21100000",
        verification_status="found",
        source_type="municipal-static-html",
        source_scope="citywide second vote plus WK24/WK25 first vote",
        url="https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021zweit.html",
        verified_from="city archive page",
        notes=(
            "City archive also links https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK25.html "
            "and https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK24.html."
        ),
    ),
    CitySourceRecord(
        ags="31200000",
        verification_status="found",
        source_type="municipal-result-app",
        source_scope="WK44 and WK45 result apps",
        url="https://wahlen.kaiserslautern.de/ltw202144_app.html",
        verified_from="city election page",
        notes=(
            "City page also links https://wahlen.kaiserslautern.de/ltw202145_app.html "
            "for the second split constituency."
        ),
    ),
    CitySourceRecord(
        ags="31400000",
        verification_status="not_found",
        source_type="unknown",
        source_scope="split city follow-up needed",
        url="",
        verified_from="web search result summary",
        notes=(
            "Ludwigshafen is split across two Wahlkreise in the official tree, "
            "but this pass did not confirm a separate municipal 2021 result portal."
        ),
    ),
    CitySourceRecord(
        ags="31500000",
        verification_status="found",
        source_type="municipal-result-app",
        source_scope="split city with citywide and constituency pages",
        url="https://wahl.mainz.de/wahlapp/ltw2021wk27.html",
        verified_from="direct result page plus city elections page",
        notes=(
            "Direct page verified. Mainz also used wk28 and wk29 pages and a citywide second-vote portal."
        ),
    ),
    CitySourceRecord(
        ags="31800000",
        verification_status="found",
        source_type="municipal-result-app",
        source_scope="citywide result app",
        url="http://chamaeleon-hosting.de/sv_speyer/wahlen/app/ltw2021.html",
        verified_from="city archive page",
        notes=(
            "Speyer archive page also links state portal pages for the same election."
        ),
    ),
    CitySourceRecord(
        ags="31900000",
        verification_status="found",
        source_type="municipal-result-app",
        source_scope="citywide result app",
        url="https://wahlen.worms.de/webapp/ltw2021.html",
        verified_from="direct URL",
        notes="Direct 2021 Worms portal verified.",
    ),
    CitySourceRecord(
        ags="32000000",
        verification_status="candidate",
        source_type="municipal-result-app",
        source_scope="citywide candidate URL",
        url="https://wahlen.zweibruecken.de/2021/ltw2021.html",
        verified_from="search result only",
        notes=(
            "Search results indicated a city portal, but certificate/path verification failed from this environment."
        ),
    ),
]


def fetch_bytes(url: str, timeout_seconds: int = 60) -> bytes:
    request = Request(url, headers={"User-Agent": "wahl-monitor-setup/1.0"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read()
    except (HTTPError, URLError, OSError):
        completed = subprocess.run(
            ["curl", "-L", "--fail", "--silent", "--show-error", url],
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            message = completed.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"Failed to fetch {url}: {message}") from None
        return completed.stdout


def decode_json_url(url: str) -> Any:
    return json.loads(fetch_bytes(url).decode("utf-8"))


def ensure_out_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def digits_only(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def ags_from_node_id(node_id: str) -> str:
    digits = digits_only(node_id)
    if len(digits) < 11:
        return ""
    return digits[3:11]


def wahlkreis_id_from_node_id(node_id: str) -> str:
    digits = digits_only(node_id)
    return digits[:3] if len(digits) >= 3 else ""


def node_overview_url(node_id: str) -> str:
    return f"https://wahlen.rlp-ltw-2021.23degrees.eu/wk/{node_id}/overview"


def base_name_from_fragment(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    if "/" in text:
        return text.split("/", 1)[0].strip()
    if ", " in text:
        left, right = text.split(", ", 1)
        if right in {"Kfr.", "TeilKfr.", "VG"}:
            return left.strip()
    return text


def leaf_nodes(tree: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        node
        for node in tree
        if not node.get("children") and int(node.get("level") or 0) in {4, 5}
    ]


def municipality_name_by_ags(nodes: List[Dict[str, Any]]) -> Dict[str, str]:
    names_by_ags: Dict[str, Counter[str]] = defaultdict(Counter)
    for node in nodes:
        ags = ags_from_node_id(str(node.get("bezeichnung") or ""))
        if not ags:
            continue
        derived = CITY_NAME_OVERRIDES.get(ags) or base_name_from_fragment(str(node.get("name") or ""))
        if derived:
            names_by_ags[ags][derived] += 1

    out: Dict[str, str] = {}
    for ags, counts in names_by_ags.items():
        if ags in CITY_NAME_OVERRIDES:
            out[ags] = CITY_NAME_OVERRIDES[ags]
            continue
        out[ags] = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return out


def build_wahlkreis_name_map(tree: List[Dict[str, Any]]) -> Dict[str, str]:
    return {
        wahlkreis_id_from_node_id(str(node.get("bezeichnung") or "")): str(node.get("name") or "")
        for node in tree
        if int(node.get("level") or 0) == 2
    }


def build_fragment_rows(tree: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    leaves = leaf_nodes(tree)
    municipality_names = municipality_name_by_ags(leaves)
    wahlkreis_names = build_wahlkreis_name_map(tree)
    wahlkreise_by_ags: Dict[str, set[str]] = defaultdict(set)
    for node in leaves:
        ags = ags_from_node_id(str(node.get("bezeichnung") or ""))
        wk = wahlkreis_id_from_node_id(str(node.get("bezeichnung") or ""))
        if ags and wk:
            wahlkreise_by_ags[ags].add(wk)

    rows: List[Dict[str, Any]] = []
    for node in sorted(leaves, key=lambda item: (ags_from_node_id(str(item["bezeichnung"])), str(item["bezeichnung"]))):
        node_id = str(node.get("bezeichnung") or "")
        ags = ags_from_node_id(node_id)
        wk = wahlkreis_id_from_node_id(node_id)
        if not ags or not wk:
            continue
        rows.append(
            {
                "ags": ags,
                "municipality_name": municipality_names.get(ags, ""),
                "fragment_name": str(node.get("name") or ""),
                "node_id": node_id,
                "level": int(node.get("level") or 0),
                "wahlkreis_id": wk,
                "wahlkreis_name": wahlkreis_names.get(wk, ""),
                "is_split_ags": "true" if len(wahlkreise_by_ags.get(ags, set())) > 1 else "false",
                "source": "official-state-portal-2021-tree",
                "source_url": node_overview_url(node_id),
            }
        )
    return rows


def build_municipality_rows(fragment_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in fragment_rows:
        ags = str(row["ags"])
        bucket = grouped.setdefault(
            ags,
            {
                "ags": ags,
                "municipality_name": str(row["municipality_name"]),
                "fragment_count": 0,
                "wahlkreise": set(),
            },
        )
        bucket["fragment_count"] += 1
        bucket["wahlkreise"].add(str(row["wahlkreis_id"]))
        if not bucket["municipality_name"]:
            bucket["municipality_name"] = str(row["municipality_name"])

    output: List[Dict[str, Any]] = []
    for ags, bucket in sorted(grouped.items()):
        wahlkreise = sorted(bucket["wahlkreise"])
        output.append(
            {
                "ags": ags,
                "municipality_name": bucket["municipality_name"],
                "source": "official-state-portal-2021-tree",
                "fragment_count": bucket["fragment_count"],
                "wahlkreis_count": len(wahlkreise),
                "wahlkreis_ids": "|".join(wahlkreise),
            }
        )
    return output


def build_split_rows(municipality_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "ags": row["ags"],
            "municipality_name": row["municipality_name"],
            "wahlkreis_count": row["wahlkreis_count"],
            "wahlkreis_ids": row["wahlkreis_ids"],
            "reason": "leaf nodes from the official 2021 state portal appear in more than one Wahlkreis",
        }
        for row in municipality_rows
        if int(row["wahlkreis_count"]) > 1
    ]


def build_city_source_rows(
    municipality_rows: List[Dict[str, Any]],
    split_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    municipality_name_map = {str(row["ags"]): str(row["municipality_name"]) for row in municipality_rows}
    split_agss = {str(row["ags"]) for row in split_rows}
    rows: List[Dict[str, Any]] = []
    for record in CITY_SOURCE_RECORDS:
        rows.append(
            {
                "ags": record.ags,
                "municipality_name": municipality_name_map.get(record.ags, CITY_NAME_OVERRIDES.get(record.ags, "")),
                "is_split_ags": "true" if record.ags in split_agss else "false",
                "verification_status": record.verification_status,
                "source_type": record.source_type,
                "source_scope": record.source_scope,
                "url": record.url,
                "verified_from": record.verified_from,
                "notes": record.notes,
            }
        )
    return sorted(rows, key=lambda row: row["ags"])


def write_setup_manifest(global_config: Dict[str, Any], municipality_rows: List[Dict[str, Any]], split_rows: List[Dict[str, Any]], city_source_rows: List[Dict[str, Any]]) -> None:
    manifest = {
        "election_key": "2026-rlp",
        "election_name": "Landtagswahl Rheinland-Pfalz 2026",
        "election_date": "2026-03-22",
        "tracking_start_local": "2026-03-22T18:30:00",
        "timezone": "Europe/Berlin",
        "official_sources": {
            "election_info_url": ELECTION_INFO_URL,
            "results_page_url": RESULTS_PAGE_URL,
            "predicted_result_portal_url": RESULT_PORTAL_URL_2026,
            "historical_tree_url_2021": TREE_URL_2021,
            "historical_global_url_2021": GLOBAL_URL_2021,
            "historical_results_xlsx_url_2021": RESULTS_XLSX_URL_2021,
            "geodata_zip_url_2026": GEODATA_URL_2026,
        },
        "historical_portal_2021": {
            "timestamp": global_config.get("timestamp"),
            "state": global_config.get("state"),
            "counted_node_count": len(global_config.get("counted", {})),
        },
        "derived_counts": {
            "municipality_count": len(municipality_rows),
            "split_municipality_count": len(split_rows),
            "city_source_inventory_count": len(city_source_rows),
            "split_municipalities_with_verified_secondary_source": sum(
                1
                for row in city_source_rows
                if row["is_split_ags"] == "true" and row["verification_status"] == "found"
            ),
        },
    }
    path = OUT_DIR / "setup_manifest.json"
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_source_inventory_md(split_rows: List[Dict[str, Any]], city_source_rows: List[Dict[str, Any]]) -> None:
    lines = [
        "# Rheinland-Pfalz 2026 Setup Notes",
        "",
        "## Official Published Inputs",
        f"- Election date: `2026-03-22` from [{ELECTION_INFO_URL}]({ELECTION_INFO_URL}).",
        f"- Official results landing page: [{RESULTS_PAGE_URL}]({RESULTS_PAGE_URL}).",
        f"- Official page says the detailed result portal starts about 30 minutes after polls close; a predictable portal pattern is [{RESULT_PORTAL_URL_2026}]({RESULT_PORTAL_URL_2026}).",
        f"- Official 2026 geodata ZIP: [{GEODATA_URL_2026}]({GEODATA_URL_2026}).",
        f"- Official 2021 machine-readable tree: [{TREE_URL_2021}]({TREE_URL_2021}).",
        f"- Official 2021 state workbook download: [{RESULTS_XLSX_URL_2021}]({RESULTS_XLSX_URL_2021}).",
        "",
        "## Lessons From The 2026 BW Rollout",
        "- Do not assume one municipality maps to one Wahlkreis. Split municipalities need to be identified before election night.",
        "- Keep state-level and city-level sources separate. Secondary city portals are validation or drill-down sources, not silent replacements.",
        "- Build the source inventory before the portal goes live. The missing work is discovery, not parsing.",
        "- Avoid state-specific fallback logic. BW assumptions about source names, CSV shape, and HTML routes should not leak into RLP setup.",
        "",
        "## Split Municipalities From The Official 2021 State Tree",
    ]

    for row in split_rows:
        lines.append(
            f"- `{row['ags']}` {row['municipality_name']}: Wahlkreise `{row['wahlkreis_ids'].replace('|', ', ')}`"
        )

    lines.extend(
        [
            "",
            "Only five AGS are split across multiple Wahlkreise in the official 2021 tree. "
            "Those are the main places where a secondary city source is operationally valuable.",
            "",
            "## City Secondary Sources Found In This Pass",
        ]
    )

    for row in city_source_rows:
        prefix = f"- `{row['ags']}` {row['municipality_name']}: `{row['verification_status']}`"
        if row["url"]:
            lines.append(
                f"{prefix}, [{row['url']}]({row['url']})"
            )
        else:
            lines.append(prefix)
        lines.append(f"  Scope: {row['source_scope']}. {row['notes']}")

    (OUT_DIR / "source_inventory.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_or_fetch_json(cache_path: Path, url: str) -> Any:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    data = decode_json_url(url)
    cache_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return data


def main() -> int:
    ensure_out_dir()

    tree_cache_path = OUT_DIR / "official_portal_tree_2021.json"
    global_cache_path = OUT_DIR / "official_portal_global_2021.json"
    tree = load_or_fetch_json(tree_cache_path, TREE_URL_2021)
    global_config = load_or_fetch_json(global_cache_path, GLOBAL_URL_2021)

    fragment_rows = build_fragment_rows(tree)
    municipality_rows = build_municipality_rows(fragment_rows)
    split_rows = build_split_rows(municipality_rows)
    city_source_rows = build_city_source_rows(municipality_rows, split_rows)

    write_csv(
        OUT_DIR / "municipality_fragments_2021.csv",
        [
            "ags",
            "municipality_name",
            "fragment_name",
            "node_id",
            "level",
            "wahlkreis_id",
            "wahlkreis_name",
            "is_split_ags",
            "source",
            "source_url",
        ],
        fragment_rows,
    )
    write_csv(
        OUT_DIR / "municipalities.csv",
        ["ags", "municipality_name", "source", "fragment_count", "wahlkreis_count", "wahlkreis_ids"],
        municipality_rows,
    )
    write_csv(
        OUT_DIR / "split_municipalities.csv",
        ["ags", "municipality_name", "wahlkreis_count", "wahlkreis_ids", "reason"],
        split_rows,
    )
    write_csv(
        OUT_DIR / "city_sources.csv",
        [
            "ags",
            "municipality_name",
            "is_split_ags",
            "verification_status",
            "source_type",
            "source_scope",
            "url",
            "verified_from",
            "notes",
        ],
        city_source_rows,
    )
    write_setup_manifest(global_config, municipality_rows, split_rows, city_source_rows)
    write_source_inventory_md(split_rows, city_source_rows)

    print(f"Wrote metadata to {OUT_DIR}", file=sys.stderr)
    print(f"Municipalities: {len(municipality_rows)}", file=sys.stderr)
    print(f"Split municipalities: {len(split_rows)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
