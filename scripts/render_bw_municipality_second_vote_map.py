#!/usr/bin/env python3
"""Render a Baden-Württemberg municipality map by second-vote winner share."""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont

import poll_election_core as core


BKG_VG250_BW_GEOJSON_URL = (
    "https://sgx.geodatenzentrum.de/wfs_vg250"
    "?service=WFS"
    "&version=2.0.0"
    "&request=GetFeature"
    "&typenames=vg250_gem"
    "&cql_filter=sn_l='08'"
    "&count=2000"
    "&outputFormat=application/json"
)

PARTY_COLORS = {
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
    "PdF": "#ffb27f",
    "PIRATEN": "#f28c28",
    "BÜNDNIS DEUTSCHLAND": "#143d8d",
}

CANVAS_WIDTH = 2000
CANVAS_HEIGHT = 2400
MAP_PADDING = 80
LEGEND_HEIGHT = 340
BACKGROUND = "#f7f5ef"
WATER = "#ebf1f8"
NO_RESULT_FILL = "#dad8d1"
NO_RESULT_OUTLINE = "#a7a39c"
OUTLINE = "#f3f0e9"
TITLE = "Baden-Württemberg: stärkste Zweitstimmenpartei je Gemeinde"
SUBTITLE = "Farbton = Partei, Sättigung = Anteil der führenden Partei"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--election-key",
        default="2026-bw",
        help="Election storage key. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Refresh the cached municipality polygon GeoJSON from BKG.",
    )
    parser.add_argument(
        "--source",
        choices=("statla", "kommone"),
        default="statla",
        help="Result source for second-vote winners. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--out-png",
        type=Path,
        default=None,
        help="PNG output path. Defaults to out-<election-key>-second-vote-majority-map.png.",
    )
    parser.add_argument(
        "--join-report",
        type=Path,
        default=None,
        help="Optional CSV report with geometry/result join status.",
    )
    parser.add_argument(
        "--enriched-geojson",
        type=Path,
        default=None,
        help="Optional GeoJSON with municipality polygons enriched by result properties.",
    )
    return parser.parse_args()


def load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in (
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
    ):
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def fetch_bkg_geojson(path: Path, *, force_download: bool) -> Dict[str, Any]:
    if not path.exists() or force_download:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(BKG_VG250_BW_GEOJSON_URL, timeout=300) as response:  # nosec B310
                payload = response.read()
        except Exception:
            payload = subprocess.check_output(
                ["curl", "-L", "--silent", BKG_VG250_BW_GEOJSON_URL],
                text=False,
            )
        path.write_bytes(payload)
    return json.loads(path.read_text(encoding="utf-8"))


def geometry_feature_name(props: Dict[str, Any]) -> str:
    gen = str(props.get("gen") or "").strip()
    bez = str(props.get("bez") or "").strip()
    return f"{gen}, {bez}" if bez else gen


def load_kommone_result_winners(path: Path) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in read_csv_rows(path):
        if str(row.get("vote_type") or "") != "Zweitstimmen":
            continue
        ags = str(row.get("ags") or "").strip()
        if not ags:
            continue
        grouped[ags].append(row)

    winners: Dict[str, Dict[str, Any]] = {}
    for ags, rows in grouped.items():
        top_row = max(rows, key=lambda row: float(row.get("percent") or 0.0))
        winners[ags] = {
            "ags": ags,
            "municipality_name": str(top_row.get("municipality_name") or "").strip(),
            "winner_party": str(top_row.get("party") or "").strip(),
            "winner_percent": float(top_row.get("percent") or 0.0),
            "winner_votes": int(float(top_row.get("votes") or 0.0)),
            "party_count": len(rows),
        }
    return winners


def load_statla_result_winners(
    snapshots_path: Path,
    party_path: Path,
) -> Dict[str, Dict[str, Any]]:
    snapshot_by_row_key: Dict[str, Dict[str, Any]] = {}
    municipality_summary_ags: set[str] = set()
    precinct_rows_by_ags: Dict[str, List[str]] = defaultdict(list)
    for row in read_csv_rows(snapshots_path):
        row_key = str(row.get("row_key") or "").strip()
        ags = str(row.get("ags") or "").strip()
        if not row_key or not ags:
            continue
        gebietsart = str(row.get("gebietsart") or "").strip()
        snapshot_by_row_key[row_key] = {
            "ags": ags,
            "municipality_name": str(row.get("municipality_name") or "").strip(),
            "valid_votes_zweit": int(str(row.get("valid_votes_zweit") or "0") or 0),
            "gebietsart": gebietsart,
        }
        if str(row.get("is_municipality_summary") or "") == "True":
            municipality_summary_ags.add(ags)
        elif gebietsart in {"URNENWAHLBEZIRK", "BRIEFWAHLBEZIRK"}:
            precinct_rows_by_ags[ags].append(row_key)

    grouped_by_row_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    grouped_precinct_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in read_csv_rows(party_path):
        if str(row.get("vote_type") or "") != "Zweitstimmen":
            continue
        row_key = str(row.get("row_key") or "").strip()
        snapshot = snapshot_by_row_key.get(row_key)
        if not snapshot:
            continue
        normalized = {
            "party": core.canonical_party_name(str(row.get("party_name") or "").strip(), "Zweitstimmen"),
            "votes": int(str(row.get("votes") or "0") or 0),
            "ags": snapshot["ags"],
            "municipality_name": snapshot["municipality_name"],
            "valid_votes_zweit": snapshot["valid_votes_zweit"],
        }
        if snapshot["gebietsart"] == "GEMEINDE":
            grouped_by_row_key[row_key].append(normalized)
        elif snapshot["gebietsart"] in {"URNENWAHLBEZIRK", "BRIEFWAHLBEZIRK"}:
            grouped_precinct_by_ags[str(snapshot["ags"])].append(normalized)

    winners: Dict[str, Dict[str, Any]] = {}
    for rows in grouped_by_row_key.values():
        top_row = max(rows, key=lambda row: row["votes"])
        total_votes = int(top_row["valid_votes_zweit"]) or sum(row["votes"] for row in rows)
        winner_percent = ((top_row["votes"] / total_votes) * 100.0) if total_votes > 0 else 0.0
        winners[str(top_row["ags"])] = {
            "ags": str(top_row["ags"]),
            "municipality_name": str(top_row["municipality_name"]),
            "winner_party": str(top_row["party"]),
            "winner_percent": winner_percent,
            "winner_votes": int(top_row["votes"]),
            "party_count": len(rows),
            "valid_votes_total": total_votes,
        }

    for ags, rows in grouped_precinct_by_ags.items():
        if ags in municipality_summary_ags or not rows:
            continue
        party_totals: Dict[str, int] = defaultdict(int)
        municipality_name = str(rows[0]["municipality_name"])
        for row in rows:
            party_totals[str(row["party"])] += int(row["votes"])
        total_votes = sum(party_totals.values())
        if total_votes <= 0:
            continue
        winner_party, winner_votes = max(party_totals.items(), key=lambda item: item[1])
        winners[ags] = {
            "ags": ags,
            "municipality_name": municipality_name,
            "winner_party": winner_party,
            "winner_percent": (winner_votes / total_votes) * 100.0,
            "winner_votes": winner_votes,
            "party_count": len(party_totals),
            "valid_votes_total": total_votes,
            "is_synthesized": True,
        }
    return winners


def hex_to_rgb(value: str) -> Tuple[int, int, int]:
    value = value.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def rgb_to_hex(color: Tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*color)


def blend(color_a: Tuple[int, int, int], color_b: Tuple[int, int, int], ratio: float) -> Tuple[int, int, int]:
    clamped = max(0.0, min(1.0, ratio))
    return tuple(
        int(round((1.0 - clamped) * a + clamped * b))
        for a, b in zip(color_a, color_b)
    )


def fill_for_result(result: Optional[Dict[str, Any]]) -> str:
    if not result:
        return NO_RESULT_FILL
    base = hex_to_rgb(PARTY_COLORS.get(str(result["winner_party"]), "#7a7a7a"))
    bg = hex_to_rgb(BACKGROUND)
    share = float(result["winner_percent"])
    strength = (share - 20.0) / 25.0
    return rgb_to_hex(blend(bg, base, 0.25 + max(0.0, min(1.0, strength)) * 0.75))


def iter_rings(geometry: Dict[str, Any]) -> Iterable[List[List[float]]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geom_type == "Polygon":
        for ring in coords:
            yield ring
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                yield ring


def load_target_municipalities(path: Path) -> Dict[str, str]:
    rows = read_csv_rows(path)
    out: Dict[str, str] = {}
    for row in rows:
        ags = str(row.get("ags") or "").strip()
        if not ags:
            continue
        out[ags] = str(row.get("municipality_name") or "").strip()
    return out


def municipality_bbox(features: Iterable[Dict[str, Any]]) -> Tuple[float, float, float, float]:
    min_x = math.inf
    min_y = math.inf
    max_x = -math.inf
    max_y = -math.inf
    for feature in features:
        for ring in iter_rings(feature.get("geometry") or {}):
            for point in ring:
                if len(point) < 2:
                    continue
                x = float(point[0])
                y = float(point[1])
                min_x = min(min_x, x)
                min_y = min(min_y, y)
                max_x = max(max_x, x)
                max_y = max(max_y, y)
    if not math.isfinite(min_x):
        raise RuntimeError("No geometry coordinates found.")
    return min_x, min_y, max_x, max_y


def project_point(
    x: float,
    y: float,
    *,
    min_x: float,
    min_y: float,
    scale: float,
    pad_x: float,
    pad_y: float,
    usable_height: float,
) -> Tuple[float, float]:
    px = pad_x + (x - min_x) * scale
    py = pad_y + usable_height - (y - min_y) * scale
    return px, py


def write_join_report(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ags",
                "metadata_name",
                "geometry_name",
                "geometry_found",
                "result_found",
                "winner_party",
                "winner_percent",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_enriched_geojson(path: Path, features: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "type": "FeatureCollection",
        "features": features,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def render_map(
    features: List[Dict[str, Any]],
    winners: Dict[str, Dict[str, Any]],
    out_png: Path,
    report_counts: Dict[str, int],
    winner_counts: Counter[str],
) -> None:
    min_x, min_y, max_x, max_y = municipality_bbox(features)
    usable_width = CANVAS_WIDTH - 2 * MAP_PADDING
    usable_height = CANVAS_HEIGHT - LEGEND_HEIGHT - 2 * MAP_PADDING
    scale = min(
        usable_width / max(max_x - min_x, 1.0),
        usable_height / max(max_y - min_y, 1.0),
    )
    pad_x = MAP_PADDING + (usable_width - (max_x - min_x) * scale) / 2.0
    pad_y = MAP_PADDING + (usable_height - (max_y - min_y) * scale) / 2.0

    image = Image.new("RGB", (CANVAS_WIDTH, CANVAS_HEIGHT), BACKGROUND)
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, CANVAS_WIDTH, CANVAS_HEIGHT - LEGEND_HEIGHT), fill=WATER)

    for feature in features:
        props = feature.get("properties") or {}
        ags = str(props.get("ags") or "").strip()
        result = winners.get(ags)
        fill = fill_for_result(result)
        outline = OUTLINE if result else NO_RESULT_OUTLINE
        for ring in iter_rings(feature.get("geometry") or {}):
            projected = [
                project_point(
                    float(point[0]),
                    float(point[1]),
                    min_x=min_x,
                    min_y=min_y,
                    scale=scale,
                    pad_x=pad_x,
                    pad_y=pad_y,
                    usable_height=usable_height,
                )
                for point in ring
                if len(point) >= 2
            ]
            if len(projected) >= 3:
                draw.polygon(projected, fill=fill, outline=outline)

    title_font = load_font(46)
    subtitle_font = load_font(24)
    legend_font = load_font(22)
    small_font = load_font(18)

    draw.text((MAP_PADDING, 18), TITLE, fill="#1f1f1f", font=title_font)
    draw.text((MAP_PADDING, 72), SUBTITLE, fill="#4c4c4c", font=subtitle_font)

    legend_top = CANVAS_HEIGHT - LEGEND_HEIGHT + 24
    draw.rounded_rectangle(
        (MAP_PADDING, legend_top, CANVAS_WIDTH - MAP_PADDING, CANVAS_HEIGHT - 30),
        radius=24,
        fill="#fffdf9",
        outline="#d7d0c3",
        width=2,
    )

    summary = (
        f"Polygonquelle: BKG VG250 Gemeinden BW | "
        f"Quelle: {report_counts['source_label']} | "
        f"Polygone: {report_counts['geometry_features']} | "
        f"Ergebnisse mit Zweitstimmen: {report_counts['results_found']} | "
        f"Ohne Ergebnis: {report_counts['missing_results']}"
    )
    draw.text((MAP_PADDING + 24, legend_top + 20), summary, fill="#45413a", font=small_font)

    x = MAP_PADDING + 24
    y = legend_top + 64
    max_per_row = 5
    row = 0
    col = 0
    for party, count in winner_counts.most_common():
        box_x = x + col * 350
        box_y = y + row * 46
        draw.rounded_rectangle((box_x, box_y, box_x + 26, box_y + 26), radius=6, fill=PARTY_COLORS.get(party, "#7a7a7a"))
        draw.text((box_x + 38, box_y + 2), f"{party}: {count}", fill="#222222", font=legend_font)
        col += 1
        if col >= max_per_row:
            col = 0
            row += 1

    no_result_y = legend_top + 230
    draw.rounded_rectangle(
        (MAP_PADDING + 24, no_result_y, MAP_PADDING + 50, no_result_y + 26),
        radius=6,
        fill=NO_RESULT_FILL,
        outline=NO_RESULT_OUTLINE,
    )
    draw.text(
        (MAP_PADDING + 64, no_result_y + 2),
        "Noch kein kommunales Zweitstimmenergebnis",
        fill="#222222",
        font=legend_font,
    )

    out_png.parent.mkdir(parents=True, exist_ok=True)
    image.save(out_png)


def main() -> None:
    args = parse_args()
    core.set_active_election(election_key=args.election_key)

    metadata_path = core.META_DIR / "municipalities.csv"
    kommone_result_path = core.LATEST_DIR / "kommone_party_results.csv"
    statla_snapshots_path = core.LATEST_DIR / "statla_snapshots.csv"
    statla_party_path = core.LATEST_DIR / "statla_party_results.csv"
    cached_geojson_path = core.META_DIR / "VG250_GEM_BW.geojson"

    default_png = core.ROOT / f"out-{args.election_key}-second-vote-majority-map.png"
    default_report = core.META_DIR / "municipality_second_vote_map_join_report.csv"
    default_enriched = core.META_DIR / "VG250_GEM_BW_second_vote_enriched.geojson"

    out_png = args.out_png or default_png
    join_report = args.join_report or default_report
    enriched_geojson = args.enriched_geojson or default_enriched

    metadata = load_target_municipalities(metadata_path)
    if args.source == "statla":
        winners = load_statla_result_winners(statla_snapshots_path, statla_party_path)
    else:
        winners = load_kommone_result_winners(kommone_result_path)
    source_geojson = fetch_bkg_geojson(cached_geojson_path, force_download=args.force_download)

    filtered_features: List[Dict[str, Any]] = []
    join_rows: List[Dict[str, Any]] = []
    geometry_by_ags: Dict[str, Dict[str, Any]] = {}
    for feature in source_geojson.get("features", []) or []:
        props = dict(feature.get("properties") or {})
        ags = str(props.get("ags") or "").strip()
        if ags not in metadata:
            continue
        geometry_by_ags[ags] = feature

    winner_counts: Counter[str] = Counter()
    for ags, metadata_name in metadata.items():
        feature = geometry_by_ags.get(ags)
        result = winners.get(ags)
        geometry_name = ""
        if feature:
            props = dict(feature.get("properties") or {})
            geometry_name = geometry_feature_name(props)
            enriched_props = dict(props)
            enriched_props["metadata_name"] = metadata_name
            enriched_props["result_found"] = bool(result)
            if result:
                enriched_props.update(result)
                winner_counts[str(result["winner_party"])] += 1
            filtered_features.append(
                {
                    "type": "Feature",
                    "geometry": feature.get("geometry"),
                    "properties": enriched_props,
                }
            )
        join_rows.append(
            {
                "ags": ags,
                "metadata_name": metadata_name,
                "geometry_name": geometry_name,
                "geometry_found": bool(feature),
                "result_found": bool(result),
                "winner_party": "" if not result else result["winner_party"],
                "winner_percent": "" if not result else f"{result['winner_percent']:.3f}",
            }
        )

    write_join_report(join_report, join_rows)
    write_enriched_geojson(enriched_geojson, filtered_features)

    report_counts = {
        "geometry_features": len(filtered_features),
        "results_found": sum(1 for row in join_rows if row["result_found"]),
        "missing_results": sum(1 for row in join_rows if not row["result_found"]),
        "source_label": args.source.upper(),
    }
    render_map(filtered_features, winners, out_png, report_counts, winner_counts)

    missing_geometry = [row["ags"] for row in join_rows if not row["geometry_found"]]
    print(f"PNG: {out_png}")
    print(f"Join report: {join_report}")
    print(f"Enriched GeoJSON: {enriched_geojson}")
    print(f"Municipalities in metadata: {len(metadata)}")
    print(f"Municipalities with polygons: {report_counts['geometry_features']}")
    print(f"Municipalities with second-vote winners: {report_counts['results_found']}")
    if missing_geometry:
        preview = ", ".join(missing_geometry[:15])
        print(f"Missing polygons for {len(missing_geometry)} AGS: {preview}")


if __name__ == "__main__":
    main()
