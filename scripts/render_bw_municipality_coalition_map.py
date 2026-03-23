#!/usr/bin/env python3
"""Render a Baden-Wuerttemberg municipality map by coalition majority."""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
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

COALITION_A = "AfD+CDU"
COALITION_B = "CDU+GRÜNE"
TIE_LABEL = "Gleichstand"

COALITION_COLORS = {
    COALITION_A: "#0f2f6b",
    COALITION_B: "#1a5d2f",
    TIE_LABEL: "#8d877e",
}

CANVAS_WIDTH = 2000
CANVAS_HEIGHT = 2400
MAP_PADDING = 80
LEGEND_HEIGHT = 320
BACKGROUND = "#f7f5ef"
WATER = "#ebf1f8"
NO_RESULT_FILL = "#dad8d1"
NO_RESULT_OUTLINE = "#a7a39c"
OUTLINE = "#f3f0e9"
TITLE = "Baden-Württemberg: größere Zweitstimmen-Koalition je Gemeinde"
SUBTITLE = "Dunkelblau = AfD+CDU, Dunkelgrün = CDU+GRÜNE"


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
        help="Result source for municipality second votes. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--out-png",
        type=Path,
        default=None,
        help="PNG output path. Defaults to out-<election-key>-coalition-majority-map.png.",
    )
    parser.add_argument(
        "--report-csv",
        type=Path,
        default=None,
        help="CSV output path for the per-municipality coalition analysis.",
    )
    parser.add_argument(
        "--enriched-geojson",
        type=Path,
        default=None,
        help="Optional GeoJSON with municipality polygons enriched by coalition properties.",
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


def load_target_municipalities(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in read_csv_rows(path):
        ags = str(row.get("ags") or "").strip()
        if not ags:
            continue
        out[ags] = str(row.get("municipality_name") or "").strip()
    return out


def load_kommone_party_totals(path: Path) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in read_csv_rows(path):
        if str(row.get("vote_type") or "").strip() != "Zweitstimmen":
            continue
        ags = str(row.get("ags") or "").strip()
        if not ags:
            continue
        party = core.canonical_party_name(str(row.get("party") or "").strip(), "Zweitstimmen")
        votes = int(float(row.get("votes") or 0))
        entry = grouped.setdefault(
            ags,
            {
                "ags": ags,
                "municipality_name": str(row.get("municipality_name") or "").strip(),
                "party_votes": defaultdict(int),
            },
        )
        entry["party_votes"][party] += votes

    result: Dict[str, Dict[str, Any]] = {}
    for ags, entry in grouped.items():
        party_votes = dict(entry["party_votes"])
        result[ags] = {
            "ags": ags,
            "municipality_name": entry["municipality_name"],
            "party_votes": party_votes,
            "valid_votes_total": sum(party_votes.values()),
        }
    return result


def load_statla_party_totals(
    snapshots_path: Path,
    party_path: Path,
) -> Dict[str, Dict[str, Any]]:
    snapshot_by_row_key: Dict[str, Dict[str, Any]] = {}
    grouped_summary_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    grouped_precinct_by_ags: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in read_csv_rows(snapshots_path):
        row_key = str(row.get("row_key") or "").strip()
        ags = str(row.get("ags") or "").strip()
        if not row_key or not ags:
            continue
        snapshot_by_row_key[row_key] = {
            "ags": ags,
            "municipality_name": str(row.get("municipality_name") or "").strip(),
            "gebietsart": str(row.get("gebietsart") or "").strip(),
            "valid_votes_zweit": int(str(row.get("valid_votes_zweit") or "0") or 0),
        }

    for row in read_csv_rows(party_path):
        if str(row.get("vote_type") or "").strip() != "Zweitstimmen":
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
            grouped_summary_by_ags[str(snapshot["ags"])].append(normalized)
        elif snapshot["gebietsart"] in {"URNENWAHLBEZIRK", "BRIEFWAHLBEZIRK"}:
            grouped_precinct_by_ags[str(snapshot["ags"])].append(normalized)

    result: Dict[str, Dict[str, Any]] = {}
    for ags, rows in grouped_summary_by_ags.items():
        if not rows:
            continue
        party_totals: Dict[str, int] = defaultdict(int)
        municipality_name = str(rows[0]["municipality_name"])
        valid_votes_total = max(int(row["valid_votes_zweit"]) for row in rows)
        for row in rows:
            party_totals[str(row["party"])] += int(row["votes"])
        total_votes = valid_votes_total or sum(party_totals.values())
        result[ags] = {
            "ags": ags,
            "municipality_name": municipality_name,
            "party_votes": dict(party_totals),
            "valid_votes_total": total_votes,
        }

    for ags, rows in grouped_precinct_by_ags.items():
        if ags in result or not rows:
            continue
        party_totals = defaultdict(int)
        municipality_name = str(rows[0]["municipality_name"])
        for row in rows:
            party_totals[str(row["party"])] += int(row["votes"])
        result[ags] = {
            "ags": ags,
            "municipality_name": municipality_name,
            "party_votes": dict(party_totals),
            "valid_votes_total": sum(party_totals.values()),
            "is_synthesized": True,
        }
    return result


def percent(votes: int, total_votes: int) -> float:
    if total_votes <= 0:
        return 0.0
    return (votes / total_votes) * 100.0


def analyze_coalition(result: Dict[str, Any]) -> Dict[str, Any]:
    party_votes = result.get("party_votes") or {}
    valid_votes_total = int(result.get("valid_votes_total") or 0)
    cdu_votes = int(party_votes.get("CDU") or 0)
    afd_votes = int(party_votes.get("AfD") or 0)
    gruene_votes = int(party_votes.get("GRÜNE") or 0)

    coalition_a_votes = afd_votes + cdu_votes
    coalition_b_votes = cdu_votes + gruene_votes

    if coalition_a_votes > coalition_b_votes:
        winning_coalition = COALITION_A
    elif coalition_b_votes > coalition_a_votes:
        winning_coalition = COALITION_B
    else:
        winning_coalition = TIE_LABEL

    margin_votes = abs(coalition_a_votes - coalition_b_votes)

    return {
        "municipality_name": str(result.get("municipality_name") or "").strip(),
        "valid_votes_total": valid_votes_total,
        "cdu_votes": cdu_votes,
        "cdu_percent": percent(cdu_votes, valid_votes_total),
        "afd_votes": afd_votes,
        "afd_percent": percent(afd_votes, valid_votes_total),
        "gruene_votes": gruene_votes,
        "gruene_percent": percent(gruene_votes, valid_votes_total),
        "afd_cdu_votes": coalition_a_votes,
        "afd_cdu_percent": percent(coalition_a_votes, valid_votes_total),
        "cdu_gruene_votes": coalition_b_votes,
        "cdu_gruene_percent": percent(coalition_b_votes, valid_votes_total),
        "winning_coalition": winning_coalition,
        "coalition_margin_votes": margin_votes,
        "coalition_margin_percent": percent(margin_votes, valid_votes_total),
    }


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


def fill_for_analysis(analysis: Optional[Dict[str, Any]]) -> str:
    if not analysis:
        return NO_RESULT_FILL
    return COALITION_COLORS.get(str(analysis.get("winning_coalition") or ""), NO_RESULT_FILL)


def write_report_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ags",
        "municipality_name",
        "geometry_found",
        "result_found",
        "valid_votes_total",
        "afd_votes",
        "afd_percent",
        "cdu_votes",
        "cdu_percent",
        "gruene_votes",
        "gruene_percent",
        "afd_cdu_votes",
        "afd_cdu_percent",
        "cdu_gruene_votes",
        "cdu_gruene_percent",
        "winning_coalition",
        "coalition_margin_votes",
        "coalition_margin_percent",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
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
    out_png: Path,
    report_counts: Dict[str, int | str],
    coalition_counts: Counter[str],
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
        analysis = props.get("coalition_analysis")
        fill = fill_for_analysis(analysis)
        outline = OUTLINE if analysis else NO_RESULT_OUTLINE
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

    legend_rows = [
        (COALITION_A, coalition_counts.get(COALITION_A, 0)),
        (COALITION_B, coalition_counts.get(COALITION_B, 0)),
        (TIE_LABEL, coalition_counts.get(TIE_LABEL, 0)),
    ]

    y = legend_top + 70
    for label, count in legend_rows:
        draw.rounded_rectangle(
            (MAP_PADDING + 24, y, MAP_PADDING + 50, y + 26),
            radius=6,
            fill=COALITION_COLORS[label],
        )
        draw.text(
            (MAP_PADDING + 64, y + 2),
            f"{label}: {count}",
            fill="#222222",
            font=legend_font,
        )
        y += 42

    draw.rounded_rectangle(
        (MAP_PADDING + 24, y + 10, MAP_PADDING + 50, y + 36),
        radius=6,
        fill=NO_RESULT_FILL,
        outline=NO_RESULT_OUTLINE,
    )
    draw.text(
        (MAP_PADDING + 64, y + 12),
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

    default_png = core.ROOT / f"out-{args.election_key}-coalition-majority-map.png"
    default_report = core.ROOT / "data" / args.election_key / "reports" / "municipality_coalition_majority.csv"
    default_enriched = (
        core.ROOT
        / "data"
        / args.election_key
        / "reports"
        / "VG250_GEM_BW_coalition_majority_enriched.geojson"
    )

    out_png = args.out_png or default_png
    report_csv = args.report_csv or default_report
    enriched_geojson = args.enriched_geojson or default_enriched

    metadata = load_target_municipalities(metadata_path)
    if args.source == "statla":
        results_by_ags = load_statla_party_totals(statla_snapshots_path, statla_party_path)
    else:
        results_by_ags = load_kommone_party_totals(kommone_result_path)
    source_geojson = fetch_bkg_geojson(cached_geojson_path, force_download=args.force_download)

    geometry_by_ags: Dict[str, Dict[str, Any]] = {}
    for feature in source_geojson.get("features", []) or []:
        props = dict(feature.get("properties") or {})
        ags = str(props.get("ags") or "").strip()
        if ags in metadata:
            geometry_by_ags[ags] = feature

    report_rows: List[Dict[str, Any]] = []
    filtered_features: List[Dict[str, Any]] = []
    coalition_counts: Counter[str] = Counter()

    for ags, metadata_name in metadata.items():
        feature = geometry_by_ags.get(ags)
        result = results_by_ags.get(ags)
        analysis = analyze_coalition(result) if result else None

        report_row: Dict[str, Any] = {
            "ags": ags,
            "municipality_name": metadata_name,
            "geometry_found": bool(feature),
            "result_found": bool(result),
            "valid_votes_total": "",
            "afd_votes": "",
            "afd_percent": "",
            "cdu_votes": "",
            "cdu_percent": "",
            "gruene_votes": "",
            "gruene_percent": "",
            "afd_cdu_votes": "",
            "afd_cdu_percent": "",
            "cdu_gruene_votes": "",
            "cdu_gruene_percent": "",
            "winning_coalition": "",
            "coalition_margin_votes": "",
            "coalition_margin_percent": "",
        }
        if analysis:
            report_row.update(
                {
                    "valid_votes_total": analysis["valid_votes_total"],
                    "afd_votes": analysis["afd_votes"],
                    "afd_percent": f"{analysis['afd_percent']:.3f}",
                    "cdu_votes": analysis["cdu_votes"],
                    "cdu_percent": f"{analysis['cdu_percent']:.3f}",
                    "gruene_votes": analysis["gruene_votes"],
                    "gruene_percent": f"{analysis['gruene_percent']:.3f}",
                    "afd_cdu_votes": analysis["afd_cdu_votes"],
                    "afd_cdu_percent": f"{analysis['afd_cdu_percent']:.3f}",
                    "cdu_gruene_votes": analysis["cdu_gruene_votes"],
                    "cdu_gruene_percent": f"{analysis['cdu_gruene_percent']:.3f}",
                    "winning_coalition": analysis["winning_coalition"],
                    "coalition_margin_votes": analysis["coalition_margin_votes"],
                    "coalition_margin_percent": f"{analysis['coalition_margin_percent']:.3f}",
                }
            )
            coalition_counts[str(analysis["winning_coalition"])] += 1
        report_rows.append(report_row)

        if feature:
            props = dict(feature.get("properties") or {})
            enriched_props = dict(props)
            enriched_props["metadata_name"] = metadata_name
            enriched_props["result_found"] = bool(result)
            if result:
                enriched_props["party_votes"] = dict(result.get("party_votes") or {})
                enriched_props["valid_votes_total"] = int(result.get("valid_votes_total") or 0)
                enriched_props["source_municipality_name"] = str(result.get("municipality_name") or "")
            if analysis:
                enriched_props["coalition_analysis"] = analysis
            filtered_features.append(
                {
                    "type": "Feature",
                    "geometry": feature.get("geometry"),
                    "properties": enriched_props,
                }
            )

    write_report_csv(report_csv, report_rows)
    write_enriched_geojson(enriched_geojson, filtered_features)

    report_counts: Dict[str, int | str] = {
        "geometry_features": len(filtered_features),
        "results_found": sum(1 for row in report_rows if row["result_found"]),
        "missing_results": sum(1 for row in report_rows if not row["result_found"]),
        "source_label": args.source.upper(),
    }
    render_map(filtered_features, out_png, report_counts, coalition_counts)

    missing_geometry = [row["ags"] for row in report_rows if not row["geometry_found"]]
    print(f"PNG: {out_png}")
    print(f"Report CSV: {report_csv}")
    print(f"Enriched GeoJSON: {enriched_geojson}")
    print(f"Municipalities in metadata: {len(metadata)}")
    print(f"Municipalities with polygons: {report_counts['geometry_features']}")
    print(f"Municipalities with coalition analysis: {report_counts['results_found']}")
    print(
        f"{COALITION_A}: {coalition_counts.get(COALITION_A, 0)} | "
        f"{COALITION_B}: {coalition_counts.get(COALITION_B, 0)} | "
        f"{TIE_LABEL}: {coalition_counts.get(TIE_LABEL, 0)}"
    )
    if missing_geometry:
        preview = ", ".join(missing_geometry[:15])
        print(f"Missing polygons for {len(missing_geometry)} AGS: {preview}")


if __name__ == "__main__":
    main()
