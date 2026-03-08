#!/usr/bin/env python3
"""Validate Wahlkreis map geometry against Schaubild 8 (page 63)."""

from __future__ import annotations

import argparse
import json
import subprocess
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
from PIL import Image, ImageDraw

import poll_election_core as core

ROOT = Path(__file__).resolve().parents[1]
META_DIR = core.META_DIR
REF_DIR = META_DIR / "reference"
GEOJSON_PATH = core.WAHLKREIS_GEOJSON_PATH
DEFAULT_PDF_URL = (
    "https://www.statistik-bw.de/fileadmin/user_upload/Service/Veroeff/"
    "Statistische_Berichte/423525001.pdf"
)


def fetch_if_missing(url: str, path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=30) as response:  # nosec B310
        path.write_bytes(response.read())


def extract_pdf_page_png(pdf_path: Path, page: int, out_png: Path) -> None:
    out_png.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "gs",
        "-q",
        "-dNOPAUSE",
        "-dBATCH",
        "-sDEVICE=pngalpha",
        "-r180",
        f"-dFirstPage={page}",
        f"-dLastPage={page}",
        f"-sOutputFile={str(out_png)}",
        str(pdf_path),
    ]
    subprocess.run(cmd, check=True)


def _gray_from_rgb(arr_rgb: np.ndarray) -> np.ndarray:
    return 0.299 * arr_rgb[:, :, 0] + 0.587 * arr_rgb[:, :, 1] + 0.114 * arr_rgb[:, :, 2]


def detect_frame_bounds(page_rgba: np.ndarray) -> Tuple[int, int, int, int]:
    alpha = page_rgba[:, :, 3] > 0
    gray = _gray_from_rgb(page_rgba[:, :, :3])
    dark = alpha & (gray < 120)

    row_counts = dark.sum(axis=1)
    col_counts = dark.sum(axis=0)

    row_threshold = int(page_rgba.shape[1] * 0.6)
    col_threshold = int(page_rgba.shape[0] * 0.6)

    rows = np.where(row_counts >= row_threshold)[0]
    cols = np.where(col_counts >= col_threshold)[0]

    if len(rows) >= 2 and len(cols) >= 2:
        top = int(rows[0])
        bottom = int(rows[-1])
        left = int(cols[0])
        right = int(cols[-1])
        return left, top, right, bottom

    ys, xs = np.where(alpha)
    if len(xs) == 0:
        raise RuntimeError("Reference page appears empty.")
    return int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())


def largest_connected_component(mask: np.ndarray) -> np.ndarray:
    h, w = mask.shape
    visited = np.zeros((h, w), dtype=np.uint8)
    best_pixels: List[Tuple[int, int]] = []

    for y in range(h):
        for x in range(w):
            if not mask[y, x] or visited[y, x]:
                continue
            queue: deque[Tuple[int, int]] = deque([(y, x)])
            visited[y, x] = 1
            pixels: List[Tuple[int, int]] = []

            while queue:
                cy, cx = queue.popleft()
                pixels.append((cy, cx))
                if cy > 0 and mask[cy - 1, cx] and not visited[cy - 1, cx]:
                    visited[cy - 1, cx] = 1
                    queue.append((cy - 1, cx))
                if cy + 1 < h and mask[cy + 1, cx] and not visited[cy + 1, cx]:
                    visited[cy + 1, cx] = 1
                    queue.append((cy + 1, cx))
                if cx > 0 and mask[cy, cx - 1] and not visited[cy, cx - 1]:
                    visited[cy, cx - 1] = 1
                    queue.append((cy, cx - 1))
                if cx + 1 < w and mask[cy, cx + 1] and not visited[cy, cx + 1]:
                    visited[cy, cx + 1] = 1
                    queue.append((cy, cx + 1))

            if len(pixels) > len(best_pixels):
                best_pixels = pixels

    out = np.zeros((h, w), dtype=np.uint8)
    for py, px in best_pixels:
        out[py, px] = 1
    return out


def iter_exterior_rings(geometry: Dict[str, Any]) -> Iterable[List[List[float]]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geom_type == "Polygon":
        if coords:
            yield coords[0]
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            if polygon:
                yield polygon[0]


def maybe_fix_mojibake(text: str) -> str:
    if "Ã" not in text and "Â" not in text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except Exception:  # pylint: disable=broad-except
        return text


def project_point(
    lon: float,
    lat: float,
    min_lon: float,
    min_lat: float,
    scale: float,
    pad: float,
    height: float,
) -> Tuple[float, float]:
    x = pad + (lon - min_lon) * scale
    y = height - pad - (lat - min_lat) * scale
    return x, y


def load_features() -> List[Dict[str, Any]]:
    payload = json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))
    return payload.get("features", []) or []


def render_generated_maps(
    features: List[Dict[str, Any]],
    width: int,
    height: int,
) -> Tuple[Image.Image, Image.Image]:
    all_points: List[Tuple[float, float]] = []
    for feature in features:
        for ring in iter_exterior_rings(feature.get("geometry") or {}):
            for point in ring:
                if len(point) >= 2:
                    all_points.append((float(point[0]), float(point[1])))
    if not all_points:
        raise RuntimeError("No coordinates found in Wahlkreis GeoJSON.")

    min_lon = min(p[0] for p in all_points)
    max_lon = max(p[0] for p in all_points)
    min_lat = min(p[1] for p in all_points)
    max_lat = max(p[1] for p in all_points)

    pad = 40.0
    scale_x = (width - 2 * pad) / max(max_lon - min_lon, 1e-9)
    scale_y = (height - 2 * pad) / max(max_lat - min_lat, 1e-9)
    scale = min(scale_x, scale_y)

    base = Image.new("RGB", (width, height), (255, 255, 255))
    labeled = Image.new("RGB", (width, height), (255, 255, 255))
    draw_base = ImageDraw.Draw(base)
    draw_labeled = ImageDraw.Draw(labeled)

    for feature in features:
        props = feature.get("properties") or {}
        wk = str(props.get("Nummer") or "").strip()
        wk_name = maybe_fix_mojibake(str(props.get("WK Name") or "").strip())
        if not wk:
            continue

        centroid_acc_x = 0.0
        centroid_acc_y = 0.0
        centroid_count = 0

        for ring in iter_exterior_rings(feature.get("geometry") or {}):
            if len(ring) < 3:
                continue
            projected = [
                project_point(
                    float(pt[0]),
                    float(pt[1]),
                    min_lon=min_lon,
                    min_lat=min_lat,
                    scale=scale,
                    pad=pad,
                    height=float(height),
                )
                for pt in ring
            ]
            draw_base.polygon(projected, fill=(214, 215, 217), outline=(160, 160, 160))
            draw_labeled.polygon(projected, fill=(214, 215, 217), outline=(160, 160, 160))
            for x, y in projected:
                centroid_acc_x += x
                centroid_acc_y += y
                centroid_count += 1

        if centroid_count > 0:
            cx = centroid_acc_x / centroid_count
            cy = centroid_acc_y / centroid_count
            short_name = wk_name.replace("Stuttgart ", "S ")
            label = f"WK {wk} {short_name}"
            draw_labeled.text((cx - 22, cy - 5), label, fill=(95, 95, 95))

    return base, labeled


def mask_bbox(mask: np.ndarray) -> Tuple[int, int, int, int]:
    ys, xs = np.where(mask > 0)
    if len(xs) == 0:
        raise RuntimeError("Empty mask.")
    return int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())


def normalize_mask(mask: np.ndarray, out_size: Tuple[int, int]) -> np.ndarray:
    left, top, right, bottom = mask_bbox(mask)
    cropped = (mask[top : bottom + 1, left : right + 1] * 255).astype(np.uint8)
    resized = Image.fromarray(cropped, mode="L").resize(out_size, resample=Image.NEAREST)
    return (np.array(resized) > 0).astype(np.uint8)


def iou(mask_a: np.ndarray, mask_b: np.ndarray) -> float:
    inter = np.logical_and(mask_a > 0, mask_b > 0).sum()
    union = np.logical_or(mask_a > 0, mask_b > 0).sum()
    if union == 0:
        return 0.0
    return float(inter / union)


def compose_comparison_image(
    reference_interior: Image.Image,
    generated_labeled: Image.Image,
    reference_norm: np.ndarray,
    generated_norm: np.ndarray,
    out_path: Path,
    iou_score: float,
) -> None:
    panel_w, panel_h = 520, 720

    ref_panel = reference_interior.resize((panel_w, panel_h), resample=Image.BILINEAR).convert("RGB")
    gen_panel = generated_labeled.resize((panel_w, panel_h), resample=Image.BILINEAR).convert("RGB")

    diff_rgb = np.zeros((reference_norm.shape[0], reference_norm.shape[1], 3), dtype=np.uint8)
    both = (reference_norm > 0) & (generated_norm > 0)
    only_ref = (reference_norm > 0) & (generated_norm == 0)
    only_gen = (reference_norm == 0) & (generated_norm > 0)
    diff_rgb[both] = (120, 120, 120)
    diff_rgb[only_ref] = (220, 38, 38)
    diff_rgb[only_gen] = (37, 99, 235)
    diff_panel = Image.fromarray(diff_rgb, mode="RGB").resize((panel_w, panel_h), resample=Image.NEAREST)

    canvas_w = panel_w * 3 + 80
    canvas_h = panel_h + 80
    canvas = Image.new("RGB", (canvas_w, canvas_h), (255, 255, 255))
    draw = ImageDraw.Draw(canvas)

    x0 = 20
    y0 = 40
    canvas.paste(ref_panel, (x0, y0))
    canvas.paste(gen_panel, (x0 + panel_w + 20, y0))
    canvas.paste(diff_panel, (x0 + 2 * (panel_w + 20), y0))

    draw.text((x0, 12), "Schaubild 8 (page 63)", fill=(17, 24, 39))
    draw.text((x0 + panel_w + 20, 12), f"Generated from {GEOJSON_PATH.name}", fill=(17, 24, 39))
    draw.text((x0 + 2 * (panel_w + 20), 12), f"Normalized mask diff (IoU: {iou_score:.3f})", fill=(17, 24, 39))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out_path)


def run(pdf_url: str, threshold: float, page: int) -> Dict[str, Any]:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"Missing geometry file: {GEOJSON_PATH}")

    pdf_path = REF_DIR / "423525001.pdf"
    page_png = REF_DIR / "schaubild8-page63.png"
    generated_png = REF_DIR / "schaubild8-generated-map.png"
    comparison_png = REF_DIR / "schaubild8-map-comparison.png"
    report_json = REF_DIR / "schaubild8-map-test.json"
    report_md = REF_DIR / "schaubild8-map-test.md"

    fetch_if_missing(pdf_url, pdf_path)
    extract_pdf_page_png(pdf_path, page=page, out_png=page_png)

    page_img = Image.open(page_png).convert("RGBA")
    page_rgba = np.array(page_img)
    left, top, right, bottom = detect_frame_bounds(page_rgba)

    interior = page_img.crop((left + 2, top + 2, right - 1, bottom - 1)).convert("RGB")
    interior_arr = np.array(interior)
    interior_gray = _gray_from_rgb(interior_arr)
    ref_mask_raw = interior_gray < 245
    ref_mask = largest_connected_component(ref_mask_raw)

    features = load_features()
    generated_base, generated_labeled = render_generated_maps(
        features,
        width=interior.width,
        height=interior.height,
    )
    generated_labeled.save(generated_png)

    gen_arr = np.array(generated_base)
    gen_gray = _gray_from_rgb(gen_arr)
    gen_mask_raw = gen_gray < 245
    gen_mask = largest_connected_component(gen_mask_raw)

    normalized_size = (600, 900)
    ref_norm = normalize_mask(ref_mask, normalized_size)
    gen_norm = normalize_mask(gen_mask, normalized_size)
    iou_score = iou(ref_norm, gen_norm)

    compose_comparison_image(
        reference_interior=interior,
        generated_labeled=generated_labeled,
        reference_norm=ref_norm,
        generated_norm=gen_norm,
        out_path=comparison_png,
        iou_score=iou_score,
    )

    wk_nums = sorted(
        int((feature.get("properties") or {}).get("Nummer"))
        for feature in features
        if str((feature.get("properties") or {}).get("Nummer", "")).strip().isdigit()
    )
    expected_range = list(range(1, 71))
    numbering_ok = wk_nums == expected_range

    report: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pdf_url": pdf_url,
        "page": page,
        "reference_pdf": str(pdf_path.relative_to(ROOT)),
        "reference_page_image": str(page_png.relative_to(ROOT)),
        "generated_map_image": str(generated_png.relative_to(ROOT)),
        "comparison_image": str(comparison_png.relative_to(ROOT)),
        "iou": round(iou_score, 6),
        "threshold": threshold,
        "passed": bool(iou_score >= threshold and numbering_ok),
        "wahlkreis_count": len(features),
        "wahlkreis_numbering_ok": numbering_ok,
    }

    report_json.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    md_lines = [
        "# Schaubild 8 Map Test",
        "",
        f"- Generated at (UTC): **{report['generated_at_utc']}**",
        f"- IoU (normalized outline overlap): **{report['iou']:.3f}**",
        f"- Threshold: **{threshold:.3f}**",
        f"- Wahlkreis count: **{report['wahlkreis_count']}**",
        f"- Numbering 1..70 complete: **{report['wahlkreis_numbering_ok']}**",
        f"- Passed: **{report['passed']}**",
        "",
        f"![Schaubild 8 comparison]({comparison_png.relative_to(ROOT)})",
        "",
    ]
    report_md.write_text("\n".join(md_lines), encoding="utf-8")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Wahlkreis map against Schaubild 8 (page 63).")
    parser.add_argument(
        "--election-key",
        default=core.DEFAULT_ELECTION_KEY,
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    parser.add_argument("--pdf-url", default=DEFAULT_PDF_URL, help="PDF URL for the official report.")
    parser.add_argument("--threshold", type=float, default=0.90, help="Minimum IoU to pass.")
    parser.add_argument("--page", type=int, default=63, help="PDF page number containing Schaubild 8.")
    args = parser.parse_args()

    core.set_active_election(election_key=args.election_key)
    core.load_config()
    global META_DIR, REF_DIR, GEOJSON_PATH
    META_DIR = core.META_DIR
    REF_DIR = META_DIR / "reference"
    GEOJSON_PATH = core.WAHLKREIS_GEOJSON_PATH

    report = run(pdf_url=args.pdf_url, threshold=args.threshold, page=args.page)
    print(f"IoU: {report['iou']:.3f}")
    print(f"Wahlkreis count: {report['wahlkreis_count']}")
    print(f"Numbering OK: {report['wahlkreis_numbering_ok']}")
    print(f"PASS: {report['passed']}")

    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
