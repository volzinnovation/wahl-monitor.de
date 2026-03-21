#!/usr/bin/env python3
"""Build zero-result latest exports for Rheinland-Pfalz 2026 from official metadata."""

from __future__ import annotations

import csv
import json
import re
import struct
import subprocess
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
ELECTION_KEY = "2026-rlp"
DATA_DIR = ROOT / "data" / ELECTION_KEY
META_DIR = DATA_DIR / "metadata"
LATEST_DIR = DATA_DIR / "latest"
RAW_STATLA_DIR = DATA_DIR / "raw" / "statla"
TMP_DIR = Path("/tmp")

WORKBOOK_CACHE_PATH = TMP_DIR / "LW_2021_GESAMT.xlsx"
GEODATA_CACHE_PATH = TMP_DIR / "Geodaten_LW2026_RP.zip"

RAW_HEADERS = [
    "Wahlkreisnummer",
    "Gemeindename",
    "Gebietsname",
    "Gebietsnummer",
    "Bezirksnummer",
    "Gebietsart",
    "AGS",
]
SNAPSHOT_HEADERS = [
    "row_key",
    "ags",
    "municipality_name",
    "gebietsart",
    "gebietsnummer",
    "reported_precincts",
    "total_precincts",
    "voters_total",
    "valid_votes_erst",
    "valid_votes_zweit",
    "voters_total_2021",
    "valid_votes_erst_2021",
    "valid_votes_zweit_2021",
    "delta_voters_total_vs_2021",
    "delta_valid_votes_erst_vs_2021",
    "delta_valid_votes_zweit_vs_2021",
    "payload_hash",
    "is_municipality_summary",
]
PARTY_HEADERS = [
    "row_key",
    "vote_type",
    "party_key",
    "party_name",
    "votes",
    "votes_2021",
    "share_percent_2021",
    "delta_votes_vs_2021",
    "delta_share_percent_vs_2021",
]
MAPPING_HEADERS = ["Wahlkreisnummer", "Wahlkreisname", "Gemeindekennziffer", "Gemeindename"]

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
CELL_REF_RE = re.compile(r"([A-Z]+)")


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]], *, delimiter: str = ",") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def ensure_download(url: str, target: Path) -> Path:
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["curl", "-L", "--fail", "--silent", url, "-o", str(target)],
        check=True,
    )
    return target


def manifest() -> Dict[str, Any]:
    return json.loads((META_DIR / "setup_manifest.json").read_text(encoding="utf-8"))


def column_index_from_ref(ref: str) -> int:
    match = CELL_REF_RE.match(ref)
    if not match:
        return 0
    value = 0
    for char in match.group(1):
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value - 1


def load_xlsx_rows(path: Path) -> List[List[str]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for item in root.findall(f"{NS}si"):
                shared_strings.append("".join(node.text or "" for node in item.iter(f"{NS}t")))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        relationships = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in relationships}
        first_sheet = workbook.find(f"{NS}sheets/{NS}sheet")
        if first_sheet is None:
            raise RuntimeError("Workbook does not contain any sheet")
        rel_id = first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        worksheet_path = "xl/" + rel_map[rel_id]
        worksheet = ET.fromstring(zf.read(worksheet_path))

        rows: List[List[str]] = []
        for row_node in worksheet.findall(f".//{NS}sheetData/{NS}row"):
            values: List[str] = []
            for cell in row_node.findall(f"{NS}c"):
                col_index = column_index_from_ref(cell.attrib.get("r", "A1"))
                while len(values) <= col_index:
                    values.append("")
                raw_value = cell.find(f"{NS}v")
                if raw_value is None:
                    values[col_index] = ""
                    continue
                if cell.attrib.get("t") == "s":
                    values[col_index] = shared_strings[int(raw_value.text or "0")]
                else:
                    values[col_index] = raw_value.text or ""
            rows.append(values)
        return rows


def workbook_party_names(header: Sequence[str]) -> Tuple[List[str], List[str]]:
    invalid_indices = [index for index, value in enumerate(header) if value.strip() == "ungültige"]
    valid_indices = [index for index, value in enumerate(header) if value.strip() == "Gültige"]
    if len(invalid_indices) < 2 or len(valid_indices) < 2:
        raise RuntimeError("Unexpected 2021 workbook header shape for RLP party columns")

    first_parties = [header[index].strip() for index in range(valid_indices[0] + 2, invalid_indices[1], 2) if header[index].strip()]
    second_parties = [header[index].strip() for index in range(valid_indices[1] + 2, len(header), 2) if header[index].strip()]
    if not first_parties or not second_parties:
        raise RuntimeError("Could not derive first/second vote party lists from workbook header")
    return first_parties, second_parties


def workbook_party_columns(header: Sequence[str]) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]], int, int, int]:
    invalid_indices = [index for index, value in enumerate(header) if value.strip() == "ungültige"]
    valid_indices = [index for index, value in enumerate(header) if value.strip() == "Gültige"]
    if len(invalid_indices) < 2 or len(valid_indices) < 2:
        raise RuntimeError("Unexpected 2021 workbook header shape for RLP party columns")
    try:
        voters_index = header.index("Wähler")
    except ValueError as exc:
        raise RuntimeError("Unexpected 2021 workbook header shape for RLP turnout column") from exc
    first_party_columns = [
        (header[index].strip(), index)
        for index in range(valid_indices[0] + 2, invalid_indices[1], 2)
        if header[index].strip()
    ]
    second_party_columns = [
        (header[index].strip(), index)
        for index in range(valid_indices[1] + 2, len(header), 2)
        if header[index].strip()
    ]
    if not first_party_columns or not second_party_columns:
        raise RuntimeError("Could not derive first/second vote party columns from workbook header")
    return first_party_columns, second_party_columns, voters_index, valid_indices[0], valid_indices[1]


def wk_from_id(identifier: str) -> str:
    digits = "".join(ch for ch in str(identifier or "") if ch.isdigit())
    return str(int(digits[:3])) if len(digits) >= 3 and digits[:3] != "000" else ""


def ags_from_id(identifier: str) -> str:
    digits = "".join(ch for ch in str(identifier or "") if ch.isdigit())
    if len(digits) < 11:
        return ""
    ags = digits[3:11]
    return "" if ags == "00000000" else ags


def parse_workbook_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    return int(float(text))


def share_percent(votes: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((votes / total) * 100.0, 6)


def workbook_history_row(
    values: Sequence[str],
    first_party_columns: Sequence[Tuple[str, int]],
    second_party_columns: Sequence[Tuple[str, int]],
    voters_index: int,
    valid_votes_erst_index: int,
    valid_votes_zweit_index: int,
) -> Dict[str, Any]:
    return {
        "voters_total": parse_workbook_int(values[voters_index] if len(values) > voters_index else ""),
        "valid_votes_erst": parse_workbook_int(values[valid_votes_erst_index] if len(values) > valid_votes_erst_index else ""),
        "valid_votes_zweit": parse_workbook_int(values[valid_votes_zweit_index] if len(values) > valid_votes_zweit_index else ""),
        "party_votes": {
            "Erststimmen": {
                party_name: parse_workbook_int(values[index] if len(values) > index else "")
                for party_name, index in first_party_columns
            },
            "Zweitstimmen": {
                party_name: parse_workbook_int(values[index] if len(values) > index else "")
                for party_name, index in second_party_columns
            },
        },
    }


def combine_history_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not rows:
        return None
    combined = {
        "voters_total": 0,
        "valid_votes_erst": 0,
        "valid_votes_zweit": 0,
        "party_votes": {
            "Erststimmen": defaultdict(int),
            "Zweitstimmen": defaultdict(int),
        },
    }
    for row in rows:
        combined["voters_total"] += int(row.get("voters_total") or 0)
        combined["valid_votes_erst"] += int(row.get("valid_votes_erst") or 0)
        combined["valid_votes_zweit"] += int(row.get("valid_votes_zweit") or 0)
        for vote_type in ("Erststimmen", "Zweitstimmen"):
            for party_name, votes in (row.get("party_votes", {}).get(vote_type, {}) or {}).items():
                combined["party_votes"][vote_type][party_name] += int(votes or 0)
    return {
        "voters_total": combined["voters_total"],
        "valid_votes_erst": combined["valid_votes_erst"],
        "valid_votes_zweit": combined["valid_votes_zweit"],
        "party_votes": {
            "Erststimmen": dict(combined["party_votes"]["Erststimmen"]),
            "Zweitstimmen": dict(combined["party_votes"]["Zweitstimmen"]),
        },
    }


def clean_municipality_name(label: str) -> str:
    text = str(label or "").strip()
    for suffix in [
        ", Verbandsfreie Gemeinde",
        ", Verb.fr.Gem.",
        ", Kreisfreie Stadt",
        ", Verbandsgemeinde",
        ", Verb.gem.",
        ", Ortsgemeinde",
    ]:
        if text.endswith(suffix):
            return text[: -len(suffix)].strip()
    return text


def parse_workbook_context(
    workbook_path: Path,
    municipality_name_by_ags: Dict[str, str],
) -> Tuple[List[str], List[str], List[Dict[str, str]], Dict[str, str], Dict[str, List[str]], Dict[str, Any]]:
    rows = load_xlsx_rows(workbook_path)
    if not rows:
        raise RuntimeError("Official 2021 workbook did not contain any rows")
    header = rows[0]
    first_parties, second_parties = workbook_party_names(header)
    first_party_columns, second_party_columns, voters_index, valid_votes_erst_index, valid_votes_zweit_index = workbook_party_columns(header)

    booths: List[Dict[str, str]] = []
    summary_name_by_ags: Dict[str, str] = {}
    observed_wks_by_ags: Dict[str, set[str]] = defaultdict(set)
    history: Dict[str, Any] = {
        "land": None,
        "wahlkreis": {},
        "municipality": {},
        "wahlkreis_teil": {},
        "booth": {},
    }
    for raw_values in rows[1:]:
        values = list(raw_values) + [""] * max(0, len(header) - len(raw_values))
        identifier = values[0].strip()
        stimmbezirk = values[1].strip()
        label = values[2].strip()
        guw = values[3].strip().upper()
        if not identifier:
            continue
        ags = ags_from_id(identifier)
        wk = wk_from_id(identifier)
        history_row = workbook_history_row(
            values,
            first_party_columns,
            second_party_columns,
            voters_index,
            valid_votes_erst_index,
            valid_votes_zweit_index,
        )
        is_aggregate_row = identifier.endswith("0000")
        if not ags:
            if stimmbezirk == "00000" and guw == "G":
                if wk:
                    history["wahlkreis"][wk] = history_row
                else:
                    history["land"] = history_row
            continue
        if stimmbezirk == "00000":
            if guw == "G":
                cleaned_name = clean_municipality_name(label)
                if cleaned_name and len(cleaned_name) > len(summary_name_by_ags.get(ags, "")):
                    summary_name_by_ags[ags] = cleaned_name
                if wk:
                    if is_aggregate_row:
                        history["wahlkreis_teil"][(ags, wk)] = history_row
                elif is_aggregate_row:
                    history["municipality"][ags] = history_row
            continue
        if not stimmbezirk or guw not in {"U", "W"} or not wk:
            continue
        observed_wks_by_ags[ags].add(wk)
        gebietsart = "URNENWAHLBEZIRK" if guw == "U" else "BRIEFWAHLBEZIRK"
        booths.append(
            {
                "ags": ags,
                "wahlkreisnummer": wk,
                "municipality_name": municipality_name_by_ags.get(ags, "") or summary_name_by_ags.get(ags, ""),
                "booth_code": stimmbezirk,
                "label": label or stimmbezirk,
                "gebietsart": gebietsart,
            }
        )
        history["booth"][(ags, wk, stimmbezirk, gebietsart)] = history_row
    return (
        first_parties,
        second_parties,
        booths,
        summary_name_by_ags,
        {ags: sorted(wks, key=int) for ags, wks in observed_wks_by_ags.items()},
        history,
    )


def parse_dbf_records(data: bytes) -> List[Dict[str, str]]:
    num_records = struct.unpack("<I", data[4:8])[0]
    header_length = struct.unpack("<H", data[8:10])[0]
    record_length = struct.unpack("<H", data[10:12])[0]

    fields: List[Tuple[str, int]] = []
    cursor = 32
    while cursor < header_length and data[cursor] != 0x0D:
        name = data[cursor : cursor + 11].split(b"\x00", 1)[0].decode("ascii", errors="ignore")
        length = data[cursor + 16]
        fields.append((name, length))
        cursor += 32

    records: List[Dict[str, str]] = []
    offset = header_length
    for _ in range(num_records):
        record = data[offset : offset + record_length]
        offset += record_length
        if not record or record[0] == 0x2A:
            continue
        row: Dict[str, str] = {}
        position = 1
        for name, length in fields:
            row[name] = record[position : position + length].decode("cp1252", errors="replace").strip()
            position += length
        records.append(row)
    return records


def parse_shp_geometries(data: bytes) -> List[Dict[str, Any]]:
    geometries: List[Dict[str, Any]] = []
    offset = 100
    while offset + 8 <= len(data):
        _record_number, record_length_words = struct.unpack(">2i", data[offset : offset + 8])
        offset += 8
        content_length = record_length_words * 2
        content = data[offset : offset + content_length]
        offset += content_length
        if len(content) < 4:
            break
        shape_type = struct.unpack("<i", content[:4])[0]
        if shape_type == 0:
            geometries.append({"type": "Polygon", "coordinates": []})
            continue
        if shape_type not in {5, 15}:
            raise RuntimeError(f"Unsupported shapefile geometry type {shape_type}")

        num_parts = struct.unpack("<i", content[36:40])[0]
        num_points = struct.unpack("<i", content[40:44])[0]
        parts = [struct.unpack("<i", content[44 + index * 4 : 48 + index * 4])[0] for index in range(num_parts)]
        points_offset = 44 + num_parts * 4
        points: List[List[float]] = []
        for index in range(num_points):
            start = points_offset + index * 16
            x, y = struct.unpack("<2d", content[start : start + 16])
            points.append([x, y])

        rings: List[List[List[float]]] = []
        for index, start_idx in enumerate(parts):
            end_idx = parts[index + 1] if index + 1 < len(parts) else len(points)
            ring = points[start_idx:end_idx]
            if len(ring) >= 3:
                rings.append(ring)
        if not rings:
            geometries.append({"type": "Polygon", "coordinates": []})
        elif len(rings) == 1:
            geometries.append({"type": "Polygon", "coordinates": [rings[0]]})
        else:
            geometries.append({"type": "MultiPolygon", "coordinates": [[[ring]] if False else [ring] for ring in rings]})
    return geometries


def build_wahlkreis_geojson(geodata_zip_path: Path, wk_name_by_id: Dict[str, str]) -> Dict[str, Any]:
    with zipfile.ZipFile(geodata_zip_path) as archive:
        dbf_rows = parse_dbf_records(archive.read("LW2026_RP_WK_2_WK.dbf"))
        geometries = parse_shp_geometries(archive.read("LW2026_RP_WK_2_WK.shp"))
    if len(dbf_rows) != len(geometries):
        raise RuntimeError("RLP Wahlkreis shape/dbf record count mismatch")

    features: List[Dict[str, Any]] = []
    for dbf_row, geometry in zip(dbf_rows, geometries):
        wk = wk_from_id(dbf_row.get("26_IDEN", ""))
        if not wk:
            continue
        name = dbf_row.get("26_NAM", "").strip() or wk_name_by_id.get(wk, f"Wahlkreis {wk}")
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "Nummer": wk,
                    "WK Name": name,
                },
                "geometry": geometry,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def mapping_rows_from_fragments(fragment_rows: Sequence[Dict[str, str]]) -> Tuple[List[Dict[str, str]], Dict[str, str], Dict[str, List[str]]]:
    seen: set[Tuple[str, str]] = set()
    rows: List[Dict[str, str]] = []
    wk_name_by_id: Dict[str, str] = {}
    wks_by_ags: Dict[str, List[str]] = defaultdict(list)
    for row in fragment_rows:
        wk = str(row.get("wahlkreis_id") or "").strip()
        ags = str(row.get("ags") or "").strip()
        if not wk or not ags:
            continue
        wk_name_by_id.setdefault(wk, str(row.get("wahlkreis_name") or "").strip() or f"Wahlkreis {wk}")
        if wk not in wks_by_ags[ags]:
            wks_by_ags[ags].append(wk)
        key = (wk, ags)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "Wahlkreisnummer": wk,
                "Wahlkreisname": wk_name_by_id[wk],
                "Gemeindekennziffer": ags,
                "Gemeindename": str(row.get("municipality_name") or "").strip(),
            }
        )
    rows.sort(key=lambda item: (int(item["Wahlkreisnummer"]), item["Gemeindekennziffer"]))
    for ags in wks_by_ags:
        wks_by_ags[ags].sort(key=int)
    return rows, wk_name_by_id, wks_by_ags


def merge_mapping_rows(
    base_rows: Sequence[Dict[str, str]],
    wk_name_by_id: Dict[str, str],
    municipality_name_by_ags: Dict[str, str],
    observed_wks_by_ags: Dict[str, List[str]],
) -> Tuple[List[Dict[str, str]], Dict[str, List[str]]]:
    rows = [dict(row) for row in base_rows]
    seen = {(row["Wahlkreisnummer"], row["Gemeindekennziffer"]) for row in rows}
    merged_wks_by_ags: Dict[str, set[str]] = defaultdict(set)
    for row in rows:
        merged_wks_by_ags[row["Gemeindekennziffer"]].add(row["Wahlkreisnummer"])

    for ags, wk_list in observed_wks_by_ags.items():
        for wk in wk_list:
            merged_wks_by_ags[ags].add(wk)
            key = (wk, ags)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "Wahlkreisnummer": wk,
                    "Wahlkreisname": wk_name_by_id.get(wk, f"Wahlkreis {wk}"),
                    "Gemeindekennziffer": ags,
                    "Gemeindename": municipality_name_by_ags.get(ags, ags),
                }
            )

    rows.sort(key=lambda item: (int(item["Wahlkreisnummer"]), item["Gemeindekennziffer"]))
    return rows, {ags: sorted(wks, key=int) for ags, wks in merged_wks_by_ags.items()}


def merge_municipality_rows(
    base_rows: Sequence[Dict[str, str]],
    summary_name_by_ags: Dict[str, str],
    observed_wks_by_ags: Dict[str, List[str]],
) -> List[Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    for row in base_rows:
        ags = str(row.get("ags") or "").strip()
        if not ags:
            continue
        merged[ags] = dict(row)

    for ags, wk_list in observed_wks_by_ags.items():
        if not ags or not wk_list:
            continue
        name = summary_name_by_ags.get(ags, "").strip()
        existing = merged.get(ags)
        if existing is None:
            merged[ags] = {
                "ags": ags,
                "municipality_name": name or ags,
                "source": "official-workbook-2021-summary",
                "fragment_count": "0",
                "wahlkreis_count": str(len(wk_list)),
                "wahlkreis_ids": "|".join(wk_list),
            }
            continue
        if name and not str(existing.get("municipality_name") or "").strip():
            existing["municipality_name"] = name
        if wk_list:
            existing["wahlkreis_count"] = str(len(wk_list))
            existing["wahlkreis_ids"] = "|".join(wk_list)

    rows = sorted(merged.values(), key=lambda item: str(item.get("ags") or ""))
    return rows


def make_row_key(index: int, gebietsnummer: str, bezirksnummer: str, ags: str, gebietsart: str) -> str:
    return f"{index:06d}:{gebietsnummer or '-'}:{bezirksnummer or '-'}:{ags or '-'}:{gebietsart or '-'}"


def append_snapshot(
    snapshots: List[Dict[str, Any]],
    raw_rows: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
    *,
    ags: str,
    municipality_name: str,
    gebietsart: str,
    gebietsnummer: str,
    bezirksnummer: str,
    gebietsname: str,
    wahlkreisnummer: str,
    total_precincts: int,
    first_parties: Sequence[str],
    second_parties: Sequence[str],
    is_municipality_summary: bool,
    historical_result: Dict[str, Any] | None,
) -> None:
    row_key = make_row_key(len(snapshots), gebietsnummer, bezirksnummer, ags, gebietsart)
    historical_voters = "" if historical_result is None else int(historical_result.get("voters_total") or 0)
    historical_valid_erst = "" if historical_result is None else int(historical_result.get("valid_votes_erst") or 0)
    historical_valid_zweit = "" if historical_result is None else int(historical_result.get("valid_votes_zweit") or 0)
    snapshots.append(
        {
            "row_key": row_key,
            "ags": ags,
            "municipality_name": municipality_name,
            "gebietsart": gebietsart,
            "gebietsnummer": gebietsnummer,
            "reported_precincts": 0,
            "total_precincts": total_precincts,
            "voters_total": 0,
            "valid_votes_erst": 0,
            "valid_votes_zweit": 0,
            "voters_total_2021": historical_voters,
            "valid_votes_erst_2021": historical_valid_erst,
            "valid_votes_zweit_2021": historical_valid_zweit,
            "delta_voters_total_vs_2021": "" if historical_result is None else -int(historical_result.get("voters_total") or 0),
            "delta_valid_votes_erst_vs_2021": "" if historical_result is None else -int(historical_result.get("valid_votes_erst") or 0),
            "delta_valid_votes_zweit_vs_2021": "" if historical_result is None else -int(historical_result.get("valid_votes_zweit") or 0),
            "payload_hash": f"prep-zero:{row_key}",
            "is_municipality_summary": "true" if is_municipality_summary else "false",
        }
    )
    raw_rows.append(
        {
            "Wahlkreisnummer": wahlkreisnummer,
            "Gemeindename": municipality_name,
            "Gebietsname": gebietsname,
            "Gebietsnummer": gebietsnummer,
            "Bezirksnummer": bezirksnummer,
            "Gebietsart": gebietsart,
            "AGS": ags,
        }
    )
    for party_name in first_parties:
        historical_votes = 0 if historical_result is None else int(historical_result.get("party_votes", {}).get("Erststimmen", {}).get(party_name, 0) or 0)
        historical_total = 0 if historical_result is None else int(historical_result.get("valid_votes_erst") or 0)
        party_rows.append(
            {
                "row_key": row_key,
                "vote_type": "Erststimmen",
                "party_key": party_name,
                "party_name": party_name,
                "votes": 0,
                "votes_2021": "" if historical_result is None else historical_votes,
                "share_percent_2021": "" if historical_result is None else share_percent(historical_votes, historical_total),
                "delta_votes_vs_2021": "" if historical_result is None else -historical_votes,
                "delta_share_percent_vs_2021": "" if historical_result is None else -share_percent(historical_votes, historical_total),
            }
        )
    for party_name in second_parties:
        historical_votes = 0 if historical_result is None else int(historical_result.get("party_votes", {}).get("Zweitstimmen", {}).get(party_name, 0) or 0)
        historical_total = 0 if historical_result is None else int(historical_result.get("valid_votes_zweit") or 0)
        party_rows.append(
            {
                "row_key": row_key,
                "vote_type": "Zweitstimmen",
                "party_key": party_name,
                "party_name": party_name,
                "votes": 0,
                "votes_2021": "" if historical_result is None else historical_votes,
                "share_percent_2021": "" if historical_result is None else share_percent(historical_votes, historical_total),
                "delta_votes_vs_2021": "" if historical_result is None else -historical_votes,
                "delta_share_percent_vs_2021": "" if historical_result is None else -share_percent(historical_votes, historical_total),
            }
        )


def build_zero_exports(
    municipalities: Sequence[Dict[str, str]],
    wk_name_by_id: Dict[str, str],
    wks_by_ags: Dict[str, List[str]],
    booths: Sequence[Dict[str, str]],
    first_parties: Sequence[str],
    second_parties: Sequence[str],
    history: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    booth_count_by_ags = Counter(booth["ags"] for booth in booths)
    booth_count_by_wk = Counter(booth["wahlkreisnummer"] for booth in booths)
    booth_count_by_ags_wk = Counter((booth["ags"], booth["wahlkreisnummer"]) for booth in booths)

    snapshots: List[Dict[str, Any]] = []
    party_rows: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []

    append_snapshot(
        snapshots,
        raw_rows,
        party_rows,
        ags="",
        municipality_name="Land Rheinland-Pfalz",
        gebietsart="LAND",
        gebietsnummer="000000",
        bezirksnummer="",
        gebietsname="Land Rheinland-Pfalz",
        wahlkreisnummer="",
        total_precincts=len(booths),
        first_parties=first_parties,
        second_parties=second_parties,
        is_municipality_summary=False,
        historical_result=history.get("land"),
    )

    for wk, wk_name in sorted(wk_name_by_id.items(), key=lambda item: int(item[0])):
        append_snapshot(
            snapshots,
            raw_rows,
            party_rows,
            ags="",
            municipality_name=wk_name,
            gebietsart="WAHLKREIS",
            gebietsnummer=wk,
            bezirksnummer="",
            gebietsname=wk_name,
            wahlkreisnummer=wk,
            total_precincts=booth_count_by_wk.get(wk, 0),
            first_parties=first_parties,
            second_parties=second_parties,
            is_municipality_summary=False,
            historical_result=history.get("wahlkreis", {}).get(wk),
        )

    municipality_name_by_ags = {row["ags"]: row["municipality_name"] for row in municipalities}
    for ags, municipality_name in sorted(municipality_name_by_ags.items()):
        wk_candidates = wks_by_ags.get(ags, [])
        municipality_history = history.get("municipality", {}).get(ags)
        if municipality_history is None:
            municipality_history = combine_history_rows(
                [
                    history.get("wahlkreis_teil", {}).get((ags, wk))
                    for wk in wk_candidates
                    if history.get("wahlkreis_teil", {}).get((ags, wk)) is not None
                ]
            )
        append_snapshot(
            snapshots,
            raw_rows,
            party_rows,
            ags=ags,
            municipality_name=municipality_name,
            gebietsart="GEMEINDE",
            gebietsnummer=ags,
            bezirksnummer="",
            gebietsname=municipality_name,
            wahlkreisnummer=wk_candidates[0] if len(wk_candidates) == 1 else "",
            total_precincts=booth_count_by_ags.get(ags, 0),
            first_parties=first_parties,
            second_parties=second_parties,
            is_municipality_summary=True,
            historical_result=municipality_history,
        )
        if len(wk_candidates) <= 1:
            continue
        for wk in wk_candidates:
            append_snapshot(
                snapshots,
                raw_rows,
                party_rows,
                ags=ags,
                municipality_name=municipality_name,
                gebietsart="WAHLKREIS_TEIL",
                gebietsnummer=wk,
                bezirksnummer="",
                gebietsname=municipality_name,
                wahlkreisnummer=wk,
                total_precincts=booth_count_by_ags_wk.get((ags, wk), 0),
                first_parties=first_parties,
                second_parties=second_parties,
                is_municipality_summary=False,
                historical_result=history.get("wahlkreis_teil", {}).get((ags, wk)),
            )

    for booth in sorted(
        booths,
        key=lambda item: (
            item["ags"],
            int(item["wahlkreisnummer"]),
            item["gebietsart"],
            item["booth_code"],
            item["label"],
        ),
    ):
        append_snapshot(
            snapshots,
            raw_rows,
            party_rows,
            ags=booth["ags"],
            municipality_name=booth["municipality_name"],
            gebietsart=booth["gebietsart"],
            gebietsnummer=booth["booth_code"],
            bezirksnummer=booth["booth_code"],
            gebietsname=booth["label"],
            wahlkreisnummer=booth["wahlkreisnummer"],
            total_precincts=1,
            first_parties=first_parties,
            second_parties=second_parties,
            is_municipality_summary=False,
            historical_result=history.get("booth", {}).get(
                (
                    booth["ags"],
                    booth["wahlkreisnummer"],
                    booth["booth_code"],
                    booth["gebietsart"],
                )
            ),
        )

    return snapshots, party_rows, raw_rows


def write_run_metadata(raw_filename_stem: str) -> None:
    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generated_by": "scripts/build_rlp_zero_latest.py",
        "run_label": raw_filename_stem,
        "statla_mode": "PREP_ZERO",
        "statla_url": "https://www.wahlen.rlp.de/landtagswahl/ergebnisse",
        "statla_error": "",
        "kommone_municipalities_polled": 0,
    }
    (LATEST_DIR / "run_metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    manifest_data = manifest()
    official_sources = manifest_data.get("official_sources", {})
    workbook_path = ensure_download(str(official_sources.get("historical_results_xlsx_url_2021") or ""), WORKBOOK_CACHE_PATH)
    geodata_path = ensure_download(str(official_sources.get("geodata_zip_url_2026") or ""), GEODATA_CACHE_PATH)

    municipality_rows = read_csv_rows(META_DIR / "municipalities.csv")
    fragment_rows = read_csv_rows(META_DIR / "municipality_fragments_2021.csv")
    municipality_name_by_ags = {row["ags"]: row["municipality_name"] for row in municipality_rows}
    mapping_rows, wk_name_by_id, _base_wks_by_ags = mapping_rows_from_fragments(fragment_rows)
    first_parties, second_parties, booth_rows, summary_name_by_ags, observed_wks_by_ags, history = parse_workbook_context(
        workbook_path,
        municipality_name_by_ags,
    )
    municipality_rows = merge_municipality_rows(municipality_rows, summary_name_by_ags, observed_wks_by_ags)
    municipality_name_by_ags = {row["ags"]: row["municipality_name"] for row in municipality_rows}
    mapping_rows, wks_by_ags = merge_mapping_rows(mapping_rows, wk_name_by_id, municipality_name_by_ags, observed_wks_by_ags)
    for booth in booth_rows:
        booth["municipality_name"] = municipality_name_by_ags.get(booth["ags"], booth["municipality_name"])
    snapshots, party_rows, raw_rows = build_zero_exports(
        municipality_rows,
        wk_name_by_id,
        wks_by_ags,
        booth_rows,
        first_parties,
        second_parties,
        history,
    )

    wahlkreis_geojson = build_wahlkreis_geojson(geodata_path, wk_name_by_id)

    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    RAW_STATLA_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(
        META_DIR / "municipalities.csv",
        ["ags", "municipality_name", "source", "fragment_count", "wahlkreis_count", "wahlkreis_ids"],
        municipality_rows,
    )
    write_csv(META_DIR / "wahlkreis-mapping.csv", MAPPING_HEADERS, mapping_rows, delimiter=";")
    (META_DIR / "wahlkreise.geojson").write_text(json.dumps(wahlkreis_geojson, ensure_ascii=False), encoding="utf-8")
    write_csv(LATEST_DIR / "statla_snapshots.csv", SNAPSHOT_HEADERS, snapshots)
    write_csv(LATEST_DIR / "statla_party_results.csv", PARTY_HEADERS, party_rows)
    raw_stem = "prep-zero-rlp"
    write_csv(RAW_STATLA_DIR / f"{raw_stem}-statla.csv", RAW_HEADERS, raw_rows, delimiter=";")
    write_run_metadata(raw_stem)

    print(f"Snapshots: {len(snapshots)}")
    print(f"Party rows: {len(party_rows)}")
    print(f"Booths: {len(booth_rows)}")
    print(f"Wahlkreise: {len(wk_name_by_id)}")
    print(f"Municipalities: {len(municipality_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
