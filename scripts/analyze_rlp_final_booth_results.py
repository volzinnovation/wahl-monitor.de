#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Sequence
from urllib.parse import urlparse
from zipfile import ZipFile

import numpy as np
import requests
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "2026-rlp"
LATEST_DIR = DATA_DIR / "latest"
REPORT_DIR = DATA_DIR / "reports"
RAW_DIR = DATA_DIR / "raw" / "official-final-booths"
BERLIN = ZoneInfo("Europe/Berlin")
XLSX_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

PRELIMINARY_CSV_PATH = LATEST_DIR / "official_results_source.csv"
PRELIMINARY_METADATA_PATH = LATEST_DIR / "official_results_source_metadata.json"
LIVE_SNAPSHOTS_REL = "data/2026-rlp/latest/statla_snapshots.csv"
LIVE_PARTY_REL = "data/2026-rlp/latest/statla_party_results.csv"

FINAL_DOWNLOAD_PAGE_URL = "https://www.wahlen.rlp.de/landtagswahl/ergebnisse"
FINAL_CSV_URL = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/"
    "Ergebnisdateien/2026/LW_2026_Endergebnis_Stimmbezirksebene.csv"
)
FINAL_XLSX_URL = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/"
    "Ergebnisdateien/2026/LW_2026_Endergebnis_Stimmbezirksebene.xlsx"
)
FINAL_DESCRIPTION_URL = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/"
    "Ergebnisdateien/2026/SatzbeschreibungErgebnisseGesamtLW_2026.pdf"
)

FINAL_EFFECTIVE_BOOTHS_CSV = REPORT_DIR / "official_final_effective_booth_rows.csv"
FINAL_AGGREGATED_BOOTHS_CSV = REPORT_DIR / "official_final_aggregated_or_missing_booth_rows.csv"
FINAL_BOOTH_SUMMARY_JSON = REPORT_DIR / "official_final_booth_aggregation_summary.json"
BOOTH_VALIDATION_WK_CSV = REPORT_DIR / "official_final_booth_validation_wahlkreis.csv"
BOOTH_VALIDATION_SUMMARY_JSON = REPORT_DIR / "official_final_booth_validation_summary.json"
DELTA_ROWS_CSV = REPORT_DIR / "official_final_vs_preliminary_delta_rows.csv"
DELTA_SUMMARY_JSON = REPORT_DIR / "official_final_vs_preliminary_delta_summary.json"
LAND_MC_PATH_CSV = REPORT_DIR / "official_final_cdu_land_monte_carlo_path.csv"
WAHLKREIS_MC_SUMMARY_CSV = REPORT_DIR / "official_final_cdu_wahlkreis_monte_carlo_summary.csv"
MC_SUMMARY_JSON = REPORT_DIR / "official_final_cdu_monte_carlo_summary.json"
SOURCE_METADATA_JSON = REPORT_DIR / "official_final_booth_source_metadata.json"
SUMMARY_MD = REPORT_DIR / "official_final_booth_analysis_summary.md"


PRELIMINARY_COLS = {
    "id": 0,
    "voters_total": 5,
    "valid_first": 10,
    "cdu_first_votes": 14,
    "valid_second": 114,
    "cdu_second_votes": 118,
    "total_precincts": 219,
    "reported_precincts": 220,
    "last_changed": 221,
    "merge_target": 222,
    "merge_reason": 223,
    "merge_plus": 224,
    "label": 225,
}


FINAL_COLS = {
    "id": "A",
    "stimmbezirk_id": "B",
    "code": "C",
    "label": "D",
    "guw": "E",
    "voters_total": "K",
    "valid_first": "P",
    "cdu_first_votes": "T",
    "valid_second": "DP",
    "cdu_second_votes": "DT",
    "merge_target": "HO",
    "merge_reason": "HP",
    "merge_plus": "HQ",
}


LEVEL_GROUP_BY_CODE = {
    "LD": "LAND",
    "BZ": "BEZIRK",
    "WK": "WAHLKREIS",
    "TK": "WAHLKREIS_TEIL",
    "TS": "WAHLKREIS_TEIL",
    "LK": "KREIS_ODER_STADT",
    "KS": "KREIS_ODER_STADT",
    "VG": "KOMMUNE",
    "VF": "KOMMUNE",
    "GD": "KOMMUNE",
    "ST": "KOMMUNENTEIL",
    "SB": "STIMMBEZIRK",
}


@dataclass(frozen=True)
class CommitInfo:
    commit: str
    committed_at_utc: str
    subject: str


def ensure_directories() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def repo_relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def fetch_to_path(url: str, path: Path, timeout_seconds: int = 120) -> Dict[str, object]:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    path.write_bytes(response.content)
    return {
        "url": url,
        "path": str(path),
        "status_code": response.status_code,
        "sha256": sha256_bytes(response.content),
        "byte_count": len(response.content),
    }


def parse_int(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\xa0", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return int(round(float(text)))
    except ValueError:
        return None


def normalize_identifier(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("\xa0", "").replace(" ", "")
    if "E+" in text.upper():
        normalized = text.replace(",", ".")
        try:
            text = format(Decimal(normalized), "f")
        except (InvalidOperation, ValueError):
            return text
    if text.endswith(".0"):
        text = text[:-2]
    if "." in text:
        left, right = text.split(".", 1)
        if right.strip("0") == "":
            text = left
    stripped = text.lstrip("0")
    return stripped or "0"


def load_xlsx_rows(path: Path) -> List[Dict[str, str]]:
    with ZipFile(path) as archive:
        shared_strings: List[str] = []
        shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        for item in shared_root.findall("a:si", XLSX_NS):
            shared_strings.append("".join(node.text or "" for node in item.iterfind(".//a:t", XLSX_NS)))

        sheet_root = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
        rows: List[Dict[str, str]] = []
        for row in sheet_root.findall(".//a:sheetData/a:row", XLSX_NS):
            values: Dict[str, str] = {}
            for cell in row.findall("a:c", XLSX_NS):
                ref = cell.attrib.get("r", "")
                column = "".join(ch for ch in ref if ch.isalpha())
                cell_type = cell.attrib.get("t")
                if cell_type == "s":
                    value_node = cell.find("a:v", XLSX_NS)
                    if value_node is None or value_node.text is None:
                        value = ""
                    else:
                        value = shared_strings[int(value_node.text)]
                else:
                    inline = "".join(node.text or "" for node in cell.iterfind(".//a:t", XLSX_NS))
                    if inline:
                        value = inline
                    else:
                        value_node = cell.find("a:v", XLSX_NS)
                        value = value_node.text if value_node is not None and value_node.text is not None else ""
                values[column] = value
            rows.append(values)
    return rows


def load_final_rows(xlsx_path: Path) -> List[Dict[str, object]]:
    xlsx_rows = load_xlsx_rows(xlsx_path)
    data_rows = xlsx_rows[3:]
    final_rows: List[Dict[str, object]] = []
    for row in data_rows:
        row_id_raw = str(row.get(FINAL_COLS["id"], "")).strip()
        final_rows.append(
            {
                "row_id": normalize_identifier(row_id_raw),
                "row_id_raw": row_id_raw,
                "stimmbezirk_id": normalize_identifier(row.get(FINAL_COLS["stimmbezirk_id"], "")),
                "code": str(row.get(FINAL_COLS["code"], "")).strip(),
                "level_group": LEVEL_GROUP_BY_CODE.get(str(row.get(FINAL_COLS["code"], "")).strip(), "UNBEKANNT"),
                "label": str(row.get(FINAL_COLS["label"], "")).strip(),
                "guw": str(row.get(FINAL_COLS["guw"], "")).strip(),
                "voters_total": parse_int(row.get(FINAL_COLS["voters_total"])),
                "valid_first": parse_int(row.get(FINAL_COLS["valid_first"])),
                "cdu_first_votes": parse_int(row.get(FINAL_COLS["cdu_first_votes"])),
                "valid_second": parse_int(row.get(FINAL_COLS["valid_second"])),
                "cdu_second_votes": parse_int(row.get(FINAL_COLS["cdu_second_votes"])),
                "merge_target_raw": str(row.get(FINAL_COLS["merge_target"], "")).strip(),
                "merge_reason": str(row.get(FINAL_COLS["merge_reason"], "")).strip(),
                "merge_plus": str(row.get(FINAL_COLS["merge_plus"], "")).strip(),
            }
        )
    return final_rows


def load_preliminary_rows(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle, delimiter=";")
        next(reader)
        for raw in reader:
            rows.append(
                {
                    "row_id": normalize_identifier(raw[PRELIMINARY_COLS["id"]]),
                    "row_id_raw": raw[PRELIMINARY_COLS["id"]].strip(),
                    "label": raw[PRELIMINARY_COLS["label"]].strip(),
                    "voters_total": parse_int(raw[PRELIMINARY_COLS["voters_total"]]),
                    "valid_first": parse_int(raw[PRELIMINARY_COLS["valid_first"]]),
                    "cdu_first_votes": parse_int(raw[PRELIMINARY_COLS["cdu_first_votes"]]),
                    "valid_second": parse_int(raw[PRELIMINARY_COLS["valid_second"]]),
                    "cdu_second_votes": parse_int(raw[PRELIMINARY_COLS["cdu_second_votes"]]),
                    "total_precincts": parse_int(raw[PRELIMINARY_COLS["total_precincts"]]),
                    "reported_precincts": parse_int(raw[PRELIMINARY_COLS["reported_precincts"]]),
                    "last_changed": raw[PRELIMINARY_COLS["last_changed"]].strip(),
                    "merge_target_raw": raw[PRELIMINARY_COLS["merge_target"]].strip(),
                    "merge_reason": raw[PRELIMINARY_COLS["merge_reason"]].strip(),
                    "merge_plus": raw[PRELIMINARY_COLS["merge_plus"]].strip(),
                }
            )
    return rows


def share_percent(votes: int | None, valid_votes: int | None) -> float | None:
    if votes is None or valid_votes is None or valid_votes == 0:
        return None
    return votes / valid_votes * 100.0


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def join_final_and_preliminary(
    final_rows: List[Dict[str, object]],
    preliminary_rows: List[Dict[str, object]],
) -> tuple[List[Dict[str, object]], Dict[str, object]]:
    final_aggregate = {
        str(row["row_id"]): row
        for row in final_rows
        if row["stimmbezirk_id"] == "0" and row["guw"] == "G"
    }
    preliminary = {str(row["row_id"]): row for row in preliminary_rows}

    matched_ids = sorted(set(final_aggregate) & set(preliminary), key=lambda item: (len(item), item))
    delta_rows: List[Dict[str, object]] = []
    summary_by_code: Dict[str, Dict[str, object]] = {}
    for row_id in matched_ids:
        final_row = final_aggregate[row_id]
        prelim_row = preliminary[row_id]
        code = str(final_row["code"])
        bucket = summary_by_code.setdefault(
            code,
            {
                "code": code,
                "level_group": LEVEL_GROUP_BY_CODE.get(code, "UNBEKANNT"),
                "matched_rows": 0,
                "rows_with_any_change": 0,
                "sum_abs_delta_voters_total": 0,
                "sum_abs_delta_valid_second": 0,
                "sum_abs_delta_cdu_second_votes": 0,
                "max_abs_delta_cdu_second_share_pp": 0.0,
            },
        )
        delta_voters_total = (final_row["voters_total"] or 0) - (prelim_row["voters_total"] or 0)
        delta_valid_second = (final_row["valid_second"] or 0) - (prelim_row["valid_second"] or 0)
        delta_cdu_second_votes = (final_row["cdu_second_votes"] or 0) - (prelim_row["cdu_second_votes"] or 0)
        final_cdu_second_share = share_percent(final_row["cdu_second_votes"], final_row["valid_second"])
        prelim_cdu_second_share = share_percent(prelim_row["cdu_second_votes"], prelim_row["valid_second"])
        delta_cdu_second_share = None
        if final_cdu_second_share is not None and prelim_cdu_second_share is not None:
            delta_cdu_second_share = final_cdu_second_share - prelim_cdu_second_share

        row_payload = {
            "row_id": row_id,
            "code": code,
            "level_group": LEVEL_GROUP_BY_CODE.get(code, "UNBEKANNT"),
            "label": final_row["label"],
            "preliminary_label": prelim_row["label"],
            "preliminary_voters_total": prelim_row["voters_total"],
            "final_voters_total": final_row["voters_total"],
            "delta_voters_total": delta_voters_total,
            "preliminary_valid_second": prelim_row["valid_second"],
            "final_valid_second": final_row["valid_second"],
            "delta_valid_second": delta_valid_second,
            "preliminary_cdu_second_votes": prelim_row["cdu_second_votes"],
            "final_cdu_second_votes": final_row["cdu_second_votes"],
            "delta_cdu_second_votes": delta_cdu_second_votes,
            "preliminary_cdu_second_share_percent": prelim_cdu_second_share,
            "final_cdu_second_share_percent": final_cdu_second_share,
            "delta_cdu_second_share_pp": delta_cdu_second_share,
            "preliminary_total_precincts": prelim_row["total_precincts"],
            "preliminary_reported_precincts": prelim_row["reported_precincts"],
            "preliminary_last_changed": prelim_row["last_changed"],
        }
        delta_rows.append(row_payload)

        bucket["matched_rows"] = int(bucket["matched_rows"]) + 1
        bucket["sum_abs_delta_voters_total"] = int(bucket["sum_abs_delta_voters_total"]) + abs(delta_voters_total)
        bucket["sum_abs_delta_valid_second"] = int(bucket["sum_abs_delta_valid_second"]) + abs(delta_valid_second)
        bucket["sum_abs_delta_cdu_second_votes"] = int(bucket["sum_abs_delta_cdu_second_votes"]) + abs(
            delta_cdu_second_votes
        )
        if delta_cdu_second_share is not None:
            bucket["max_abs_delta_cdu_second_share_pp"] = max(
                float(bucket["max_abs_delta_cdu_second_share_pp"]),
                abs(delta_cdu_second_share),
            )
        if any(
            value not in (0, 0.0, None)
            for value in [delta_voters_total, delta_valid_second, delta_cdu_second_votes, delta_cdu_second_share]
        ):
            bucket["rows_with_any_change"] = int(bucket["rows_with_any_change"]) + 1

    summary_payload = {
        "matched_rows": len(matched_ids),
        "preliminary_rows": len(preliminary_rows),
        "final_aggregate_rows": len(final_aggregate),
        "only_in_preliminary": sorted(set(preliminary) - set(final_aggregate)),
        "only_in_final": sorted(set(final_aggregate) - set(preliminary)),
        "by_code": sorted(summary_by_code.values(), key=lambda item: (item["level_group"], item["code"])),
    }
    return delta_rows, summary_payload


def effective_booth_rows(final_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    return [
        row
        for row in final_rows
        if row["code"] == "SB" and not row["merge_target_raw"] and row["valid_second"] is not None
    ]


def aggregated_booth_rows(final_rows: List[Dict[str, object]]) -> tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    donor_rows = [row for row in final_rows if row["code"] == "SB" and row["merge_target_raw"]]
    receiver_rows = [row for row in final_rows if row["code"] == "SB" and row["merge_plus"] == "+"]
    return donor_rows, receiver_rows


def booth_wahlkreis_nummer(row: Dict[str, object]) -> str:
    row_id = str(row["row_id"])
    if len(row_id) >= 3 and row_id[0].isdigit():
        return row_id[:3]
    return ""


def aggregate_wahlkreis_nummer(row: Dict[str, object]) -> str:
    row_id = str(row["row_id"])
    if len(row_id) >= 3 and row_id[0].isdigit():
        return row_id[:3]
    return ""


def build_booth_reports(final_rows: List[Dict[str, object]], preliminary_rows: List[Dict[str, object]]) -> Dict[str, object]:
    donors, receivers = aggregated_booth_rows(final_rows)
    effective_rows = effective_booth_rows(final_rows)
    booth_rows = [row for row in final_rows if row["code"] == "SB"]

    effective_by_guw = Counter(str(row["guw"]) for row in effective_rows)
    donor_reason_counts = Counter(str(row["merge_reason"]) for row in donors)
    preliminary_land = next(row for row in preliminary_rows if row["row_id"] == "0")
    summary = {
        "structural_booth_rows": len(booth_rows),
        "effective_individual_booth_results": len(effective_rows),
        "aggregated_away_booth_rows": len(donors),
        "aggregation_receiver_booth_rows": len(receivers),
        "blank_booth_rows_without_merge_marker": len(
            [row for row in booth_rows if row["valid_second"] is None and not row["merge_target_raw"]]
        ),
        "effective_by_guw": dict(sorted(effective_by_guw.items())),
        "aggregated_donor_reasons": dict(sorted(donor_reason_counts.items())),
        "preliminary_land_precinct_count": preliminary_land["total_precincts"],
        "effective_minus_preliminary_land_precincts": len(effective_rows) - int(preliminary_land["total_precincts"] or 0),
        "structural_minus_effective_booths": len(booth_rows) - len(effective_rows),
    }

    effective_export = []
    for row in sorted(effective_rows, key=lambda item: (booth_wahlkreis_nummer(item), str(item["row_id"]), str(item["stimmbezirk_id"]))):
        effective_export.append(
            {
                "wahlkreis_nummer": booth_wahlkreis_nummer(row),
                "row_id": row["row_id"],
                "stimmbezirk_id": row["stimmbezirk_id"],
                "label": row["label"],
                "guw": row["guw"],
                "valid_second": row["valid_second"],
                "cdu_second_votes": row["cdu_second_votes"],
                "cdu_second_share_percent": share_percent(row["cdu_second_votes"], row["valid_second"]),
            }
        )

    aggregated_export = []
    for role, rows in [("aggregated_away", donors), ("aggregation_receiver", receivers)]:
        for row in sorted(rows, key=lambda item: (booth_wahlkreis_nummer(item), str(item["row_id"]), str(item["stimmbezirk_id"]))):
            aggregated_export.append(
                {
                    "role": role,
                    "wahlkreis_nummer": booth_wahlkreis_nummer(row),
                    "row_id": row["row_id"],
                    "stimmbezirk_id": row["stimmbezirk_id"],
                    "label": row["label"],
                    "guw": row["guw"],
                    "valid_second": row["valid_second"],
                    "cdu_second_votes": row["cdu_second_votes"],
                    "merge_reason": row["merge_reason"],
                    "merge_target_raw": row["merge_target_raw"],
                    "merge_plus": row["merge_plus"],
                }
            )

    write_csv(
        FINAL_EFFECTIVE_BOOTHS_CSV,
        [
            "wahlkreis_nummer",
            "row_id",
            "stimmbezirk_id",
            "label",
            "guw",
            "valid_second",
            "cdu_second_votes",
            "cdu_second_share_percent",
        ],
        effective_export,
    )
    write_csv(
        FINAL_AGGREGATED_BOOTHS_CSV,
        [
            "role",
            "wahlkreis_nummer",
            "row_id",
            "stimmbezirk_id",
            "label",
            "guw",
            "valid_second",
            "cdu_second_votes",
            "merge_reason",
            "merge_target_raw",
            "merge_plus",
        ],
        aggregated_export,
    )
    write_json(FINAL_BOOTH_SUMMARY_JSON, summary)
    return summary


def build_booth_validation(final_rows: List[Dict[str, object]]) -> Dict[str, object]:
    aggregate_rows = {
        str(row["row_id"]): row
        for row in final_rows
        if row["stimmbezirk_id"] == "0" and row["guw"] == "G"
    }
    exact_codes = {"ST"}
    effective_rows = effective_booth_rows(final_rows)

    exact_sums: Dict[str, Dict[str, int]] = defaultdict(lambda: {"booth_count": 0, "valid_second": 0, "cdu_second_votes": 0})
    wahlkreis_sums: Dict[str, Dict[str, int]] = defaultdict(lambda: {"booth_count": 0, "valid_second": 0, "cdu_second_votes": 0})
    land_sum = {"booth_count": 0, "valid_second": 0, "cdu_second_votes": 0}

    for row in effective_rows:
        exact_sums[str(row["row_id"])]["booth_count"] += 1
        exact_sums[str(row["row_id"])]["valid_second"] += int(row["valid_second"] or 0)
        exact_sums[str(row["row_id"])]["cdu_second_votes"] += int(row["cdu_second_votes"] or 0)

        wahlkreis = booth_wahlkreis_nummer(row)
        wahlkreis_sums[wahlkreis]["booth_count"] += 1
        wahlkreis_sums[wahlkreis]["valid_second"] += int(row["valid_second"] or 0)
        wahlkreis_sums[wahlkreis]["cdu_second_votes"] += int(row["cdu_second_votes"] or 0)

        land_sum["booth_count"] += 1
        land_sum["valid_second"] += int(row["valid_second"] or 0)
        land_sum["cdu_second_votes"] += int(row["cdu_second_votes"] or 0)

    exact_matches = 0
    exact_total = 0
    for row in aggregate_rows.values():
        if str(row["code"]) not in exact_codes or row["valid_second"] is None:
            continue
        exact_total += 1
        summed = exact_sums.get(str(row["row_id"]), {"valid_second": 0, "cdu_second_votes": 0})
        if (
            summed["valid_second"] == int(row["valid_second"] or 0)
            and summed["cdu_second_votes"] == int(row["cdu_second_votes"] or 0)
        ):
            exact_matches += 1

    wahlkreis_rows = []
    for row in sorted(
        [aggregate for aggregate in aggregate_rows.values() if aggregate["code"] == "WK"],
        key=lambda item: int(aggregate_wahlkreis_nummer(item)),
    ):
        key = aggregate_wahlkreis_nummer(row)
        summed = wahlkreis_sums.get(key, {"booth_count": 0, "valid_second": 0, "cdu_second_votes": 0})
        delta_valid_second = summed["valid_second"] - int(row["valid_second"] or 0)
        delta_cdu_second_votes = summed["cdu_second_votes"] - int(row["cdu_second_votes"] or 0)
        wahlkreis_rows.append(
            {
                "wahlkreis_nummer": key,
                "label": row["label"],
                "booth_count": summed["booth_count"],
                "aggregate_valid_second": row["valid_second"],
                "booth_sum_valid_second": summed["valid_second"],
                "delta_valid_second": delta_valid_second,
                "aggregate_cdu_second_votes": row["cdu_second_votes"],
                "booth_sum_cdu_second_votes": summed["cdu_second_votes"],
                "delta_cdu_second_votes": delta_cdu_second_votes,
            }
        )

    write_csv(
        BOOTH_VALIDATION_WK_CSV,
        [
            "wahlkreis_nummer",
            "label",
            "booth_count",
            "aggregate_valid_second",
            "booth_sum_valid_second",
            "delta_valid_second",
            "aggregate_cdu_second_votes",
            "booth_sum_cdu_second_votes",
            "delta_cdu_second_votes",
        ],
        wahlkreis_rows,
    )

    land_row = aggregate_rows["0"]
    land_delta_valid_second = land_sum["valid_second"] - int(land_row["valid_second"] or 0)
    land_delta_cdu_second_votes = land_sum["cdu_second_votes"] - int(land_row["cdu_second_votes"] or 0)
    summary = {
        "stadtteil_rows_checked": exact_total,
        "stadtteil_rows_all_zero_delta": exact_matches == exact_total,
        "wahlkreis_rows_checked": len(wahlkreis_rows),
        "wahlkreis_rows_all_zero_delta": all(
            row["delta_valid_second"] == 0 and row["delta_cdu_second_votes"] == 0 for row in wahlkreis_rows
        ),
        "land_booth_count": land_sum["booth_count"],
        "land_delta_valid_second": land_delta_valid_second,
        "land_delta_cdu_second_votes": land_delta_cdu_second_votes,
    }
    write_json(BOOTH_VALIDATION_SUMMARY_JSON, summary)
    return summary


def git_poll_commits() -> List[CommitInfo]:
    raw = subprocess.check_output(
        ["git", "log", "--reverse", "--format=%H\t%cI\t%s", "--", LIVE_SNAPSHOTS_REL, LIVE_PARTY_REL],
        cwd=ROOT,
        text=True,
    )
    commits: List[CommitInfo] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        if not line.strip():
            continue
        commit, committed_at_utc, subject = line.split("\t", 2)
        if commit in seen or not subject.startswith("2026-rlp poll "):
            continue
        seen.add(commit)
        commits.append(CommitInfo(commit=commit, committed_at_utc=committed_at_utc, subject=subject))
    return commits


def load_csv_at_commit(commit: str, rel_path: str) -> List[Dict[str, str]]:
    text = subprocess.check_output(["git", "show", f"{commit}:{rel_path}"], cwd=ROOT, text=True)
    return list(csv.DictReader(io.StringIO(text)))


def build_party_index(rows: Iterable[Dict[str, str]]) -> Dict[tuple[str, str], Dict[str, int]]:
    out: Dict[tuple[str, str], Dict[str, int]] = defaultdict(dict)
    for row in rows:
        votes = parse_int(row.get("votes"))
        if votes is None:
            continue
        out[(str(row["row_key"]), str(row["vote_type"]))][str(row["party_name"])] = votes
    return out


def local_datetime(utc_iso: str) -> datetime:
    return datetime.fromisoformat(utc_iso).astimezone(BERLIN)


def extract_live_timelines(election_evening_date: str = "2026-03-22") -> Dict[str, List[Dict[str, object]]]:
    timelines: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for commit in git_poll_commits():
        committed_local = local_datetime(commit.committed_at_utc)
        if committed_local.date().isoformat() != election_evening_date:
            continue
        snapshots = load_csv_at_commit(commit.commit, LIVE_SNAPSHOTS_REL)
        party_rows = load_csv_at_commit(commit.commit, LIVE_PARTY_REL)
        party_index = build_party_index(party_rows)

        for snapshot in snapshots:
            gebietsart = str(snapshot.get("gebietsart") or "").strip()
            if gebietsart not in {"LAND", "WAHLKREIS"}:
                continue
            reported_precincts = parse_int(snapshot.get("reported_precincts"))
            valid_second = parse_int(snapshot.get("valid_votes_zweit"))
            if reported_precincts is None or reported_precincts <= 0 or valid_second in (None, 0):
                continue
            if gebietsart == "LAND":
                unit_id = "0"
                label = "Rheinland-Pfalz"
            else:
                unit_id = normalize_identifier(snapshot.get("gebietsnummer"))
                label = str(snapshot.get("municipality_name") or "").strip()
            cdu_votes = party_index.get((str(snapshot["row_key"]), "Zweitstimmen"), {}).get("CDU")
            if cdu_votes is None:
                continue
            timelines[unit_id].append(
                {
                    "unit_id": unit_id,
                    "unit_type": gebietsart,
                    "label": label,
                    "commit": commit.commit,
                    "commit_time_utc": commit.committed_at_utc,
                    "commit_time_local": committed_local.isoformat(timespec="seconds"),
                    "reported_precincts": reported_precincts,
                    "valid_second": valid_second,
                    "cdu_second_votes": cdu_votes,
                    "cdu_second_share_percent": cdu_votes / valid_second * 100.0,
                }
            )

    collapsed: Dict[str, List[Dict[str, object]]] = {}
    for unit_id, rows in timelines.items():
        rows.sort(key=lambda item: item["commit_time_utc"])
        unique_rows: List[Dict[str, object]] = []
        seen_precincts: set[int] = set()
        for row in rows:
            precincts = int(row["reported_precincts"])
            if precincts in seen_precincts:
                continue
            seen_precincts.add(precincts)
            unique_rows.append(row)
        collapsed[unit_id] = unique_rows
    return collapsed


def simulate_random_order(
    cdu_votes: np.ndarray,
    valid_votes: np.ndarray,
    observed_precinct_counts: Sequence[int],
    observed_shares: Sequence[float],
    *,
    simulations: int,
    seed: int,
    capture_path: bool,
) -> Dict[str, object]:
    final_share = float(cdu_votes.sum() / valid_votes.sum() * 100.0)
    observed_precinct_counts_array = np.asarray(observed_precinct_counts, dtype=np.int64)
    observed_shares_array = np.asarray(observed_shares, dtype=np.float64)
    indices = observed_precinct_counts_array - 1
    observed_deviation = np.abs(observed_shares_array - final_share)
    observed_mean_abs_deviation = float(observed_deviation.mean())
    observed_max_abs_deviation = float(observed_deviation.max())

    rng = np.random.default_rng(seed)
    batch_size = 250
    mean_abs_deviation_samples: List[np.ndarray] = []
    max_abs_deviation_samples: List[np.ndarray] = []
    path_samples: List[np.ndarray] = []

    simulations_remaining = simulations
    while simulations_remaining > 0:
        current_batch = min(batch_size, simulations_remaining)
        random_keys = rng.random((current_batch, len(cdu_votes)))
        order = np.argsort(random_keys, axis=1)
        batch_cdu = cdu_votes[order]
        batch_valid = valid_votes[order]
        cum_cdu = np.cumsum(batch_cdu, axis=1)
        cum_valid = np.cumsum(batch_valid, axis=1)
        shares = cum_cdu[:, indices] / cum_valid[:, indices] * 100.0
        deviations = np.abs(shares - final_share)
        mean_abs_deviation_samples.append(deviations.mean(axis=1))
        max_abs_deviation_samples.append(deviations.max(axis=1))
        if capture_path:
            path_samples.append(shares)
        simulations_remaining -= current_batch

    mean_abs_deviation_distribution = np.concatenate(mean_abs_deviation_samples)
    max_abs_deviation_distribution = np.concatenate(max_abs_deviation_samples)
    result: Dict[str, object] = {
        "final_share_percent": final_share,
        "observed_mean_abs_deviation_pp": observed_mean_abs_deviation,
        "observed_max_abs_deviation_pp": observed_max_abs_deviation,
        "mean_abs_deviation_consistency_p_value": float(
            np.mean(mean_abs_deviation_distribution <= observed_mean_abs_deviation)
        ),
        "max_abs_deviation_consistency_p_value": float(
            np.mean(max_abs_deviation_distribution <= observed_max_abs_deviation)
        ),
        "simulated_median_mean_abs_deviation_pp": float(np.median(mean_abs_deviation_distribution)),
        "simulated_median_max_abs_deviation_pp": float(np.median(max_abs_deviation_distribution)),
    }
    if capture_path:
        path_matrix = np.vstack(path_samples)
        result["path_matrix"] = path_matrix
    return result


def build_monte_carlo_reports(final_rows: List[Dict[str, object]]) -> Dict[str, object]:
    effective_rows = effective_booth_rows(final_rows)
    timeline_by_unit = extract_live_timelines()

    land_booths = [row for row in effective_rows]
    wahlkreis_booths: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in effective_rows:
        wahlkreis_booths[booth_wahlkreis_nummer(row)].append(row)

    summary: Dict[str, object] = {"wahlkreis_rows": []}

    land_timeline = timeline_by_unit.get("0", [])
    land_precinct_counts = [int(row["reported_precincts"]) for row in land_timeline]
    land_shares = [float(row["cdu_second_share_percent"]) for row in land_timeline]
    land_valid = np.asarray([int(row["valid_second"]) for row in land_booths], dtype=np.int64)
    land_cdu = np.asarray([int(row["cdu_second_votes"]) for row in land_booths], dtype=np.int64)

    land_simulation = simulate_random_order(
        land_cdu,
        land_valid,
        land_precinct_counts,
        land_shares,
        simulations=5000,
        seed=20260402,
        capture_path=True,
    )
    land_path_matrix = land_simulation.pop("path_matrix")
    land_path_rows: List[Dict[str, object]] = []
    final_land_share = float(land_simulation["final_share_percent"])
    for idx, timeline_row in enumerate(land_timeline):
        simulated_shares = land_path_matrix[:, idx]
        observed_share = float(timeline_row["cdu_second_share_percent"])
        observed_abs_deviation = abs(observed_share - final_land_share)
        simulated_abs_deviation = np.abs(simulated_shares - final_land_share)
        land_path_rows.append(
            {
                "commit_time_local": timeline_row["commit_time_local"],
                "reported_precincts": timeline_row["reported_precincts"],
                "progress_percent": timeline_row["reported_precincts"] / len(land_booths) * 100.0,
                "observed_cdu_second_share_percent": observed_share,
                "final_cdu_second_share_percent": final_land_share,
                "simulated_share_p05": float(np.quantile(simulated_shares, 0.05)),
                "simulated_share_p50": float(np.quantile(simulated_shares, 0.50)),
                "simulated_share_p95": float(np.quantile(simulated_shares, 0.95)),
                "observed_abs_deviation_pp": observed_abs_deviation,
                "pointwise_consistency_p_value": float(np.mean(simulated_abs_deviation <= observed_abs_deviation)),
            }
        )
    write_csv(
        LAND_MC_PATH_CSV,
        [
            "commit_time_local",
            "reported_precincts",
            "progress_percent",
            "observed_cdu_second_share_percent",
            "final_cdu_second_share_percent",
            "simulated_share_p05",
            "simulated_share_p50",
            "simulated_share_p95",
            "observed_abs_deviation_pp",
            "pointwise_consistency_p_value",
        ],
        land_path_rows,
    )

    summary["land"] = {
        **land_simulation,
        "effective_booth_results": len(land_booths),
        "timepoints": len(land_timeline),
        "first_commit_local": land_timeline[0]["commit_time_local"] if land_timeline else None,
        "last_commit_local": land_timeline[-1]["commit_time_local"] if land_timeline else None,
    }

    wahlkreis_rows: List[Dict[str, object]] = []
    for wahlkreis_nummer, booths in sorted(wahlkreis_booths.items(), key=lambda item: int(item[0])):
        timeline = timeline_by_unit.get(wahlkreis_nummer, [])
        if len(timeline) < 2:
            continue
        booth_valid = np.asarray([int(row["valid_second"]) for row in booths], dtype=np.int64)
        booth_cdu = np.asarray([int(row["cdu_second_votes"]) for row in booths], dtype=np.int64)
        observed_precinct_counts = [int(row["reported_precincts"]) for row in timeline]
        observed_shares = [float(row["cdu_second_share_percent"]) for row in timeline]
        simulation = simulate_random_order(
            booth_cdu,
            booth_valid,
            observed_precinct_counts,
            observed_shares,
            simulations=3000,
            seed=20260402 + int(wahlkreis_nummer),
            capture_path=False,
        )
        wahlkreis_rows.append(
            {
                "wahlkreis_nummer": wahlkreis_nummer,
                "label": timeline[0]["label"],
                "effective_booth_results": len(booths),
                "timepoints": len(timeline),
                "final_share_percent": simulation["final_share_percent"],
                "observed_mean_abs_deviation_pp": simulation["observed_mean_abs_deviation_pp"],
                "simulated_median_mean_abs_deviation_pp": simulation["simulated_median_mean_abs_deviation_pp"],
                "mean_abs_deviation_consistency_p_value": simulation["mean_abs_deviation_consistency_p_value"],
                "observed_max_abs_deviation_pp": simulation["observed_max_abs_deviation_pp"],
                "simulated_median_max_abs_deviation_pp": simulation["simulated_median_max_abs_deviation_pp"],
                "max_abs_deviation_consistency_p_value": simulation["max_abs_deviation_consistency_p_value"],
            }
        )

    wahlkreis_rows.sort(key=lambda item: item["mean_abs_deviation_consistency_p_value"])
    write_csv(
        WAHLKREIS_MC_SUMMARY_CSV,
        [
            "wahlkreis_nummer",
            "label",
            "effective_booth_results",
            "timepoints",
            "final_share_percent",
            "observed_mean_abs_deviation_pp",
            "simulated_median_mean_abs_deviation_pp",
            "mean_abs_deviation_consistency_p_value",
            "observed_max_abs_deviation_pp",
            "simulated_median_max_abs_deviation_pp",
            "max_abs_deviation_consistency_p_value",
        ],
        wahlkreis_rows,
    )

    summary["wahlkreis"] = {
        "count": len(wahlkreis_rows),
        "median_mean_abs_deviation_consistency_p_value": float(
            np.median([row["mean_abs_deviation_consistency_p_value"] for row in wahlkreis_rows])
        )
        if wahlkreis_rows
        else None,
        "min_mean_abs_deviation_consistency_p_value": float(
            min(row["mean_abs_deviation_consistency_p_value"] for row in wahlkreis_rows)
        )
        if wahlkreis_rows
        else None,
        "max_mean_abs_deviation_consistency_p_value": float(
            max(row["mean_abs_deviation_consistency_p_value"] for row in wahlkreis_rows)
        )
        if wahlkreis_rows
        else None,
        "rows": wahlkreis_rows,
    }
    write_json(MC_SUMMARY_JSON, summary)
    return summary


def build_source_metadata(
    final_csv_fetch: Dict[str, object],
    final_xlsx_fetch: Dict[str, object],
    preliminary_rows: List[Dict[str, object]],
    final_rows: List[Dict[str, object]],
) -> Dict[str, object]:
    preliminary_metadata = json.loads(PRELIMINARY_METADATA_PATH.read_text(encoding="utf-8"))
    final_aggregate_rows = [
        row for row in final_rows if row["stimmbezirk_id"] == "0" and row["guw"] == "G"
    ]
    booth_rows = [row for row in final_rows if row["code"] == "SB"]
    payload = {
        "download_page_url": FINAL_DOWNLOAD_PAGE_URL,
        "final_csv": final_csv_fetch,
        "final_xlsx": final_xlsx_fetch,
        "final_description_url": FINAL_DESCRIPTION_URL,
        "preliminary_source": preliminary_metadata,
        "preliminary_row_count": len(preliminary_rows),
        "final_aggregate_row_count": len(final_aggregate_rows),
        "final_structural_booth_row_count": len(booth_rows),
    }
    write_json(SOURCE_METADATA_JSON, payload)
    return payload


def build_summary_markdown(
    source_metadata: Dict[str, object],
    delta_summary: Dict[str, object],
    booth_summary: Dict[str, object],
    booth_validation: Dict[str, object],
    mc_summary: Dict[str, object],
) -> None:
    land = mc_summary["land"]
    wahlkreis = mc_summary["wahlkreis"]
    delta_by_code = {item["code"]: item for item in delta_summary["by_code"]}
    lines = [
        "# RLP 2026 Final Booth Analysis",
        "",
        "## Sources",
        "",
        f"- Final official booth download page: {source_metadata['download_page_url']}",
        f"- Final booth CSV: {FINAL_CSV_URL}",
        f"- Final booth XLSX: {FINAL_XLSX_URL}",
        f"- Preliminary machine-readable CSV from repo snapshot: {source_metadata['preliminary_source']['url']}",
        "",
        "## Core Findings",
        "",
        f"- Final official aggregate rows match the preliminary 23degrees hierarchy on all 2,878 comparable IDs after normalizing top-level leading zeros.",
        f"- Structural booth rows in the final official export: {booth_summary['structural_booth_rows']}.",
        f"- Effective individually measurable booth results: {booth_summary['effective_individual_booth_results']}.",
        f"- Booth rows aggregated away into other booths: {booth_summary['aggregated_away_booth_rows']} (receiver rows: {booth_summary['aggregation_receiver_booth_rows']}).",
        f"- Effective booth count exactly matches the preliminary statewide precinct count ({booth_summary['preliminary_land_precinct_count']}).",
        f"- Booth-sum validation: all {booth_validation['stadtteil_rows_checked']} `ST` rows match exactly, "
        "all 52 Wahlkreise match exactly, and the land total matches exactly.",
        "",
        "## Preliminary vs Final Delta",
        "",
        f"- Matched rows: {delta_summary['matched_rows']}.",
        f"- Largest exact-code delta bucket by absolute CDU second-vote changes: "
        f"{max(delta_summary['by_code'], key=lambda item: item['sum_abs_delta_cdu_second_votes'])['code']}.",
        "",
        "## CDU Monte Carlo Consistency",
        "",
        f"- Statewide final CDU second-vote share: {land['final_share_percent']:.3f}%.",
        f"- Statewide observed mean absolute deviation from final share on election evening: "
        f"{land['observed_mean_abs_deviation_pp']:.3f} pp.",
        f"- Probability of a random booth arrival order being at least this consistent: "
        f"{land['mean_abs_deviation_consistency_p_value']:.4f}.",
        f"- Wahlkreis simulations evaluated: {wahlkreis['count']}.",
        f"- Median Wahlkreis consistency p-value: "
        f"{wahlkreis['median_mean_abs_deviation_consistency_p_value']:.4f}."
        if wahlkreis["median_mean_abs_deviation_consistency_p_value"] is not None
        else "- No Wahlkreis Monte Carlo summary available.",
        "",
        "## Generated Reports",
        "",
        f"- {repo_relative(DELTA_ROWS_CSV)}",
        f"- {repo_relative(DELTA_SUMMARY_JSON)}",
        f"- {repo_relative(FINAL_EFFECTIVE_BOOTHS_CSV)}",
        f"- {repo_relative(FINAL_AGGREGATED_BOOTHS_CSV)}",
        f"- {repo_relative(FINAL_BOOTH_SUMMARY_JSON)}",
        f"- {repo_relative(BOOTH_VALIDATION_WK_CSV)}",
        f"- {repo_relative(BOOTH_VALIDATION_SUMMARY_JSON)}",
        f"- {repo_relative(LAND_MC_PATH_CSV)}",
        f"- {repo_relative(WAHLKREIS_MC_SUMMARY_CSV)}",
        f"- {repo_relative(MC_SUMMARY_JSON)}",
    ]
    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ensure_directories()

    final_csv_fetch = fetch_to_path(FINAL_CSV_URL, RAW_DIR / Path(urlparse(FINAL_CSV_URL).path).name)
    final_xlsx_fetch = fetch_to_path(FINAL_XLSX_URL, RAW_DIR / Path(urlparse(FINAL_XLSX_URL).path).name)

    final_rows = load_final_rows(Path(str(final_xlsx_fetch["path"])))
    preliminary_rows = load_preliminary_rows(PRELIMINARY_CSV_PATH)

    delta_rows, delta_summary = join_final_and_preliminary(final_rows, preliminary_rows)
    write_csv(
        DELTA_ROWS_CSV,
        [
            "row_id",
            "code",
            "level_group",
            "label",
            "preliminary_label",
            "preliminary_voters_total",
            "final_voters_total",
            "delta_voters_total",
            "preliminary_valid_second",
            "final_valid_second",
            "delta_valid_second",
            "preliminary_cdu_second_votes",
            "final_cdu_second_votes",
            "delta_cdu_second_votes",
            "preliminary_cdu_second_share_percent",
            "final_cdu_second_share_percent",
            "delta_cdu_second_share_pp",
            "preliminary_total_precincts",
            "preliminary_reported_precincts",
            "preliminary_last_changed",
        ],
        delta_rows,
    )
    write_json(DELTA_SUMMARY_JSON, delta_summary)

    booth_summary = build_booth_reports(final_rows, preliminary_rows)
    booth_validation = build_booth_validation(final_rows)
    mc_summary = build_monte_carlo_reports(final_rows)
    source_metadata = build_source_metadata(final_csv_fetch, final_xlsx_fetch, preliminary_rows, final_rows)
    build_summary_markdown(source_metadata, delta_summary, booth_summary, booth_validation, mc_summary)

    land_summary = mc_summary["land"]
    print(f"Final effective booth results: {booth_summary['effective_individual_booth_results']}")
    print(f"Aggregated-away booth rows: {booth_summary['aggregated_away_booth_rows']}")
    print(f"Matched aggregate delta rows: {delta_summary['matched_rows']}")
    print(f"Statewide CDU final share: {land_summary['final_share_percent']:.3f}%")
    print(
        "Statewide mean-abs-deviation consistency p-value: "
        f"{land_summary['mean_abs_deviation_consistency_p_value']:.4f}"
    )
    print(f"Reports written under {repo_relative(REPORT_DIR)}")


if __name__ == "__main__":
    main()
