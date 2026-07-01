#!/usr/bin/env python3
"""Parse and interpret the official RLP 2026 Wahlkreis structure workbook."""

from __future__ import annotations

import math
import re
import statistics
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping
from xml.etree import ElementTree as ET


DEFAULT_STRUCTURE_WORKBOOK_URL = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/LW_2026_Strukturbericht_Wahlkreise.xlsx"
)

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
CELL_REF_RE = re.compile(r"([A-Z]+)")
WAHLKREIS_VALUE_START = 2
WAHLKREIS_VALUE_END = 54


@dataclass(frozen=True)
class MetricSpec:
    key: str
    label: str
    unit: str
    row: int
    value_row: int | None = None
    percent: bool = False
    decimals: int = 1


# 2021 election rows already live elsewhere in the dashboard, so this export
# focuses on the structure part of the workbook.
STRUCTURE_METRICS: List[MetricSpec] = [
    MetricSpec("area_km2", "Fläche", "km²", 20),
    MetricSpec("population_total", "Bevölkerung", "Einwohner", 23, decimals=0),
    MetricSpec("population_male", "Bevölkerung männlich", "Einwohner", 24, decimals=0),
    MetricSpec("population_female", "Bevölkerung weiblich", "Einwohner", 25, decimals=0),
    MetricSpec("share_u18_percent", "Unter 18 Jahre", "%", 26, percent=True),
    MetricSpec("share_18_25_percent", "18 bis 25 Jahre", "%", 27, percent=True),
    MetricSpec("share_25_40_percent", "25 bis 40 Jahre", "%", 28, percent=True),
    MetricSpec("share_40_60_percent", "40 bis 60 Jahre", "%", 29, percent=True),
    MetricSpec("share_60_80_percent", "60 bis 80 Jahre", "%", 30, percent=True),
    MetricSpec("share_80_plus_percent", "80 Jahre und älter", "%", 31, percent=True),
    MetricSpec("youth_dependency_ratio", "Jugendquotient", "", 33),
    MetricSpec("old_age_dependency_ratio", "Altenquotient", "", 34),
    MetricSpec("population_density_per_km2", "Bevölkerungsdichte", "EW/km²", 36, value_row=37),
    MetricSpec("population_growth_2014_2024_percent", "Bevölkerungsentwicklung 2014 bis 2024", "%", 40, percent=True),
    MetricSpec("population_forecast_2020_2040_percent", "Bevölkerungsprognose 2020 bis 2040", "%", 41, percent=True),
    MetricSpec("foreign_share_percent", "Ausländeranteil", "%", 43, percent=True),
    MetricSpec("evangelical_share_percent", "Evangelische Bevölkerung", "%", 45, value_row=46, percent=True),
    MetricSpec("catholic_share_percent", "Katholische Bevölkerung", "%", 48, value_row=49, percent=True),
    MetricSpec("childcare_rate_u3_percent", "U3-Betreuungsquote", "%", 56, percent=True),
    MetricSpec("general_schools_count", "Allgemeinbildende Schulen", "Anzahl", 62, decimals=0),
    MetricSpec("general_students_total", "Schüler/-innen allgemeinbildende Schulen", "Anzahl", 63, decimals=0),
    MetricSpec("general_students_primary_share_percent", "Primarbereich", "%", 66, percent=True),
    MetricSpec("general_students_secondary_i_share_percent", "Sekundarbereich I", "%", 67, percent=True),
    MetricSpec("general_students_secondary_ii_share_percent", "Sekundarbereich II", "%", 68, percent=True),
    MetricSpec("vocational_units_count", "Berufsbildende Verwaltungseinheiten", "Anzahl", 71, decimals=0),
    MetricSpec("vocational_students_total", "Schüler/-innen berufsbildende Schulen", "Anzahl", 72, decimals=0),
    MetricSpec("vocational_berufsschule_share_percent", "Anteil Berufsschulen", "%", 73, percent=True),
    MetricSpec("employment_total", "Sozialversicherungspflichtig Beschäftigte", "Anzahl", 80, decimals=0),
    MetricSpec("employment_agriculture_share_percent", "Beschäftigte Land- und Forstwirtschaft", "%", 81, percent=True),
    MetricSpec("employment_manufacturing_share_percent", "Beschäftigte Produzierendes Gewerbe", "%", 82, percent=True),
    MetricSpec("employment_trade_transport_share_percent", "Beschäftigte Handel/Gastgewerbe/Verkehr", "%", 83, percent=True),
    MetricSpec("employment_services_share_percent", "Beschäftigte Sonstige Dienstleistungen", "%", 84, percent=True),
    MetricSpec("commuter_balance", "Pendlersaldo", "Anzahl", 86, decimals=0),
    MetricSpec("debt_total_per_capita_eur", "Schulden gesamt je Einwohner", "EUR", 93),
    MetricSpec("debt_public_sector_per_capita_eur", "Schulden öffentlicher Bereich je Einwohner", "EUR", 95),
]

STRUCTURE_PROFILE_METADATA: Dict[str, Dict[str, str]] = {
    "urban_services": {
        "label": "Verdichtete Dienstleistungsräume",
        "color": "#0f766e",
        "description": "Hohe Dichte, hoher Dienstleistungsanteil und überdurchschnittliche Urbanität.",
    },
    "growth_belt": {
        "label": "Junge Wachstumsräume",
        "color": "#65a30d",
        "description": "Überdurchschnittliche Dynamik bei Wachstum, Kinderanteil und Kinderbetreuung.",
    },
    "industrial_space": {
        "label": "Industriell geprägte Arbeitsräume",
        "color": "#b45309",
        "description": "Hoher Industrieanteil und eher arbeitsplatzorientierte Wirtschaftsstruktur.",
    },
    "aging_space": {
        "label": "Ältere Flächenräume",
        "color": "#7c2d12",
        "description": "Hoher Altenquotient, ältere Bevölkerung und schwächere Bevölkerungsdynamik.",
    },
}

CSV_FIELDNAMES: List[str] = [
    "wahlkreisnummer",
    "wahlkreisname",
    "structure_profile_key",
    "structure_profile_label",
    "structure_profile_color",
    "structure_summary",
    "urbanity_score",
    "growth_score",
    "industrial_score",
    "aging_score",
    "commuter_balance_per_1000",
]
CSV_FIELDNAMES.extend(spec.key for spec in STRUCTURE_METRICS)


def display_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def canonical_wahlkreis_name(value: Any) -> str:
    text = display_text(value)
    text = re.sub(r",?\s*wahlkreis$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*/\s*", " / ", text)
    return text.casefold()


def normalize_wahlkreis_nummer(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    if not text:
        return ""
    return str(int(text))


def column_index_from_ref(ref: str) -> int:
    match = CELL_REF_RE.match(ref)
    if not match:
        return 0
    value = 0
    for char in match.group(1):
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value - 1


def load_sheet_rows(path: Path, sheet_name: str = "Strukturbericht") -> List[List[str]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in root.findall(f"{NS}si"):
                shared_strings.append("".join(node.text or "" for node in item.iter(f"{NS}t")))

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in relationships}

        worksheet_node = workbook.find(f"{NS}sheets/{NS}sheet[@name='{sheet_name}']")
        if worksheet_node is None:
            worksheet_node = workbook.find(f"{NS}sheets/{NS}sheet")
        if worksheet_node is None:
            raise RuntimeError(f"No worksheet found in {path}")

        rel_id = worksheet_node.attrib[f"{REL_NS}id"]
        worksheet_path = "xl/" + rel_map[rel_id]
        worksheet = ET.fromstring(archive.read(worksheet_path))

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


def parse_numeric_cell(raw_value: Any, *, percent: bool = False) -> float | None:
    text = re.sub(r"\s+", "", str(raw_value or ""))
    if not text:
        return None
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        value = float(text)
    except ValueError:
        return None
    if percent:
        while 100 < value < 1000 and not value.is_integer() and "e" not in text.lower():
            value /= 10
    return value


def _z_scores(values: Iterable[float | None]) -> List[float]:
    materialized = list(values)
    series = [value for value in materialized if value is not None]
    if not series:
        return [0.0 for _ in materialized]
    mean = statistics.mean(series)
    stddev = statistics.pstdev(series) or 1.0
    return [0.0 if value is None else (value - mean) / stddev for value in materialized]


def _format_decimal(value: Any, decimals: int = 1) -> str:
    if value is None:
        return ""
    number = float(value)
    if decimals == 0:
        return f"{int(round(number)):,}".replace(",", ".")
    return f"{number:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_percent(value: Any, decimals: int = 1, *, signed: bool = False) -> str:
    if value is None:
        return ""
    number = float(value)
    if signed:
        sign = "+" if number > 0 else ""
        return f"{sign}{_format_decimal(number, decimals)} %"
    return f"{_format_decimal(number, decimals)} %"


def build_profile_summary(row: Mapping[str, Any]) -> str:
    key = str(row.get("structure_profile_key") or "")
    density = _format_decimal(row.get("population_density_per_km2"), 0)
    services = _format_percent(row.get("employment_services_share_percent"), 1)
    foreign_share = _format_percent(row.get("foreign_share_percent"), 1)
    growth = _format_percent(row.get("population_growth_2014_2024_percent"), 1, signed=True)
    forecast = _format_percent(row.get("population_forecast_2020_2040_percent"), 1, signed=True)
    youth = _format_percent(row.get("share_u18_percent"), 1)
    childcare = _format_percent(row.get("childcare_rate_u3_percent"), 1)
    manufacturing = _format_percent(row.get("employment_manufacturing_share_percent"), 1)
    aging = _format_decimal(row.get("old_age_dependency_ratio"), 1)
    eighty_plus = _format_percent(row.get("share_80_plus_percent"), 1)
    commuter = _format_decimal(row.get("commuter_balance_per_1000"), 1)

    if key == "urban_services":
        return (
            f"Dichte ({density} EW/km²), Dienstleistungsanteil ({services}) und Ausländeranteil "
            f"({foreign_share}) liegen klar über dem Landesmittel."
        )
    if key == "growth_belt":
        return (
            f"Bevölkerung wächst überdurchschnittlich ({growth} seit 2014, {forecast} bis 2040) "
            f"bei hohem U18-Anteil ({youth}) und solider U3-Betreuung ({childcare})."
        )
    if key == "industrial_space":
        return (
            f"Das Profil ist arbeitsplatz- und industrieorientiert: Produzierendes Gewerbe {manufacturing}, "
            f"Pendlersaldo {commuter} je 1.000 Einwohner."
        )
    if key == "aging_space":
        return (
            f"Hoher Altenquotient ({aging}) und überdurchschnittlicher 80+-Anteil ({eighty_plus}) "
            f"treffen auf verhaltene Bevölkerungsdynamik."
        )
    return ""


def derive_structure_rows(
    path: Path,
    official_wahlkreisnummer_by_name: Mapping[str, str] | None = None,
) -> List[Dict[str, Any]]:
    rows = load_sheet_rows(path)
    if len(rows) < 95:
        raise RuntimeError(f"Unexpected workbook shape in {path}")

    raw_wahlkreisnummern = [normalize_wahlkreis_nummer(value) for value in rows[2][WAHLKREIS_VALUE_START:WAHLKREIS_VALUE_END]]
    wahlkreisnamen = [display_text(value) for value in rows[3][WAHLKREIS_VALUE_START:WAHLKREIS_VALUE_END]]
    if len(raw_wahlkreisnummern) != 52 or len(wahlkreisnamen) != 52:
        raise RuntimeError(f"Expected 52 Wahlkreis columns, found {len(raw_wahlkreisnummern)}")

    wahlkreisnummern: List[str] = []
    unmatched_names: List[str] = []
    for raw_wk, name in zip(raw_wahlkreisnummern, wahlkreisnamen):
        official_wk = ""
        if official_wahlkreisnummer_by_name:
            official_wk = normalize_wahlkreis_nummer(
                official_wahlkreisnummer_by_name.get(canonical_wahlkreis_name(name))
            )
            if not official_wk:
                unmatched_names.append(name)
        wahlkreisnummern.append(official_wk or raw_wk)

    if official_wahlkreisnummer_by_name and unmatched_names:
        raise RuntimeError(
            "Could not match workbook Wahlkreis names to official IDs: "
            + ", ".join(sorted(unmatched_names))
        )

    output_rows: List[Dict[str, Any]] = []
    for wk, name in zip(wahlkreisnummern, wahlkreisnamen):
        if not wk:
            continue
        output_rows.append(
            {
                "wahlkreisnummer": wk,
                "wahlkreisname": name,
            }
        )

    if len(output_rows) != 52:
        raise RuntimeError(f"Expected 52 Wahlkreise, found {len(output_rows)}")

    for spec in STRUCTURE_METRICS:
        raw_values = rows[(spec.value_row or spec.row) - 1][WAHLKREIS_VALUE_START:WAHLKREIS_VALUE_END]
        parsed_values = [parse_numeric_cell(value, percent=spec.percent) for value in raw_values]
        if len(parsed_values) != len(output_rows):
            raise RuntimeError(f"Metric row {spec.row} does not match Wahlkreis count")
        for row, parsed_value in zip(output_rows, parsed_values):
            row[spec.key] = parsed_value

    for row in output_rows:
        population = row.get("population_total")
        commuter_balance = row.get("commuter_balance")
        if population in (None, 0) or commuter_balance is None:
            row["commuter_balance_per_1000"] = None
        else:
            row["commuter_balance_per_1000"] = float(commuter_balance) / float(population) * 1000

    density_log = [
        math.log1p(float(row["population_density_per_km2"])) if row.get("population_density_per_km2") is not None else None
        for row in output_rows
    ]
    z_density = _z_scores(density_log)
    z_foreign = _z_scores(row.get("foreign_share_percent") for row in output_rows)
    z_services = _z_scores(row.get("employment_services_share_percent") for row in output_rows)
    z_commuter = _z_scores(row.get("commuter_balance_per_1000") for row in output_rows)
    z_growth = _z_scores(row.get("population_growth_2014_2024_percent") for row in output_rows)
    z_forecast = _z_scores(row.get("population_forecast_2020_2040_percent") for row in output_rows)
    z_u18 = _z_scores(row.get("share_u18_percent") for row in output_rows)
    z_childcare = _z_scores(row.get("childcare_rate_u3_percent") for row in output_rows)
    z_manufacturing = _z_scores(row.get("employment_manufacturing_share_percent") for row in output_rows)
    z_aging = _z_scores(row.get("old_age_dependency_ratio") for row in output_rows)
    z_eighty_plus = _z_scores(row.get("share_80_plus_percent") for row in output_rows)

    for index, row in enumerate(output_rows):
        urbanity_score = z_density[index] + z_foreign[index] + z_services[index] + z_commuter[index]
        growth_score = z_growth[index] + z_forecast[index] + z_u18[index] + z_childcare[index]
        industrial_score = z_manufacturing[index] - z_services[index] + 0.5 * z_commuter[index]
        aging_score = z_aging[index] + z_eighty_plus[index] - z_u18[index] - z_growth[index]

        profile_scores = {
            "urban_services": urbanity_score,
            "growth_belt": growth_score,
            "industrial_space": industrial_score,
            "aging_space": aging_score,
        }
        profile_key = max(profile_scores, key=profile_scores.get)
        profile_meta = STRUCTURE_PROFILE_METADATA[profile_key]

        row["urbanity_score"] = round(urbanity_score, 3)
        row["growth_score"] = round(growth_score, 3)
        row["industrial_score"] = round(industrial_score, 3)
        row["aging_score"] = round(aging_score, 3)
        row["structure_profile_key"] = profile_key
        row["structure_profile_label"] = profile_meta["label"]
        row["structure_profile_color"] = profile_meta["color"]
        row["structure_summary"] = build_profile_summary(row)

    output_rows.sort(key=lambda item: int(str(item["wahlkreisnummer"])))
    return output_rows


def structure_rows_by_wk(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {normalize_wahlkreis_nummer(row.get("wahlkreisnummer")): dict(row) for row in rows if normalize_wahlkreis_nummer(row.get("wahlkreisnummer"))}


def enrich_geojson_with_structure(
    geojson: Mapping[str, Any],
    structure_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    output = {
        "type": geojson.get("type", "FeatureCollection"),
        "features": [],
    }
    by_wk = structure_rows_by_wk(structure_rows)
    for feature in geojson.get("features", []) or []:
        feature_copy = dict(feature)
        props = dict(feature.get("properties") or {})
        wk = normalize_wahlkreis_nummer(props.get("Nummer") or props.get("wahlkreisnummer"))
        structure = by_wk.get(wk)
        if structure:
            props.update(structure)
        feature_copy["properties"] = props
        output["features"].append(feature_copy)
    return output
