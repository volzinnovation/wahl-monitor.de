#!/usr/bin/env python3
"""Normalize the official Sachsen-Anhalt 2021 result downloads.

The result portal publishes one CSV for the land, districts and Wahlkreise and
another CSV for municipalities.  This script turns those wide files into
small, stable reference tables used by the 2026 pre-election site.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable


DEFAULT_LAND_URL = "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/erg/csv/lt21dat1.csv"
DEFAULT_MUNICIPALITY_URL = "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/erg/csv/lt21dat2.csv"
DEFAULT_SEAT_URL = "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/sitz/sitzverteilung.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--land-csv", type=Path, required=True)
    parser.add_argument("--municipality-csv", type=Path, required=True)
    parser.add_argument("--seats-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def read_portal_csv(path: Path) -> list[dict[str, str]]:
    raw = path.read_bytes()
    text = raw.decode("cp1252")
    return list(csv.DictReader(text.splitlines(), delimiter=";"))


def read_seats(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig")
    return list(csv.DictReader(text.splitlines(), delimiter=";"))


def as_int(value: Any) -> int:
    text = str(value or "").strip().replace(".", "")
    if not text:
        return 0
    text = re.sub(r"[^0-9-]", "", text)
    return int(text or 0)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def area_level(record_type: str) -> str:
    return {
        "LAN": "LAND",
        "KRS": "KREIS",
        "WKR": "WAHLKREIS",
        "GEM": "GEMEINDE",
    }.get(record_type, record_type)


def area_row(row: dict[str, str]) -> dict[str, Any]:
    record_type = str(row.get("Satzart") or "").strip()
    return {
        "area_level": area_level(record_type),
        "area_id": str(row.get("Schlüsselnummer") or "").strip(),
        "area_name": str(row.get("Name") or "").strip(),
        "eligible_voters": as_int(row.get("A - Wahlberechtigte")),
        "voters": as_int(row.get("B - Wähler")),
        "invalid_first_votes": as_int(row.get("C - Ungültige Erststimmen")),
        "valid_first_votes": as_int(row.get("D - Gültige Erststimmen")),
        "invalid_second_votes": as_int(row.get("E - Ungültige Zweitstimmen")),
        "valid_second_votes": as_int(row.get("F - Gültige Zweitstimmen")),
    }


def party_rows(row: dict[str, str]) -> Iterable[dict[str, Any]]:
    record_type = str(row.get("Satzart") or "").strip()
    level = area_level(record_type)
    area_id = str(row.get("Schlüsselnummer") or "").strip()
    area_name = str(row.get("Name") or "").strip()
    for prefix, vote_type, valid_key in (
        ("D", "Erststimmen", "D - Gültige Erststimmen"),
        ("F", "Zweitstimmen", "F - Gültige Zweitstimmen"),
    ):
        valid_votes = as_int(row.get(valid_key))
        for key, raw_label in row.items():
            if not re.fullmatch(rf"{prefix}\d+\s+-\s+.*", key):
                continue
            party_code = key.split(" - ", 1)[0].strip()
            party_name = key.split(" - ", 1)[1].strip()
            votes = as_int(raw_label)
            yield {
                "area_level": level,
                "area_id": area_id,
                "area_name": area_name,
                "vote_type": vote_type,
                "party_code": party_code,
                "party_name": party_name,
                "votes": votes,
                "valid_votes": valid_votes,
                "share_percent": round((votes / valid_votes) * 100, 4) if valid_votes else 0.0,
            }


def winning_party(rows: list[dict[str, Any]], vote_type: str) -> tuple[str, int, float]:
    totals: dict[str, int] = {}
    valid_votes = 0
    for row in rows:
        if row["vote_type"] != vote_type:
            continue
        party = str(row["party_name"])
        totals[party] = totals.get(party, 0) + int(row["votes"])
        valid_votes = int(row["valid_votes"])
    if not totals:
        return "", 0, 0.0
    party, votes = max(totals.items(), key=lambda item: (item[1], item[0]))
    return party, votes, round((votes / valid_votes) * 100, 4) if valid_votes else 0.0


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    land_rows = read_portal_csv(args.land_csv)
    municipality_rows = read_portal_csv(args.municipality_csv)
    all_rows = land_rows + municipality_rows

    write_csv(
        args.output_dir / "areas.csv",
        [
            "area_level",
            "area_id",
            "area_name",
            "eligible_voters",
            "voters",
            "invalid_first_votes",
            "valid_first_votes",
            "invalid_second_votes",
            "valid_second_votes",
        ],
        (area_row(row) for row in all_rows),
    )

    normalized_party_rows = [party_row for row in all_rows for party_row in party_rows(row)]
    write_csv(
        args.output_dir / "party_results.csv",
        [
            "area_level",
            "area_id",
            "area_name",
            "vote_type",
            "party_code",
            "party_name",
            "votes",
            "valid_votes",
            "share_percent",
        ],
        normalized_party_rows,
    )

    wahlkreis_summary = []
    for row in land_rows:
        if str(row.get("Satzart") or "").strip() != "WKR":
            continue
        area = area_row(row)
        row_party_rows = [
            party_row
            for party_row in normalized_party_rows
            if party_row["area_level"] == "WAHLKREIS" and party_row["area_id"] == area["area_id"]
        ]
        first = winning_party(row_party_rows, "Erststimmen")
        second = winning_party(row_party_rows, "Zweitstimmen")
        wahlkreis_summary.append(
            {
                "wahlkreisnummer": area["area_id"],
                "wahlkreisname": area["area_name"],
                "valid_first_votes": area["valid_first_votes"],
                "winner_first": first[0],
                "winner_first_votes": first[1],
                "winner_first_share_percent": first[2],
                "valid_second_votes": area["valid_second_votes"],
                "winner_second": second[0],
                "winner_second_votes": second[1],
                "winner_second_share_percent": second[2],
            }
        )
    write_csv(
        args.output_dir / "wahlkreis_summary.csv",
        [
            "wahlkreisnummer",
            "wahlkreisname",
            "valid_first_votes",
            "winner_first",
            "winner_first_votes",
            "winner_first_share_percent",
            "valid_second_votes",
            "winner_second",
            "winner_second_votes",
            "winner_second_share_percent",
        ],
        wahlkreis_summary,
    )

    seat_rows = []
    for row in read_seats(args.seats_csv):
        seat_rows.append({key.strip(): str(value or "").strip() for key, value in row.items()})
    write_csv(
        args.output_dir / "seats.csv",
        ["Listennummer", "Partei", "Sitze gesamt", "Kreiswahlvorschlaege", "Landeswahlvorschlaege"],
        seat_rows,
    )

    manifest = {
        "election": "Landtagswahl Sachsen-Anhalt 2021",
        "election_date": "2021-06-06",
        "sources": {
            "land_and_wahlkreise": {
                "url": DEFAULT_LAND_URL,
                "sha256": sha256(args.land_csv),
            },
            "municipalities": {
                "url": DEFAULT_MUNICIPALITY_URL,
                "sha256": sha256(args.municipality_csv),
            },
            "seat_distribution": {
                "url": DEFAULT_SEAT_URL,
                "sha256": sha256(args.seats_csv),
            },
        },
        "counts": {
            "land_and_wahlkreis_rows": len(land_rows),
            "municipality_rows": len(municipality_rows),
            "party_result_rows": len(normalized_party_rows),
            "wahlkreis_summary_rows": len(wahlkreis_summary),
        },
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest["counts"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
