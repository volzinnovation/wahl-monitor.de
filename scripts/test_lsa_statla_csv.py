#!/usr/bin/env python3
"""Regression test for the published Sachsen-Anhalt 2026 CSV schema."""

from __future__ import annotations

import sys

sys.path.insert(0, "scripts")

import poll_election_core as core


HEADER = ";".join(
    [
        "Ergebnisart",
        "Datum",
        "Uhrzeit",
        "Soll.Wahlbezirke",
        "Ist.Wahlbezirke",
        "Satzart",
        "Schlüsselnummer",
        "Name",
        "Wahllokal",
        "A.Wahlberechtigte",
        "B.Wähler",
        "E.Ungültige.Zweitstimmen",
        "F.Gültige.Zweitstimmen",
        "F01.CDU",
        "F02.AfD",
        "C.Ungültige.Erststimmen",
        "D.Gültige.Erststimmen",
        "D01.CDU",
        "D02.AfD",
        "Gewählt im Wahlkreis",
    ]
)


def row(area_type: str, number: str, name: str, polling_type: str, cdu: str = "0") -> str:
    values = [
        "L",
        "",
        "",
        "10",
        "0",
        area_type,
        number,
        name,
        polling_type,
        "100",
        "0",
        "0",
        "0",
        cdu,
        "0",
        "0",
        "0",
        cdu,
        "0",
        "",
    ]
    return ";".join(values)


def main() -> int:
    sample = "\n".join(
        [
            HEADER,
            row("LAN", "15", "Sachsen-Anhalt", "U", "2"),
            row("LAN", "15", "Sachsen-Anhalt", "B", "1"),
            row("LAN", "15", "Sachsen-Anhalt", "", "3"),
            row("GEM", "15001000", "Dessau-Roßlau, Stadt", "", "3"),
        ]
    )
    assert core.looks_like_statla_csv(sample)
    snapshots, party_rows = core.parse_statla_csv_rows(sample)
    assert len(snapshots) == 2
    assert {row["gebietsart"] for row in snapshots} == {"LAND", "GEMEINDE"}
    assert next(row for row in snapshots if row["gebietsart"] == "LAND")["valid_votes_zweit"] == 0
    assert len(party_rows) == 8
    assert {row["party_name"] for row in party_rows} == {"CDU", "AfD"}
    assert core.statla_snapshot_shape_stats(snapshots) == {
        "row_count": 2,
        "ags_count": 1,
        "wahlkreis_count": 0,
    }
    print("LSA StatLA CSV schema test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
