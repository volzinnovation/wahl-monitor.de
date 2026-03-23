#!/usr/bin/env python3
"""Official interim RLP seat summary from the Wahlnachtanalyse."""

from __future__ import annotations

from typing import Any, Dict, List


RLP_INTERIM_BASE_SEATS = 101
RLP_INTERIM_TOTAL_SEATS = 105
RLP_INTERIM_EXTRA_SEATS = RLP_INTERIM_TOTAL_SEATS - RLP_INTERIM_BASE_SEATS
RLP_INTERIM_SOURCE_URL = (
    "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/PDF/Wahlnachtanalyse_LW2026.pdf"
)

RLP_INTERIM_SEAT_ROWS: List[Dict[str, Any]] = [
    {
        "party": "CDU",
        "seats": 39,
        "direct_seats": 39,
        "list_seats": 0,
        "share_percent": 30.9592,
        "color": "#2D3C4B",
    },
    {
        "party": "SPD",
        "seats": 32,
        "direct_seats": 13,
        "list_seats": 19,
        "share_percent": 25.9188,
        "color": "#E3000F",
    },
    {
        "party": "AfD",
        "seats": 24,
        "direct_seats": 0,
        "list_seats": 24,
        "share_percent": 19.4658,
        "color": "#00CCFF",
    },
    {
        "party": "GRÜNE",
        "seats": 10,
        "direct_seats": 0,
        "list_seats": 10,
        "share_percent": 7.8921,
        "color": "#008939",
    },
]


def official_interim_seat_summary() -> Dict[str, Any]:
    return {
        "title": "Sitzberechnung",
        "subtitle": (
            "Vorläufiges amtliches Endergebnis laut Wahlnachtanalyse des "
            "Statistischen Landesamts Rheinland-Pfalz vom 23. März 2026."
        ),
        "base_seats": RLP_INTERIM_BASE_SEATS,
        "total_seats": RLP_INTERIM_TOTAL_SEATS,
        "extra_seats": RLP_INTERIM_EXTRA_SEATS,
        "rows": [
            {
                "party": str(row["party"]),
                "seats": int(row["seats"]),
                "direct_seats": int(row["direct_seats"]),
                "list_seats": int(row["list_seats"]),
                "share_percent": float(row["share_percent"]),
            }
            for row in RLP_INTERIM_SEAT_ROWS
        ],
        "footnote": (
            "Vorläufiges amtliches Endergebnis: CDU mit 2 Überhangmandaten, "
            "SPD und AfD mit je 1 Ausgleichsmandat."
        ),
        "source_url": RLP_INTERIM_SOURCE_URL,
    }
