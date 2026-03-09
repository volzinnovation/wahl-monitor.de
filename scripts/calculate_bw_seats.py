#!/usr/bin/env python3
"""
Calculate Baden-Wuerttemberg Landtag seats from the official Statistik BW CSV.

Rules implemented for the 2026 law:
- direct mandates come from the 70 Wahlkreise via first votes
- only parties at or above 5% of valid second votes join the proportional allocation
- proportional seats are allocated with Sainte-Lague/Schepers
- if direct mandates exceed proportional seats, the parliament size is increased
  until all overhang mandates are fully compensated

The official special case for successful direct winners without a corresponding
second-vote list cannot be reconstructed from aggregate public CSV data. The
script therefore aborts if that case occurs.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import ssl
from dataclasses import dataclass
from fractions import Fraction
from typing import Dict, Iterable, List, Tuple
from urllib import request

from poll_election_core import STATLA_PARTY_CODEBOOK, load_config, set_active_election


NOMINAL_SEATS = 120
THRESHOLD = Fraction(5, 100)


@dataclass(frozen=True)
class PartySeatRow:
    party: str
    second_votes: int
    second_vote_share: float
    direct_mandates: int
    total_seats: int
    list_seats: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--election-key", default="2026-bw", help="Election key from config/<key>.json")
    parser.add_argument("--csv-path", help="Read a local Statistik BW CSV instead of fetching the live URL")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a text table")
    return parser.parse_args()


def fetch_csv_text(url: str) -> str:
    try:
        with request.urlopen(url, timeout=300) as response:
            return response.read().decode("utf-8-sig")
    except Exception:
        context = ssl._create_unverified_context()
        with request.urlopen(url, timeout=300, context=context) as response:
            return response.read().decode("utf-8-sig")


def read_statla_rows(csv_text: str) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    return list(reader)


def parse_int(value: str | None) -> int:
    if value is None:
        return 0
    raw = value.strip()
    if not raw:
        return 0
    return int(raw)


def party_code_map(vote_type: str) -> Dict[str, str]:
    return {code: party for code, party in STATLA_PARTY_CODEBOOK[vote_type]}


def get_land_row(rows: Iterable[Dict[str, str]]) -> Dict[str, str]:
    for row in rows:
        if (row.get("Gebietsart") or "").strip() == "LAND":
            return row
    raise ValueError("LAND row missing from Statistik BW CSV")


def get_wahlkreis_rows(rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    wahlkreis_rows = [row for row in rows if (row.get("Gebietsart") or "").strip() == "WAHLKREIS"]
    if len(wahlkreis_rows) != 70:
        raise ValueError(f"Expected 70 Wahlkreis rows, found {len(wahlkreis_rows)}")
    return wahlkreis_rows


def second_vote_totals(land_row: Dict[str, str]) -> Tuple[Dict[str, int], int]:
    code_to_party = party_code_map("Zweitstimmen")
    totals: Dict[str, int] = {}
    for code, party in code_to_party.items():
        totals[party] = parse_int(land_row.get(code))
    valid_votes = parse_int(land_row.get("Zweitstimmen gueltige (F)"))
    return totals, valid_votes


def direct_winner_party(wahlkreis_row: Dict[str, str]) -> str:
    code_to_party = party_code_map("Erststimmen")
    tallies = [(parse_int(wahlkreis_row.get(code)), party) for code, party in code_to_party.items()]
    tallies.sort(reverse=True)
    if len(tallies) < 2:
        raise ValueError("Missing first-vote tallies in Wahlkreis row")
    if tallies[0][0] == 0:
        number = wahlkreis_row.get("Wahlkreisnummer") or "?"
        raise ValueError(f"No first-vote data for Wahlkreis {number}")
    if tallies[0][0] == tallies[1][0]:
        number = wahlkreis_row.get("Wahlkreisnummer") or "?"
        raise ValueError(f"First-vote tie in Wahlkreis {number}; official lot result required")
    return tallies[0][1]


def direct_mandates(wahlkreis_rows: Iterable[Dict[str, str]]) -> Dict[str, int]:
    mandates: Dict[str, int] = {}
    for row in wahlkreis_rows:
        party = direct_winner_party(row)
        mandates[party] = mandates.get(party, 0) + 1
    return mandates


def qualifying_parties(second_votes: Dict[str, int], valid_votes: int) -> Dict[str, int]:
    threshold_votes = Fraction(valid_votes) * THRESHOLD
    return {
        party: votes
        for party, votes in second_votes.items()
        if votes and Fraction(votes) >= threshold_votes
    }


def allocate_sainte_lague(votes: Dict[str, int], seat_count: int) -> Dict[str, int]:
    quotients: List[Tuple[Fraction, str]] = []
    for party, party_votes in votes.items():
        for seat_index in range(seat_count):
            quotients.append((Fraction(party_votes, 1) / Fraction(2 * seat_index + 1, 2), party))
    quotients.sort(key=lambda item: (item[0], item[1]), reverse=True)

    if seat_count < len(quotients) and quotients[seat_count - 1][0] == quotients[seat_count][0]:
        raise ValueError("Exact Sainte-Lague tie at the final seat boundary; official lot result required")

    allocation = {party: 0 for party in votes}
    for quotient, party in quotients[:seat_count]:
        if quotient <= 0:
            continue
        allocation[party] += 1
    return allocation


def balanced_allocation(votes: Dict[str, int], direct_by_party: Dict[str, int]) -> Tuple[int, Dict[str, int]]:
    total_seats = NOMINAL_SEATS
    while True:
        allocation = allocate_sainte_lague(votes, total_seats)
        if all(allocation.get(party, 0) >= direct_by_party.get(party, 0) for party in votes):
            return total_seats, allocation
        total_seats += 1


def ensure_supported_edge_cases(
    direct_by_party: Dict[str, int],
    qualifying: Dict[str, int],
    second_votes: Dict[str, int],
) -> None:
    unsupported = [
        party
        for party, wins in direct_by_party.items()
        if wins > 0 and party not in second_votes
    ]
    if unsupported:
        joined = ", ".join(sorted(unsupported))
        raise ValueError(
            "Successful direct winners without a second-vote list require ballot-level "
            f"second-vote exclusions that the aggregate CSV does not expose: {joined}"
        )

    non_qualifying_winners = [
        party
        for party, wins in direct_by_party.items()
        if wins > 0 and party not in qualifying
    ]
    if non_qualifying_winners:
        joined = ", ".join(sorted(non_qualifying_winners))
        raise ValueError(
            "Successful direct winners below the 5% threshold need additional legal handling "
            f"that is not represented explicitly in the published aggregate CSV: {joined}"
        )


def seat_rows(
    qualifying_votes: Dict[str, int],
    valid_second_votes: int,
    direct_by_party: Dict[str, int],
    allocation: Dict[str, int],
) -> List[PartySeatRow]:
    rows: List[PartySeatRow] = []
    for party, votes in qualifying_votes.items():
        total_seats = allocation[party]
        direct = direct_by_party.get(party, 0)
        rows.append(
            PartySeatRow(
                party=party,
                second_votes=votes,
                second_vote_share=(votes / valid_second_votes) * 100,
                direct_mandates=direct,
                total_seats=total_seats,
                list_seats=total_seats - direct,
            )
        )
    rows.sort(key=lambda row: (-row.total_seats, -row.second_votes, row.party))
    return rows


def build_output(
    *,
    config_url: str,
    land_row: Dict[str, str],
    valid_second_votes: int,
    direct_by_party: Dict[str, int],
    allocation: Dict[str, int],
    total_seats: int,
    rows: List[PartySeatRow],
) -> Dict[str, object]:
    return {
        "source_url": config_url,
        "reported_precincts": parse_int(land_row.get("gemeldete Wahlbezirke")),
        "total_precincts": parse_int(land_row.get("Anzahl Wahlbezirke")),
        "valid_second_votes": valid_second_votes,
        "nominal_seats": NOMINAL_SEATS,
        "total_seats": total_seats,
        "direct_mandates_total": sum(direct_by_party.values()),
        "seats": [
            {
                "party": row.party,
                "second_votes": row.second_votes,
                "second_vote_share_percent": round(row.second_vote_share, 4),
                "direct_mandates": row.direct_mandates,
                "list_seats": row.list_seats,
                "total_seats": row.total_seats,
            }
            for row in rows
        ],
        "direct_mandates_by_party": dict(sorted(direct_by_party.items())),
        "allocation_by_party": dict(sorted(allocation.items())),
    }


def print_table(payload: Dict[str, object]) -> None:
    print(
        "Statistik BW live seat calculation "
        f"({payload['reported_precincts']}/{payload['total_precincts']} precincts reported)"
    )
    print(f"Source: {payload['source_url']}")
    print(f"Nominal seats: {payload['nominal_seats']}  Total seats after balance: {payload['total_seats']}")
    print("")
    print(f"{'Party':<16} {'2nd votes':>10} {'Share':>7} {'Direct':>7} {'List':>5} {'Seats':>5}")
    print(f"{'-' * 16} {'-' * 10} {'-' * 7} {'-' * 7} {'-' * 5} {'-' * 5}")
    for row in payload["seats"]:
        print(
            f"{row['party']:<16} "
            f"{row['second_votes']:>10} "
            f"{row['second_vote_share_percent']:>6.2f}% "
            f"{row['direct_mandates']:>7} "
            f"{row['list_seats']:>5} "
            f"{row['total_seats']:>5}"
        )


def main() -> int:
    args = parse_args()
    set_active_election(election_key=args.election_key)
    config = load_config()

    csv_text = fetch_csv_text(config.statla_live_csv_url) if not args.csv_path else open(args.csv_path, encoding="utf-8-sig").read()
    rows = read_statla_rows(csv_text)
    land_row = get_land_row(rows)
    wahlkreis_rows = get_wahlkreis_rows(rows)
    second_votes, valid_second_votes = second_vote_totals(land_row)
    direct_by_party = direct_mandates(wahlkreis_rows)
    qualifying = qualifying_parties(second_votes, valid_second_votes)
    ensure_supported_edge_cases(direct_by_party, qualifying, second_votes)
    total_seats, allocation = balanced_allocation(qualifying, direct_by_party)
    rows_out = seat_rows(qualifying, valid_second_votes, direct_by_party, allocation)
    payload = build_output(
        config_url=config.statla_live_csv_url if not args.csv_path else args.csv_path,
        land_row=land_row,
        valid_second_votes=valid_second_votes,
        direct_by_party=direct_by_party,
        allocation=allocation,
        total_seats=total_seats,
        rows=rows_out,
    )

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print_table(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
