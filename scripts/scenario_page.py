#!/usr/bin/env python3
"""Generate static what-if scenario pages for election dashboards."""

from __future__ import annotations

import csv
import html
import json
import re
from pathlib import Path
from typing import Any, Callable

import poll_election_core as core


WritePage = Callable[..., None]

DEFAULT_PARTY_COLORS = {
    "GRÜNE": "#008939",
    "CDU": "#2d3c4b",
    "SPD": "#e3000f",
    "FDP": "#ffed00",
    "AfD": "#00a7d8",
    "Die Linke": "#e6007b",
    "FREIE WÄHLER": "#F29204",
    "BSW": "#a21749",
    "Volt": "#502379",
}

# The 2026 Berlin election uses Bezirkslisten for these parties.  Their
# statewide entitlement is distributed to the twelve Wahlkreisverbände before
# direct mandates are checked for overhang.  The remaining qualifying parties
# compete with a Landesliste and are handled at the statewide level.  HEIMAT
# and B* have district lists only in individual districts; include their
# canonical names so a scenario remains correct if either crosses the
# threshold or wins a direct mandate.
BERLIN_DISTRICT_LIST_PARTIES = {"CDU", "SPD", "Die Linke", "Die Heimat", "B*"}


# These are the statutory starting sizes used by the scenario model.  The
# final parliament can be larger where the applicable law requires
# overhang/compensation mandates.
SEAT_RULES = {
    "bw": {
        "base_seats": 120,
        "direct_seats": 0,
        "allocation_method": "sainte_lague",
        "direct_threshold_exception": False,
        "compensation_rule": "none",
    },
    "rlp": {
        "base_seats": 101,
        "direct_seats": 0,
        "allocation_method": "sainte_lague",
        "direct_threshold_exception": False,
        "compensation_rule": "none",
    },
    "lsa": {
        "base_seats": 83,
        "direct_seats": 41,
        "allocation_method": "hare_niemeyer",
        "direct_threshold_exception": False,
        "compensation_rule": "lsa",
    },
    "mv": {
        "base_seats": 71,
        "direct_seats": 36,
        "allocation_method": "hare_niemeyer",
        "direct_threshold_exception": False,
        "compensation_rule": "mv",
    },
    "be": {
        "base_seats": 130,
        "direct_seats": 78,
        "allocation_method": "hare_niemeyer",
        "direct_threshold_exception": True,
        "compensation_rule": "berlin",
    },
}


def read_csv_rows(path: Path, delimiter: str = ",") -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter=delimiter))


def state_code(election_key: str) -> str:
    return election_key.rsplit("-", 1)[-1].lower()


def seat_count_for(election_key: str) -> int:
    return SEAT_RULES.get(state_code(election_key), {}).get("base_seats", 100)


def direct_seat_count_for(election_key: str) -> int:
    return SEAT_RULES.get(state_code(election_key), {}).get("direct_seats", 0)


def allocation_method_for(election_key: str) -> str:
    return SEAT_RULES.get(state_code(election_key), {}).get("allocation_method", "sainte_lague")


def allocation_method_label(method: str) -> str:
    return {
        "hare_niemeyer": "Hare/Niemeyer",
        "sainte_lague": "Sainte-Laguë",
    }.get(method, method)


def slug_for_party(party: str) -> str:
    normalized = party.lower()
    normalized = normalized.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized or "party"


def land_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return party-result rows for the state-wide level.

    StatLA uses keys such as ``lsa:LAND:15`` for the state row. Older
    datasets used a trailing ``:LAND`` key, and some callers provide an
    explicit ``gebietsart`` column, so accept all three representations.
    """
    result = []
    for row in rows:
        area_level = str(row.get("gebietsart") or "").strip().upper()
        key_parts = {part.strip().upper() for part in str(row.get("row_key") or "").split(":")}
        if area_level == "LAND" or "LAND" in key_parts:
            result.append(row)
    return result


def land_snapshot() -> dict[str, str]:
    for row in read_csv_rows(core.LATEST_DIR / "statla_snapshots.csv"):
        if str(row.get("gebietsart") or "").strip().upper() == "LAND":
            return row
    return {}


def reference_2021_path(config: core.Config, filename: str) -> Path:
    return core.ROOT / "data" / config.election_key / "reference" / "2021" / filename


def reference_2021_party_rows(config: core.Config) -> list[dict[str, str]]:
    return read_csv_rows(reference_2021_path(config, "party_results.csv"))


def reference_2021_direct_seat_counts(config: core.Config) -> dict[str, int]:
    state = state_code(config.election_key)
    delimiter = ";" if state == "be" else ","
    rows = read_csv_rows(reference_2021_path(config, "wahlkreis_summary.csv"), delimiter=delimiter)
    counts: dict[str, int] = {}
    for row in rows:
        party = core.canonical_party_name(str(row.get("winner_first") or ""), "Erststimmen")
        if party:
            counts[party] = counts.get(party, 0) + 1
    return dict(sorted(counts.items()))


def load_berlin_district_model(config: core.Config) -> list[dict[str, Any]]:
    """Load current Berlin district second votes and direct winners.

    Berlin's CDU, SPD and Die Linke lists are distributed by Bezirk; HEIMAT and
    B* can also appear with district lists in individual districts. Keep the
    district-level inputs in the scenario payload so the browser can apply the
    statutory overhang check instead of comparing only statewide totals.
    """
    if state_code(config.election_key) != "be":
        return []

    rows = read_csv_rows(core.LATEST_DIR / "statla_party_results.csv")
    mapping_rows = read_csv_rows(core.META_DIR / "wahlkreis-mapping.csv", delimiter=";")
    wahlkreis_to_district = {
        str(row.get("Wahlkreisnummer") or "").strip().lstrip("0") or "0":
            str(row.get("Bezirk") or "").strip().zfill(2)
        for row in mapping_rows
        if str(row.get("Wahlkreisnummer") or "").strip()
    }

    district_votes: dict[str, dict[str, int]] = {}
    for row in rows:
        key = str(row.get("row_key") or "")
        key_parts = {part.strip().upper() for part in key.split(":")}
        if not key.lower().startswith("berlin:") or "GEMEINDE" not in key_parts:
            continue
        if core.canonical_vote_type(str(row.get("vote_type") or "")) != "Zweitstimmen":
            continue
        ags = key.split(":")[-1].strip()
        district = ags[-2:] if len(ags) >= 2 else ""
        if not district.isdigit():
            continue
        party = core.canonical_party_name(
            str(row.get("party_name") or row.get("party_key") or ""),
            "Zweitstimmen",
        )
        if not party:
            continue
        district_votes.setdefault(district, {})[party] = core.parse_int(row.get("votes")) or 0

    direct_by_district: dict[str, dict[str, int]] = {}
    by_wahlkreis: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        key = str(row.get("row_key") or "")
        key_parts = {part.strip().upper() for part in key.split(":")}
        if not key.lower().startswith("berlin:") or "WAHLKREIS" not in key_parts:
            continue
        if core.canonical_vote_type(str(row.get("vote_type") or "")) != "Erststimmen":
            continue
        votes = core.parse_int(row.get("votes")) or 0
        party = core.canonical_party_name(
            str(row.get("party_name") or row.get("party_key") or ""),
            "Erststimmen",
        )
        if votes <= 0 or not party:
            continue
        wahlkreis = key.split(":")[-1].strip().lstrip("0") or "0"
        by_wahlkreis.setdefault(wahlkreis, []).append((votes, party))
    for wahlkreis, entries in by_wahlkreis.items():
        district = wahlkreis_to_district.get(wahlkreis)
        if not district:
            continue
        _votes, winner = max(entries, key=lambda item: (item[0], item[1]))
        district_by_party = direct_by_district.setdefault(district, {})
        district_by_party[winner] = district_by_party.get(winner, 0) + 1

    districts = []
    for district in sorted(set(district_votes) | set(direct_by_district)):
        districts.append(
            {
                "district": district,
                "secondVotes": district_votes.get(district, {}),
                "directSeats": direct_by_district.get(district, {}),
            }
        )
    return districts


def load_party_baseline(config: core.Config, party_colors: dict[str, str]) -> dict[str, Any]:
    rows = land_rows(read_csv_rows(core.LATEST_DIR / "statla_party_results.csv"))
    second_vote_rows = [
        row
        for row in rows
        if core.canonical_vote_type(str(row.get("vote_type") or "")) == "Zweitstimmen"
    ]
    snapshot = land_snapshot()
    current_total = core.parse_int(snapshot.get("valid_votes_zweit")) or 0
    reference_total = core.parse_int(snapshot.get("valid_votes_zweit_2021")) or 0
    use_reference = current_total <= 0 and reference_total > 0
    reference_rows_loaded = False
    if current_total <= 0 and state_code(config.election_key) in {"lsa", "mv", "be"}:
        reference_rows = reference_2021_party_rows(config)
        second_vote_rows = [
            row
            for row in reference_rows
            if str(row.get("area_level") or "") == "LAND"
            and core.canonical_vote_type(str(row.get("vote_type") or "")) == "Zweitstimmen"
        ]
        reference_total = next(
            (
                core.parse_int(row.get("valid_votes")) or 0
                for row in second_vote_rows
                if core.parse_int(row.get("valid_votes")) is not None
            ),
            0,
        )
        use_reference = bool(second_vote_rows and reference_total > 0)
        reference_rows_loaded = use_reference

    parties: list[dict[str, Any]] = []
    for row in second_vote_rows:
        party = core.canonical_party_name(str(row.get("party_name") or row.get("party_key") or ""), "Zweitstimmen")
        if not party:
            continue
        vote_field = "votes" if reference_rows_loaded else ("votes_2021" if use_reference else "votes")
        votes = core.parse_int(row.get(vote_field)) or 0
        if votes <= 0:
            continue
        parties.append(
            {
                "party": party,
                "slug": slug_for_party(party),
                "votes": votes,
                "color": party_colors.get(party, DEFAULT_PARTY_COLORS.get(party, "#6b7280")),
            }
        )

    total_votes = reference_total if use_reference else current_total
    if total_votes <= 0:
        total_votes = sum(int(row["votes"]) for row in parties)
    for row in parties:
        row["share"] = round((int(row["votes"]) / total_votes) * 100, 4) if total_votes else 0.0
    parties.sort(key=lambda item: (-int(item["votes"]), str(item["party"])))

    return {
        "baselineMode": "reference_2021" if use_reference else ("current" if total_votes > 0 else "none"),
        "validVotes": total_votes,
        "reportedPrecincts": core.parse_int(snapshot.get("reported_precincts")) or 0,
        "totalPrecincts": core.parse_int(snapshot.get("total_precincts")) or 0,
        "parties": parties,
    }


def load_direct_seat_counts(config: core.Config) -> dict[str, int]:
    """Count current first-vote leaders in the state's constituencies.

    These leaders are provisional while the live result is incomplete. The
    scenario keeps them fixed while users vary the second-vote shares.
    """
    state = state_code(config.election_key)
    if state not in {"lsa", "mv", "be"}:
        return {}
    row_prefix = "berlin" if state == "be" else state
    rows = read_csv_rows(core.LATEST_DIR / "statla_party_results.csv")
    by_wahlkreis: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        key = str(row.get("row_key") or "")
        if not key.lower().startswith(f"{row_prefix}:"):
            continue
        key_parts = {part.strip().upper() for part in key.split(":")}
        if "WAHLKREIS" not in key_parts:
            continue
        if core.canonical_vote_type(str(row.get("vote_type") or "")) != "Erststimmen":
            continue
        votes = core.parse_int(row.get("votes")) or 0
        party = core.canonical_party_name(
            str(row.get("party_name") or row.get("party_key") or ""),
            "Erststimmen",
        )
        if votes > 0 and party:
            by_wahlkreis.setdefault(key, []).append((votes, party))

    counts: dict[str, int] = {}
    for entries in by_wahlkreis.values():
        winner_votes, winner_party = max(entries, key=lambda item: (item[0], item[1]))
        if winner_votes > 0:
            counts[winner_party] = counts.get(winner_party, 0) + 1
    if counts:
        return dict(sorted(counts.items()))
    return reference_2021_direct_seat_counts(config)


def build_payload(config: core.Config, party_colors: dict[str, str]) -> dict[str, Any]:
    baseline = load_party_baseline(config, party_colors)
    direct_seat_counts = load_direct_seat_counts(config)
    berlin_districts = load_berlin_district_model(config)
    vote_label = config.second_vote_label or "Zweitstimmen"
    state = state_code(config.election_key)
    allocation_method = allocation_method_for(config.election_key)
    rule = SEAT_RULES.get(state, {})
    notes_by_state = {
        "lsa": [
            "Sachsen-Anhalt: gesetzliche Ausgangszahl 83 Sitze (41 Direktmandate und 42 Listenmandate).",
            "Die Sitzverteilung nutzt die 5-Prozent-Schwelle und Hare/Niemeyer.",
            "Die aktuellen Direktmandatsführer werden berücksichtigt; bei Überhang werden zusätzliche Sitze nach der gesetzlichen Ausgleichslogik ergänzt.",
        ],
        "mv": [
            "Mecklenburg-Vorpommern: gesetzliche Ausgangszahl 71 Sitze (36 Direktmandate und 35 Listenmandate).",
            "Die Sitzverteilung nutzt die 5-Prozent-Schwelle und Hare/Niemeyer; eine Grundmandatsklausel gibt es nicht.",
            "Direktmandate werden auf den proportionalen Sitzanspruch angerechnet; Überhang- und Ausgleichsmandate werden nach der M-V-Regel modelliert.",
        ],
        "be": [
            "Berlin: mindestens 130 Sitze (78 Direktmandate und mindestens 52 Listenmandate).",
            "Die Sitzverteilung nutzt die 5-Prozent-Schwelle und Hare/Niemeyer.",
            "2026 treten CDU, SPD und Die Linke mit Bezirkslisten an; ihre Listenmandate werden je Bezirk verteilt, bevor Überhang- und Ausgleichsmandate nach der Berliner Regel berechnet werden.",
            "Eine Partei nimmt auch unter 5 % an der Sitzverteilung teil, wenn sie mindestens ein Wahlkreismandat gewinnt.",
        ],
    }
    notes = notes_by_state.get(
        state,
        [
            f"Die Sitzverteilung nutzt ein proportionales {allocation_method_label(allocation_method)}-Modell mit 5-Prozent-Schwelle.",
            "Direktmandate, Überhangmandate, Mehrheitssicherungen und amtliche Losentscheide werden hier nicht simuliert.",
        ],
    )
    notes.append("Die Regler geben absolute Stimmenanteile an; bei einer Summe ungleich 100 % wird für die Sitznäherung proportional normiert.")
    return {
        "electionKey": config.election_key,
        "electionName": config.election_name,
        "voteLabel": vote_label,
        "baseSeats": seat_count_for(config.election_key),
        "directSeats": direct_seat_count_for(config.election_key),
        "directSeatCounts": direct_seat_counts,
        "reportedDirectSeats": sum(direct_seat_counts.values()),
        "berlinDistricts": berlin_districts,
        "berlinDistrictListParties": sorted(BERLIN_DISTRICT_LIST_PARTIES),
        "allocationMethod": allocation_method,
        "thresholdDirectException": bool(rule.get("direct_threshold_exception", False)),
        "compensationRule": rule.get("compensation_rule", "none"),
        "seatBasis": "gesetzliche Ausgangszahl" if state in {"lsa", "mv", "be"} else "Modellbasis",
        "seatNote": (
            "Gesetzliche Ausgangszahl: 83 Sitze (41 Direktmandate + 42 Listenmandate). "
            "Die Sitznäherung wird mit den aktuellen Zweitstimmen und Hare/Niemeyer berechnet; Überhang und Ausgleich können die Gesamtzahl erhöhen."
            if state == "lsa"
            else "Gesetzliche Ausgangszahl: 71 Sitze (36 Direktmandate + 35 Listenmandate). "
            "Die Sitznäherung verwendet Hare/Niemeyer sowie die landesspezifische Überhang- und Ausgleichslogik."
            if state == "mv"
            else "Gesetzliche Ausgangszahl: mindestens 130 Sitze (78 Direktmandate + mindestens 52 Listenmandate). "
            "Die Sitznäherung verwendet Hare/Niemeyer sowie die landesspezifische Überhang- und Ausgleichslogik."
            if state == "be"
            else "Transparente Modellbasis ohne landesspezifische Überhang- und Ausgleichsmandate."
        ),
        "thresholdPercent": 5.0,
        "swingLimitPercent": 100.0,
        "baselineMode": baseline["baselineMode"],
        "validVotes": baseline["validVotes"],
        "reportedPrecincts": baseline["reportedPrecincts"],
        "totalPrecincts": baseline["totalPrecincts"],
        "parties": baseline["parties"],
        "coalitions": single_party_presets(baseline["parties"]) + coalition_presets(config.election_key),
        "notes": notes,
    }


def single_party_presets(parties: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"label": f"{party['party']} allein", "parties": [party["party"]]}
        for party in parties
        if str(party.get("party") or "").strip()
    ]


def coalition_presets(election_key: str) -> list[dict[str, Any]]:
    if state_code(election_key) == "lsa":
        return [
            {"label": "CDU + SPD", "parties": ["CDU", "SPD"]},
            {"label": "CDU + AfD", "parties": ["CDU", "AfD"]},
            {"label": "CDU + FDP", "parties": ["CDU", "FDP"]},
            {"label": "SPD + GRÜNE + Die Linke", "parties": ["SPD", "GRÜNE", "Die Linke"]},
        ]
    if state_code(election_key) == "rlp":
        return [
            {"label": "Ampel", "parties": ["SPD", "GRÜNE", "FDP"]},
            {"label": "CDU + SPD", "parties": ["CDU", "SPD"]},
            {"label": "CDU + FDP + FW", "parties": ["CDU", "FDP", "FREIE WÄHLER"]},
            {"label": "CDU + AfD", "parties": ["CDU", "AfD"]},
        ]
    return [
        {"label": "GRÜNE + CDU", "parties": ["GRÜNE", "CDU"]},
        {"label": "CDU + SPD + FDP", "parties": ["CDU", "SPD", "FDP"]},
        {"label": "GRÜNE + SPD + FDP", "parties": ["GRÜNE", "SPD", "FDP"]},
        {"label": "CDU + AfD", "parties": ["CDU", "AfD"]},
    ]


def scenario_css() -> str:
    return """
<style>
  .scenario-shell { display: grid; gap: 20px; }
  .scenario-alert {
    border-left: 4px solid var(--warning);
    background: #fff8eb;
    color: #6f4300;
    padding: 12px 14px;
    border-radius: 10px;
  }
  .scenario-workspace {
    display: grid;
    grid-template-columns: minmax(260px, 0.95fr) minmax(0, 1.35fr);
    gap: 20px;
  }
  .scenario-controls {
    display: grid;
    gap: 12px;
  }
  .scenario-control {
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 12px;
    background: #fbfdff;
  }
  .scenario-control-head {
    align-items: center;
    display: flex;
    gap: 8px;
    justify-content: space-between;
    margin-bottom: 8px;
  }
  .scenario-party {
    align-items: center;
    display: inline-flex;
    gap: 8px;
    font-weight: 700;
  }
  .scenario-dot {
    border: 1px solid rgba(0,0,0,0.15);
    border-radius: 999px;
    display: inline-block;
    height: 12px;
    width: 12px;
  }
  .scenario-control output {
    color: var(--muted);
    font-variant-numeric: tabular-nums;
    font-weight: 700;
  }
  .scenario-total {
    margin: 10px 0 0;
    font-weight: 700;
  }
  .scenario-total.ok { color: #15803d; }
  .scenario-total.mismatch { color: #b45309; }
  .scenario-control input {
    accent-color: var(--accent);
    width: 100%;
  }
  .scenario-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 12px;
  }
  .scenario-actions button {
    background: var(--accent);
    border: 0;
    border-radius: 10px;
    color: #fff;
    cursor: pointer;
    font: inherit;
    font-weight: 700;
    min-height: 42px;
    padding: 0 14px;
  }
  .scenario-actions button.secondary {
    background: #eef2ff;
    color: var(--accent);
  }
  .seat-bars {
    display: grid;
    gap: 10px;
  }
  .seat-row {
    display: grid;
    gap: 8px;
    grid-template-columns: minmax(90px, 150px) minmax(0, 1fr) 60px;
    align-items: center;
  }
  .seat-track {
    background: #e9edf4;
    border-radius: 999px;
    height: 16px;
    overflow: hidden;
  }
  .seat-fill {
    border-radius: inherit;
    height: 100%;
    min-width: 2px;
  }
  .coalition-grid {
    display: grid;
    gap: 10px;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  }
  .coalition {
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 12px;
    background: #fbfdff;
  }
  .coalition strong { display: block; }
  .coalition.ok { border-color: rgba(22, 163, 74, 0.35); background: #f0fdf4; }
  .coalition.miss { color: var(--muted); }
  .scenario-table table { min-width: 760px; }
  @media (max-width: 900px) {
    .scenario-workspace { grid-template-columns: 1fr; }
    .seat-row { grid-template-columns: 95px minmax(0, 1fr) 48px; }
  }
</style>
"""


def scenario_script() -> str:
    return r"""
<script>
(function () {
  const dataUrl = "scenario-data.json";
  const controls = document.querySelector("[data-scenario-controls]");
  const seatsRoot = document.querySelector("[data-seat-bars]");
  const coalitionsRoot = document.querySelector("[data-coalitions]");
  const tableBody = document.querySelector("[data-scenario-table]");
  const summary = document.querySelector("[data-scenario-summary]");
  const coalitionSummary = document.querySelector("[data-coalition-summary]");
  const scenarioTotal = document.querySelector("[data-scenario-total]");
  const resetButton = document.querySelector("[data-reset]");
  const copyButton = document.querySelector("[data-copy]");
  const params = new URLSearchParams(window.location.search);
  const defaultSwingLimit = 100;
  let payload = null;

  function clampScenarioShare(value) {
    const limit = Number(payload && payload.swingLimitPercent) || defaultSwingLimit;
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
      return 0;
    }
    return Math.min(limit, Math.max(0, numericValue));
  }

  function formatPercent(value) {
    return `${value.toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} %`;
  }

  function formatPoints(value) {
    const sign = value >= 0 ? "+" : "";
    return `${sign}${value.toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} pp`;
  }

  function escapeHtml(value) {
    return String(value || "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;",
    }[char]));
  }

  function isEligibleForAllocation(party, threshold, directSeatCounts) {
    const directSeats = Number(directSeatCounts[party.party]) || 0;
    return party.adjustedShare >= threshold || (payload.thresholdDirectException && directSeats > 0);
  }

  function initializeAllocation(parties, seats, threshold, directSeatCounts) {
    const eligible = parties.filter((party) => isEligibleForAllocation(party, threshold, directSeatCounts));
    const ineligible = parties.filter((party) => !isEligibleForAllocation(party, threshold, directSeatCounts));
    const allocation = new Map(parties.map((party) => [party.party, 0]));
    ineligible.forEach((party) => {
      allocation.set(party.party, Number(directSeatCounts[party.party]) || 0);
    });
    const excludedDirectSeats = ineligible.reduce(
      (total, party) => total + (Number(directSeatCounts[party.party]) || 0),
      0,
    );
    return { eligible, allocation, seatsToAllocate: Math.max(0, seats - excludedDirectSeats) };
  }

  function allocateSainteLague(parties, seats, threshold, directSeatCounts = {}) {
    const initialized = initializeAllocation(parties, seats, threshold, directSeatCounts);
    const { eligible, allocation, seatsToAllocate } = initialized;
    const quotients = [];
    eligible.forEach((party) => {
      for (let index = 0; index < seatsToAllocate; index += 1) {
        quotients.push({ party: party.party, value: party.adjustedShare / (2 * index + 1) });
      }
    });
    quotients.sort((a, b) => b.value - a.value || a.party.localeCompare(b.party));
    quotients.slice(0, seatsToAllocate).forEach((item) => allocation.set(item.party, (allocation.get(item.party) || 0) + 1));
    return allocation;
  }

  function allocateHareNiemeyer(parties, seats, threshold, directSeatCounts = {}) {
    const initialized = initializeAllocation(parties, seats, threshold, directSeatCounts);
    const { eligible, allocation, seatsToAllocate } = initialized;
    const eligibleTotal = eligible.reduce((total, party) => total + party.adjustedShare, 0);
    if (eligibleTotal <= 0) {
      return allocation;
    }
    let assigned = 0;
    const remainders = eligible.map((party) => {
      const exact = (party.adjustedShare / eligibleTotal) * seatsToAllocate;
      const whole = Math.floor(exact);
      allocation.set(party.party, (allocation.get(party.party) || 0) + whole);
      assigned += whole;
      return { party: party.party, remainder: exact - whole, share: party.adjustedShare };
    });
    remainders.sort((a, b) => b.remainder - a.remainder || b.share - a.share || a.party.localeCompare(b.party));
    remainders.slice(0, seatsToAllocate - assigned).forEach((item) => {
      allocation.set(item.party, (allocation.get(item.party) || 0) + 1);
    });
    return allocation;
  }

  function allocateHareNiemeyerWithFixed(parties, seats, threshold, directSeatCounts = {}, fixedSeats = {}) {
    const initialized = initializeAllocation(parties, seats, threshold, directSeatCounts);
    const { eligible, allocation, seatsToAllocate } = initialized;
    const eligibleTotal = eligible.reduce((total, party) => total + party.adjustedShare, 0);
    if (eligibleTotal <= 0) {
      return allocation;
    }
    let assigned = 0;
    const remainders = [];
    eligible.forEach((party) => {
      const fixed = Number(fixedSeats[party.party]);
      if (Number.isFinite(fixed) && fixed > 0) {
        allocation.set(party.party, fixed);
        assigned += fixed;
        return;
      }
      const exact = (party.adjustedShare / eligibleTotal) * seatsToAllocate;
      const whole = Math.floor(exact);
      allocation.set(party.party, whole);
      assigned += whole;
      remainders.push({ party: party.party, remainder: exact - whole, share: party.adjustedShare });
    });
    remainders.sort((a, b) => b.remainder - a.remainder || b.share - a.share || a.party.localeCompare(b.party));
    remainders.slice(0, Math.max(0, seatsToAllocate - assigned)).forEach((item) => {
      allocation.set(item.party, (allocation.get(item.party) || 0) + 1);
    });
    return allocation;
  }

  function allocateDistrictSeats(party, seats, districts) {
    const weights = districts.map((district) => ({
      district: district.district,
      votes: Number((district.secondVotes || {})[party.party]) || 0,
    }));
    const totalVotes = weights.reduce((total, item) => total + item.votes, 0);
    const allocation = new Map(weights.map((item) => [item.district, 0]));
    if (totalVotes <= 0 || seats <= 0) {
      return allocation;
    }
    let assigned = 0;
    const remainders = weights.map((item) => {
      const exact = (item.votes / totalVotes) * seats;
      const whole = Math.floor(exact);
      allocation.set(item.district, whole);
      assigned += whole;
      return { district: item.district, remainder: exact - whole, votes: item.votes };
    });
    remainders.sort((a, b) => b.remainder - a.remainder || b.votes - a.votes || a.district.localeCompare(b.district));
    remainders.slice(0, seats - assigned).forEach((item) => {
      allocation.set(item.district, (allocation.get(item.district) || 0) + 1);
    });
    return allocation;
  }

  function countOverhang(parties, allocation, threshold, directSeatCounts) {
    return parties.reduce((total, party) => {
      if (!isEligibleForAllocation(party, threshold, directSeatCounts)) {
        return total;
      }
      const directSeats = Number(directSeatCounts[party.party]) || 0;
      return total + Math.max(0, directSeats - (allocation.get(party.party) || 0));
    }, 0);
  }

  function countBerlinPartySeats(parties, allocation, threshold, directSeatCounts) {
    const districts = Array.isArray(payload.berlinDistricts) ? payload.berlinDistricts : [];
    const districtListParties = new Set(payload.berlinDistrictListParties || []);
    const totals = new Map();
    parties.forEach((party) => {
      if (!isEligibleForAllocation(party, threshold, directSeatCounts)) {
        totals.set(party.party, Number(directSeatCounts[party.party]) || 0);
        return;
      }
      const partySeats = allocation.get(party.party) || 0;
      if (!districtListParties.has(party.party) || districts.length === 0) {
        totals.set(party.party, Math.max(partySeats, Number(directSeatCounts[party.party]) || 0));
        return;
      }
      const districtAllocation = allocateDistrictSeats(party, partySeats, districts);
      const total = districts.reduce((sum, district) => {
        const districtDirect = Number((district.directSeats || {})[party.party]) || 0;
        return sum + Math.max(districtAllocation.get(district.district) || 0, districtDirect);
      }, 0);
      totals.set(party.party, total);
    });
    return totals;
  }

  function allocateBerlinSeats(parties, seats, threshold, directSeatCounts) {
    const firstAllocation = allocateHareNiemeyer(parties, seats, threshold, directSeatCounts);
    const firstPartySeats = countBerlinPartySeats(parties, firstAllocation, threshold, directSeatCounts);
    const eligible = parties.filter((party) => isEligibleForAllocation(party, threshold, directSeatCounts));
    const eligibleTotal = eligible.reduce((total, party) => total + party.adjustedShare, 0);
    const overhangParties = eligible.filter((party) =>
      (firstPartySeats.get(party.party) || 0) > (firstAllocation.get(party.party) || 0),
    );
    const initialOverhang = overhangParties.reduce(
      (total, party) => total + (firstPartySeats.get(party.party) || 0) - (firstAllocation.get(party.party) || 0),
      0,
    );
    if (overhangParties.length === 0 || eligibleTotal <= 0) {
      return { allocation: firstAllocation, totalSeats: seats, overhang: 0, initialOverhang };
    }

    const requiredTotals = overhangParties.map((party) => {
      const partySeats = firstPartySeats.get(party.party) || 0;
      const required = party.adjustedShare > 0
        ? Math.floor((partySeats * eligibleTotal / party.adjustedShare) + 0.5)
        : seats;
      return { party: party.party, seats: partySeats, required: Math.max(seats, required) };
    });
    const totalSeats = Math.max(...requiredTotals.map((item) => item.required));
    const highestRequired = Math.max(...requiredTotals.map((item) => item.required));
    const fixedSeats = Object.fromEntries(
      requiredTotals
        .filter((item) => item.required === highestRequired)
        .map((item) => [item.party, item.seats]),
    );
    const allocation = allocateHareNiemeyerWithFixed(
      parties,
      totalSeats,
      threshold,
      directSeatCounts,
      fixedSeats,
    );
    // The fixed party's seats already include its district-level overhang.
    // The remaining parties receive the Hare/Niemeyer compensation seats.
    return { allocation, totalSeats, overhang: 0, initialOverhang };
  }

  function allocateSeats(parties, seats, threshold) {
    const directSeatCounts = payload.directSeatCounts || {};
    if (payload.compensationRule === "berlin" && Array.isArray(payload.berlinDistricts) && payload.berlinDistricts.length > 0) {
      return allocateBerlinSeats(parties, seats, threshold, directSeatCounts);
    }
    let totalSeats = seats;
    let allocation = payload.allocationMethod === "hare_niemeyer"
      ? allocateHareNiemeyer(parties, totalSeats, threshold, directSeatCounts)
      : allocateSainteLague(parties, totalSeats, threshold, directSeatCounts);
    const initialOverhang = countOverhang(parties, allocation, threshold, directSeatCounts);
    let overhang = initialOverhang;
    if (payload.compensationRule === "mv") {
      // M-V applies the two-times-overhang increase after each recalculation.
      // Repeating the step is important: in the 2021 result, overhang fell
      // from three seats to one before disappearing at 79 total seats.
      for (let iteration = 0; iteration < 10000 && overhang > 0; iteration += 1) {
        totalSeats += 2 * overhang;
        allocation = payload.allocationMethod === "hare_niemeyer"
          ? allocateHareNiemeyer(parties, totalSeats, threshold, directSeatCounts)
          : allocateSainteLague(parties, totalSeats, threshold, directSeatCounts);
        overhang = countOverhang(parties, allocation, threshold, directSeatCounts);
      }
    } else {
      // Fallback for elections without a district-level compensation model.
      for (let iteration = 0; iteration < 10000 && overhang > 0; iteration += 1) {
        totalSeats += 1;
        allocation = payload.allocationMethod === "hare_niemeyer"
          ? allocateHareNiemeyer(parties, totalSeats, threshold, directSeatCounts)
          : allocateSainteLague(parties, totalSeats, threshold, directSeatCounts);
        overhang = countOverhang(parties, allocation, threshold, directSeatCounts);
      }
    }
    if (payload.compensationRule === "mv" && totalSeats > seats && totalSeats % 2 === 0) {
      totalSeats += 1;
      allocation = payload.allocationMethod === "hare_niemeyer"
        ? allocateHareNiemeyer(parties, totalSeats, threshold, directSeatCounts)
        : allocateSainteLague(parties, totalSeats, threshold, directSeatCounts);
      overhang = countOverhang(parties, allocation, threshold, directSeatCounts);
    }
    return { allocation, totalSeats, overhang, initialOverhang };
  }

  function currentScenarioShares() {
    const shares = new Map();
    controls.querySelectorAll("input[data-party]").forEach((input) => {
      shares.set(input.dataset.party, clampScenarioShare(input.value));
    });
    return shares;
  }

  function adjustedParties() {
    const shares = currentScenarioShares();
    const raw = payload.parties.map((party) => ({
      ...party,
      scenarioShare: shares.has(party.party) ? shares.get(party.party) : party.share,
    }));
    const sum = raw.reduce((total, party) => total + party.scenarioShare, 0);
    return raw.map((party) => ({
      ...party,
      swing: party.scenarioShare - party.share,
      adjustedShare: sum > 0 ? (party.scenarioShare / sum) * 100 : 0,
    }));
  }

  function updateUrl(parties) {
    const next = new URLSearchParams();
    parties.forEach((party) => {
      if (Math.abs(party.swing) >= 0.05) {
        next.set(party.slug, party.scenarioShare.toFixed(1));
      }
    });
    const suffix = next.toString();
    const nextUrl = `${window.location.pathname}${suffix ? `?${suffix}` : ""}`;
    window.history.replaceState(null, "", nextUrl);
  }

  function render() {
    const parties = adjustedParties();
    const scenarioShareTotal = parties.reduce((total, party) => total + party.scenarioShare, 0);
    const mismatch = scenarioShareTotal - 100;
    const totalMatches = Math.abs(mismatch) < 0.05;
    scenarioTotal.classList.toggle("ok", totalMatches);
    scenarioTotal.classList.toggle("mismatch", !totalMatches);
    scenarioTotal.textContent = totalMatches
      ? `Summe der Szenarioanteile: ${formatPercent(scenarioShareTotal)}`
      : `Summe der Szenarioanteile: ${formatPercent(scenarioShareTotal)} · Abweichung ${formatPoints(mismatch)}. Für die Sitznäherung werden die Werte auf 100 % normiert.`;
    const seatResult = allocateSeats(parties, payload.baseSeats, payload.thresholdPercent);
    const allocation = seatResult.allocation;
    const totalSeats = seatResult.totalSeats;
    const extraSeats = Math.max(0, totalSeats - payload.baseSeats);
    parties.forEach((party) => {
      party.seats = allocation.get(party.party) || 0;
      party.directSeats = Number((payload.directSeatCounts || {})[party.party]) || 0;
      party.qualifies = isEligibleForAllocation(party, payload.thresholdPercent, payload.directSeatCounts || {});
    });
    parties.sort((a, b) => b.seats - a.seats || b.adjustedShare - a.adjustedShare || a.party.localeCompare(b.party));
    const majority = Math.floor(totalSeats / 2) + 1;
    const methodLabel = payload.allocationMethod === "hare_niemeyer" ? "Hare/Niemeyer" : "Sainte-Laguë";
    const totalLabel = extraSeats > 0
      ? `${totalSeats} Sitze (${extraSeats} zusätzliche Sitze)`
      : `${totalSeats} Sitze`;
    const directLabel = payload.reportedDirectSeats
      ? ` Aktuelle Direktmandatsführer: ${payload.reportedDirectSeats} von ${payload.directSeats}.`
      : "";
    const overhangLabel = payload.compensationRule === "berlin" && seatResult.initialOverhang > 0
      ? ` Bezirksebene: ${seatResult.initialOverhang} Überhangmandate; Ausgleich auf ${totalSeats} Sitze.`
      : "";
    summary.textContent = `${totalLabel}, gesetzliche Ausgangszahl ${payload.baseSeats}, Mehrheit ab ${majority}. Modell: 5%-Schwelle und ${methodLabel}.${directLabel}${overhangLabel}`;
    coalitionSummary.textContent = `Absolute Mehrheit ab ${majority} von ${totalSeats} Sitzen.`;

    seatsRoot.replaceChildren();
    parties.filter((party) => party.seats > 0).forEach((party) => {
      const row = document.createElement("div");
      row.className = "seat-row";
      const partyLabel = escapeHtml(party.party);
      row.innerHTML = `
        <span class="scenario-party"><span class="scenario-dot" style="background:${party.color}"></span>${partyLabel}</span>
        <span class="seat-track"><span class="seat-fill" style="width:${Math.max(2, (party.seats / totalSeats) * 100)}%; background:${party.color}"></span></span>
        <strong>${party.seats}</strong>
      `;
      seatsRoot.appendChild(row);
    });

    coalitionsRoot.replaceChildren();
    payload.coalitions.forEach((coalition) => {
      const seats = coalition.parties.reduce((total, party) => total + (allocation.get(party) || 0), 0);
      const card = document.createElement("div");
      card.className = `coalition ${seats >= majority ? "ok" : "miss"}`;
      const status = seats >= majority ? "absolute Mehrheit" : `unter ${majority} Sitzen`;
      card.innerHTML = `<strong>${escapeHtml(coalition.label)}</strong><span>${seats} / ${totalSeats} Sitze · ${status}</span>`;
      coalitionsRoot.appendChild(card);
    });

    tableBody.replaceChildren();
    parties.forEach((party) => {
      const row = document.createElement("tr");
      const partyLabel = escapeHtml(party.party);
      row.innerHTML = `
        <td><span class="scenario-party"><span class="scenario-dot" style="background:${party.color}"></span>${partyLabel}</span></td>
        <td>${formatPercent(party.share)}</td>
        <td>${party.swing >= 0 ? "+" : ""}${party.swing.toFixed(1)} pp</td>
        <td>${formatPercent(party.adjustedShare)}</td>
        <td>${party.qualifies ? "ja" : "nein"}</td>
        <td>${party.seats}</td>
      `;
      tableBody.appendChild(row);
    });
    updateUrl(parties);
  }

  function buildControls() {
    controls.replaceChildren();
    const swingLimit = Number(payload.swingLimitPercent) || defaultSwingLimit;
    payload.parties.forEach((party) => {
      const requested = params.get(party.slug);
      const initial = clampScenarioShare(requested === null ? party.share : requested);
      const wrapper = document.createElement("label");
      wrapper.className = "scenario-control";
      const partyLabel = escapeHtml(party.party);
      wrapper.innerHTML = `
        <span class="scenario-control-head">
          <span class="scenario-party"><span class="scenario-dot" style="background:${party.color}"></span>${partyLabel}</span>
          <output>${formatPercent(initial)}</output>
        </span>
        <input data-party="${partyLabel}" data-default="${party.share}" type="range" min="0" max="${swingLimit}" step="0.01" value="${initial}" aria-label="${partyLabel} Szenarioanteil">
      `;
      const input = wrapper.querySelector("input");
      const output = wrapper.querySelector("output");
      input.addEventListener("input", () => {
        output.textContent = formatPercent(Number(input.value));
        render();
      });
      controls.appendChild(wrapper);
    });
  }

  resetButton.addEventListener("click", () => {
    controls.querySelectorAll("input[data-party]").forEach((input) => {
      input.value = input.dataset.default;
      input.closest(".scenario-control").querySelector("output").textContent = formatPercent(Number(input.value));
    });
    render();
  });
  copyButton.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      copyButton.textContent = "Link kopiert";
    } catch (_error) {
      copyButton.textContent = "Link in Adresszeile";
    }
    setTimeout(() => { copyButton.textContent = "Link kopieren"; }, 1400);
  });

  fetch(dataUrl)
    .then((response) => response.json())
    .then((data) => {
      payload = data;
      if (!Array.isArray(payload.parties) || payload.parties.length === 0) {
        summary.textContent = "Keine belastbaren Ausgangsdaten für ein Szenario vorhanden.";
        return;
      }
      buildControls();
      render();
    })
    .catch((error) => {
      summary.textContent = `Szenariodaten konnten nicht geladen werden: ${error.message}`;
    });
})();
</script>
"""


def render_scenario_page(
    config: core.Config,
    output_root: Path,
    write_page: WritePage,
    party_colors: dict[str, str] | None = None,
) -> None:
    colors = party_colors or DEFAULT_PARTY_COLORS
    payload = build_payload(config, colors)
    (output_root / "scenario-data.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    baseline_note = {
        "current": "Ausgangsdaten aus dem aktuellen landesweiten Ergebnis.",
        "reference_2021": "Ausgangsdaten aus der 2021-Referenz, weil noch keine positiven aktuellen Landesstimmen vorliegen.",
        "none": "Noch keine Ausgangsdaten vorhanden.",
    }.get(str(payload["baselineMode"]), "Ausgangsdaten aus den neuesten verfügbaren Daten.")
    body = (
        f"{scenario_css()}"
        "<div class='hero'><div class='topbar'><a href='index.html'>Startseite dieser Wahl</a><span>/</span>"
        "<a href='search.html'>Suche</a><span>/</span><a href='../index.html'>Alle Wahlen</a></div>"
        f"<h1>Was-wäre-wenn: {html.escape(config.election_name)}</h1>"
        f"<p class='muted'>Interaktive Szenario-Stimmenanteile für {html.escape(str(payload['voteLabel']))}, "
        "Schwelle, Sitznäherung und Koalitionsmehrheiten.</p></div>"
        "<div class='scenario-shell'>"
        f"<div class='scenario-alert'>{html.escape(baseline_note)} {html.escape(str(payload['seatNote']))} "
        "Die Sitznäherung ist ein transparentes Rechenmodell, kein amtliches Ergebnis.</div>"
        "<div class='scenario-workspace'>"
        "<div class='panel'><h2>Stimmenanteile festlegen</h2>"
        "<p class='small'>Je Partei sind 0 bis 100 % möglich. Die Summe aller Regler sollte 100,0 % ergeben.</p>"
        "<p class='scenario-total mismatch' data-scenario-total aria-live='polite'>Summe der Szenarioanteile wird berechnet …</p>"
        "<div class='scenario-controls' data-scenario-controls></div>"
        "<div class='scenario-actions'><button type='button' data-reset>Zurücksetzen</button>"
        "<button class='secondary' type='button' data-copy>Link kopieren</button></div></div>"
        "<div class='panel'><h2>Sitznäherung</h2><p class='small' data-scenario-summary>Lade Szenario...</p>"
        "<div class='seat-bars' data-seat-bars></div></div>"
        "</div>"
        "<div class='panel'><h2>Koalitionsmehrheiten</h2><p class='small' data-coalition-summary>Sitzmehrheit wird berechnet …</p><div class='coalition-grid' data-coalitions></div></div>"
        "<div class='panel scenario-table'><h2>Parteien im Szenario</h2>"
        "<table><thead><tr><th>Partei</th><th>Ausgangswert</th><th>Verschiebung</th><th>Szenario</th><th>5 %</th><th>Sitze</th></tr></thead>"
        "<tbody data-scenario-table></tbody></table></div>"
        "</div>"
        f"{scenario_script()}"
    )
    write_page(
        output_root / "scenario.html",
        f"Was-wäre-wenn-Szenario {config.election_name} | wahl-monitor.de",
        body,
        description=(
            f"Interaktives Was-wäre-wenn-Szenario zur {config.election_name}: "
            "Stimmenanteile festlegen, die Summe auf 100 % prüfen und Sitznäherung vergleichen."
        ),
        breadcrumbs=[
            ("wahl-monitor.de", "/"),
            (config.election_name, f"/{config.election_key}/"),
            ("Was-wäre-wenn-Szenario", f"/{config.election_key}/scenario.html"),
        ],
    )
