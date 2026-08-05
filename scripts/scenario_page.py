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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def state_code(election_key: str) -> str:
    return election_key.rsplit("-", 1)[-1].lower()


def seat_count_for(election_key: str) -> int:
    return {"bw": 120, "rlp": 101, "lsa": 97}.get(state_code(election_key), 100)


def slug_for_party(party: str) -> str:
    normalized = party.lower()
    normalized = normalized.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized or "party"


def land_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if str(row.get("row_key") or "").endswith(":LAND")]


def land_snapshot() -> dict[str, str]:
    for row in read_csv_rows(core.LATEST_DIR / "statla_snapshots.csv"):
        if str(row.get("gebietsart") or "").strip().upper() == "LAND":
            return row
    return {}


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
    if not second_vote_rows and state_code(config.election_key) == "lsa":
        reference_path = core.ROOT / "data" / config.election_key / "reference" / "2021" / "party_results.csv"
        reference_rows = read_csv_rows(reference_path)
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

    parties: list[dict[str, Any]] = []
    for row in second_vote_rows:
        party = core.canonical_party_name(str(row.get("party_name") or row.get("party_key") or ""), "Zweitstimmen")
        if not party:
            continue
        vote_field = "votes" if state_code(config.election_key) == "lsa" and use_reference else ("votes_2021" if use_reference else "votes")
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


def build_payload(config: core.Config, party_colors: dict[str, str]) -> dict[str, Any]:
    baseline = load_party_baseline(config, party_colors)
    vote_label = config.second_vote_label or "Zweitstimmen"
    return {
        "electionKey": config.election_key,
        "electionName": config.election_name,
        "voteLabel": vote_label,
        "baseSeats": seat_count_for(config.election_key),
        "thresholdPercent": 5.0,
        "baselineMode": baseline["baselineMode"],
        "validVotes": baseline["validVotes"],
        "reportedPrecincts": baseline["reportedPrecincts"],
        "totalPrecincts": baseline["totalPrecincts"],
        "parties": baseline["parties"],
        "coalitions": coalition_presets(config.election_key),
        "notes": [
            "Die Sitzverteilung nutzt ein proportionales Sainte-Laguë-Modell mit 5-Prozent-Schwelle.",
            "Direktmandate, Überhangmandate, Mehrheitssicherungen und amtliche Losentscheide werden hier nicht simuliert.",
        ],
    }


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
  const resetButton = document.querySelector("[data-reset]");
  const copyButton = document.querySelector("[data-copy]");
  const params = new URLSearchParams(window.location.search);
  let payload = null;

  function formatPercent(value) {
    return `${value.toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} %`;
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

  function allocate(parties, seats, threshold) {
    const eligible = parties.filter((party) => party.adjustedShare >= threshold);
    const allocation = new Map(eligible.map((party) => [party.party, 0]));
    const quotients = [];
    eligible.forEach((party) => {
      for (let index = 0; index < seats; index += 1) {
        quotients.push({ party: party.party, value: party.adjustedShare / (2 * index + 1) });
      }
    });
    quotients.sort((a, b) => b.value - a.value || a.party.localeCompare(b.party));
    quotients.slice(0, seats).forEach((item) => allocation.set(item.party, (allocation.get(item.party) || 0) + 1));
    return allocation;
  }

  function currentSwings() {
    const swings = new Map();
    controls.querySelectorAll("input[data-party]").forEach((input) => {
      swings.set(input.dataset.party, Number(input.value || 0));
    });
    return swings;
  }

  function adjustedParties() {
    const swings = currentSwings();
    const raw = payload.parties.map((party) => ({
      ...party,
      swing: swings.get(party.party) || 0,
      rawShare: Math.max(0, party.share + (swings.get(party.party) || 0)),
    }));
    const sum = raw.reduce((total, party) => total + party.rawShare, 0) || 1;
    return raw.map((party) => ({
      ...party,
      adjustedShare: (party.rawShare / sum) * 100,
    }));
  }

  function updateUrl(parties) {
    const next = new URLSearchParams();
    parties.forEach((party) => {
      if (Math.abs(party.swing) >= 0.05) {
        next.set(party.slug, party.swing.toFixed(1));
      }
    });
    const suffix = next.toString();
    const nextUrl = `${window.location.pathname}${suffix ? `?${suffix}` : ""}`;
    window.history.replaceState(null, "", nextUrl);
  }

  function render() {
    const parties = adjustedParties();
    const allocation = allocate(parties, payload.baseSeats, payload.thresholdPercent);
    parties.forEach((party) => {
      party.seats = allocation.get(party.party) || 0;
      party.qualifies = party.adjustedShare >= payload.thresholdPercent;
    });
    parties.sort((a, b) => b.seats - a.seats || b.adjustedShare - a.adjustedShare || a.party.localeCompare(b.party));
    const majority = Math.floor(payload.baseSeats / 2) + 1;
    summary.textContent = `${payload.baseSeats} Sitze, Mehrheit ab ${majority}. Modell: 5%-Schwelle und Sainte-Laguë.`;

    seatsRoot.replaceChildren();
    parties.filter((party) => party.seats > 0).forEach((party) => {
      const row = document.createElement("div");
      row.className = "seat-row";
      const partyLabel = escapeHtml(party.party);
      row.innerHTML = `
        <span class="scenario-party"><span class="scenario-dot" style="background:${party.color}"></span>${partyLabel}</span>
        <span class="seat-track"><span class="seat-fill" style="width:${Math.max(2, (party.seats / payload.baseSeats) * 100)}%; background:${party.color}"></span></span>
        <strong>${party.seats}</strong>
      `;
      seatsRoot.appendChild(row);
    });

    coalitionsRoot.replaceChildren();
    payload.coalitions.forEach((coalition) => {
      const seats = coalition.parties.reduce((total, party) => total + (allocation.get(party) || 0), 0);
      const card = document.createElement("div");
      card.className = `coalition ${seats >= majority ? "ok" : "miss"}`;
      card.innerHTML = `<strong>${escapeHtml(coalition.label)}</strong><span>${seats} / ${majority} Sitze</span>`;
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
    payload.parties.forEach((party) => {
      const initial = Number(params.get(party.slug) || 0);
      const wrapper = document.createElement("label");
      wrapper.className = "scenario-control";
      const partyLabel = escapeHtml(party.party);
      wrapper.innerHTML = `
        <span class="scenario-control-head">
          <span class="scenario-party"><span class="scenario-dot" style="background:${party.color}"></span>${partyLabel}</span>
          <output>${initial.toFixed(1)} pp</output>
        </span>
        <input data-party="${partyLabel}" type="range" min="-8" max="8" step="0.5" value="${initial}">
      `;
      const input = wrapper.querySelector("input");
      const output = wrapper.querySelector("output");
      input.addEventListener("input", () => {
        output.textContent = `${Number(input.value).toFixed(1)} pp`;
        render();
      });
      controls.appendChild(wrapper);
    });
  }

  resetButton.addEventListener("click", () => {
    controls.querySelectorAll("input[data-party]").forEach((input) => {
      input.value = "0";
      input.closest(".scenario-control").querySelector("output").textContent = "0.0 pp";
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
        f"<p class='muted'>Interaktive Stimmenverschiebung für {html.escape(str(payload['voteLabel']))}, "
        "Schwelle, Sitznäherung und Koalitionsmehrheiten.</p></div>"
        "<div class='scenario-shell'>"
        f"<div class='scenario-alert'>{html.escape(baseline_note)} "
        "Die Sitznäherung ist ein transparentes Rechenmodell, kein amtliches Ergebnis.</div>"
        "<div class='scenario-workspace'>"
        "<div class='panel'><h2>Stimmen verschieben</h2>"
        "<div class='scenario-controls' data-scenario-controls></div>"
        "<div class='scenario-actions'><button type='button' data-reset>Zurücksetzen</button>"
        "<button class='secondary' type='button' data-copy>Link kopieren</button></div></div>"
        "<div class='panel'><h2>Sitznäherung</h2><p class='small' data-scenario-summary>Lade Szenario...</p>"
        "<div class='seat-bars' data-seat-bars></div></div>"
        "</div>"
        "<div class='panel'><h2>Koalitionsmehrheiten</h2><div class='coalition-grid' data-coalitions></div></div>"
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
            "Stimmenanteile verschieben, 5-Prozent-Schwelle prüfen und Sitznäherung vergleichen."
        ),
        breadcrumbs=[
            ("wahl-monitor.de", "/"),
            (config.election_name, f"/{config.election_key}/"),
            ("Was-wäre-wenn-Szenario", f"/{config.election_key}/scenario.html"),
        ],
    )
