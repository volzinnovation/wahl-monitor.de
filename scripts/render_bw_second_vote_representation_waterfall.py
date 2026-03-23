#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter, MaxNLocator


ROOT = Path(__file__).resolve().parents[1]
SECOND_VOTE_THRESHOLD = 0.05

PARTY_COLORS = {
    "GRÜNE": "#008939",
    "CDU": "#2D3C4B",
    "AfD": "#00CCFF",
    "SPD": "#E3000F",
    "FDP": "#FFED00",
    "FREIE WÄHLER": "#F58220",
    "Die Linke": "#BE3075",
    "BSW": "#7A1F5C",
    "Volt": "#502379",
    "Tierschutzpartei": "#3D7A57",
    "ÖDP": "#F58220",
    "PdH": "#E84A5F",
}

CHART_COLORS = {
    "start_total": "#6C7A89",
    "subtotal": "#A6B1BB",
    "exclusion": "#D68C45",
    "represented_total": "#556572",
    "connector": "#9AA6B2",
    "text": "#17212B",
    "muted": "#5D6975",
    "grid": "#D9E2EC",
    "approx_edge": "#7B8794",
}

DEFAULT_SECOND_VOTE_LABEL = "Zweitstimmen"
LABEL_BREAKS = {
    "Nichtwahlberechtigte": "Nichtwahl-\nberechtigte",
    "Wahlberechtigte": "Wahl-\nberechtigte",
    "Ungültige Zweitstimmen": "Ungültige\nZweitstimmen",
    "Ungültige Landesstimmen": "Ungültige\nLandesstimmen",
    "Gültige Zweitstimmen": "Gültige\nZweitstimmen",
    "Gültige Landesstimmen": "Gültige\nLandesstimmen",
    "Parteien < 5 %": "Parteien\n< 5 %",
    "Repräsentierte Zweitstimmen": "Repräsentierte\nZweitstimmen",
    "Repräsentierte Landesstimmen": "Repräsentierte\nLandesstimmen",
}


@dataclass(frozen=True)
class ElectionBaseline:
    population_total: int
    population_summary: str
    population_source_url: str
    eligible_voters_total: int
    eligible_voters_summary: str
    eligible_voters_source_url: str


ELECTION_BASELINES: Dict[str, ElectionBaseline] = {
    "2026-bw": ElectionBaseline(
        population_total=11_240_000,
        population_summary="Statistisches Landesamt Baden-Württemberg, Ende September 2025, rund 11,24 Mio.",
        population_source_url=(
            "https://www.statistik-bw.de/presse/pressemitteilungen/pressemitteilung/"
            "baden-wuerttemberg-maenner-sind-leicht-in-der-unterzahl/"
        ),
        eligible_voters_total=7_773_341,
        eligible_voters_summary=(
            "vorläufiges amtliches Endergebnis der Landtagswahl vom 8. März 2026"
        ),
        eligible_voters_source_url=(
            "https://im.baden-wuerttemberg.de/fileadmin/redaktion/m-im/intern/dateien/pdf/"
            "20260309_Anlage_PM_vorlaeufiges_Wahlergebnis.pdf"
        ),
    ),
    "2026-rlp": ElectionBaseline(
        population_total=4_122_500,
        population_summary=(
            "Statistisches Landesamt Rheinland-Pfalz, vorläufige Schätzung zum 31. Dezember 2025, rund 4,12 Mio."
        ),
        population_source_url=(
            "https://www.statistik.rlp.de/nachrichten/nachichtendetailseite/"
            "nahezu-stagnierende-bevoelkerungszahl-in-2025"
        ),
        eligible_voters_total=2_987_228,
        eligible_voters_summary=(
            "vorläufiges amtliches Endergebnis der Landtagswahl vom 22. März 2026"
        ),
        eligible_voters_source_url="https://rlp-ltw26.wahlen.23degrees.eu/wk/0000000000000/details",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--election-key", default="2026-bw")
    parser.add_argument("--config-path")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-png")
    parser.add_argument("--title")
    return parser.parse_args()


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def format_millions(value: float) -> str:
    return f"{value / 1_000_000:.2f}".replace(".", ",") + " Mio."


def format_bar_value(value: int, *, approximate: bool = False) -> str:
    if value >= 1_000_000:
        label = format_millions(value)
    else:
        label = format_int(value)
    return f"≈ {label}" if approximate else label


def format_axis(value: float, _: int) -> str:
    if abs(value) < 1:
        return "0"
    return format_millions(value)


def wrap_label(label: str) -> str:
    if label in LABEL_BREAKS:
        return LABEL_BREAKS[label]
    if len(label) <= 14:
        return label
    return "\n".join(textwrap.wrap(label, width=14))


def hex_to_rgb(color: str) -> tuple[float, float, float]:
    color = color.lstrip("#")
    return tuple(int(color[index : index + 2], 16) / 255.0 for index in (0, 2, 4))


def label_color_for_fill(color: str) -> str:
    red, green, blue = hex_to_rgb(color)
    luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue
    return "#FFFFFF" if luminance < 0.48 else CHART_COLORS["text"]


def load_config(election_key: str, config_path: str | None) -> Dict[str, Any]:
    path = Path(config_path) if config_path else ROOT / "config" / f"{election_key}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def get_election_baseline(election_key: str) -> ElectionBaseline:
    baseline = ELECTION_BASELINES.get(election_key)
    if baseline is None:
        supported = ", ".join(sorted(ELECTION_BASELINES))
        raise RuntimeError(f"Unsupported election key {election_key!r}. Supported keys: {supported}.")
    return baseline


def get_default_title(config: Dict[str, Any], election_key: str) -> str:
    election_name = str(config.get("election_name") or election_key)
    return f"{election_name}. Politische Repräsentation."


def get_second_vote_label(config: Dict[str, Any]) -> str:
    return str(config.get("second_vote_label") or DEFAULT_SECOND_VOTE_LABEL)


def load_land_snapshot(election_key: str) -> Dict[str, str]:
    path = ROOT / "data" / election_key / "latest" / "statla_snapshots.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("gebietsart") or "").strip() == "LAND":
                return row
    raise RuntimeError(f"LAND row missing in {path}")


def load_second_vote_party_totals(election_key: str, land_row_key: str) -> Dict[str, int]:
    path = ROOT / "data" / election_key / "latest" / "statla_party_results.csv"
    party_votes: Dict[str, int] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("row_key") != land_row_key:
                continue
            if (row.get("vote_type") or "").strip() != "Zweitstimmen":
                continue
            party_name = str(row.get("party_name") or "").strip()
            votes = int(str(row.get("votes") or "0").strip() or "0")
            party_votes[party_name] = votes
    if not party_votes:
        raise RuntimeError(f"No statewide second-vote party data found in {path}")
    return party_votes


def build_chart_rows(election_key: str, baseline: ElectionBaseline, second_vote_label: str) -> List[Dict[str, object]]:
    land_snapshot = load_land_snapshot(election_key)
    land_row_key = str(land_snapshot.get("row_key") or "").strip()
    if not land_row_key:
        raise RuntimeError("LAND snapshot row is missing row_key.")
    party_votes = load_second_vote_party_totals(election_key, land_row_key)

    voters_total = int(str(land_snapshot.get("voters_total") or "0").strip() or "0")
    valid_second_votes = int(str(land_snapshot.get("valid_votes_zweit") or "0").strip() or "0")
    invalid_second_votes = voters_total - valid_second_votes

    if voters_total <= 0 or valid_second_votes <= 0:
        raise RuntimeError("Statewide vote totals are empty.")
    if invalid_second_votes < 0:
        raise RuntimeError("Invalid second votes computed as negative.")
    if baseline.eligible_voters_total < voters_total:
        raise RuntimeError("Eligible-voter baseline is lower than voters total.")
    if baseline.population_total < baseline.eligible_voters_total:
        raise RuntimeError("Population baseline is lower than eligible voters.")

    qualifying_parties = sorted(
        (
            {
                "party": party,
                "votes": votes,
                "share": votes / valid_second_votes,
            }
            for party, votes in party_votes.items()
            if votes / valid_second_votes >= SECOND_VOTE_THRESHOLD
        ),
        key=lambda item: (-int(item["votes"]), str(item["party"])),
    )

    represented_total = sum(int(item["votes"]) for item in qualifying_parties)
    below_threshold_votes = valid_second_votes - represented_total
    if represented_total <= 0 or below_threshold_votes < 0:
        raise RuntimeError("Represented-vote decomposition is inconsistent.")

    rows: List[Dict[str, object]] = []

    def add_total(label: str, value: int, color: str, *, approximate: bool = False, role: str = "subtotal") -> None:
        rows.append(
            {
                "label": label,
                "display_label": wrap_label(label),
                "type": "total",
                "role": role,
                "amount": value,
                "signed_amount": value,
                "start": 0,
                "end": value,
                "color": color,
                "approximate": approximate,
            }
        )

    def add_delta(label: str, amount: int, current_before: int, color: str, *, approximate: bool = False, role: str = "delta") -> int:
        current_after = current_before - amount
        rows.append(
            {
                "label": label,
                "display_label": wrap_label(label),
                "type": "delta",
                "role": role,
                "amount": amount,
                "signed_amount": -amount,
                "start": current_before,
                "end": current_after,
                "color": color,
                "approximate": approximate,
            }
        )
        return current_after

    valid_label = f"Gültige {second_vote_label}"
    invalid_label = f"Ungültige {second_vote_label}"
    represented_label = f"Repräsentierte {second_vote_label}"

    current = baseline.population_total
    add_total("Einwohner", current, CHART_COLORS["start_total"], approximate=True, role="start_total")

    non_eligible = baseline.population_total - baseline.eligible_voters_total
    current = add_delta(
        "Nichtwahlberechtigte",
        non_eligible,
        current,
        CHART_COLORS["exclusion"],
        approximate=True,
        role="exclusion",
    )
    add_total("Wahlberechtigte", current, CHART_COLORS["subtotal"])

    non_voters = baseline.eligible_voters_total - voters_total
    current = add_delta("Nichtwähler", non_voters, current, CHART_COLORS["exclusion"], role="exclusion")
    add_total("Wählende", current, CHART_COLORS["subtotal"])

    current = add_delta(
        invalid_label,
        invalid_second_votes,
        current,
        CHART_COLORS["exclusion"],
        role="exclusion",
    )
    add_total(valid_label, current, CHART_COLORS["subtotal"])

    current = add_delta("Parteien < 5 %", below_threshold_votes, current, CHART_COLORS["exclusion"], role="exclusion")
    add_total(represented_label, current, CHART_COLORS["represented_total"], role="represented_total")

    for party in qualifying_parties:
        party_name = str(party["party"])
        current = add_delta(
            party_name,
            int(party["votes"]),
            current,
            PARTY_COLORS.get(party_name, CHART_COLORS["represented_total"]),
            role="represented_party",
        )

    if current != 0:
        raise RuntimeError(f"Waterfall does not end at zero: {current}")

    for index, row in enumerate(rows, start=1):
        row["order"] = index

    return rows


def write_csv_report(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "order",
                "label",
                "type",
                "role",
                "amount",
                "signed_amount",
                "start",
                "end",
                "approximate",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "order": row["order"],
                    "label": row["label"],
                    "type": row["type"],
                    "role": row["role"],
                    "amount": row["amount"],
                    "signed_amount": row["signed_amount"],
                    "start": row["start"],
                    "end": row["end"],
                    "approximate": row["approximate"],
                }
            )


def write_png_report(
    path: Path,
    rows: List[Dict[str, object]],
    title: str,
    baseline: ElectionBaseline,
    second_vote_label: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    fig, ax = plt.subplots(figsize=(16, 10))
    bar_width = 0.72
    max_value = max(int(row["start"]) for row in rows)  # first row equals population total
    upper_margin = max_value * 0.08
    label_threshold = max_value * 0.055
    inside_padding = max_value * 0.018

    for index, row in enumerate(rows):
        start = int(row["start"])
        end = int(row["end"])
        bottom = min(start, end)
        height = abs(start - end) if str(row["type"]) == "delta" else end
        if str(row["type"]) == "total":
            bottom = 0
            height = end

        color = str(row["color"])
        approximate = bool(row["approximate"])
        hatch = "///" if approximate else None
        edge_color = CHART_COLORS["approx_edge"] if approximate else color
        bar = ax.bar(
            index,
            height,
            bottom=bottom,
            width=bar_width,
            color=color,
            edgecolor=edge_color,
            linewidth=1.4 if approximate else 0.8,
            hatch=hatch,
            zorder=3,
        )[0]

        amount = int(row["amount"])
        label_text = format_bar_value(amount, approximate=approximate and str(row["role"]) in {"start_total", "exclusion"})
        fill_label_color = label_color_for_fill(color)
        text_x = bar.get_x() + bar.get_width() / 2.0

        if height >= label_threshold:
            if str(row["type"]) == "total":
                text_y = bottom + height - inside_padding
                va = "top"
            else:
                text_y = bottom + (height / 2.0)
                va = "center"
            ax.text(
                text_x,
                text_y,
                label_text,
                ha="center",
                va=va,
                fontsize=10,
                fontweight="bold",
                color=fill_label_color,
                zorder=4,
            )
        else:
            leader_top = bottom + height
            text_y = leader_top + max_value * 0.018
            ax.plot(
                [text_x, text_x],
                [leader_top, text_y - max_value * 0.006],
                color=CHART_COLORS["muted"],
                linewidth=0.9,
                zorder=4,
            )
            ax.text(
                text_x,
                text_y,
                label_text,
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
                color=CHART_COLORS["text"],
                zorder=4,
            )

    for left_row, right_row in zip(rows, rows[1:]):
        current_level = int(left_row["end"])
        left_x = int(left_row["order"]) - 1 + (bar_width / 2.0)
        right_x = int(right_row["order"]) - 1 - (bar_width / 2.0)
        ax.plot(
            [left_x, right_x],
            [current_level, current_level],
            color=CHART_COLORS["connector"],
            linewidth=1.2,
            linestyle=(0, (3, 2)),
            zorder=2,
        )

    ax.set_title(title, fontsize=19, fontweight="bold", loc="left", pad=18)
    ax.set_ylabel("Personen", fontsize=12)
    ax.set_xticks(range(len(rows)))
    ax.set_xticklabels([str(row["display_label"]) for row in rows], fontsize=10)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=7))
    ax.yaxis.set_major_formatter(FuncFormatter(format_axis))
    ax.set_ylim(0, max_value + upper_margin)
    ax.set_xlim(-0.8, len(rows) - 0.2)
    ax.grid(axis="y", color=CHART_COLORS["grid"], linewidth=0.8)
    ax.grid(axis="x", visible=False)
    ax.axhline(0, color=CHART_COLORS["text"], linewidth=1.1)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    party_legend_rows = [row for row in rows if str(row["role"]) == "represented_party"]
    legend_items = [
        Patch(facecolor=CHART_COLORS["start_total"], edgecolor=CHART_COLORS["start_total"], label="Ausgangswert / Zwischensumme"),
        Patch(facecolor=CHART_COLORS["exclusion"], edgecolor=CHART_COLORS["exclusion"], label="Nicht repräsentiert"),
        Patch(
            facecolor=CHART_COLORS["represented_total"],
            edgecolor=CHART_COLORS["represented_total"],
            label=f"Repräsentierte {second_vote_label}",
        ),
    ]
    legend_items.extend(
        Patch(facecolor=str(row["color"]), edgecolor=str(row["color"]), label=str(row["label"]))
        for row in party_legend_rows
    )
    legend_items.append(
        Patch(facecolor=CHART_COLORS["start_total"], edgecolor=CHART_COLORS["approx_edge"], hatch="///", label="Gerundeter Bevölkerungswert"),
    )
    ax.legend(
        handles=legend_items,
        loc="upper right",
        frameon=False,
        fontsize=10,
        ncol=2,
    )

    footnote = (
        f"Einwohner: {baseline.population_summary}. "
        f"Wahlberechtigte und {second_vote_label}: {baseline.eligible_voters_summary}. "
        "Die Balken für Einwohner und Nichtwahlberechtigte sind deshalb gerundet."
    )
    sources = f"Quellen: {baseline.population_source_url} | {baseline.eligible_voters_source_url}"
    fig.text(0.01, 0.045, footnote, ha="left", va="bottom", fontsize=9, color=CHART_COLORS["muted"])
    fig.text(0.01, 0.02, sources, ha="left", va="bottom", fontsize=8.5, color=CHART_COLORS["muted"])

    fig.tight_layout(rect=(0, 0.1, 1, 0.95))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    config = load_config(args.election_key, args.config_path)
    baseline = get_election_baseline(args.election_key)
    second_vote_label = get_second_vote_label(config)
    title = args.title or get_default_title(config, args.election_key)

    report_dir = ROOT / "data" / args.election_key / "reports"
    output_csv = Path(args.output_csv) if args.output_csv else report_dir / "statla_second_vote_representation_waterfall.csv"
    output_png = Path(args.output_png) if args.output_png else report_dir / "statla_second_vote_representation_waterfall.png"

    rows = build_chart_rows(args.election_key, baseline, second_vote_label)
    write_csv_report(output_csv, rows)
    write_png_report(output_png, rows, title, baseline, second_vote_label)

    represented_label = f"Repräsentierte {second_vote_label}"
    represented_total = next(int(row["amount"]) for row in rows if row["label"] == represented_label)
    print(
        json.dumps(
            {
                "election_key": args.election_key,
                "csv": str(output_csv),
                "png": str(output_png),
                "represented_second_votes": represented_total,
                "eligible_voters": baseline.eligible_voters_total,
                "population_total": baseline.population_total,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
