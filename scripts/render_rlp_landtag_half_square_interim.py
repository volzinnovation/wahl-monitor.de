#!/usr/bin/env python3
"""Render a half-square seat chart for the interim RLP landtag distribution."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from rlp_interim_seat_summary import RLP_INTERIM_SEAT_ROWS, RLP_INTERIM_TOTAL_SEATS


TITLE = "Sitzverteilung im Landtag von RLP"
NOTE = "Zwischenstand"
TOTAL_SEATS = RLP_INTERIM_TOTAL_SEATS
ROW_LENGTHS = [21, 19, 17, 15, 13, 11, 9]
OUTPUT_DIR = Path("data/2026-rlp/reports")
OUTPUT_STEM = "rlp-landtag-half-square-interim"

PARTY_ORDER = ["GRÜNE", "SPD", "CDU", "AfD"]
PARTIES = [
    {
        "name": str(row["party"]),
        "seats": int(row["seats"]),
        "color": str(row["color"]),
    }
    for party in PARTY_ORDER
    for row in RLP_INTERIM_SEAT_ROWS
    if str(row["party"]) == party
]

BG = "#F2EDE2"
CARD = "#FBF7EF"
TEXT = "#1B2530"
MUTED = "#5F6A73"
ACCENT_BG = "#E9D9B4"
ACCENT_TEXT = "#76561B"
GRID_STROKE = "#E6DED0"


def fmt_pct(value: float) -> str:
    return f"{value:.1f}".replace(".", ",") + " %"


def build_positions() -> list[tuple[int, int]]:
    max_cols = max(ROW_LENGTHS)
    positions: list[tuple[int, int]] = []
    for y, row_len in enumerate(ROW_LENGTHS):
        start_x = (max_cols - row_len) // 2
        for col in range(row_len):
            positions.append((start_x + col, y))
    return positions


def assign_parties() -> list[dict[str, str | int]]:
    positions = sorted(build_positions(), key=lambda item: (item[0], item[1]))
    seats: list[dict[str, str | int]] = []
    cursor = 0
    for party in PARTIES:
        for _ in range(party["seats"]):
            x, y = positions[cursor]
            seats.append(
                {
                    "x": x,
                    "y": y,
                    "name": party["name"],
                    "color": party["color"],
                }
            )
            cursor += 1
    if cursor != TOTAL_SEATS:
        raise ValueError(f"Expected {TOTAL_SEATS} seats, assigned {cursor}")
    return seats


def rounded_box(
    ax: plt.Axes,
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    facecolor: str,
    edgecolor: str = "none",
    linewidth: float = 0.0,
    radius: float = 0.18,
    alpha: float = 1.0,
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle=f"round,pad=0.02,rounding_size={radius}",
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=linewidth,
        alpha=alpha,
    )
    ax.add_patch(patch)
    return patch


def main() -> int:
    if sum(ROW_LENGTHS) != TOTAL_SEATS:
        raise ValueError("Half-square row lengths must sum to the total number of seats")
    if sum(party["seats"] for party in PARTIES) != TOTAL_SEATS:
        raise ValueError("Party seat counts must sum to the total number of seats")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14, 8.5), dpi=200, facecolor=BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8.5)
    ax.axis("off")

    rounded_box(ax, 0.35, 0.35, 13.3, 7.8, facecolor=CARD, radius=0.3)

    ax.text(
        0.9,
        7.55,
        TITLE,
        fontsize=27,
        fontweight=700,
        color=TEXT,
        family="DejaVu Sans",
    )
    ax.text(
        0.9,
        7.05,
        f"{TOTAL_SEATS} Sitze",
        fontsize=14,
        fontweight=600,
        color=MUTED,
        family="DejaVu Sans",
    )

    rounded_box(ax, 10.65, 7.08, 2.1, 0.55, facecolor=ACCENT_BG, radius=0.18)
    ax.text(
        11.7,
        7.355,
        NOTE,
        fontsize=13,
        fontweight=700,
        color=ACCENT_TEXT,
        ha="center",
        va="center",
        family="DejaVu Sans",
    )

    pitch = 0.42
    seat_size = 0.32
    chart_origin_x = 1.15
    chart_origin_y = 1.5

    for seat in assign_parties():
        rounded_box(
            ax,
            chart_origin_x + float(seat["x"]) * pitch,
            chart_origin_y + float(seat["y"]) * pitch,
            seat_size,
            seat_size,
            facecolor=str(seat["color"]),
            edgecolor=GRID_STROKE,
            linewidth=0.8,
            radius=0.06,
        )

    ax.text(
        0.9,
        0.8,
        "Hinweis: Zwischenstand, noch keine amtliche Endverteilung.",
        fontsize=11.5,
        color=MUTED,
        family="DejaVu Sans",
    )

    legend_x = 10.1
    legend_y = 5.85
    legend_gap = 1.15
    for index, party in enumerate(PARTIES):
        y = legend_y - index * legend_gap
        share = party["seats"] / TOTAL_SEATS * 100
        rounded_box(
            ax,
            legend_x,
            y,
            0.36,
            0.36,
            facecolor=party["color"],
            edgecolor="none",
            radius=0.08,
        )
        ax.text(
            legend_x + 0.52,
            y + 0.25,
            party["name"],
            fontsize=17,
            fontweight=700,
            color=TEXT,
            va="center",
            family="DejaVu Sans",
        )
        ax.text(
            legend_x + 0.52,
            y - 0.02,
            f"{party['seats']} Sitze · {fmt_pct(share)}",
            fontsize=12.5,
            color=MUTED,
            va="center",
            family="DejaVu Sans",
        )

    svg_path = OUTPUT_DIR / f"{OUTPUT_STEM}.svg"
    png_path = OUTPUT_DIR / f"{OUTPUT_STEM}.png"
    fig.savefig(svg_path, format="svg", bbox_inches="tight", facecolor=fig.get_facecolor())
    fig.savefig(png_path, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    print(svg_path)
    print(png_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
