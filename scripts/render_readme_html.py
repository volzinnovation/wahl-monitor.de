#!/usr/bin/env python3
"""Render README.md to HTML with official StatLA party colors."""

from __future__ import annotations

import argparse
import re
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README_MD = ROOT / "README.md"
README_HTML = ROOT / "README.html"

PARTY_COLORS = {
    "GRÜNE": "#008939",
    "CDU": "#2d3c4b",
    "SPD": "#e3000f",
    "FDP": "#ffed00",
    "AfD": "#00ccff",
    "Die Linke": "#e6007b",
    "FREIE WÄHLER": "#F29204",
    "Die PARTEI": "#b91023",
    "dieBasis": "#00cdd8",
    "KlimalisteBW": "#e1ede0",
    "ÖDP": "#ffa338",
    "Volt": "#502379",
    "Bündnis C": "#00529c",
    "BSW": "#a21749",
    "Die Gerechtigkeitspartei": "#d4d0d3",
    "Tierschutzpartei": "#018787",
    "Werteunion": "#646464",
    "WerteUnion": "#646464",
    "PdH": "#ededed",
    "Verjüngungsforschung": "#b5b2b4",
    "PDR": "#7e68b0",
    "PdF": "#FFB27F",
    "Anderer Kreiswahlvorschlag": "#eeeeee",
}

PARTY_CELL_RE = re.compile(r"<td>([^<]+)</td>")
PARTY_SUMMARY_RE = re.compile(r"<summary>([^<]+)</summary>")

STYLE_BLOCK = """
    .party-name {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      padding-left: 0.625rem;
      border-left: 0.4rem solid var(--party-color);
      background: linear-gradient(90deg, color-mix(in srgb, var(--party-color) 14%, white), rgba(255,255,255,0));
      font-weight: 600;
    }
    .party-swatch {
      width: 0.82rem;
      height: 0.82rem;
      border-radius: 999px;
      background: var(--party-color);
      border: 1px solid rgba(0, 0, 0, 0.12);
      flex: 0 0 auto;
    }
    summary.party-summary {
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.2rem 0.45rem 0.2rem 0.35rem;
      border-left: 0.35rem solid var(--party-color);
      background: color-mix(in srgb, var(--party-color) 12%, white);
      font-weight: 600;
    }
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render README.md to README.html with party colors.")
    parser.add_argument("--input", type=Path, default=README_MD)
    parser.add_argument("--output", type=Path, default=README_HTML)
    return parser.parse_args()


def render_markdown(input_path: Path, output_path: Path) -> None:
    subprocess.run(
        ["pandoc", "-s", str(input_path), "-o", str(output_path)],
        cwd=ROOT,
        check=True,
    )


def inject_styles(html_text: str) -> str:
    if STYLE_BLOCK.strip() in html_text:
        return html_text
    return html_text.replace("</style>", f"{STYLE_BLOCK}\n  </style>", 1)


def decorate_party_cell(match: re.Match[str]) -> str:
    party = match.group(1)
    color = PARTY_COLORS.get(party)
    if not color:
        return match.group(0)
    return (
        f'<td class="party-name" style="--party-color: {color};">'
        f'<span class="party-swatch" aria-hidden="true"></span>{party}</td>'
    )


def decorate_party_summary(match: re.Match[str]) -> str:
    party = match.group(1)
    color = PARTY_COLORS.get(party)
    if not color:
        return match.group(0)
    return (
        f'<summary class="party-summary" style="--party-color: {color};">'
        f'<span class="party-swatch" aria-hidden="true"></span>{party}</summary>'
    )


def postprocess_html(output_path: Path) -> None:
    html_text = output_path.read_text(encoding="utf-8")
    html_text = inject_styles(html_text)
    html_text = PARTY_CELL_RE.sub(decorate_party_cell, html_text)
    html_text = PARTY_SUMMARY_RE.sub(decorate_party_summary, html_text)
    output_path.write_text(html_text, encoding="utf-8")


def main() -> int:
    args = parse_args()
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_output = Path(tmpdir) / "README.html"
        render_markdown(args.input, temp_output)
        postprocess_html(temp_output)
        args.output.write_text(temp_output.read_text(encoding="utf-8"), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
