#!/usr/bin/env python3
"""Export eight German tweet illustrations from the frozen demographic pilot."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

os.environ.setdefault("MPLCONFIGDIR", "/tmp/lsa-demographic-tweets-mpl")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.text import Text
from matplotlib.ticker import FuncFormatter, MultipleLocator, NullLocator
import numpy as np
import pandas as pd
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/2026-lsa/reports/demographic-feasibility"
OUT = SOURCE / "tweets-de"
INK, MUTED, BLUE, GRID, PAPER = "#222a35", "#66717e", "#3478a5", "#e2e6eb", "#ffffff"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 16, "text.color": INK,
    "axes.labelcolor": INK, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.edgecolor": MUTED, "axes.spines.top": False,
    "axes.spines.right": False, "axes.linewidth": .9,
    "svg.fonttype": "none", "savefig.facecolor": PAPER,
})

TWEETS = [
    "1/8 Sachsen-Anhalt 2026: Wie hängen Sozialstruktur und Zweitstimmen zusammen? Wir vergleichen alle 218 Gemeinden für die sechs Parteien über 5 %. Bevölkerung: 2025, Zensus: 2022. ρ steht für Spearman-Korrelation; jede Gemeinde zählt gleich. Wahlergebnis vorläufig.",
    "2/8 CDU und BSW zeigen bei den elf überall verfügbaren Merkmalen nur schwache Zusammenhänge. Die jeweils größte absolute Korrelation: CDU mit dem Anteil 18- bis 29-Jähriger (ρ=−0,15), BSW mit Bevölkerungsdichte (−0,13). Eine starke Erklärung liefert das noch nicht.",
    "3/8 Die SPD erzielt tendenziell niedrigere Zweitstimmenanteile in Gemeinden mit mehr selbstgenutztem Wohneigentum (ρ=−0,24). Ohne Halle, Magdeburg und Dessau-Roßlau beträgt der Zusammenhang −0,22. Das beschreibt Gemeinden, nicht das Wahlverhalten ihrer Eigentümer.",
    "4/8 Die Linke erzielt tendenziell höhere Zweitstimmenanteile in Gemeinden mit mehr Einpersonenhaushalten (ρ=+0,42). Ohne Halle, Magdeburg und Dessau-Roßlau liegt der Wert bei +0,40. Daraus lässt sich nicht ableiten, wie Alleinlebende persönlich gewählt haben.",
    "5/8 Die GRÜNEN sind tendenziell in einwohnerreicheren Gemeinden stärker: Einwohnerzahl und Zweitstimmenanteil korrelieren mit ρ=+0,36. Ohne Halle, Magdeburg und Dessau-Roßlau bleibt der Wert bei +0,34. Das Muster reicht somit über diese drei Städte hinaus.",
    "6/8 Bei der AfD verläuft der Zusammenhang umgekehrt: In einwohnerreicheren Gemeinden fällt ihr Zweitstimmenanteil tendenziell niedriger aus (ρ=−0,34). Ohne Halle, Magdeburg und Dessau-Roßlau beträgt der Wert −0,31. Gemeindegröße allein erklärt das Ergebnis nicht.",
    "7/8 Bildung zeigt stärkere Zusammenhänge: Der Anteil mit Abitur/Fachhochschulreife korreliert mit GRÜNE bei ρ=+0,76 und AfD bei −0,59. Aber: Diese Zensusdaten liegen nur für 54 der 218 Gemeinden vor. Die Ergebnisse gelten nicht automatisch für kleinere Gemeinden.",
    "8/8 Die wichtigste Grenze: Diese Gemeindekorrelationen belegen keine Ursachen und erklären keine individuellen Wahlentscheidungen. Die Auswertung ist explorativ. Erst gemeinsame Modelle mit Sozialstruktur, regionalen Unterschieden und Vorwahlergebnissen können Erklärungen prüfen.",
]

FEATURE_LABELS = {
    "log_population": "Einwohnerzahl",
    "log_density": "Bevölkerungsdichte",
    "age65_share": "Anteil 65+",
    "age18_29_share": "Anteil 18–29 Jahre",
    "male_share": "Männeranteil",
    "population_change_2024_25": "Bevölkerungsänderung",
    "foreign_citizenship_share": "Ausländische Staatsangehörigkeit",
    "single_household_share": "Einpersonenhaushalte",
    "owner_occupancy_share": "Wohneigentumsquote",
    "vacancy_share": "Wohnungsleerstand",
    "rent_eur_m2": "Nettokaltmiete je m²",
}


def number(value, digits=2, signed=False):
    result = f"{value:+.{digits}f}" if signed else f"{value:.{digits}f}"
    return result.replace(".", ",").replace("-", "−")


def tweet_length(text):
    # Twitter's one-weight ranges; these drafts contain no URLs or emoji.
    ranges = [(0, 4351), (8192, 8205), (8208, 8223), (8242, 8247)]
    return sum(1 if any(lo <= ord(c) <= hi for lo, hi in ranges) else 2 for c in text)


def base(index, title, subtitle, source):
    fig = plt.figure(figsize=(16, 10), dpi=100, facecolor=PAPER)
    fig.text(.055, .948, "SACHSEN-ANHALT 2026  /  GEMEINDEN & WAHLERGEBNIS", fontsize=14, color=MUTED)
    fig.text(.945, .948, f"{index:02d} / 08", fontsize=14, ha="right", color=MUTED)
    fig.text(.055, .880, title, fontsize=30, fontweight="bold")
    fig.text(.055, .826, subtitle, fontsize=17)
    fig.add_artist(Line2D([.055, .945], [.12, .12], transform=fig.transFigure, color=GRID, lw=1))
    fig.text(.055, .080, source, fontsize=12, color=MUTED)
    fig.text(.055, .047, "Wahlstand: 07.09.2026, 04:06 MESZ · vorläufig · Auswertung: wahl-monitor.de", fontsize=12, color=MUTED)
    return fig


def style_axes(ax, grid="both"):
    ax.set_axisbelow(True)
    ax.grid(axis=grid, color=GRID, lw=.8)
    ax.tick_params(length=0, pad=8, labelsize=15)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, pos: number(v, 0)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, pos: number(v, 0)))


def note(fig, first, second=None):
    fig.text(.055, .189 if second else .168, first, fontsize=16, fontweight="bold")
    if second:
        fig.text(.055, .153, second, fontsize=14, color=MUTED)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(SOURCE / "municipality_analysis.csv", dtype={"ags": str, "kreis": str})
    correlations = pd.read_csv(SOURCE / "party_correlations.csv")
    maxima = pd.read_csv(SOURCE / "party_strongest_descriptive_associations.csv").set_index("party")
    validation = json.loads((SOURCE / "validation.json").read_text())
    election_path = ROOT / validation["election_source"]
    election = json.loads(election_path.read_text())["lsa:LAND:15:TOTAL"]
    assert hashlib.sha256(election_path.read_bytes()).hexdigest() == validation["election_sha256"]
    parties = {election["party_names"][code]: code for code, votes in election["parties"].items()
               if code.startswith("F") and votes / election["valid_votes_zweit"] > .05}
    assert set(parties) == {"CDU", "BSW", "SPD", "Die Linke", "GRÜNE", "AfD"}
    assert len(frame) == 218 and frame.ags.nunique() == 218
    assert int(frame.valid_second_votes.sum()) == election["valid_votes_zweit"]
    without = ~frame.ags.isin(["15001000", "15002000", "15003000"])
    records, bounds_qa, chart_contract = [], [], []

    def correlation(party, feature):
        row = correlations[(correlations.party == party) & (correlations.feature == feature)].iloc[0]
        sub = frame.dropna(subset=[feature, f"{parties[party]}_share"])
        # Independently recompute the Spearman statistic from plotted points.
        actual = sub[feature].rank().corr(sub[f"{parties[party]}_share"].rank())
        assert np.isclose(actual, row.spearman, atol=1e-12)
        subset = sub.loc[without.reindex(sub.index)]
        reduced = subset[feature].rank().corr(subset[f"{parties[party]}_share"].rank())
        assert np.isclose(reduced, row.spearman_without_three_cities, atol=1e-12)
        assert len(sub) == row.n
        return row

    def save(fig, index, slug, alt, family, fields, n, takeaway):
        name = f"{index:02d}-{slug}.png"
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        width, height = fig.canvas.get_width_height()
        outside = []
        for artist in fig.findobj(Text):
            if not artist.get_visible() or not artist.get_text().strip():
                continue
            if artist.axes is not None:
                # Invisible off-limit tick objects are excluded by Matplotlib at draw time.
                axis_text = artist in artist.axes.get_xticklabels() + artist.axes.get_yticklabels()
                if axis_text:
                    continue
            box = artist.get_window_extent(renderer)
            if box.x0 < 0 or box.y0 < 0 or box.x1 > width or box.y1 > height:
                outside.append(artist.get_text())
        assert not outside, outside
        header_boxes = [(artist.get_text(), artist.get_window_extent(renderer)) for artist in fig.texts]
        collisions = [(left[0], right[0]) for i, left in enumerate(header_boxes)
                      for right in header_boxes[i + 1:] if left[1].overlaps(right[1])]
        assert not collisions, collisions
        fig.savefig(OUT / name, dpi=100)
        plt.close(fig)
        with Image.open(OUT / name) as im:
            assert im.size == (1600, 1000)
        count = tweet_length(TWEETS[index - 1])
        assert count <= 280
        records.append({"index": index, "tweet": TWEETS[index - 1], "weighted_characters": count,
                        "image": name, "alt_text": alt})
        bounds_qa.append({"image": name, "pixels": [1600, 1000], "text_outside_canvas": outside})
        chart_contract.append({"image": name, "family": family, "fields": fields,
                               "observations": n, "takeaway": takeaway,
                               "renderer": "Matplotlib/Agg, static PNG", "palette": "single-root blue with neutral references",
                               "non_color_encoding": "direct labels, numeric axes, filled/open markers",
                               "grain": "municipality except chart 1 (statewide party totals)"})

    fig = base(1, "Die sechs Parteien über 5 Prozent", "Landesweite Zweitstimmenanteile · 1.315.315 gültige Zweitstimmen", "Quelle: Statistisches Landesamt Sachsen-Anhalt, archivierter Ergebnisstand.")
    ax = fig.add_axes([.16, .285, .55, .47])
    ranked = sorted(parties, key=lambda p: election["parties"][parties[p]], reverse=True)
    values = [100 * election["parties"][parties[p]] / election["valid_votes_zweit"] for p in ranked]
    ax.barh(range(6), values, height=.58, color=BLUE, edgecolor=BLUE, linewidth=.8)
    ax.set_yticks(range(6), ranked)
    ax.invert_yaxis()
    ax.set_xlim(0, 50)
    ax.set_xlabel("Zweitstimmenanteil (%)", labelpad=15)
    ax.grid(axis="x", color=GRID, lw=.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0, pad=9)
    ax.spines["left"].set_visible(False)
    for i, value in enumerate(values):
        ax.text(value + .65, i, number(value, 2) + " %", va="center", fontsize=17, fontweight="bold")
    fig.text(.76, .65, "218", fontsize=52, fontweight="bold", color=BLUE)
    fig.text(.76, .599, "Gemeinden", fontsize=19)
    fig.text(.76, .50, "11", fontsize=42, fontweight="bold", color=BLUE)
    fig.text(.76, .451, "Merkmale mit\nvollständigen Daten", fontsize=17, linespacing=1.5, va="top")
    note(fig, "Für die Korrelation zählt jede Gemeinde gleich.", "Demografie: Bevölkerung 2025 und Zensus 2022 · Verknüpfung über amtliche Gemeindeschlüssel.")
    save(fig, 1, "parteien-und-datenbasis", "Balkendiagramm der sechs Parteien über fünf Prozent: " + "; ".join(p + " " + number(v) + " Prozent" for p, v in zip(ranked, values)) + ". Die Korrelationsanalyse umfasst 218 Gemeinden und elf vollständig verfügbare Merkmale.", "horizontal bar", ["statewide second votes", "party"], 6, "Six parties qualify; the pilot has 218 municipalities.")

    fig = base(2, "CDU und BSW: elf Merkmale im Vergleich", "Spearman-Korrelation mit dem Zweitstimmenanteil · jeweils 218 Gemeinden", "Quellen: Statistisches Landesamt Sachsen-Anhalt; Destatis, GV-ISys; Zensus 2022.")
    ax = fig.add_axes([.355, .295, .56, .465])
    keys = list(FEATURE_LABELS)
    for party, offset, color, fill in [("CDU", -.11, BLUE, BLUE), ("BSW", .11, INK, PAPER)]:
        vals = [correlation(party, feat).spearman for feat in keys]
        ax.scatter(vals, np.arange(len(keys)) + offset, s=85, facecolors=fill, edgecolors=color, linewidths=1.5, zorder=3, label=party)
    ax.set_yticks(range(len(keys)), [FEATURE_LABELS[k] for k in keys], fontsize=14)
    ax.invert_yaxis()
    ax.set_xlim(-1, 1)
    ax.set_xticks([-1, -.5, 0, .5, 1])
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, pos: number(v, 1)))
    ax.axvline(0, color=MUTED, lw=1)
    ax.grid(axis="x", color=GRID, lw=.8)
    ax.tick_params(length=0, pad=9)
    ax.spines["left"].set_visible(False)
    ax.set_xlabel("gegenläufig   ←   Spearman ρ   →   gleichgerichtet", labelpad=12)
    ax.legend(loc="upper right", frameon=False, fontsize=16)
    note(fig, "Größter Betrag: CDU −0,15 (18–29 Jahre) · BSW −0,13 (Dichte).", "In diesen elf Einzelvergleichen zeigen beide Parteien nur schwache Zusammenhänge.")
    save(fig, 2, "cdu-bsw", "Punktdiagramm mit elf Merkmalen und einer Korrelationsachse von minus eins bis plus eins. CDU als gefüllte blaue Punkte, BSW als offene Punkte. Alle Werte liegen nahe null. Größter absoluter Wert: CDU mit dem Anteil 18- bis 29-Jähriger minus 0,15; BSW mit Bevölkerungsdichte minus 0,13. Je 218 Gemeinden.", "paired dot", ["11 full-coverage features", "CDU share", "BSW share"], 218, "All 22 correlations have small absolute magnitude.")

    def scatter_card(index, party, feature, xfield, xlabel, title, source, takeaway, detail, xlim, ylim, xticks=None, log=False):
        row = correlation(party, feature)
        fig = base(index, title, "Zweitstimmen 2026 · ein Punkt = eine Gemeinde · n = 218 · gleich gewichtet", source)
        ax = fig.add_axes([.10, .30, .605, .465])
        style_axes(ax)
        ax.scatter(frame[xfield], frame[f"{parties[party]}_share"], s=47, c=BLUE, alpha=.66, edgecolors=PAPER, linewidths=.45, zorder=3)
        ax.set(xlim=xlim, ylim=ylim, xlabel=xlabel, ylabel="Zweitstimmenanteil (%)")
        ax.xaxis.labelpad = 14
        ax.yaxis.labelpad = 14
        if log:
            ax.set_xscale("log")
            ax.xaxis.set_minor_locator(NullLocator())
            ax.set_xticks(xticks, ["1.000", "5.000", "20.000", "100.000"])
        elif xticks is not None:
            ax.set_xticks(xticks)
        ax.yaxis.set_major_locator(MultipleLocator(5 if ylim[1] <= 25 else 10))
        fig.text(.758, .708, "Spearman ρ", fontsize=18, color=MUTED)
        fig.text(.751, .630, number(row.spearman, signed=True), fontsize=42, fontweight="bold", color=BLUE)
        fig.text(.758, .505, "Ohne die drei\nkreisfreien Städte*", fontsize=16, linespacing=1.4, color=MUTED)
        fig.text(.758, .427, number(row.spearman_without_three_cities, signed=True), fontsize=32, fontweight="bold")
        fig.text(.758, .370, "n = 215", fontsize=16, color=MUTED)
        note(fig, takeaway, detail)
        fig.text(.758, .271, "* Halle, Magdeburg,\n  Dessau-Roßlau", fontsize=12, color=MUTED, linespacing=1.4)
        alt = f"Streudiagramm: {xlabel} auf der x-Achse, Zweitstimmenanteil {party} auf der y-Achse. 218 Gemeinden. Spearman-Korrelation {number(row.spearman, signed=True)}; ohne Halle, Magdeburg und Dessau-Roßlau {number(row.spearman_without_three_cities, signed=True)} bei 215 Gemeinden. {takeaway} {detail}"
        save(fig, index, party.lower().replace(" ", "-").replace("ü", "ue"), alt, "scatter", [xfield, f"{parties[party]}_share", "ags", "kreis", "valid_second_votes"], 218, takeaway)

    scatter_card(3, "SPD", "owner_occupancy_share", "owner_occupancy_share", "Wohneigentumsquote 2022 (%)", "SPD und selbstgenutztes Wohneigentum", "Quellen: Statistisches Landesamt Sachsen-Anhalt; Zensus 2022, Gebäude- und Wohnungszählung.", "Mehr Wohneigentum geht tendenziell mit niedrigeren SPD-Anteilen einher.", "Quote: selbstgenutzte Wohnungen / bewohnte Wohnungen in Wohngebäuden, ohne Wohnheime.", (10, 95), (0, 20), [20, 40, 60, 80])
    scatter_card(4, "Die Linke", "single_household_share", "single_household_share", "Einpersonenhaushalte 2022 (%)", "Die Linke und Einpersonenhaushalte", "Quellen: Statistisches Landesamt Sachsen-Anhalt; Zensus 2022, Haushalte.", "Mehr Einpersonenhaushalte gehen tendenziell mit höheren Linke-Anteilen einher.", "Anteil an allen privaten Haushalten · Kein Rückschluss auf die Wahlentscheidung Alleinlebender.", (20, 60), (0, 15), [20, 30, 40, 50, 60])
    scatter_card(5, "GRÜNE", "log_population", "population_2025", "Einwohnerzahl 2025 (logarithmische Skala)", "GRÜNE und Gemeindegröße", "Quelle: Statistisches Landesamt Sachsen-Anhalt, Wahlergebnis und Bevölkerungsstatistik 2025.", "Die GRÜNEN sind tendenziell in einwohnerreicheren Gemeinden stärker.", "Der Zusammenhang bleibt auch ohne die drei kreisfreien Städte positiv.", (600, 300000), (0, 25), [1000, 5000, 20000, 100000], True)
    scatter_card(6, "AfD", "log_population", "population_2025", "Einwohnerzahl 2025 (logarithmische Skala)", "AfD und Gemeindegröße", "Quelle: Statistisches Landesamt Sachsen-Anhalt, Wahlergebnis und Bevölkerungsstatistik 2025.", "Die AfD ist tendenziell in einwohnerärmeren Gemeinden stärker.", "Der Zusammenhang bleibt auch ohne die drei kreisfreien Städte negativ.", (600, 300000), (0, 70), [1000, 5000, 20000, 100000], True)

    fig = base(7, "Bildungsstruktur und Zweitstimmen", "Anteil mit Abitur/Fachhochschulreife 2022 · nur 54 von 218 Gemeinden mit Daten", "Quellen: Statistisches Landesamt Sachsen-Anhalt; Zensus 2022, höchster Schulabschluss.")
    sub = frame.dropna(subset=["abitur_share"])
    assert len(sub) == 54
    for party, left in [("GRÜNE", .10), ("AfD", .565)]:
        row = correlation(party, "abitur_share")
        ax = fig.add_axes([left, .295, .36, .405])
        style_axes(ax)
        ax.scatter(sub.abitur_share, sub[f"{parties[party]}_share"], s=66, color=BLUE, edgecolors=PAPER, linewidths=.55, alpha=.78, zorder=3)
        ax.set(xlim=(10, 42), ylim=(0, 70), xlabel="Abitur/Fachhochschulreife (%)")
        ax.set_xticks([10, 20, 30, 40])
        ax.yaxis.set_major_locator(MultipleLocator(10))
        ax.xaxis.labelpad = 14
        ax.set_ylabel("Zweitstimmenanteil (%)", labelpad=12)
        fig.text(left, .744, party, fontsize=23, fontweight="bold")
        fig.text(left + .36, .744, "ρ = " + number(row.spearman, signed=True), fontsize=23, fontweight="bold", color=BLUE, ha="right")
    note(fig, "Die stärkeren Zusammenhänge gelten zunächst nur für diese 54 Gemeinden.", "Bildungsanteil: Personen ab 15 Jahren in der Zensus-Haushaltsstichprobe · Keine Aussage über Einzelpersonen.")
    save(fig, 7, "bildung", "Zwei Streudiagramme mit identischen Achsen. X: Anteil der Personen ab 15 Jahren mit Abitur oder Fachhochschulreife im Zensus 2022. Y: Zweitstimmenanteil 2026 von null bis siebzig Prozent. GRÜNE: Spearman plus 0,76; AfD: minus 0,59. Nur 54 der 218 Gemeinden haben Bildungsdaten. Die Ergebnisse sind nicht ohne Weiteres auf kleinere Gemeinden übertragbar.", "faceted scatter", ["abitur_share", "F6_share", "F2_share", "ags", "kreis", "valid_second_votes"], 54, "The stronger education associations are restricted to 54 municipalities.")

    fig = base(8, "Vergleich mit und ohne kreisfreie Städte", "Je Partei: stärkste absolute Korrelation unter elf vollständig verfügbaren Merkmalen", "Quellen: Statistisches Landesamt Sachsen-Anhalt; Zensus 2022; Destatis, GV-ISys.")
    ax = fig.add_axes([.315, .30, .61, .42])
    order = ["CDU", "BSW", "SPD", "Die Linke", "GRÜNE", "AfD"]
    labels = []
    for i, party in enumerate(order):
        row = correlation(party, maxima.loc[party, "feature"])
        labels.append(party + " · " + FEATURE_LABELS[row.feature])
        ax.plot([row.spearman, row.spearman_without_three_cities], [i, i], color=MUTED, lw=1.5, zorder=2)
        ax.scatter(row.spearman, i, s=100, color=BLUE, edgecolor=BLUE, zorder=4)
        ax.scatter(row.spearman_without_three_cities, i, s=100, facecolor=PAPER, edgecolor=INK, lw=1.4, zorder=5, marker="D")
    ax.set_yticks(range(6), labels, fontsize=14)
    ax.invert_yaxis()
    ax.set_xlim(-1, 1)
    ax.set_xticks([-1, -.5, 0, .5, 1])
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, pos: number(v, 1)))
    ax.axvline(0, color=MUTED, lw=1)
    ax.grid(axis="x", color=GRID, lw=.8)
    ax.tick_params(length=0, pad=9)
    ax.spines["left"].set_visible(False)
    ax.set_xlabel("Spearman-Korrelation ρ", labelpad=15)
    handles = [Line2D([], [], color=BLUE, marker="o", linestyle="", markersize=10, label="Alle 218 Gemeinden"),
               Line2D([], [], color=INK, marker="D", markerfacecolor=PAPER, linestyle="", markersize=9, label="Ohne die drei Städte: 215")]
    fig.legend(handles=handles, loc="center", bbox_to_anchor=(.62, .764), ncol=2, frameon=False, fontsize=15)
    note(fig, "Ähnliche Korrelationen belegen keine Ursachen oder individuellen Wahlmotive.", "Ausgelassen: Halle, Magdeburg, Dessau-Roßlau · Einzelvergleiche ersetzen kein gemeinsames Erklärungsmodell.")
    save(fig, 8, "aussagegrenzen", "Vergleich der stärksten vollständigen Gemeindekorrelation je Partei. Gefüllter Kreis: alle 218 Gemeinden. Offene Raute: 215 Gemeinden ohne Halle, Magdeburg und Dessau-Roßlau. CDU: minus 0,15 versus minus 0,13; BSW: minus 0,13 versus minus 0,13; SPD: minus 0,24 versus minus 0,22; Die Linke: plus 0,42 versus plus 0,40; GRÜNE: plus 0,36 versus plus 0,34; AfD: minus 0,34 versus minus 0,31. Die ähnlichen Werte belegen keine Ursachen und keine individuellen Wahlmotive.", "paired dot", ["party", "strongest full-coverage feature", "spearman", "spearman_without_three_cities"], [218, 215], "Removing three independent cities changes these associations little, without establishing causality.")

    manifest = {"format": "1600 × 1000 PNG", "order": "editorial priority, low to high", "party_filter": "statewide second-vote share >5% in frozen snapshot", "source_snapshot": validation["source_snapshot"], "sources": {}, "tweets": records, "chart_contracts": chart_contract}
    for path in [SOURCE / "municipality_analysis.csv", SOURCE / "party_correlations.csv", SOURCE / "party_strongest_descriptive_associations.csv", election_path]:
        manifest["sources"][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    (OUT / "tweets.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    md = ["# Sachsen-Anhalt: acht Tweets mit Bildern", "", "Priorität von niedrig zu hoch. Nur Parteien mit mehr als 5 % der landesweiten Zweitstimmen im ausgewerteten Stand. Alle Tweets inklusive Nummerierung höchstens 280 gewichtete Zeichen.", ""]
    for record in records:
        md += [record["tweet"], "", f'![{record["alt_text"]}]({record["image"]})', "", "**Alternativtext:** " + record["alt_text"], ""]
    (OUT / "tweets.md").write_text("\n".join(md))
    (OUT / "README.md").write_text("# Reproduction\n\nRun `python3 scripts/render_lsa_demographic_tweets.py` from the repository root with Matplotlib, NumPy, pandas and Pillow installed. The exporter reads only the retained demographic pilot and its frozen election snapshot. It does not refresh data.\n\nEight 1600 × 1000 PNGs correspond to tweets.md and tweets.json in increasing editorial priority. Alt text and input hashes are retained. All 218 municipalities have equal weight in correlations; education has 54 observations. Spearman statistics and their 215/51-row sensitivity checks are independently recalculated from plotted data. No fitted line, confidence interval, causal claim, or individual voting inference is drawn. Population charts intentionally share a logarithmic x-axis; education panels share both axis ranges.\n\nThe one-root palette is neutral blue for this nonpartisan series. Direct labels and filled/open markers convey identity without depending on color. Repeated scatter charts answer the same party/feature association question using separate fields. Tweet 8 uses paired dots for a sensitivity comparison, not uncertainty intervals.\n")
    qa = {"data_checks_passed": True, "party_filter": sorted(parties), "tweet_weighted_characters": [r["weighted_characters"] for r in records], "exports": bounds_qa, "visual_review": "pending"}
    (OUT / "qa.json").write_text(json.dumps(qa, ensure_ascii=False, indent=2) + "\n")
    with ZipFile(OUT / "sachsen-anhalt-tweets-mit-bildern.zip", "w", ZIP_DEFLATED) as archive:
        for path in sorted(OUT.iterdir()):
            if path.suffix in {".png", ".md", ".json"}:
                archive.write(path, path.name)
        archive.write(Path(__file__), "render_lsa_demographic_tweets.py")
    print(json.dumps({"output": str(OUT), "images": len(records), "character_counts": qa["tweet_weighted_characters"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
