#!/usr/bin/env python3
"""Map 2026 LSA precincts with Die Linke + SPD + GRÜNE > 50% of valid second votes.

Offline renderer: reads the captured official CSV and reviewed municipal geometry.
Requires matplotlib, shapely, pyproj. See the output directory README for sources.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET

os.environ.setdefault("MPLCONFIGDIR", "/tmp/lsa-majority-matplotlib")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import PathPatch, Patch
from matplotlib.path import Path as MplPath
from matplotlib.lines import Line2D
import matplotlib.patheffects as pe
from pyproj import Transformer
from shapely.geometry import shape, Polygon, mapping
from shapely.ops import unary_union, transform, polylabel
from shapely import make_valid

ROOT = Path(__file__).resolve().parents[1]
DEFAULT = ROOT / "data/2026-lsa/reports/linke-spd-gruene-majority"
INK, MUTED, PINK, PALE, EDGE = "#222a35", "#616c78", "#ad527c", "#e9edef", "#9aa3aa"
PARTIES = ("F03.Die Linke", "F04.SPD", "F06.GRÜNE")
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
    "text.color": INK, "axes.titlecolor": INK, "figure.facecolor": "white",
    "svg.fonttype": "none", "svg.hashsalt": "lsa-left-majority-2026"})


def jread(path):
    return json.loads(path.read_text())


def jwrite(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")


def fmt(value, digits=0):
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def load_results(out):
    with (out / "Ergebnisse_WBZ_LT_2026.csv").open(encoding="utf-8-sig", newline="") as f:
        raw = list(csv.DictReader(f, delimiter=";"))
    rows, seen = [], set()
    for r in raw:
        key = (r["Gemeindeschlüssel"], r["Wahlkreisnummer"], r["Wahlbezirk"].zfill(6))
        assert key not in seen, f"Duplicate precinct {key}"
        seen.add(key)
        valid = int(r["F.Gültige.Zweitstimmen"])
        assert valid > 0, f"Missing valid votes for {key}"
        votes = [int(r[p]) for p in PARTIES]
        assert sum(int(v) for k, v in r.items() if k.startswith("F") and k[1:3].isdigit()) == valid
        assert r["Wahllokal"] in ("U", "B")
        rows.append({"ags": key[0], "wahlkreis": key[1], "wahlbezirk": key[2],
            "gemeinde": r["Gemeindename"], "name": r["Wahlbezirksname"],
            "wahlart": r["Wahllokal"], "linke": votes[0], "spd": votes[1], "gruene": votes[2],
            "summe": sum(votes), "gueltige_zweitstimmen": valid,
            "anteil_prozent": 100 * sum(votes) / valid, "mehr_als_50_prozent": 2 * sum(votes) > valid})
    return rows


def polygon_path(geom):
    vertices, codes = [], []
    polygons = [geom] if isinstance(geom, Polygon) else list(geom.geoms)
    for poly in polygons:
        from shapely.geometry.polygon import orient
        poly = orient(poly, sign=1)
        for ring in [poly.exterior, *poly.interiors]:
            coords = list(ring.coords)
            vertices.extend(coords)
            codes.extend([MplPath.MOVETO] + [MplPath.LINETO] * (len(coords) - 2) + [MplPath.CLOSEPOLY])
    return MplPath(vertices, codes)


def draw_shape(ax, geom, color, edge=EDGE, lw=.5, gid=None, zorder=1):
    patch = PathPatch(polygon_path(geom), facecolor=color, edgecolor=edge,
                      linewidth=lw, zorder=zorder, joinstyle="round")
    if gid:
        patch.set_gid(gid)
    ax.add_patch(patch)


def join_geometry(source, rows, ags, mode, page_coordinates=False):
    lookup = {r["wahlbezirk"]: r for r in rows if r["ags"] == ags and r["wahlart"] == mode}
    assert len(lookup) == sum(r["ags"] == ags and r["wahlart"] == mode for r in rows)
    joined, seen = [], set()
    for f in source["features"]:
        key = str(f["properties"]["booth_id"]).zfill(6)
        assert key in lookup and key not in seen, f"Unmatched/duplicate geometry {ags}/{key}"
        seen.add(key)
        geom = make_valid(shape(f["geometry"]))
        assert geom.is_valid and not geom.is_empty, f"Invalid geometry {key}"
        if page_coordinates:
            geom = transform(lambda x, y, z=None: (x, -y), geom)
        joined.append({"geometry": geom, "row": lookup[key], "source_properties": f["properties"]})
    assert seen == set(lookup), f"Missing geometry for {set(lookup)-seen}"
    return joined


def draw_city(ax, joined, title, subtitle, labels=False, all_city=False, scale_meters=True, postal_short=False):
    selected = [f for f in joined if f["row"]["mehr_als_50_prozent"]]
    envelope = unary_union([f["geometry"] for f in joined if all_city or f["row"]["mehr_als_50_prozent"]])
    x0, y0, x1, y1 = envelope.bounds
    margin = max(x1-x0, y1-y0) * .08
    for f in sorted(joined, key=lambda f: f["row"]["mehr_als_50_prozent"]):
        r, g = f["row"], f["geometry"]
        chosen = r["mehr_als_50_prozent"]
        draw_shape(ax, g, PINK if chosen else PALE, "white" if chosen else EDGE,
                   .6 if chosen else .35, f"wbz-{r['ags']}-{r['wahlbezirk']}", 2 if chosen else 1)
        if labels and chosen:
            largest = g if isinstance(g, Polygon) else max(g.geoms, key=lambda p: p.area)
            pt = polylabel(largest, tolerance=max(x1-x0,y1-y0)/5000)
            label = str(int(r["wahlbezirk"]) - 90000) if postal_short else r["wahlbezirk"].lstrip("0")
            ax.text(pt.x, pt.y, label, fontsize=7.2, color="white",
                    ha="center", va="center", zorder=4,
                    path_effects=[pe.withStroke(linewidth=1.8, foreground=PINK)])
    ax.set_xlim(x0-margin, x1+margin)
    ax.set_ylim(y0-margin, y1+margin)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.text(0, 1.10, title, transform=ax.transAxes, fontsize=18, fontweight="bold")
    ax.text(0, 1.045, subtitle, transform=ax.transAxes, fontsize=10.5, color=MUTED)
    if scale_meters:
        length = 1000 if x1-x0 < 9000 else 2000
        sx, sy = x0, y0-margin*.60
        ax.plot([sx, sx+length], [sy, sy], color=INK, lw=2, zorder=5)
        ax.text(sx+length/2, sy+margin*.1, f"{length//1000} km", fontsize=8,
                ha="center", va="bottom", zorder=5)
    return selected


def add_svg_tooltips(svg_path, rows):
    ns = "http://www.w3.org/2000/svg"
    ET.register_namespace("", ns)
    tree = ET.parse(svg_path)
    index = {f"wbz-{r['ags']}-{r['wahlbezirk']}": r for r in rows}
    seen = Counter()
    for node in tree.iter():
        r = index.get(node.get("id"))
        if r:
            original_id = node.get("id")
            seen[original_id] += 1
            node.set("id", f"{original_id}-panel-{seen[original_id]}")
            title = ET.Element(f"{{{ns}}}title")
            title.text = (f"{r['gemeinde']} · {r['wahlbezirk']} · {r['name']}\n"
                f"Die Linke {r['linke']} + SPD {r['spd']} + GRÜNE {r['gruene']} = "
                f"{r['summe']} / {r['gueltige_zweitstimmen']} ({fmt(r['anteil_prozent'],2)} %)")
            node.insert(0, title)
    tree.write(svg_path, encoding="utf-8", xml_declaration=True)


def save(fig, out, name, rows):
    fig.savefig(out / f"{name}.png", dpi=180, facecolor="white")
    fig.savefig(out / f"{name}.svg", facecolor="white", metadata={"Date": None})
    add_svg_tooltips(out / f"{name}.svg", rows)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT)
    args = parser.parse_args()
    out = args.output_dir
    rows = load_results(out)
    majority = [r for r in rows if r["mehr_als_50_prozent"]]
    urn = [r for r in majority if r["wahlart"] == "U"]
    postal = [r for r in majority if r["wahlart"] == "B"]
    hall = join_geometry(jread(out / "sources/halle_urn_2026_epsg25832.geojson"), rows, "15002000", "U")
    hall_b = join_geometry(jread(out / "sources/halle_postal_2026_epsg25832.geojson"), rows, "15002000", "B")
    md_source = jread(out / "sources/magdeburg_urn_2026_page.json")
    md = join_geometry(md_source, rows, "15003000", "U", page_coordinates=True)
    for name, values in [("all_precincts", rows), ("majority_precincts", majority)]:
        with (out / f"{name}.csv").open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(values)
    # The state source is the project's official 2026 EPSG:25832 Wahlkreis geometry.
    state_features = jread(ROOT / "data/2026-lsa/metadata/wahlkreise.geojson")["features"]
    state = unary_union([shape(f["geometry"]).buffer(0) for f in state_features])
    to_utm = Transformer.from_crs(4326, 25832, always_xy=True)
    osm = jread(out / "friedensau_osm.json")
    nodes = {n['id']: (n['lon'], n['lat']) for n in osm['elements'] if n['type']=='node'}
    way = next(n for n in osm['elements'] if n['type']=='way')
    point = Polygon([nodes[n] for n in way['nodes']]).centroid
    friedensau = to_utm.transform(point.x, point.y)
    fig = plt.figure(figsize=(16, 10.5))
    fig.text(.045, .947, "Sachsen-Anhalt · Die Linke + SPD + Grüne", fontsize=25, fontweight="bold")
    fig.text(.045, .904, "Wahlbezirke mit mehr als 50 % der gültigen Zweitstimmen", fontsize=18)
    fig.text(.045, .864, "Landtagswahl am 6. September 2026 · Vorläufiges Ergebnis · Amtlicher Datenabruf 9. September 2026", fontsize=11.5, color=MUTED)
    fig.text(.045, .803, f"{len(urn)} von 2.150 Urnenwahlbezirken", fontsize=17, fontweight="bold", color=PINK)
    fig.text(.35, .805, f"Zusätzlich {len(postal)} von 511 Briefwahlbezirken; siehe Begleitkarte und vollständige Ergebnistabelle.", fontsize=10.5, color=MUTED)
    ax = fig.add_axes([.035, .195, .295, .52])
    draw_shape(ax, state, "#f2f4f5", EDGE, .65)
    x0, y0, x1, y1 = state.bounds
    ax.set_xlim(x0-15000, x1+15000); ax.set_ylim(y0-7000, y1+7000)
    ax.set_aspect("equal"); ax.axis("off")
    ax.text(0, 1.09, "Landesübersicht", transform=ax.transAxes, fontsize=18, fontweight="bold")
    ax.text(0, 1.035, "Lage der qualifizierenden Urnenwahlbezirke", transform=ax.transAxes, fontsize=9.5, color=MUTED)
    for name, xy, txt, offset, marker in [
        ("Halle (Saale)", unary_union([f['geometry'] for f in hall if f['row']['mehr_als_50_prozent']]).centroid.coords[0], "33 Wahlbezirke", (12,-26), "o"),
        ("Magdeburg", to_utm.transform(11.634,52.126), "16 Wahlbezirke", (-104,-25), "o"),
        ("Friedensau", friedensau, "1 Wahlbezirk · 50,41 %", (12,12), "D")]:
        ax.scatter(*xy, s=50, marker=marker, color=PINK, edgecolors="white", linewidths=1, zorder=5)
        ax.annotate(name+"\n"+txt, xy, xytext=offset, textcoords="offset points", fontsize=10,
                    va="center", zorder=6, arrowprops={"arrowstyle":"-","color":MUTED,"lw":.6},
                    bbox={"facecolor":"white","alpha":.87,"edgecolor":"none","pad":2})
    ax.text(.08, .01, "● Stadtlage / Detailkarte     ◆ Wahllokal", transform=ax.transAxes, fontsize=8, color=MUTED)
    draw_city(fig.add_axes([.365, .22, .265, .495]), hall, "Halle (Saale)", "33 von 126 Urnenwahlbezirken · Detailausschnitt")
    draw_city(fig.add_axes([.69, .22, .265, .495]), md, "Magdeburg", "16 von 148 Urnenwahlbezirken · Detailausschnitt", scale_meters=False)
    fig.legend(handles=[Patch(facecolor=PINK, edgecolor="white", label="Die Linke + SPD + Grüne > 50 %"),
                        Patch(facecolor=PALE, edgecolor=EDGE, label="Höchstens 50 %")],
               loc="lower left", bbox_to_anchor=(.365,.132), frameon=False, ncol=2, fontsize=11)
    fig.text(.045, .105, "Friedensau (Möckern), WBZ 000007: 7 + 35 + 19 = 61 von 121 gültigen Zweitstimmen. Raute: Wahllokal Ahornstraße 1; Bezirksgrenze nicht verfügbar.", fontsize=9)
    fig.text(.045, .074, "Berechnung: (Die Linke + SPD + GRÜNE) / gültige Zweitstimmen; strikte Mehrheit vor Rundung. Urnen- und Briefwahl werden getrennt ausgewertet.", fontsize=9, color=MUTED)
    fig.text(.045, .044, "Quellen: Statistisches Landesamt Sachsen-Anhalt; Datenquelle: Stadt Halle (Saale), CC BY 3.0 DE; Landeshauptstadt Magdeburg, Wahlbezirke 2026; © OpenStreetMap-Mitwirkende.", fontsize=8, color=MUTED)
    save(fig, out, "sachsen_anhalt_majority_map", rows)
    # Full-resolution detail sheets retain every booth in each city's geography.
    for slug, joined, city, desc, projected in [
        ("halle_urnenwahl", hall, "Halle (Saale)", "Urnenwahl", True),
        ("magdeburg_urnenwahl", md, "Magdeburg", "Urnenwahl", False),
        ("halle_briefwahl", hall_b, "Halle (Saale)", "Briefwahl", True)]:
        n = sum(f["row"]["mehr_als_50_prozent"] for f in joined)
        fig = plt.figure(figsize=(12, 11))
        fig.text(.055,.95, city+" · "+desc, fontsize=25, fontweight="bold")
        fig.text(.055,.904, f"Die Linke + SPD + Grüne > 50 % · {n} von {len(joined)} Wahlbezirken", fontsize=17)
        fig.text(.055,.864, "Landtagswahl 6. September 2026 · Gültige Zweitstimmen · Vorläufiges Ergebnis", fontsize=11, color=MUTED)
        draw_city(fig.add_axes([.05,.20,.29,.56]), joined, "Stadtgebiet", "Alle Wahlbezirke", all_city=True, scale_meters=projected)
        label_note = "Kartennummer + 90000 = amtliche Briefwahlbezirksnummer" if desc == "Briefwahl" else "Nummern der Wahlbezirke mit mehr als 50 %"
        draw_city(fig.add_axes([.41,.20,.54,.56]), joined, "Detail", label_note, labels=True, scale_meters=projected, postal_short=desc == "Briefwahl")
        fig.legend(handles=[Patch(facecolor=PINK,label="> 50 %"),Patch(facecolor=PALE,edgecolor=EDGE,label="≤ 50 %")],loc="lower left",bbox_to_anchor=(.41,.125),frameon=False,ncol=2)
        if desc == "Briefwahl":
            fig.text(.055,.10,"Landesweit: 55 von 511 Briefwahlbezirken > 50 %. Hier sind die 30 Halle-Bezirke mit amtlichen Flächen dargestellt.",fontsize=9.5)
            fig.text(.055,.074,"Weitere 24 in Magdeburg und 1 in Dessau-Roßlau: keine gesicherten Bezirksflächen im Kartenpaket; alle in der Ergebnistabelle.",fontsize=9.5,color=MUTED)
        else:
            fig.text(.055,.092,"Zahlen in den Flächen = amtliche Wahlbezirksnummern (ohne führende Nullen). SVG: Details beim Überfahren der Flächen.",fontsize=9.5,color=MUTED)
        source = "Datenquelle: Stadt Halle (Saale) · CC BY 3.0 DE" if projected else "Geometrie: Landeshauptstadt Magdeburg · Amtliche Wahlbezirkskarte 2026 (PDF)"
        fig.text(.055,.04,"Ergebnisse: Statistisches Landesamt Sachsen-Anhalt, Abruf 09.09.2026 · "+source,fontsize=8,color=MUTED)
        save(fig,out,slug,rows)
    geom_features=[]
    inverse=Transformer.from_crs(25832,4326,always_xy=True)
    for f in hall+hall_b:
        if f['row']['mehr_als_50_prozent']:
            geom_features.append({'type':'Feature','properties':f['row'], 'geometry':mapping(transform(inverse.transform,f['geometry']))})
    jwrite(out/'halle_majority.geojson',{'type':'FeatureCollection','features':geom_features})
    summary = {"total_precincts":len(rows), "majority_precincts":len(majority),"in_person_majority":len(urn),"postal_majority":len(postal),
        "in_person_total":sum(r['wahlart']=='U' for r in rows),"postal_total":sum(r['wahlart']=='B' for r in rows),
        "valid_second_votes":sum(r['gueltige_zweitstimmen'] for r in rows),
        "majorities_by_municipality_and_type":dict(Counter(r['gemeinde']+' / '+r['wahlart'] for r in majority)),
        "polygon_coverage":{"in_person":49,"postal":30},"point_coverage":{"in_person":1},
        "unmapped_postal_precincts":25,"threshold":"2 * (Linke + SPD + GRÜNE) > valid_second_votes (integer comparison)",
        "source_sha256":hashlib.sha256((out/'Ergebnisse_WBZ_LT_2026.csv').read_bytes()).hexdigest(),
        "checks":{"unique_precinct_keys":True,"all_party_sums_equal_denominators":True,"halle_all_186_polygons_joined":True,"magdeburg_all_148_polygons_joined":True}}
    jwrite(out/'summary.json',summary)
    print(json.dumps(summary,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
