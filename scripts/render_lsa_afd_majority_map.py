#!/usr/bin/env python3
"""Render AfD >50% at precinct level, with a municipality overview and city detail.

Uses the same captured preliminary results as render_lsa_left_majority_map.py.
Municipality colors classify how many constituent precincts pass the threshold;
they are never a substitute calculation of municipality-wide AfD vote shares.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
from pathlib import Path
import xml.etree.ElementTree as ET

import render_lsa_left_majority_map as base
from shapely import make_valid
from shapely.geometry import shape, mapping
from shapely.ops import unary_union, transform
from pyproj import Transformer
from matplotlib.patches import Patch

ROOT = base.ROOT
SOURCE_DIR = base.DEFAULT
DEFAULT = ROOT / "data/2026-lsa/reports/afd-majority"
BLUE, LIGHT_BLUE = "#3478a5", "#b6d2e4"
base.PINK = BLUE
plt = base.plt


def load_results():
    source = SOURCE_DIR / "Ergebnisse_WBZ_LT_2026.csv"
    with source.open(encoding="utf-8-sig", newline="") as handle:
        raw = list(csv.DictReader(handle, delimiter=";"))
    rows, keys = [], set()
    for r in raw:
        key = (r["Gemeindeschlüssel"], r["Wahlkreisnummer"], r["Wahlbezirk"].zfill(6))
        assert key not in keys, f"Duplicate precinct {key}"
        keys.add(key)
        valid, afd = int(r["F.Gültige.Zweitstimmen"]), int(r["F02.AfD"])
        assert valid > 0 and 0 <= afd <= valid
        assert sum(int(v) for k,v in r.items() if k.startswith("F") and k[1:3].isdigit()) == valid
        assert r["Wahllokal"] in ("U", "B")
        rows.append({"ags":key[0], "wahlkreis":key[1], "wahlbezirk":key[2],
                     "gemeinde":r["Gemeindename"], "name":r["Wahlbezirksname"],
                     "wahlart":r["Wahllokal"], "afd":afd, "gueltige_zweitstimmen":valid,
                     "anteil_prozent":100*afd/valid, "mehr_als_50_prozent":2*afd>valid,
                     "genau_50_prozent":2*afd==valid})
    return rows


def municipality_summary(rows):
    groups = defaultdict(list)
    for r in rows:
        groups[r['ags']].append(r)
    result = {}
    for ags, items in sorted(groups.items()):
        urn = [r for r in items if r['wahlart']=='U']
        postal = [r for r in items if r['wahlart']=='B']
        count = sum(r['mehr_als_50_prozent'] for r in urn)
        result[ags] = {'ags':ags, 'gemeinde':items[0]['gemeinde'],
            'urnenwahlbezirke':len(urn), 'urnenwahlbezirke_afd_ueber_50':count,
            'anteil_qualifizierender_urnenwahlbezirke_prozent':100*count/len(urn),
            'briefwahlbezirke':len(postal), 'briefwahlbezirke_afd_ueber_50':sum(r['mehr_als_50_prozent'] for r in postal),
            'kartenkategorie':'keiner' if count==0 else 'alle' if count==len(urn) else 'ein_teil'}
    return result


def write_csv(path, rows):
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]))
        writer.writeheader();writer.writerows(rows)


def save(fig, out, slug, rows, municipalities):
    fig.savefig(out/f'{slug}.png',dpi=180,facecolor='white')
    svg=out/f'{slug}.svg'
    fig.savefig(svg,facecolor='white',metadata={'Date':None})
    ns='http://www.w3.org/2000/svg'
    ET.register_namespace('',ns)
    tree=ET.parse(svg)
    lookup={f"wbz-{r['ags']}-{r['wahlbezirk']}":r for r in rows}
    used=Counter()
    for node in tree.iter():
        gid=node.get('id','')
        r=lookup.get(gid)
        m=municipalities.get(gid.removeprefix('gemeinde-')) if gid.startswith('gemeinde-') else None
        if r:
            caption=(f"{r['gemeinde']} · WBZ {r['wahlbezirk']} · {r['name']}\n"
                     f"AfD: {r['afd']} von {r['gueltige_zweitstimmen']} gültigen Zweitstimmen "
                     f"({base.fmt(r['anteil_prozent'],2)} %)")
        elif m:
            caption=(f"{m['gemeinde']}: {m['urnenwahlbezirke_afd_ueber_50']} von "
                     f"{m['urnenwahlbezirke']} Urnenwahlbezirken mit AfD > 50 %. "
                     "Die Flächenfarbe zeigt die Verteilung der Wahlbezirke, keinen Gemeinde-Stimmenanteil.")
        else:
            continue
        used[gid]+=1
        node.set('id',f'{gid}-panel-{used[gid]}')
        title=ET.Element(f'{{{ns}}}title');title.text=caption;node.insert(0,title)
    tree.write(svg,encoding='utf-8',xml_declaration=True)
    plt.close(fig)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=DEFAULT)
    out=parser.parse_args().output_dir;out.mkdir(parents=True,exist_ok=True)
    rows=load_results()
    selected=[r for r in rows if r['mehr_als_50_prozent']]
    urn=[r for r in rows if r['wahlart']=='U'];postal=[r for r in rows if r['wahlart']=='B']
    municipalities=municipality_summary(rows)
    geo=base.jread(out/'sources/VG250_GEM_LSA.geojson')
    geometry_ids=[f['properties']['ags'] for f in geo['features']]
    assert len(geometry_ids)==len(set(geometry_ids)) and set(geometry_ids)==set(municipalities)
    hall=base.join_geometry(base.jread(SOURCE_DIR/'sources/halle_urn_2026_epsg25832.geojson'),rows,'15002000','U')
    md=base.join_geometry(base.jread(SOURCE_DIR/'sources/magdeburg_urn_2026_page.json'),rows,'15003000','U',page_coordinates=True)
    hall_n=sum(f['row']['mehr_als_50_prozent'] for f in hall)
    md_n=sum(f['row']['mehr_als_50_prozent'] for f in md)
    urn_n=sum(r['mehr_als_50_prozent'] for r in urn)
    postal_n=sum(r['mehr_als_50_prozent'] for r in postal)
    affected=sum(m['urnenwahlbezirke_afd_ueber_50']>0 for m in municipalities.values())
    color={'keiner':base.PALE,'ein_teil':LIGHT_BLUE,'alle':BLUE}
    fig=plt.figure(figsize=(16,10.5))
    fig.text(.045,.947,'Sachsen-Anhalt · AfD über 50 %',fontsize=27,fontweight='bold')
    fig.text(.045,.904,'Wahlbezirke mit mehr als 50 % der gültigen Zweitstimmen',fontsize=18)
    fig.text(.045,.864,'Landtagswahl am 6. September 2026 · Vorläufiges Ergebnis · Amtlicher Datenabruf 9. September 2026',fontsize=11.5,color=base.MUTED)
    fig.text(.045,.803,f'{base.fmt(urn_n)} von {base.fmt(len(urn))} Urnenwahlbezirken',fontsize=17,fontweight='bold',color=BLUE)
    fig.text(.405,.805,f'In {affected} von {len(municipalities)} Gemeinden · {postal_n} von {len(postal)} Briefwahlbezirken',fontsize=11,color=base.MUTED)
    ax=fig.add_axes([.035,.195,.31,.52])
    county_geoms=defaultdict(list)
    shapes=[]
    enriched=[]
    for f in geo['features']:
        ags=f['properties']['ags'];m=municipalities[ags]
        geom=make_valid(shape(f['geometry']));assert geom.is_valid and not geom.is_empty
        shapes.append(geom);county_geoms[f['properties']['sn_k']].append(geom)
        base.draw_shape(ax,geom,color[m['kartenkategorie']],'white',.4,'gemeinde-'+ags)
        enriched.append({'type':'Feature','properties':m,'geometry':mapping(geom)})
    for county in county_geoms.values():
        base.draw_shape(ax,unary_union(county),'none','#668399',.6,zorder=2)
    state=unary_union(shapes);x0,y0,x1,y1=state.bounds
    ax.set_xlim(x0-6000,x1+6000);ax.set_ylim(y0-8000,y1+5000);ax.set_aspect('equal');ax.axis('off')
    ax.text(0,1.09,'Landesübersicht nach Gemeinden',transform=ax.transAxes,fontsize=16,fontweight='bold')
    ax.text(0,1.035,'Urnenwahlbezirke mit AfD > 50 % je Gemeinde',transform=ax.transAxes,fontsize=9.5,color=base.MUTED)
    to_utm=Transformer.from_crs(4326,25832,always_xy=True)
    for label,xy,offset in [('Magdeburg',to_utm.transform(11.635,52.125),(8,10)),('Halle (Saale)',to_utm.transform(11.970,51.486),(8,-18))]:
        ax.scatter(*xy,s=17,color=base.INK,zorder=4)
        ax.annotate(label,xy,xytext=offset,textcoords='offset points',fontsize=9,zorder=5,
                    bbox={'facecolor':'white','edgecolor':'none','alpha':.9,'pad':2})
    hall_ax=fig.add_axes([.385,.22,.25,.495]);hall_ax.set_anchor('N')
    md_ax=fig.add_axes([.70,.22,.25,.495]);md_ax.set_anchor('N')
    base.draw_city(hall_ax,hall,'Halle (Saale)',f'{hall_n} von {len(hall)} Urnenwahlbezirken',all_city=True)
    base.draw_city(md_ax,md,'Magdeburg',f'{md_n} von {len(md)} Urnenwahlbezirken',all_city=True,scale_meters=False)
    fig.legend(handles=[Patch(facecolor=base.PALE,edgecolor=base.EDGE,label='Keiner'),Patch(facecolor=LIGHT_BLUE,label='Ein Teil'),Patch(facecolor=BLUE,label='Alle')],
               loc='lower left',bbox_to_anchor=(.035,.132),frameon=False,ncol=3,fontsize=10,handlelength=1.3,columnspacing=1.1)
    fig.legend(handles=[Patch(facecolor=BLUE,label='AfD > 50 %'),Patch(facecolor=base.PALE,edgecolor=base.EDGE,label='AfD ≤ 50 %')],
               loc='lower left',bbox_to_anchor=(.385,.132),frameon=False,ncol=2,fontsize=11)
    fig.text(.045,.102,'Links: Gemeinden nach der Zahl ihrer qualifizierenden Wahlbezirke; kein Gemeinde-Stimmenanteil. Rechts: tatsächliche Wahlbezirksgrenzen.',fontsize=9.5)
    ties=sum(r['genau_50_prozent'] for r in rows)
    fig.text(.045,.075,f'Berechnung je Wahlbezirk: AfD / gültige Zweitstimmen > 50 %. {ties} Bezirke mit genau 50 % ausgeschlossen. Wahlbezirksgrenzen außerhalb beider Städte nicht im Kartenpaket.',fontsize=9,color=base.MUTED)
    fig.text(.045,.044,'Quellen: Statistisches Landesamt Sachsen-Anhalt; Datenquelle: Stadt Halle (Saale), CC BY 3.0 DE; Landeshauptstadt Magdeburg; © BKG 2026 dl-de/by-2-0 (Gemeindegrenzen, 01.01.2025).',fontsize=8,color=base.MUTED)
    save(fig,out,'sachsen_anhalt_afd_majority_map',rows,municipalities)
    for slug,joined,city,projected in [('halle_afd_urnenwahl',hall,'Halle (Saale)',True),('magdeburg_afd_urnenwahl',md,'Magdeburg',False)]:
        n=sum(f['row']['mehr_als_50_prozent'] for f in joined)
        fig=plt.figure(figsize=(10,11))
        fig.text(.065,.95,f'{city} · AfD über 50 %',fontsize=25,fontweight='bold')
        fig.text(.065,.904,f'{n} von {len(joined)} Urnenwahlbezirken · Gültige Zweitstimmen',fontsize=16)
        fig.text(.065,.864,'Landtagswahl 6. September 2026 · Vorläufiges Ergebnis · Stand 09.09.2026',fontsize=11,color=base.MUTED)
        base.draw_city(fig.add_axes([.08,.20,.84,.53]),joined,'Stadtgebiet','Exakte Wahlbezirksgrenzen; Ergebnisse im SVG',all_city=True,labels=False,scale_meters=projected)
        fig.legend(handles=[Patch(facecolor=BLUE,label='AfD > 50 %'),Patch(facecolor=base.PALE,edgecolor=base.EDGE,label='AfD ≤ 50 %')],loc='lower left',bbox_to_anchor=(.065,.13),frameon=False,ncol=2)
        fig.text(.065,.095,'SVG: Wahlbezirksnummer, Name, Stimmenzahl und Anteil beim Überfahren der Flächen.',fontsize=9,color=base.MUTED)
        attribution='Datenquelle: Stadt Halle (Saale), CC BY 3.0 DE' if projected else 'Landeshauptstadt Magdeburg, Wahlbezirkskarte 2026'
        fig.text(.065,.05,'Ergebnisse: Statistisches Landesamt Sachsen-Anhalt · '+attribution,fontsize=8,color=base.MUTED)
        save(fig,out,slug,rows,municipalities)
    for name,values in [('all_precincts',rows),('majority_precincts',selected),('municipality_summary',list(municipalities.values()))]:
        write_csv(out/f'{name}.csv',values)
    inverse=Transformer.from_crs(25832,4326,always_xy=True)
    for f in enriched:
        f['geometry']=mapping(transform(inverse.transform,shape(f['geometry'])))
    base.jwrite(out/'municipality_precinct_counts.geojson',{'type':'FeatureCollection','features':enriched})
    base.jwrite(out/'summary.json',{'total_precincts':len(rows),'majority_precincts':len(selected),
        'in_person_total':len(urn),'in_person_majority':urn_n,'postal_total':len(postal),'postal_majority':postal_n,
        'exactly_50_excluded':ties,'municipalities_with_majority_precincts':affected,
        'municipality_categories':dict(Counter(m['kartenkategorie'] for m in municipalities.values())),
        'halle_majority':hall_n,'magdeburg_majority':md_n,'exact_majority_polygons_in_city_maps':hall_n+md_n,
        'other_majority_precincts_shown_as_municipality_counts':urn_n-hall_n-md_n,
        'valid_second_votes':sum(r['gueltige_zweitstimmen'] for r in rows),'afd_votes':sum(r['afd'] for r in rows),
        'threshold':'2 * AfD > valid_second_votes (integer comparison)',
        'source_csv':str((SOURCE_DIR/'Ergebnisse_WBZ_LT_2026.csv').relative_to(ROOT)),
        'source_sha256':hashlib.sha256((SOURCE_DIR/'Ergebnisse_WBZ_LT_2026.csv').read_bytes()).hexdigest(),
        'checks':{'all_party_sums_equal_denominator':True,'unique_precinct_keys':True,'all_218_municipality_geometries_join':True,'all_274_city_precinct_geometries_join':True}})
    print((out/'summary.json').read_text())


if __name__=='__main__':
    main()
