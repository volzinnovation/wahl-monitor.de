#!/usr/bin/env python3
"""Port the LSA census extraction to MV Ämter/towns and Berlin Bezirke.

Default is offline, reusing retained official workbooks. --download retrieves only
missing context files. Small source extracts and workbook hashes travel with reports.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone

import numpy as np
import openpyxl
import pandas as pd

from post_election_common import ROOT, ELECTIONS, POPULATION, BERLIN_CENSUS_URL, save_json, sha

LSA_RAW = ROOT / "data/2026-lsa/reports/demographic-feasibility/raw"
FEATURES = {
    "log_population": ("Einwohnerzahl (log)", "ln(Bevölkerung insgesamt)"),
    "age67_share": ("Anteil 67 Jahre und älter", "100 × (67–74 + 75+) / Bevölkerung"),
    "age19_24_share": ("Anteil 19–24 Jahre", "100 × Personen 19–24 / Bevölkerung"),
    "male_share": ("Männeranteil", "100 × männliche Personen / Bevölkerung"),
    "foreign_citizenship_share": ("Ausländische Staatsangehörigkeit", "100 × nichtdeutsche Personen / Bevölkerung; kein Migrationshintergrund"),
    "single_household_share": ("Einpersonenhaushalte", "100 × Einpersonenhaushalte / private Haushalte"),
    "owner_occupancy_share": ("Wohneigentumsquote", "Amtliche ETQ für bewohnte Wohnungen in Wohngebäuden ohne Wohnheime"),
    "vacancy_share": ("Wohnungsleerstand", "Amtliche LEQ für Wohnungen in Wohngebäuden ohne Wohnheime"),
    "rent_eur_m2": ("Nettokaltmiete je m²", "Amtliche durchschnittliche Nettokaltmiete, EUR/m²"),
    "abitur_share": ("Abitur/Fachhochschulreife", "100 × Hochschulreife / Personen ab 15 in der Haushaltsstichprobe"),
    "no_vocational_qualification_share": ("Ohne Berufsabschluss", "100 × ohne Berufsabschluss / Personen ab 15 in der Haushaltsstichprobe"),
    "ilo_unemployment_share": ("ILO-Erwerbslosenanteil", "100 × Erwerbslose / Erwerbspersonen; keine BA-Arbeitslosenquote"),
    "manufacturing_share": ("Produzierendes Gewerbe", "100 × Erwerbstätige im produzierenden Gewerbe / alle Erwerbstätigen der Haushaltsstichprobe"),
}


def number(value):
    if value is None or str(value).strip() in {"", "/", ".", "...", "x", "( )"}:
        return np.nan
    if str(value).strip() in {"–", "-"}:
        return 0.0
    return float(str(value).replace(" ", "").replace("\xa0", "").replace(",", "."))


def ratio(a, b):
    a, b = number(a), number(b)
    return 100 * a / b if np.isfinite(b) and b > 0 else np.nan


def download(url, path, enabled):
    if path.exists():
        return
    if not enabled:
        raise ValueError(f"Missing {path}; rerun preparation with --download")
    payload = subprocess.check_output(["curl", "-fsSL", "--max-time", "90", url])
    path.write_bytes(payload)


def berlin(source):
    w = openpyxl.load_workbook(source / "census_berlin.xlsx", read_only=True, data_only=True)
    tables = {}
    for name in ["Demografie", "Haushalte", "Bildung", "Erwerbstätigkeit", "Wohnungen"]:
        values = list(w[name].values)
        # Fail loudly on a revised source layout rather than silently changing meaning.
        if name == "Bildung":
            assert "Hochschulreife" in " ".join(str(r[57]) for r in values[5:10])
        if name == "Wohnungen":
            assert "Nettokaltmiete" in " ".join(str(r[77]) for r in values[5:10])
        tables[name] = {f"11{int(r[0]):02d}": r for r in values if r[2] == "Bezirk"}
        assert len(tables[name]) == 12
        pd.DataFrame([dict(geo_id=k, **{f"column_{i+1}":v for i,v in enumerate(r)})
                      for k,r in tables[name].items()]).to_csv(source / (name + ".csv"), index=False)
    records = []
    for key, d in tables["Demografie"].items():
        h, b, e, o = [tables[t][key] for t in ["Haushalte", "Bildung", "Erwerbstätigkeit", "Wohnungen"]]
        records.append(dict(geo_id=key, name=d[1], block=key, population=number(d[3]),
            log_population=np.log(number(d[3])), age67_share=ratio(number(d[17])+number(d[18]),d[3]),
            age19_24_share=ratio(d[13],d[3]), male_share=ratio(d[4],d[3]),
            foreign_citizenship_share=ratio(d[7],d[3]), single_household_share=ratio(h[4],h[3]),
            owner_occupancy_share=number(o[79]), vacancy_share=number(o[78]), rent_eur_m2=number(o[77]),
            abitur_share=ratio(b[57],b[39]), no_vocational_qualification_share=ratio(b[90],b[63]),
            ilo_unemployment_share=ratio(e[12],e[6]), manufacturing_share=ratio(e[117],e[111])))
    w.close()
    return pd.DataFrame(records)


def mv(source):
    specs = [("census_demography.xlsx", "CSV-Demografie"), ("census_households.xlsx", "CSV-Haushalte"),
             ("census_housing.xlsx", "CSV-Wohnungen"), ("census_education.xlsx", "CSV-Hoechster_Schulabschluss"),
             ("census_education.xlsx", "CSV-Hoechster_berufl_Abschluss"),
             ("census_education.xlsx", "CSV-Erwerbsstatus"), ("census_education.xlsx", "CSV-ET_Wirtschaftszweig")]
    tables = {}
    for filename, sheet in specs:
        w = openpyxl.load_workbook(LSA_RAW / filename, read_only=True, data_only=True)
        iterator = w[sheet].values
        header = next(iterator)
        rows = [dict(zip(header,r)) for r in iterator if str(r[1]).startswith("13") and
                (r[3] == "Gemeindeverband" or (r[3] == "Gemeinde" and int(str(r[1])[5:9]) < 5000))]
        for row in rows:
            rs = row["_RS"]
            row["geo_id"] = rs if len(rs) == 9 else rs[:5] + rs[-3:]
        assert len(rows) == len({r["geo_id"] for r in rows})
        pd.DataFrame(rows).to_csv(source / (sheet + ".csv"), index=False)
        tables[sheet] = {r["geo_id"]:r for r in rows}
        w.close()
    records = []
    for key,d in tables["CSV-Demografie"].items():
        h,o,b,v,e,i = [tables[t][key] for t in ["CSV-Haushalte","CSV-Wohnungen","CSV-Hoechster_Schulabschluss",
                    "CSV-Hoechster_berufl_Abschluss","CSV-Erwerbsstatus","CSV-ET_Wirtschaftszweig"]]
        n = d["0_Insgesamt_"]
        records.append(dict(geo_id=key,name=d["Name"],block=key[:5],population=number(n),
            log_population=np.log(number(n)),age67_share=ratio(number(d['Alter_infr__10'])+number(d['Alter_infr__11']),n),
            age19_24_share=ratio(d['Alter_infr__06'],n),male_share=ratio(d['GESCHLECHT__1'],n),
            foreign_citizenship_share=ratio(d['Staatsange_kurz__2'],n),single_household_share=ratio(h['HH_SIZE_NAT__1'],h['0_Insgesamt_']),
            owner_occupancy_share=number(o['ETQ']),vacancy_share=number(o['LEQ']),rent_eur_m2=number(o['QMMIETE']),
            abitur_share=ratio(b['SCHULABS_STP__24'],b['SCHULABS_STP']),
            no_vocational_qualification_share=ratio(v['BERUFABS_AUSF_STP__2'],v['BERUFABS_AUSF_STP']),
            ilo_unemployment_share=ratio(e['ERWERBSTAT_KURZ_STP__12'],e['ERWERBSTAT_KURZ_STP__1']),
            manufacturing_share=ratio(i['ET_WIRTSZWG_STP__21'],i['ET_WIRTSZWG_STP'])))
    return pd.DataFrame(records)


def prepare(key, fetch=False):
    source = ROOT / "data" / key / "reports/post-election/sources"
    source.mkdir(parents=True, exist_ok=True)
    download(POPULATION[key][1], source / "population_2025.html", fetch)
    evidence = (source / "population_2025.html").read_text()
    assert f"{POPULATION[key][0]:,}".replace(",", ".") in evidence, "Population release changed; review baseline"
    manifest = []
    if key == "2026-be":
        download(BERLIN_CENSUS_URL, source / "census_berlin.xlsx", fetch)
        data = berlin(source)
        manifest.append(dict(file="census_berlin.xlsx",url=BERLIN_CENSUS_URL,sha256=sha((source/"census_berlin.xlsx").read_bytes())))
    else:
        data = mv(source)
        retained = json.loads((LSA_RAW/"acquisition_manifest_more.json").read_text())
        for item in retained:
            if item.get("file", "").startswith("census_") and item["file"].endswith(".xlsx"):
                digest = sha((LSA_RAW / item["file"]).read_bytes())
                assert digest == item["sha256"]
                manifest.append({**item, "file":str((LSA_RAW/item['file']).relative_to(ROOT))})
    assert data.geo_id.is_unique
    for feature in FEATURES:
        assert not np.isinf(data[feature]).any()
        if feature.endswith('_share'):
            assert data[feature].dropna().between(0,100).all()
    data.to_csv(source / "demographics.csv",index=False)
    manifest.append(dict(file="population_2025.html",url=POPULATION[key][1],sha256=sha((source/"population_2025.html").read_bytes()),
                         population=POPULATION[key][0],date="2025-12-31"))
    save_json(source / "manifest.json", dict(prepared_at=datetime.now(timezone.utc).isoformat(),sources=manifest,
        census_date="2022-05-15", geographic_unit="Bezirke" if key=="2026-be" else "Ämter und amtsfreie Gemeinden",
        features={k:dict(label=v[0],definition=v[1],date="2022-05-15") for k,v in FEATURES.items()},
        adaptation="Published census age bands 67+ and 19–24 replace LSA's 2025 bands 65+ and 18–29. Density and 2024–25 change are not computed without comparable dated area/population inputs.",
        extracts={p.name:sha(p.read_bytes()) for p in source.glob('*.csv')}))
    print(key, len(data), "demographic areas prepared")


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--election-key', choices=ELECTIONS, required=True)
    parser.add_argument('--download', action='store_true')
    args=parser.parse_args()
    prepare(args.election_key,args.download)
