#!/usr/bin/env python3
"""Reproduce the LSA municipal demographic feasibility pilot from retained inputs.

No network, credentials, election collection, or publishing. Requires numpy,
pandas, openpyxl and pdfplumber (available in the Codex bundled Python).
"""
from pathlib import Path
import csv
import hashlib
import json
import re

import numpy as np
import openpyxl
import pandas as pd
import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/2026-lsa/reports/demographic-feasibility"
RAW = OUT / "raw"
ELECTION = ROOT / "data/2026-lsa/reports/git-timeline/80fa3052/latest_official_rows.json"


def save_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def number(value):
    if value is None or str(value).strip() in {"", "/", ".", "...", "x"}:
        return np.nan
    if str(value).strip() in {"–", "-"}:
        return 0.0  # Source legends: exactly zero or changed to zero.
    return float(str(value).replace(" ", "").replace("\u00a0", "").replace(",", "."))


def ars_to_ags(ars):
    assert re.fullmatch(r"15\d{10}", ars), ars
    return ars[:5] + ars[-3:]  # Remove the FOUR municipal-association digits.


def age_rows():
    rows = []
    current = None
    with pdfplumber.open(RAW / "age_2025.pdf") as pdf:
        for page_index, page in enumerate(pdf.pages[8:], 8):
            lines = {}
            for word in sorted(page.extract_words(), key=lambda w: w["top"]):
                key = next((y for y in lines if abs(y - word["top"]) < 2), word["top"])
                lines.setdefault(key, []).append(word)
            for _, words in sorted(lines.items()):
                words.sort(key=lambda w: w["x0"])
                text = " ".join(w["text"] for w in words)
                code = next((w["text"] for w in words if w["x0"] < 105 and re.fullmatch(r"15\d{3}(?:\d{3})?", w["text"])), None)
                if code:
                    if code in {"15001", "15002", "15003"}:
                        current = code + "000"
                    elif len(code) == 8:
                        current = code
                    else:
                        current = None
                match = re.search(r"(?<!\d)(unter 3|\d+ bis unter \d+|85 und älter|Insgesamt)(?= |$)", text)
                if not current or not match:
                    continue
                values = []
                for left, right in [(338, 408), (408, 480), (480, 550)]:
                    values.append(number("".join(w["text"] for w in words if left <= w["x0"] < right)))
                assert all(np.isfinite(values)), (page_index, current, text, values)
                rows.append(dict(ags=current, age_band=match[1], total=int(values[0]), male=int(values[1]), female=int(values[2]), pdf_page=page_index + 1))
    df = pd.DataFrame(rows)
    assert not df.duplicated(["ags", "age_band"]).any()
    assert len(df) == 218 * 21, len(df)
    assert (df["male"] + df["female"] == df["total"]).all()
    total = df[df.age_band == "Insgesamt"].set_index("ags")
    bands = df[df.age_band != "Insgesamt"].copy()
    assert (bands.groupby("ags")["total"].sum() == total["total"].sort_index()).all()
    assert int(total.total.sum()) == 2120252
    bands["lower"] = bands.age_band.str.extract(r"(\d+)").astype(int)
    bands.loc[bands.age_band == "unter 3", "lower"] = 0
    result = total[["total", "male", "female"]].rename(columns={"total": "population_2025", "male": "male_2025", "female": "female_2025"})
    result["age65_share"] = 100 * bands[bands.lower >= 65].groupby("ags").total.sum() / result.population_2025
    result["age18_29_share"] = 100 * bands[bands.lower.between(18, 29)].groupby("ags").total.sum() / result.population_2025
    result["male_share"] = 100 * result.male_2025 / result.population_2025
    result["log_population"] = np.log(result.population_2025)
    df.to_csv(OUT / "age_source_rows.csv", index=False)
    return result


def census_sheet(filename, sheet):
    w = openpyxl.load_workbook(RAW / filename, read_only=True, data_only=True)
    iterator = w[sheet].values
    header = next(iterator)
    rows = [dict(zip(header, r)) for r in iterator if str(r[1]).startswith("15") and r[3] == "Gemeinde"]
    w.close()
    df = pd.DataFrame(rows)
    df["ags"] = df._RS.map(ars_to_ags)
    assert len(df) == 218 and df.ags.nunique() == 218, (filename, sheet, len(df))
    df.to_csv(OUT / (sheet + "_sachsen_anhalt.csv"), index=False)
    return df.set_index("ags")


def register_rows():
    w = openpyxl.load_workbook(RAW / "gv_2026q2.xlsx", read_only=True, data_only=True)
    rows = []
    for r in w.worksheets[1].values:
        if r[0] == "60" and r[2] == "15":
            ars = "".join(r[2:7])
            rows.append(dict(ags=ars_to_ags(ars), ars_2026q2=ars, register_name=r[7], area_km2=number(r[8]), population_2024=number(r[9]), longitude=number(r[14]), latitude=number(r[15]), urban_class=r[18], urban_class_name=r[19], is_city=r[1] in {"61", "62", "63"}))
    w.close()
    df = pd.DataFrame(rows).set_index("ags")
    assert len(df) == 218 and df.index.is_unique
    return df


def build_dataset():
    source = json.loads(ELECTION.read_text())
    municipal = [r for r in source.values() if r["level"] == "GEMEINDE" and r["mode"] == "TOTAL"]
    land = source["lsa:LAND:15:TOTAL"]
    parties = {k: v for k, v in land["party_names"].items() if k.startswith("F")}
    assert len(municipal) == len({r["number"] for r in municipal}) == 218
    assert all(r["reported_precincts"] == r["total_precincts"] for r in municipal)
    assert sum(r["valid_votes_zweit"] for r in municipal) == land["valid_votes_zweit"] == 1315315
    assert sum(r["total_precincts"] for r in municipal) == 2661
    for k in parties:
        assert sum(r["parties"][k] for r in municipal) == land["parties"][k]
    records = []
    for r in municipal:
        n = r["valid_votes_zweit"]
        assert sum(r["parties"][k] for k in parties) == n
        record = dict(ags=r["number"], name=r["name"], kreis=r["number"][:5], valid_second_votes=n, eligible_voters=r["extra"]["eligible_voters"], voters=r["voters_total"])
        for k in parties:
            record[k + "_votes"] = r["parties"][k]
            record[k + "_share"] = 100 * r["parties"][k] / n
        records.append(record)
    df = pd.DataFrame(records).set_index("ags").join(age_rows(), how="left", validate="one_to_one").join(register_rows(), how="left", validate="one_to_one")
    assert df.population_2025.notna().all() and df.area_km2.notna().all()
    df["density_2025"] = df.population_2025 / df.area_km2
    df["log_density"] = np.log(df.density_2025)
    df["population_change_2024_25"] = 100 * (df.population_2025 / df.population_2024 - 1)
    df["turnout"] = 100 * df.voters / df.eligible_voters
    dem = census_sheet("census_demography.xlsx", "CSV-Demografie")
    hh = census_sheet("census_households.xlsx", "CSV-Haushalte")
    housing = census_sheet("census_housing.xlsx", "CSV-Wohnungen")
    school = census_sheet("census_education.xlsx", "CSV-Hoechster_Schulabschluss")
    vocational = census_sheet("census_education.xlsx", "CSV-Hoechster_berufl_Abschluss")
    work = census_sheet("census_education.xlsx", "CSV-Erwerbsstatus")
    industry = census_sheet("census_education.xlsx", "CSV-ET_Wirtschaftszweig")
    for data in [dem, hh, housing, school, vocational, work, industry]:
        assert set(data.index) == set(df.index)
    df["census_ars"] = dem._RS
    df["census_population"] = dem["0_Insgesamt_"].map(number)
    df["foreign_citizenship_share"] = 100 * dem.Staatsange_kurz__2.map(number) / df.census_population
    df["single_household_share"] = 100 * hh.HH_SIZE_NAT__1.map(number) / hh["0_Insgesamt_"].map(number)
    df["owner_occupancy_share"] = housing.ETQ.map(number)
    df["vacancy_share"] = housing.LEQ.map(number)
    df["rent_eur_m2"] = housing.QMMIETE.map(number)
    df["abitur_share"] = 100 * school.SCHULABS_STP__24.map(number) / school.SCHULABS_STP.map(number)
    df["no_vocational_qualification_share"] = 100 * vocational.BERUFABS_AUSF_STP__2.map(number) / vocational.BERUFABS_AUSF_STP.map(number)
    df["ilo_unemployment_share"] = 100 * work.ERWERBSTAT_KURZ_STP__12.map(number) / work.ERWERBSTAT_KURZ_STP__1.map(number)
    df["manufacturing_share"] = 100 * industry.ET_WIRTSZWG_STP__21.map(number) / industry.ET_WIRTSZWG_STP.map(number)
    assert (df.area_km2 > 0).all()
    assert (df.census_ars == df.ars_2026q2).all(), "Review association/boundary changes before joining"
    df.reset_index().to_csv(OUT / "municipality_analysis.csv", index=False)
    return df, parties


FEATURES = {
    "log_population": ("Population size (log)", "2025-12-31", "Natural log of resident population"),
    "log_density": ("Population density (log)", "2025-12-31 / area 2024", "Natural log of 2025 population / GV area; June 2026 boundaries"),
    "age65_share": ("Residents aged 65+ (%)", "2025-12-31", "100 × residents aged 65+ / all residents"),
    "age18_29_share": ("Residents aged 18–29 (%)", "2025-12-31", "100 × residents aged 18–29 / all residents"),
    "male_share": ("Male residents (%)", "2025-12-31", "100 × male residents / all residents"),
    "population_change_2024_25": ("Population change 2024–25 (%)", "2024-12-31 to 2025-12-31", "100 × (2025 / 2024 population − 1); both based on Zensus 2022"),
    "foreign_citizenship_share": ("Non-German citizenship (%)", "2022-05-15", "100 × non-German citizens / demographic population; citizenship is not migration background"),
    "single_household_share": ("One-person households (%)", "2022-05-15", "100 × one-person private households / all private households"),
    "owner_occupancy_share": ("Owner-occupied dwellings (%)", "2022-05-15", "Published ETQ: owner-occupied / inhabited dwellings in residential buildings excluding dormitories"),
    "vacancy_share": ("Vacant dwellings (%)", "2022-05-15", "Published LEQ: empty / occupied and empty dwellings in residential buildings excluding dormitories"),
    "rent_eur_m2": ("Net cold rent (EUR/m²)", "2022-05-15", "Published mean dwelling-level rent per m²; rental dwellings in residential buildings excluding dormitories and rent-free dwellings"),
    "abitur_share": ("Abitur/Fachhochschulreife (%)", "2022-05-15", "100 × persons with university entrance qualification / persons aged 15+ in sampled household population"),
    "no_vocational_qualification_share": ("No vocational qualification (%)", "2022-05-15", "100 × persons without vocational qualification / persons aged 15+ in sampled household population"),
    "ilo_unemployment_share": ("ILO unemployment (%)", "2022-05-15", "100 × unemployed / labour force in census household sample; not registered BA unemployment"),
    "manufacturing_share": ("Manufacturing employment (%)", "2022-05-15", "100 × employed residents in manufacturing / all employed residents in census household sample"),
}


def corr(x, y, rank=False, weights=None):
    x, y = np.asarray(x, float), np.asarray(y, float)
    if rank:
        x, y = pd.Series(x).rank().to_numpy(), pd.Series(y).rank().to_numpy()
    if weights is None:
        return float(np.corrcoef(x, y)[0, 1])
    w = np.asarray(weights, float)
    x = x - np.average(x, weights=w)
    y = y - np.average(y, weights=w)
    return float(np.sum(w*x*y) / np.sqrt(np.sum(w*x*x)*np.sum(w*y*y)))


def run():
    OUT.mkdir(exist_ok=True, parents=True)
    df, parties = build_dataset()
    coverage = []
    for feature, (label, date, definition) in FEATURES.items():
        n = int(df[feature].notna().sum())
        coverage.append(dict(feature=feature, label=label, date=date, definition=definition, n=n, missing=218-n, scope="all municipalities" if n == 218 else "published subset", min=float(df[feature].min()), max=float(df[feature].max())))
    pd.DataFrame(coverage).to_csv(OUT / "feature_coverage.csv", index=False)
    correlations = []
    for k, party in parties.items():
        for feature, (label, date, definition) in FEATURES.items():
            sample = df.dropna(subset=[feature, k + "_share"])
            x, y = sample[feature], sample[k + "_share"]
            loo = [corr(s[feature], s[k + "_share"], rank=True) for district in sorted(sample.kreis.unique()) if len(s := sample[sample.kreis != district]) >= 20]
            rural = sample[~sample.kreis.isin(["15001", "15002", "15003"])]
            cities = sample[sample.is_city]
            # Demeaning both ranked variables by district estimates a within-district
            # descriptive association, not an independent or causal effect.
            ranks = sample[[feature, k + "_share"]].rank()
            residual = ranks - ranks.groupby(sample.kreis).transform("mean")
            correlations.append(dict(party=party, code=k, feature=feature, label=label, n=len(sample), scope="all municipalities" if len(sample)==218 else "published subset", spearman=corr(x,y,rank=True), pearson=corr(x,y), pearson_vote_weighted=corr(x,y,weights=sample.valid_second_votes), spearman_without_three_cities=corr(rural[feature],rural[k+"_share"],rank=True), n_without_three_cities=len(rural), spearman_cities_only=corr(cities[feature],cities[k+"_share"],rank=True), n_cities_only=len(cities), spearman_within_kreis=corr(residual[feature],residual[k+"_share"]), leave_one_kreis_min=min(loo), leave_one_kreis_max=max(loo)))
    results = pd.DataFrame(correlations)
    results.to_csv(OUT / "party_correlations.csv", index=False)
    # The unrestricted feature grid is retained, making selection transparent.
    full = results[results.n == 218]
    strongest = full.loc[full.spearman.abs().groupby(full.code).idxmax()].sort_values("party")
    strongest.to_csv(OUT / "party_strongest_descriptive_associations.csv", index=False)
    checks = dict(municipalities=218, municipalities_matched=218, election_precincts=2661, valid_second_votes=1315315, parties=len(parties), pairwise_correlations=len(results), all_municipality_features=int((pd.DataFrame(coverage).n==218).sum()), matching_census_and_register_ars=218, age_rows=4578, population_2025=int(df.population_2025.sum()), cities=int(df.is_city.sum()), election_source=str(ELECTION.relative_to(ROOT)), election_sha256=hashlib.sha256(ELECTION.read_bytes()).hexdigest(), source_snapshot="80fa3052044a45af29f4f0b2867957d8a3b1df35; capture 2026-09-07 04:06:37 CEST; preliminary result", analysis="Exploratory descriptive associations only. No p values, causal claims, independent-factor ranking, or individual-voter inferences.")
    save_json(OUT / "validation.json", checks)
    save_json(OUT / "party_names.json", parties)
    save_json(OUT / "source_checksums.json", {str(p.relative_to(OUT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(RAW.iterdir()) if p.is_file()})
    print(json.dumps(checks, ensure_ascii=False, indent=2))
    print(pd.DataFrame(coverage)[["feature","n"]].to_string(index=False))
    print(strongest[["party","label","spearman","spearman_without_three_cities","spearman_within_kreis"]].round(3).to_string(index=False))


if __name__ == "__main__":
    run()
