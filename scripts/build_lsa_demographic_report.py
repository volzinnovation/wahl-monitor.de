#!/usr/bin/env python3
"""Build the canonical local report payload and executed companion notebook."""
from pathlib import Path
import contextlib
import io
import json
import platform
import sqlite3

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/2026-lsa/reports/demographic-feasibility"


def records(df):
    return json.loads(df.to_json(orient="records", force_ascii=False))


def main():
    data = pd.read_csv(OUT / "municipality_analysis.csv", dtype={"ags": str, "kreis": str, "census_ars": str, "ars_2026q2": str})
    correlations = pd.read_csv(OUT / "party_correlations.csv")
    strongest = pd.read_csv(OUT / "party_strongest_descriptive_associations.csv")
    coverage = pd.read_csv(OUT / "feature_coverage.csv")
    education = correlations[correlations.feature == "abitur_share"].sort_values("party")
    catalogue = [
        dict(topic="Taxpayers and taxable income", provider="Sachsen-Anhalt GENESIS", table="73111-0001", grain="Municipality", availability="Catalogue verified; available years/cells not extracted; not disposable household income"),
        dict(topic="Registered labour-market indicators", provider="Sachsen-Anhalt GENESIS", table="13211-0003 / 13211-0004", grain="District / independent city", availability="Catalogue verified, annual / monthly; do not copy district rates as independent municipal observations"),
        dict(topic="Population and sex", provider="Sachsen-Anhalt GENESIS", table="12411-0001", grain="Municipality", availability="Catalogue verified; 2025 values extracted from official age report"),
        dict(topic="Age structure", provider="Sachsen-Anhalt GENESIS", table="12411-0003", grain="Municipality", availability="Catalogue verified; all 218 municipalities in 2025 PDF"),
        dict(topic="Citizenship", provider="Sachsen-Anhalt GENESIS", table="12411-0014", grain="Municipality", availability="Catalogue verified; pilot uses census 2022 values"),
        dict(topic="Births, deaths, migration", provider="Sachsen-Anhalt GENESIS", table="12XXX-0005-1 / 12XXX-0005-2", grain="Municipality", availability="Catalogue verified: through 2024 / from 2025; not extracted"),
        dict(topic="Population and sex", provider="Regionaldatenbank", table="12411-01-01-5", grain="Municipality", availability="Catalogue verified; individual year/cell coverage not queried"),
        dict(topic="Age structure", provider="Regionaldatenbank", table="12411-02-03-5", grain="Municipality", availability="Catalogue verified: 17 age groups; individual year/cell coverage not queried"),
        dict(topic="Average age", provider="Regionaldatenbank", table="12411-07-01-5", grain="Municipality", availability="Catalogue verified; not extracted"),
        dict(topic="Citizenship and sex", provider="Regionaldatenbank", table="12411-11-01-5", grain="Municipality", availability="Catalogue verified; not extracted"),
        dict(topic="Area, population, coordinates, urban class", provider="Destatis GV-ISys", table="Onlineprodukt_Gemeinden30062026", grain="Municipality (AGS + ARS)", availability="Downloaded and matched 218/218; June 2026 boundaries, area/population 2024"),
        dict(topic="Demography, households, housing", provider="Destatis Zensus 2022", table="CSV-Demografie / CSV-Haushalte / CSV-Wohnungen", grain="Municipality (12-digit ARS)", availability="Downloaded; selected factors present for 218/218"),
        dict(topic="Education and employment", provider="Destatis Zensus 2022", table="CSV-Hoechster_Schulabschluss / CSV-Erwerbsstatus and companion sheets", grain="Published municipality subset", availability="54 education/manufacturing observations; 52 ILO unemployment ratios"),
        dict(topic="Registered unemployment", provider="Bundesagentur für Arbeit", table="Arbeitslose – Kreise und Gemeinden", grain="Municipality counts; standard rate series at district level", availability="Official product verified; not downloaded in this pilot"),
    ]
    pd.DataFrame(catalogue).to_csv(OUT / "source_catalogue.csv", index=False)
    urls = {
        "age": "https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Bevoelkerung/Berichte/Bevoelkerungsstand/6A119_2025-A.pdf",
        "census": "https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Zensus2022/Publikationen/publikationen-akkordeon-regionaltabellen.html",
        "gv": "https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/_inhalt.html",
        "regional": "https://www.regionalstatistik.de/genesis/online",
        "lsa": "https://genesis.sachsen-anhalt.de/genesis/online?operation=statistic&code=12411",
        "api": "https://genesis.destatis.de/datenbank/online/docs/GENESIS-Webservices_Introduction.pdf",
        "ba": "https://statistik.arbeitsagentur.de/DE/Navigation/Statistiken/Fachstatistiken/Arbeitsuche-Arbeitslosigkeit-Unterbeschaeftigung/Produkte/Zeitreihen-Arbeitslose-Arbeitsuchende-Arbeitslosenquoten/Zeitreihen-Arbeitslose-Arbeitsuchende-Arbeitslosenquoten-Nav.html",
    }
    source_files = ["data/2026-lsa/reports/git-timeline/80fa3052/latest_official_rows.json", "raw/age_2025.pdf", "raw/gv_2026q2.xlsx:Onlineprodukt_Gemeinden30062026", "raw/census_demography.xlsx:CSV-Demografie", "raw/census_households.xlsx:CSV-Haushalte", "raw/census_housing.xlsx:CSV-Wohnungen", "raw/census_education.xlsx:CSV-Hoechster_Schulabschluss,CSV-Hoechster_berufl_Abschluss,CSV-Erwerbsstatus,CSV-ET_Wirtschaftszweig"]
    sources = [dict(id="analysis", label="Official election + Destatis/StatLA municipal sources; reproducible joined pilot", path="scripts/analyze_lsa_demographics.py", query=dict(description="Join one complete TOTAL second-vote municipality result per AGS to published demographic rows. Compute Pearson and average-rank Spearman correlations separately for each party and feature.", language="python", tables_used=source_files, filters=["Sachsen-Anhalt, 218 AGS", "Election capture 2026-09-07 04:06:37 CEST; preliminary results", "Population 2025; register area/population 2024 on June 2026 boundaries; census 2022", "No imputation of suppressed census education/employment cells"], metric_definitions={"party_share":"100 × party second votes / all valid second votes in municipality", "spearman":"Pearson correlation of within-sample average ranks; every municipality has equal weight", "strongest":"Largest absolute Spearman correlation among the 11 factors present for all 218 municipalities; exploratory selection", "cities":"GV text codes 61/63 in Sachsen-Anhalt, 104 municipalities with city status"})),
        dict(id="catalogue", label="Verified official source catalogue and access checks, 7 September 2026", path="data/2026-lsa/reports/demographic-feasibility/source_catalogue.csv", query=dict(description="Read public catalogues and inspect downloaded source sheets; availability distinguishes catalogued tables from successfully read numeric cells.", tables_used=["raw/regional_12411.html", "raw/lsa_12411.html", "source_catalogue.csv"], language="text")),
    ]
    sources += [dict(id=k, label={"age":"StatLA: age and sex, 31 December 2025", "census":"Destatis: Zensus 2022 regional workbooks", "gv":"Destatis: GV-ISys register", "regional":"Regionaldatenbank: API and public access notices", "lsa":"Sachsen-Anhalt GENESIS: population catalogue", "api":"Destatis: current API documentation", "ba":"BA: municipality unemployment count series"}[k], href=v) for k,v in urls.items()]
    # Derive the two chart cohorts directly from raw vote counts and extracted
    # census counts in SQLite. This supplies the portable renderer's required
    # executable SQL provenance, and independently verifies the Python ratios.
    from analyze_lsa_demographics import number
    raw_election = json.loads((ROOT / "data/2026-lsa/reports/git-timeline/80fa3052/latest_official_rows.json").read_text())
    vote_counts = pd.DataFrame([dict(ags=r["number"], name=r["name"], kreis=r["number"][:5], valid_second_votes=r["valid_votes_zweit"], F3_votes=r["parties"]["F3"], F6_votes=r["parties"]["F6"]) for r in raw_election.values() if r["level"]=="GEMEINDE" and r["mode"]=="TOTAL"])
    household_counts = pd.read_csv(OUT / "CSV-Haushalte_sachsen_anhalt.csv", dtype={"ags":str})[["ags","0_Insgesamt_","HH_SIZE_NAT__1"]].copy()
    school_counts = pd.read_csv(OUT / "CSV-Hoechster_Schulabschluss_sachsen_anhalt.csv", dtype={"ags":str})[["ags","SCHULABS_STP","SCHULABS_STP__24"]].copy()
    for frame in [household_counts, school_counts]:
        for column in frame.columns:
            if column!="ags": frame[column]=frame[column].map(number)
    connection = sqlite3.connect(":memory:")
    for name,frame in [("municipal_vote_counts",vote_counts),("census_household_counts",household_counts),("census_school_counts",school_counts)]: frame.to_sql(name,connection,index=False)
    household_sql = '''SELECT e.ags, e.name, e.kreis, e.valid_second_votes, e.F3_votes,
       h.HH_SIZE_NAT__1 AS one_person_households, h."0_Insgesamt_" AS private_households,
       100.0 * h.HH_SIZE_NAT__1 / h."0_Insgesamt_" AS single_household_share,
       100.0 * e.F3_votes / e.valid_second_votes AS F3_share
FROM municipal_vote_counts AS e JOIN census_household_counts AS h ON e.ags = h.ags
WHERE h."0_Insgesamt_" > 0 AND e.valid_second_votes > 0
ORDER BY e.ags'''
    education_sql = '''SELECT e.ags, e.name, e.kreis, e.valid_second_votes, e.F6_votes,
       s.SCHULABS_STP AS persons_15plus, s.SCHULABS_STP__24 AS persons_abitur,
       100.0 * s.SCHULABS_STP__24 / s.SCHULABS_STP AS abitur_share,
       100.0 * e.F6_votes / e.valid_second_votes AS F6_share
FROM municipal_vote_counts AS e JOIN census_school_counts AS s ON e.ags = s.ags
WHERE s.SCHULABS_STP > 0 AND s.SCHULABS_STP__24 IS NOT NULL AND e.valid_second_votes > 0
ORDER BY e.ags'''
    household_chart = pd.read_sql_query(household_sql,connection)
    education_chart = pd.read_sql_query(education_sql,connection)
    connection.close()
    import numpy as np
    for frame,fields in [(household_chart,["single_household_share","F3_share"]),(education_chart,["abitur_share","F6_share"])]:
        for field in fields: assert np.allclose(frame[field], data.set_index("ags").loc[frame.ags,field])
    assert len(household_chart)==218 and len(education_chart)==54
    for id,sql,inputs in [("household-sql",household_sql,["municipal_vote_counts","census_household_counts"]),("education-sql",education_sql,["municipal_vote_counts","census_school_counts"])]:
        (OUT / (id+".sql")).write_text(sql+"\n")
        sources.append(dict(id=id,label="Official municipal election counts joined to Zensus 2022 counts",path="scripts/build_lsa_demographic_report.py",query=dict(sql=sql,engine="SQLite",language="sql",tables_used=inputs,description="SQL computes both plotted ratios from original counts and joins by AGS. Source tables load the retained official election JSON and extracted census municipality CSVs; suppression becomes SQL NULL. Ratios independently reconcile to the Python analysis.",upstream_files=[source_files[0],"CSV-Haushalte_sachsen_anhalt.csv" if id=="household-sql" else "CSV-Hoechster_Schulabschluss_sachsen_anhalt.csv"])))
    blocks = []
    def md(id, body, source=None):
        block = dict(id=id, type="markdown", body=body)
        if source: block["sourceId"] = source
        blocks.append(block)
    title = "Sachsen-Anhalt: demographic correlates of the 2026 election"
    md("title", "# " + title)
    md("summary", "## A municipal analysis is feasible, with a separate education sample\n\nAll **218 municipalities**, including **104 with city status**, match the official demographic sources. Eleven factors are available for every municipality. Four education/employment factors cover only 52–54 municipalities.\n\nThe pilot computes **225 descriptive correlations for all 15 second-vote parties**. The clearest associations among the full-coverage factors are larger municipalities with higher GRÜNE and lower AfD shares, and more one-person households with higher Die Linke shares. CDU and BSW have only weak bivariate associations in this factor set. These are characteristics of places, not causal explanations or estimates of individual voting behaviour.", "analysis")
    md("definitions", "## Compare the same geographic units and explicit denominators\n\nOne observation is one municipality, using its combined in-person and postal **second votes / valid second votes**. Election results are the complete, preliminary archived capture from **7 September 2026, 04:06:37 CEST**; the 1,315,315 valid second votes reconcile to the state total. Constituencies, districts and voting modes are not added as extra observations.\n\nResident and housing attributes predate the election: population/age/sex are from 31 December 2025; census attributes from 15 May 2022. GV-ISys uses June 2026 boundaries with December 2024 population and area. Density uses 2025 residents divided by that area. Population change compares the 2024 and 2025 Zensus-2022-based resident totals. Residents include people outside the electorate.\n\nSpearman ρ ranges from −1 to +1. A positive value means higher values of a municipal characteristic tend to accompany a higher party share. The primary estimates give every municipality equal weight. A monotonic log transform does not change Spearman ρ.", "analysis")
    md("availability", "## GENESIS is a family of databases; the host matters\n\nThe municipal analysis needs **Regionaldatenbank Deutschland and Sachsen-Anhalt GENESIS**, together with **Destatis GV-ISys and Zensus 2022**. Federal GENESIS is useful for reference statistics, but a statistic code does not guarantee municipal detail. Table codes below belong to their named host. Numeric downloads were checked separately from catalogue entries.\n\nThe federal API returned HTTP 401 without authentication. Its current documentation requires credentials or a personal token in POST request headers. Regionaldatenbank also requires registration for its API and temporarily refused further anonymous catalogue requests in this session. Its 12411 catalogue was retrieved successfully. The pilot uses verified official public downloads; no API account was created and no credentials were searched for. Income, BA employment/commuting, and additional Regionaldatenbank tables remain enrichment work, not verified model inputs.")
    blocks.append(dict(id="catalogue-table", type="table", tableId="catalogue"))
    md("coverage", "## Education is missing systematically in smaller municipalities\n\nThe census workbook contains municipality rows for all 218 units, but education and employment cells derived from its household sample are not published for many small municipalities. **Abitur, vocational qualifications and manufacturing cover 54; the ILO unemployment ratio covers 52.** The 54 municipalities account for 70.7% of valid second votes, but only 24.8% of municipalities. This is a different population of places.\n\nSuppressed values remain missing. They are never treated as zero, and municipal-association averages are not copied into member municipalities. Household, housing and demographic counts can be perturbed for disclosure protection; their sums need not be exact. Definitions and dates for every feature follow.", "analysis")
    blocks.append(dict(id="coverage-table", type="table", tableId="coverage"))
    md("findings", "## The full-coverage associations are mostly modest\n\nThis table shows each party’s largest absolute Spearman association **among the 11 factors available for all 218 municipalities**. It is an exploratory maximum, not an independently validated factor ranking. All 225 pairwise results are preserved in the CSV. The second estimate excludes Halle, Magdeburg and Dessau-Roßlau; the third restricts the sample to the 104 municipalities with city status. These sensitivities answer different geographic questions.\n\nFor example, Die Linke–one-person households is ρ = **+0.421** across all municipalities and **+0.400** without the three independent cities. GRÜNE–population is **+0.362** and AfD–population **−0.339**. CDU’s largest absolute correlation is **0.152**, BSW’s **0.128**: this pilot does not reveal a strong bivariate demographic correlate for either.", "analysis")
    blocks.append(dict(id="strongest-table", type="table", tableId="strongest"))
    blocks.append(dict(id="households-plot", type="chart", chartId="households"))
    md("households-interpretation", "The scatter retains one point per municipality. The positive rank association does not imply that people living alone voted for Die Linke; age, housing, urbanisation and regional history may all contribute to the municipal pattern.", "analysis")
    md("education", "## Education has a stronger association within the published subset\n\nAmong the **54 municipalities with published school qualifications**, the share of residents aged 15+ with Abitur/Fachhochschulreife correlates with GRÜNE at **ρ = +0.757** and AfD at **ρ = −0.587**. Excluding the three independent cities leaves **+0.719** and **−0.519**, respectively. These results should not be generalised to the missing smaller municipalities. No other factor is controlled for in these estimates.", "analysis")
    blocks.append(dict(id="education-plot", type="chart", chartId="education"))
    md("education-interpretation", "The chart includes only municipalities with a published numerator and denominator. Its education measure describes the sampled household population aged 15+, not actual voters or the 2026 population. The adjacent party table uses this same restricted cohort.", "analysis")
    blocks.append(dict(id="education-table", type="table", tableId="education"))
    md("join", "## Join by AGS, with ARS conversion and boundary checks\n\nKeep identifiers as strings. Election records already contain the eight-digit **AGS**: Dessau-Roßlau is `15001000`, Halle `15002000`, Magdeburg `15003000`. Census municipality rows use a twelve-digit **ARS**. Convert it with `ars[:5] + ars[-3:]`, removing the four municipal-association digits, not by taking the first eight digits. For example `150815051026` (Apenburg-Winterfeld) maps to `15081026`.\n\nFilter the census to regional level **Gemeinde** before joining; it also contains state, district and association rows. All 218 converted keys match the election one-to-one, and all 218 full census ARS match the June 2026 register ARS. The age PDF uses five-digit district codes for the three independent cities; only those three are explicitly mapped to their corresponding municipality AGS.\n\nFor future vintages, check dated municipality changes as well as keys: a stable key alone cannot prove unchanged internal boundaries. Aggregate counts and their denominators onto one boundary vintage before recomputing rates. Postal codes and municipality names are not reliable primary keys. Constituencies can split municipalities; a city-wide demographic value cannot identify variation between its voting precincts.", "analysis")
    md("methods", "## Descriptive checks are complete; independent effects require a model\n\nThe reproducible script verifies election aggregation, 218 unique joins, source dates, 4,578 age rows, all age/sex sums, and per-feature missingness. It calculates Pearson r, average-rank Spearman ρ, valid-vote-weighted Pearson r, exclusion of the three independent cities, city-only Spearman, leave-one-district-out ranges, and a within-district correlation after demeaning ranked variables. The latter controls district-level rank differences only; it does not control other demographics. Leave-one-district-out ranges are sensitivity ranges, not confidence intervals.\n\nNo significance threshold is used: these are descriptive finite-set associations, with systematic source missingness and many comparisons. The maximum-selection table is particularly unsuitable as a significance ranking. No multiple-testing correction, spatial uncertainty model, causal identification or out-of-sample validation has been applied. Party shares are compositional and sum to 100%, and covariates such as education, rent, density and household structure are correlated.", "analysis")
    md("next", "## Build two models and validate geography before calling anything a factor\n\n1. Use the 218-municipality dataset for a small prespecified model with age, population/density, population change and selected household/housing indicators. Avoid redundant age shares and highly collinear predictors.\n2. Fit an explicitly separate education/employment model on the published subset, or aggregate **both votes and demographic counts** to municipal associations. Do not manufacture independent municipal observations from one association statistic.\n3. Add recent BA resident employment, registered unemployment counts, qualification structure and commuting where publication permits. An unemployment count divided by working-age residents is a proxy, not the official unemployment rate. Income-tax statistics measure taxpayers/taxable income, not disposable household income; district GDP is not municipal resident income. Verify each table’s geographic coverage and year before use.\n4. Use historical, boundary-harmonised 2021 party shares to study 2026 share changes or condition on prior support. BSW has no like-for-like 2021 party baseline. Keep first-vote candidate effects separate.\n5. Estimate fractional/compositional models with restrained complexity; compare equally weighted and valid-vote-weighted estimands. Validate by leaving whole districts or spatial blocks out, report uncertainty suitable for the small number of districts, and correct for multiple testing if inferential screening is added.\n\nThe remaining question is whether these municipal associations persist after correlated characteristics and prior party support are accounted for. This pilot supplies a validated starting dataset and a concrete source catalogue for that next step.")
    tables = [
        dict(id="catalogue", title="Available sources and exact table identifiers", dataset="catalogue", sourceId="catalogue", columns=[dict(field=f,label=l) for f,l in [("topic","Topic"),("provider","Database"),("table","Table / sheet"),("grain","Geographic level"),("availability","Verified status")]], defaultSort=dict(field="provider",direction="asc")),
        dict(id="coverage", title="Feature coverage and definitions", dataset="coverage", sourceId="analysis", columns=[dict(field=f,label=l) for f,l in [("label","Feature"),("n","Municipalities"),("missing","Missing"),("date","Reference date"),("definition","Definition")]], defaultSort=dict(field="n",direction="desc")),
        dict(id="strongest", title="Largest absolute full-coverage association for each party", dataset="strongest", sourceId="analysis", columns=[dict(field=f,label=l) for f,l in [("party","Party"),("label","Municipal characteristic"),("spearman","ρ, all 218"),("spearman_without_three_cities","ρ, exclude three cities"),("spearman_cities_only","ρ, 104 cities")]], defaultSort=dict(field="party",direction="asc")),
        dict(id="education", title="Abitur/Fachhochschulreife and party shares: restricted sample", dataset="education", sourceId="analysis", columns=[dict(field=f,label=l) for f,l in [("party","Party"),("n","Municipalities"),("spearman","Spearman ρ"),("spearman_without_three_cities","ρ, exclude three cities")]], defaultSort=dict(field="party",direction="asc")),
    ]
    charts = [dict(id="households", title="One-person households and Die Linke second-vote share", subtitle="218 municipalities; household share in 2022 and vote share in 2026, both in percent", type="scatter", dataset="municipalities", sourceId="analysis", encodings=dict(x=dict(field="single_household_share",label="One-person households (%)"),y=dict(field="F3_share",label="Die Linke second votes (%)")), options=dict(showLegend=False)),
        dict(id="education", title="School qualifications and GRÜNE second-vote share", subtitle="54 municipalities with published education data; 2022 qualifications and 2026 vote shares, both in percent", type="scatter", dataset="education_municipalities", sourceId="analysis", encodings=dict(x=dict(field="abitur_share",label="Abitur/Fachhochschulreife (%)"),y=dict(field="F6_share",label="GRÜNE second votes (%)")), options=dict(showLegend=False))]
    charts[0]["sourceId"] = "household-sql"
    charts[1]["sourceId"] = "education-sql"
    artifact = dict(surface="report", manifest=dict(version=1, surface="report", title=title, description="Verified data availability, AGS/ARS joins and exploratory correlations; snapshot dated 7 September 2026.", sources=sources, blocks=blocks, tables=tables, charts=charts), snapshot=dict(version=1,status="ready", datasets=dict(municipalities=records(household_chart),education_municipalities=records(education_chart),strongest=records(strongest.round(3)),coverage=records(coverage),education=records(education.round(3)),catalogue=catalogue)))
    # Native portable tables incorrectly require SQL even for documentary
    # catalogues and Python results. Preserve truthful provenance with bounded
    # Markdown tables rather than inventing an upstream SQL query.
    table_map = {t["id"]:t for t in tables}
    for block in blocks:
        if block["type"] != "table": continue
        table = table_map[block.pop("tableId")]
        rows = artifact["snapshot"]["datasets"][table["dataset"]]
        sort = table["defaultSort"]
        rows = sorted(rows, key=lambda r:r[sort["field"]], reverse=sort["direction"]=="desc")
        def escaped(value): return str(value).replace("|","\\|").replace("\n"," ")
        lines = [" | ".join(escaped(c["label"]) for c in table["columns"]), " | ".join("---" for c in table["columns"])]
        lines += [" | ".join(escaped(row[c["field"]]) for c in table["columns"]) for row in rows]
        block.update(type="markdown",body="### "+table["title"]+"\n\n"+"\n".join(lines),sourceId=table["sourceId"])
    artifact["manifest"]["tables"] = []
    (OUT / "artifact.json").write_text(json.dumps(artifact,ensure_ascii=False,indent=2,allow_nan=False))
    notes = """# Reproduction and validation

Run from the repository root with Python containing numpy, pandas, openpyxl and pdfplumber:

```sh
python3 scripts/analyze_lsa_demographics.py
python3 scripts/build_lsa_demographic_report.py
```

The first command reads the retained official PDFs/workbooks and frozen preliminary election JSON, validates joins/totals, and regenerates CSVs. No network, account, collection, commit or publication is required. The second generates the canonical report payload and executes the companion notebook. Use the Data Analytics portable builder to generate report.html from artifact.json; the generated HTML is self-contained.

Source URLs and retrieval hashes are in raw/acquisition_manifest*.json and source_checksums.json. The GV download page and workbook identify their separate boundary and measurement dates. GENESIS catalogue entries are distinguished from numeric downloads in source_catalogue.csv. The generic federal GET probe returned 405; the correct unauthenticated POST returned 401. Authentication is required by the current federal documentation. Further anonymous Regionaldatenbank catalogue requests returned a temporary login requirement, not evidence of absent tables.

The report is a new local research artifact. It does not modify election data or the website. The existing audit directory is an input, not rebuilt. The optional catalogue probes are preserved as access evidence. No API tokens were read or saved.

Chart contract: two native scatter plots, each one municipality per point, deliberately repeated to inspect two different party/feature relationships and coverage cohorts. No colour grouping; numeric axes and labels carry the meaning. Tables preserve exact results for all parties and source/feature lookup. The full correlation grid is in party_correlations.csv. Report structure maps to technical summary, definitions, source evidence, coverage, full-sample results, restricted-sample results, joins, methods/uncertainty, next steps and further questions.

Primary correlations are equally weighted and descriptive. The 15 selected maxima are drawn from 165 full-coverage comparisons, not independent causal effects. All 225 correlations are retained, including 60 restricted-sample comparisons. No inferential p values or confidence claims are made; future inferential screening requires multiplicity and spatial dependence treatment. Source suppression is not imputed. No high-dimensional model was fit or claimed validated.
"""
    (OUT / "README.md").write_text(notes)
    cells = []
    def markdown(text): cells.append(dict(cell_type="markdown",metadata={},source=text.splitlines(keepends=True)))
    def code(text): cells.append(dict(cell_type="code",metadata={},source=text.splitlines(keepends=True),execution_count=None,outputs=[]))
    markdown("# Sachsen-Anhalt municipal demographic pilot\nIndependent checks of the saved dataset and descriptive correlations. Rebuild raw extraction with `scripts/analyze_lsa_demographics.py`. The analysis is ecological, observational and preliminary.")
    code("from pathlib import Path\nimport json, pandas as pd, numpy as np\nroot = Path.cwd()\nif not (root / 'data/2026-lsa').exists():\n    root = next(p for p in root.parents if (p / 'data/2026-lsa').exists())\np = root / 'data/2026-lsa/reports/demographic-feasibility'\nd = pd.read_csv(p / 'municipality_analysis.csv', dtype={'ags':str, 'kreis':str, 'census_ars':str, 'ars_2026q2':str})\nr = pd.read_csv(p / 'party_correlations.csv')\nassert len(d) == d.ags.nunique() == 218\nassert d.valid_second_votes.sum() == 1315315\nassert d.population_2025.sum() == 2120252\nassert d.is_city.sum() == 104\nassert (d.census_ars.str[:5] + d.census_ars.str[-3:] == d.ags).all()\nassert (d.census_ars == d.ars_2026q2).all()\nprint('218 unique, complete municipal joins; 104 cities; vote/population totals match.')")
    code("coverage = pd.read_csv(p / 'feature_coverage.csv')\nassert (coverage.n == 218).sum() == 11\nassert d.abitur_share.notna().sum() == 54\nassert d.ilo_unemployment_share.notna().sum() == 52\nprint(coverage[['label','n','missing']].to_string(index=False))\nprint('Education cohort fraction of valid votes:', d.loc[d.abitur_share.notna(),'valid_second_votes'].sum()/d.valid_second_votes.sum())")
    code("official = json.loads((root / 'data/2026-lsa/reports/git-timeline/80fa3052/latest_official_rows.json').read_text())\nparties = json.loads((p / 'party_names.json').read_text())\nland = official['lsa:LAND:15:TOTAL']\nfor party in parties:\n    assert d[party + '_votes'].sum() == land['parties'][party]\n    assert np.allclose(d[party + '_share'], 100*d[party + '_votes']/d.valid_second_votes)\nassert np.allclose(d[[c+'_share' for c in parties]].sum(axis=1),100)\nprint('All 15 party numerators and every valid-vote denominator reconcile.')")
    code("for row in r.itertuples():\n    sample = d[[row.feature, row.code+'_share']].dropna()\n    assert len(sample) == row.n\n    independently_computed = sample.rank(method='average').corr().iloc[0,1]\n    assert abs(independently_computed-row.spearman)<1e-12\nprint('All 225 Spearman values independently reproduced from saved source joins.')\nprint(r[(r.feature=='abitur_share') & r.party.isin(['AfD','GRÜNE'])][['party','n','spearman']].to_string(index=False))")
    markdown("## Interpretation\nThe cohort includes every municipality, but education/employment comparisons use the published subset only. The study does not infer individual party preferences or causal effects. The report and CSV include sensitivity to the three independent cities, city status, district exclusions, weighting and within-district ranks. Follow-up models must deal with correlated features, prior party support, compositional shares, spatial dependence and source suppression.")
    env = {}
    count = 0
    for cell in cells:
        if cell["cell_type"] == "code":
            count += 1
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf): exec("".join(cell["source"]),env)
            cell["execution_count"] = count
            cell["outputs"] = [dict(output_type="stream",name="stdout",text=buf.getvalue().splitlines(keepends=True))]
    notebook = dict(cells=cells,metadata=dict(kernelspec=dict(display_name="Python 3",language="python",name="python3"),language_info=dict(name="python",version=platform.python_version())),nbformat=4,nbformat_minor=5)
    for i,cell in enumerate(cells): cell["id"] = f"lsa-demographic-{i}"
    (ROOT / "analysis/lsa_demographic_correlations.ipynb").write_text(json.dumps(notebook,ensure_ascii=False,indent=2))
    print('Created artifact.json, source catalogue, README and executed notebook (4 code cells passed).')


if __name__ == "__main__": main()
