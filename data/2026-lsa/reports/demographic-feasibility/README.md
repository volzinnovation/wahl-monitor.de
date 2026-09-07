# Reproduction and validation

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

## Delivery verification

The canonical HTML package passed validation and structural verification. Installed Chrome could not complete the static-chart check (11.2-second timeout), so browser layout, chart rendering and source-dialog interaction remain unverified. The generated semantic chart data and report text are retained. See report_qa.json. No browser was installed.

The renderer requires SQL provenance for native tables, including documentary catalogues. Those four bounded tables therefore use Markdown blocks with their original source metadata. The two native scatter datasets are independently calculated using the included executable SQLite queries over original vote/census counts; their results match the Python calculations. No substitute SQL provenance was invented.
