# Berlin post-election report

Open `report.html` (self-contained, offline). PNG/SVG charts and exact CSVs accompany it.

Mode: dry-run; evidence cutoff: 2026-09-20T22:02:06.433854+00:00. This is not a certified final result.

Ported from `analyze_lsa_demographics.py`, `analyze_lsa_vote_weighted.py`, the LSA timeline/additional chart scripts and the BW representation waterfall. LSA-specific anomaly stories are not assumed to occur here.

Rebuild current data from the repository root:

```sh
python3 scripts/build_post_election_report.py --election-key 2026-be --dry-run
```

Use the documented `--require-complete --seats-file ... --seats-source ...` workflow for the completion report. Existing captures are retained. `sources/` preserves exact input bytes; `manifest.json` records hashes. The demographic source manifest documents its workbook/extract provenance.

See `analysis.md` for interpretation, `validation.json` for checks, `demographic_coverage.csv` for missingness, and `chart_map.json` for chart-to-CSV mapping.
