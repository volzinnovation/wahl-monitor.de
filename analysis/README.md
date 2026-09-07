# TODO Analysis

* Check Basic Consistency of Data (Totals across rows, Totals across Columns)
* Check Deltas to prior Versions, show news
* Basic stats: min, max, mean
* Calculation of relative numbers based on absolute numbers published
* Chart results

## Sachsen-Anhalt 2026: complete archived history

The [current German tweet report](../data/2026-lsa/reports/git-timeline/80fa3052/REPORT.md)
rebuilds all results and charts through Git endpoint `80fa3052044a45af29f4f0b2867957d8a3b1df35`
(last data capture 7 September 2026, 04:06:37 MESZ). It covers 124 data commits,
122 dated captures (including three early zero templates), and 119 election-night
observations. The report contains ten findings in increasing priority (10 is highest), with 25 PNG images in the series and supporting appendix.

All 2,661 final individual vote identities match the HTML status inventory.
All voter, valid-vote and party aggregates reconcile across geographic levels.
The historical Aschersleben gap is closed; one status withdrawal, 20 municipal
revisions and the new Aken identity remain documented. Polling remains stopped.

- [Complete reproducible ZIP](../data/2026-lsa/reports/git-timeline/lsa-full-history-report-80fa3052.zip): scripts, notebook, evidence, frozen comparison inputs and checksums.
- [Audit notebook](lsa_git_audit.ipynb): eight executed Python cells independently check raw CSVs, historical changes, external methods and the new chart calculations.
- [Three-election relative share changes](../data/2026-lsa/reports/git-timeline/80fa3052/charts/25_drei_wahlen_relativer_anteilsdelta.png): first and last positive statewide Git results, six parties, BW/RLP/SA. First RLP export has a documented FREIE WÄHLER parser defect outside the displayed cohort; no renormalization of the valid-vote denominator.
- AfD arrival distributions: [first positive result](../data/2026-lsa/reports/git-timeline/80fa3052/charts/22_afd_erste_ergebnisse.png) and [first complete result](../data/2026-lsa/reports/git-timeline/80fa3052/charts/23_afd_vollmeldungen.png), with separate geographic panels and actual arrival shares.
- [Political representation waterfall](../data/2026-lsa/reports/git-timeline/80fa3052/charts/24_politische_repraesentation.png): population, electorate, participation, valid votes and votes for parties with seats.
- [Comparison with Lauras Wahlforensik](../data/2026-lsa/reports/git-timeline/80fa3052/EXTERNAL_COMPARISON.md): 135,837 matching source values, 24,171 independently reproduced robust-MAD values and 86 exact Git revision matches; statistical and historical evidence kept distinct.
- [Aschersleben reconciliation](../data/2026-lsa/reports/git-timeline/80fa3052/AGGREGATION_ASCHERSLEBEN.md), [Aken 000010](../data/2026-lsa/reports/git-timeline/80fa3052/AKEN_000010.md) and [Bitterfeld-Wolfen 000028](../data/2026-lsa/reports/git-timeline/80fa3052/BITTERFELD_WOLFEN_000028.md). Final precinct votes are available; historical per-precinct party differences cannot be reconstructed from one final snapshot.
- [Methods and exact reproduction commands](../data/2026-lsa/reports/git-timeline/80fa3052/METHODS.md) and [validation](../data/2026-lsa/reports/git-timeline/80fa3052/VALIDATION.md).
- [Existing results animation](../data/2026-lsa/reports/git-timeline/animation-20260907T020335Z/README.md): 114 Git captures plus the saved 04:03 MESZ source check, ending at 1,315,315 valid second votes. All 115 observations fit in 80 seconds, with six parties alphabetically and a fixed 0–60% axis. This movie was not regenerated in the current report update.
- Earlier reports remain available: [03:10 full-history report](../data/2026-lsa/reports/git-timeline/23377a96/REPORT.md) and [original 98.27% report](../data/2026-lsa/reports/git-timeline/034045f3/REPORT.md).

From the repository root, follow the pinned commands in METHODS.md. The analyzer
uses the Python standard library; charts require Matplotlib and NumPy. Preserve
the frozen external and population inputs in the report directory. The notebook
can be executed with `python3 scripts/run_lsa_audit_notebook.py` without Jupyter.
None of these commands collects live data, changes schedules, commits, pushes or
publishes. Validation passed 18 regression tests, all eight notebook cells and an
independent byte-identical reconstruction of 124 evidence/chart files.
