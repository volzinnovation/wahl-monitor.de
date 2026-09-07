# Methods and reproducibility

Git endpoint: `80fa3052044a45af29f4f0b2867957d8a3b1df35`; last data-changing commit: `eeae7f0d9e0cbf3b825b19eb14727dc64704ac7b`.
Capture time: `2026-09-07T02:06:37.855848+00:00` UTC = `2026-09-07T04:06:37.855848+02:00` Europe/Berlin.
The audit reads all 124 first-parent data commits, including 122 dated captures, three pre-opening templates and two setup steps. First-parent scope is checked against the full reachable data-commit set. No live election source is requested.

## Reproduce from the repository root

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref 80fa3052044a45af29f4f0b2867957d8a3b1df35 --full-history --require-complete --output data/2026-lsa/reports/git-timeline/80fa3052
python3 scripts/compare_lsa_external_analysis.py --input data/2026-lsa/reports/git-timeline/80fa3052
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/80fa3052 --baseline data/2026-lsa/reports/git-timeline/034045f3
python3 scripts/run_lsa_audit_notebook.py
python3 scripts/verify_lsa_git_report.py --input data/2026-lsa/reports/git-timeline/80fa3052 --visual-reviewed
python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v
```

The external directory is a frozen input. Keep it with a second output directory before running the comparison. The sources are versioned JSON payloads plus the catalogue/index; verify every archived payload against external/manifest.json. It is not a Git election observation. Acquisition requires network only when explicitly refreshing the external comparison; reconstruction itself is offline.

## Measures and event units

All valid second votes are the party-share denominator; choose a fixed cohort strictly above 5% in the final Land row. Every party is included in numerical audits. Sum counts and denominators before computing aggregate percentages. Each geographic level is evaluated independently; do not add overlapping levels. Municipality-to-Kreis uses AGS; individual booths use municipality AGS and their actual constituency, including split municipalities. U+B is checked against each available total.

Missing numeric cells remain null, not zero. Blank candidate cells are omitted from party sums and retained as missing cells in exports. The final regional CSV omits Ist.Wahlbezirke, Soll.Wahlbezirke and Uhrzeit; it renames Gewählt im Wahlkreis to Gewählt.im.Wahlkreis and switches Ergebnisart Z to V. Null counters are not a district disappearance, vote reset or numerical denominator decrease. Final completeness is established separately by HTML, municipality counters, 2,661 matching individual identities and reconciled vote aggregates; raw Land counters remain null.

A fixed-count revision is one area transition with changed voter/valid-vote/party values at equal non-null reported counts; after-complete means the previous positive reported and expected counts agreed. Candidate events also include decreases, missing values, disappearance, reset and changes in two known denominators. U/B and parent copies are retained but not counted as separate municipal incidents. The 162 historical aggregate differences are 27 fields at two relations in three captures; the final capture has no nonzero difference.

## Statistical comparison

GFrei.News's frozen robust-MAD groups are independently rebuilt from original Git CSV rows: complete municipality totals and individual booths, voters >=100, split by geographic level, voting mode and floor(log10(voters)); >=20 peers; z=(value-median)/(1.4826*MAD). Features are turnout (non-postal A>0), both invalid-ballot rates, and six statewide >5% party shares. The original orange/red thresholds are reproduced, not treated as p-values or calibrated fraud probabilities. Every checked observed value, median, MAD, group size, z and classification matches. Eighteen insufficient comparisons are not silently filled.

All 86 external revision flags match exact raw Git before/after values. Different capture clocks are retained. Five digit-test observation samples reproduce exactly; external simulation-derived q-values are cited as external results, not a locally recomputed significance test. Multivariate and spatial model results are not independently validated here. The local-context chart uses the same voter-size class and voting mode within the booth's municipality; it is descriptive and includes the queried booth.

## Limits and provenance

Status observations begin at 19:05; one earlier zero template lacks original source CSV bytes. Only the last Git capture contains individual vote rows. Thus final values cannot identify historical per-booth revisions or decompose multi-booth municipality changes. Observed time is the collector's capture time, not necessarily publication, counting or certification time. Step charts connect observations but do not establish continuous unchanged state between them. No inference of electoral misconduct follows from these checks; plausible but incorrect underlying records may pass arithmetic.

Reconstruction changes only derived artifacts. It does not poll, alter schedules, commit, publish or deploy. The earlier reports and movie remain available. The prior polling-stop control record is historical evidence, not a newly queried control state.

## Arrival distributions, representation and three-election share comparison

AfD arrival charts show each of 218 municipalities, 41 constituencies and 14 counties once, with its AfD share at the actual first positive or first complete archived result; these are separate definitions. Final WBZ reconciliation supplies completion evidence for remaining higher-level rows with removed counters. Equal-area boxplots use 30-minute bins only with at least five arrivals. Whiskers are 1.5 IQR, not uncertainty intervals. Later revisions do not overwrite the first observed share.

The representation waterfall follows the BW/RLP structure. Population 2,120,252 on 31 December 2025 comes from the retained official release linked in representation_sources.json. The election electorate is 1,706,851. Because dates differ, population minus electorate is an approximate contextual bridge. Parties with seats come from the archived preliminary seat CSV; their 1,224,292 second votes reconcile to the sum of six qualifying parties. The chart shows votes, not seat proportions or a legal measure of representation.

For BW/RLP/SA, first and last mean first/last positive LIVE statewide result in the selected Git history. Later PREP_ZERO states are excluded and logged. Exact endpoint normalized files and hashes are retained in cross_election_sources. Party sums are checked against valid second votes. The first RLP export has a documented 4,652-vote residual: its parser misclassified FREIE WÄHLER as the voter total (fixed in 80b01c01). This party is outside the displayed cohort; selected party votes and the published valid denominator remain unchanged. No renormalization to the incomplete party sum is performed. Relative change is 100*(last_share-first_share)/first_share; a missing or zero first share is undefined. The cohort is the union strictly above 5% at any final endpoint. The first RLP voter count is inconsistent and is not used in the share calculation; independently retained original result payloads are unavailable for BW/RLP at these endpoints.

The electorate check retains 152 partial-state observations (17 overlapping area rows) as denominator limitations, separate from arithmetic contradictions in complete results. Postal voters can arrive alongside only part of the in-person electorate; B/A is then not an election-turnout estimate. See turnout_denominator_observations.json.
