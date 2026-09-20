# Berlin and Mecklenburg-Vorpommern post-election reports

These are local ports of the Sachsen-Anhalt reports. They do not change polling,
deploy the website, commit results or publish reports.

## Retained artifacts and remaining work

Repository audit on 21 September 2026, against latest committed capture
`db48a805` (Berlin 00:53 CEST):

| Election | Retained report | Counting coverage in that report |
| --- | --- | --- |
| Mecklenburg-Vorpommern | [Full-count report, 00:05 CEST](../data/2026-mv/reports/post-election/full-count-2026-09-21-00-05-18/README.md) | 1,974 / 1,974 precincts; reconciled |
| Berlin | [Interim report, 00:02 CEST](../data/2026-be/reports/post-election/dry-run-2026-09-21-00-02-03/README.md) | 4,071 / 4,114 precincts; reconciled |

The earlier MV dry run remains in the [MV report index](../data/2026-mv/reports/post-election/index.html).
The [Berlin report index](../data/2026-be/reports/post-election/index.html) and
[election-night artifact inventory](mv-berlin-election-night-artifacts.md) link the
other retained outputs. The standalone MV Wahlkreis 14 comparison is retained as
[CSV](../data/2026-mv/reports/wahlkreis-14-briefwahl-vs-urnenwahl.csv) and
[PNG](../data/2026-mv/reports/wahlkreis-14-briefwahl-vs-urnenwahl.png).

Remaining completion requirements, based on the retained evidence:

- Berlin's latest committed capture is newer than its frozen report and contains
  4,095 / 4,114 precincts. Nineteen precincts remain unreported in that capture;
  rebuild the completion report when the reconciliation gate passes.
- Both elections still need reviewed official seat evidence for the final
  representation waterfall. The retained MV mandate check at 00:23 CEST found
  only a zero-seat template, explicitly rejected as seat evidence.
- Berlin's retained source has no precinct-level export. Precinct dispersion and
  exact voting-mode counts remain unavailable; the existing mode comparison uses
  separately timestamped official percentages.

The audit reran all 12 offline regression tests and independently verified all
three retained packages: source/output hashes, vote aggregation, demographic
correlations, waterfall arithmetic, embedded charts and available model validation.
These checks validate the frozen packages, not a later official result.

## Setup and dry run

Use Python with numpy, pandas, Matplotlib, openpyxl, scipy and statsmodels (the same
scientific dependencies used by the LSA analyses). For an isolated environment:

```sh
python3 -m venv .venv-reports
.venv-reports/bin/python -m pip install numpy pandas matplotlib openpyxl scipy statsmodels
```

Prepare static sources once. `--download` retrieves only missing public official
context files; it does not poll election results. MV reuses the retained nationwide
Zensus workbooks in the LSA report directory. Both preparations save small source
extracts, definitions and hashes under the respective `reports/post-election/sources/`.

```sh
python3 scripts/prepare_post_election_demographics.py --election-key 2026-be --download
python3 scripts/prepare_post_election_demographics.py --election-key 2026-mv --download
python3 scripts/build_post_election_report.py --election-key 2026-be --dry-run
python3 scripts/build_post_election_report.py --election-key 2026-mv --dry-run
python3 scripts/test_post_election_reports.py
python3 scripts/verify_post_election_report.py data/2026-mv/reports/post-election/<capture-directory>
```

Each build creates a dated directory containing a self-contained German HTML report,
Markdown analysis, PNG/SVG charts, exact CSVs, a source manifest and validation JSON.
`reports/post-election/index.html` lists successful reports. Existing captures are
never overwritten. For a revision of a capture, use a new `--output-dir`.

The matching local `data/<key>/raw/statla/<run_label>-statla.csv` is required because
the normalized CSVs omit the electorate and some geographical/mode fields. If it is
absent, run the existing poller locally or replay a retained report's source directory:

```sh
python3 scripts/build_post_election_report.py --election-key 2026-be --dry-run \
  --source-dir data/2026-be/reports/post-election/<capture-directory>/sources \
  --output-dir /tmp/berlin-report-replay
```

Replay uses the frozen election, demographic and geometry bytes. The counting history
is reconstructed from locally available git captures up to the frozen evidence cutoff;
the report records the exact git ref. Retain this repository's history to reproduce it.

## Completion rebuild

Read-only status (exit 0 = reconciled full counting coverage; exit 2 = pending):

```sh
python3 scripts/build_post_election_report.py --election-key 2026-be --check
python3 scripts/build_post_election_report.py --election-key 2026-mv --check
```

All Land, constituency and municipality/district counters must be complete. Party
sums and independently aggregated geographical levels must reconcile. The original
and normalized source totals must match. No arbitrary elapsed-time or percentage
threshold can promote an incomplete capture.

Regenerate the full-count results and demographic report as soon as these checks
pass, even if the separate official seat publication is still pending:

```sh
python3 scripts/build_post_election_report.py --election-key 2026-mv --complete-count
```

This creates a `full-count-<capture>` report without the general dry-run label.
The waterfall retains its explicit seat scenario until an official seat table is
available. The vote-data cutoff remains the actual capture time, even when a later
source check confirms unchanged data.

Once those checks pass, retain the **official seat publication** and extract a reviewed
CSV with columns `party,seats` (exact party names as in `statla_party_results.csv`).
Use the publication URL as `--seats-source`; keep the original publication beside
the extracted CSV for review. Then run for each election:

```sh
python3 scripts/build_post_election_report.py --election-key 2026-be --require-complete \
  --seats-file /path/to/berlin-official-seats.csv --seats-source https://official-seat-publication
python3 scripts/build_post_election_report.py --election-key 2026-mv --require-complete \
  --seats-file /path/to/mv-official-seats.csv --seats-source https://official-seat-publication
```

Without the seat table, `--require-complete` refuses a report. Dry runs and
`--complete-count` without seats label the waterfall as a scenario using parties at 5% or above plus current
constituency first-vote leaders. An incomplete electorate-minus-voters remainder is
labelled as not yet recorded voters, **not** as a known final number of nonvoters.
Population is official population at 31 December 2025, so its difference from the
2026 electorate is explicitly approximate.

An automation attached to the originating task checks for completion and performs
the local rebuild when inputs are ready. Its stopping condition must include a
reviewed official seat source. It must also revisit any missing Berlin precinct
export before declaring the full requested report scope complete. No deployment
or publishing is authorized by this report workflow.

## Ported scope and geographical changes

| LSA analysis | Berlin/MV implementation |
| --- | --- |
| Political representation waterfall | Population → electorate → recorded voters → valid votes → represented parties; official seats required for completion |
| First/second votes; geographic dispersion | Both elections; precinct dispersion where precinct rows exist |
| Voting mode | MV booth names identify postal/in-person ballots; Berlin can use the official published percentage chart retained as `sources/official_voting_modes.html`, with a separate timestamp and no inferred absolute counts |
| Winner and majority maps | Constituency winner, AfD absolute majority, combined Linke/SPD/GRÜNE absolute majority |
| Counting and party trajectories | All locally archived live CSV captures up to the report cutoff |
| First positive/full AfD arrivals | Actual share at first observation, not substituted final values |
| Descriptive and weighted demographics | Pearson, Spearman, weighted ranks, grouped binary ballot/context correlation, complete-area sensitivity, full feature grid and coverage |
| Geographic sensitivity | Leave one Kreis out (MV), leave one Bezirk out (Berlin); within-Kreis ranks and MV exclusion of Rostock/Schwerin in CSV |
| Grouped binomial models | MV: five prespecified covariates, geographic validation, training-only scaling, coefficients and overdispersion; Berlin: insufficient independent areas for that specification |
| LSA-specific anomalies and external audit comparisons | Not assumed to recur; generic observed changes are retained in `area_changes.csv` |

**MV:** The result feed contains synthetic municipality IDs for pooled Amtsbriefwahl.
The demographic unit is therefore an Amt or an amtsfreie Gemeinde, using the census's
own Amt totals and actual result `Amt` codes. Votes from all 799 reporting units join
to 116 non-overlapping demographic units, including pooled postal votes. Individual
municipality correlations would confound that missing postal allocation.

**Berlin:** The district code joins twelve Bezirke to the official Berlin Zensus
workbook. The repository's synthetic `11000001` etc. keys are not municipal AGS.
No census value for the whole municipality Berlin is duplicated across constituencies.

All thirteen demographic features refer to the 2022 census. Published age bands are
67+ and 19–24, differing from the LSA 2025 age data. Density and 2024–25 population
change are omitted without comparable dated source inputs. Education/employment
suppression stays missing. Correlations are ecological/descriptive, with no causal
or individual-voter claims and no ballot-level significance tests.
