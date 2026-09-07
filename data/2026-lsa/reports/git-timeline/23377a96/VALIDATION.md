# Validation report

## Overall assessment: Share with caveats

The interim report answers the archived-data question. It does not establish election misconduct or reconstruct votes for district 000028.

## Checks completed

- 575 source objects match the archived SHA-256 manifests.
- 0 arithmetic violations; 0 normalized/source differences.
- 61,831 geographic and 1,003,800 U+B field comparisons; 162 and 0 nonzero residuals respectively. Repeated states are included.
- 19 tweet drafts; maximum 257 weighted characters; all local links resolve.
- 16 PNGs; visual review recorded: True.
- Independent rerun: 51 byte-identical files; exact list in verification.json.
- Bitterfeld municipality party deltas reconcile to first- and second-vote total deltas separately. District attribution is explicitly unavailable.

## Independent checks and regression tests

The companion notebook parses original CSVs independently, recomputes municipality and party totals against Land, and reads the overview JSON at both Bitterfeld captures. All six code cells of the updated notebook executed sequentially with Python. A Jupyter kernel runner was unavailable locally (nbformat/nbclient missing); no dependencies were installed.

`python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v` passes 14 tests covering growth, denominator changes, cross-level vote gaps, redistribution, missing vs zero, resets, initial templates, row order, arithmetic, separate geographic sums, the completeness guard, and future booth U/B grouping. The completeness guard rejects the unfinished Land counter. No source data was altered to pass checks.

## Required caveats

- Reporting completeness is not vote completeness or official certification.
- District-status history starts at 19:05; original district vote rows were absent.
- Bitterfeld 000028 returns with four other districts. Municipality deltas cannot be assigned to it.
- HTML/CSV publication timing can differ. Source lag is not a count of lost ballots.
- Changes between captures and offsetting revisions can remain unseen; arithmetic alone cannot identify causes or rule out substantive errors.

No remaining blocker to a labelled interim report. Missing historical district vote data blocks individual-district party attribution. No tweets or website were published.

## Full-history findings validation

- Inventoried 123 data commits: 121 dated captures, 2 setup commits without capture times. Includes 3 earlier zero templates, with 1 missing original source CSV explicitly disclosed.
- Independently parsed the latest original CSVs in the notebook: municipality minus Land equals 1517 voters, 1509 valid first votes and 1513 valid second votes. Every party delta matches the published Aschersleben municipality transition.
- Replayed all 162 nonzero geographic fields without suppressing them: the same 27-field vector at two aggregation relations in three captures. Zero within-row arithmetic violations and zero normalized/source replay differences.
- Verified one added status identity (Aken 000010), no removed identities, one status withdrawal, and five additional fixed-count municipal revisions after the original report.
- The 2661/2661 HTML counter is not substituted for the 2660/2661 Land CSV counter. Computed municipality totals are explicitly separated from official Land totals and shares.
- Polling-stop evidence records the paused heartbeat, disabled GitHub workflow and empty active/queued collection list. No further collection or publication is scheduled.
