# Validation report

## Overall assessment: Share with caveats

The interim report answers the archived-data question. It does not establish election misconduct or reconstruct votes for district 000028.

## Checks completed

- 379 source objects match the archived SHA-256 manifests.
- 0 arithmetic violations; 0 normalized/source differences.
- 40,880 geographic and 669,200 U+B field comparisons; zero non-null residuals. Repeated states are included.
- 17 tweet drafts; maximum 259 weighted characters; all local links resolve.
- 14 PNGs; visual review recorded: True.
- Independent rerun: 46 byte-identical files; exact list in verification.json.
- Bitterfeld municipality party deltas reconcile to first- and second-vote total deltas separately. District attribution is explicitly unavailable.

## Independent checks and regression tests

The companion notebook parses original CSVs independently, recomputes municipality and party totals against Land, and reads the overview JSON at both Bitterfeld captures. All five code cells executed sequentially with Python. A Jupyter kernel runner was unavailable locally (nbformat/nbclient missing); no dependencies were installed.

`python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v` passes 12 tests covering growth, redistribution, missing vs zero, resets, initial templates, row order, arithmetic, separate geographic sums, the completeness guard, and future booth U/B grouping. The CLI `--require-complete` also correctly returned nonzero on this 2614/2660 snapshot. Initial implementation and notebook syntax errors were fixed before this validation. No source data was altered to pass checks.

## Required caveats

- Reporting completeness is not vote completeness or official certification.
- District-status history starts at 19:05; original district vote rows were absent.
- Bitterfeld 000028 returns with four other districts. Municipality deltas cannot be assigned to it.
- HTML/CSV publication timing can differ. Source lag is not a count of lost ballots.
- Changes between captures and offsetting revisions can remain unseen; arithmetic alone cannot identify causes or rule out substantive errors.

No remaining blocker to a labelled interim report. Missing historical district vote data blocks individual-district party attribution. No tweets or website were published.

## Aken addendum validation

- Replayed 8 post-baseline HTML observations; verified all 24 retained files against Git blob hashes and every HTML file against its collector SHA-256 and byte count.
- Checked 191,520 baseline status rows for any earlier occurrence of Aken 000010; none found.
- Exactly one added identity and no removed identities in the adjacent captures; total districts increase 2660 to 2661. Both HTML and CSV metadata confirm the changed total.
- First presence is in the 00:51:10 MESZ archive run, following absence at 00:45:41. Exact HTML-fetch times are retained separately; neither timestamp is asserted to be the precise publication time.
- The original 15 tweets and baseline evidence remain unchanged. Two addendum tweets and one image cover only this later finding; statewide vote shares and anomaly counts were not rerun.
- The addendum was rebuilt independently from the original report package and bundled sources; its generated report, data and image were compared byte-for-byte.
- Individual precinct party votes and the reason for adding the entry remain unavailable in these sources.
