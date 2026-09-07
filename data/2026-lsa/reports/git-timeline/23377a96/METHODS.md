# Methods and reproduction

## Scope and report contract

All available Sachsen-Anhalt data commits on the archived default-branch history, including pre-election zero templates and an inventory of setup commits. Time-series images show the election-night window from 6 September 18:00 Europe/Berlin. The selected primary artifact is the user-requested tweet
series with standalone PNGs; REPORT.md is its reading copy. Audience: general
stakeholders. Summary → findings with visuals → next run/questions → caveats.
The Twitter format is the explicit surface override; no website is published.

## Reproduce this exact snapshot

From the repository root, using Python 3 with Matplotlib and NumPy:

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref 23377a96d918a4b77f71dbabbf9c385f78334126 --full-history
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/23377a96
python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v
```

The companion `analysis/lsa_git_audit.ipynb` exposes the audit and spot checks.
Analysis uses the standard library; chart versions are recorded in environment.json.
All inputs are read with `git show` from full commits, never from mutable latest
files, SQLite, live websites, or the site build. Neither command polls, commits,
pushes, deploys, publishes tweets, or changes schedules.

## Full-data rerun

After the collector has archived a new complete capture and that Git history is
available locally, select its full commit hash as NEW_COMMIT:

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref NEW_COMMIT --output data/2026-lsa/reports/git-timeline/full-rerun --require-complete
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/full-rerun --baseline data/2026-lsa/reports/git-timeline/23377a96
```

Use the renderer only after the first command succeeds. The guard checks
reported districts equal the positive expected total; it does not certify a
legally final result. The next run recalculates the >5% party cohort at its own
cutoff and retains the entire earlier history. No automatic wakeup is scheduled.

## Observations and provenance

- Cutoff commit: `23377a96d918a4b77f71dbabbf9c385f78334126`.
- 121 dated captures (118 after 18:00, 3 pre-opening templates); 50 distinct normalized numeric/name states.
- First capture: 2026-08-20T12:54:58.222512+02:00.
- Last capture: 2026-09-07T03:10:10.827443+02:00.
- Status coverage starts: 2026-09-06T19:05:31.763958+02:00; 110 observations.
- Largest interval between saved acquisitions: 24445.27 minutes.
- 575 retained source objects match the capture manifest SHA-256.
- Branch first-parent order defines chronology; acquisition timestamps are labels,
  not source modification times. The original CSV retains local source timestamps.
- Pre-opening zero templates are included in the data audit and explicitly separated from election-night charts. Initial zero-denominator templates are
  retained with undefined percentage; population initialization is not an anomaly.
- [Official download page](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html).
- [Pinned source directory](https://github.com/volzinnovation/wahl-monitor.de/tree/23377a96d918a4b77f71dbabbf9c385f78334126/data/2026-lsa/latest/official_sources).

## Keys, denominators, and aggregation

Normalized keys are official area type + identifier. Raw keys add the U/B/TOTAL
mode; only TOTAL contributes to area aggregates. District-status keys combine
Wahlkreis, Kreis, municipality label, and district number, since the overview
does not expose AGS. Identity is never array position or a district number alone.
Numeric blanks remain missing; they never become zero. Party blank cells mean
not applicable and are explicitly counted in aggregation missingness columns.

Party selection: strictly above 5% of all valid Land second votes at the cutoff.
Use that same cohort at every earlier date and every locality; no per-place
reselection and no renormalization to selected parties. All parties remain in
the audit, including those below 5%. First and second votes are checked separately.
Shares are sum(votes)/sum(valid votes), not averages of percentages. City-versus-
rest is a disjoint partition. Distribution boxes weight geographic units equally
and are explicitly distinguished from the vote-weighted Land result.

Municipalities sum to Kreise by the first five AGS digits. Each of municipality,
Kreis, and Wahlkreis levels separately sums to Land. Kreis and Wahlkreis totals
overlap and must never be summed together. Split municipalities are not assigned
whole to Wahlkreise; direct official Wahlkreis values are used. Individual
district vote totals cannot be inferred from municipality totals or status rows.

## Detection and interpretation

The audit compares every consecutive observation of an area, including all
party values, voters, valid votes, and reporting counters. It records row loss,
reappearance, missing-value transitions, resets, numeric decreases, denominator
changes, any vote revision at unchanged reporting count, and changes after the
previous snapshot reported 100%. Raw U/B rows add invalid votes and eligibility.
Initial population setup and growing result counts are retained as ordinary
events. Status changes are a separate ledger; a lost status is not a lost vote.

One event = one area and one snapshot transition, possibly several changed
fields. Municipal revisions and the same changes in their Kreis/Wahlkreis/Land
must not be counted as independent incidents. Fixed-reporting-count changes are
a lower bound: corrections may occur while new reports arrive, and offsetting
changes may be invisible between captures. A stable count does not identify
which individual district was corrected. No causal or fraud classification is made.

CSV/HTML source lag is kept separate from within-file arithmetic. A status
withdrawal time is interval-censored: after the prior capture and at/before the
first capture with zero. Restoration is bounded analogously. Step charts draw
observed states; they do not prove continuous state between samples.

## Supporting artifacts

`area_timeline.csv.gz` and `party_timeline.csv.gz` form the normalized history.
`raw_timeline.jsonl.gz` preserves all raw modes, timestamps, parties and extras.
`status_timeline.jsonl.gz` preserves every archived status row.
`candidate_events.csv` is the normalized review ledger; `raw_candidate_events.csv`
also includes invalid-vote and mode-specific changes. `area_events.csv` retains
ordinary growth. `aggregation_checks.csv.gz` includes zero residuals and missing
child counts. Source links, byte hashes and times are in `source_manifest.json`.
`chart_map.json` documents question, data, palette and interpretation per chart.
`tweets.json` carries body, weighted character count, images, alt text and evidence.

## Known limits for the next run

No initial district vote export existed at this cutoff. The parser accepts the
documented Wahlbezirk CSV identity fields when they first appear. Any schema
change fails explicitly and must be reviewed, not converted to an empty table.
New district files cannot reconstruct missing historical vote snapshots.
Freshness and capture gaps limit all negative findings. Arithmetic checks alone
cannot detect a plausible but factually incorrect count or attribute intent.

## Full-history source reconciliation and stopped collection

The final source files disagree across geographic CSV levels. The verifier must reproduce the nonzero residuals; it must not edit or exclude them to obtain zero. `aggregation_gap.json` matches the entire latest 27-field residual vector to the new Aschersleben municipality row transition. Its 1,513 second-vote delta reconciles across all parties; repeated appearances under Land and Kreis and across three captures are not independent incidents. The official Land denominator remains the headline party denominator. Municipality sums are explicitly labelled computed aggregates. `district_identity_changes.json` identifies every new, removed or reappearing status identity and compares the Aken transition directly. `post_midnight_revisions.json` preserves the five additional fixed-count revisions.

Polling was stopped at the user's request: the Codex heartbeat is paused and the GitHub archive workflow is disabled. `polling_stopped.json` records the control-state verification. No scheduled rerun is planned. Reproduction reads the fixed Git endpoint and never resumes collection. A future complete-source audit requires already archived consistent data or new authorization to collect it.

The full-history renderer is `scripts/lsa_full_history_report.py`, called automatically when the analyzer's `--full-history` flag is present. For the comparison section, pass `--baseline data/2026-lsa/reports/git-timeline/034045f3` to the renderer. Keep `polling_stopped.json` with the output as external control-state evidence; it does not change numerical analysis.
