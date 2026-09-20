# MV and Berlin election-night artifacts

Frozen, local analysis of the 20–21 September 2026 election-night captures. The
updated MV video ends with all 1,974 precincts reported at 00:05 CEST on
21 September; the Berlin video and both voting-mode comparisons retain their
earlier interim cutoffs. Complete counting does not imply a certified final
result. Nothing is published or scheduled by these generators.

| Election | Timelapse and audit data | Statewide Briefwahl / Urnenwahl comparison |
| --- | --- | --- |
| Mecklenburg-Vorpommern | [78 captures, 20 Sep 19:12–21 Sep 00:05 CEST; all precincts reported](../data/2026-mv/reports/git-timeline/animation-604f5ef2/README.md) | [23:46:56 CEST, 1,973 / 1,974 precincts](../data/2026-mv/reports/statewide-voting-modes-60845e75/README.md) |
| Berlin | [68 captures, 19:32–23:47 CEST](../data/2026-be/reports/git-timeline/animation-60845e75/README.md) | [23:59:22 CEST, 4,071 / 4,114 precincts](../data/2026-be/reports/statewide-voting-modes-20260920/README.md) |

Both movies use the Sachsen-Anhalt layout: 1920 × 1080, 25 fps, 80 seconds,
alphabetical parties, a 0–60% scale and a fixed cohort strictly above 5% in the
last archived state total. Acquisition intervals are compressed proportionally,
without interpolating votes. MP4 and MPEG-2 versions are included.

Both static charts use the Wahlkreis-14 comparison rule: parties strictly above
5% in either voting mode, with all valid second votes in that mode as the
denominator. PNG, SVG, CSV and a short German analysis are included.

MV uses precinct-level counts, reconciled party by party to the state total.
Berlin uses the official state's published mode percentages. Its archived CSV
has no mode split, and the saved official chart does not expose exact counts or
valid-vote denominators by mode. Those fields remain missing, not zero, in the
comparison CSV. Berlin's `Sonstige` is a composite, not a party eligible for the
chart's >5% selection. The timelapse cutoff and comparison cutoff differ.

## Reproduce

From the repository root:

```sh
python3 scripts/render_state_results_animation.py --election 2026-mv --ref 604f5ef2 --output /tmp/mv-animation
python3 scripts/render_state_results_animation.py --election 2026-be --ref 60845e75 --output /tmp/be-animation
python3 scripts/analyze_state_voting_modes.py --election 2026-mv --source-dir data/2026-mv/reports/statewide-voting-modes-60845e75/sources --output /tmp/mv-modes
python3 scripts/analyze_state_voting_modes.py --election 2026-be --source-dir data/2026-be/reports/statewide-voting-modes-20260920/sources --output /tmp/be-modes
```

The mode-analysis replay reads frozen, hashed inputs without network access.
Omitting `--source-dir` reads MV from the requested Git ref and downloads Berlin's
current official results page. Use a new output directory for a new cutoff.
Dependencies: Python, Matplotlib, BeautifulSoup, ffmpeg and ffprobe.

On the author's machine, Homebrew ffmpeg 8.1.1 referenced libx265.216 while the
active x265 installation supplied libx265.217. The already-installed compatible
library was selected only for the movie processes with
`DYLD_LIBRARY_PATH=/opt/homebrew/Cellar/x265/4.2/lib`; no system install or library
links were changed. Other machines with working ffmpeg need no override.

## Validation performed

- Every timelapse's all-party second-vote sum equals its recorded Land denominator;
  times strictly increase, every retained capture receives frames, and all bars
  fit the fixed axis.
- Both MP4s and both MPEG-2 files fully decode. Each contains 2,000 frames,
  1920 × 1080 pixels, 25 fps, and 80 seconds.
- Updated MV endpoint: all 36 constituencies, 799 municipality rows and 1,974
  precincts report complete counters; their second-vote sums each reconcile to
  the statewide 1,015,323 valid second votes. The previous 75-capture video is
  retained in `animation-60845e75`.
- MV's raw precinct votes reconcile to the Land totals and were independently
  cross-checked against the normalized precinct party export.
- Full mode shares sum to 100% and reported percentage-point differences match
  the source shares. Berlin's series order is verified against the official
  chart legend, and both complete published series include `Sonstige`.
- Replaying both frozen mode sources produces identical CSVs.
- Both comparison PNGs and sampled first/final timelapse frames were inspected.
- The shared renderer's original six-party LSA frame is pixel-identical before
  and after the code change when rendered in the same runtime. Old archived PNG
  bytes can differ across runtime/font versions.
