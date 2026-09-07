# Auszählung Landtagswahl Sachsen-Anhalt — Animation

[MPEG-2-Film (.mpg)](auszaehlung-landtagswahl-sachsen-anhalt.mpg) · [MP4-Vorschau](auszaehlung-landtagswahl-sachsen-anhalt.mp4)

114 archivierte Abrufe, vom **06.09.2026, 18:39:34 MESZ** bis
**07.09.2026, 03:10:10 MESZ**. 1920 × 1080 Pixel, 25 Bilder/s,
80 Sekunden, ohne Ton. Titel, alphabetische Reihenfolge
und Skala von 0 bis 60 % bleiben in jedem Bild gleich.

## Data and timing

- Six parties strictly above 5% in the last archived official Land TOTAL row:
  AfD, BSW, CDU, Die Linke, GRÜNE, SPD. The cohort stays fixed throughout.
- Each share uses all valid second votes as its denominator, including votes for
  parties outside this selection. Subtitle: valid second-vote total and capture
  time in Europe/Berlin (MESZ, UTC+02:00). The CSV's own result timestamp is
  displayed separately in the footer; capture time is not the counting time.
- All 114 positive-vote captures are included in archive order, including
  unchanged observations and corrections. Earlier zero-vote templates are
  excluded because their percentages are undefined.
- 30636.6 seconds of archive time are compressed proportionally into
  75 seconds, plus a 2-second opening hold and a
  3-second final hold. Boundaries are rounded to the nearest video frame.
  Values change only at actual captures; no intermediate vote values are invented.
- The last Land CSV contains **1,313,802** valid second votes and reports
  **2,660/2,661** districts. Municipality totals contain **1,315,315** valid second
  votes. The documented **1,513-vote aggregation gap** is explained in the
  [full-history audit](../23377a96/AGGREGATION_ASCHERSLEBEN.md). This movie follows
  the official Land series consistently and does not represent a certified final
  election result or replace the last Land observation with municipality sums.
- Immutable archive endpoint: `23377a96d918a4b77f71dbabbf9c385f78334126`. Every capture in
  [snapshots.csv](snapshots.csv) links to its pinned original CSV and records its
  hash, fetch time, source timestamp, votes, percentages and exact video frames.
  [snapshots.json](snapshots.json) also retains votes for all second-vote parties.

## Reproduce

From the repository root, with Python 3.10+, Matplotlib, ffmpeg and ffprobe:

```sh
python3 scripts/render_lsa_results_animation.py --input data/2026-lsa/reports/git-timeline/23377a96 --timeline-seconds 75 --fps 25
```

Use `--output /tmp/lsa-animation-reproduction` to keep the delivered files intact.
The input audit can be rebuilt from Git using its [methods](../23377a96/METHODS.md).
The generator is also included here as `render_lsa_results_animation.py`; when
using that copy, pass explicit `--input` and `--output` paths.

`manifest.json` pins input hashes, generator hash, runtime versions, timing and
codec checks. `frames.ffconcat` and the 114 PNGs retain the exact rendered inputs.
The script verifies every denominator, the final >5% cohort, increasing capture
times, bar bounds and encoded frame counts, then decodes both whole videos.
Reproduction preserves analytical values and frame timing. Byte-identical video
output also depends on the recorded font, Matplotlib and ffmpeg versions.
This command reads the frozen audit only; polling remains stopped.
