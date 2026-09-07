# Auszählung Landtagswahl Sachsen-Anhalt — Animation

[MPEG-2-Film (.mpg)](auszaehlung-landtagswahl-sachsen-anhalt.mpg) · [MP4-Vorschau](auszaehlung-landtagswahl-sachsen-anhalt.mp4)

115 gespeicherte Abrufe, vom **06.09.2026, 18:39:34 MESZ** bis
**07.09.2026, 04:03:35 MESZ**. 1920 × 1080 Pixel, 25 Bilder/s,
80 Sekunden, ohne Ton. Titel, alphabetische Reihenfolge
und Skala von 0 bis 60 % bleiben in jedem Bild gleich. Die Quellen- und
Statuszeile unter der Grafik wurde vollständig aus allen Bildern entfernt.

## Data and timing

- Six parties strictly above 5% in the last archived official Land TOTAL row:
  AfD, BSW, CDU, Die Linke, GRÜNE, SPD. The cohort stays fixed throughout.
- Each share uses all valid second votes as its denominator, including votes for
  parties outside this selection. Subtitle: valid second-vote total and capture
  time in Europe/Berlin (MESZ, UTC+02:00). Capture time is not the counting time.
  Source timestamps and reporting counts are retained in the data only.
- All 115 positive-vote captures are included in archive order, including
  unchanged observations and corrections. Earlier zero-vote templates are
  excluded because their percentages are undefined.
- 33841.1 seconds of archive time are compressed proportionally into
  75 seconds, plus a 2-second opening hold and a
  3-second final hold. Boundaries are rounded to the nearest video frame.
  Values change only at actual captures; no intermediate vote values are invented.
- The last Land CSV contains **1,315,315** valid second votes.
  The last Land CSV omits its reporting-count fields; these remain missing in the data.  This movie follows
  the official Land series consistently and does not represent a certified final
  election result or replace the last Land observation with municipality sums.
- Immutable Git endpoint: `23377a96d918a4b77f71dbabbf9c385f78334126` (114 positive-vote captures).
  1 additional local acquisition(s) follow the Git history. Their exact downloaded files and manifests are retained in `additional_sources/`; they have no fabricated Git commit or GitHub URL. Each capture in
  [snapshots.csv](snapshots.csv) identifies its original Git CSV or local source file and records its
  hash, fetch time, source timestamp, votes, percentages and exact video frames.
  [snapshots.json](snapshots.json) also retains votes for all second-vote parties.

## Reproduce

From the repository root, with Python 3.10+, Matplotlib, ffmpeg and ffprobe:

```sh
python3 scripts/render_lsa_results_animation.py --input data/2026-lsa/reports/git-timeline/23377a96 --extra-capture data/2026-lsa/reports/source-sync-checks/20260907T020328.446381Z --timeline-seconds 75 --fps 25
```

Use `--output /tmp/lsa-animation-reproduction` to keep the delivered files intact.
The input audit can be rebuilt from Git using its [methods](../23377a96/METHODS.md).
The generator is also included here as `render_lsa_results_animation.py`; when
using that copy, pass explicit `--input` and `--output` paths.

`manifest.json` pins input hashes, generator hash, runtime versions, timing and
codec checks. `frames.ffconcat` and the 115 PNGs retain the exact rendered inputs.
The script verifies every denominator, the final >5% cohort, increasing capture
times, bar bounds and encoded frame counts, then decodes both whole videos.
Reproduction preserves analytical values and frame timing. Byte-identical video
output also depends on the recorded font, Matplotlib and ffmpeg versions.
This command reads the frozen audit and any explicitly supplied saved captures;
polling remains stopped.
