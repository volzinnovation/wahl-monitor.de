#!/usr/bin/env python3
"""Render exact archived Land second-vote observations as MPEG-2 and MP4 movies."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import platform
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

os.environ.setdefault("MPLCONFIGDIR", "/tmp/lsa-report-matplotlib")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data/2026-lsa/reports/git-timeline/23377a96"
TITLE = "Auszählung Landtagswahl Sachsen-Anhalt"
BERLIN = ZoneInfo("Europe/Berlin")
COLORS = {"AfD": "#3478a5", "BSW": "#b28b25", "CDU": "#222a35",
          "Die Linke": "#b6658b", "GRÜNE": "#79883a", "SPD": "#cc783c"}
INK, MUTED = "#222a35", "#66717e"
BASENAME = "auszaehlung-landtagswahl-sachsen-anhalt"


def num(value, digits=0):
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_observations(folder):
    summary = json.loads((folder / "summary.json").read_text())
    with (folder / "versions.csv").open(newline="", encoding="utf-8") as stream:
        versions = list(csv.DictReader(stream))
    land = {}
    with gzip.open(folder / "raw_timeline.jsonl.gz", "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row["level"] == "LAND" and row["mode"] == "TOTAL":
                require(row["commit"] not in land, "Duplicate Land TOTAL row")
                land[row["commit"]] = row
    manifests = json.loads((folder / "source_manifest.json").read_text())
    sources = {x["commit"]: x for x in manifests
               if x["url"].endswith("Ergebnisse_Land_RKR_WKR_LT_2026.csv")}
    selected = sorted(summary["selected_parties"], key=lambda x: x["party"].casefold())
    require(len(selected) == 6, "This chart contract requires six final parties above 5%")
    require(all(x["party"] in COLORS for x in selected), "Missing party color")
    observations = []
    for version in versions:
        if int(version["valid_votes_zweit"]) == 0:
            continue  # Zero templates have no defined percentage.
        commit = version["commit"]
        row, source = land[commit], sources[commit]
        total = row["valid_votes_zweit"]
        require(total == int(version["valid_votes_zweit"]), "Land/normalized denominator differs")
        all_votes = {code: value for code, value in row["parties"].items() if code.startswith("F")}
        require(all(isinstance(v, int) and v >= 0 for v in all_votes.values()), "Missing/invalid second votes")
        require(sum(all_votes.values()) == total, "All-party sum differs from valid second votes")
        require(source["verified"] and source["retained"], "Unverified raw source")
        local = datetime.fromisoformat(version["acquired_at_utc"]).astimezone(BERLIN)
        require(local.isoformat() == version["acquired_at_local"], "Timestamp conversion differs")
        record = {
            "capture_index": len(observations) + 1, "commit": commit,
            "acquired_at_utc": version["acquired_at_utc"], "acquired_at_local": local.isoformat(),
            "source_time_local": row["source_time_local"],
            "source_fetched_at_utc": source["fetched_at_utc"],
            "source_sha256": source["content_hash"],
            "source_git_url": f"https://github.com/volzinnovation/wahl-monitor.de/blob/{commit}/data/2026-lsa/latest/official_sources/{source['filename']}",
            "valid_votes_zweit": total, "reported_precincts": row["reported_precincts"],
            "total_precincts": row["total_precincts"], "all_party_votes": all_votes,
        }
        for party in selected:
            name, code = party["party"], party["code"]
            record[f"{name}_votes"] = all_votes[code]
            record[f"{name}_percent"] = 100 * all_votes[code] / total
            require(0 <= record[f"{name}_percent"] <= 60, "Bar exceeds requested 0–60% scale")
        observations.append(record)
    require(len(observations) > 1, "Need at least two positive-vote captures")
    last = observations[-1]
    above_five = {k for k, v in last["all_party_votes"].items() if v / last["valid_votes_zweit"] > .05}
    require(above_five == {x["code"] for x in selected}, "Final >5% cohort differs")
    for party in selected:
        require(last[f"{party['party']}_votes"] == party["votes"], "Final report vote differs")
    return summary, selected, observations


def assign_frames(rows, seconds, fps):
    """Compress elapsed acquisition time; retain exact states with no interpolation."""
    dates = [datetime.fromisoformat(x["acquired_at_utc"]) for x in rows]
    require(all(a < b for a, b in zip(dates, dates[1:])), "Capture times must increase")
    span = (dates[-1] - dates[0]).total_seconds()
    timeline_frames = round(seconds * fps)
    ticks = [round((date - dates[0]).total_seconds() / span * timeline_frames) for date in dates]
    require(all(a < b for a, b in zip(ticks, ticks[1:])),
            "Duration too short to include every capture; increase --timeline-seconds")
    start = 0
    for i, row in enumerate(rows):
        count = ticks[i + 1] - ticks[i] if i < len(rows) - 1 else 3 * fps
        if i == 0:
            count += 2 * fps
        row.update({"frame_start": start, "frame_count": count,
                    "movie_start_seconds": start / fps, "movie_duration_seconds": count / fps,
                    "image": f"frames/capture-{i + 1:03d}.png"})
        start += count
    require(start == timeline_frames + 5 * fps, "Movie duration mismatch")
    return span, start


def render_frames(output, selected, rows, ref):
    (output / "frames").mkdir(exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 15,
                         "text.color": INK, "axes.labelcolor": INK,
                         "xtick.color": INK, "ytick.color": MUTED,
                         "figure.facecolor": "white", "axes.facecolor": "white"})
    fig = plt.figure(figsize=(16, 9), dpi=120)
    ax = fig.add_axes([.08, .185, .88, .605])
    fig.text(.08, .932, TITLE, fontsize=27, weight="bold", ha="left", va="top")
    subtitle = fig.text(.08, .867, "", fontsize=19, ha="left", va="top")
    footer = fig.text(.08, .073, "", fontsize=11.5, color=MUTED)
    status = fig.text(.96, .073, "", fontsize=11.5, color=MUTED, ha="right")
    fig.text(.08, .035, f"wahl-monitor.de · Git-Archiv {ref[:8]} · Zeitraffer · Auswahl: sechs Parteien über 5 % beim letzten Abruf",
             fontsize=11, color=MUTED)
    names = [p["party"] for p in selected]
    bars = ax.bar(names, [0] * 6, width=.65, color=[COLORS[n] for n in names],
                  edgecolor=[COLORS[n] for n in names], linewidth=.7, zorder=3)
    labels = [ax.text(i, 0, "", ha="center", va="top", color="white", fontsize=20,
                      weight="bold", zorder=4) for i in range(6)]
    ax.set_ylim(0, 60)
    ax.set_yticks(range(0, 61, 10))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, pos: f"{int(value)} %"))
    ax.set_ylabel("Anteil der gültigen Zweitstimmen", fontsize=14, labelpad=16)
    ax.tick_params(axis="x", labelsize=19, length=0, pad=15)
    ax.tick_params(axis="y", length=0, pad=12)
    ax.grid(axis="y", color="#e5e8ec", linewidth=.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#acb2b9")
        ax.spines[side].set_linewidth(.8)
    for i, row in enumerate(rows):
        date = datetime.fromisoformat(row["acquired_at_local"])
        subtitle.set_text(f"{num(row['valid_votes_zweit'])} gültige Zweitstimmen @ {date:%d.%m.%Y, %H:%M:%S} MESZ · Abruf")
        footer.set_text(f"Amtliche Landes-CSV · Datenstand: {row['source_time_local']} MESZ")
        status.set_text(f"{num(row['reported_precincts'])}/{num(row['total_precincts'])} Wahlbezirke · Abruf {i + 1}/{len(rows)}")
        for name, bar, label in zip(names, bars, labels):
            share = row[f"{name}_percent"]
            bar.set_height(share)
            label.set_text(num(share, 2) + " %")
            # Large bars: inside labels keep the 59.88% first observation within the axis.
            inside = share >= 8
            label.set_y(share - 1.8 if inside else share + 1.1)
            label.set_va("top" if inside else "bottom")
            label.set_color("white" if inside else INK)
        fig.savefig(output / row["image"], dpi=120, metadata={"Software": "LSA archived-results animation"})
        if (i + 1) % 25 == 0 or i + 1 == len(rows):
            print(f"Rendered {i + 1}/{len(rows)} captures", flush=True)
    plt.close(fig)


def encode(output, rows, fps, total_frames):
    concat = ["ffconcat version 1.0"]
    for row in rows:
        concat += [f"file '{row['image']}'", f"duration {row['frame_count'] / fps:.8f}"]
    concat.append(f"file '{rows[-1]['image']}'")
    (output / "frames.ffconcat").write_text("\n".join(concat) + "\n")
    encodings = {
        "mpg": ["-c:v", "mpeg2video", "-q:v", "2", "-maxrate", "12M", "-bufsize", "4M", "-g", "15", "-bf", "2", "-f", "mpeg"],
        "mp4": ["-c:v", "libx264", "-crf", "18", "-preset", "medium", "-tune", "stillimage", "-g", "50", "-movflags", "+faststart"],
    }
    validation = {}
    for extension, options in encodings.items():
        filename = f"{BASENAME}.{extension}"
        command = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0",
                   "-i", "frames.ffconcat", "-an", "-vf", f"fps={fps}", "-frames:v", str(total_frames),
                   "-threads", "2", "-pix_fmt", "yuv420p", *options, filename]
        print(f"Encoding {filename}", flush=True)
        subprocess.run(command, cwd=output, check=True)
        probe = json.loads(subprocess.check_output([
            "ffprobe", "-v", "error", "-select_streams", "v:0", "-count_frames",
            "-show_entries", "stream=codec_name,width,height,r_frame_rate,nb_read_frames:format=duration,format_name",
            "-of", "json", filename], cwd=output))
        stream = probe["streams"][0]
        require(int(stream["nb_read_frames"]) == total_frames, "Encoded frame count differs")
        require((stream["width"], stream["height"]) == (1920, 1080), "Wrong output resolution")
        require(stream["r_frame_rate"] == f"{fps}/1", "Wrong output frame rate")
        require(stream["codec_name"] == ("mpeg2video" if extension == "mpg" else "h264"), "Wrong output codec")
        subprocess.run(["ffmpeg", "-v", "error", "-i", filename, "-f", "null", "-"], cwd=output, check=True)
        validation[filename] = {"probe": probe, "full_decode": "passed", "command": command}
    return validation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeline-seconds", type=float, default=75)
    parser.add_argument("--fps", type=int, default=25, choices=[25, 30])
    args = parser.parse_args()
    require(args.timeline_seconds > 0, "Timeline must have positive duration")
    require(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg and ffprobe are required")
    source = args.input.resolve()
    summary, selected, rows = read_observations(source)
    output = (args.output or source.parent / f"animation-{summary['ref'][:8]}").resolve()
    require(output != source, "Use a separate animation folder to preserve the frozen audit")
    output.mkdir(parents=True, exist_ok=True)
    span, total_frames = assign_frames(rows, args.timeline_seconds, args.fps)
    write_json(output / "snapshots.json", rows)
    flat = [{k: v for k, v in row.items() if k != "all_party_votes"} for row in rows]
    with (output / "snapshots.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(flat[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(flat)
    render_frames(output, selected, rows, summary["ref"])
    validation = encode(output, rows, args.fps, total_frames)
    script = Path(__file__).resolve()
    shutil.copyfile(script, output / script.name)
    manifest = {
        "title": TITLE, "source_ref": summary["ref"], "captures": len(rows),
        "first_capture_local": rows[0]["acquired_at_local"], "last_capture_local": rows[-1]["acquired_at_local"],
        "parties_alphabetical": [x["party"] for x in selected], "selection": "strictly above 5% in final Land TOTAL row",
        "denominator": "all valid second votes in official Land TOTAL row, including parties not displayed",
        "y_axis_percent": [0, 60], "archive_duration_seconds": span,
        "timeline_seconds": round(args.timeline_seconds * args.fps) / args.fps,
        "initial_hold_seconds": 2, "final_hold_seconds": 3,
        "movie_duration_seconds": total_frames / args.fps, "frames": total_frames, "fps": args.fps,
        "timing": "Acquisition intervals proportional to elapsed time, rounded to nearest frame; exact step observations, no interpolation",
        "checks": {"all_captures_present": True, "all_party_sums_equal_denominator": True,
                   "party_cohort_matches_final_above_five": True, "timestamps_strictly_increasing": True,
                   "all_bars_within_fixed_axis": True, "every_capture_has_at_least_one_frame": True},
        "input_sha256": {name: digest(source / name) for name in
                         ("summary.json", "versions.csv", "raw_timeline.jsonl.gz", "source_manifest.json")},
        "script_sha256": digest(script),
        "environment": {"python": platform.python_version(), "matplotlib": matplotlib.__version__,
                        "ffmpeg": subprocess.check_output(["ffmpeg", "-version"], text=True).splitlines()[0]},
        "videos": validation,
    }
    write_json(output / "manifest.json", manifest)
    relative_input = os.path.relpath(source, ROOT)
    report_link = os.path.relpath(source, output)
    last = rows[-1]
    aggregation_note = ""
    if summary["ref"] == "23377a96d918a4b77f71dbabbf9c385f78334126":
        aggregation_note = f"""Municipality totals contain **1,315,315** valid second
  votes. The documented **1,513-vote aggregation gap** is explained in the
  [full-history audit]({report_link}/AGGREGATION_ASCHERSLEBEN.md)."""
    readme = f"""# Auszählung Landtagswahl Sachsen-Anhalt — Animation

[MPEG-2-Film (.mpg)]({BASENAME}.mpg) · [MP4-Vorschau]({BASENAME}.mp4)

{len(rows)} archivierte Abrufe, vom **06.09.2026, 18:39:34 MESZ** bis
**07.09.2026, 03:10:10 MESZ**. 1920 × 1080 Pixel, {args.fps} Bilder/s,
{total_frames / args.fps:g} Sekunden, ohne Ton. Titel, alphabetische Reihenfolge
und Skala von 0 bis 60 % bleiben in jedem Bild gleich.

## Data and timing

- Six parties strictly above 5% in the last archived official Land TOTAL row:
  {', '.join(x['party'] for x in selected)}. The cohort stays fixed throughout.
- Each share uses all valid second votes as its denominator, including votes for
  parties outside this selection. Subtitle: valid second-vote total and capture
  time in Europe/Berlin (MESZ, UTC+02:00). The CSV's own result timestamp is
  displayed separately in the footer; capture time is not the counting time.
- All {len(rows)} positive-vote captures are included in archive order, including
  unchanged observations and corrections. Earlier zero-vote templates are
  excluded because their percentages are undefined.
- {span:g} seconds of archive time are compressed proportionally into
  {manifest['timeline_seconds']:g} seconds, plus a 2-second opening hold and a
  3-second final hold. Boundaries are rounded to the nearest video frame.
  Values change only at actual captures; no intermediate vote values are invented.
- The last Land CSV contains **{last['valid_votes_zweit']:,}** valid second votes and reports
  **{last['reported_precincts']:,}/{last['total_precincts']:,}** districts. {aggregation_note} This movie follows
  the official Land series consistently and does not represent a certified final
  election result or replace the last Land observation with municipality sums.
- Immutable archive endpoint: `{summary['ref']}`. Every capture in
  [snapshots.csv](snapshots.csv) links to its pinned original CSV and records its
  hash, fetch time, source timestamp, votes, percentages and exact video frames.
  [snapshots.json](snapshots.json) also retains votes for all second-vote parties.

## Reproduce

From the repository root, with Python 3.10+, Matplotlib, ffmpeg and ffprobe:

```sh
python3 scripts/render_lsa_results_animation.py --input {relative_input} --timeline-seconds {args.timeline_seconds:g} --fps {args.fps}
```

Use `--output /tmp/lsa-animation-reproduction` to keep the delivered files intact.
The input audit can be rebuilt from Git using its [methods]({report_link}/METHODS.md).
The generator is also included here as `render_lsa_results_animation.py`; when
using that copy, pass explicit `--input` and `--output` paths.

`manifest.json` pins input hashes, generator hash, runtime versions, timing and
codec checks. `frames.ffconcat` and the 114 PNGs retain the exact rendered inputs.
The script verifies every denominator, the final >5% cohort, increasing capture
times, bar bounds and encoded frame counts, then decodes both whole videos.
Reproduction preserves analytical values and frame timing. Byte-identical video
output also depends on the recorded font, Matplotlib and ffmpeg versions.
This command reads the frozen audit only; polling remains stopped.
"""
    # Derive user-facing endpoint labels from the selected archive, including reruns.
    for old, row in [("06.09.2026, 18:39:34", rows[0]), ("07.09.2026, 03:10:10", rows[-1])]:
        readme = readme.replace(old, datetime.fromisoformat(row["acquired_at_local"]).strftime("%d.%m.%Y, %H:%M:%S"))
    readme = readme.replace("the 114 PNGs", f"the {len(rows)} PNGs")
    (output / "README.md").write_text(readme, encoding="utf-8")
    checksums = [f"{digest(path)}  {path.relative_to(output).as_posix()}"
                 for path in sorted(output.rglob("*")) if path.is_file() and path.name != "SHA256SUMS"]
    (output / "SHA256SUMS").write_text("\n".join(checksums) + "\n")
    print(json.dumps({"output": str(output), "captures": len(rows), "frames": total_frames,
                      "seconds": total_frames / args.fps}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
