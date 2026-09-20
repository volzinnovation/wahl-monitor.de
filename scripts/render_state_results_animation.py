#!/usr/bin/env python3
"""Render MV/Berlin archived statewide second votes using the LSA movie layout."""
from __future__ import annotations
import argparse
import csv
import io
import json
import platform
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from render_lsa_results_animation import (
    COLORS, assign_frames, digest, encode, render_frames, require, write_json,
)

ROOT = Path(__file__).resolve().parents[1]
STATES = {
    '2026-mv': ('Landtagswahl Mecklenburg-Vorpommern', 'landtagswahl-mecklenburg-vorpommern'),
    '2026-be': ('Abgeordnetenhauswahl Berlin', 'abgeordnetenhauswahl-berlin'),
}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def read_history(key, ref):
    base = f'data/{key}/latest'
    commits = git('rev-list', '--reverse', '--first-parent', ref, '--', f'{base}/run_metadata.json').decode().splitlines()
    observations, excluded, seen = [], [], set()
    for commit in commits:
        meta_bytes = git('show', f'{commit}:{base}/run_metadata.json')
        meta = json.loads(meta_bytes)
        if meta.get('statla_mode') != 'LIVE_CSV_DOWNLOAD' or meta.get('statla_error'):
            excluded.append({'commit': commit, 'reason': 'not a successful live CSV capture'})
            continue
        timestamp = meta['generated_at_utc']
        if timestamp in seen:
            excluded.append({'commit': commit, 'reason': 'same capture already retained'})
            continue
        snapshots = git('show', f'{commit}:{base}/statla_snapshots.csv')
        parties = git('show', f'{commit}:{base}/statla_party_results.csv')
        rows = list(csv.DictReader(io.StringIO(snapshots.decode())))
        lands = [r for r in rows if r['gebietsart'] == 'LAND']
        require(len(lands) == 1, f'{commit}: expected one Land row')
        land = lands[0]
        total = int(land['valid_votes_zweit'] or 0)
        if total == 0:
            excluded.append({'commit': commit, 'reason': 'zero valid second votes'})
            continue
        party_rows = [r for r in csv.DictReader(io.StringIO(parties.decode()))
                      if r['row_key'] == land['row_key'] and r['vote_type'] == 'Zweitstimmen']
        votes = {r['party_name']: int(r['votes']) for r in party_rows}
        require(len(votes) == len(party_rows), f'{commit}: duplicate party')
        require(all(v >= 0 for v in votes.values()) and sum(votes.values()) == total,
                f'{commit}: party sum differs from valid second votes')
        seen.add(timestamp)
        import hashlib
        observations.append({
            'capture_index': len(observations) + 1, 'commit': commit,
            'acquired_at_utc': timestamp,
            'acquired_at_local': datetime.fromisoformat(timestamp).astimezone(ZoneInfo('Europe/Berlin')).isoformat(),
            'valid_votes_zweit': total,
            'reported_precincts': int(land['reported_precincts']) if land['reported_precincts'] else None,
            'total_precincts': int(land['total_precincts']) if land['total_precincts'] else None,
            'all_party_votes': votes, 'source_url': meta['statla_url'],
            'source_git_url': f'https://github.com/volzinnovation/wahl-monitor.de/tree/{commit}/{base}',
            'input_sha256': {name: hashlib.sha256(data).hexdigest() for name, data in
                             [('run_metadata.json', meta_bytes), ('statla_snapshots.csv', snapshots),
                              ('statla_party_results.csv', parties)]},
        })
    require(len(observations) > 1, 'Need at least two positive captures')
    selected = sorted([{'party': name, 'votes': votes} for name, votes in observations[-1]['all_party_votes'].items()
                       if votes / observations[-1]['valid_votes_zweit'] > .05], key=lambda p: p['party'].casefold())
    require(all(p['party'] in COLORS for p in selected), 'Missing party color')
    for row in observations:
        for p in selected:
            name = p['party']
            require(name in row['all_party_votes'], f'Missing party {name}')
            row[f'{name}_votes'] = row['all_party_votes'][name]
            row[f'{name}_percent'] = 100 * row[f'{name}_votes'] / row['valid_votes_zweit']
            require(0 <= row[f'{name}_percent'] <= 60, 'Bar exceeds fixed 0–60% axis')
    return selected, observations, excluded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--election', choices=STATES, required=True)
    parser.add_argument('--ref', default='HEAD')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--timeline-seconds', type=float, default=75)
    parser.add_argument('--fps', type=int, choices=[25, 30], default=25)
    args = parser.parse_args()
    require(args.timeline_seconds > 0, 'Positive timeline duration required')
    ref = git('rev-parse', args.ref + '^{commit}').decode().strip()
    title, slug = STATES[args.election]
    selected, rows, excluded = read_history(args.election, ref)
    output = (args.output or ROOT / f'data/{args.election}/reports/git-timeline/animation-{ref[:8]}').resolve()
    output.mkdir(parents=True, exist_ok=True)
    span, frames = assign_frames(rows, args.timeline_seconds, args.fps)
    write_json(output / 'snapshots.json', rows)
    flat = [{k: v for k, v in row.items() if k not in {'all_party_votes', 'input_sha256'}} for row in rows]
    with (output / 'snapshots.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(flat[0]))
        writer.writeheader(); writer.writerows(flat)
    render_frames(output, selected, rows, title='Auszählung ' + title,
                  selection_note='wahl-monitor.de · Zeitraffer · Auswahl: Parteien über 5 % beim letzten archivierten Abruf')
    videos = encode(output, rows, args.fps, frames, basename='auszaehlung-' + slug)
    last = rows[-1]
    counting_complete = bool(last['total_precincts'] and last['reported_precincts'] == last['total_precincts'])
    result_status = ('Vollständig ausgezählt; kein festgestelltes amtliches Endergebnis.' if counting_complete
                     else 'Zwischenergebnis, kein amtliches Endergebnis.')
    manifest = {
        'election_key': args.election, 'source_ref': ref, 'captures': len(rows), 'excluded': excluded,
        'first_capture_local': rows[0]['acquired_at_local'], 'last_capture_local': rows[-1]['acquired_at_local'],
        'counting_complete': counting_complete, 'result_status': result_status,
        'selected_parties': selected, 'selection': 'strictly above 5% in last archived Land observation',
        'denominator': 'all valid second votes in each normalized official Land row',
        'archive_duration_seconds': span, 'movie_duration_seconds': frames / args.fps,
        'fps': args.fps, 'frames': frames, 'axis_percent': [0, 60],
        'timing': 'capture completion times; elapsed-time proportional steps without interpolation; 2s opening/3s closing holds',
        'checks': {'every_party_sum_reconciled': True, 'every_capture_present': True, 'fixed_axis_bounds': True},
        'generator_sha256': {p.name: digest(p) for p in [Path(__file__), ROOT / 'scripts/render_lsa_results_animation.py']},
        'python': platform.python_version(), 'videos': videos,
    }
    write_json(output / 'manifest.json', manifest)
    (output / 'README.md').write_text(f'''# Auszählung {title} 2026 — Zeitraffer

[MP4](auszaehlung-{slug}.mp4) · [MPEG-2](auszaehlung-{slug}.mpg) · [Daten](snapshots.csv)

{len(rows)} archivierte Abrufe vom **{rows[0]['acquired_at_local']}** bis **{last['acquired_at_local']}**.
1920 × 1080, {args.fps} Bilder/s, {frames / args.fps:g} Sekunden, ohne Ton.
Letzter Stand: **{last['valid_votes_zweit']:,} gültige Zweitstimmen**;
**{last['reported_precincts']} von {last['total_precincts']} Wahlbezirken**. {result_status}

## Methode und Quellen

- Gleiches Layout wie Sachsen-Anhalt: alphabetische Reihenfolge, feste 0–60%-Skala.
- Feste Auswahl: {', '.join(p['party'] for p in selected)}; mehr als 5 % im letzten archivierten Landesstand.
- Nenner: alle gültigen Zweitstimmen im jeweiligen Landesstand, einschließlich nicht gezeigter Parteien.
- Zeitstempel: Abschluss des gespeicherten Abrufs, nicht Zeitpunkt der Stimmabgabe oder Auszählung.
- {span:.1f} Sekunden Archivzeit proportional auf {frames / args.fps - 5:g} Sekunden komprimiert,
  plus 2 Sekunden am Anfang und 3 am Ende. Echte, unveränderte Beobachtungen ohne Interpolation;
  unveränderte Stände und Korrekturen bleiben enthalten. Nullstände und Vorlagen sind ausgeschlossen.
- Quelle: versionierte normalisierte amtliche Landesdaten, Git-Endpunkt `{ref}`.
  Jede Beobachtung enthält Git-Verweis, Originalquellen-URLs und SHA-256 der drei Eingabedateien.
  Dies ist eine Auswertung des verfügbaren Git-Archivs, keine Behauptung lückenloser amtlicher Meldungen.
- `snapshots.json` enthält sämtliche Parteistimmen. `manifest.json` dokumentiert Ausschlüsse und Prüfungen.
  Alle Parteisummen wurden mit dem Nenner abgeglichen. Beide Filme wurden vollständig dekodiert;
  Bildzahl, Auflösung und Codec wurden mit ffprobe geprüft.

## Reproduce

```sh
python3 scripts/render_state_results_animation.py --election {args.election} --ref {ref} --timeline-seconds {args.timeline_seconds:g} --fps {args.fps} --output /tmp/{args.election}-animation
```

Requires Python, Matplotlib, ffmpeg and ffprobe. No fetch, poll, or deployment is performed.
''')
    (output / 'SHA256SUMS').write_text(''.join(f'{digest(p)}  {p.relative_to(output)}\n' for p in sorted(output.rglob('*')) if p.is_file() and p.name != 'SHA256SUMS'))
    print(json.dumps({'output': str(output), 'captures': len(rows), 'selected': selected}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
