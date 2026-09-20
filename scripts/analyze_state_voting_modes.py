#!/usr/bin/env python3
"""Reproduce the Wahlkreis-14 Briefwahl/Urne chart for MV and Berlin statewide."""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from poll_election_core import mv_csv_rows_from_text

ROOT = Path(__file__).resolve().parents[1]
BERLIN_URL = 'https://www.wahlen-berlin.de/wahlen/Be2026/AFSPRAES/agh/index.html'
NAMES = {'2026-mv': 'Mecklenburg-Vorpommern', '2026-be': 'Berlin'}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def num(value, digits=0):
    return f'{value:,.{digits}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def mv_inputs(source):
    raw = mv_csv_rows_from_text((source / 'official-results-source.csv').read_text(encoding='utf-8-sig'))
    booths = [r for r in raw if r['Ausgabe'] == 'A' and r['Erst-/Zweitstimme'] == '2']
    identities = {(r['Gemeinde'], r['Wahlkreis'], r['Wahlbezirk']) for r in booths}
    require(len(identities) == len(booths), 'Duplicate precinct rows')
    snapshots = list(csv.DictReader((source / 'statla_snapshots.csv').open()))
    land, = [r for r in snapshots if r['gebietsart'] == 'LAND']
    normalized = list(csv.DictReader((source / 'statla_party_results.csv').open()))
    land_votes = {r['party_name']: int(r['votes']) for r in normalized
                  if r['row_key'] == land['row_key'] and r['vote_type'] == 'Zweitstimmen'}
    groups = {mode: {'valid': 0, 'precincts': 0, 'positive_precincts': 0,
                     'votes': {p: 0 for p in land_votes}} for mode in ['briefwahl', 'urnenwahl']}
    detail = []
    for row in booths:
        mode = 'briefwahl' if 'brief' in row['Wahlbezirksname'].lower() else 'urnenwahl'
        require((int(row['Wahlberechtigte']) == 0) == (mode == 'briefwahl'), 'Ambiguous mode classification')
        valid = int(row['Gültige Stimmen'])
        votes = {p: int(row[p]) for p in land_votes}
        require(all(v >= 0 for v in votes.values()) and sum(votes.values()) == valid, 'Precinct party sum mismatch')
        group = groups[mode]
        group['valid'] += valid
        group['precincts'] += 1
        group['positive_precincts'] += int(valid > 0)
        for p, value in votes.items():
            group['votes'][p] += value
        detail.append({'gemeinde': row['Gemeinde'], 'wahlkreis': row['Wahlkreis'],
                       'wahlbezirk': row['Wahlbezirk'], 'name': row['Wahlbezirksname'],
                       'mode': mode, 'valid_second_votes': valid, **votes})
    require(sum(g['valid'] for g in groups.values()) == int(land['valid_votes_zweit']), 'Modes do not reconcile to Land')
    require(all(sum(g['votes'][p] for g in groups.values()) == n for p, n in land_votes.items()), 'Party modes do not reconcile to Land')
    rows = []
    for p in land_votes:
        r = {'party': p}
        for mode, g in groups.items():
            require(g['valid'] > 0, 'Mode has no valid votes')
            r.update({mode + '_votes': g['votes'][p], mode + '_share_percent': 100 * g['votes'][p] / g['valid'],
                      mode + '_valid_second_votes': g['valid'], mode + '_wahlbezirke': g['precincts']})
        r['total_votes'] = land_votes[p]
        rows.append(r)
    times = sorted({r['Berechnungsdatum'] for r in booths})
    require(len(times) == 1, 'Mixed precinct source times')
    context = {'source_time': times[0], 'reported': int(land['reported_precincts']),
               'expected': int(land['total_precincts']), 'valid_second_votes': int(land['valid_votes_zweit']),
               'groups': groups, 'all_party_sum_reconciled': True,
               'mode_party_totals_reconciled_to_land': True,
               'source_url': 'https://wahlen.mvnet.de/dateien/ergebnisse.2026/landtagswahl/csv/l_wahlbezirke.csv'}
    write_csv(source.parent / 'precincts.csv', detail)
    return rows, context


def berlin_inputs(source):
    soup = BeautifulSoup((source / 'official-results.html').read_bytes(), 'html.parser')
    charts = []
    for node in soup.select('[data-chartoptions][data-chartdata]'):
        options = json.loads(node['data-chartoptions'])
        if options.get('texte', {}).get('title') == 'Vergleich Urne-/Briefwahl: Zweitstimmen':
            charts.append((options, json.loads(node['data-chartdata'])))
    require(len(charts) == 1, 'Expected one official statewide second-vote mode chart')
    options, data = charts[0]
    require('Berlin, Zwischenergebnis' in options['texte']['subTitle'], 'Wrong scope/status')
    legend = options['legende']
    require('Urnen' in legend[0]['label'] and 'Brief' in legend[1]['label'], 'Unexpected series order')
    urne, brief = data['dataSeries']
    urne = {r['label']: float(r['value']) for r in urne['dataSets']}
    brief = {r['label']: float(r['value']) for r in brief['dataSets']}
    require(urne.keys() == brief.keys(), 'Unmatched party labels')
    for shares in [urne, brief]:
        require(all(0 <= x <= 100 for x in shares.values()), 'Invalid percentage')
        require(abs(sum(shares.values()) - 100) < 1e-6, 'Official shares do not sum to 100')
    info = options['texte']['info']
    match = re.search(r'Ausgezählte Gebiete: ([\d.]+) von ([\d.]+).*?, (\d{2}\.\d{2}\.\d{4}, \d{2}:\d{2}:\d{2})', info)
    require(match is not None, 'Missing official coverage and timestamp')
    rows = [{'party': p, 'briefwahl_votes': None, 'briefwahl_share_percent': brief[p],
             'urnenwahl_votes': None, 'urnenwahl_share_percent': urne[p],
             'briefwahl_valid_second_votes': None, 'urnenwahl_valid_second_votes': None,
             'briefwahl_wahlbezirke': None, 'urnenwahl_wahlbezirke': None} for p in urne]
    context = {'source_time': match[3], 'reported': int(match[1].replace('.', '')),
               'expected': int(match[2].replace('.', '')), 'source_url': BERLIN_URL,
               'source_chart': options['texte'], 'official_percentage_sums_reconciled': True,
               'missing_fields': ['absolute party votes by mode', 'valid second votes by mode', 'precinct counts by mode'],
               'note': 'Official published percentage series, not inferred counts; Sonstige is a composite category.'}
    save_json(source.parent / 'official-chart.json', {'options': options, 'data': data})
    return rows, context


def write_csv(path, rows):
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)


def chart(output, key, rows, context):
    selected = [r for r in rows if r['selected_for_chart']]
    selected.sort(key=lambda r: -max(r['briefwahl_share_percent'], r['urnenwahl_share_percent']))
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 15})
    fig = plt.figure(figsize=(19.2, 10.8), dpi=140, facecolor='#f7f9fc')
    ax = fig.add_axes([.065, .18, .905, .64])
    fig.text(.045, .955, NAMES[key] + ' — landesweiter Vergleich', fontsize=29, weight='bold', color='#0f172a', va='top')
    fig.text(.065, .881, 'Briefwahl vs. Urnenwahl · ' + ('Landtagswahl 2026' if key == '2026-mv' else 'Abgeordnetenhauswahl 2026'),
             fontsize=20, weight='bold', color='#334155')
    ax.text(.5, .5, 'wahl-monitor.de', transform=ax.transAxes, fontsize=69, color='#e5e7eb',
            weight='bold', ha='center', va='center', zorder=1)
    for mode, offset, color, label in [('briefwahl', -.19, '#306f9f', 'Briefwahl'), ('urnenwahl', .19, '#d29a30', 'Urnenwahl')]:
        vals = [r[mode + '_share_percent'] for r in selected]
        bars = ax.bar([i + offset for i in range(len(selected))], vals, width=.38, color=color, edgecolor='#64748b', linewidth=.5, label=label, zorder=3)
        for bar, value in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width()/2, value + .75, num(value, 1) + ' %', ha='center', fontsize=15, color='#334155')
    ax.set_xticks(range(len(selected)), [r['party'] for r in selected])
    ax.set_ylim(0, 65); ax.set_yticks(range(0, 61, 10))
    ax.set_ylabel('Zweitstimmenanteil (%)', color='#334155', labelpad=15)
    ax.tick_params(colors='#475569'); ax.grid(axis='y', color='#dae2eb', zorder=0)
    ax.set_axisbelow(True); ax.legend(loc='upper right', frameon=False, ncol=2)
    for side in ['top', 'right']: ax.spines[side].set_visible(False)
    for side in ['bottom', 'left']: ax.spines[side].set_color('#94a3b8')
    fig.text(.045, .114, f"Zwischenergebnis · Stand {context['source_time']} MESZ · {num(context['reported'])} von {num(context['expected'])} Wahlbezirken",
             fontsize=13, color='#475569')
    fig.text(.045, .082, 'Parteien mit mehr als 5 % in mindestens einer Kategorie · Nenner: gültige Zweitstimmen der jeweiligen Wahlart', fontsize=12, color='#64748b')
    if key == '2026-mv':
        b, u = context['groups']['briefwahl'], context['groups']['urnenwahl']
        note = f"Briefwahl: {num(b['valid'])} gültige Stimmen · Urnenwahl: {num(u['valid'])} gültige Stimmen · Alle Parteisummen mit Landesstand abgeglichen"
    else:
        note = 'Amtliche Prozentwerte · Absolute Stimmen und Wahlbezirkszahlen je Wahlart in dieser Quelle nicht ausgewiesen'
    fig.text(.045, .048, note, fontsize=12, color='#64748b')
    fig.savefig(output / 'briefwahl-vs-urnenwahl.png', facecolor=fig.get_facecolor())
    fig.savefig(output / 'briefwahl-vs-urnenwahl.svg', facecolor=fig.get_facecolor())
    plt.close(fig)
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--election', choices=NAMES, required=True)
    parser.add_argument('--ref', default='HEAD', help='MV immutable Git snapshot')
    parser.add_argument('--source-dir', type=Path, help='Replay an existing sources folder without network')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve(); source = output / 'sources'; source.mkdir(parents=True, exist_ok=True)
    if args.source_dir:
        import shutil
        require(args.source_dir.resolve() != source, 'Use a separate output for replay')
        shutil.copytree(args.source_dir, source, dirs_exist_ok=True)
        acquisition = json.loads((source / 'acquisition.json').read_text())
        for name, expected in acquisition['sha256'].items():
            require(sha((source/name).read_bytes()) == expected, 'Replay source checksum differs')
    else:
        acquisition = {'election_key': args.election}
        if args.election == '2026-mv':
            ref = subprocess.check_output(['git', 'rev-parse', args.ref + '^{commit}'], cwd=ROOT, text=True).strip()
            acquisition['git_ref'] = ref
            for name in ['official-results-source.csv', 'official_results_source_metadata.json', 'run_metadata.json', 'statla_snapshots.csv', 'statla_party_results.csv']:
                (source / name).write_bytes(subprocess.check_output(['git', 'show', f'{ref}:data/2026-mv/latest/{name}'], cwd=ROOT))
            meta = json.loads((source / 'official_results_source_metadata.json').read_text())
            archived = (source / 'official-results-source.csv').read_bytes()
            # Poller transcodes CP1252 to UTF-8; Git normalizes CRLF to LF.
            original = archived.decode('utf-8').replace('\r\n', '\n').replace('\n', '\r\n').encode('cp1252')
            require(sha(original) == meta['content_hash'], 'Original CP1252/CRLF source hash mismatch')
            acquisition['original_source_sha256'] = meta['content_hash']
            acquisition['archive_encoding'] = 'UTF-8/LF; verified after restoring CP1252/CRLF source encoding'
            acquisition['fetched_at_utc'] = meta['fetched_at_utc']
        else:
            payload = subprocess.check_output(['curl', '--fail', '--silent', '--show-error', '--location',
                                               '--max-time', '60', BERLIN_URL])
            acquisition.update({'url': BERLIN_URL, 'status': 200, 'fetched_at_utc': datetime.now(timezone.utc).isoformat()})
            (source / 'official-results.html').write_bytes(payload)
        acquisition['sha256'] = {p.name: sha(p.read_bytes()) for p in source.iterdir() if p.is_file() and p.name != 'acquisition.json'}
        save_json(source / 'acquisition.json', acquisition)
    require(acquisition['election_key'] == args.election, 'Wrong election in source manifest')
    rows, context = mv_inputs(source) if args.election == '2026-mv' else berlin_inputs(source)
    for r in rows:
        r['brief_minus_urne_pp'] = r['briefwahl_share_percent'] - r['urnenwahl_share_percent']
        r['selected_for_chart'] = r['party'] != 'Sonstige' and max(r['briefwahl_share_percent'], r['urnenwahl_share_percent']) > 5
        r['source_url'] = context['source_url']; r['source_time'] = context['source_time']
    write_csv(output / 'briefwahl-vs-urnenwahl.csv', rows)
    selected = chart(output, args.election, rows, context)
    save_json(output / 'manifest.json', {'election_key': args.election, 'source': acquisition, 'context': context,
                                       'generator_sha256': sha(Path(__file__).read_bytes()),
                                       'selection': '>5% in either mode; exclude composite Sonstige',
                                       'selected_parties': [r['party'] for r in selected]})
    high = max(selected, key=lambda r:r['brief_minus_urne_pp']); low = min(selected,key=lambda r:r['brief_minus_urne_pp'])
    table = '\n'.join(f"| {r['party']} | {num(r['briefwahl_share_percent'], 2)} % | {num(r['urnenwahl_share_percent'], 2)} % | {r['brief_minus_urne_pp']:+.2f} |" for r in selected)
    detail = ('Die Stimmen aller 1.974 Wahlbezirke wurden je Wahlart summiert und sowohl insgesamt als auch für jede Partei mit dem Landesstand abgeglichen. '
              'Briefwahl wird über die amtliche Wahlbezirksbezeichnung erkannt und zusätzlich anhand der Wahlberechtigtenzahl null geprüft. '
              'Wahlbezirke ohne bisher gültige Stimmen tragen null Stimmen bei; das ist kein abgeschlossenes Nullergebnis. '
              'Stimmengewichtete Anteile, keine Mittelwerte von Wahlbezirksprozenten. `precincts.csv` enthält die Kontrollsummen und Stimmen je Wahlbezirk.'
              if args.election == '2026-mv' else
              'Verwendet werden die präzisen Prozentwerte der amtlichen landesweiten Grafik „Vergleich Urne-/Briefwahl: Zweitstimmen“. '
              'Beide vollständigen Prozentreihen einschließlich Sonstige ergeben jeweils 100 %. '
              'Absolute Parteistimmen, gültige Zweitstimmen und Wahlbezirkszahlen je Wahlart sind in dieser Quelle nicht ausgewiesen und bleiben im CSV leer. '
              'Es werden keine Stimmenzahlen aus gerundeten Anteilen rekonstruiert. Einzelne kleine Parteien sind nur als Sonstige verfügbar; '
              'Sonstige wird als Sammelkategorie nicht wie eine Partei in die >5%-Auswahl aufgenommen. `official-chart.json` erhält beide Originalreihen.')
    (output / 'README.md').write_text(f'''# {NAMES[args.election]}: Briefwahl und Urnenwahl 2026

Die größten Unterschiede unter den dargestellten Parteien: **{high['party']} liegt bei der Briefwahl um {num(high['brief_minus_urne_pp'], 2)} Prozentpunkte höher**, **{low['party']} um {num(-low['brief_minus_urne_pp'], 2)} Prozentpunkte niedriger** als bei der Urnenwahl.

**Zwischenergebnis, Stand {context['source_time']} MESZ: {num(context['reported'])} von {num(context['expected'])} Wahlbezirken.** Die Auszählung ist noch nicht vollständig; spätere Meldungen können die Anteile verändern.

![Landesweiter Vergleich](briefwahl-vs-urnenwahl.png)

| Partei | Briefwahl | Urnenwahl | Brief minus Urne (Prozentpunkte) |
|---|---:|---:|---:|
{table}

[PNG](briefwahl-vs-urnenwahl.png) · [SVG](briefwahl-vs-urnenwahl.svg) · [CSV mit allen verfügbaren Kategorien](briefwahl-vs-urnenwahl.csv)

## Einordnung und Methode

Wie beim Vergleich für Wahlkreis 14 werden Parteien mit mehr als 5 % in mindestens einer Wahlart gezeigt. Jeder Anteil bezieht sich auf **alle gültigen Zweitstimmen der jeweiligen Wahlart**, einschließlich nicht gezeigter Parteien. Die Differenz ist Briefwahl minus Urnenwahl in Prozentpunkten.

{detail}

Die Unterschiede beschreiben die bisher ausgezählten Wählergruppen. Sie belegen keinen kausalen Einfluss der Wahlart auf die Parteiwahl. Unterschiede in der Zusammensetzung und im Meldestand der Gruppen werden hier nicht statistisch bereinigt.

## Quelle und Reproduktion

[Amtliche Quelle]({context['source_url']}); eingefrorene Eingabedateien und SHA-256 in `sources/acquisition.json`, Prüfungen in `manifest.json`.
Der Berliner Vergleich kann einen anderen Quellenstand als das Git-basierte Video haben; die Zeitangaben sind für jedes Artefakt separat ausgewiesen.

```sh
python3 scripts/analyze_state_voting_modes.py --election {args.election} --source-dir {os.path.relpath(source, ROOT)} --output /tmp/{args.election}-voting-modes
```

Dependencies: Python, Matplotlib, BeautifulSoup and the repository poller parser. Replay uses saved sources without fetching or polling. No deployment is performed.
''')
    print(json.dumps({'output': str(output), 'source_time': context['source_time'], 'reported': context['reported'], 'selected': selected}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
