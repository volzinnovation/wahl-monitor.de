#!/usr/bin/env python3
"""Reproduce the Aken 000010 addendum from pinned, bundled GitHub source files.

Run after render_lsa_tweet_report.py. No live requests or changes to baseline data.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from analyze_lsa_git_timeline import overview_rows, write_csv, write_json

BERLIN = ZoneInfo('Europe/Berlin')
SOURCE = 'addendum_sources/aken_capture_bundle.json'
CHART = 'charts/14_aken_neuer_wahlbezirk.png'
IDENTITY = ['23, Zerbst', 'Anhalt-Bitterfeld', 'Aken (Elbe), Stadt', '000010']
GEO = ['Wahlkreis', 'Landkreis', 'Gemeinde', 'wbz']
SERIES = 'aken_addendum'


def local(value):
    return datetime.fromisoformat(value).astimezone(BERLIN)


def clock(value):
    """Round run-completion labels to the nearest second, keeping exact ISO in JSON."""
    return (local(value) + timedelta(microseconds=500000)).strftime('%H:%M:%S')


def derive(path):
    bundle = json.loads((path / SOURCE).read_text())
    summary = json.loads((path / 'summary.json').read_text())
    assert summary['ref'] == bundle['baseline_ref'], 'Addendum belongs to a different baseline'
    observations, sources, snapshots = [], [], {}
    for capture in bundle['captures']:
        files = {f['path']: f for f in capture['files']}
        for f in files.values():
            raw = f['content'].encode('utf-8')
            blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
            assert blob == f['git_blob_sha'], f"Git blob mismatch: {f['url']}"
            sources.append({'commit': capture['commit'], 'path': f['path'], 'url': f['url'],
                            'sha256': hashlib.sha256(raw).hexdigest(), 'git_blob_sha': blob})
        manifest = json.loads(files['official_sources/manifest.json']['content'])
        fetch = next(f for f in manifest['fetches'] if f['filename'] == 'overview.html')
        raw = files['official_sources/overview.html']['content'].encode('utf-8')
        assert fetch['status_code'] == 200 and not fetch['error_message']
        assert len(raw) == fetch['byte_count']
        assert hashlib.sha256(raw).hexdigest() == fetch['content_hash']
        rows = overview_rows(raw)
        metadata = json.loads(files['run_metadata.json']['content'])
        target = [r for r in rows.values() if [r[k] for k in GEO] == IDENTITY]
        assert len(target) <= 1
        assert sum(r['total_precincts'] for r in rows.values()) == metadata['overview_total_precincts']
        assert sum(r['reported_precincts'] for r in rows.values()) == metadata['overview_reported_precincts']
        snapshots[capture['commit']] = rows
        observations.append({'commit': capture['commit'],
                             'capture_utc': metadata['generated_at_utc'],
                             'capture_local': local(metadata['generated_at_utc']).isoformat(),
                             'overview_fetched_local': local(fetch['fetched_at_utc']).isoformat(),
                             'present': bool(target), 'district': target[0] if target else None,
                             'overview_reported': metadata['overview_reported_precincts'],
                             'overview_total': metadata['overview_total_precincts'],
                             'csv_reported': metadata['csv_reported_precincts'],
                             'csv_total': metadata['csv_total_precincts'],
                             'individual_vote_rows': metadata['wahlbezirk_rows']})
    assert observations == sorted(observations, key=lambda r: r['capture_utc'])
    before = next(o for o in observations if o['commit'] == bundle['last_absent_ref'])
    after = next(o for o in observations if o['present'])
    assert after['commit'] == bundle['first_present_ref']
    assert observations[-2:] == [before, after]
    assert before['individual_vote_rows'] == after['individual_vote_rows'] == 0
    first_commit = next(c for c in bundle['overview_change_commits'] if c['sha'] == after['commit'])
    assert first_commit['parents'] == [before['commit']], 'Not adjacent Git captures'
    expected = {c['sha'] for c in bundle['overview_change_commits']} - {summary['ref']}
    assert expected.issubset(snapshots), 'A changed overview was not inspected'
    baseline_rows = 0
    with gzip.open(path / 'status_timeline.jsonl.gz', 'rt') as f:
        for line in f:
            row = json.loads(line)
            assert [row[k] for k in GEO] != IDENTITY, 'District existed in the baseline history'
            baseline_rows += 1
    old, new = snapshots[before['commit']], snapshots[after['commit']]
    added = [new[k] for k in sorted(new.keys() - old.keys())]
    removed = [old[k] for k in sorted(old.keys() - new.keys())]
    assert len(added) == 1 and not removed
    assert [added[0][k] for k in GEO] == IDENTITY
    assert added[0]['reported_precincts'] == added[0]['total_precincts'] == 1
    groups = []
    for name, field, value in [('Aken (Elbe)', 'Gemeinde', IDENTITY[2]),
                               ('Wahlkreis 23 Zerbst', 'Wahlkreis', IDENTITY[0]),
                               ('Anhalt-Bitterfeld', 'Landkreis', IDENTITY[1]),
                               ('Sachsen-Anhalt', None, None)]:
        group = {'area': name}
        for tag, data in [('before', old), ('after', new)]:
            rows = [r for r in data.values() if field is None or r[field] == value]
            group[tag + '_total'] = sum(r['total_precincts'] for r in rows)
            group[tag + '_reported'] = sum(r['reported_precincts'] for r in rows)
        assert group['after_total'] - group['before_total'] == 1
        groups.append(group)
    return {'baseline_ref': summary['ref'], 'identity': dict(zip(GEO, IDENTITY)),
            'before': before, 'after': after, 'observations': observations,
            'added': added, 'removed': removed, 'groups': groups, 'sources': sources,
            'baseline_status_rows_checked': baseline_rows,
            'individual_district_party_votes_available': False,
            'cause_documented': False, 'timezone': 'Europe/Berlin (MESZ, UTC+02:00)'}


def graphic(path, case):
    # Contract: four overlapping geographic totals, two adjacent captures.
    # Exact lookup matters; use a before/after table, not a truncated-scale trend.
    # Static tweet image, 1960x1120; one blue root + neutral, direct text labels.
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/lsa-report-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    ink, muted, blue = '#222a35', '#66717e', '#3478a5'
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'text.color': ink})
    fig = plt.figure(figsize=(14, 8), facecolor='white')
    fig.text(.06, .94, 'Aken (Elbe): Wahlbezirk 000010', fontsize=24, weight='bold', va='top')
    fig.text(.06, .878, 'Nachtrag · 7. September 2026 · deutsche Ortszeit: MESZ (UTC+2)', fontsize=13, color=muted)
    fig.text(.06, .802, 'Neue Zeile in der amtlichen Übersicht; beim ersten Auftauchen bereits gemeldet.', fontsize=13)
    fig.text(.06, .706, 'Wahlbezirke insgesamt (Soll)', fontsize=13, weight='bold')
    fig.text(.57, .706, clock(case['before']['capture_local']), fontsize=17, ha='center', weight='bold')
    fig.text(.81, .706, clock(case['after']['capture_local']), fontsize=17, ha='center', weight='bold')
    fmt = lambda n: f'{n:,}'.replace(',', '.')
    for y, row in zip([.61, .52, .43, .34], case['groups']):
        fig.text(.06, y, row['area'], fontsize=16)
        fig.text(.57, y, fmt(row['before_total']), fontsize=22, ha='center')
        fig.text(.69, y, '→', fontsize=22, ha='center', color=muted)
        fig.text(.81, y, fmt(row['after_total']), fontsize=22, ha='center', color=blue, weight='bold')
        fig.text(.925, y, '+1', fontsize=15, ha='right', color=blue)
    fig.text(.06, .222, '000010: vorher nicht aufgeführt → danach gemeldet (1/1)', fontsize=15, weight='bold')
    fig.text(.06, .155, 'Überlappende Gebietsebenen: dieselbe zusätzliche Zeile, keine vier neuen Bezirke.', fontsize=11, color=muted)
    fig.text(.06, .117, 'Zeiten: Archivlauf, auf Sekunden gerundet. Kein Nachweis der Ursache oder der genauen Veröffentlichung.', fontsize=10, color=muted)
    fig.text(.06, .05, f"Quelle: amtliches HTML im Git-Archiv {case['before']['commit'][:8]} → {case['after']['commit'][:8]} · wahl-monitor.de", fontsize=9, color=muted)
    fig.savefig(path / CHART, dpi=140, metadata={'Software': 'wahl-monitor reproducible audit'})
    plt.close(fig)


def section(text, name, body, anchor=None):
    pattern = rf'<!-- {name}:start -->.*?<!-- {name}:end -->\n*'
    text = re.sub(pattern, '', text, flags=re.S)
    block = f'<!-- {name}:start -->\n{body.rstrip()}\n<!-- {name}:end -->\n\n'
    if anchor:
        assert text.count(anchor) == 1
        return text.replace(anchor, block + anchor, 1)
    return text.rstrip() + '\n\n' + block


def apply_update(path):
    case = derive(path)
    before, after = case['before'], case['after']
    write_json(path / 'aken_case.json', case)
    write_csv(path / 'aken_capture_timeline.csv', [{k: v for k, v in o.items() if k != 'district'} for o in case['observations']])
    write_csv(path / 'aken_aggregation_diff.csv', case['groups'])
    graphic(path, case)
    alt = 'Aken: Wahlbezirk 000010 erscheint gemeldet. 07.09.2026, 00:45:41 → 00:51:10 MESZ: Aken 9 → 10, Wahlkreis Zerbst 79 → 80, Anhalt-Bitterfeld 204 → 205, Sachsen-Anhalt 2.660 → 2.661 Wahlbezirke insgesamt.'
    texts = [
        'Nachtrag 1/2: Am 07.09.2026 erscheint Wahlbezirk 000010 in Aken (Elbe), Wahlkreis 23 Zerbst, neu in der Übersicht. Um 00:45:41 MESZ noch nicht aufgeführt, im Archivlauf 00:51:10 MESZ bereits gemeldet. Deutsche Ortszeit: UTC+2.',
        'Nachtrag 2/2: Der Aken-Eintrag erhöht die Zahl der Wahlbezirke von 2.660 auf 2.661. Aken: 9 → 10; Wahlkreis Zerbst: 79 → 80; Anhalt-Bitterfeld: 204 → 205. Keine Zeile verschwindet. Warum der Eintrag hinzukam, ist in den geprüften Quellen nicht erklärt.'
    ]
    tweets = [t for t in json.loads((path / 'tweets.json').read_text()) if t.get('series') != SERIES]
    extra = []
    for text in texts:
        weight = sum(1 if ord(c) <= 0x10ff or 0x2000 <= ord(c) <= 0x200d or 0x2010 <= ord(c) <= 0x201f or 0x2032 <= ord(c) <= 0x2037 else 2 for c in text)
        assert weight <= 280
        extra.append({'series': SERIES, 'text': text, 'weighted_characters': weight,
                      'images': [CHART], 'alt_text': [alt], 'evidence': ['aken_case.json', 'AKEN_000010.md']})
    write_json(path / 'tweets.json', tweets + extra)
    (path / 'tweets.txt').write_text('\n\n'.join(t['text'] for t in tweets + extra) + '\n')
    charts = [c for c in json.loads((path / 'chart_map.json').read_text()) if c['id'] != '14_aken_neuer_wahlbezirk']
    charts.append({'id': '14_aken_neuer_wahlbezirk', 'path': CHART, 'alt_text': alt,
                   'question': 'Which added status identity explains the changed expected total, and when was it first seen?',
                   'family': 'before/after table', 'renderer': 'Matplotlib PNG',
                   'fields': ['area', 'before_total', 'after_total', 'capture_local', 'district identity'],
                   'source': 'aken_case.json; aken_aggregation_diff.csv',
                   'note': 'Four overlapping geographic totals; exact lookup, no summed groups or interpolated publication time. Eight post-baseline observations checked.',
                   'palette_policy': 'single blue root plus neutrals', 'non_color_encoding': 'direct values, arrows, status text'})
    write_json(path / 'chart_map.json', charts)
    url = lambda ref, file: f'https://github.com/volzinnovation/wahl-monitor.de/blob/{ref}/data/2026-lsa/latest/{file}'
    rows = '\n'.join(f"| {g['area']} | {g['before_total']:,} | {g['after_total']:,} | +1 |".replace(',', '.') for g in case['groups'])
    detail = f'''# Aken (Elbe), Wahlbezirk 000010: zusätzlicher Eintrag nach Mitternacht

**Erstmals im Archivlauf vom 07.09.2026 um {clock(after['capture_local'])} MESZ sichtbar, bereits gemeldet (1/1).** Im direkt vorherigen Lauf um {clock(before['capture_local'])} MESZ fehlt der Eintrag. Er gehört zu Wahlkreis 23 Zerbst und Landkreis Anhalt-Bitterfeld.

Dieser Nachtrag ergänzt den eingefrorenen Bericht von 00:00:20 MESZ. Er ist kein vollständiger neuer Landes-Audit; dessen 98,27 %, Parteianteile, 46 offene Meldungen und Revisionszahlen beziehen sich weiterhin auf den ursprünglichen Stand.

## Eine zusätzliche Zeile erklärt das neue Soll

Verglichen werden die vollständigen Identitäten aus Wahlkreis, Kreis, Gemeinde und Bezirksnummer. Genau eine kommt hinzu, keine verschwindet. Die Tabelle zeigt das gesamte Soll, nicht die Zahl eingegangener Meldungen. Alle vier Ebenen enthalten denselben zusätzlichen Bezirk und dürfen nicht addiert werden.

| Gebiet | Vorher: {clock(before['capture_local'])} MESZ | Nachher: {clock(after['capture_local'])} MESZ | Änderung |
|---|---:|---:|---:|
{rows}

![{alt}]({CHART})

Belege: [vorherige amtliche Übersicht]({url(before['commit'], 'official_sources/overview.html')}) · [erste Übersicht mit 000010]({url(after['commit'], 'official_sources/overview.html')}) · [berechneter Identitäts- und Aggregationsvergleich](aken_case.json).

## Deutsche Zeit und Genauigkeit

Alle Ortszeiten sind **Europe/Berlin: MESZ, UTC+02:00**, am **7. September 2026**. Die UTC-Datumsangaben im Git-Archiv liegen noch am 6. September. Die sichtbaren Archivlauf-Zeiten sind auf Sekunden gerundet; die vollständigen Zeitstempel bleiben in den Belegen erhalten.

| Ereignis | Letzter Lauf ohne 000010 | Erster Lauf mit 000010 |
|---|---|---|
| Archivlauf, UTC | {before['capture_utc']} | {after['capture_utc']} |
| Archivlauf, MESZ | {before['capture_local']} | {after['capture_local']} |
| HTML-Abruf laut Manifest, MESZ | {before['overview_fetched_local']} | {after['overview_fetched_local']} |

Die bisherigen Angaben 00:45:41 und 00:51:10 bezeichnen die Archivläufe. Für die HTML-Quelle dokumentieren die Manifeste genauer **00:45:37,326 → 00:51:06,885 MESZ**. Das grenzt den beobachteten Wechsel ein; der genaue Veröffentlichungszeitpunkt bleibt unbekannt.

Belege: [Manifest vorher]({url(before['commit'], 'official_sources/manifest.json')}) · [Manifest nachher]({url(after['commit'], 'official_sources/manifest.json')}) · [gesamte geprüfte Folge nach Mitternacht](aken_capture_timeline.csv).

## Was daraus folgt – und was offen bleibt

Der Nenner der Meldequote kann sich ändern. Der nächste vollständige Lauf muss das jeweils veröffentlichte Soll verwenden und hinzugekommene Identitäten gesondert ausweisen. Die 2.660 des eingefrorenen Berichts bleiben für dessen Zeitpunkt korrekt.

Der Landes-Meldestand im HTML steigt in diesem Übergang von {before['overview_reported']}/{before['overview_total']} auf {after['overview_reported']}/{after['overview_total']}; parallel ändern sich weitere Meldestatus. Der neue Aken-Eintrag erklärt den Anstieg des Solls um eins, nicht sämtliche neuen Meldungen im Land. In beiden Läufen stimmen HTML und CSV beim Soll überein; beim Ist zeigt der vorherige Lauf einen Publikationsversatz (CSV: {before['csv_reported']}/{before['csv_total']}).

Die Quellen erklären nicht, warum der Eintrag hinzugefügt wurde. Weder eine physische Neueinrichtung noch eine bestimmte Korrektur oder Wahlunregelmäßigkeit ist damit nachgewiesen. Einzel-Wahlbezirksergebnisse mit Parteistimmen waren in diesen beiden Läufen weiterhin nicht veröffentlicht. Die Gemeinde-Stimmen dürfen nicht ohne weitere Belege vollständig 000010 zugerechnet werden.

Nächste Prüfung: amtliche Begründung und gegebenenfalls Bezirkstyp nachreichen; bei verfügbaren Einzelstimmen deren Identität und Summen abgleichen.
'''
    (path / 'AKEN_000010.md').write_text(detail)
    report = (path / 'REPORT.md').read_text()
    note = '**Nachtrag vom 07.09.2026, 00:51 MESZ:** Aken (Elbe), Wahlbezirk **000010**, erscheint neu und bereits gemeldet; das Landes-Soll steigt **2.660 → 2.661**. [Zeitpunkte, Belege und Einordnung](AKEN_000010.md). Die folgenden 15 Tweets behalten ihren Stand von 00:00:20 MESZ; zwei ergänzende Tweets stehen danach.'
    report = section(report, 'aken-summary', note, '## Tweet-Serie mit Bildern')
    addendum = ['## Nachtrag: Aken 000010 und das erhöhte Soll', '',
                'Am 7. September zwischen den Archivläufen 00:45:41 und 00:51:10 MESZ erscheint genau eine zusätzliche Bezirksidentität. Die beiden Tweets ergänzen den ursprünglichen Bericht; sie aktualisieren nicht dessen übrige Auswertungen.', '']
    for i, t in enumerate(extra, 1):
        addendum += [f'### Nachtrag-Tweet {i}', '', t['text'], '']
    addendum += [f'![{alt}]({CHART})', '',
                 'Die Grafik zeigt das Soll auf überlappenden Gebietsebenen. Der Anstieg um eins geht jeweils auf dieselbe neue Zeile zurück. Der Grund ihrer Aufnahme und der genaue Veröffentlichungszeitpunkt bleiben offen.', '',
                 'Belege und genaue HTML-Abrufzeiten: [Detailbericht Aken](AKEN_000010.md) · [Identitätsvergleich](aken_case.json) · [Abruffolge](aken_capture_timeline.csv).']
    report = section(report, 'aken-detail', '\n'.join(addendum), '## Vollständige Liste: Revisionen bei gleichem Meldestand')
    (path / 'REPORT.md').write_text(report)
    methods = f'''## Reproduce the Aken addendum

After reproducing the original report above, retain or copy `{SOURCE}` into the output folder and run:

```sh
python3 scripts/add_lsa_aken_update.py --input data/2026-lsa/reports/git-timeline/034045f3
```

The bundle retains exact UTF-8 file contents, immutable commit URLs and Git blob identities for eight post-baseline observations: every changed overview up to the first Aken entry, plus its immediate predecessor. Each HTML file is verified against the collector's SHA-256 and byte count; all 24 files are verified against Git blob hashes. The baseline status history is also checked for earlier occurrences. Full identity sets and four geographic totals are compared. All inputs are offline; no collection, publication or full-data rerun occurs.

`aken_case.json`, `aken_capture_timeline.csv`, `aken_aggregation_diff.csv`, `AKEN_000010.md` and image 14 are generated. Two separately labelled addendum tweets are appended; the original 15 tweet objects and baseline evidence are preserved. The command is idempotent. UTC timestamps use Europe/Berlin conversion, with exact source-fetch and run-completion times retained separately. Seconds in the tweet image are rounded run labels, not exact publication times.

Chart choice: an exact before/after table for four overlapping totals; a time trend would imply unobserved continuity. Existing report structure and visual order remain intact. The addendum supplies one finding, evidence, an interpretation limit and the next verification step; the existing stakeholder report remains the primary reading surface.
'''
    (path / 'METHODS.md').write_text(section((path / 'METHODS.md').read_text(), 'aken-methods', methods))
    env = json.loads((path / 'environment.json').read_text())
    env['aken_addendum_script_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    write_json(path / 'environment.json', env)
    print(json.dumps({'added': case['identity'], 'captures': len(case['observations']),
                      'tweet_count': len(tweets + extra), 'chart_count': len(charts)}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    apply_update(parser.parse_args().input.resolve())
