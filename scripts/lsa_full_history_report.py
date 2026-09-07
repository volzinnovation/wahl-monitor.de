"""Full-history report findings, derived only from the immutable audit outputs."""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import textwrap
from datetime import datetime, timedelta

from analyze_lsa_git_timeline import write_csv, write_json

BLUE, INK, MUTED = '#3478a5', '#222a35', '#66717e'


def rows(path):
    with path.open() as f:
        return list(csv.DictReader(f))


def n(value):
    return f'{value:,}'.replace(',', '.')


def tm(value):
    return (datetime.fromisoformat(value) + timedelta(microseconds=500000)).strftime('%H:%M:%S')


def derive(r):
    s, path = r.s, r.path
    additions = s['status_identity_additions']
    aken = next(e for e in additions if e['name'] == 'Aken (Elbe), Stadt / 000010')
    status = {aken['previous_commit']: {}, aken['commit']: {}}
    with gzip.open(path/'status_timeline.jsonl.gz', 'rt') as f:
        for line in f:
            row = json.loads(line)
            if row['commit'] in status:
                status[row['commit']][row['key']] = row
    before, after = status[aken['previous_commit']], status[aken['commit']]
    added = sorted(after.keys() - before.keys())
    removed = sorted(before.keys() - after.keys())
    assert added == [aken['key']] and not removed
    assert after[aken['key']]['reported_precincts'] == 1
    groups = []
    for label, field, value in [('Aken (Elbe)', 'Gemeinde', 'Aken (Elbe), Stadt'),
                               ('Wahlkreis 23 Zerbst', 'Wahlkreis', '23, Zerbst'),
                               ('Anhalt-Bitterfeld', 'Landkreis', 'Anhalt-Bitterfeld'),
                               ('Sachsen-Anhalt', None, None)]:
        group = {'area': label}
        for tag, data in [('before', before), ('after', after)]:
            group[tag] = sum(x['total_precincts'] for x in data.values() if field is None or x[field] == value)
        groups.append(group)
    provenance = json.loads((path/'source_manifest.json').read_text())
    source_times = {p['commit']: p['fetched_at_utc'] for p in provenance if p['filename'] == 'overview.html'}
    identity = {'additions': additions, 'removals': s['status_identity_removals'],
                'reappearances': s['status_identity_reappearances'], 'denominator_changes': s['denominator_changes'],
                'aken': {'event': aken, 'row': after[aken['key']], 'groups': groups,
                         'previous_html_fetch_utc': source_times[aken['previous_commit']],
                         'first_html_fetch_utc': source_times[aken['commit']]}}
    residuals = rows(path/'aggregation_nonzero.csv')
    last = s['last_capture']['commit']
    final = [x for x in residuals if x['commit'] == last and x['relation'] == 'GEMEINDE->LAND']
    first = residuals[0]['commit']
    deltas = {x['field']: int(x['delta']) for x in final}
    events = rows(path/'area_events.csv')
    matches = []
    for event in events:
        changes = json.loads(event['changes'])
        if event['commit'] == first and event['level'] == 'GEMEINDE' and {k: v['delta'] for k,v in changes.items()} == deltas:
            matches.append({**event, 'changes': changes})
    assert len(matches) == 1, 'Final residual must match exactly one municipal arrival'
    municipal = [x for x in r.latest.values() if x['level'] == 'GEMEINDE']
    names = r.latest['lsa:LAND:15']['party_names']
    totals = []
    for code in sorted(names, key=lambda c:(c[0],int(c[1:]))):
        land = r.latest['lsa:LAND:15']['parties'].get(code)
        total = sum(x['parties'].get(code) or 0 for x in municipal)
        if land is None:
            continue
        totals.append({'code':code, 'party':names[code], 'vote_type':'Erststimmen' if code[0]=='D' else 'Zweitstimmen',
                       'land_votes':land, 'municipality_sum':total, 'delta':total-land})
    assert sum(x['delta'] for x in totals if x['code'].startswith('F')) == deltas['valid_votes_zweit']
    assert sum(x['delta'] for x in totals if x['code'].startswith('D')) == deltas['valid_votes_erst']
    extra_status = [x for x in rows(path/'status_events.csv') if x['commit']==first]
    gap = {'first_capture':r.version_by_commit[first], 'last_capture':s['last_capture'],
           'observed_commits':list(dict.fromkeys(x['commit'] for x in residuals)),
           'geographic_field_residual_count':len(residuals), 'latest_land_deltas':deltas,
           'latest_residuals':final, 'matching_municipality_arrival':matches[0],
           'status_changes_at_first_gap':extra_status, 'party_comparison':totals,
           'municipality_sum_valid_erst':sum(x['valid_votes_erst'] for x in municipal),
           'municipality_sum_valid_zweit':sum(x['valid_votes_zweit'] for x in municipal),
           'municipality_sum_reported':sum(x['reported_precincts'] for x in municipal)}
    midnight = datetime.fromisoformat('2026-09-07T00:00:21+02:00')
    late = [e for e in s['municipality_revisions'] if datetime.fromisoformat(e['acquired_at_local']) > midnight]
    return identity, gap, late


def render(r, baseline):
    path, s = r.path, r.s
    identity, gap, late = derive(r)
    write_json(path/'district_identity_changes.json', identity)
    write_json(path/'aggregation_gap.json', gap)
    write_json(path/'post_midnight_revisions.json', late)
    write_csv(path/'land_vs_municipality_parties.csv', gap['party_comparison'])
    a = identity['aken']; event = a['event']
    fig = r.figure('Wahlbezirke beim neuen Aken-Eintrag', f"07.09.2026 · {tm(event['previous_acquired_at_local'])} → {tm(event['acquired_at_local'])} MESZ · 000010 erscheint bereits gemeldet")
    fig.text(.06,.73,'Gebiet',fontsize=15,weight='bold');fig.text(.64,.73,'Soll vorher',fontsize=15,ha='center');fig.text(.84,.73,'Soll nachher',fontsize=15,ha='center')
    for y,g in zip([.62,.50,.38,.26],a['groups']):
        fig.text(.06,y,g['area'],fontsize=18);fig.text(.64,y,n(g['before']),ha='center',fontsize=24)
        fig.text(.74,y,'→',ha='center',fontsize=24,color=MUTED);fig.text(.84,y,n(g['after']),ha='center',fontsize=24,weight='bold',color=BLUE)
    fig.text(.06,.13,'Genau eine neue Identität, keine entfernte. Überlappende Gebietsebenen: nicht addieren.',fontsize=11,color=MUTED)
    r.save(fig,'14_aken_neuer_wahlbezirk','Which new identity increased the denominator?','before/after table',['area','expected districts','capture','identity'],'district_identity_changes.json','Four overlapping totals; one identity change. Run timestamps rounded to seconds; exact HTML fetch timestamps retained separately.')

    selected = [next(x for x in gap['party_comparison'] if x['code']==p['code']) for p in r.selected]
    rest = gap['latest_land_deltas']['valid_votes_zweit'] - sum(x['delta'] for x in selected)
    labels=[x['party'] for x in selected]+['Übrige Parteien'];values=[x['delta'] for x in selected]+[rest]
    fig=r.figure('Gemeindesumme minus amtliche Landes-CSV',f"{r.cutoff:%d.%m.%Y %H:%M} MESZ · +{n(sum(values))} gültige Zweitstimmen in der Gemeindesumme · Differenz seit 03:04")
    ax=fig.add_axes([.17,.23,.73,.54]);ax.barh(labels,values,color=BLUE,height=.62)
    for i,v in enumerate(values):ax.text(v+5,i,f'+{n(v)}',va='center',fontsize=14)
    ax.invert_yaxis();ax.set_xlim(0,max(values)*1.17);ax.set_xlabel('Differenz der Zweitstimmen (Anzahl)');ax.grid(axis='x',alpha=.13);ax.set_axisbelow(True)
    fig.text(.17,.115,'Alle Partei- und Summendifferenzen entsprechen exakt dem neuen Gemeindezuwachs in Aschersleben.',fontsize=10.5,color=MUTED)
    r.save(fig,'15_aggregationsdifferenz','Which parties account for the cross-level CSV gap?','horizontal delta bar',['party','municipality votes','Land votes','delta'],'land_vs_municipality_parties.csv','Zero baseline; all selected >5% parties plus explicit remainder. This is a source aggregation gap, not proof of lost votes or a certified precinct result.')

    names = r.latest['lsa:LAND:15']['party_names']
    def changes(e):
        labels={'voters_total':'Wählende','valid_votes_erst':'gültige Erststimmen','valid_votes_zweit':'gültige Zweitstimmen'}
        output=[]
        for field, d in e['changes'].items():
            if field.startswith('party:'):
                code=field[6:];label=names[code]+(' (E)' if code.startswith('D') else ' (Z)')
            else:label=labels.get(field,field)
            output.append(f"{label} {d['delta']:+}")
        return '; '.join(output)
    fig=r.figure('Gemeinderevisionen nach dem ersten Bericht',f"{len(late)} Änderungen nach 00:00:20 MESZ · jeweils unveränderte Zahl gemeldeter Wahlbezirke",height=10)
    for i,e in enumerate(late):
        y=.75-i*.12
        label=e['name'].replace(', Stadt','')
        fig.text(.06,y,tm(e['acquired_at_local'])+' · '+label,fontsize=13,weight='bold',va='top')
        fig.text(.37,y,textwrap.fill(changes(e),width=73),fontsize=12.5,va='top',linespacing=1.5)
    fig.text(.06,.105,'E = Erststimmen, Z = Zweitstimmen. Vorher-/Nachher-Werte aller Parteien stehen im vollständigen Audit.',fontsize=10.5,color=MUTED)
    r.save(fig,'16_revisionen_nach_mitternacht','Which fixed-count municipal revisions arrived after the first report?','annotated table',['municipality','capture','party and total deltas'],'post_midnight_revisions.json','Exact five-event lookup. Signed labels; source causes remain undocumented. No district-level attribution.')

    # Detailed cross-level reconciliation, including every party and both ballots.
    detail=['# Aschersleben: Die Gemeindesumme liegt über den höheren Aggregaten','',
            f"**Ab {tm(gap['first_capture']['acquired_at_local'])} MESZ enthält die Gemeinde-CSV {n(gap['latest_land_deltas']['valid_votes_zweit'])} gültige Zweitstimmen mehr als die Landes-CSV.** Die Differenz bleibt bis zum letzten Archivlauf {tm(s['last_capture']['acquired_at_local'])} MESZ bestehen. Alle Partei- und Summendifferenzen stimmen exakt mit dem gleichzeitig neu erfassten Zuwachs der Gemeinde Aschersleben überein.",'',
            'Die Gemeinde steigt von 25/26 auf 26/26 gemeldete Bezirke. Im HTML wechselt Aschersleben / 000965 auf gemeldet; zusätzlich wechselt Petersberg / 000012 seinen Status. Eine ursprüngliche Einzelbezirk-Stimmentabelle liegt weiterhin nicht vor. Der rechnerisch passende Gemeindezuwachs ist deshalb ausdrücklich kein unabhängig belegtes Einzelbezirksergebnis.','',
            '## Dieselbe Differenz wird mehrfach sichtbar','',
            f"{len(gap['observed_commits'])} archivierte Stände zeigen jeweils 27 abweichende Felder sowohl Gemeinde → Land als auch Gemeinde → Salzlandkreis: zusammen {s['aggregation_nonzero']} nicht-null Vergleiche. Das sind Wiederholungen derselben Aggregationslücke, keine 162 unabhängigen Ereignisse.",'',
            '| Größe | Amtliche Landes-CSV | Summe Gemeinde-CSV | Differenz |','|---|---:|---:|---:|']
    for field,label in [('reported_precincts','Gemeldete Bezirke'),('voters_total','Wählende'),('valid_votes_erst','Gültige Erststimmen'),('valid_votes_zweit','Gültige Zweitstimmen')]:
        land=r.latest['lsa:LAND:15'][field];delta=gap['latest_land_deltas'][field]
        detail.append(f'| {label} | {n(land)} | {n(land+delta)} | +{n(delta)} |')
    detail += ['', 'Der Wahlkreis Aschersleben bleibt im höheren CSV-Aggregat bei 54/55, der Salzlandkreis bei 193/194, das Land bei 2.660/2.661. Die Gemeinde-CSV und das HTML weisen am letzten Stand alle Bezirke als gemeldet aus. Das ist mit einem nicht fortgeschriebenen höheren Aggregat vereinbar; den tatsächlichen Publikationsablauf oder Grund belegen die Dateien nicht.','',
               '## Parteivergleich','', '| Stimmen | Partei | Landes-CSV | Gemeindesumme | Δ |','|---|---|---:|---:|---:|']
    for x in gap['party_comparison']:
        detail.append(f"| {x['vote_type']} | {x['party']} | {n(x['land_votes'])} | {n(x['municipality_sum'])} | {x['delta']:+} |")
    detail += ['', 'Die landesweiten Parteianteile im Hauptbericht verwenden unverändert die amtliche Landes-CSV als Nenner und Quelle. Die Gemeindesumme ist eine gesondert ausgewiesene eigene Aggregation, kein ersetztes amtliches Landesergebnis.','',
               '[Vollständige berechnete Lücke](aggregation_gap.json) · [Parteitabelle CSV](land_vs_municipality_parties.csv) · [Alle nicht-null Aggregationsvergleiche](aggregation_nonzero.csv) · [Gesamtbericht](REPORT.md)','',
               f"[Unveränderliche amtliche Quelldateien](https://github.com/volzinnovation/wahl-monitor.de/tree/{s['ref']}/data/2026-lsa/latest/official_sources)."]
    (path/'AGGREGATION_ASCHERSLEBEN.md').write_text('\n'.join(detail)+'\n')
    aken_text=['# Aken (Elbe), Wahlbezirk 000010: erstmaliger Eintrag','',
               f"**Erstmals im Archivlauf {tm(event['acquired_at_local'])} MESZ am 07.09.2026 vorhanden, bereits gemeldet.** Im direkt vorherigen Archivlauf {tm(event['previous_acquired_at_local'])} MESZ noch nicht aufgeführt. Die vollständige Statushistorie enthält genau diese eine neue Identität und keine entfernte Identität.",'',
               'Wahlkreis 23 Zerbst, Landkreis Anhalt-Bitterfeld. Deutsche Ortszeit: Europe/Berlin, MESZ (UTC+2). Zeiten der Archivläufe sind auf Sekunden gerundet; für die HTML-Abrufe liegen genauere Zeitstempel vor:','',
               f"- Vorher, UTC: `{a['previous_html_fetch_utc']}` = 07.09.2026, 00:45:37,326 MESZ.",
               f"- Nachher, UTC: `{a['first_html_fetch_utc']}` = 07.09.2026, 00:51:06,885 MESZ.",'',
               'Der tatsächliche Veröffentlichungszeitpunkt liegt zwischen den beobachteten Zuständen und ist nicht exakt bekannt.','',
               '| Gebiet | Soll vorher | Soll nachher |','|---|---:|---:|']
    aken_text += [f"| {g['area']} | {n(g['before'])} | {n(g['after'])} |" for g in a['groups']]
    aken_event=next(e for e in s['denominator_changes'] if e['level']=='GEMEINDE')
    aken_text += ['', 'Dieselbe neue Identität erhöht alle vier überlappenden Summen. Zeitgleich steigt Aken um 440 Wählende, 432 gültige Erst- und 435 gültige Zweitstimmen. Dieser Gemeindezuwachs wird in Wahlkreis, Kreis und Land identisch fortgeschrieben; die später beobachtete Aschersleben-Lücke ist ein anderer Vorgang.','',
                  'Warum der Eintrag hinzugefügt wurde, wird in den geprüften Quellen nicht erklärt. Die Änderung beweist weder eine neu eingerichtete physische Wahlstelle noch Wahlmanipulation. Die Gemeinde-Stimmendifferenzen sind keine separat veröffentlichten Stimmen des Bezirks.','',
                  '[Identitäts- und Summenänderungen mit allen Partei-Deltas](district_identity_changes.json) · [Gesamtbericht](REPORT.md)','',
                  f"[Vorherige Quelle](https://github.com/volzinnovation/wahl-monitor.de/tree/{event['previous_commit']}/data/2026-lsa/latest/official_sources) · [Erste Quelle mit 000010](https://github.com/volzinnovation/wahl-monitor.de/tree/{event['commit']}/data/2026-lsa/latest/official_sources)"]
    assert aken_event['changes']['valid_votes_zweit']['delta']==435
    (path/'AKEN_000010.md').write_text('\n'.join(aken_text)+'\n')

    tweets=[]
    def add(text, images, evidence):
        tweets.append({'text':text, 'images':[r.chart_files[c] for c in images], 'evidence':evidence})
    add(f"LSA: Polling beendet. Vollständige Git-Historie bis 07.09.2026, 03:10 MESZ: {s['history_commit_count']} Daten-Commits, {s['versions']} datierte Abrufe, davon {s['election_night_captures']} am Wahlabend/in der Nacht. Alle Zahlen und Grafiken wurden neu berechnet.",['01_auszaehlung'],['history_inventory.csv','polling_stopped.json'])
    add('Letzter Stand: HTML und Gemeinde-CSV 2.661/2.661 Bezirke gemeldet; Landes-CSV 2.660/2.661 (99,96 %). Vollständige Statusmeldungen bedeuten hier noch keine übereinstimmenden Stimmenaggregate. Kein amtliches Endergebnis.',['07_offene_meldungen'],['summary.json','aggregation_gap.json'])
    add('Zweitstimmen der amtlichen Landes-CSV: '+', '.join(f"{p['party']} {p['share_percent']:.2f} %".replace('.',',') for p in r.selected)+'. Auswahl: strikt über 5 %; Nenner sind alle gültigen Zweitstimmen.',['02_parteien'],['summary.json'])
    add('Ab 03:04 MESZ: Gemeindesumme 1.315.315 gültige Zweitstimmen, Landes-CSV 1.313.802. Differenz +1.513, bis 03:10 unverändert. Sie entspricht exakt dem neuen Gemeindezuwachs in Aschersleben.',['15_aggregationsdifferenz'],['aggregation_gap.json','AGGREGATION_ASCHERSLEBEN.md'])
    add('Gemeindesumme minus Landes-CSV, Zweitstimmen: CDU +401, AfD +373, SPD +211, BSW +148, Linke +146, GRÜNE +122, übrige +112. Eine Aggregationslücke in den veröffentlichten Dateien; kein Beleg für verlorene Stimmzettel.',['15_aggregationsdifferenz'],['land_vs_municipality_parties.csv'])
    add('Magdeburg, Halle und Dessau-Roßlau im Vergleich mit den übrigen 215 Gemeinden. Die übrigen Gemeinden werden nach Stimmen gewichtet. Gemeindezahlen enthalten den späteren Aschersleben-Zuwachs; die amtliche Landes-CSV noch nicht.',['04_staedte'],['results_by_area.csv','aggregation_gap.json'])
    add('Alle 14 Kreise/kreisfreien Städte: dieselbe landesweite Parteiauswahl, lokale Anteile unter 5 % bleiben sichtbar. Überlappende Gebietsebenen werden nicht addiert. Der Salzlandkreis bleibt im höheren CSV-Aggregat bei 193/194 Meldungen.',['05_kreise'],['results_by_area.csv'])
    add('Alle 41 Wahlkreise und die Streuung in 218 Gemeinden. Wahlkreise stammen direkt aus dem amtlichen Export; geteilte Gemeinden werden nicht pauschal zugeordnet. Regionale Unterschiede allein belegen keine Unregelmäßigkeit.',['06_wahlkreise','12_gebietsstreuung'],['results_by_area.csv'])
    u,b=r.raw['lsa:LAND:15:U'],r.raw['lsa:LAND:15:B']
    add(f"Landes-CSV: Urne {u['reported_precincts']}/{u['total_precincts']}, Brief {b['reported_precincts']}/{b['total_precincts']}. Der offene Zähler liegt bei der Briefwahl. Die erfassten Parteianteile unterscheiden sich nach Wahltyp; dies ist keine Prognose.",['11_urne_brief'],['latest_official_rows.json'])
    add('Der Parteiverlauf zeigt die Zusammensetzung eingehender Meldungen, keine wechselnden Wählerpräferenzen. Frühe Ergebnisse sind selektiv. Die Kurven zeigen die Wahlnacht; frühere Nullvorlagen bleiben im vollständigen Daten-Audit.',['03_parteiverlauf'],['raw_timeline.jsonl.gz','history_inventory.csv'])
    add('Bitterfeld-Wolfen / 000028: gemeldet, ab 19:35 wieder nicht gemeldet, ab 22:11 erneut gemeldet. Es bleibt die einzige beobachtete Statusrücknahme. Einzelstimmen dieses Bezirks wurden nicht archiviert.',['08_status_ruecknahme'],['summary.json','BITTERFELD_WOLFEN_000028.md'])
    distinct=len({e['key'] for e in s['municipality_revisions']});complete=sum('revision_after_complete' in e['event'] for e in s['municipality_revisions'])
    add(f"Vollständige Historie: {len(s['municipality_revisions'])} Änderungen in {distinct} Gemeinden bei gleicher Zahl gemeldeter Bezirke; {complete} nach vollständiger Meldung. Seit dem Bericht um 00:00 kommen fünf hinzu. Alle Parteien werden im Änderungs-Audit geprüft.",['09_summenrevisionen','16_revisionen_nach_mitternacht'],['summary.json','post_midnight_revisions.json'])
    add('Bitterfeld-Wolfen, 00:23 MESZ: Erststimmen Linke +50, GRÜNE −28, FREIE WÄHLER −19, AfD −2, CDU −1. Summe unverändert, 31/31 Bezirke gemeldet. Ein späterer Gemeinde-Diff; Bezirk 000028 ist damit nicht als Ursache identifiziert.',['16_revisionen_nach_mitternacht'],['post_midnight_revisions.json'])
    add('Weitere spätere Änderungen: Harzgerode −21 Wählende, Tangermünde −24. Leuna: Linke −9 und GRÜNE +8 Zweitstimmen, gültige Summe −1. Querfurt: PdF +1, Volt −1 Zweitstimme. Ursachen sind nicht dokumentiert.',['16_revisionen_nach_mitternacht'],['post_midnight_revisions.json','raw_candidate_events.csv'])
    add('Aken (Elbe) / 000010 erscheint neu: 00:45:41 MESZ noch nicht aufgeführt, um 00:51:10 bereits gemeldet. Das Soll steigt 2.660 → 2.661. Aken 9 → 10, Wahlkreis Zerbst 79 → 80, Anhalt-Bitterfeld 204 → 205. Keine Identität entfernt.',['14_aken_neuer_wahlbezirk'],['AKEN_000010.md','district_identity_changes.json'])
    add(f"Prüfung: {n(s['retained_sources_verified'])} Quellen per SHA-256 bestätigt; keine Fehler innerhalb der Zeilensummen und keine Abweichung zwischen Export und Quell-CSV. {s['aggregation_nonzero']} geografische Feldabweichungen betreffen dieselbe Aschersleben-Lücke in drei Ständen.",['15_aggregationsdifferenz'],['aggregation_nonzero.csv','source_manifest.json','summary.json'])
    add(f"HTML und CSV sind nicht immer synchron: {s['overview_csv_mismatching_captures']} von {s['status_observations']} gemeinsamen Abrufen unterscheiden sich beim Meldestand. Zuletzt zeigt HTML keinen offenen Bezirk. Für die Stimmen bleibt der dokumentierte Unterschied zwischen den CSV-Ebenen.",['10_quellenversatz','07_offene_meldungen'],['versions.csv','aggregation_gap.json'])
    add('Grenzen: Für 0 Bezirke liegen Einzelstimmen vor; die Statushistorie beginnt um 19:05. Drei frühere Nullvorlagen und zwei Einrichtungsschritte sind inventarisiert. Änderungen zwischen Abrufen und ausgleichende Korrekturen können unsichtbar bleiben.',[],['history_inventory.csv','summary.json'])
    add('Die Erfassung ist gestoppt; es ist kein weiterer Abruf geplant. Offen bleiben amtliche Erklärungen zur Rücknahme, zu Korrekturen, zum Aken-Eintrag und zur Aggregationslücke. Das Archiv belegt Datenänderungen, nicht deren Ursache oder Wahlmanipulation.',[],['polling_stopped.json','METHODS.md'])
    for i,t in enumerate(tweets,1):
        t['text']=f'{i}/{len(tweets)} '+t['text']
        t['weighted_characters']=sum(1 if ord(c)<=0x10ff or 0x2000<=ord(c)<=0x200d or 0x2010<=ord(c)<=0x201f or 0x2032<=ord(c)<=0x2037 else 2 for c in t['text'])
        assert t['weighted_characters']<=280, (i,t['weighted_characters'],t['text'])
        t['alt_text']=[next(c['alt_text'] for c in r.chart_map if c['path']==image) for image in t['images']]
    write_json(path/'tweets.json',tweets);(path/'tweets.txt').write_text('\n\n'.join(t['text'] for t in tweets)+'\n')
    write_json(path/'chart_map.json',r.chart_map)
    original=(path/'REPORT.md').read_text()
    ledger=original.split('## Vollständige Liste: Revisionen bei gleichem Meldestand',1)[1].split('## Nächster Lauf und offene Fragen',1)[0]
    report=['# Sachsen-Anhalt 2026: Auswertung der vollständigen Git-Historie','','## Executive Summary','',
            f"- **Der gesamte verfügbare LSA-Datenverlauf wurde neu ausgewertet:** {s['history_commit_count']} Daten-Commits, {s['versions']} datierte Abrufe einschließlich {s['preopening_captures']} Nullvorlagen, dazu {s['setup_commit_count']} Einrichtungsschritte ohne Abrufzeit. Ende: **07.09.2026, {tm(s['last_capture']['acquired_at_local'])} MESZ**. Polling ist gestoppt.",
            f"- **Neue Befunde gegenüber dem Mitternachtsbericht:** fünf weitere Gemeinderevisionen; insgesamt **20 Änderungen in 19 Gemeinden**, davon 17 nach vollständiger Meldung. Die eine Statusrücknahme in Bitterfeld-Wolfen bleibt bestehen; Aken 000010 erhöht das Soll um eins.",
            '- **Die letzten Quellen sind nicht vollständig konsistent:** HTML und Gemeinde-CSV melden 2.661/2.661, die Landes-CSV 2.660/2.661. Die Gemeindesumme enthält **1.513 gültige Zweitstimmen mehr**; alle Differenzen entsprechen dem neuen Aschersleben-Zuwachs.',
            '- **Keine Rechenfehler innerhalb der geprüften Zeilen, aber eine Aggregationslücke:** 162 nicht-null Feldvergleiche wiederholen dieselbe Lücke in drei Ständen. Diese Beobachtungen belegen weder ihre Ursachen noch Wahlmanipulation.','',
            f"Git-Endpunkt: `{s['ref']}`. Alle Zeitangaben sind deutsche Ortszeit **MESZ (UTC+2)**. Dies ersetzt den bisherigen Bericht inhaltlich durch eine vollständige Neuberechnung, ist aber kein amtliches Endergebnis. Die Parteiauswahl >5 % und Landesanteile beziehen sich auf die amtliche Landes-CSV; eigene Gemeindesummen sind gesondert bezeichnet.",'',
            '## Tweet-Serie mit Bildern','', 'Jeder nummerierte Absatz ist ein separater Entwurf mit maximal 280 gewichteten Zeichen. Belege, Bildtexte und ergänzende Erklärungen gehören zur Berichtskopie.','']
    for i,t in enumerate(tweets,1):
        report += [f'### Tweet {i}','',t['text'],'']
        for image,alt in zip(t['images'],t['alt_text']):report += [f'![{alt}]({image})','']
        report += ['Belege: '+', '.join(f'[{e}]({e})' for e in t['evidence'])+'.','']
    report += ['## CSV-Aggregation: Aschersleben wird noch nicht vollständig fortgeschrieben','',
               'Die höheren CSV-Aggregate stimmen untereinander überein; die Gemeinde-CSV ist im letzten Übergang weiter. Seit 03:04 liegt ihre Summe um 1 gemeldeten Bezirk, 1.517 Wählende, 1.509 gültige Erst- und 1.513 gültige Zweitstimmen über dem Landeswert. Dieselben 27 Felddifferenzen erscheinen Gemeinde → Salzlandkreis und Gemeinde → Land, in drei gespeicherten Ständen. Eine wiederholte Summenfortschreibung ist kein unabhängiger Vorfall.','',
               '[Alle Parteien und Vorher-/Nachher-Summen](AGGREGATION_ASCHERSLEBEN.md). Die mathematische Übereinstimmung mit Ascherslebens neuem Gemeindezuwachs ist belegt. Ein späterer amtlicher Abgleich kann die Ursache klären; das gestoppte Archiv enthält ihn nicht.','',
               '## Neuer Bezirkseintrag: Aken 000010','',
               'Die vollständige Identität kommt einmal hinzu; kein Bezirkseintrag verschwindet. Der Nenner steigt von 2.660 auf 2.661. Die Zahl 2.660 im älteren Bericht war für dessen Zeitpunkt korrekt. Aken steigt gleichzeitig um 435 gültige Zweitstimmen; dieser Zuwachs wird damals über alle höheren Ebenen konsistent fortgeschrieben.','',
               '[Genaue Abrufzeiten, Identitäten und Summen](AKEN_000010.md). Der Grund für das zusätzliche Soll ist nicht dokumentiert.','',
               '## Vollständige Liste: Revisionen bei gleichem Meldestand'+ledger,
               '## Umfang und Prüfgrenzen','',
               'Die Historie umfasst alle 123 Änderungen des LSA-Latest-Datenverzeichnisses im erreichbaren Git-Verlauf bis zum festgehaltenen Endpunkt. Die vollständige und die First-Parent-Aufzählung enthalten dieselben Daten-Commits. Von 121 datierten Abrufen liegen drei vor Öffnung der Auszählung; sie enthalten keine Stimmen. Die Zeitdiagramme konzentrieren sich auf die 118 Abrufe ab 18:00, der Daten-Audit enthält auch die frühen Vorlagen. Zwei Einrichtungsschritte ohne Abrufzeit sind im Inventar ausgewiesen.','',
               f"Bei {len(s['captures_without_raw_sources'])} früher Nullvorlage fehlt die ursprüngliche Quell-CSV; der normalisierte Export ist im Git vorhanden und geprüft. Alle {s['retained_sources_verified']} erhaltenen Quellobjekte der übrigen Abrufe wurden gegen ihre gespeicherten SHA-256 geprüft. Die Statushistorie beginnt erst um 19:05; Einzelstimmen je Wahlbezirk fehlen vollständig.",'',
               'Gleicher Meldestand ist nur ein Änderungsdetektor. Korrekturen können gleichzeitig mit neuen Meldungen auftreten oder sich zwischen Abrufen ausgleichen. Die Aken-Sollerhöhung zählt daher gesondert und nicht als eine der 20 Revisionen bei festem Meldestand. Regionale Unterschiede und vollständige Arithmetik sind für sich weder Beleg noch Ausschluss sachlicher Wahlfehler.','',
               '## Erfassung beendet und offene Fragen','',
               'Die Codex-Automation ist pausiert; der GitHub-Workflow „Archive Sachsen-Anhalt 2026“ ist manuell deaktiviert. Bei der Abschlusskontrolle war kein Sammellauf mehr aktiv oder vorgemerkt. Es ist kein weiterer Abruf geplant.','',
               'Die offenen Fragen betreffen die amtliche Begründung der Statusrücknahme, die dokumentierten Gemeindekorrekturen, die Aufnahme von Aken 000010 und den Abgleich der Aschersleben-Summen. Für eine spätere weitere Datenerfassung ist eine neue Anweisung nötig.','',
               '[Methoden und Reproduktion](METHODS.md) · [Prüfurteil](VALIDATION.md) · [Git-Inventar](history_inventory.csv) · [Alle Gebiets-/Parteiergebnisse](results_by_area.csv) · [Bitterfeld-Detaildiff](BITTERFELD_WOLFEN_000028.md) · [Erfassung gestoppt](polling_stopped.json)','']
    if baseline and (path/'comparison.json').exists():
        c=json.loads((path/'comparison.json').read_text())
        # Pre-opening observations newly included in a full-history run are not new nightly captures.
        c['new_preopening_captures_in_scope']=s['preopening_captures']
        c['new_election_night_captures']=c['new_captures']-s['preopening_captures']
        c['new_status_identities']=identity['additions']
        c['latest_aggregation_gap']=gap['latest_land_deltas']
        write_json(path/'comparison.json',c)
        report += ['## Vergleich zum ursprünglichen Bericht','',
                   f"Gegenüber 00:00:20 MESZ wurden {c['new_election_night_captures']} spätere Abrufe und zusätzlich drei frühere Nullvorlagen einbezogen. {len(c['new_fixed_reporting_municipality_revisions'])} neue Revisionen bei festem Meldestand, ein zusätzliches Soll und die neue Aggregationslücke sind in der Neuberechnung enthalten. Alle 46 zuvor offenen HTML-Statusmeldungen sind nun geschlossen.",'',
                   '[Vollständiger Vergleich einschließlich Parteistimmen und Anteilsänderungen](comparison.json)','']
    (path/'REPORT.md').write_text('\n'.join(report))
    methods=(path/'METHODS.md').read_text()
    methods=methods.replace('Frozen Sachsen-Anhalt 2026 election-night observations, starting 6 September at\n18:00 Europe/Berlin.', 'All available Sachsen-Anhalt data commits on the archived default-branch history, including pre-election zero templates and an inventory of setup commits. Time-series images show the election-night window from 6 September 18:00 Europe/Berlin.')
    methods=methods.replace(f'--ref {s["ref"]}\n',f'--ref {s["ref"]} --full-history\n')
    methods=methods.replace(f"- {s['versions']} post-18:00 captures;",f"- {s['versions']} dated captures ({s['election_night_captures']} after 18:00, {s['preopening_captures']} pre-opening templates);")
    methods=methods.replace('Pre-opening templates are excluded.', 'Pre-opening zero templates are included in the data audit and explicitly separated from election-night charts.')
    methods += '''\n## Full-history source reconciliation and stopped collection\n\nThe final source files disagree across geographic CSV levels. The verifier must reproduce the nonzero residuals; it must not edit or exclude them to obtain zero. `aggregation_gap.json` matches the entire latest 27-field residual vector to the new Aschersleben municipality row transition. Its 1,513 second-vote delta reconciles across all parties; repeated appearances under Land and Kreis and across three captures are not independent incidents. The official Land denominator remains the headline party denominator. Municipality sums are explicitly labelled computed aggregates. `district_identity_changes.json` identifies every new, removed or reappearing status identity and compares the Aken transition directly. `post_midnight_revisions.json` preserves the five additional fixed-count revisions.\n\nPolling was stopped at the user's request: the Codex heartbeat is paused and the GitHub archive workflow is disabled. `polling_stopped.json` records the control-state verification. No scheduled rerun is planned. Reproduction reads the fixed Git endpoint and never resumes collection. A future complete-source audit requires already archived consistent data or new authorization to collect it.\n\nThe full-history renderer is `scripts/lsa_full_history_report.py`, called automatically when the analyzer's `--full-history` flag is present. For the comparison section, pass `--baseline data/2026-lsa/reports/git-timeline/034045f3` to the renderer. Keep `polling_stopped.json` with the output as external control-state evidence; it does not change numerical analysis.\n'''
    (path/'METHODS.md').write_text(methods)
    env=json.loads((path/'environment.json').read_text());env['full_history_renderer_sha256']=hashlib.sha256(__import__('pathlib').Path(__file__).read_bytes()).hexdigest();write_json(path/'environment.json',env)
    bf=next(e for e in late if e['name']=='Bitterfeld-Wolfen, Stadt')
    with (path/'BITTERFELD_WOLFEN_000028.md').open('a') as f:
        f.write('\n\n## Spätere Gemeinderevision um 00:23 MESZ\n\n'+changes(bf)+'. Die Summe der Erststimmen bleibt unverändert; 31/31 Bezirke sind gemeldet. Dieser später beobachtete Gemeinde-Diff kann Bezirk 000028 nicht zugeordnet werden. [Vollständige Vorher-/Nachher-Werte](post_midnight_revisions.json).\n')
