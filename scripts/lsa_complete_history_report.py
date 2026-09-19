"""Integrated report for a complete, reconciled final archived LSA snapshot."""
from __future__ import annotations
import csv, gzip, hashlib, json, platform, statistics, textwrap
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import numpy as np
from analyze_lsa_git_timeline import aggregation_checks, numeric_values, write_csv, write_json
from lsa_full_history_report import derive as derive_historical, tm, n

BLUE, INK, MUTED, ORANGE = '#3478a5', '#222a35', '#66717e', '#cc783c'

def j(path):return json.loads(path.read_text())
def rows(path):
    with path.open() as f:return list(csv.DictReader(f))

def derive(r):
    path,s=r.path,r.s
    residuals=rows(path/'aggregation_nonzero.csv')
    last_gap=residuals[-1]['commit']
    old={}
    with gzip.open(path/'raw_timeline.jsonl.gz','rt') as f:
        for line in f:
            a=json.loads(line)
            if a['commit']==last_gap and a['mode']=='TOTAL':old[a['area_key']]={**a,'key':a['area_key']}
    old_s={**s,'last_capture':r.version_by_commit[last_gap]}
    identity,gap,late=derive_historical(SimpleNamespace(s=old_s,path=path,latest=old,version_by_commit=r.version_by_commit))
    gap['first_reconciled_capture']=s['last_capture']
    gap['final_aggregation_nonzero']=s['reporting_evidence']['final_aggregation_nonzero']
    gap['interpretation']='Historic residuals persist in three captures, then are zero at the first subsequent Git data capture. Publication time between captures is unknown.'
    final_rows=j(path/'latest_official_rows.json');cases={}
    arrivals={'Aschersleben':gap['matching_municipality_arrival'], 'Aken':next(e for e in s['denominator_changes'] if e['level']=='GEMEINDE')}
    for name,ags,number in [('Aschersleben','15089015','000965'),('Aken','15082005','000010'),('Bitterfeld-Wolfen','15082015','000028')]:
        found=[a for a in final_rows.values() if a['level']=='WAHLBEZIRK' and a['area_key'].split(':')[2]==ags and a['number']==number]
        assert len(found)==1
        booth=found[0];v=numeric_values(booth);arrival=arrivals.get(name)
        vector={k:d['delta'] for k,d in arrival['changes'].items() if k not in ['reported_precincts','total_precincts']} if arrival else None
        discrepancies={k:{'municipal_arrival':d,'final_booth':v.get(k)} for k,d in vector.items() if d!=v.get(k)} if vector else None
        cases[name]={'row':booth,'first_individual_vote_capture':s['last_capture'],'historical_municipal_arrival':arrival,'arrival_vector_matches_final_booth':not discrepancies if vector else None,'arrival_vs_final_discrepancies':discrepancies,'historical_individual_vote_diff_available':False}
    checks=aggregation_checks(r.latest)
    # Match the published status identities with the final original vote rows.
    statuses={}
    with gzip.open(path/'status_timeline.jsonl.gz','rt') as f:
        for line in f:
            a=json.loads(line)
            if a['commit']==s['status_last_capture']['commit']:statuses[(int(a['Wahlkreis'].split(',')[0]),a['Gemeinde'],a['wbz'])]=a
    vote_ids={}
    for a in final_rows.values():
        if a['level']=='WAHLBEZIRK':vote_ids[(int(a['area_key'].split(':')[3]),a['name'].rsplit(' / ',1)[0],a['number'])]=a
    reconciliation={'by_relation':{},'status_vote_identity_matches':len(statuses.keys() & vote_ids.keys()),'status_only':sorted(statuses.keys()-vote_ids.keys()),'votes_only':sorted(vote_ids.keys()-statuses.keys())}
    for relation in sorted({a['relation'] for a in checks}):
        subset=[a for a in checks if a['relation']==relation]
        reconciliation['by_relation'][relation]={'parents':len({a['parent'] for a in subset}),'comparisons':sum(a['delta'] is not None for a in subset),'nonzero':sum(a['delta'] not in (0,None) for a in subset),'unknown_counter_comparisons':sum(a['delta'] is None for a in subset)}
    assert not reconciliation['status_only'] and not reconciliation['votes_only']
    return identity,gap,late,cases,reconciliation

def change_text(e,names):
    labels={'voters_total':'Wählende','valid_votes_erst':'gültige Erststimmen','valid_votes_zweit':'gültige Zweitstimmen'}
    return '; '.join(f"{names[f[6:]]+' ('+('E' if f[6]=='D' else 'Z')+')' if f.startswith('party:') else labels.get(f,f)} {d['delta']:+}" for f,d in e['changes'].items())

def render(r,baseline):
    p,s=r.path,r.s
    identity,gap,late,cases,reconciliation=derive(r)
    for fn,data in [('district_identity_changes',identity),('aggregation_gap',gap),('post_midnight_revisions',late),('final_precinct_cases',cases),('final_reconciliation',reconciliation)]:write_json(p/(fn+'.json'),data)
    write_csv(p/'land_vs_municipality_parties.csv',gap['party_comparison'])
    final_checks=aggregation_checks(r.latest);write_csv(p/'final_aggregation_checks.csv',final_checks)
    names=r.latest['lsa:LAND:15']['party_names'];ext=j(p/'external_comparison.json')
    # Reuse the Aken identity view while adding the newly published individual result.
    a=identity['aken'];ev=a['event']
    fig=r.figure('Ein neuer Eintrag erhöht das Soll um eins',f"Aken 000010 · {tm(ev['previous_acquired_at_local'])} → {tm(ev['acquired_at_local'])} MESZ · bereits gemeldet")
    for y,g in zip([.70,.56,.42,.28],a['groups']):
        fig.text(.07,y,g['area'],fontsize=19);fig.text(.71,y,f"{n(g['before'])} → {n(g['after'])}",fontsize=25,color=BLUE,ha='center')
    fig.text(.07,.13,'Gemeindezuwachs damals: +435 Zweitstimmen. Finaler Bezirk 000010: 1.139. Beides ist getrennt zu lesen.',fontsize=11,color=MUTED)
    r.save(fig,'14_aken_neuer_wahlbezirk','What changed when Aken appeared?','before/after table',['status identity','expected count','municipal arrival','final booth'],'district_identity_changes.json; final_precinct_cases.json','One identity raises four overlapping totals. Its final votes are not the municipal arrival delta.')
    fig=r.figure('Die Aschersleben-Lücke ist geschlossen','Gemeindesumme minus Landes-CSV · gültige Zweitstimmen · archivierte Beobachtungen')
    versions=rows(p/'versions.csv');before=gap['matching_municipality_arrival']['previous_commit'];selected=[v for v in versions if v['commit'] in [before,*gap['observed_commits'],s['last_capture']['commit']]]
    dates=[datetime.fromisoformat(v['acquired_at_local']) for v in selected];values=[1513 if v['commit'] in gap['observed_commits'] else 0 for v in selected]
    ax=fig.add_axes([.10,.23,.83,.55]);ax.step(dates[:-1],values[:-1],where='post',color=BLUE,lw=2.5);ax.axvspan(dates[-2],dates[-1],color='#e8edf1',alpha=.65);ax.text(dates[-2]+(dates[-1]-dates[-2])/2,750,'Kein Git-Datenabruf\nZeitpunkt des Abgleichs unbekannt',ha='center',fontsize=12,color=MUTED);ax.scatter(dates,values,color=BLUE,s=50);ax.set_ylim(-120,1900);ax.set_ylabel('Differenz gültiger Zweitstimmen');r.time_axis(ax);ax.grid(axis='y',alpha=.15)
    ax.annotate('+1.513 ab 03:04', (dates[1],1513),xytext=(5,25),textcoords='offset points',fontsize=14)
    ax.annotate('0 um 04:06', (dates[-1],0),xytext=(-110,35),textcoords='offset points',fontsize=14)
    fig.text(.10,.115,'000965 hat final genau 1.513 Zweitstimmen. Alle 15 Parteien und beide Stimmensummen passen exakt.',fontsize=11,color=MUTED)
    r.save(fig,'15_aggregationsdifferenz','When was the historic gap observed and resolved?','step timeline',['capture','municipality minus Land'],'aggregation_gap.json; final_precinct_cases.json','Three nonzero captures; last at 03:10. No captures between 03:10 and 04:06, so closure is interval censored.')
    fig=r.figure('Fünf Gemeinderevisionen nach Mitternacht','Unveränderte Zahl gemeldeter Bezirke · E = Erststimmen, Z = Zweitstimmen',height=10)
    for i,e in enumerate(late):
        fig.text(.06,.76-i*.12,tm(e['acquired_at_local'])+' · '+e['name'].replace(', Stadt',''),fontsize=12.5,weight='bold',va='top')
        fig.text(.38,.76-i*.12,textwrap.fill(change_text(e,names),width=70),fontsize=12,va='top',linespacing=1.5)
    r.save(fig,'16_revisionen_nach_mitternacht','What changed after the initial report?','revision table',['municipality','time','vote deltas'],'post_midnight_revisions.json','All five post-baseline fixed-count revisions, including unchanged-total party redistribution.')
    fig=r.figure('Vier unabhängige Summen ergeben denselben Landeswert','Gebietsebenen einzeln summiert · 1.315.315 gültige Zweitstimmen · keine Ebenen zusammengezählt')
    for y,level,label in zip([.70,.56,.42,.28],['KREIS','WAHLKREIS','GEMEINDE','WAHLBEZIRK'],['14 Kreise / kreisfreie Städte','41 Wahlkreise','218 Gemeinden','2.661 Wahlbezirke']):
        total=sum(x['valid_votes_zweit'] for x in r.latest.values() if x['level']==level)
        fig.text(.07,y,label,fontsize=19);fig.text(.70,y,n(total),fontsize=26,weight='bold',color=BLUE)
    fig.text(.07,.13,'Auch Wählende, gültige Erststimmen und sämtliche 31 Partei-/Kandidatenfelder stimmen überein.',fontsize=11,color=MUTED)
    r.save(fig,'17_finaler_aggregatabgleich','Do final geographic vote sums reconcile?','comparison table',['geographic level','valid second votes'],'final_aggregation_checks.csv','Land independently compared with each level; every WBZ-to-municipality and WBZ-to-constituency group is also checked.')
    fig=r.figure('Parteianteile in allen 2.661 Wahlbezirken','Jeder Bezirk zählt einmal · alphabetische Parteifolge · Rauten: Landesanteile',height=9)
    booths=[x for x in r.latest.values() if x['level']=='WAHLBEZIRK'];selected=sorted(r.selected,key=lambda x:x['party'])
    ax=fig.add_axes([.16,.20,.78,.57]);arrays=[[r.percent(x,a['code']) for x in booths if x['valid_votes_zweit']] for a in selected]
    boxes=ax.boxplot(arrays,vert=False,tick_labels=[x['party'] for x in selected],patch_artist=True,showfliers=True,flierprops={'marker':'.','markersize':3},medianprops={'color':INK})
    for b in boxes['boxes']:b.set(facecolor='#d8e6ef',edgecolor=BLUE)
    ax.scatter([x['share_percent'] for x in selected],range(1,7),marker='D',color='#c62828',zorder=4);ax.invert_yaxis();ax.set_xlim(0,100);ax.set_xlabel('Anteil an gültigen Zweitstimmen des Bezirks (%)');ax.grid(axis='x',alpha=.12)
    fig.text(.16,.10,'Box: mittlere 50 %; Medianlinie; Whisker: 1,5 × IQR. Punkte außerhalb sind keine Fehlerdiagnose.',fontsize=10.5,color=MUTED)
    r.save(fig,'18_wahlbezirke_streuung','How do all individual precinct vote shares vary?','box plot',['party','booth share'],'results_by_area.csv','Equal booth weight, valid-vote denominator per booth, 0–100% scale retains every observation.')
    # Screening comparison: independently regenerated flags, not copied graphics.
    flags=rows(p/'external_mad_flags.csv');fields=sorted({x['field'] for x in flags})
    fig=r.figure('Die 118 statistischen Hinweise sind reproduzierbar','GFrei.News-Schwellen nachgerechnet · 106 verschiedene Gebiete · keine 118 festgestellten Fehler')
    ax=fig.add_axes([.25,.22,.66,.55]);orange=[sum(x['field']==f and x['recomputed_level']=='orange' for x in flags) for f in fields];red=[sum(x['field']==f and x['recomputed_level']=='red' for x in flags) for f in fields]
    ax.barh(fields,orange,color='#b1c9da',label='Schwelle |z| ≥ 6 + Abstand ≥ 2 pp');ax.barh(fields,red,left=orange,color=BLUE,hatch='///',label='Schwelle |z| ≥ 10 + Abstand ≥ 5 pp')
    for i,(x,y) in enumerate(zip(orange,red)):ax.text(x+y+1,i,f'{x+y} ({x} + {y})',va='center')
    ax.invert_yaxis();ax.set_xlim(0,max(a+b for a,b in zip(orange,red))*1.25);ax.set_xlabel('Hinweise auf einzelne Merkmale (Anzahl)');ax.legend(frameon=False,loc='lower right',fontsize=9);ax.grid(axis='x',alpha=.12);ax.set_axisbelow(True)
    fig.text(.07,.12,'91 Hinweise betreffen GRÜNE. Vergleichsgruppen berücksichtigen Wahlart und Größe, keine Stadt-/Ortsstruktur.',fontsize=10.5,color=MUTED)
    r.save(fig,'19_statistische_hinweise','Which external screening signals can be independently reproduced?','stacked bar',['feature','robust z','screening threshold'],'external_mad_recalculation.csv','All 24,171 checked MAD values reproduce to numerical tolerance. Flags are descriptive; no calibrated fraud probabilities.')
    # Contextualize the largest green-party signals with an actual local peer median.
    top=sorted([x for x in flags if x['field']=='GRÜNE'],key=lambda x:abs(float(x['z'])),reverse=True)[:12]
    raw=r.raw;context=[]
    for x in top:
        parts=x['row'].split(':');ags=parts[3];mode=parts[-1]
        peers=[a for a in raw.values() if a['level']=='WAHLBEZIRK' and a['area_key'].split(':')[2]==ags and a['mode']==mode and 100<=a['voters_total']<1000]
        context.append({**x,'local_peer_n':len(peers),'local_peer_median':statistics.median(r.percent(a,'F6') for a in peers)})
    write_csv(p/'screening_local_context.csv',context)
    fig=r.figure('Hohe GRÜNE-Anteile im örtlichen Vergleich','Zwölf größte robuste GRÜNE-Abstände · jeweilige Gemeinde, Wahlart und Größenklasse ergänzen den Vergleich',height=10)
    ax=fig.add_axes([.34,.21,.60,.52]);yy=range(len(context));obs=[float(x['observed']) for x in context];broad=[float(x['median']) for x in context];local=[x['local_peer_median'] for x in context]
    for i,(b,l,o) in enumerate(zip(broad,local,obs)):ax.plot([b,o],[i,i],color='#c5cbd0',lw=1)
    ax.scatter(broad,yy,marker='x',color=MUTED,label='Landesweite Größen-/Wahlartgruppe');ax.scatter(local,yy,marker='s',facecolors='white',edgecolors=ORANGE,label='Median in derselben Gemeinde');ax.scatter(obs,yy,color=BLUE,label='Beobachteter Bezirk')
    ax.set_yticks(list(yy),[x['name'].replace(', Landeshauptstadt','').replace(', Stadt','') for x in context],fontsize=10);ax.invert_yaxis();ax.set_xlim(0,60);ax.set_xlabel('GRÜNE-Zweitstimmenanteil (%)');ax.grid(axis='x',alpha=.12);ax.legend(frameon=False,fontsize=9,loc='lower left',bbox_to_anchor=(0,1.01))
    fig.text(.07,.10,'Lokaler Median ist zusätzliche Beschreibung, kein Gegenbeweis. Beide Vergleiche enthalten den jeweiligen Bezirk.',fontsize=10.5,color=MUTED)
    r.save(fig,'20_hinweise_ortsstruktur','How do the largest screening signals compare with their local context?','paired dot',['booth share','state peer median','municipality peer median'],'screening_local_context.csv','Context comparison, not a new significance test. Identical mode and voter-size class for local peers; no causal inference.')
    fig=r.figure('Zwei Analysen: gleiche Werte, andere Zähleinheiten','Unsere Git-Historie und Lauras Wahlforensik · versionierter Datenvergleich')
    labels=[('Originalwerte','135.837 Vergleiche · 0 Unterschiede'),('Robuste Statistik','118 Merkmals-Hinweise in 106 Gebieten'),('Historische Revisionen','86 Feld-Hinweise · alle exakt im Git belegt'),('Unser Gemeinde-Audit','20 Änderungen in 19 Gemeinden')]
    for y,(a,b) in zip([.70,.55,.40,.25],labels):fig.text(.06,y,a,fontsize=16,weight='bold');fig.text(.38,y,b,fontsize=17,color=BLUE)
    fig.text(.06,.12,'86 zählt auch Parteien, Wahlarten und höhere Aggregate einzeln. 20 zählt Gemeinde-Übergänge bei festem Meldestand.',fontsize=10.5,color=MUTED)
    r.save(fig,'21_vergleich_analysen','Why do the two analyses report different counts?','comparison table',['numeric checks','flags','municipal transitions'],'external_comparison.json; summary.json','Different units and sampling times; do not add these counts or interpret them as independent incidents.')
    # Export every final party, including candidates below the chart threshold.
    all_results=[]
    for a in r.latest.values():
        for code,value in a['parties'].items():
            denom=a['valid_votes_erst' if code.startswith('D') else 'valid_votes_zweit']
            all_results.append({'key':a['key'],'level':a['level'],'name':a['name'],'party_code':code,'party':a['party_names'][code],'votes':value,'valid_votes':denom,'share_percent':100*value/denom if value is not None and denom else None})
    write_csv(p/'all_party_results_by_area.csv',all_results)
    # Detailed cases retain the historical attribution limit and add final values.
    party_case=[]
    for name,c in cases.items():
        a=c['row']
        for code,value in a['parties'].items():
            party_case.append({'case':name,'precinct':a['number'],'mode':a['mode'],'party':a['party_names'][code],'vote_type':'Erststimmen' if code.startswith('D') else 'Zweitstimmen','votes':value})
    write_csv(p/'final_case_party_votes.csv',party_case)
    for name,filename in [('Aschersleben','AGGREGATION_ASCHERSLEBEN.md'),('Aken','AKEN_000010.md')]:
        c=cases[name];a=c['row'];lines=[f'# {name}, Wahlbezirk {a["number"]}','','Vorläufiger Einzelbezirkstand erstmals im Git-Abruf 07.09.2026, 04:06:37,856 MESZ gespeichert. Der tatsächliche Veröffentlichungszeitpunkt liegt davor.','']
        if name=='Aschersleben':lines+=['**Die historische Aggregationslücke ist geschlossen.** Vom Abruf 03:04 bis einschließlich 03:10 enthielt die Gemeindesumme 1.513 Zweitstimmen mehr als die Landes-CSV. Um 04:06 sind alle geprüften Summen gleich. Die nun vorliegenden 1.517 Wählenden, 1.509 gültigen Erst- und 1.513 gültigen Zweitstimmen des Briefwahlbezirks 000965 stimmen mit dem früheren Zuwachs exakt überein – einschließlich aller Parteien.','', 'Die 162 historischen Felddifferenzen bestehen aus 27 Feldern × zwei überlappenden Beziehungen × drei Abrufen. Sie werden im Audit beibehalten. Das Intervall zwischen 03:10 und 04:06 enthält keinen Git-Datenabruf; der genaue Zeitpunkt des Abgleichs ist nicht bestimmt.','']
        else:lines+=['**Erste Identität 000010 um 00:51:10 MESZ, vorher um 00:45:41 nicht vorhanden.** Aken 9→10, Zerbst 79→80, Anhalt-Bitterfeld 204→205, Land 2.660→2.661. Exakte HTML-Abrufe: '+identity['aken']['previous_html_fetch_utc']+' und '+identity['aken']['first_html_fetch_utc']+' (UTC; jeweils +2 Stunden für MESZ).','', '**Der damalige Gemeindezuwachs von 435 Zweitstimmen ist nicht das jetzt verfügbare Bezirksergebnis von 1.139.** Die finalen Werte des Briefwahlbezirks sind 1.150 Wählende und 1.129 gültige Erststimmen. Der vollständige Bezirksvektor war damals nicht archiviert; mögliche Umverteilungen zwischen Bezirken oder frühere Zuordnungen sind daraus nicht rekonstruierbar. Die Daten belegen einen neu sichtbaren Eintrag, nicht die Einrichtung einer zusätzlichen physischen Wahlstelle.','']
        lines+=['| Partei | Erststimmen | Zweitstimmen |','|---|---:|---:|']
        for code in sorted((c for c in a['parties'] if c.startswith('F')),key=lambda c:a['party_names'][c]):
            first=a['parties'].get('D'+code[1:]);lines.append(f"| {a['party_names'][code]} | {n(first) if first is not None else '—'} | {n(a['parties'][code])} |")
        lines+=['','— bezeichnet eine nicht besetzte/fehlende Kandidatenspalte, keine erfundene Null.','', '[Strukturierter Vorher-/Nachher-Vergleich](final_precinct_cases.json) · [Alle Parteien als CSV](final_case_party_votes.csv) · [Gesamtbericht](REPORT.md)']
        (p/filename).write_text('\n'.join(lines)+'\n')
    bf=p/'BITTERFELD_WOLFEN_000028.md';text=bf.read_text().replace('Die Stimmenexporte reichen hier bis zur Gemeinde.','Die Stimmenexporte der beiden historischen Abrufe reichen hier bis zur Gemeinde.')
    a=cases['Bitterfeld-Wolfen']['row'];text+='\n\n## Neu: tatsächlicher Einzelbezirkstand um 04:06 MESZ\n\nBriefwahlbezirk 000028: **1.028 Wählende, 1.021 gültige Erststimmen, 1.024 gültige Zweitstimmen.** Dieser eine späte Einzelbezirkstand liefert keinen Parteidiff zwischen 19:35 und 22:11 nach.\n\n| Partei | Erststimmen | Zweitstimmen |\n|---|---:|---:|\n'
    for code in sorted((x for x in a['parties'] if x.startswith('F')),key=lambda x:a['party_names'][x]):
        first=a['parties'].get('D'+code[1:]);text+=f"| {a['party_names'][code]} | {n(first) if first is not None else '—'} | {n(a['parties'][code])} |\n"
    text+='\nSpätere Gemeindeänderung um 00:23 MESZ: '+change_text(next(e for e in late if 'Bitterfeld' in e['name']),names)+'. Keine Zuordnung zum einzelnen Bezirk möglich.\n';bf.write_text(text)
    from render_lsa_additional_charts import render as render_additional
    representation=render_additional(r)
    from render_cross_election_share_delta import render as render_cross_election
    cross_election=render_cross_election(r)
    tweets=[]
    def add(finding,text,charts,evidence):
        tweets.append({'finding':finding,'priority_rank':len(tweets)+1,'text':text,'images':[r.chart_files[c] for c in charts],'evidence':evidence})
    # Increasing editorial priority: context, source limitations, observed changes, final reconciliation.
    add('Frühe Parteianteile hängen von der Meldestichprobe ab',
        'Vom ersten positiven zum letzten Git-Stand sinkt der AfD-Anteil relativ: BW −21,7 %, RLP −16,8 %, SA −26,9 %. Keine Prozentpunkte. Die frühen Stichproben unterscheiden sich; der dokumentierte RLP-Parserfehler betrifft keine gezeigte Partei.',
        ['25_drei_wahlen_relativer_anteilsdelta'],['cross_election_share_delta.csv','cross_election_endpoints.json'])
    add('Unvollständige Teilstände liefern keine belastbare Beteiligungsquote',
        '152 Teilstände zeigen mehr Wählende als veröffentlichte Wahlberechtigte: 17 überlappende Gebiete, kein vollständiger Stand. Beispiel Balgstädt: Briefwahl trifft auf einen unvollständigen Urnen-Nenner. Der Quotient ist dann keine belastbare Beteiligungsquote.',
        [],['turnout_denominator_observations.json','METHODS.md'])
    add('Statistische Hinweise sind keine festgestellten Wahlfehler',
        'Die robuste MAD-Prüfung liefert 118 Merkmals-Hinweise in 106 Gebieten, davon 91 zu GRÜNE-Anteilen. Alle 24.171 prüfbaren MAD-Werte sind reproduziert. Die Vergleichsgruppen berücksichtigen keine Ortsstruktur; die Hinweise sind keine nachgewiesenen Wahlfehler.',
        ['19_statistische_hinweise','20_hinweise_ortsstruktur'],['external_mad_recalculation.csv','screening_local_context.csv'])
    add('Beide Analysen stimmen bei den geprüften Daten überein',
        'Lauras Wahlforensik und unser Archiv stimmen in 135.837 Zahlen überein. Alle 86 externen Revisionsfelder passen exakt zu Git-Übergängen. Unsere 20 Gemeinderevisionen zählen andere Einheiten; die Zahlen widersprechen sich nicht.',
        ['21_vergleich_analysen'],['EXTERNAL_COMPARISON.md','external_revision_comparison.json'])
    add('Entfernte Meldezähler sind ein Schemawechsel',
        'Die vorläufige Landes-CSV entfernt Ist/Soll-Meldezähler. Das ist ein Schemawechsel, kein Rückgang auf 0/0. HTML und Gemeinde-CSV zeigen 2.661/2.661; der neue Einzelbezirk-Export enthält genau dieselben 2.661 Identitäten.',
        ['07_offene_meldungen'],['source_schema_events.json','final_reconciliation.json'])
    add('Aken erklärt den Anstieg auf 2.661 Wahlbezirke',
        'Aken 000010 wird zwischen 00:45:41 und 00:51:10 MESZ neu sichtbar; das Soll steigt von 2.660 auf 2.661. Die damaligen +435 Zweitstimmen der Gemeinde sind nicht die finalen 1.139 dieses Bezirks. Seine frühere Stimmenzuordnung fehlt.',
        ['14_aken_neuer_wahlbezirk'],['AKEN_000010.md','district_identity_changes.json'])
    add('20 Gemeinderevisionen sind im Verlauf dokumentiert',
        '20 Änderungen bei gleicher Meldezahl in 19 Gemeinden, davon 17 nach Vollmeldung. Fünf nach Mitternacht: Bitterfeld-Wolfen, Harzgerode, Tangermünde, Leuna und Querfurt. Änderungen sind belegt; ihre Ursachen erklärt das Archiv nicht.',
        ['16_revisionen_nach_mitternacht'],['summary.json','post_midnight_revisions.json','raw_candidate_events.csv'])
    add('Bitterfeld 000028 verliert vorübergehend seinen Meldestatus',
        'Bitterfeld-Wolfen 000028: einzige beobachtete Statusrücknahme, ab 19:35 nicht gemeldet, ab 22:11 wieder gemeldet. Der späte Einzelstand enthält 1.024 Zweitstimmen. Der Parteidiff 19:35/22:11 ist nur für die Gemeinde verfügbar, nicht für diesen Bezirk.',
        ['08_status_ruecknahme','13_bitterfeld_gemeindediff'],['BITTERFELD_WOLFEN_000028.md','bitterfeld_party_diff.csv'])
    add('Die geschlossene Aschersleben-Lücke passt exakt zu Bezirk 000965',
        'Die Aschersleben-Lücke von 1.513 Zweitstimmen besteht in drei Abrufen 03:04–03:10 und ist um 04:06 geschlossen. Alle Parteiwerte passen zum jetzt einzeln belegten Briefwahlbezirk 000965. Der genaue Zeitpunkt des Abgleichs zwischen den Abrufen bleibt offen.',
        ['15_aggregationsdifferenz'],['AGGREGATION_ASCHERSLEBEN.md','aggregation_gap.json','final_precinct_cases.json'])
    add('Die abschließende Gesamtprüfung ergibt übereinstimmende Summen',
        'Gesamtbefund: Alle 2.661 Wahlbezirke ergeben exakt die Landes-, Kreis-, Wahlkreis- und Gemeindesummen: 1.315.315 gültige Zweitstimmen. Keine finalen Rechenabweichungen. Das erklärt historische Änderungen nicht und ist keine amtliche Endfeststellung.',
        ['17_finaler_aggregatabgleich'],['final_reconciliation.json','final_aggregation_checks.csv','VALIDATION.md'])
    for i,t in enumerate(tweets,1):
        t['text']=f'{i}/{len(tweets)} '+t['text'];t['weighted_characters']=sum(1 if ord(c)<=0x10ff or 0x2000<=ord(c)<=0x200d or 0x2010<=ord(c)<=0x201f or 0x2032<=ord(c)<=0x2037 else 2 for c in t['text']);assert t['weighted_characters']<=280,(i,t['weighted_characters'])
        t['alt_text']=[next(c['alt_text'] for c in r.chart_map if c['path']==x) for x in t['images']]
    write_json(p/'tweets.json',tweets);(p/'tweets.txt').write_text('\n\n'.join(t['text'] for t in tweets)+'\n');write_json(p/'chart_map.json',r.chart_map)
    comparison_text(p,ext)
    lines=['# Sachsen-Anhalt 2026: die zehn wichtigsten Befunde','',
           '**Zehn Tweets in steigender Priorität: 1 = ergänzender Kontext, 10 = wichtigste Gesamtbewertung.** Die Reihenfolge richtet sich nach der Bedeutung für die Verlässlichkeit der veröffentlichten Ergebnisse: zunächst Einordnung und Quellenlimits, dann belegte Änderungen und abschließend der vollständige Summenabgleich. Das ist eine redaktionelle Gewichtung, kein statistischer Schweregrad.','',
           'Die Serie bündelt zusammengehörige Beobachtungen zu zehn Befunden. Alle 26 Grafiken, die politischen Repräsentationsdaten und die vollständigen Prüfbelege sind in Serie und Anhang zugänglich. Ergänzende Ergebnis- und Verteilungsgrafiken stehen im Anhang.','',
           f"Archiv-Endpunkt `{s['ref']}`, letzter Datencommit `{s['last_capture']['commit']}`. Letzter Git-Abruf **07.09.2026, 04:06:37,856 MESZ**. {s['history_commit_count']} Daten-Commits, {s['versions']} datierte Abrufe: {s['election_night_captures']} ab Wahlabend, {s['preopening_captures']} frühe Nullvorlagen; {s['setup_commit_count']} Einrichtungsschritte ohne Abrufzeit. Alle Zeiten sind Europe/Berlin (MESZ, UTC+2).",'',
           '[Vergleich beider Analysen](EXTERNAL_COMPARISON.md) · [Methoden und Reproduktion](METHODS.md) · [Prüfurteil](VALIDATION.md) · [Tweet-Texte](tweets.txt)','', '## Zehn Befunde, von niedriger zu höherer Priorität','', '10 einzelne Entwürfe mit höchstens 280 gewichteten Zeichen. Tweet 10 hat die höchste Priorität. Alle LSA-Abrufzeiten unten sind MESZ. Bilder, Belege und Bildtexte gehören zur Berichtskopie; es wurde nichts veröffentlicht.','']
    for i,t in enumerate(tweets,1):
        lines += [f"### Tweet {i}: {t['finding']}",'',t['text'],'']
        for im,alt in zip(t['images'],t['alt_text']):lines += [f'![{alt}]({im})','']
        lines += ['Belege: '+', '.join(f'[{x}]({x})' for x in t['evidence'])+'.','']
    used_images={image for tweet in tweets for image in tweet['images']}
    supporting=[chart for chart in r.chart_map if chart['path'] not in used_images]
    lines += ['## Grafikanhang: Ergebnisse und ergänzende Einordnung','',
              'Diese Grafiken ergänzen die zehn Befunde. Sie sind keine weiteren Tweets. Dazu gehören die Landesergebnisse, regionale Verteilungen, AfD-Anteile beim Eintreffen der Gebiete und die politische Repräsentation.','']
    for chart in supporting:
        lines += [f"![{chart['alt_text']}]({chart['path']})",'']
        if chart['path'].endswith('09_summenrevisionen.png'):
            lines += ['Die elf hier gezeigten Änderungen von Wähler- oder gültigen Stimmensummen sind eine Teilmenge der 20 Gemeinderevisionen. Die übrigen neun verändern nur Partei-/Kandidatenwerte bei unveränderten Gesamtsummen.','']
    lines += ['## Detailanhang: alle Revisionen bei gleicher Meldezahl','', '| Zeit (MESZ) | Gemeinde | Meldestand | Änderungen |','|---|---|---:|---|']
    for e in s['municipality_revisions']:lines.append(f"| {tm(e['acquired_at_local'])} | {e['name']} | {e['reported']}/{e['total']} | {change_text(e,names)} |")
    lines+=['', 'Diese Auswahl umfasst positive wie negative Parteiverschiebungen bei unveränderter Meldezahl. Sie ist kein vollständiges Verzeichnis sachlicher Korrekturen: Änderungen zusammen mit neuen Meldungen können darin fehlen. Die vollständigen Kandidaten einschließlich U/B-Teilsummen stehen in [raw_candidate_events.csv](raw_candidate_events.csv).','',
            '## Einzelfälle und Prüfgrenzen','', '[Aschersleben 000965: Lücke und Auflösung](AGGREGATION_ASCHERSLEBEN.md) · [Aken 000010: Identität und tatsächliche Stimmen](AKEN_000010.md) · [Bitterfeld-Wolfen 000028: historischer Diff und finaler Bezirkstand](BITTERFELD_WOLFEN_000028.md)','',
            'Die Ergebnisse sind vorläufige amtlich veröffentlichte Zahlen, keine endgültige Feststellung. Exakte Arithmetik schließt plausible, aber sachlich falsche Eingaben nicht aus. Ursachen der Rücknahme, Gemeindekorrekturen und späteren Aufnahme des Aken-Eintrags sind im geprüften Material nicht erklärt. Ausgleichende Korrekturen und Änderungen zwischen Abrufen können unsichtbar bleiben.','',
            'In einer frühen Nullvorlage fehlt die ursprüngliche CSV; ihr normalisierter Git-Export wurde geprüft. Der Zeitraum vor der ersten Statusübersicht um 19:05 bietet keine bezirksscharfe Statushistorie. Vor dem letzten Git-Abruf fehlen individuelle Wahlbezirk-Stimmen vollständig. Die externe Analyse hat andere Abrufzeitpunkte; ihr Software-Commit ist kein zusätzlicher Commit dieses Git-Archivs.','',
            '[Alle Parteien und Gebiete](all_party_results_by_area.csv) · [Finale Summenprüfungen](final_aggregation_checks.csv) · [Historische Summendifferenzen](aggregation_nonzero.csv) · [CSV-Schemawechsel](source_schema_events.json)','']
    if baseline:
        old=j(baseline/'summary.json');oldland=j(baseline/'latest_areas.json')['lsa:LAND:15'];land=r.latest['lsa:LAND:15']
        comparison={'baseline_ref':old['ref'],'current_ref':s['ref'],'valid_second_vote_delta':land['valid_votes_zweit']-oldland['valid_votes_zweit'],'current_land_counters':None,'current_html_reported':s['reporting_evidence']['html_reported'],'parties':[{'party':names[c],'before':oldland['parties'].get(c),'after':v,'delta':v-oldland['parties'].get(c,0)} for c,v in land['parties'].items() if c.startswith('F')]}
        write_json(p/'comparison.json',comparison);lines+=['## Vergleich zur ursprünglichen 98,27%-Auswertung','', f"Seit dem damaligen Stand: +{n(comparison['valid_second_vote_delta'])} gültige Zweitstimmen, einschließlich Nachmeldungen und Korrekturen. Alle 46 damals offenen Statuszeilen sind geschlossen. Der Nenner stieg durch Aken von 2.660 auf 2.661. [Alle Parteien im Vergleich](comparison.json).",'']
    (p/'REPORT.md').write_text('\n'.join(lines)+'\n')
    methods(p,s,r)
    with (p/'REPORT.md').open('a') as f:
        f.write('\n## Frühe Nenner und Vergleich der drei Wahlen\n\n152 Teilstands-Beobachtungen in 17 überlappenden Gebieten ergeben Wählende > veröffentlichte Wahlberechtigte; keine davon ist vollständig gemeldet. Beispiel Balgstädt 19:19: 93 Urnenwählende aus einem von drei Urnenbezirken mit 134 Wahlberechtigten plus 155 Briefwählende, zusammen 248. Dieser gemischte Teilstand erlaubt keine Beteiligungsquote 248/134. Zeilenbilanzen bleiben korrekt. [Alle betroffenen Zustände](turnout_denominator_observations.json).\n\nDer Vergleich BW/RLP/SA prüft die ersten und letzten positiven archivierten Landesanteile. BW/RLP beruhen auf normalisierten Git-Exporten; ursprüngliche Nutzdaten sind dort nicht so vollständig archiviert wie für LSA. Im ersten RLP-Stand ist die normalisierte Wählerzahl inkonsistent mit den gültigen Stimmen; Ursache ist der belegte Parserfehler, der FREIE WÄHLER als Wählerzahl erfasste (später behoben in Git 80b01c01). Die Restdifferenz beträgt genau 4.652 Stimmen. Die sechs gezeigten Parteizähler und der Nenner 90.362 bleiben unverändert; es wird nicht auf die unvollständige Parteisumme umnormiert. Der spätere RLP-PREP_ZERO-Reset wird nicht als Wahl-Endergebnis interpretiert. Die Exportdateien der Endpunkte liegen unter cross_election_sources.\n')

def comparison_text(p,x):
    lines=['# Vergleich mit Lauras Wahlforensik (GFrei.News)','',
           'Der Vergleich verwendet einen eingefrorenen öffentlichen Export der [verlinkten Analyse](https://gfrei.news/wp-content/uploads/wahlbeobachtung/). Deren Software-Commit und Uhrzeit gehören zu ihrem eigenen System. Sie werden nicht als zusätzliche Git-Wahlbeobachtung gezählt.','',
           f"Externe Version `{x['external_version']}`, erzeugt `{x['external_generated_at']}`. Datenstände und JSON-Teile wurden lokal mit SHA-256 archiviert: [Manifest](external/manifest.json), [Katalog](external/current.json). Die drei gemeinsamen Ergebnis-CSV-Hashes sind identisch; ebenso die weiteren gemeinsam gespeicherten HTML-Dateien. Ein späterer externer Prüfzeitpunkt bedeutet daher keine neueren Stimmen.",'',
           '| Gegenstand | Vergleichsergebnis |','|---|---|',
           f"| Originalwerte | {n(x['raw_rows_compared'])} Zeilen einschließlich U/B und Gesamtsummen, {n(x['numeric_comparisons'])} Zahlen, 0 Differenzen |",
           '| Arithmetik | Beide Analysen melden keine Bilanz-/Parteisummenfehler; unser Historienaudit umfasst zusätzlich alle früheren Git-Stände |',
           '| Revisionen | Alle 86 extern markierten Felder passen mit Vorher-/Nachher-Werten exakt zu Git-Übergängen. 33 Kombinationen aus Gebiets-/Wahlartzeile und Git-Übergang; keine 86 unabhängigen Vorfälle |',
           '| Unser Revisionsmaß | 20 Übergänge von Gemeinde-Gesamtsummen bei gleicher Meldezahl, in 19 Gemeinden. Positive und negative Parteiverschiebungen zählen mit |',
           '| Robuste MAD-Prüfung | Alle 24.171 prüfbaren Werte einschließlich Median, MAD, z, Gruppengröße und Farbklasse unabhängig reproduziert; 18 unzureichende Vergleiche gesondert |',
           '| Statistische Hinweise | 88 orange + 30 rote Merkmalswerte in 106 verschiedenen Gebieten. 91 Hinweise betreffen GRÜNE |',
           '| Endziffern | Alle fünf Stichproben und zehn Ziffernhäufigkeiten pro Partei reproduziert. Externe q-Werte jeweils 1; Simulationen hier nicht erneut ausgeführt |',
           '| Multivariate / räumliche Verfahren | Katalog und drei multivariate Ergebnislisten archiviert; Modelle, historische Vergleichsanteile und Geometrieprüfungen nicht unabhängig neu gerechnet. Keine Übernahme als neue Fehlerfeststellung |','',
           'Die robusten Gruppen trennen Gebietsebene, Wahlart und Zehnerpotenz der Wählerzahl. Sie berücksichtigen keine Stadt-/Landlage, Sozialstruktur oder Kandidaten. Die 91 GRÜNE-Hinweise sind daher auffällige Abstände in diesem Modell, keine 91 Wahlfehler. Die ergänzte Grafik zeigt für die zwölf größten Abstände auch den Median vergleichbarer Bezirke derselben Gemeinde.','',
           'Die Endziffernprüfung berücksichtigt hier disjunkte Wahlbezirke: gültige Zweitstimmen >400, Parteistimmen mindestens 100 und geschätzte binomiale Standardabweichung mindestens 10; mindestens 100 geeignete Bezirke pro Partei. Die aktuelle Berechnungsbeschreibung nennt diesen Bezirksvorrang; die allgemeine Feldbeschreibung spricht noch von Gemeinden. Der Ergebnisexport benennt WBZ und ist für die überprüfte Population maßgeblich.','',
           'Inhaltlich besteht bei den verglichenen Werten kein Widerspruch. Unser Beitrag ergänzt die vollständige Git-Zeitfolge, die 19:35–22:11-Statusrücknahme, das genaue Auftauchen von Aken und die belegte Auflösung der Aschersleben-Lücke. Die andere Analyse ergänzt ein breites statistisches Screening. Diese Verfahren sind abhängig und dürfen nicht als mehrere unabhängige Beweise für einen Fehler addiert werden.','',
           '[Alle Zahlenprüfungen](external_comparison.json) · [Exakte Revisionszuordnung](external_revision_comparison.json) · [Alle robusten Neuberechnungen](external_mad_recalculation.csv) · [Örtlicher Kontext](screening_local_context.csv) · [Gesamtbericht](REPORT.md)','']
    (p/'EXTERNAL_COMPARISON.md').write_text('\n'.join(lines))

def methods(p,s,r):
    ref=s['ref'];text=f'''# Methods and reproducibility

Git endpoint: `{ref}`; last data-changing commit: `{s['last_capture']['commit']}`.
Capture time: `{s['last_capture']['acquired_at_utc']}` UTC = `{s['last_capture']['acquired_at_local']}` Europe/Berlin.
The audit reads all {s['history_commit_count']} first-parent data commits, including {s['versions']} dated captures, three pre-opening templates and two setup steps. First-parent scope is checked against the full reachable data-commit set. No live election source is requested.

## Reproduce from the repository root

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref {ref} --full-history --require-complete --output data/2026-lsa/reports/git-timeline/{ref[:8]}
python3 scripts/compare_lsa_external_analysis.py --input data/2026-lsa/reports/git-timeline/{ref[:8]}
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/{ref[:8]} --baseline data/2026-lsa/reports/git-timeline/034045f3
python3 scripts/run_lsa_audit_notebook.py
python3 scripts/verify_lsa_git_report.py --input data/2026-lsa/reports/git-timeline/{ref[:8]} --visual-reviewed
python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v
```

The external directory is a frozen input. Keep it with a second output directory before running the comparison. The sources are versioned JSON payloads plus the catalogue/index; verify every archived payload against external/manifest.json. It is not a Git election observation. Acquisition requires network only when explicitly refreshing the external comparison; reconstruction itself is offline.

## Measures and event units

All valid second votes are the party-share denominator; choose a fixed cohort strictly above 5% in the final Land row. Every party is included in numerical audits. Sum counts and denominators before computing aggregate percentages. Each geographic level is evaluated independently; do not add overlapping levels. Municipality-to-Kreis uses AGS; individual booths use municipality AGS and their actual constituency, including split municipalities. U+B is checked against each available total.

Missing numeric cells remain null, not zero. Blank candidate cells are omitted from party sums and retained as missing cells in exports. The final regional CSV omits Ist.Wahlbezirke, Soll.Wahlbezirke and Uhrzeit; it renames Gewählt im Wahlkreis to Gewählt.im.Wahlkreis and switches Ergebnisart Z to V. Null counters are not a district disappearance, vote reset or numerical denominator decrease. Final completeness is established separately by HTML, municipality counters, 2,661 matching individual identities and reconciled vote aggregates; raw Land counters remain null.

A fixed-count revision is one area transition with changed voter/valid-vote/party values at equal non-null reported counts; after-complete means the previous positive reported and expected counts agreed. Candidate events also include decreases, missing values, disappearance, reset and changes in two known denominators. U/B and parent copies are retained but not counted as separate municipal incidents. The 162 historical aggregate differences are 27 fields at two relations in three captures; the final capture has no nonzero difference.

## Statistical comparison

GFrei.News's frozen robust-MAD groups are independently rebuilt from original Git CSV rows: complete municipality totals and individual booths, voters >=100, split by geographic level, voting mode and floor(log10(voters)); >=20 peers; z=(value-median)/(1.4826*MAD). Features are turnout (non-postal A>0), both invalid-ballot rates, and six statewide >5% party shares. The original orange/red thresholds are reproduced, not treated as p-values or calibrated fraud probabilities. Every checked observed value, median, MAD, group size, z and classification matches. Eighteen insufficient comparisons are not silently filled.

All 86 external revision flags match exact raw Git before/after values. Different capture clocks are retained. Five digit-test observation samples reproduce exactly; external simulation-derived q-values are cited as external results, not a locally recomputed significance test. Multivariate and spatial model results are not independently validated here. The local-context chart uses the same voter-size class and voting mode within the booth's municipality; it is descriptive and includes the queried booth.

## Limits and provenance

Status observations begin at 19:05; one earlier zero template lacks original source CSV bytes. Only the last Git capture contains individual vote rows. Thus final values cannot identify historical per-booth revisions or decompose multi-booth municipality changes. Observed time is the collector's capture time, not necessarily publication, counting or certification time. Step charts connect observations but do not establish continuous unchanged state between them. No inference of electoral misconduct follows from these checks; plausible but incorrect underlying records may pass arithmetic.

Reconstruction changes only derived artifacts. It does not poll, alter schedules, commit, publish or deploy. The earlier reports and movie remain available. The prior polling-stop control record is historical evidence, not a newly queried control state.
'''
    text+='''
## Arrival distributions, representation and three-election share comparison

AfD arrival charts show each of 218 municipalities, 41 constituencies and 14 counties once, with its AfD share at the actual first positive or first complete archived result; these are separate definitions. Final WBZ reconciliation supplies completion evidence for remaining higher-level rows with removed counters. Equal-area boxplots use 30-minute bins only with at least five arrivals. Whiskers are 1.5 IQR, not uncertainty intervals. Later revisions do not overwrite the first observed share.

The representation waterfall follows the BW/RLP structure. Population 2,120,252 on 31 December 2025 comes from the retained official release linked in representation_sources.json. The election electorate is 1,706,851. Because dates differ, population minus electorate is an approximate contextual bridge. Parties with seats come from the archived preliminary seat CSV; their 1,224,292 second votes reconcile to the sum of six qualifying parties. The chart shows votes, not seat proportions or a legal measure of representation.

For BW/RLP/SA, first and last mean first/last positive LIVE statewide result in the selected Git history. Later PREP_ZERO states are excluded and logged. Exact endpoint normalized files and hashes are retained in cross_election_sources. Party sums are checked against valid second votes. The first RLP export has a documented 4,652-vote residual: its parser misclassified FREIE WÄHLER as the voter total (fixed in 80b01c01). This party is outside the displayed cohort; selected party votes and the published valid denominator remain unchanged. No renormalization to the incomplete party sum is performed. Relative change is 100*(last_share-first_share)/first_share; a missing or zero first share is undefined. The cohort is the union strictly above 5% at any final endpoint. The first RLP voter count is inconsistent and is not used in the share calculation; independently retained original result payloads are unavailable for BW/RLP at these endpoints.

The electorate check retains 152 partial-state observations (17 overlapping area rows) as denominator limitations, separate from arithmetic contradictions in complete results. Postal voters can arrive alongside only part of the in-person electorate; B/A is then not an election-turnout estimate. See turnout_denominator_observations.json.
'''
    (p/'METHODS.md').write_text(text)
    import matplotlib
    files=['analyze_lsa_git_timeline.py','render_lsa_tweet_report.py','lsa_full_history_report.py','lsa_complete_history_report.py','compare_lsa_external_analysis.py','render_lsa_additional_charts.py','render_cross_election_share_delta.py','render_bw_second_vote_representation_waterfall.py','run_lsa_audit_notebook.py','verify_lsa_git_report.py','test_lsa_git_timeline.py']
    write_json(p/'environment.json',{'python':platform.python_version(),'matplotlib':matplotlib.__version__,'numpy':np.__version__,'scripts':{f:hashlib.sha256((Path(__file__).parent/f).read_bytes()).hexdigest() for f in files}})
