"""Compare first and last archived positive statewide vote shares across BW/RLP/SA."""
import csv,hashlib,io,json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
from analyze_lsa_git_timeline import git,write_csv,write_json
BERLIN=ZoneInfo('Europe/Berlin')

def canonical(name):
    return {'DIE LINKE':'Die Linke','DIE LINKE.':'Die Linke','GRUENE':'GRÜNE'}.get(name,name)

def endpoints(r,election):
    base=f'data/{election}/latest/'
    logs=git('log','--first-parent','--reverse','--format=%H',r.s['ref'],'--',base+'statla_snapshots.csv',base+'statla_party_results.csv',base+'run_metadata.json').decode().splitlines()
    examined=[];cache={}
    def load(ref):
        if ref in cache:return cache[ref]
        files=set(git('ls-tree','-r','--name-only',ref,base).decode().splitlines())
        if base+'run_metadata.json' not in files or base+'statla_snapshots.csv' not in files:return None
        meta=json.loads(git('show',f'{ref}:{base}run_metadata.json'))
        date=meta.get('generated_at_utc');mode=meta.get('statla_mode','')
        data=git('show',f'{ref}:{base}statla_snapshots.csv')
        land=next((x for x in csv.DictReader(io.StringIO(data.decode('utf-8-sig'))) if x['gebietsart']=='LAND'),None)
        valid=int(land.get('valid_votes_zweit') or 0) if land else 0
        include=bool(date and mode.startswith('LIVE') and valid>0)
        examined.append({'election':election,'commit':ref,'capture_utc':date,'mode':mode,'valid_second_votes':valid,'eligible_for_endpoint':include})
        if not include:cache[ref]=None;return None
        party_bytes=git('show',f'{ref}:{base}statla_party_results.csv')
        party={canonical(x['party_name']):int(x['votes']) for x in csv.DictReader(io.StringIO(party_bytes.decode('utf-8-sig'))) if x['row_key']==land['row_key'] and x['vote_type']=='Zweitstimmen' and x['votes']!=''}
        residual=valid-sum(party.values());quality_note=None
        if residual:
            parser=git('show',f'{ref}:scripts/poll_election_core.py').decode()
            assert election=='2026-rlp' and residual==int(land['voters_total']) and party.get('FREIE WÄHLER')==0 and 'if "wahler" in normalized_label or "waehler" in normalized_label:' in parser,(election,ref,residual)
            quality_note='Known early parser defect: FREIE WÄHLER classified as voter total. Residual equals misassigned voter field; selected party votes and valid denominator are retained unchanged. Fixed later in commit 80b01c01.'
        point={'election':election,'commit':ref,'capture_utc':date,'capture_local':datetime.fromisoformat(date).astimezone(BERLIN).isoformat(),'valid_second_votes':valid,'voters_total':int(land.get('voters_total') or 0),'reported':int(land['reported_precincts']) if land.get('reported_precincts') else None,'total':int(land['total_precincts']) if land.get('total_precincts') else None,'mode':mode,'parties':party,'party_sum_matches_valid':residual==0,'party_sum_residual':residual,'quality_note':quality_note,'snapshot_sha256':hashlib.sha256(data).hexdigest(),'parties_sha256':hashlib.sha256(party_bytes).hexdigest(),'source_url':meta.get('statla_url'),'source_error':meta.get('statla_error')}
        cache[ref]=point
        # Retain exact endpoint exports for independent replay and packaging.
        folder=r.path/'cross_election_sources'/election/ref;folder.mkdir(parents=True,exist_ok=True)
        (folder/'statla_snapshots.csv').write_bytes(data);(folder/'statla_party_results.csv').write_bytes(party_bytes);(folder/'run_metadata.json').write_bytes(git('show',f'{ref}:{base}run_metadata.json'))
        return point
    first=next(x for ref in logs if (x:=load(ref)) is not None)
    last=next(x for ref in reversed(logs) if (x:=load(ref)) is not None)
    return {'first':first,'last':last,'boundary_candidates_examined':examined,'relevant_data_commits':len(logs)}

def render(r):
    points={}
    for election,label in [('2026-bw','BW'),('2026-rlp','RLP'),('2026-lsa','SA')]:points[label]=endpoints(r,election)
    cohort=sorted({party for pair in points.values() for party,votes in pair['last']['parties'].items() if votes/pair['last']['valid_second_votes']>.05})
    assert 'FREIE WÄHLER' not in cohort or not points['RLP']['first']['party_sum_residual'], 'Cannot compare the party affected by the known early parser defect'
    table=[]
    for party in cohort:
        for election,pair in points.items():
            a,b=pair['first'],pair['last'];before=a['parties'].get(party);after=b['parties'].get(party)
            first_share=100*before/a['valid_second_votes'] if before is not None else None;last_share=100*after/b['valid_second_votes'] if after is not None else None
            delta=100*(last_share-first_share)/first_share if first_share is not None and first_share>0 and last_share is not None else None
            table.append({'party':party,'election':election,'first_votes':before,'last_votes':after,'first_valid_votes':a['valid_second_votes'],'last_valid_votes':b['valid_second_votes'],'first_share_percent':first_share,'last_share_percent':last_share,'relative_delta_percent':delta,'change_percentage_points':last_share-first_share if first_share is not None and last_share is not None else None,'first_commit':a['commit'],'last_commit':b['commit'],'first_time_local':a['capture_local'],'last_time_local':b['capture_local'],'undefined_reason':'Party absent from first/last export' if before is None or after is None else 'First share is zero' if before==0 else None})
    write_json(r.path/'cross_election_endpoints.json',points);write_csv(r.path/'cross_election_share_delta.csv',table)
    fig=r.figure('Relative Änderung der Parteianteile: erster → letzter Stand','Zweitstimmen / RLP: Landesstimmen · (letzter Anteil − erster Anteil) ÷ erster Anteil × 100 · keine Prozentpunkte',height=10)
    ax=fig.add_axes([.09,.35,.85,.43]);positions=np.arange(len(cohort));width=.24
    colors=['#222a35','#3478a5','#b3cedf'];hatches=['','///','...']
    for i,label in enumerate(['BW','RLP','SA']):
        values=[next(x['relative_delta_percent'] for x in table if x['election']==label and x['party']==p) for p in cohort]
        nums=[v for v in values if v is not None]
        bars=ax.bar(positions+(i-1)*width,[v if v is not None else 0 for v in values],width,color=colors[i],hatch=hatches[i],label=label+'*' if label=='RLP' else label,edgecolor='#66717e',linewidth=.3)
        for bar,value in zip(bars,values):
            x=bar.get_x()+bar.get_width()/2
            if value is None:ax.text(x,0,'n. a.',rotation=90,ha='center',va='bottom',fontsize=9,color='#66717e')
            else:ax.annotate(f'{value:+.1f}'.replace('.',','),(x,value),xytext=(0,5 if value>=0 else -5),textcoords='offset points',ha='center',va='bottom' if value>=0 else 'top',fontsize=9)
    values=[x['relative_delta_percent'] for x in table if x['relative_delta_percent'] is not None];lo,hi=min(values),max(values);pad=(hi-lo)*.16
    ax.set_ylim(min(0,lo)-pad,max(0,hi)+pad);ax.axhline(0,color='#222a35',lw=1);ax.set_xticks(positions,cohort,fontsize=12);ax.set_ylabel('Relative Änderung des Stimmenanteils (%)');ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True);ax.legend(frameon=False,ncol=3,loc='upper left')
    for i,(label,pair) in enumerate(points.items()):
        a,b=pair['first'],pair['last'];local_a=datetime.fromisoformat(a['capture_local']);local_b=datetime.fromisoformat(b['capture_local'])
        fig.text(.09,.245-i*.045,f"{label}: {local_a:%d.%m.%Y %H:%M:%S} → {local_b:%d.%m.%Y %H:%M:%S} · {a['valid_second_votes']:,} → {b['valid_second_votes']:,} gültige Stimmen".replace(',','.'),fontsize=10.5,color='#66717e')
    fig.text(.09,.085,'Parteiauswahl: >5 % in mindestens einem letzten Landesstand. n. a. = fehlende Partei oder Ausgangsanteil null.',fontsize=10,color='#66717e')
    fig.text(.09,.06,'* RLP: früher Parserfehler bei FREIE WÄHLER; hier gezeigte Parteien und Nenner unverändert. Details im Bericht.',fontsize=10,color='#66717e')
    fig.text(.09,.038,'Selektive Startmeldungen, kein Vergleich von Wahlfehlern. Zeiten: MEZ im März, MESZ im September.',fontsize=9.5,color='#66717e')
    fig.texts[2].set_y(.015)
    fig.texts[2].set_text('Quelle: normalisierte Git-Exporte BW/RLP, amtliche CSV Sachsen-Anhalt · feste Endpunkte und Einschränkungen im Bericht · wahl-monitor.de')
    r.save(fig,'25_drei_wahlen_relativer_anteilsdelta','How did party shares change relatively from the first to last archived positive result?','grouped bar',['party','election','relative share delta'],'cross_election_share_delta.csv; cross_election_endpoints.json','Three bars per party; zero/absent initial share undefined. Fixed cohort is union of >5% parties at final endpoints. BW/RLP use archived normalized exports; party sums checked. Later prep-zero resets are excluded, not treated as final results.')
    print(json.dumps({'cross_election_endpoints':{k:{tag:{f:x[tag][f] for f in ['commit','capture_local','valid_second_votes']} for tag in ['first','last']} for k,x in points.items()}},ensure_ascii=False))
    return table
