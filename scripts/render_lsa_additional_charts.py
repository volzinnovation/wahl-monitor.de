"""AfD arrival distributions and the cross-election political-representation bridge."""
import csv, gzip, hashlib, io, json, math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
from analyze_lsa_git_timeline import git, BASE, is_complete, write_csv, write_json
from render_bw_second_vote_representation_waterfall import CHART_COLORS, PARTY_COLORS, wrap_label, label_color_for_fill

INK,BLUE,MUTED='#222a35','#3478a5','#66717e'
POPULATION_URL='https://statistik.sachsen-anhalt.de/news/news-details/212-mio-menschen-lebten-am-31122025-in-sachsen-anhalt'

def de(n):return f'{n:,}'.replace(',','.')
def arrivals(r):
    first={};complete={};levels={'GEMEINDE','WAHLKREIS','KREIS'}
    with gzip.open(r.path/'raw_timeline.jsonl.gz','rt') as f:
        for line in f:
            x=json.loads(line)
            if x['level'] not in levels or x['mode']!='TOTAL' or not x['valid_votes_zweit']:continue
            v=r.version_by_commit[x['commit']]
            entry={'key':x['area_key'],'level':x['level'],'name':x['name'],'commit':x['commit'],'time_local':v['acquired_at_local'],'valid_votes_zweit':x['valid_votes_zweit'],'afd_votes':x['parties']['F2'],'afd_share':100*x['parties']['F2']/x['valid_votes_zweit'],'reported':x['reported_precincts'],'total':x['total_precincts'],'completion_evidence':'CSV Ist=Soll>0'}
            first.setdefault(x['area_key'],entry)
            if is_complete(x):complete.setdefault(x['area_key'],entry)
    for key,x in r.latest.items():
        if x['level'] in levels and key not in complete:
            assert r.s['reporting_evidence']['complete_reconciled']
            complete[key]={'key':key,'level':x['level'],'name':x['name'],'commit':r.s['last_capture']['commit'],'time_local':r.s['last_capture']['acquired_at_local'],'valid_votes_zweit':x['valid_votes_zweit'],'afd_votes':x['parties']['F2'],'afd_share':100*x['parties']['F2']/x['valid_votes_zweit'],'reported':None,'total':None,'completion_evidence':'Finaler WBZ-Abgleich; CSV-Meldezähler fehlt'}
    assert len(first)==len(complete)==273
    result=[]
    for kind,data in [('first_positive_result',first),('first_complete_result',complete)]:
        result.extend({'arrival_definition':kind,**x} for x in data.values())
    write_csv(r.path/'afd_arrival_observations.csv',result)
    return result

def arrival_chart(r,data,kind,slug,title):
    allrows=[x for x in data if x['arrival_definition']==kind]
    definition='erster positiver Stand je Gebiet' if kind=='first_positive_result' else 'erster Vollständigkeitsbeleg je Gebiet'
    fig=r.figure(title,f'x: {definition} · y: AfD-Anteil zu genau diesem Abruf · keine nachträglich eingesetzten Endanteile',height=14)
    start=datetime.fromisoformat('2026-09-06T18:30:00+02:00');end=datetime.fromisoformat('2026-09-07T04:30:00+02:00')
    if kind=='first_positive_result':
        latest=max(datetime.fromisoformat(x['time_local']) for x in allrows)
        end=latest.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)
    stats=[]
    for index,(level,label) in enumerate([('GEMEINDE','218 Gemeinden'),('WAHLKREIS','41 Wahlkreise'),('KREIS','14 Kreise / kreisfreie Städte')]):
        ax=fig.add_axes([.09,.65-index*.245,.84,.195]);subset=[x for x in allrows if x['level']==level];bins=defaultdict(list)
        for x in subset:
            t=datetime.fromisoformat(x['time_local']);minute=t.replace(minute=(t.minute//30)*30,second=0,microsecond=0);bins[minute].append(x)
        for t,group in sorted(bins.items()):
            values=[x['afd_share'] for x in group];position=mdates.date2num(t+timedelta(minutes=15))
            if len(values)>=5:
                ax.boxplot([values],positions=[position],widths=20/(24*60),manage_ticks=False,patch_artist=True,showfliers=False,
                           boxprops={'facecolor':'#dce8f0','edgecolor':BLUE,'linewidth':1},medianprops={'color':INK,'linewidth':1.4},whiskerprops={'color':BLUE},capprops={'color':BLUE})
            stats.append({'arrival_definition':kind,'level':level,'bin_start':t.isoformat(),'n':len(values),'median':float(np.median(values)),'q25':float(np.percentile(values,25)),'q75':float(np.percentile(values,75)),'min':min(values),'max':max(values),'box_shown':len(values)>=5})
            ax.text(position,76,str(len(values)),ha='center',fontsize=8,color=MUTED)
        ordinary=[x for x in subset if x['completion_evidence']=='CSV Ist=Soll>0'];inferred=[x for x in subset if x not in ordinary]
        ax.scatter([datetime.fromisoformat(x['time_local']) for x in ordinary],[x['afd_share'] for x in ordinary],s=24,color=BLUE,alpha=.60,zorder=3)
        if inferred:ax.scatter([datetime.fromisoformat(x['time_local']) for x in inferred],[x['afd_share'] for x in inferred],s=55,marker='D',facecolors='white',edgecolors=INK,zorder=4)
        for city,offset in [('Magdeburg',(8,8)),('Halle (Saale)',(8,-15)),('Dessau-Roßlau',(8,4))]:
            x=next((x for x in subset if x['name'].startswith(city) and (level!='WAHLKREIS')),None)
            if x:
                ax.annotate(city,(datetime.fromisoformat(x['time_local']),x['afd_share']),xytext=offset,textcoords='offset points',fontsize=8.5,color=INK,arrowprops={'arrowstyle':'-','lw':.6,'color':MUTED})
        ax.set_title(label,loc='left',fontsize=13,pad=12);ax.set_ylim(0,80);ax.set_yticks([0,20,40,60]);ax.set_xlim(start,end);ax.set_ylabel('AfD (%)');ax.grid(axis='y',alpha=.15)
        ax.xaxis.set_major_locator(mdates.HourLocator(tz=r.cutoff.tzinfo));ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M',tz=r.cutoff.tzinfo))
        if index==2:ax.set_xlabel('Abrufzeit 6./7. September 2026 · MESZ')
    fig.text(.09,.10,'Punkte = einzelne Gebiete; Boxen je 30 Minuten nur ab n ≥ 5: Median, mittlere 50 %, Whisker 1,5 × IQR.',fontsize=10.5,color=MUTED)
    fig.text(.09,.075,'Zahlen oben = Gebiete je Zeitfenster. Jede Gebietseinheit zählt gleich; Gebietsebenen bleiben getrennt.',fontsize=10.5,color=MUTED)
    if kind=='first_complete_result':fig.text(.09,.05,'Raute = Vollständigkeit erst durch finalen Bezirksabgleich belegt. Spätere Revisionen ändern diesen ersten Stand nicht.',fontsize=9.5,color=MUTED)
    else:fig.text(.09,.05,'Frühe Gebietsergebnisse können auf wenigen Stimmen beruhen. Gültige Stimmen und Meldezähler stehen im CSV.',fontsize=9.5,color=MUTED)
    # Move the provenance footer below the method note in this tall figure.
    fig.texts[2].set_y(.02)
    r.save(fig,slug,title,'time scatter with binned box plots',['capture time','AfD share at arrival','geographic level','valid votes'], 'afd_arrival_observations.csv','First positive and first complete result are separate charts. Actual observation times are not jittered. Each area appears once per chart. Boxplots describe arrivals, not cumulative results or confidence intervals.')
    return stats

def representation(r):
    p=r.path;land=r.raw['lsa:LAND:15:TOTAL'];population=2120252;eligible=land['extra']['eligible_voters'];voters=land['voters_total'];valid=land['valid_votes_zweit']
    context=p/'context_sources';context.mkdir(exist_ok=True)
    population_bytes=(context/'population_2025.html').read_bytes()
    assert b'2 120 252' in population_bytes or b'2\xc2\xa0120\xc2\xa0252' in population_bytes
    manifest=json.loads(git('show',f"{r.s['ref']}:{BASE}official_sources/manifest.json"))
    seat_fetch=next(x for x in manifest['fetches'] if 'Sitzverteilung' in x['url']);seat_bytes=git('show',f"{r.s['ref']}:{BASE}official_sources/{seat_fetch['filename']}")
    assert hashlib.sha256(seat_bytes).hexdigest()==seat_fetch['content_hash'];(context/'seat_distribution.csv').write_bytes(seat_bytes)
    seats=list(csv.DictReader(io.StringIO(seat_bytes.decode('utf-8-sig')),delimiter=';'))
    represented={x['Partei'] for x in seats if x['Partei']!='Insgesamt' and int(x['Sitze.insgesamt'])>0}
    parties=[{'party':land['party_names'][c],'votes':v} for c,v in land['parties'].items() if c.startswith('F') and land['party_names'][c] in represented]
    parties.sort(key=lambda x:-x['votes']);represented_total=sum(x['votes'] for x in parties);below=valid-represented_total
    assert represented=={x['party'] for x in r.selected}
    data=[]
    def total(label,value,role='subtotal',approx=False):data.append({'label':label,'type':'total','role':role,'amount':value,'start':0,'end':value,'approximate':approx})
    def delta(label,value,start,role='exclusion',approx=False):data.append({'label':label,'type':'delta','role':role,'amount':value,'start':start,'end':start-value,'approximate':approx});return start-value
    current=population;total('Einwohner',current,'population');current=delta('Nichtwahlberechtigte*',population-eligible,current,approx=True);total('Wahlberechtigte',current)
    current=delta('Nichtwählende',eligible-voters,current);total('Wählende',current);current=delta('Ungültige Zweitstimmen',voters-valid,current);total('Gültige Zweitstimmen',current)
    current=delta('Parteien ohne Sitze',below,current);total('Repräsentierte Zweitstimmen',current,'represented')
    for party in parties:current=delta(party['party'],party['votes'],current,'party')
    assert current==0
    for i,x in enumerate(data):x['order']=i+1;x['signed_amount']=-x['amount'] if x['type']=='delta' else x['amount']
    write_csv(p/'statla_second_vote_representation_waterfall.csv',data)
    sources={'population_total':population,'population_date':'2025-12-31','population_url':POPULATION_URL,'population_file':'context_sources/population_2025.html','population_sha256':hashlib.sha256(population_bytes).hexdigest(),'eligible':eligible,'voters':voters,'valid_second_votes':valid,'represented_votes':represented_total,'not_represented_valid_votes':below,'represented_share_valid':100*represented_total/valid,'represented_share_eligible':100*represented_total/eligible,'represented_share_population':100*represented_total/population,'seat_source':seat_fetch,'git_ref':r.s['ref'],'note':'Population is dated 31 December 2025; eligible/votes are election results dated 6/7 September 2026. Their difference is an approximate contextual bridge, not a measured election-day count of ineligible people. Representation here means votes for parties with seats in the published preliminary seat CSV.'}
    write_json(p/'representation_sources.json',sources)
    fig,ax=plt.subplots(figsize=(18,10));fig.subplots_adjust(left=.065,right=.985,bottom=.24,top=.84)
    fig.text(.045,.96,'Landtagswahl Sachsen-Anhalt 2026 · Politische Repräsentation',fontsize=23,weight='bold',va='top')
    fig.text(.045,.91,f"{de(represented_total)} Zweitstimmen für Parteien mit Sitzen · {format(100*represented_total/valid,'.2f').replace('.',',')} % der gültigen Stimmen · {format(100*represented_total/eligible,'.2f').replace('.',',')} % der Wahlberechtigten",fontsize=13,color=MUTED)
    for i,x in enumerate(data):
        color=PARTY_COLORS.get(x['label'],INK) if x['role']=='party' else CHART_COLORS['exclusion'] if x['role']=='exclusion' else CHART_COLORS['represented_total'] if x['role']=='represented' else CHART_COLORS['start_total'] if x['role']=='population' else CHART_COLORS['subtotal']
        bottom=min(x['start'],x['end']) if x['type']=='delta' else 0;height=x['amount']
        ax.bar(i,height,bottom=bottom,width=.72,color=color,edgecolor=MUTED if x['approximate'] else color,hatch='///' if x['approximate'] else None,zorder=3)
        label=('≈ ' if x['approximate'] else '')+de(x['amount'])
        if height>population*.07 and x['role']!='population':ax.text(i,bottom+height/2,label,ha='center',va='center',fontsize=9,weight='bold',color=label_color_for_fill(color))
        else:ax.annotate(label,(i,bottom+height),xytext=(0,9),textcoords='offset points',ha='center',fontsize=9.5,weight='bold',arrowprops={'arrowstyle':'-','color':MUTED,'lw':.6})
        if i<len(data)-1:ax.plot([i+.36,i+.64],[x['end'],x['end']],color=MUTED,linestyle='--',lw=.8)
    labels=[wrap_label(x['label']).replace('Nichtwahlberech-\ntigte*','Nichtwahl-\nberechtigte*') for x in data]
    labels[1]='Nichtwahl-\nberechtigte*';labels[7]='Parteien\nohne Sitze'
    ax.set_xticks(range(len(data)),labels,fontsize=10);ax.set_ylim(0,population*1.08);ax.set_xlim(-.7,len(data)-.3);ax.set_ylabel('Personen / Zweitstimmen');ax.yaxis.set_major_formatter(FuncFormatter(lambda v,pos:f'{v/1e6:.1f}'.replace('.',',')+' Mio.'));ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
    fig.text(.045,.15,'Orange: Abzüge bis zu Stimmen für Parteien mit Sitzen. Danach Aufteilung dieser Stimmen auf die sechs Parteien.',fontsize=11,color=MUTED)
    fig.text(.045,.115,'* Bevölkerung: 31.12.2025; Wahlberechtigte: Wahl 2026. Die Differenz ist wegen verschiedener Stichtage nur eine Annäherung.',fontsize=10.5,color=MUTED)
    fig.text(.045,.080,'„Repräsentiert“ bezeichnet hier Stimmen für Parteien mit vorläufig zugeteilten Sitzen; die Grafik zeigt keine Sitzanteile.',fontsize=10.5,color=MUTED)
    fig.text(.045,.045,f"Quellen: Statistisches Landesamt (Bevölkerung 2025), amtliche Wahl- und Sitz-CSV im Git {r.s['ref'][:8]} · Stand 07.09.2026, 04:06 MESZ",fontsize=10,color=MUTED)
    # Register the graphic with the common source/alt-text map.
    r.save(fig,'24_politische_repraesentation','How do population, participation and votes for represented parties relate?','waterfall',['population','eligible','voters','invalid second votes','party votes','published seats'],'statla_second_vote_representation_waterfall.csv; representation_sources.json','Same additive waterfall structure as BW/RLP. Seat-bearing parties are verified against the preliminary seat CSV. Different population/election dates are marked; the chart ends at zero.')
    return sources

def render(r):
    data=arrivals(r)
    stats=arrival_chart(r,data,'first_positive_result','22_afd_erste_ergebnisse','AfD-Anteile beim Eintreffen erster Gebietsergebnisse')
    stats+=arrival_chart(r,data,'first_complete_result','23_afd_vollmeldungen','AfD-Anteile: Wann Gebiete erstmals vollständig melden')
    write_csv(r.path/'afd_arrival_distribution_bins.csv',stats)
    return representation(r)
