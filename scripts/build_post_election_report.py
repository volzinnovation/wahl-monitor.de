#!/usr/bin/env python3
"""Build reproducible Berlin/MV post-election reports from a coherent local capture.

--dry-run labels interim data and representation scenarios explicitly.
--complete-count requires complete, reconciled vote data; the waterfall remains
a scenario until an official seat table is supplied.
--require-complete refuses incomplete/reconciliation-failing captures and requires
an official seat table. No mode polls, commits, pushes or publishes anything.
"""
from __future__ import annotations

import argparse
import base64
import html
import json
import re
import shutil
import sys
import textwrap
from pathlib import Path
from html.parser import HTMLParser

import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, Patch
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd

from post_election_common import (ROOT,ELECTIONS,POPULATION,capture,raw_context,validate_capture,
                                 read_csv,write_csv,save_json,sha)
from analyze_post_election import demographic_analysis,history
from prepare_post_election_demographics import FEATURES
from render_bw_second_vote_representation_waterfall import PARTY_COLORS,CHART_COLORS,label_color_for_fill

INK='#222a35'; MUTED='#637183'; BLUE='#3478a5'; ORANGE='#d97732'
COLORS={**PARTY_COLORS,'DIE LINKE':'#BE3075','GRÜNE':'#008939','Die Linke':'#BE3075'}
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False})


def de(value):return f'{int(value):,}'.replace(',','.')


class Report:
    def __init__(self,key,out,status,dry):
        self.key,self.out,self.status,self.dry=key,out,status,dry
        self.charts=[]
        self.stamp=('VOLLSTÄNDIGE AUSZÄHLUNG · PROBELAUF · ' if dry and status['counting_complete'] else 'PROBELAUF · ' if dry else 'VOLLSTÄNDIG ERFASSTER STAND · ') + status['capture'] + ' MESZ'
    def figure(self,title,note='',height=7,width=13):
        fig,ax=plt.subplots(figsize=(width,height))
        fig.subplots_adjust(top=.78,bottom=.18,left=.12,right=.97)
        fig.text(.04,.96,ELECTIONS[self.key]+' 2026 · '+title,fontsize=18,weight='bold',va='top',color=INK)
        fig.text(.04,.895,'\n'.join(textwrap.wrap(note,140)),fontsize=10.5,color=MUTED,va='top')
        fig.text(.04,.035,self.stamp+' · wahl-monitor.de',fontsize=9,color=MUTED)
        ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
        return fig,ax
    def save(self,fig,slug,title,note,source):
        for ext in ['png','svg']:fig.savefig(self.out/'charts'/f'{slug}.{ext}',dpi=150,facecolor='white',bbox_inches='tight',pad_inches=.2)
        plt.close(fig)
        self.charts.append(dict(slug=slug,title=title,note=note,source=source,path=f'charts/{slug}.png'))


def representation(r,land,ctx,party_map,seats):
    valid=land['valid_votes_zweit'];voters=ctx['voters'];eligible=ctx['eligible'];population=POPULATION[r.key][0]
    if not 0<=valid<=voters<=eligible<=population:raise ValueError('Invalid representation chain')
    second={p:v for (rk,t,p),v in party_map.items() if rk==land['row_key'] and t=='Zweitstimmen'}
    if seats is not None:
        represented={x['party'] for x in seats if int(x['seats'])>0}
        if represented-set(second):raise ValueError('Seat party names must match the result party names')
        basis='Stimmen für Parteien mit Sitzen laut amtlicher Sitzquelle'
    else:
        represented={p for p,v in second.items() if v/valid>=.05}
        first={}
        for (rk,t,p),v in party_map.items():
            if ':WAHLKREIS:' in rk and t=='Erststimmen':first.setdefault(rk,[]).append((p,v))
        # Include all ties as possible direct winners; this is explicitly a scenario.
        for candidates in first.values():
            top=max(v for _,v in candidates)
            represented.update(p for p,v in candidates if top>0 and v==top and p in second)
        basis='Szenario: Parteien ab 5 % oder mit aktueller Erststimmenführung; keine amtliche Sitzfeststellung'
    represented_votes=sum(second[p] for p in represented)
    rows=[]
    def total(label,value,role='total'):rows.append(dict(label=label,amount=value,start=0,end=value,role=role))
    def subtract(label,value,start,role='excluded'):
        rows.append(dict(label=label,amount=value,start=start,end=start-value,role=role));return start-value
    total('Einwohner',population)
    current=subtract('Differenz zum\nWahlberechtigten-\nstand*',population-eligible,population)
    total('Wahlberechtigte',current)
    label='Nichtwählende' if r.status['counting_complete'] else 'Noch nicht als\nWählende erfasst**'
    current=subtract(label,eligible-voters,current);total('Erfasste\nWählende',current)
    current=subtract('Ungültige\nZweitstimmen',ctx['invalid'],current);total('Gültige\nZweitstimmen',current)
    current=subtract('Übrige Parteien' if seats is None else 'Parteien\nohne Sitze',valid-represented_votes,current)
    total('Repräsentierte\nZweitstimmen'+(' (Szenario)' if seats is None else ''),current,'represented')
    for p in sorted(represented,key=lambda p:-second[p]):current=subtract(p,second[p],current,'party')
    assert current==0 and rows[8]['amount']==represented_votes
    write_csv(r.out/'representation_waterfall.csv',rows)
    save_json(r.out/'representation_sources.json',dict(population=population,population_date='2025-12-31',
        population_url=POPULATION[r.key][1],eligible_voters=eligible,voters=voters,valid_second_votes=valid,
        represented_votes=represented_votes,represented_share_valid=100*represented_votes/valid,
        basis=basis,official_seats=seats is not None,represented_parties=sorted(represented)))
    note=f'{basis}. {de(represented_votes)} Stimmen · {100*represented_votes/valid:.2f} % der erfassten gültigen Zweitstimmen.'
    fig,ax=r.figure('Politische Repräsentation',note,height=9,width=19)
    fig.subplots_adjust(left=.065,bottom=.30,top=.79)
    for i,x in enumerate(rows):
        color=COLORS.get(x['label'],INK) if x['role']=='party' else ORANGE if x['role']=='excluded' else BLUE if x['role']=='represented' else '#718092'
        bottom=x['end'] if x['role'] in {'party','excluded'} else 0
        ax.bar(i,x['amount'],bottom=bottom,color=color,width=.72,hatch='///' if i==1 else None,zorder=3)
        inside=x['amount']>population*.07
        ax.text(i,bottom+x['amount']/2 if inside else bottom+x['amount']+population*.018,
                ('≈ ' if i==1 else '')+de(x['amount']),ha='center',va='center' if inside else 'bottom',fontsize=9,
                color=label_color_for_fill(color) if inside else INK,weight='bold')
        if i<len(rows)-1:ax.plot([i+.36,i+.64],[x['end']]*2,'--',color=MUTED,lw=.7)
    ax.set_xticks(range(len(rows)),[x['label'].replace(' (Szenario)','\n(Szenario)') for x in rows],fontsize=9)
    ax.set_ylim(0,population*1.08);ax.set_ylabel('Personen / Zweitstimmen')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v,pos:f'{v/1e6:.1f} Mio.'))
    fig.text(.04,.18,'* Bevölkerung: 31.12.2025; Wahlberechtigte: Wahlstand 2026. Die Differenz ist nur eine zeitliche Annäherung.',fontsize=11,color=MUTED)
    fig.text(.04,.13,'** Bei offener Auszählung umfasst dieser Rest auch noch nicht erfasste Wählende.' if not r.status['counting_complete'] else 'Vollständige Erfassung ist keine Feststellung des endgültigen amtlichen Ergebnisses.',fontsize=11,color=MUTED)
    fig.text(.04,.08,'Die Grafik zeigt Stimmen für Parteien, keine Sitzanteile und keine Aussagen über einzelne Bevölkerungsgruppen.',fontsize=11,color=MUTED)
    r.save(fig,'01_politische_repraesentation','Politische Repräsentation',note,'representation_waterfall.csv; representation_sources.json')


def berlin_voting_modes(r):
    """Use the published mode percentages, retaining their own evidence timestamp."""
    class Charts(HTMLParser):
        def __init__(self):super().__init__();self.charts=[]
        def handle_starttag(self,tag,attrs):
            a=dict(attrs)
            if a.get('data-chartoptions') and a.get('data-chartdata'):
                options=json.loads(a['data-chartoptions'])
                if options.get('texte',{}).get('title')=='Vergleich Urne-/Briefwahl: Zweitstimmen':
                    self.charts.append((options,json.loads(a['data-chartdata'])))
    parser=Charts();parser.feed((r.out/'sources/official_voting_modes.html').read_text())
    if len(parser.charts)!=1:raise ValueError('Expected one official Berlin mode chart')
    options,chart=parser.charts[0]
    if 'Berlin,' not in options['texte']['subTitle'] or '2026' not in options['texte']['subTitle']:
        raise ValueError('Wrong Berlin mode chart scope')
    if 'Urnen' not in options['legende'][0]['label'] or 'Brief' not in options['legende'][1]['label']:
        raise ValueError('Unexpected voting-mode series order')
    time=re.search(r'(\d{2}\.\d{2}\.\d{4}, \d{2}:\d{2}:\d{2})',options['texte']['info'])
    if time is None:raise ValueError('Mode source timestamp missing')
    data=[]
    for mode,series in zip(['Urnenwahl','Briefwahl'],chart['dataSeries']):
        values=[float(x['value']) for x in series['dataSets']]
        if not all(0<=v<=100 for v in values) or abs(sum(values)-100)>.11:
            raise ValueError('Published mode percentages do not sum to 100 within rounding tolerance')
        data.extend(dict(mode=mode,party=x['label'],share=float(x['value']),votes=None,valid=None,
                         source_time=time[1]) for x in series['dataSets'])
    write_csv(r.out/'voting_modes.csv',data)
    selected=list(dict.fromkeys(x['party'] for x in data if x['share']>5 and x['party']!='Sonstige'))
    note=f'Amtlich publizierte Prozentwerte je Modus · {time[1]} MESZ · Parteien über 5 % in mindestens einem Modus. Absolute Stimmen/Nenner fehlen.'
    fig,ax=r.figure('Briefwahl und Urnenwahl',note)
    for j,mode in enumerate(['Urnenwahl','Briefwahl']):
        ax.bar(np.arange(len(selected))+(j-.5)*.38,[next(x['share'] for x in data if x['party']==p and x['mode']==mode) for p in selected],.36,label=mode,color=[BLUE,ORANGE][j])
    ax.set_xticks(range(len(selected)),selected);ax.set_ylabel('Zweitstimmenanteil (%)');ax.legend()
    r.save(fig,'04_brief_urne','Briefwahl und Urnenwahl',note,'voting_modes.csv; sources/official_voting_modes.html')
    save_json(r.out/'voting_modes_source.json',dict(url='https://www.wahlen-berlin.de/wahlen/Be2026/AFSPRAES/agh/index.html',
        time=time[1],coverage=options['texte']['info'],sha256=sha((r.out/'sources/official_voting_modes.html').read_bytes()),
        note='Published percentages only; no inferred absolute counts. Evidence time is independent of the normalized CSV capture.'))


def result_charts(r,snapshots,party_map,context,land,selected):
    rows=[]
    for p in selected:
        rows.append(dict(party=p,erst=100*party_map.get((land['row_key'],'Erststimmen',p),0)/land['valid_votes_erst'],
                         zweit=100*party_map[(land['row_key'],'Zweitstimmen',p)]/land['valid_votes_zweit']))
    write_csv(r.out/'party_results.csv',rows)
    fig,ax=r.figure('Erst- und Zweitstimmen','Parteien ab 1 % der erfassten Zweitstimmen; Anteile an den jeweils gültigen Stimmen.')
    x=np.arange(len(rows));ax.bar(x-.19,[a['erst'] for a in rows],.36,color=[COLORS.get(a['party'],MUTED) for a in rows],alpha=.4,label='Erststimmen')
    ax.bar(x+.19,[a['zweit'] for a in rows],.36,color=[COLORS.get(a['party'],MUTED) for a in rows],label='Zweitstimmen')
    ax.set_xticks(x,[a['party'] for a in rows]);ax.set_ylabel('Anteil (%)');ax.legend()
    r.save(fig,'02_parteien','Erst- und Zweitstimmen','Unterschiedliche Stimmarten haben eigene Nenner.','party_results.csv')
    areas=[]
    for s in snapshots:
        if not s['valid_votes_zweit']:continue
        for p in selected:
            areas.append(dict(row_key=s['row_key'],level=s['gebietsart'],name=s['municipality_name'] or s['gebietsnummer'],
                party=p,votes=party_map.get((s['row_key'],'Zweitstimmen',p),0),valid=s['valid_votes_zweit'],
                share=100*party_map.get((s['row_key'],'Zweitstimmen',p),0)/s['valid_votes_zweit'],
                mode=context.get(s['row_key'],{}).get('mode','unavailable')))
    write_csv(r.out/'results_by_area.csv',areas)
    frame=pd.DataFrame(areas)
    for level,label in [('WAHLKREIS','Wahlkreise'),('WAHLBEZIRK','Wahlbezirke')]:
        subset=frame[frame.level==level]
        if subset.empty:continue
        fig,ax=r.figure('Streuung der Zweitstimmen · '+label,'Jedes Gebiet zählt gleich. Box: mittlere 50 %; Linie: Median; Whisker: 1,5 × IQR; Punkte: Ausreißer.')
        ax.boxplot([subset[subset.party==p].share for p in selected],tick_labels=selected,showfliers=True,
                   flierprops=dict(markersize=2),patch_artist=True,boxprops=dict(facecolor='#dce8f0'))
        ax.set_ylim(0,100);ax.set_ylabel('Zweitstimmenanteil (%)')
        r.save(fig,'03_streuung_'+level.lower(),'Streuung · '+label,'Deskriptive Gebietsverteilung, keine Unsicherheitsintervalle.','results_by_area.csv')
    booths=frame[frame.level=='WAHLBEZIRK']
    if not booths.empty:
        modes=[]
        for (mode,p),group in booths.groupby(['mode','party']):
            modes.append(dict(mode=mode,party=p,votes=int(group.votes.sum()),valid=int(group.valid.sum()),share=100*group.votes.sum()/group.valid.sum()))
        write_csv(r.out/'voting_modes.csv',modes)
        fig,ax=r.figure('Briefwahl und Urnenwahl','Stimmengewichtete Anteile je Wahlmodus; Zuordnung aus den amtlichen Wahlbezirksnamen. Keine kausale Moduswirkung.')
        for j,mode in enumerate(['Urnenwahl','Briefwahl']):
            ax.bar(np.arange(len(selected))+(j-.5)*.38,[next(a['share'] for a in modes if a['party']==p and a['mode']==mode) for p in selected],.36,label=mode,color=[BLUE,ORANGE][j])
        ax.set_xticks(range(len(selected)),selected);ax.set_ylabel('Zweitstimmenanteil (%)');ax.legend()
        r.save(fig,'04_brief_urne','Briefwahl und Urnenwahl','Gesamtergebnis je Modus, gewichtet mit den gültigen Stimmen.','voting_modes.csv')
    elif r.key=='2026-be' and (r.out/'sources/official_voting_modes.html').exists():
        berlin_voting_modes(r)
    geometry=json.loads((r.out/'sources/wahlkreise.geojson').read_text())
    wks={int(s['gebietsnummer']):s for s in snapshots if s['gebietsart']=='WAHLKREIS'}
    for slug,title,group in [('05_staerkste_partei','Stärkste Partei nach Zweitstimmen',None),
                             ('06_afd_mehrheit','AfD: absolute Mehrheit der Zweitstimmen',['AfD']),
                             ('07_linke_spd_gruene','Linke, SPD und GRÜNE: gemeinsame Zweitstimmenmehrheit',['Die Linke','DIE LINKE','SPD','GRÜNE'])]:
        note='Wahlkreise; Grau = keine absolute Mehrheit.' if group else 'Wahlkreise; die Farbe zeigt die Partei mit den meisten Zweitstimmen, keine Sitzmehrheit.'
        fig,ax=r.figure(title,note,height=9)
        used=set();maprows=[]
        for f in geometry['features']:
            number=int(f['properties']['Nummer']);s=wks[number];valid=s['valid_votes_zweit'] or 0
            votes={p:v for (rk,t,p),v in party_map.items() if rk==s['row_key'] and t=='Zweitstimmen'}
            if group:
                share=sum(votes.get(p,0) for p in group)/valid if valid else 0
                winner='Mehrheit > 50 %' if share>.5 else 'Keine Mehrheit'
                color=BLUE if share>.5 else '#e1e6eb'
            else:
                highest=max(votes.values(),default=0)
                winners=[p for p,v in votes.items() if v==highest]
                winner=winners[0] if valid and len(winners)==1 else 'Gleichstand / offen'
                color=COLORS.get(winner,'#d3d9de');share=highest/valid if valid else 0
            used.add((winner,color));maprows.append(dict(wahlkreis=number,label=winner,share=share,valid=valid))
            g=f['geometry'];polygons=g['coordinates'] if g['type']=='MultiPolygon' else [g['coordinates']]
            for polygon in polygons:
                ax.add_patch(Polygon(polygon[0],closed=True,facecolor=color,edgecolor='white',linewidth=.6))
                for hole in polygon[1:]:ax.add_patch(Polygon(hole,closed=True,facecolor='white',edgecolor='white'))
        ax.autoscale()
        # Berlin is EPSG:4326; the retained MV geometry uses projected metres.
        ax.set_aspect(1/np.cos(np.deg2rad(np.mean(ax.get_ylim())))) if max(abs(v) for v in ax.get_xlim())<=180 else ax.set_aspect('equal')
        ax.set_axis_off()
        ax.legend(handles=[Patch(color=c,label=p) for p,c in sorted(used)],loc='lower left',frameon=False,fontsize=9)
        write_csv(r.out/(slug+'.csv'),maprows)
        r.save(fig,slug,title,'Kartierung der aktuellen Wahlkreisergebnisse; keine Sitzmehrheiten.',slug+'.csv; sources/wahlkreise.geojson')


def history_charts(r,selected):
    d=pd.read_csv(r.out/'counting_timeline.csv');p=pd.read_csv(r.out/'party_timeline.csv')
    if d.empty:return
    time=pd.to_datetime(d.time,utc=True).dt.tz_convert('Europe/Berlin')
    fig,ax=r.figure('Verlauf der Auszählung','Beobachtete Abrufe; Verbindungen behaupten keine lückenlose Beobachtung zwischen den Abrufen.')
    ax.step(time,100*d.reported/d.expected,where='post',color=BLUE);ax.set_ylim(0,102);ax.set_ylabel('Erfasste Wahlbezirke (%)')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M',tz=time.dt.tz));ax.set_xlabel('Abrufzeit (MESZ)')
    r.save(fig,'08_auszaehlung','Verlauf der Auszählung','Der Nenner stammt aus jedem einzelnen Abruf.','counting_timeline.csv')
    fig,ax=r.figure('Parteianteile im Verlauf','Anteile an den bis zum Abruf erfassten gültigen Zweitstimmen; keine Hochrechnung.')
    for party in selected:
        q=p[p.party==party];ax.plot(pd.to_datetime(q.time,utc=True).dt.tz_convert('Europe/Berlin'),q.share,label=party,color=COLORS.get(party,MUTED))
    ax.set_ylim(bottom=0);ax.set_ylabel('Zweitstimmenanteil (%)');ax.legend(ncol=3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M',tz=time.dt.tz))
    r.save(fig,'09_parteien_verlauf','Parteianteile im Verlauf','Frühe Ergebnisse sind geografisch selektiv.','party_timeline.csv')
    arrivals=pd.read_csv(r.out/'arrival_observations.csv')
    for kind,title in [('first_positive','AfD-Anteil beim ersten positiven Ergebnis'),('first_complete','AfD-Anteil bei erster Vollmeldung')]:
        fig,ax=r.figure(title,'Jedes Gebiet einmal je Definition; Anteil zum tatsächlichen ersten Abruf. Nicht nachträglich mit Endwerten ersetzt.')
        for level,color,marker,label in [('GEMEINDE',BLUE,'o','Meldegebiete'),('WAHLKREIS',ORANGE,'D','Wahlkreise')]:
            a=arrivals[(arrivals.kind==kind)&(arrivals.level==level)]
            ax.scatter(pd.to_datetime(a.time,utc=True).dt.tz_convert('Europe/Berlin'),a.afd_share,label=label,color=color,marker=marker,s=22,alpha=.5)
        ax.set_ylim(0,100);ax.set_ylabel('AfD-Zweitstimmenanteil (%)');ax.legend()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M',tz=time.dt.tz))
        r.save(fig,'10_ankunft_'+kind,title,'MV-Meldegebiete enthalten gesonderte Briefwahlgebiete; hier keine demografischen Einheiten.','arrival_observations.csv')


def demographic_charts(r,data,corr,strongest,models,selected):
    grid=corr.pivot(index='feature',columns='party',values='pearson_vote_weighted').reindex(index=FEATURES,columns=selected)
    fig,ax=r.figure('Demografische Zusammenhänge','Stimmengewichtete Pearson-Korrelationen zwischen Gebietseigenschaften und Parteianteilen; keine Aussagen über einzelne Wählende.',height=10)
    fig.subplots_adjust(left=.34,right=.90,bottom=.15,top=.79)
    matrix=grid.to_numpy();im=ax.imshow(np.ma.masked_invalid(matrix),vmin=-1,vmax=1,cmap='RdBu',aspect='auto')
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if np.isfinite(matrix[i,j]):ax.text(j,i,f'{matrix[i,j]:.2f}',ha='center',va='center',fontsize=9,color='white' if abs(matrix[i,j])>.65 else INK)
    ax.set_yticks(range(len(grid)),[FEATURES[f][0]+f" (n={int(corr[corr.feature==f].n.iloc[0])})" for f in grid.index],fontsize=9)
    ax.set_xticks(range(len(selected)),selected,rotation=25,ha='right');ax.grid(False)
    fig.colorbar(im,ax=ax,label='Pearson r',fraction=.035,pad=.03)
    r.save(fig,'11_demografie','Demografische Zusammenhänge','Vollständiges Merkmalsraster; unterschiedliche Abdeckung steht im Coverage-CSV.','demographic_correlations.csv; demographic_coverage.csv')
    top=strongest.set_index('party').reindex(selected).dropna(subset=['feature'])
    fig,ax=r.figure('Was verändert die Stimmengewichtung?','Pro Partei: stärkster absoluter gewichteter Zusammenhang unter voll abgedeckten Merkmalen. Explorative Auswahl aus dem gesamten Raster.',height=8)
    x=np.arange(len(top));ax.bar(x-.18,top.pearson_equal,.34,color='#bcc8d4',label='Jedes Gebiet gleich')
    ax.bar(x+.18,top.pearson_vote_weighted,.34,color=BLUE,label='Gewicht: gültige Zweitstimmen')
    ax.set_xticks(x,[p+'\n'+textwrap.fill(FEATURES[row.feature][0],19) for p,row in top.iterrows()],fontsize=9)
    ax.set_ylim(-1,1);ax.set_ylabel('Pearson r');ax.axhline(0,color=INK,lw=.6);ax.legend()
    fig.subplots_adjust(bottom=.26)
    r.save(fig,'12_stimmengewichtung','Was verändert die Stimmengewichtung?','Gewichte ändern die Fragestellung; sie liefern keine Individualdaten.','demographic_strongest.csv')
    fig,ax=r.figure('Wie empfindlich sind die Zusammenhänge?','Spanne nach Weglassen jeweils eines Kreises (MV) bzw. Bezirks (Berlin); keine Konfidenzintervalle.',height=8)
    y=np.arange(len(top));ax.hlines(y,top.leave_one_block_min,top.leave_one_block_max,color=BLUE,lw=3)
    ax.scatter(top.pearson_vote_weighted,y,color=INK,s=35);ax.axvline(0,color=MUTED,lw=.7)
    ax.set_yticks(y,[p+' · '+FEATURES[row.feature][0] for p,row in top.iterrows()],fontsize=9)
    ax.set_xlim(-1,1);ax.set_xlabel('Stimmengewichtetes Pearson r');fig.subplots_adjust(left=.39)
    r.save(fig,'13_geografische_sensitivitaet','Geografische Sensitivität','Punkt: alle Gebiete; Linie: Spannweite nach geografischem Ausschluss.','demographic_leave_one_block_out.csv')
    # Two strongest relationships, each municipality/district shown once.
    example=strongest.reindex(strongest.pearson_vote_weighted.abs().sort_values(ascending=False).index).head(2)
    for j,(_,row) in enumerate(example.iterrows(),1):
        d=data.dropna(subset=[row.feature]);d=d[d.valid_second_votes>0]
        fig,ax=r.figure(row.party+' und '+FEATURES[row.feature][0],f'{len(d)} Gebiete · Zensus 15.05.2022 · r (stimmengewichtet) = {row.pearson_vote_weighted:.2f}. Zusammenhang, keine Ursache.')
        ax.scatter(d[row.feature],100*d[row.party]/d.valid_second_votes,s=24,alpha=.7,color=COLORS.get(row.party,BLUE))
        if r.key=='2026-be':
            for _,q in d.iterrows():ax.annotate(q['name'],(q[row.feature],100*q[row.party]/q.valid_second_votes),xytext=(3,4),textcoords='offset points',fontsize=7)
        ax.set_xlabel(FEATURES[row.feature][0]);ax.set_ylabel(row.party+' · Zweitstimmen (%)');ax.set_ylim(bottom=0)
        r.save(fig,'14_demografie_scatter_'+str(j),row.party+' und '+FEATURES[row.feature][0],'Jeder Punkt ist ein Gebiet, keine Person.','demographic_areas.csv')
    if models['status']=='fit':
        m=pd.read_csv(r.out/'model_validation.csv')
        fig,ax=r.figure('Trägt das angepasste Zählmodell geografisch?','Leave-one-Kreis-out: Vorhersagen und Vergleichsanteil nur aus den Trainingskreisen; kleinerer Fehler ist besser.')
        x=np.arange(len(m));ax.bar(x-.18,m.cv_weighted_rmse_pp,.34,label='Demografisches Modell',color=BLUE)
        ax.bar(x+.18,m.baseline_weighted_rmse_pp,.34,label='Trainingsanteil ohne Demografie',color='#b9c5d0')
        ax.set_xticks(x,m.party);ax.set_ylabel('Stimmengewichteter RMSE (Prozentpunkte)');ax.legend()
        r.save(fig,'15_zaehlmodell','Geografische Validierung der Zählmodelle','Keine Prognose für Einzelpersonen; Überdispersion und Koeffizientenstabilität stehen in den CSVs.','model_validation.csv; model_predictions.csv')


def document(r,data,strongest,models,history_status,land,seats):
    esc=html.escape
    unit='Bezirke' if r.key=='2026-be' else 'Ämter und amtsfreie Gemeinden'
    count=f"{de(r.status['reported_precincts'])} von {de(r.status['total_precincts'])} Wahlbezirken"
    results=read_csv(r.out/'party_results.csv')
    leader=max(results,key=lambda q:float(q['zweit']))
    summaries=[f"{count} sind erfasst; {de(land['valid_votes_zweit'])} gültige Zweitstimmen liegen vor. {leader['party']} liegt mit {float(leader['zweit']):.2f} % der erfassten Zweitstimmen vorn.",
        f"Die demografische Analyse verknüpft {len(data)} {unit} mit dem Zensus 2022. Urnen- und Briefstimmen sind in diesen Gebieten gemeinsam enthalten."]
    if len(strongest):
        row=strongest.loc[strongest.pearson_vote_weighted.abs().idxmax()]
        summaries.append(f"Unter den vollständig abgedeckten Merkmalen zeigt {row.party} mit „{FEATURES[row.feature][0]}“ den größten absoluten gewichteten Zusammenhang (r = {row.pearson_vote_weighted:.2f}). Dies ist eine explorative Auswahl, keine kausale Erklärung.")
    limits=["Zensuswerte beschreiben alle Einwohner bzw. die angegebenen Haushalts-/Personengruppen von 2022, nicht die Wahlberechtigten von 2026. Gebietsmerkmale lassen keine individuellen Wahlentscheidungen erkennen.",
        "Altersbänder sind hier 67+ und 19–24 Jahre (veröffentlichte Zensusklassen); sie entsprechen nicht den LSA-Bändern 65+ und 18–29 aus 2025. Dichte und Bevölkerungsänderung 2024–25 werden ohne passende Datengrundlage nicht berechnet.",
        "Unterdrückte oder fehlende Zensuswerte werden nicht durch null ersetzt. Paarweise Stichprobengrößen und Stimmenabdeckung werden für jedes Merkmal ausgewiesen.",
        "Pearson und Spearman werden gleichgewichtet und stimmengewichtet ausgegeben; die binäre Stimmkorrelation bezeichnet das Wohngebiet als Kontext. Es werden keine p-Werte, Betrugssignale oder Individualeffekte behauptet.",
        "Vollständige Auszählung bedeutet einen vollständig erfassten (gegebenenfalls vorläufigen) Stand, nicht ein amtlich festgestelltes Endergebnis."]
    if not r.status['counting_complete']:limits.insert(0,"Die Auszählung ist offen. Fehlende Meldungen sind geografisch selektiv; alle Ergebnisse und Korrelationen dieses Probelaufs können sich ändern. Das CSV enthält zusätzlich eine Auswertung nur vollständig gemeldeter Gebiete.")
    if seats is None:limits.insert(0,"Die amtliche Sitzverteilung liegt diesem Bericht noch nicht bei. Der Repräsentations-Waterfall ist deshalb ausdrücklich ein Szenario, kein Nachweis tatsächlich zugeteilter Sitze.")
    if r.key=='2026-be':
        limits.append("Die aktuelle normalisierte Quelle enthält Wahlkreise und Bezirke, aber keine einzelnen Wahlbezirke. Wahlbezirksstreuung bleibt deshalb aus. Bei zwölf Bezirken wird das fünfvariable LSA-Zählmodell nicht geschätzt.")
        if (r.out/'voting_modes.csv').exists():
            limits.append("Brief-/Urnenvergleich: separat datierte, amtlich veröffentlichte Prozentwerte. Absolute Stimmen und gültige Stimmen je Modus werden in dieser Quelle nicht angegeben und nicht zurückgerechnet.")
        else:limits.append("Ein separat datierter amtlicher Brief-/Urnenvergleich liegt diesem Paket nicht bei.")
    else:limits.append("Gemeinsame Amtsbriefwahl darf keiner einzelnen Gemeinde zugerechnet werden. MV wird deshalb auf Amtsebene bzw. für amtsfreie Gemeinden analysiert. Der Ausschluss von Rostock und Schwerin sowie die acht Kreisblöcke sind separat nachprüfbar.")
    cards=[];md=[]
    findings=[]
    for _,row in strongest.iterrows():
        stable=row.leave_one_block_min*row.leave_one_block_max>0
        findings.append(dict(Partei=row.party,Merkmal=FEATURES[row.feature][0],
            **{'r gleichgewichtet':round(row.pearson_equal,3),'r stimmengewichtet':round(row.pearson_vote_weighted,3),
               'Geografischer Ausschluss':f'{row.leave_one_block_min:.2f} bis {row.leave_one_block_max:.2f}',
               'Vorzeichen bei Ausschluss':'stabil' if stable else 'wechselt / berührt null'}))
    finding_table=pd.DataFrame(findings).to_html(index=False,border=0)
    model_readout=''
    if models['status']=='fit':
        scores=pd.read_csv(r.out/'model_validation.csv')
        better=scores[scores.cv_weighted_rmse_pp<scores.baseline_weighted_rmse_pp]
        model_readout=f"Das demografische Zählmodell erzielt bei {len(better)} von {len(scores)} Parteien einen kleineren geografischen Vorhersagefehler als der jeweilige Trainingsanteil ohne Demografie. Die Überdispersion reicht von {scores.pearson_dispersion.min():.1f} bis {scores.pearson_dispersion.max():.1f}; gewöhnliche binomiale Standardfehler wären daher irreführend."
    for c in r.charts:
        encoded=base64.b64encode((r.out/c['path']).read_bytes()).decode()
        cards.append(f'<section><h2>{esc(c["title"])}</h2><p>{esc(c["note"])}</p><img src="data:image/png;base64,{encoded}" alt="{esc(c["title"])}"><p class="source">Datengrundlage: {esc(c["source"])}</p></section>')
        md += [f'## {c["title"]}',c['note'],f'![{c["title"]}]({c["path"]})',f'Daten: {c["source"]}','']
    coverage=pd.read_csv(r.out/'demographic_coverage.csv')
    table=coverage[['label','areas','missing','vote_coverage_percent']].rename(columns={'label':'Merkmal','areas':'Gebiete','missing':'Fehlend','vote_coverage_percent':'Stimmenabdeckung (%)'}).to_html(index=False,float_format=lambda v:f'{v:.2f}',border=0)
    status='Probelauf mit aktuellem Datenstand' if r.dry else 'Bericht zum vollständig erfassten Stand'
    text=f'''<!doctype html><html lang="de"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{esc(ELECTIONS[r.key])} 2026 · Nachwahlanalyse</title>
<style>body{{font:17px/1.6 system-ui,sans-serif;color:{INK};margin:0;background:#f6f8fa}}main{{max-width:1200px;margin:auto;padding:48px 24px}}h1{{font-size:38px;line-height:1.15}}h2{{font-size:26px;line-height:1.25;margin-top:0}}p{{max-width:1000px}}.status{{color:#965122;font-weight:700}}section{{margin:44px 0;background:white;padding:24px;border-top:2px solid #d8e0e8}}img{{width:100%;height:auto}}.source,small{{font-size:13px;color:{MUTED}}}table{{border-collapse:collapse;font-size:14px;width:100%}}td,th{{padding:8px;text-align:left;border-bottom:1px solid #ddd}}a{{color:#216599}}.scroll{{overflow:auto}}@media(max-width:600px){{main{{padding:24px 12px}}section{{padding:12px}}h1{{font-size:30px}}}}</style>
<main><p class="status">{esc(status)} · {esc(r.status['capture'])} MESZ</p><h1>{esc(ELECTIONS[r.key])} 2026: Stimmen, Repräsentation und demografische Zusammenhänge</h1>
<ul>{''.join('<li>'+esc(s)+'</li>' for s in summaries)}</ul>
<details open><summary><strong>Einordnung des Datenstands</strong></summary><ul>{''.join('<li>'+esc(s)+'</li>' for s in limits)}</ul></details>
<section><h2>Die stärksten Zusammenhänge je Partei</h2><p>Auswahl aus allen vollständig abgedeckten Merkmalen; positive Werte bedeuten höhere Parteianteile in Gebieten mit höherem Merkmalswert. Das Vorzeichen kann durch einzelne Gebiete geprägt sein. Die vollständigen Korrelationen stehen im CSV.</p><div class="scroll">{finding_table}</div><p>{esc(model_readout)}</p></section>
{''.join(cards)}<section><h2>Abdeckung der demografischen Merkmale</h2><div class="scroll">{table}</div></section>
<section><h2>Methodik und Reproduktion</h2><p>Erfasste Git-Stände: {history_status['captures']}. Änderungen bei gleichem Meldezähler: {history_status['fixed_counter_party_changes']}; deskriptive Datenänderungen, keine Bewertung ihrer Ursache. Originalzeitpunkte, Nenner und Parteidifferenzen stehen in den CSVs.</p><p>Modellstatus: {esc(models['status'])}. {esc(models.get('reason',models.get('method','')))}</p><p>Quellenbytes, SHA-256-Prüfsummen, Gebietszuordnung und Rechenprüfungen liegen im Berichtspaket. README.md enthält den Reproduktionsbefehl. Die HTML-Datei bettet alle Diagramme ein und funktioniert offline.</p><p><a href="{esc(POPULATION[r.key][1])}">Amtliche Bevölkerung 2025</a> · <a href="{esc('https://www.statistik-berlin-brandenburg.de/bevoelkerung/zensus/zensus2022/' if r.key=='2026-be' else 'https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Zensus2022/Publikationen/publikationen-akkordeon-regionaltabellen.html')}">Zensus 2022</a></p></section></main></html>'''
    (r.out/'report.html').write_text(text)
    (r.out/'analysis.md').write_text('# '+ELECTIONS[r.key]+' 2026 · '+status+'\n\n'+'\n\n'.join(summaries)+'\n\n'+'\n\n'.join(limits)+'\n\n'+'\n\n'.join(md))
    save_json(r.out/'chart_map.json',r.charts)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--election-key',required=True,choices=ELECTIONS)
    modes=p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--dry-run',action='store_true');modes.add_argument('--require-complete',action='store_true');modes.add_argument('--check',action='store_true')
    modes.add_argument('--complete-count',action='store_true',help='Require fully counted, reconciled votes; keep any seat scenario explicit')
    p.add_argument('--seats-file',type=Path,help='Reviewed official seat CSV: party,seats; exact result party names')
    p.add_argument('--seats-source',help='Official URL from which the seat table was extracted')
    p.add_argument('--output-dir',type=Path)
    p.add_argument('--source-dir',type=Path,help='Replay a retained report sources/ directory instead of live latest')
    args=p.parse_args()
    meta,snapshots,parties,raw,content=capture(args.election_key,args.source_dir)
    ctx=raw_context(args.election_key,raw)
    status,land,party_map=validate_capture(args.election_key,meta,snapshots,parties,ctx)
    if args.check:
        print(json.dumps({k:v for k,v in status.items() if k!='checks'},ensure_ascii=False,indent=2));return 0 if status['ready'] else 2
    if status['issues']:raise ValueError('Capture failed reconciliation: '+ '; '.join(status['issues'][:10]))
    seats=read_csv(args.seats_file) if args.seats_file else None
    if seats is not None:
        if not args.seats_source or not seats or len({s['party'] for s in seats})!=len(seats) or any(int(s['seats'])<0 for s in seats) or sum(int(s['seats']) for s in seats)<=0:
            raise ValueError('A valid official seat table and its source URL are required')
    if args.require_complete and (not status['ready'] or seats is None):
        print('Pending: all precincts must reconcile and a reviewed official seat table is required.',file=sys.stderr);return 2
    if args.complete_count and not status['ready']:
        print('Pending: all precincts must be counted and totals must reconcile.',file=sys.stderr);return 2
    mode='dry-run' if args.dry_run else 'full-count' if args.complete_count else 'complete'
    base=ROOT/'data'/args.election_key/'reports/post-election'
    out=args.output_dir or base/(mode+'-'+meta['run_label'])
    if out.exists():raise ValueError(f'Preserving existing report {out}; choose a new --output-dir for a revision')
    out.mkdir(parents=True);(out/'charts').mkdir();(out/'sources').mkdir()
    for name,payload in content.items():(out/'sources'/name).write_bytes(payload)
    for path in (args.source_dir or base/'sources').iterdir():
        if path.is_file():shutil.copy2(path,out/'sources'/path.name)
    geometry=(args.source_dir/'wahlkreise.geojson') if args.source_dir else ROOT/'data'/args.election_key/'metadata/wahlkreise.geojson'
    shutil.copy2(geometry,out/'sources/wahlkreise.geojson')
    if seats is not None:shutil.copy2(args.seats_file,out/'sources/seats.csv')
    r=Report(args.election_key,out,status,args.dry_run)
    selected=sorted([p for (rk,t,p),v in party_map.items() if rk==land['row_key'] and t=='Zweitstimmen' and v/land['valid_votes_zweit']>=.01],key=lambda p:-party_map[(land['row_key'],'Zweitstimmen',p)])
    representation(r,land,ctx[land['row_key']],party_map,seats)
    result_charts(r,snapshots,party_map,ctx,land,selected)
    data,corr,strongest,models=demographic_analysis(args.election_key,out,snapshots,party_map,ctx,selected)
    if int(data.valid_second_votes.sum())!=land['valid_votes_zweit']:
        raise ValueError('Demographic area votes do not reconcile to Land')
    history_status=history(args.election_key,out,meta,snapshots,parties)
    history_charts(r,selected)
    demographic_charts(r,data,corr,strongest,models,selected)
    document(r,data,strongest,models,history_status,land,seats)
    write_csv(out/'aggregation_checks.csv',status.pop('checks'))
    save_json(out/'validation.json',{**status,'demographic_areas':len(data),'demographic_votes':int(data.valid_second_votes.sum()),
        'land_votes':land['valid_votes_zweit'],'demographic_votes_reconcile':int(data.valid_second_votes.sum())==land['valid_votes_zweit'],
        'charts':len(r.charts),'correlations':len(corr),'official_seats':seats is not None,'html_embedded_images':len(r.charts),
        'history':history_status})
    manifest=dict(election=args.election_key,mode=mode,as_of=meta['generated_at_utc'],
                  capture=meta['run_label'],seats_source=args.seats_source,
                  scripts={name:sha((ROOT/'scripts'/name).read_bytes()) for name in
                      ['post_election_common.py','prepare_post_election_demographics.py','analyze_post_election.py','build_post_election_report.py','render_bw_second_vote_representation_waterfall.py']},
                  files={str(path.relative_to(out)):sha(path.read_bytes()) for path in sorted(out.rglob('*')) if path.is_file()})
    save_json(out/'manifest.json',manifest)
    (out/'README.md').write_text(f'''# {ELECTIONS[args.election_key]} post-election report\n\nOpen `report.html` (self-contained, offline). PNG/SVG charts and exact CSVs accompany it.\n\nMode: {manifest['mode']}; evidence cutoff: {manifest['as_of']}. This is not a certified final result.\n\nPorted from `analyze_lsa_demographics.py`, `analyze_lsa_vote_weighted.py`, the LSA timeline/additional chart scripts and the BW representation waterfall. LSA-specific anomaly stories are not assumed to occur here.\n\nRebuild current data from the repository root:\n\n```sh\npython3 scripts/build_post_election_report.py --election-key {args.election_key} {'--dry-run' if args.dry_run else '--complete-count'}\n```\n\nUse the documented `--require-complete --seats-file ... --seats-source ...` workflow for the completion report. Existing captures are retained. `sources/` preserves exact input bytes; `manifest.json` records hashes. The demographic source manifest documents its workbook/extract provenance.\n\nSee `analysis.md` for interpretation, `validation.json` for checks, `demographic_coverage.csv` for missingness, and `chart_map.json` for chart-to-CSV mapping.\n''')
    if out.parent.resolve()==base.resolve():
        def report_order(path):
            m=json.loads((path.parent/'manifest.json').read_text())
            return m['as_of'],{'dry-run':0,'full-count':1,'complete':2}[m['mode']],path.parent.name
        completed=sorted((p for p in base.glob('*/report.html') if (p.parent/'validation.json').exists()),key=report_order)
        links=''.join(f'<li><a href="{html.escape(p.parent.name)}/report.html">{html.escape(p.parent.name)}</a></li>' for p in reversed(completed))
        (base/'index.html').write_text(f'<!doctype html><html lang="de"><meta charset="utf-8"><title>{ELECTIONS[args.election_key]} Berichte</title><h1>{ELECTIONS[args.election_key]} 2026</h1><ul>{links}</ul>')
    print(out/'report.html')
    return 0


if __name__=='__main__':
    sys.exit(main())
