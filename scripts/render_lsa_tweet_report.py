#!/usr/bin/env python3
"""Render the audited LSA evidence as German tweet drafts and static PNGs."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import textwrap
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

os.environ.setdefault("MPLCONFIGDIR", "/tmp/lsa-report-matplotlib")
import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BERLIN = ZoneInfo("Europe/Berlin")
INK, MUTED, BLUE, GOLD, ORANGE, OLIVE, PINK = "#222a35", "#66717e", "#3478a5", "#b28b25", "#cc783c", "#79883a", "#b6658b"
PARTY_COLORS = {"AfD": BLUE, "CDU": INK, "SPD": ORANGE, "GRÜNE": OLIVE, "Die Linke": PINK, "BSW": GOLD}


def read_csv(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def jread(path):
    return json.loads(path.read_text())


def write_json(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)+"\n")


def num(value, digits=0):
    if value is None:
        return '—'
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def short(name):
    return name.replace(", Landeshauptstadt", "").replace(", Welterbestadt", "").replace(", Lutherstadt", "").replace(", Goethestadt", "").replace(", Salzstadt", "").replace(", Stadt", "").replace(", Flecken", "")


def clock(value):
    return datetime.fromisoformat(value).astimezone(BERLIN).strftime("%H:%M")


class Report:
    def __init__(self, path):
        self.path = path
        self.s = jread(path/"summary.json")
        self.latest = jread(path/"latest_areas.json")
        self.raw = jread(path/"latest_official_rows.json")
        self.versions = read_csv(path/"versions.csv")
        self.full = self.s.get('full_history', False)
        self.plot_versions = [v for v in self.versions if datetime.fromisoformat(v['acquired_at_local']) >= datetime(2026,9,6,18,tzinfo=BERLIN)]
        self.version_by_commit = {v["commit"]:v for v in self.versions}
        self.selected = self.s["selected_parties"]
        self.codes = [p["code"] for p in self.selected]
        self.names = [p["party"] for p in self.selected]
        self.colors = [PARTY_COLORS.get(p, BLUE) for p in self.names]
        self.chart_map = []
        self.chart_files = {}
        self.cutoff = datetime.fromisoformat(self.s["last_capture"]["acquired_at_local"])
        self.coverage = self.s["last_capture"]["coverage_percent"]
        self.complete = self.s.get('reporting_evidence', {}).get('complete_reconciled', False)
        if self.complete:
            self.coverage = 100.0
        self.footer = f"Quelle: amtliche CSV/HTML im Git-Archiv {self.s['ref'][:8]} · Abruf {self.cutoff:%d.%m.%Y %H:%M} MESZ · wahl-monitor.de"
        (path/"charts").mkdir(exist_ok=True)
        plt.rcParams.update({"font.family":"DejaVu Sans", "font.size":12, "axes.labelsize":12,
                             "axes.edgecolor":"#acb2b9", "text.color":INK, "axes.labelcolor":INK,
                             "xtick.color":MUTED, "ytick.color":INK, "figure.facecolor":"white",
                             "axes.facecolor":"white", "axes.spines.top":False, "axes.spines.right":False,
                             "svg.hashsalt":"lsa-git-report"})

    def figure(self, title, subtitle, height=8):
        fig = plt.figure(figsize=(14, height))
        fig.text(.055, .948, title, fontsize=23, fontweight="bold", va="top")
        fig.text(.055, .888, subtitle, fontsize=12, color=MUTED, va="top")
        fig.text(.055, .035, self.footer, fontsize=8.5, color=MUTED)
        return fig

    def save(self, fig, slug, question, family, fields, source, note):
        name = slug+".png"
        alt_text = " · ".join(t.get_text() for t in fig.texts[:2])
        fig.savefig(self.path/"charts"/name, dpi=140, facecolor="white", metadata={"Software":"wahl-monitor reproducible audit"})
        plt.close(fig)
        self.chart_files[slug] = "charts/"+name
        self.chart_map.append({"id":slug,"question":question,"family":family,"renderer":"Matplotlib PNG",
                               "fields":fields,"source":source,"note":note,"path":"charts/"+name,"alt_text":alt_text,
                               "palette_policy":"single-root or two-root; party identity uses five roots plus neutral",
                               "non_color_encoding":"direct labels, explicit values, line styles, or separate panels"})

    def percent(self, row, code):
        return 100*(row["parties"].get(code) or 0)/row["valid_votes_zweit"] if row.get("valid_votes_zweit") else float("nan")

    def export_results(self):
        rows=[]
        for r in self.latest.values():
            for p in self.selected:
                for vote, code in [("Zweitstimmen",p["code"]),("Erststimmen","D"+p["code"][1:])]:
                    denominator = r["valid_votes_zweit" if vote == "Zweitstimmen" else "valid_votes_erst"]
                    value = r["parties"].get(code)
                    rows.append({"key":r["key"],"level":r["level"],"name":r["name"],"party":p["party"],"vote_type":vote,
                                 "votes":value,"valid_votes":denominator,"share_percent":100*value/denominator if value is not None and denominator else None,
                                 "reported_precincts":r["reported_precincts"],"total_precincts":r["total_precincts"]})
        with (self.path/"results_by_area.csv").open("w", newline="") as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

    def charts(self):
        dates=[datetime.fromisoformat(v["acquired_at_local"]) for v in self.plot_versions]
        raw_land=defaultdict(dict)
        with gzip.open(self.path/"raw_timeline.jsonl.gz","rt") as f:
            for line in f:
                r=json.loads(line)
                if r["level"] == "LAND":raw_land[r["mode"]][r["commit"]]=r
        fig=self.figure("Auszählung im Verlauf", "CSV-Meldezähler bis 03:10 · letzter Stand: 100 % laut HTML und Gemeinde-CSV" if self.complete else f"Wahlbezirke mit Meldung in % des jeweiligen Solls · {self.cutoff:%d.%m.%Y %H:%M} MESZ: {num(self.coverage,2)} % gesamt")
        ax=fig.add_axes([.09,.17,.85,.62])
        for mode,label,color,style in [("TOTAL","Gesamt",INK,"-"),("U","Urne",BLUE,"--"),("B","Brief",ORANGE,":")]:
            values=[100*raw_land[mode][v["commit"]]["reported_precincts"]/raw_land[mode][v["commit"]]["total_precincts"] if raw_land[mode][v["commit"]]["total_precincts"] else np.nan for v in self.plot_versions]
            ax.step(dates,values,where="post",label=label if self.complete else f"{label}: {num(values[-1],2)} %",color=color,linestyle=style,lw=2.5)
        if self.complete:
            ax.scatter([dates[-1]], [100], marker='D', color=INK, label='Letzter HTML-/Gemeindestand', zorder=5)
        ax.set_ylim(0,104);ax.set_ylabel("Gemeldete Wahlbezirke (%)");self.time_axis(ax);ax.grid(axis="y",alpha=.15);ax.legend(frameon=False,loc="lower right")
        self.save(fig,"01_auszaehlung","How complete is each reporting mode?","step line",["reported_precincts","total_precincts","mode"],"raw_timeline.jsonl.gz",f"{len(self.plot_versions)} election-night observations; pre-election templates remain in the ledger, not the time-axis. Zero denominators have no defined percentage; no interpolation.")

        fig=self.figure("Zweitstimmen am eingefrorenen Stand", f"Parteien mit landesweit mehr als 5 % · {num(self.s['last_capture']['valid_votes_zweit'])} gültige Zweitstimmen · {num(self.coverage,2)} % der Wahlbezirke")
        ax=fig.add_axes([.14,.17,.77,.61]);values=[p["share_percent"] for p in self.selected]
        ax.barh(self.names,values,color=self.colors,height=.60)
        for i,p in enumerate(self.selected):ax.text(p["share_percent"]+.8,i,f"{num(p['share_percent'],2)} %  ·  {num(p['votes'])}",va="center",fontsize=13)
        ax.invert_yaxis();ax.set_xlim(0,max(values)*1.32);ax.set_xlabel("Anteil an allen gültigen Zweitstimmen (%)");ax.grid(axis="x",alpha=.12);ax.set_axisbelow(True)
        self.save(fig,"02_parteien","Which parties exceed 5% at cutoff?","horizontal bar",["party","votes","share_percent"],"summary.json","Strict >5% at cutoff; denominator includes every party, no renormalization.")

        fig=self.figure("Parteianteile während der Auszählung", "Feste Parteiauswahl vom Berichtsstand · frühe Meldungen sind keine repräsentative Stichprobe")
        ax=fig.add_axes([.09,.18,.86,.58]);styles=["-","--","-.",":",(0,(5,1,1,1)),(0,(1,1))]
        for p,color,style in zip(self.selected,self.colors,styles):
            vals=[self.percent(raw_land["TOTAL"][v["commit"]],p["code"]) for v in self.plot_versions]
            ax.step(dates,vals,where="post",label=p["party"],color=color,linestyle=style,lw=2)
        ax.set_ylim(0,65);ax.set_ylabel("Gültige Zweitstimmen (%)");self.time_axis(ax);ax.grid(axis="y",alpha=.15)
        ax.legend(ncol=6,frameon=False,loc="upper center",bbox_to_anchor=(.5,1.16),fontsize=11)
        self.save(fig,"03_parteiverlauf","How did the reported party mix change?","step line",["party","votes","valid_votes_zweit","capture"],"raw_timeline.jsonl.gz","Time ordered archive; reporting composition, not a voter-preference time series. Shared 0–65% scale.")

        cities=[self.latest[k] for k in ["lsa:GEMEINDE:15003000","lsa:GEMEINDE:15002000","lsa:GEMEINDE:15001000"]]
        city_keys={r["key"] for r in cities}
        rest=[r for r in self.latest.values() if r["level"]=="GEMEINDE" and r["key"] not in city_keys]
        rest_row={"name":"Übrige 215 Gemeinden", "parties":{p:sum(r["parties"].get(p) or 0 for r in rest) for p in self.codes},
                  "valid_votes_zweit":sum(r["valid_votes_zweit"] for r in rest),"reported_precincts":sum(r["reported_precincts"] for r in rest),"total_precincts":sum(r["total_precincts"] for r in rest)}
        self.heatmap(cities+[rest_row],"04_staedte","Die drei kreisfreien Städte im Vergleich", "Zweitstimmen in % · übrige Gemeinden nach gültigen Stimmen gewichtet · Meldestand je Zeile",height=7)
        regions=sorted([r for r in self.latest.values() if r["level"]=="KREIS"],key=lambda r:self.percent(r,self.codes[0]),reverse=True)
        self.heatmap(regions,"05_kreise","Alle 14 Kreise und kreisfreien Städte", "Zweitstimmen in % · alle Summen mit Wahlbezirken abgeglichen" if self.complete else "Zweitstimmen in % · feste Parteiauswahl >5 % im Land · Klammern: gemeldete / gesamte Wahlbezirke",height=10.5)
        districts=sorted([r for r in self.latest.values() if r["level"]=="WAHLKREIS"],key=lambda r:int(r["number"]))
        fig=self.figure("Alle 41 Wahlkreise", "Zweitstimmen in % · dieselbe Skala in beiden Tafeln · alle Summen mit Wahlbezirken abgeglichen" if self.complete else "Zweitstimmen in % · dieselbe Skala in beiden Tafeln · Meldestand in Klammern",height=12)
        for pos,subset in enumerate([districts[:21],districts[21:]]):
            ax=fig.add_axes([.20+pos*.48,.13,.28,.65])
            self.draw_heatmap(ax,subset,fontsize=8.5,numbered=True)
        self.save(fig,"06_wahlkreise","How do all constituencies compare?","heatmap",["wahlkreis","party","share","coverage"],"results_by_area.csv","41 direct official constituencies; split municipalities are never allocated wholesale; shared 0–65% scale.")

        missing=read_csv(self.path/"missing_wahlbezirke.csv")
        counts=Counter(r["Gemeinde"] for r in missing)
        top=sorted(counts.items(),key=lambda kv:(-kv[1],kv[0]))[:4]
        tail=sum(counts.values())-sum(n for _,n in top)
        labels=[short(k) for k,n in top]+([f"Weitere {len(counts)-len(top)} Gemeinden"] if tail else [])
        values=[n for k,n in top]+([tail] if tail else [])
        if not values: labels,values=["Keine offene Meldung"],[0]
        if self.full and not missing:
            fig=self.figure('Meldestand am Ende des Archivs', '2.661 gemeldete Bezirke; 2.661 individuelle Ergebnisse. Landes-CSV ohne Meldezähler.' if self.complete else 'HTML und Gemeindesumme sind vollständig; die Landes-CSV weist weiterhin einen Bezirk weniger aus.')
            entries=[('Amtliche HTML-Übersicht',self.s['status_last_capture']['overview_reported'],self.s['status_last_capture']['overview_rows']),
                     ('Summe der Gemeinde-CSV',sum(r['reported_precincts'] for r in self.latest.values() if r['level']=='GEMEINDE'),sum(r['total_precincts'] for r in self.latest.values() if r['level']=='GEMEINDE')),
                     ('Amtliche Landes-CSV',self.s['last_capture']['reported_precincts'],self.s['last_capture']['total_precincts'])]
            for y,(label,reported,total) in zip([.67,.48,.29],entries):
                fig.text(.09,y,label,fontsize=19);fig.text(.77,y,f'{num(reported)} / {num(total)}',ha='center',fontsize=26,weight='bold',color=BLUE)
            fig.text(.09,.13,'— = Spalten im vorläufigen Landesexport entfernt. Stimmen stimmen auf allen Gebietsebenen überein.' if self.complete else '0 offene Statuszeilen im HTML. Die Differenz zur Landes-CSV ist separat nach Stimmen geprüft.',fontsize=11,color=MUTED)
            self.save(fig,'07_offene_meldungen','Do reporting counters agree at archive cutoff?','comparison table',['source surface','reported','total'],'summary.json; latest_areas.json','Exact counter lookup across overlapping source surfaces. Removed final CSV counters remain missing; final vote aggregates independently reconcile.' if self.complete else 'Exact counter lookup across overlapping source surfaces. Zero open HTML statuses does not reconcile stale higher-level CSV aggregates.')
            self.anomaly_charts()
            return
        fig=self.figure("Welche Wahlbezirke fehlen noch?", f"{len(missing)} offene Meldungen in {len(counts)} Gemeinden · Statusübersicht, Stand {self.cutoff:%d.%m. %H:%M} MESZ")
        ax=fig.add_axes([.31,.20,.60,.56]);ax.barh(labels,values,color=BLUE,height=.6)
        for i,v in enumerate(values):ax.text(v+.3,i,str(v),va="center",fontsize=15)
        ax.invert_yaxis();ax.set_xlim(0,max(values)+4);ax.set_xlabel("Noch nicht gemeldete Wahlbezirke (Anzahl)");ax.grid(axis="x",alpha=.15);ax.set_axisbelow(True)
        self.save(fig,"07_offene_meldungen","Where are the remaining reports?","horizontal bar",["Gemeinde","unreported count"],"missing_wahlbezirke.csv","Four largest municipalities plus explicitly aggregated remainder; full identity list in CSV.")
        self.anomaly_charts()

    def anomaly_charts(self):
        losses=self.s["status_losses"]
        if losses:
            event=losses[0]
            observations=[]
            with gzip.open(self.path/"status_timeline.jsonl.gz","rt") as f:
                for line in f:
                    r=json.loads(line)
                    if r["key"] == event["key"]: observations.append(r)
            dates=[datetime.fromisoformat(self.version_by_commit[r["commit"]]["acquired_at_local"]) for r in observations]
            vals=[r["reported_precincts"] for r in observations]
            fig=self.figure("Eine Meldung wird zurückgenommen",f"{event['name']} · Statushistorie; Einzelstimmen zu den beiden historischen Zeitpunkten fehlen")
            ax=fig.add_axes([.16,.24,.78,.49]);ax.step(dates,vals,where="post",color=BLUE,lw=2.5);ax.scatter(dates,vals,color=BLUE,s=15)
            ax.set_yticks([0,1],["Nicht gemeldet","Gemeldet"]);ax.set_ylim(-.2,1.3);self.time_axis(ax);ax.grid(axis="y",alpha=.15)
            reset=datetime.fromisoformat(event["acquired_at_local"])
            ax.annotate(f"{clock(event['acquired_at_local'])}: 1 → 0",(reset,0),xytext=(12,48),textcoords="offset points",arrowprops={"arrowstyle":"-","color":MUTED},fontsize=13)
            restored=event["restored_capture"]
            if restored:
                dt=datetime.fromisoformat(restored["acquired_at_local"])
                ax.annotate(f"{clock(restored['acquired_at_local'])}: wieder 1",(dt,1),xytext=(-30,-65),textcoords="offset points",arrowprops={"arrowstyle":"-","color":MUTED},fontsize=13)
            fig.text(.16,.12,"Punkte = archivierte Abrufe; Änderungen zwischen Abrufen sind zeitlich nur eingegrenzt.",fontsize=11,color=MUTED)
            self.save(fig,"08_status_ruecknahme","Was any previously reported district reset?","binary step timeline",["precinct identity","reported status","capture"],"status_timeline.jsonl.gz","Exact archived observed values. Step line is not proof of continuous state between captures; first recorded loss shown.")

        revisions=self.s["municipality_revisions"]
        changed_totals=[r for r in revisions if any(f in r["changes"] for f in ["voters_total","valid_votes_erst","valid_votes_zweit"])]
        fig=self.figure("Spätere Änderungen von Summen",f"{len(changed_totals)} Gemeinde-Änderungen bei gleicher Zahl gemeldeter Wahlbezirke · Parteienverschiebungen separat im Audit",height=max(9,4+.5*len(changed_totals)))
        axes=[fig.add_axes([.32+i*.215,.19,.185,.59]) for i in range(3)]
        labels=[short(r["name"])+" · "+clock(r["acquired_at_local"]) for r in changed_totals]
        for ax,field,title in zip(axes,["voters_total","valid_votes_erst","valid_votes_zweit"],["Wählende","Gültige Erststimmen","Gültige Zweitstimmen"]):
            vals=[r["changes"].get(field,{}).get("delta",0) for r in changed_totals]
            ax.barh(range(len(vals)),vals,color=[ORANGE if v<0 else BLUE for v in vals],height=.55)
            for i,v in enumerate(vals):ax.text(v+(.6 if v>=0 else -.6),i,f"{v:+}" if v else "0",ha="left" if v>=0 else "right",va="center",fontsize=11)
            ax.set_yticks(range(len(vals)),labels if field=="voters_total" else [""]*len(vals),fontsize=10)
            ax.invert_yaxis();ax.axvline(0,color=INK,lw=.8);ax.set_title(title,fontsize=12,pad=14)
            ax.set_xlim(-49,10);ax.set_xticks([-40,-20,0]);ax.set_xlabel("Änderung (Anzahl)");ax.grid(axis="x",alpha=.12)
        fig.text(.32,.10,"Gleiche Skala in allen Tafeln. Änderungen sind Beobachtungen, keine Ursachenfeststellung.",fontsize=10,color=MUTED)
        self.save(fig,"09_summenrevisionen","Which fixed-reporting changes affected totals?","faceted signed bar",["municipality","capture","voter and valid-vote deltas"],"summary.json:municipality_revisions","Shows all revisions affecting sums; party-only redistributions are included in the full ledger, not visible here.")

        fig=self.figure("Übersicht und CSV laufen zeitweise auseinander", "Differenz der gemeldeten Wahlbezirke: HTML-Übersicht minus Landes-CSV · keine Differenz von Stimmen")
        versions=[v for v in self.versions if v["overview_minus_csv"] != ""]
        dates=[datetime.fromisoformat(v["acquired_at_local"]) for v in versions];vals=[int(v["overview_minus_csv"]) for v in versions]
        ax=fig.add_axes([.10,.24,.83,.55]);ax.axhline(0,color=INK,lw=1);ax.step(dates,vals,where="post",color=BLUE,lw=2);ax.scatter(dates,vals,color=BLUE,s=16)
        ax.set_ylabel("Differenz (Wahlbezirke)");self.time_axis(ax);ax.grid(axis="y",alpha=.15)
        i=max(range(len(vals)),key=lambda i:abs(vals[i]));ax.annotate(f"{vals[i]:+} Wahlbezirke",(dates[i],vals[i]),xytext=(45,-15),textcoords="offset points",arrowprops={"arrowstyle":"-","color":MUTED})
        fig.text(.10,.11,f"{sum(v != 0 for v in vals)} von {len(vals)} vergleichbaren Abrufen weichen ab. " + ('Letzter CSV-Stand ohne Zähler: kein Vergleich möglich.' if self.complete else f'Am Berichtsstand beträgt die Differenz {vals[-1]}.'),fontsize=11,color=MUTED)
        self.save(fig,"10_quellenversatz","Do source surfaces agree on reporting progress?","step line",["overview_minus_csv","capture"],"versions.csv","Different official publication surfaces may update asynchronously; do not infer missing ballots from this count difference.")

        u,b=self.raw["lsa:LAND:15:U"],self.raw["lsa:LAND:15:B"]
        fig=self.figure("Urnen- und Briefwahl: unterschiedliche Parteianteile",f"Gültige Zweitstimmen: Urne {num(u['valid_votes_zweit'])} · Brief {num(b['valid_votes_zweit'])} · je eigener Nenner")
        ax=fig.add_axes([.15,.20,.77,.57]);a=[self.percent(u,p) for p in self.codes];z=[self.percent(b,p) for p in self.codes]
        for i,(x,y) in enumerate(zip(a,z)):
            ax.plot([x,y],[i,i],color="#b7bec6",lw=2)
            ax.text(x+.65,i-.10,num(x,2),color=BLUE,fontsize=11)
            ax.text(y+.65,i+.25,num(y,2),color=ORANGE,fontsize=11)
        ax.scatter(a,range(len(a)),s=80,color=BLUE,label="Urne",zorder=3)
        ax.scatter(z,range(len(z)),s=75,facecolors="white",edgecolors=ORANGE,linewidths=2,marker="s",label="Brief",zorder=3)
        ax.set_yticks(range(len(a)),self.names);ax.invert_yaxis();ax.set_xlim(0,max(a+z)+7);ax.set_xlabel("Anteil an gültigen Zweitstimmen des jeweiligen Wahltyps (%)");ax.grid(axis="x",alpha=.15)
        ax.legend(frameon=False,ncol=2,loc="lower right")
        self.save(fig,"11_urne_brief","How does the observed vote mix differ by reporting mode?","paired dot",["party","mode","share"],"latest_official_rows.json","Valid-vote weighted statewide totals; voting mode is self-selected and observational." + ("" if self.complete else " Reporting is incomplete."))

        # A share distribution describes equal-weight geographic units, not voters.
        fig=self.figure("Streuung zwischen Gemeinden und Wahlkreisen", "Zweitstimmen in % · jede Gebietseinheit gleich gewichtet · große Punkte zeigen die Landesanteile",height=9)
        for ix,(level,label) in enumerate([("GEMEINDE","218 Gemeinden"),("WAHLKREIS","41 Wahlkreise")]):
            ax=fig.add_axes([.15+ix*.45,.19,.36,.58])
            rows=[r for r in self.latest.values() if r["level"]==level]
            arrays=[[self.percent(r,c) for r in rows if r["valid_votes_zweit"]] for c in self.codes]
            box=ax.boxplot(arrays,vert=False,tick_labels=self.names,patch_artist=True,widths=.5,showfliers=True,
                           medianprops={"color":INK},flierprops={"marker":".","markersize":3,"markerfacecolor":MUTED,"markeredgecolor":MUTED})
            for patch in box["boxes"]:patch.set(facecolor="#d8e6ef",edgecolor=BLUE)
            ax.scatter([p["share_percent"] for p in self.selected],range(1,len(self.selected)+1),color="#c62828",s=30,zorder=4)
            ax.invert_yaxis();ax.set_xlim(0,75);ax.set_title(label);ax.set_xlabel("Zweitstimmenanteil (%)");ax.grid(axis="x",alpha=.15)
            if ix:ax.set_yticklabels([])
        fig.text(.15,.10,"Box = mittlere 50 % der Gebiete; Linie = Median; Whisker = 1,5 × IQR; kleine Punkte = Ausreißer.",fontsize=10,color=MUTED)
        self.save(fig,"12_gebietsstreuung","How much do party shares vary within geographic levels?","box plot",["party","level","local share"],"results_by_area.csv","Equal area weight, not voter weight. No inference of irregularity from geographic outliers.")

    def time_axis(self, ax):
        ax.xaxis.set_major_locator(mdates.HourLocator(tz=BERLIN))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M",tz=BERLIN))
        ax.set_xlabel("Abrufzeit am 6./7. September 2026 (MESZ)")

    def draw_heatmap(self,ax,rows,fontsize=11,numbered=False):
        vals=np.array([[self.percent(r,p) for p in self.codes] for r in rows])
        cmap=LinearSegmentedColormap.from_list("lsa_blue",["#f3f7fa","#8ab2cd",BLUE,"#1d435f"])
        ax.imshow(vals,aspect="auto",vmin=0,vmax=65,cmap=cmap)
        labels=[(f'{int(r["number"]):02d} ' if numbered else '')+short(r['name'])+(f" ({r['reported_precincts']}/{r['total_precincts']})" if r['total_precincts'] is not None else '') for r in rows]
        ax.set_yticks(range(len(rows)),labels,fontsize=fontsize)
        ax.set_xticks(range(len(self.names)),["Linke" if n=="Die Linke" else n for n in self.names],fontsize=fontsize)
        ax.xaxis.tick_top();ax.tick_params(length=0,pad=6)
        for i in range(len(rows)):
            for j in range(len(self.names)):
                ax.text(j,i,num(vals[i,j],1),ha="center",va="center",fontsize=fontsize,color="white" if vals[i,j]>38 else INK)
        for spine in ax.spines.values():spine.set_visible(False)

    def heatmap(self,rows,slug,title,subtitle,height=8):
        fig=self.figure(title,subtitle,height);ax=fig.add_axes([.34,.14,.59,.63]);self.draw_heatmap(ax,rows)
        self.save(fig,slug,title,"heatmap",["area","party","share","coverage"],"results_by_area.csv","Percentages use all valid second votes. Ordered rows, numeric cells, single blue root, shared 0–65% scale.")

    def narrative(self, baseline=None):
        s=self.s;last=s["last_capture"];land=self.latest["lsa:LAND:15"]
        revisions=s["municipality_revisions"];distinct=len({e['key'] for e in revisions})
        after_complete=sum("revision_after_complete" in e["event"] for e in revisions)
        u,b=self.raw["lsa:LAND:15:U"],self.raw["lsa:LAND:15:B"]
        losses=s["status_losses"];withdrawal=losses[0] if losses else None
        tweets=[]
        def add(body, charts, evidence):
            tweets.append({"text":body,"images":[self.chart_files[c] for c in charts if c in self.chart_files],"evidence":evidence})
        add(f"Landtagswahl Sachsen-Anhalt: Git-Audit von {s['versions']} archivierten Abrufen. Stand {self.cutoff:%d.%m. %H:%M} MESZ: {num(last['reported_precincts'])}/{num(last['total_precincts'])} Wahlbezirke ({num(self.coverage,2)} %). Ein Zwischenstandsbericht als Basis für den 100%-Lauf.",["01_auszaehlung"],["versions.csv"])
        add("Zweitstimmen am Berichtsstand: "+", ".join(f"{p['party']} {num(p['share_percent'],2)} %" for p in self.selected)+". Auswahl: landesweit strikt über 5 %. Nenner: alle gültigen Zweitstimmen.",["02_parteien"],["summary.json:selected_parties"])
        add("Parteianteile verändern sich mit den eingehenden Gebieten. Das ist eine Zeitreihe der Auszählung, keine Messung wechselnder Wählerpräferenzen. Frühe Ergebnisse sind besonders selektiv; fehlende Meldungen können den Stand noch verändern.",["03_parteiverlauf"],["raw_timeline.jsonl.gz"])
        add(f"Noch offen: {u['total_precincts']-u['reported_precincts']} Urnen- und {b['total_precincts']-b['reported_precincts']} Briefwahlbezirke. Die erfassten Parteianteile unterscheiden sich deutlich nach Wahltyp. Später eingehende Briefwahl kann deshalb die Gesamtanteile verschieben; die Grafik ist keine Prognose.",["11_urne_brief"],["latest_official_rows.json:lsa:LAND:15:U/B"])
        add("Magdeburg, Halle und Dessau-Roßlau zeigen andere Parteianteile als die übrigen Gemeinden. Die Vergleichsgruppe wird aus Stimmen summiert, nicht aus Gemeindeprozenten gemittelt. Der jeweilige Meldestand steht in der Grafik.",["04_staedte"],["results_by_area.csv"])
        add("Alle 14 Kreise/kreisfreien Städte im Vergleich: Zweitstimmenanteile derselben landesweiten Parteiauswahl. Ein lokaler Anteil unter 5 % bleibt sichtbar. Verschiedene Gebietsebenen werden nicht miteinander aufsummiert.",["05_kreise"],["results_by_area.csv"])
        add("Alle 41 Wahlkreise und die Streuung in 218 Gemeinden: klare regionale Unterschiede, für sich genommen kein Unregelmäßigkeitsnachweis. Wahlkreise stammen direkt aus dem amtlichen Export; geteilte Gemeinden werden nicht pauschal zugeordnet.",["06_wahlkreise","12_gebietsstreuung"],["results_by_area.csv","latest_areas.json"])
        if withdrawal:
            restored=withdrawal["restored_capture"]
            ending=f"Ab {clock(restored['acquired_at_local'])} wieder gemeldet." if restored else "Bis zum Berichtsstand keine Rückkehr beobachtet."
            add(f"Auffälliger Statuswechsel: {short(withdrawal['name'])} war gemeldet, ab Abruf {clock(withdrawal['acquired_at_local'])} nicht mehr. {ending} Belegt ist der Meldestatus; Einzelstimmen dieses Bezirks fehlen im Archiv.",["08_status_ruecknahme"],["summary.json:status_losses","status_timeline.jsonl.gz"])
        else:
            add("In den archivierten Bezirk-Statusdaten wurde kein Rückgang einer Meldung beobachtet. Das gilt nur für die erfassten Abrufe; Veränderungen zwischen zwei Abrufen können unentdeckt bleiben.",[],["status_events.csv"])
        add(f"Bei gleichbleibender Zahl gemeldeter Wahlbezirke wurden {len(revisions)} Änderungen in {distinct} Gemeinden beobachtet, davon {after_complete} nach vollständiger Meldung. Dazu zählen auch Parteiverschiebungen bei unveränderter Gesamtsumme. Vollständige Vorher/Nachher-Liste im Audit.",["09_summenrevisionen"],["summary.json:municipality_revisions","candidate_events.csv"])
        # Examples are included only while they remain in the selected archive.
        annaburg=next((e for e in revisions if e['name']=='Annaburg, Stadt' and e['changes'].get('party:D4',{}).get('delta')==17),None)
        if annaburg:
            add("Beispiel Annaburg: SPD-Erststimmen 151 → 168 (+17), FDP 85 → 68 (−17), Summe unverändert. Bad Dürrenberg: 42 Wählende weniger; gültige Stimmen unverändert, ungültige Erst- und Zweitstimmen jeweils −42. Die Ursachen sind nicht dokumentiert.",["09_summenrevisionen"],["raw_candidate_events.csv","candidate_events.csv"])
        add(f"Arithmetik: {num(s['aggregation_checks'])} Vergleiche von Gemeinden, Kreisen und Wahlkreisen mit ihren Summen; {num(s['mode_checks'])} Vergleiche Urne + Brief = Gesamt. Nicht-null Abweichungen: {s['aggregation_nonzero']} bzw. {s['mode_nonzero']}. Wiederholte Datenstände sind mitgezählt.",[],["aggregation_checks.csv.gz","mode_nonzero.csv","summary.json"])
        add(f"Die Quellen sind nicht immer synchron: In {s['overview_csv_mismatching_captures']} von {s['status_observations']} gemeinsamen Abrufen unterscheiden sich Übersicht und CSV beim Meldestand. Solche Publikationsunterschiede sind nicht mit verlorenen Stimmen gleichzusetzen.",["10_quellenversatz"],["versions.csv"])
        add(f"Am Berichtsstand fehlen {s['missing_precincts']} Wahlbezirke. Die vollständige Liste enthält Wahlkreis, Kreis, Gemeinde und Bezirksnummer. Damit lässt sich beim nächsten Lauf prüfen, welche Lücken geschlossen wurden und ob neue Statusrücknahmen hinzukamen.",["07_offene_meldungen"],["missing_wahlbezirke.csv"])
        booth_count=s['levels_latest'].get('WAHLBEZIRK',0)
        add(f"Prüfgrenze: Einzel-Wahlbezirksergebnisse mit Stimmen sind im Berichtsstand für {booth_count} Bezirke vorhanden. Die Statushistorie beginnt erst um {clock(s['status_first_capture']['acquired_at_local'])}. Rücknahmen oder Korrekturen außerhalb der erfassten Zeitpunkte sind nicht ausschließbar.",[],["summary.json","versions.csv"])
        add("Der 100%-Lauf wird mit neuer Git-Referenz reproduziert und gegen diesen Stand verglichen. Vollständige Meldung ist nicht gleich amtliches Endergebnis. Änderungen sind belegt; ihre Ursachen und mögliche Wahlunregelmäßigkeiten brauchen zusätzliche Belege.",[],["METHODS.md"])
        count=len(tweets)
        for i,t in enumerate(tweets,1):
            t["text"]=f"{i}/{count} "+t["text"]
            # X's standard weighting ranges: Latin and usual punctuation = 1;
            # symbols outside these ranges = 2. Draft bodies contain no URLs.
            t["weighted_characters"]=sum(1 if ord(c)<=0x10ff or 0x2000<=ord(c)<=0x200d or 0x2010<=ord(c)<=0x201f or 0x2032<=ord(c)<=0x2037 else 2 for c in t["text"])
            if t["weighted_characters"]>280:
                raise ValueError(f"Tweet {i} is too long ({t['weighted_characters']}): {t['text']}")
            t["alt_text"]=[next(c['alt_text'] for c in self.chart_map if c['path']==img) for img in t['images']]
        write_json(self.path/"tweets.json",tweets)
        (self.path/"tweets.txt").write_text("\n\n".join(t["text"] for t in tweets)+"\n")
        write_json(self.path/"chart_map.json",self.chart_map)
        lines=[f"# Sachsen-Anhalt: Git-Audit bei {num(self.coverage,2)} %", "", "## Executive Summary", "",
               f"- **Eine Statusrücknahme ist beobachtet:** {len(losses)} Bezirk-Statusrückgänge; Einzelstimmen sind separat auf Verfügbarkeit zu prüfen.",
               f"- **{len(revisions)} Revisionen in {distinct} Gemeinden bei gleichem Meldestand:** {after_complete} nach vollständiger Meldung. Korrekturen und ihre Summenfortschreibung sind keine unabhängigen Vorfälle.",
               f"- **Arithmetische Prüfung und Quellennachweis:** {len(s['arithmetic_issues'])} arithmetische Fehler, {len(s['source_replay_differences'])} Unterschiede zwischen normalisiertem Export und amtlichem CSV; Publikationsversatz zwischen HTML und CSV bleibt sichtbar.",
               "",f"Stand: **{self.cutoff:%d.%m.%Y %H:%M:%S} MESZ**, Git `{s['ref']}`. Kein amtliches Endergebnis. Entwürfe wurden nicht veröffentlicht.","",
               "## Tweet-Serie mit Bildern", "", "Jeder nummerierte Absatz ist ein eigener Tweet (maximal 280 gewichtete Zeichen). Belege und Bildhinweise gehören zum Bericht, nicht zum Tweettext.",""]
        for i,t in enumerate(tweets,1):
            lines.extend([f"### Tweet {i}","",t['text'],""])
            for img,alt in zip(t['images'],t['alt_text']):lines.extend([f"![{alt}]({img})",""])
            lines.extend(["Belege: "+", ".join(f"[{e}]({e.split(':')[0]})" for e in t['evidence'])+".",""])
        lines.extend(["## Vollständige Liste: Revisionen bei gleichem Meldestand", "", "Eine Zeile ist eine Gemeinde und ein Übergang zwischen zwei Abrufen. Gezählt werden auch Änderungen einzelner Parteien unterhalb von 5 %. Negative und positive Werte können sich in Summen aufheben.","",
                      "| Abruf MESZ | Gemeinde | Gemeldet / Soll | Beobachtete Änderungen | Git vorher → nachher |", "|---|---|---:|---|---|"])
        names={code:name for r in self.latest.values() for code,name in r['party_names'].items()}
        labels={"voters_total":"Wählende","valid_votes_erst":"gültige Erst","valid_votes_zweit":"gültige Zweit"}
        for e in revisions:
            changes=[]
            for field,d in e['changes'].items():
                if field.startswith('party:'):
                    code=field.split(':')[1];label=names.get(code,code)+(' (E)' if code.startswith('D') else ' (Z)')
                else:label=labels.get(field,field)
                changes.append(f"{label}: {d['before']} → {d['after']} ({d['delta']:+})")
            previous_url=f"https://github.com/volzinnovation/wahl-monitor.de/commit/{e['previous_commit']}"
            current_url=f"https://github.com/volzinnovation/wahl-monitor.de/commit/{e['commit']}"
            lines.append(f"| {clock(e['acquired_at_local'])} | {short(e['name'])} | {e['reported']}/{e['total']} | {'; '.join(changes)} | [{e['previous_commit'][:8]}]({previous_url}) → [{e['commit'][:8]}]({current_url}) |")
        extra=[e for e in s['raw_municipality_candidates'] if 'revision_at_fixed_reporting_count' not in e['event'] and any(d['delta'] is not None and d['delta']<0 for d in e['changes'].values())]
        lines.extend(["", "### Rückgänge trotz neuer Meldungen", "", "Ein gleichbleibender Meldestand ist nur ein Detektor. Auch bei zunehmender Meldung sind Rückgänge erkennbar; gleichzeitig positive Änderungen können weitere Revisionen verdecken.", ""])
        for e in extra:
            negative=[f"{field}: {d['before']} → {d['after']}" for field,d in e['changes'].items() if d['delta'] is not None and d['delta']<0]
            lines.append(f"- {short(e['name'])}, {clock(e['acquired_at_local'])}: "+"; ".join(negative)+f". Gemeldete Bezirke {e['previous_reported']} → {e['reported']}.")
        lines.extend(["", "## Nächster Lauf und offene Fragen", "", "1. Neue erfolgreich archivierte Git-Referenz auswählen und mit `--require-complete` auswerten. Fehlt noch eine Meldung, bricht die 100%-Freigabe ab.",
                      "2. Den Bericht mit `--baseline` gegen diesen Ordner erzeugen; neue Stimmen, neue Revisionen und geschlossene Lücken getrennt ansehen.",
                      "3. Sobald Einzelbezirksergebnisse veröffentlicht sind, deren Identitäten, Änderungen und Summen prüfen. Ihr erstmaliges Auftauchen beweist keine rückwirkende Stabilität am Wahlabend.",
                      "4. Bei der zuständigen Wahlstelle die dokumentierten Gründe der Statusrücknahme und ausgewählter Korrekturen erfragen. Der Datensatz allein beantwortet diese Frage nicht.", "",
                      "## Grenzen der Aussage", "", "Meldestand ist der Anteil berichteter Bezirke, nicht der Anteil aller zu erwartenden Stimmen. Die >5%-Auswahl basiert auf dem eingefrorenen Landes-Zweitstimmenstand und ist keine Feststellung der rechtlichen Sitzberechtigung. Erststimmen werden zusätzlich in der Ergebnistabelle und im Änderungs-Audit erfasst.","",
                      "Git enthält erfolgreiche gespeicherte Abrufe, keine lückenlose Aufzeichnung jedes amtlichen Bearbeitungsschritts. Zeitangaben sind Abrufzeiten; tatsächliche Änderungen liegen zwischen den beobachteten Ständen. Eine Rücknahme des Status belegt weder gelöschte Stimmzettel noch Wahlmanipulation. Auch vollständige Summenkonsistenz schließt sachliche Fehler nicht aus.","",
                      "[Methoden und Reproduktion](METHODS.md) · [Alle Gebiets-/Parteiergebnisse](results_by_area.csv) · [Fehlende Bezirke](missing_wahlbezirke.csv) · [Prüfurteil](VALIDATION.md) · [Detaildiff Bitterfeld-Wolfen](BITTERFELD_WOLFEN_000028.md)",""])
        if baseline:
            old=jread(baseline/"summary.json");old_versions={v['commit'] for v in read_csv(baseline/'versions.csv')}
            if not old_versions.issubset(self.version_by_commit):
                raise ValueError('Baseline is not contained in this archive; do not compare unrelated histories.')
            old_land=jread(baseline/'latest_areas.json')['lsa:LAND:15']
            old_missing={r['key']:r for r in read_csv(baseline/'missing_wahlbezirke.csv')}
            current_missing={r['key']:r for r in read_csv(self.path/'missing_wahlbezirke.csv')}
            latest_status={}
            with gzip.open(self.path/'status_timeline.jsonl.gz','rt') as f:
                for line in f:
                    r=json.loads(line)
                    if r['commit']==s['status_last_capture']['commit']:latest_status[r['key']]=r
            party_comparison=[]
            for code in sorted(c for c in land['parties'].keys()|old_land['parties'].keys() if c.startswith('F')):
                a,b=old_land['parties'].get(code),land['parties'].get(code)
                party_comparison.append({'party_code':code,'party':land['party_names'].get(code,old_land['party_names'].get(code)),
                                         'before_votes':a,'after_votes':b,'vote_delta':b-a if a is not None and b is not None else None,
                                         'share_change_pp':100*b/land['valid_votes_zweit']-100*a/old_land['valid_votes_zweit'] if a is not None and b is not None else None})
            difference={"baseline_ref":old['ref'],"current_ref":s['ref'],"new_captures":sum(v['commit'] not in old_versions for v in self.versions),
                        "reported_precinct_delta":last['reported_precincts']-old['last_capture']['reported_precincts'],
                        "valid_second_vote_delta":last['valid_votes_zweit']-old['last_capture']['valid_votes_zweit'],
                        "new_fixed_reporting_municipality_revisions":[e for e in revisions if e['commit'] not in old_versions],
                        "new_status_losses":[e for e in losses if e['commit'] not in old_versions],
                        "land_party_comparison":party_comparison,
                        "closed_status_gaps":[r for k,r in old_missing.items() if k in latest_status and latest_status[k]['reported_precincts']>0],
                        "previously_open_keys_absent_now":[r for k,r in old_missing.items() if k not in latest_status],
                        "new_open_status_gaps":[r for k,r in current_missing.items() if k not in old_missing],
                        "selected_parties_before":old['selected_parties'],"selected_parties_after":s['selected_parties']}
            write_json(self.path/'comparison.json',difference)
            lines.extend(["## Vergleich mit dem vorherigen Bericht","",f"Basis `{old['ref'][:8]}`: {difference['new_captures']} neue Abrufe, {difference['reported_precinct_delta']:+} gemeldete Bezirke, {difference['valid_second_vote_delta']:+} gültige Zweitstimmen. Stimmenzuwachs umfasst neue Meldungen und Revisionen; er ist kein Korrekturbetrag.","","[Vollständiger Vergleich](comparison.json)",""])
        (self.path/"REPORT.md").write_text("\n".join(lines))
        self.methods()

    def detailed_case(self):
        """The status incident has no identified booth votes: never allocate a batch."""
        event=next((e for e in self.s['status_losses'] if 'Bitterfeld-Wolfen' in e['name'] and '000028' in e['name']),None)
        if not event or not event['restored_capture']:return
        before,after=event['commit'],event['restored_capture']['commit']
        index=next(i for i,v in enumerate(self.versions) if v['commit']==after)
        prior=self.versions[index-1]['commit']
        keys=['lsa:GEMEINDE:15082015','lsa:WAHLKREIS:028','lsa:KREIS:15082']
        rows={}
        with gzip.open(self.path/'raw_timeline.jsonl.gz','rt') as f:
            for line in f:
                r=json.loads(line)
                if r['commit'] in [before,after,prior] and r['area_key'] in keys and r['mode']=='TOTAL':rows[(r['commit'],r['area_key'])]=r
        status={prior:{},after:{}}
        with gzip.open(self.path/'status_timeline.jsonl.gz','rt') as f:
            for line in f:
                r=json.loads(line)
                if r['commit'] in status and r.get('Gemeinde')=='Bitterfeld-Wolfen, Stadt':status[r['commit']][r['wbz']]=r['reported_precincts']
        restorations=[k for k,v in status[after].items() if v>0 and status[prior].get(k)==0]
        a,b=rows[(before,keys[0])],rows[(after,keys[0])]
        diffs=[]
        for key in keys:
            old,new=rows[(before,key)],rows[(after,key)]
            for code in sorted(old['parties'].keys()|new['parties'].keys(),key=lambda c:(c[0],int(c[1:]))):
                x,y=old['parties'].get(code),new['parties'].get(code)
                if x is None and y is None:continue
                diffs.append({'scope':key,'name':old['name'],'party_code':code,'party':new['party_names'].get(code,old['party_names'].get(code)),
                              'vote_type':'Erststimmen' if code.startswith('D') else 'Zweitstimmen','before_commit':before,'after_commit':after,
                              'votes_1935':x,'votes_2211':y,'delta':y-x if x is not None and y is not None else None})
        with (self.path/'bitterfeld_party_diff.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(diffs[0]));w.writeheader();w.writerows(diffs)
        write_json(self.path/'bitterfeld_case.json',{'district_vote_diff_available':False,'reason':'No per-district vote export in these captures; multiple districts restore together.',
                   'status_loss':event,'prior_restoration_capture':self.version_by_commit[prior],
                   'simultaneously_reported_municipality_districts':restorations,'municipality_before':a,'municipality_after':b,'aggregate_party_diffs':diffs})
        fig=self.figure('Bitterfeld-Wolfen: Stimmen der gesamten Gemeinde',f"Abrufe {clock(event['acquired_at_local'])} → {clock(event['restored_capture']['acquired_at_local'])} MESZ · 13/31 → 31/31 Wahlbezirke · Einzelbezirk 000028 nicht isolierbar",height=9)
        ax=fig.add_axes([.16,.19,.75,.60]);y=np.arange(len(self.codes))
        old=[a['parties'][c] for c in self.codes];new=[b['parties'][c] for c in self.codes]
        ax.barh(y-.16,old,height=.30,color='#b8cedc',label='19:35: Gemeinde gesamt')
        ax.barh(y+.16,new,height=.30,color=BLUE,label='22:11: Gemeinde gesamt')
        for i,(x,z) in enumerate(zip(old,new)):ax.text(z+150,i+.16,f'{num(z)}  (Δ +{num(z-x)})',va='center',fontsize=11)
        ax.set_yticks(y,self.names);ax.invert_yaxis();ax.set_xlabel('Zweitstimmen (Anzahl)');ax.set_xlim(0,max(new)*1.32);ax.grid(axis='x',alpha=.12);ax.set_axisbelow(True);ax.legend(frameon=False,loc='lower right',fontsize=10)
        self.save(fig,'13_bitterfeld_gemeindediff','What do municipality party totals show at the two captures?','grouped horizontal bar',['party','capture','municipality votes'],'bitterfeld_party_diff.csv','Municipality total only. The 18 net additional reports prevent attribution to district 000028; even the last interval restores five districts.')
        lines=['# Bitterfeld-Wolfen, Wahlbezirk 000028: Was lässt sich nach Parteien vergleichen?','',
               '**Ein Parteistimmen-Diff für Wahlbezirk 000028 ist mit diesen archivierten Quellen nicht bestimmbar.** Die Übersicht enthält nur den Meldestatus. Die Stimmenexporte reichen hier bis zur Gemeinde. Die folgenden Zahlen sind ausdrücklich die gesamte Gemeinde Bitterfeld-Wolfen.','',
               f"Verglichen werden die Abrufe **{event['acquired_at_local']}** (`{before}`) und **{event['restored_capture']['acquired_at_local']}** (`{after}`).",'',
               '| Größe | Abruf 19:35 | Abruf 22:11 | Änderung |','|---|---:|---:|---:|',
               f"| Meldestatus Bezirk 000028 | nicht gemeldet (0) | gemeldet (1) | 0 → 1 |",
               f"| Gemeldete Bezirke der Gemeinde | {a['reported_precincts']}/{a['total_precincts']} | {b['reported_precincts']}/{b['total_precincts']} | +{b['reported_precincts']-a['reported_precincts']} |"]
        for field,label in [('voters_total','Wählende'),('valid_votes_erst','Gültige Erststimmen'),('valid_votes_zweit','Gültige Zweitstimmen')]:
            lines.append(f"| {label} | {num(a[field])} | {num(b[field])} | +{num(b[field]-a[field])} |")
        lines.extend(['',f"Amtliche Zeitstempel der Gemeinde-CSV-Zeile: **{a['source_time_local']}** bzw. **{b['source_time_local']}**. Diese unterscheiden sich von den Abrufzeiten.",'',
                      '![Gemeinde-Vergleich, keine Einzelbezirksergebnisse](charts/13_bitterfeld_gemeindediff.png)',''])
        for vote in ['Zweitstimmen','Erststimmen']:
            lines.extend(['',f'## {vote}: gesamte Gemeinde','', '| Partei | 19:35 | 22:11 | Δ Stimmen |','|---|---:|---:|---:|'])
            subset=sorted([r for r in diffs if r['scope']==keys[0] and r['vote_type']==vote],key=lambda r:r['votes_2211'] or 0,reverse=True)
            for r in subset:lines.append(f"| {r['party']} | {num(r['votes_1935'])} | {num(r['votes_2211'])} | {r['delta']:+,} |".replace(',','.'))
        lines.extend(['','## Weshalb auch der letzte Schritt 000028 nicht isoliert','',
                      f"Zwischen **{clock(self.version_by_commit[prior]['acquired_at_local'])}** (`{prior[:8]}`) und **22:11** melden in Bitterfeld-Wolfen gleichzeitig **{len(restorations)} Bezirke** neu: {', '.join(restorations)}. Die Gemeinde steigt dabei von {rows[(prior,keys[0])]['reported_precincts']}/31 auf 31/31 und von {num(rows[(prior,keys[0])]['valid_votes_zweit'])} auf {num(b['valid_votes_zweit'])} gültige Zweitstimmen.",'',
                      'Diesen gemeinsamen Zuwachs ausschließlich Bezirk 000028 zuzuschreiben wäre falsch. Für einen Vorher/Nachher-Stimmenvergleich dieses Bezirks wären dessen ursprüngliche und korrigierte Ergebnismeldungen oder eine amtliche Korrekturdokumentation nötig. Auch ein später veröffentlichter Endstand allein liefert den alten Einzelbezirkstand nicht nach.', '',
                      '## Quellen und Reproduktion','', '[Alle Parteidifferenzen für Gemeinde, Wahlkreis und Kreis als CSV](bitterfeld_party_diff.csv) · [Strukturierter Fall mit Originalwerten](bitterfeld_case.json) · [Gesamtbericht](REPORT.md)','',
                      f"Die Fallauswertung wird von `scripts/render_lsa_tweet_report.py` aus dem geprüften Git-Audit reproduziert. [Quelle 19:35](https://github.com/volzinnovation/wahl-monitor.de/tree/{before}/data/2026-lsa/latest/official_sources) · [Quelle 22:11](https://github.com/volzinnovation/wahl-monitor.de/tree/{after}/data/2026-lsa/latest/official_sources)",''])
        (self.path/'BITTERFELD_WOLFEN_000028.md').write_text('\n'.join(lines))

    def methods(self):
        s=self.s;ref=s['ref']
        text=f"""# Methods and reproduction

## Scope and report contract

Frozen Sachsen-Anhalt 2026 election-night observations, starting 6 September at
18:00 Europe/Berlin. The selected primary artifact is the user-requested tweet
series with standalone PNGs; REPORT.md is its reading copy. Audience: general
stakeholders. Summary → findings with visuals → next run/questions → caveats.
The Twitter format is the explicit surface override; no website is published.

## Reproduce this exact snapshot

From the repository root, using Python 3 with Matplotlib and NumPy:

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref {ref}
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/{ref[:8]}
python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v
```

The companion `analysis/lsa_git_audit.ipynb` exposes the audit and spot checks.
Analysis uses the standard library; chart versions are recorded in environment.json.
All inputs are read with `git show` from full commits, never from mutable latest
files, SQLite, live websites, or the site build. Neither command polls, commits,
pushes, deploys, publishes tweets, or changes schedules.

## Full-data rerun

After the collector has archived a new complete capture and that Git history is
available locally, select its full commit hash as NEW_COMMIT:

```sh
python3 scripts/analyze_lsa_git_timeline.py --ref NEW_COMMIT --output data/2026-lsa/reports/git-timeline/full-rerun --require-complete
python3 scripts/render_lsa_tweet_report.py --input data/2026-lsa/reports/git-timeline/full-rerun --baseline data/2026-lsa/reports/git-timeline/{ref[:8]}
```

Use the renderer only after the first command succeeds. The guard checks
reported districts equal the positive expected total; it does not certify a
legally final result. The next run recalculates the >5% party cohort at its own
cutoff and retains the entire earlier history. No automatic wakeup is scheduled.

## Observations and provenance

- Cutoff commit: `{ref}`.
- {s['versions']} post-18:00 captures; {s['unique_normalized_versions']} distinct normalized numeric/name states.
- First capture: {s['first_capture']['acquired_at_local']}.
- Last capture: {s['last_capture']['acquired_at_local']}.
- Status coverage starts: {s['status_first_capture']['acquired_at_local']}; {s['status_observations']} observations.
- Largest interval between saved acquisitions: {s['max_capture_gap_minutes']:.2f} minutes.
- {s['retained_sources_verified']} retained source objects match the capture manifest SHA-256.
- Branch first-parent order defines chronology; acquisition timestamps are labels,
  not source modification times. The original CSV retains local source timestamps.
- Pre-opening templates are excluded. Initial zero-denominator templates are
  retained with undefined percentage; population initialization is not an anomaly.
- [Official download page](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html).
- [Pinned source directory](https://github.com/volzinnovation/wahl-monitor.de/tree/{ref}/data/2026-lsa/latest/official_sources).

## Keys, denominators, and aggregation

Normalized keys are official area type + identifier. Raw keys add the U/B/TOTAL
mode; only TOTAL contributes to area aggregates. District-status keys combine
Wahlkreis, Kreis, municipality label, and district number, since the overview
does not expose AGS. Identity is never array position or a district number alone.
Numeric blanks remain missing; they never become zero. Party blank cells mean
not applicable and are explicitly counted in aggregation missingness columns.

Party selection: strictly above 5% of all valid Land second votes at the cutoff.
Use that same cohort at every earlier date and every locality; no per-place
reselection and no renormalization to selected parties. All parties remain in
the audit, including those below 5%. First and second votes are checked separately.
Shares are sum(votes)/sum(valid votes), not averages of percentages. City-versus-
rest is a disjoint partition. Distribution boxes weight geographic units equally
and are explicitly distinguished from the vote-weighted Land result.

Municipalities sum to Kreise by the first five AGS digits. Each of municipality,
Kreis, and Wahlkreis levels separately sums to Land. Kreis and Wahlkreis totals
overlap and must never be summed together. Split municipalities are not assigned
whole to Wahlkreise; direct official Wahlkreis values are used. Individual
district vote totals cannot be inferred from municipality totals or status rows.

## Detection and interpretation

The audit compares every consecutive observation of an area, including all
party values, voters, valid votes, and reporting counters. It records row loss,
reappearance, missing-value transitions, resets, numeric decreases, denominator
changes, any vote revision at unchanged reporting count, and changes after the
previous snapshot reported 100%. Raw U/B rows add invalid votes and eligibility.
Initial population setup and growing result counts are retained as ordinary
events. Status changes are a separate ledger; a lost status is not a lost vote.

One event = one area and one snapshot transition, possibly several changed
fields. Municipal revisions and the same changes in their Kreis/Wahlkreis/Land
must not be counted as independent incidents. Fixed-reporting-count changes are
a lower bound: corrections may occur while new reports arrive, and offsetting
changes may be invisible between captures. A stable count does not identify
which individual district was corrected. No causal or fraud classification is made.

CSV/HTML source lag is kept separate from within-file arithmetic. A status
withdrawal time is interval-censored: after the prior capture and at/before the
first capture with zero. Restoration is bounded analogously. Step charts draw
observed states; they do not prove continuous state between samples.

## Supporting artifacts

`area_timeline.csv.gz` and `party_timeline.csv.gz` form the normalized history.
`raw_timeline.jsonl.gz` preserves all raw modes, timestamps, parties and extras.
`status_timeline.jsonl.gz` preserves every archived status row.
`candidate_events.csv` is the normalized review ledger; `raw_candidate_events.csv`
also includes invalid-vote and mode-specific changes. `area_events.csv` retains
ordinary growth. `aggregation_checks.csv.gz` includes zero residuals and missing
child counts. Source links, byte hashes and times are in `source_manifest.json`.
`chart_map.json` documents question, data, palette and interpretation per chart.
`tweets.json` carries body, weighted character count, images, alt text and evidence.

## Known limits for the next run

No initial district vote export existed at this cutoff. The parser accepts the
documented Wahlbezirk CSV identity fields when they first appear. Any schema
change fails explicitly and must be reviewed, not converted to an empty table.
New district files cannot reconstruct missing historical vote snapshots.
Freshness and capture gaps limit all negative findings. Arithmetic checks alone
cannot detect a plausible but factually incorrect count or attribute intent.
"""
        (self.path/"METHODS.md").write_text(text)
        write_json(self.path/"environment.json",{"matplotlib":matplotlib.__version__,"numpy":np.__version__,
                  "analysis_script_sha256":s['analysis_script_sha256'],"renderer_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",required=True,type=Path)
    parser.add_argument("--baseline",type=Path)
    args=parser.parse_args()
    report=Report(args.input.resolve());report.export_results();report.charts();report.detailed_case()
    if report.complete and report.full:
        from lsa_complete_history_report import render
        render(report,args.baseline)
    else:
        report.narrative(args.baseline)
    if report.full and not report.complete:
        from lsa_full_history_report import render
        render(report,args.baseline)
    print(f"Tweet report: {report.path/'REPORT.md'}")


if __name__=="__main__":
    main()
