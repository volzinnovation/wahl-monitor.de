"""Reusable ports of LSA descriptive, vote-weighted and count-model analyses."""
from __future__ import annotations

import csv
import io
import json
import subprocess

import numpy as np
import pandas as pd

from post_election_common import ROOT, write_csv, save_json
from prepare_post_election_demographics import FEATURES


def wcorr(x, y, w):
    x, y, w = (np.asarray(a, float) for a in (x, y, w))
    if len(x) < 3 or not np.all(np.isfinite([x, y, w])) or not np.all(w > 0):
        return np.nan
    dx, dy = x - np.average(x, weights=w), y - np.average(y, weights=w)
    den = np.sqrt(np.sum(w * dx**2) * np.sum(w * dy**2))
    return float(np.sum(w * dx * dy) / den) if den > 1e-14 else np.nan


def weighted_midrank(x, w):
    values, inverse = np.unique(np.asarray(x, float), return_inverse=True)
    mass = np.bincount(inverse, weights=np.asarray(w, float), minlength=len(values))
    return (np.cumsum(mass) - .5 * mass)[inverse] / mass.sum()


def ballot_corr(x, k, n):
    """Correlation with a binary party ballot and the area's context, not voter traits."""
    x, k, n = (np.asarray(a, float) for a in (x, k, n))
    mx, py = np.average(x, weights=n), k.sum() / n.sum()
    den = np.sqrt(np.average((x-mx)**2, weights=n) * py * (1-py))
    return float(np.sum((x-mx)*(k-n*py)) / n.sum() / den) if den > 0 else np.nan


def demographic_analysis(key, out, snapshots, party_map, context, selected):
    source = out / 'sources'
    demo = pd.read_csv(source/'demographics.csv',dtype={'geo_id':str,'block':str})
    grouped = {}
    membership = []
    for s in snapshots:
        if s['gebietsart'] != 'GEMEINDE':
            continue
        c = context[s['row_key']]
        g = grouped.setdefault(c['geo_id'],dict(geo_id=c['geo_id'],valid_second_votes=0,voters=0,eligible_voters=0,
                   reported=0,expected=0, **{p:0 for p in selected}))
        membership.append(dict(geo_id=c['geo_id'],source_key=s['row_key'],source_name=s['municipality_name'],
                               valid_second_votes=s['valid_votes_zweit']))
        for target,field in [('valid_second_votes','valid_votes_zweit'),('voters','voters_total'),
                             ('reported','reported_precincts'),('expected','total_precincts')]:
            g[target] += s[field] or 0
        g['eligible_voters'] += c['eligible'] or 0
        for p in selected:
            g[p] += party_map.get((s['row_key'],'Zweitstimmen',p),0)
    votes = pd.DataFrame(grouped.values())
    data = votes.merge(demo,on='geo_id',how='left',validate='one_to_one',indicator=True)
    unmatched = data.loc[data['_merge']!='both','geo_id'].tolist()
    unused = sorted(set(demo.geo_id)-set(votes.geo_id))
    if unmatched or unused:
        raise ValueError(f"Demographic boundaries need review; election without census={unmatched}, census without election={unused}")
    data = data.drop(columns='_merge')
    data['complete'] = (data.expected > 0) & (data.reported == data.expected)
    data['turnout'] = 100 * data.voters / data.eligible_voters.replace(0,np.nan)
    data.to_csv(out/'demographic_areas.csv',index=False)
    write_csv(out/'demographic_join.csv',membership)
    cover = []
    rows = []
    folds = []
    for f,(label,definition) in FEATURES.items():
        sub = data.dropna(subset=[f])
        sub = sub[sub.valid_second_votes > 0]
        cover.append(dict(feature=f,label=label,definition=definition,date='2022-05-15',areas=len(sub),
                          missing=len(data)-len(sub),valid_votes=int(sub.valid_second_votes.sum()),
                          vote_coverage_percent=100*sub.valid_second_votes.sum()/data.valid_second_votes.sum()))
        for p in selected:
            x,k,n = sub[f], sub[p], sub.valid_second_votes
            share = k / n
            leave = []
            for block in sorted(sub.block.unique()):
                t = sub[sub.block!=block]
                r = wcorr(t[f],t[p]/t.valid_second_votes,t.valid_second_votes)
                leave.append(r)
                folds.append(dict(party=p,feature=f,omitted_block=block,n=len(t),weighted_pearson=r))
            complete = sub[sub.complete]
            without = sub[~sub.geo_id.isin(['13003000','13004000'])] if key=='2026-mv' else sub.iloc[:0]
            ranks = sub[[f,p]].copy()
            ranks[p] = share
            ranks = ranks.rank()
            within = ranks - ranks.groupby(sub.block).transform('mean')
            rows.append(dict(party=p,feature=f,label=label,n=len(sub),valid_votes=int(n.sum()),
                pearson_equal=wcorr(x,share,np.ones(len(x))),spearman_equal=wcorr(x.rank(),share.rank(),np.ones(len(x))),
                pearson_vote_weighted=wcorr(x,share,n),
                spearman_vote_weighted=wcorr(weighted_midrank(x,n),weighted_midrank(share,n),n) if len(x) else np.nan,
                ballot_context_r=ballot_corr(x,k,n) if len(x) else np.nan,
                absolute_party_votes_pearson=wcorr(x,k,np.ones(len(x))),
                complete_areas_r=wcorr(complete[f],complete[p]/complete.valid_second_votes,complete.valid_second_votes),
                complete_areas_n=len(complete),
                without_rostock_schwerin_r=wcorr(without[f],without[p]/without.valid_second_votes,without.valid_second_votes),
                within_block_rank_r=wcorr(within[f],within[p],np.ones(len(sub))),
                leave_one_block_min=min((r for r in leave if np.isfinite(r)),default=np.nan),
                leave_one_block_max=max((r for r in leave if np.isfinite(r)),default=np.nan)))
    correlations=pd.DataFrame(rows)
    correlations.to_csv(out/'demographic_correlations.csv',index=False)
    pd.DataFrame(cover).to_csv(out/'demographic_coverage.csv',index=False)
    pd.DataFrame(folds).to_csv(out/'demographic_leave_one_block_out.csv',index=False)
    full=correlations[(correlations.n==len(data)) & correlations.pearson_vote_weighted.notna()]
    strongest=full.loc[full.groupby('party').pearson_vote_weighted.apply(lambda s:s.abs().idxmax())]
    strongest.to_csv(out/'demographic_strongest.csv',index=False)
    models=count_models(key,out,data,selected)
    return data,correlations,strongest,models


def count_models(key,out,data,parties):
    """LSA grouped-binomial specification with training-only geographic CV scaling."""
    import statsmodels.api as sm
    features=['log_population','age19_24_share','age67_share','owner_occupancy_share','foreign_citizenship_share']
    d=data.dropna(subset=features).copy()
    d=d[d.valid_second_votes>0]
    if len(d)<40 or d.block.nunique()<5:
        result=dict(status='not_fit',reason='Fünf Kovariaten bei nur zwölf Berliner Bezirken: zu wenige unabhängige Gebiete für den LSA-Modellumfang.',n=len(d),features=features)
        save_json(out/'model_status.json',result)
        return result

    def fit(train,p):
        n=train.valid_second_votes.to_numpy(float)
        k=train[p].to_numpy(float)
        raw=train[features].to_numpy(float)
        center=np.average(raw,axis=0,weights=n)
        scale=np.sqrt(np.average((raw-center)**2,axis=0,weights=n))
        if (scale==0).any():raise ValueError('Constant model predictor')
        x=np.column_stack([np.ones(len(train)),(raw-center)/scale])
        if np.linalg.matrix_rank(x)!=x.shape[1]:raise ValueError('Rank-deficient count model')
        fitted=sm.GLM(np.column_stack([k,n-k]),x,family=sm.families.Binomial()).fit(maxiter=100,tol=1e-10)
        if not fitted.converged or not np.isfinite(fitted.params).all():raise ValueError('Count model failed to converge')
        if abs(np.dot(n,fitted.fittedvalues)-k.sum())>1e-4:raise ValueError('Count-model calibration failed')
        return fitted,center,scale

    coefficients=[];predictions=[];scores=[];folds=[]
    for p in parties:
        if not 0 < d[p].sum() < d.valid_second_votes.sum():continue
        model,center,scale=fit(d,p)
        held_predictions=np.zeros(len(d)); baseline=np.zeros(len(d)); fold_params=[]
        for block in sorted(d.block.unique()):
            held=d.block==block;train=d[~held];test=d[held]
            f,m,s=fit(train,p)
            x=np.column_stack([np.ones(len(test)),(test[features].to_numpy(float)-m)/s])
            held_predictions[held]=f.predict(x)
            baseline[held]=train[p].sum()/train.valid_second_votes.sum()
            # Compare coefficients in the common full-data SD units.
            params=f.params[1:]/s*scale
            fold_params.append(params)
            folds.extend(dict(party=p,omitted_block=block,feature=feature,log_odds_per_full_sd=float(value)) for feature,value in zip(features,params))
        n=d.valid_second_votes.to_numpy(float);observed=d[p].to_numpy(float)/n
        dispersion=float(model.pearson_chi2/model.df_resid)
        for j,feature in enumerate(features):
            coefficients.append(dict(party=p,feature=feature,log_odds_per_sd=float(model.params[j+1]),
                leave_one_block_min=float(np.min(fold_params,axis=0)[j]),leave_one_block_max=float(np.max(fold_params,axis=0)[j]),
                pearson_dispersion=dispersion,n=len(d)))
        scores.append(dict(party=p,n=len(d),blocks=d.block.nunique(),
            cv_weighted_rmse_pp=100*float(np.sqrt(np.average((observed-held_predictions)**2,weights=n))),
            baseline_weighted_rmse_pp=100*float(np.sqrt(np.average((observed-baseline)**2,weights=n))),pearson_dispersion=dispersion))
        predictions.extend(dict(party=p,geo_id=row.geo_id,block=row.block,n=int(row.valid_second_votes),
            party_votes=int(row[p]),observed_share=float(observed[j]),heldout_prediction=float(held_predictions[j]),
            training_baseline=float(baseline[j])) for j,(_,row) in enumerate(d.iterrows()))
    for filename,rows in [('model_coefficients',coefficients),('model_validation',scores),('model_predictions',predictions),('model_leave_one_block_out',folds)]:
        write_csv(out/(filename+'.csv'),rows)
    result=dict(status='fit',n=len(d),blocks=int(d.block.nunique()),features=features,
        method='Grouped binomial counts [k,n-k]; leave-one-Kreis-out; training-only scaling and baseline; no ballot-level inference or p values.')
    save_json(out/'model_status.json',result)
    return result


def history(key,out,meta,snapshots,parties):
    """Read observed git captures, preserving arrival shares and gaps in observation."""
    prefix=f'data/{key}/latest/'
    def git(*args):return subprocess.check_output(['git','-C',str(ROOT),*args])
    ref=git('rev-parse','HEAD').decode().strip()
    commits=git('log','--reverse','--format=%H',ref,'--',prefix+'run_metadata.json').decode().splitlines()
    observations=[];timeline=[];arrivals={};revisions=[];previous={};seen=set()
    for commit in commits+[None]:
        if commit:
            m=json.loads(git('show',commit+':'+prefix+'run_metadata.json'))
            if m.get('statla_mode')!='LIVE_CSV_DOWNLOAD' or m.get('generated_at_utc','')>meta['generated_at_utc']:continue
            ss=list(csv.DictReader(io.StringIO(git('show',commit+':'+prefix+'statla_snapshots.csv').decode())))
            pp=list(csv.DictReader(io.StringIO(git('show',commit+':'+prefix+'statla_party_results.csv').decode())))
        else:m,ss,pp=meta,snapshots,parties
        if m['run_label'] in seen:continue
        seen.add(m['run_label'])
        land=next((s for s in ss if s['gebietsart']=='LAND'),None)
        if not land:continue
        num=lambda r,f:int(r.get(f) or 0)
        second={(p['row_key'],p['party_name']):int(p['votes']) for p in pp if p['vote_type']=='Zweitstimmen' and p['votes'] not in {None,''}}
        second_by_area={}
        for (area,party),value in second.items():
            second_by_area.setdefault(area,{})[party]=value
        for p in pp:
            if p['row_key']==land['row_key'] and p['vote_type']=='Zweitstimmen' and num(land,'valid_votes_zweit'):
                observations.append(dict(capture=m['run_label'],time=m['generated_at_utc'],commit=commit or 'local',party=p['party_name'],votes=int(p['votes']),share=100*int(p['votes'])/num(land,'valid_votes_zweit')))
        timeline.append(dict(capture=m['run_label'],time=m['generated_at_utc'],commit=commit or 'local',
            reported=num(land,'reported_precincts'),expected=num(land,'total_precincts'),voters=num(land,'voters_total'),
            valid_second_votes=num(land,'valid_votes_zweit'),invalid_second_votes=num(land,'voters_total')-num(land,'valid_votes_zweit')))
        for s in ss:
            if s['gebietsart'] not in {'GEMEINDE','WAHLKREIS'}:continue
            rk=s['row_key'];valid=num(s,'valid_votes_zweit');rep=num(s,'reported_precincts');total=num(s,'total_precincts')
            for kind,condition in [('first_positive',valid>0),('first_complete',valid>0 and rep==total and total>0)]:
                if condition and (rk,kind) not in arrivals:
                    arrivals[(rk,kind)]=dict(row_key=rk,name=s.get('municipality_name') or s['gebietsnummer'],level=s['gebietsart'],
                        kind=kind,time=m['generated_at_utc'],valid_second_votes=valid,afd_share=100*second.get((rk,'AfD'),0)/valid)
            old=previous.get(rk)
            vector=second_by_area.get(rk,{})
            if old and old['parties']!=vector:
                revisions.append(dict(row_key=rk,time=m['generated_at_utc'],previous_time=old['time'],
                    delta_valid=valid-old['valid'],delta_reported=rep-old['reported'],
                    fixed_reported_count=rep==old['reported'],
                    party_deltas=json.dumps({p:vector.get(p,0)-old['parties'].get(p,0) for p in vector if vector.get(p,0)!=old['parties'].get(p,0)},ensure_ascii=False)))
            previous[rk]=dict(time=m['generated_at_utc'],valid=valid,reported=rep,parties=vector)
    write_csv(out/'counting_timeline.csv',timeline)
    write_csv(out/'party_timeline.csv',observations)
    write_csv(out/'arrival_observations.csv',arrivals.values())
    write_csv(out/'area_changes.csv',revisions,['row_key','time','previous_time','delta_valid','delta_reported','fixed_reported_count','party_deltas'])
    return dict(git_ref=ref,captures=len(timeline),first=timeline[0]['time'] if timeline else None,
                last=timeline[-1]['time'] if timeline else None,
                fixed_counter_party_changes=sum(r['fixed_reported_count'] for r in revisions))
