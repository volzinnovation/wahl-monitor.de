#!/usr/bin/env python3
"""Independently check a retained post-election package, without network access."""
import argparse
import base64
import json
import re
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd

from post_election_common import capture,raw_context,validate_capture,sha,save_json,read_csv
from prepare_post_election_demographics import FEATURES


def verify(path):
    manifest=json.loads((path/'manifest.json').read_text())
    for name,digest in manifest['files'].items():
        assert sha((path/name).read_bytes())==digest, f'Changed file: {name}'
    m,s,p,r,_=capture(manifest['election'],path/'sources')
    status,land,_=validate_capture(manifest['election'],m,s,p,raw_context(manifest['election'],r))
    assert status['reconciliation_passed'],status['issues']
    if manifest['mode']=='complete':assert status['ready']
    d=pd.read_csv(path/'demographic_areas.csv',dtype={'geo_id':str,'block':str})
    assert d.geo_id.is_unique and int(d.valid_second_votes.sum())==land['valid_votes_zweit']
    original=pd.read_csv(path/'sources/demographics.csv',dtype={'geo_id':str,'block':str}).set_index('geo_id')
    for feature in FEATURES:
        np.testing.assert_allclose(d.set_index('geo_id')[feature].sort_index(),original[feature].sort_index(),equal_nan=True)
    correlations=pd.read_csv(path/'demographic_correlations.csv')
    for _,row in correlations.iterrows():
        sub=d.dropna(subset=[row.feature]);sub=sub[sub.valid_second_votes>0]
        assert len(sub)==row.n and sub.valid_second_votes.sum()==row.valid_votes
        x=sub[row.feature];y=sub[row.party]/sub.valid_second_votes
        if len(sub)>=3 and x.var()>0 and y.var()>0:
            cov=np.cov(x,y,aweights=sub.valid_second_votes,ddof=0)
            weighted=cov[0,1]/np.sqrt(cov[0,0]*cov[1,1])
            np.testing.assert_allclose(weighted,row.pearson_vote_weighted,atol=1e-12)
            np.testing.assert_allclose(np.corrcoef(x,y)[0,1],row.pearson_equal,atol=1e-12)
    if (path/'model_predictions.csv').exists():
        predictions=pd.read_csv(path/'model_predictions.csv',dtype={'geo_id':str,'block':str})
        scores=pd.read_csv(path/'model_validation.csv').set_index('party')
        for party,part in predictions.groupby('party'):
            for block,test in part.groupby('block'):
                train=d[d.block!=block]
                np.testing.assert_allclose(test.training_baseline,train[party].sum()/train.valid_second_votes.sum(),atol=1e-12)
            error=100*np.sqrt(np.average((part.party_votes/part.n-part.heldout_prediction)**2,weights=part.n))
            np.testing.assert_allclose(error,scores.loc[party,'cv_weighted_rmse_pp'],atol=1e-10)
    waterfall=read_csv(path/'representation_waterfall.csv')
    for i,row in enumerate(waterfall):
        start,end,amount=(int(row[f]) for f in ['start','end','amount'])
        assert min(start,end,amount)>=0
        assert amount==(start-end if row['role'] in {'party','excluded'} else end)
        if i and row['role'] in {'party','excluded'}:assert start==int(waterfall[i-1]['end'])
    assert int(waterfall[-1]['end'])==0
    charts=json.loads((path/'chart_map.json').read_text())
    embedded=re.findall(r'src="data:image/png;base64,([^"]+)"',(path/'report.html').read_text())
    assert len(embedded)==len(charts)
    for encoded,chart in zip(embedded,charts):
        assert base64.b64decode(encoded)==(path/chart['path']).read_bytes()
        ET.parse(path/'charts'/(chart['slug']+'.svg'))
    result=dict(passed=True,source_file_hashes=len(manifest['files']),demographic_areas=len(d),
                independently_recalculated_correlations=len(correlations),embedded_charts=len(charts),
                counting_complete=status['counting_complete'],model_validation_checked=(path/'model_predictions.csv').exists())
    save_json(path/'numeric_qa.json',result)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report_directory',type=Path)
    print(json.dumps(verify(parser.parse_args().report_directory),indent=2))
