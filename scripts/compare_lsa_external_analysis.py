#!/usr/bin/env python3
"""Offline comparison of the frozen GFrei.News export with the LSA Git audit."""
import argparse, collections, csv, gzip, json, math, statistics
from pathlib import Path
from analyze_lsa_git_timeline import numeric_values, write_csv, write_json

LEVELS = {'LAN':'LAND','KRS':'KREIS','WKR':'WAHLKREIS','GEM':'GEMEINDE'}
FIELDS = {'eligible':'eligible_voters','voters':'voters_total','first_invalid':'invalid_votes_erst','second_invalid':'invalid_votes_zweit','first_valid':'valid_votes_erst','second_valid':'valid_votes_zweit','reported':'reported_precincts','expected':'total_precincts'}
LABELS = {'Wähler':'voters_total','Wahlberechtigte':'eligible_voters','Gültige Erststimmen':'valid_votes_erst','Gültige Zweitstimmen':'valid_votes_zweit','Ungültige Erststimmen':'invalid_votes_erst','Ungültige Zweitstimmen':'invalid_votes_zweit','Gemeldete Wahlbezirke':'reported_precincts','Vorgesehene Wahlbezirke':'total_precincts','Erwartete Wahlbezirke':'total_precincts'}

def read_export(path, name):
    obj=json.loads((path/(name+'.json')).read_text())
    if isinstance(obj,dict) and obj.get('format')=='wahlb-json-parts-v1':
        data=[r for part in obj['parts'] for r in json.loads((path/part).read_text())]
        assert len(data)==obj['total']
        return data
    return obj

def key(external):
    parts=external.split(':')
    if parts[0]=='WBZ':return f'lsa:WAHLBEZIRK:{parts[3]}:{int(parts[1])}:{parts[4]}:{parts[5]}'
    return f'lsa:{LEVELS[parts[0]]}:{parts[1]}:{parts[2]}'

def feature(row, field):
    if field=='Beteiligung':
        a=row['extra'].get('eligible_voters')
        return 100*row['voters_total']/a if a and row['mode']!='B' else None
    if field.startswith('Ungültige '):
        f='invalid_votes_erst' if field=='Ungültige Erststimmen' else 'invalid_votes_zweit'
        return 100*row['extra'][f]/row['voters_total'] if row['voters_total'] else None
    code=next((c for c,n in row['party_names'].items() if c.startswith('F') and n==field),None)
    return 100*row['parties'][code]/row['valid_votes_zweit'] if code and row['valid_votes_zweit'] else None

def compare(path):
    ext=path/'external';cat=json.loads((ext/'current.json').read_text())
    raw=json.loads((path/'latest_official_rows.json').read_text());s=json.loads((path/'summary.json').read_text())
    erows=read_export(ext,'results');mapped={e['key']:raw[key(e['key'])] for e in erows}
    assert len(mapped)==len(raw)==len(erows)
    diffs=[];checks=0
    for e in erows:
        row=mapped[e['key']];v=numeric_values(row)
        for external,ours in FIELDS.items():
            checks+=1
            if e.get(external)!=v.get(ours):diffs.append({'row':e['key'],'field':ours,'external':e.get(external),'git':v.get(ours)})
        for c,n in row['party_names'].items():
            checks+=1;external=e['first' if c.startswith('D') else 'second'].get(n)
            if external!=row['parties'][c]:diffs.append({'row':e['key'],'field':c,'external':external,'git':row['parties'][c]})
    provenance=json.loads((path/'source_manifest.json').read_text())
    final_sources={p['url']:p['content_hash'] for p in provenance if p['commit']==s['last_capture']['commit']}
    source_comparison=[{'url':q['url'],'external_sha256':q['sha256'],'git_sha256':final_sources[q['url']],'matches':q['sha256']==final_sources[q['url']]} for q in cat['sources'] if q['url'] in final_sources]
    # Recreate the robust screening from our original CSV rows, without using
    # their expected values or z-scores. Whole municipalities and individual
    # booths form separate groups, further split by voting mode and voter size.
    groups=collections.defaultdict(list)
    for r in raw.values():
        if r['level']=='WAHLBEZIRK' or (r['level']=='GEMEINDE' and r['mode']=='TOTAL'):
            if r['voters_total']>=100:groups[(r['level'],r['mode'],int(math.log10(r['voters_total'])))].append(r)
    mad=read_export(ext,'method-robust-mad');mad_comparison=[];cache={}
    for e in mad:
        if e['status']!='checked':continue
        r=mapped[e['row']];g=(r['level'],r['mode'],int(math.log10(r['voters_total'])),e['field'])
        if g not in cache:
            vals=[feature(x,e['field']) for x in groups[g[:3]]];vals=[v for v in vals if v is not None]
            median=statistics.median(vals);spread=statistics.median(abs(x-median) for x in vals);cache[g]=(median,spread,len(vals))
        median,spread,n=cache[g];observed=feature(r,e['field']);z=(observed-median)/(1.4826*spread)
        level='red' if abs(z)>=10 and abs(observed-median)>=5 else 'orange' if abs(z)>=6 and abs(observed-median)>=2 else 'neutral'
        match=all(math.isclose(a,b,abs_tol=1e-9,rel_tol=1e-10) for a,b in [(observed,e['observed']),(median,e['expected']),(spread,e['mad']),(z,e['z'])]) and n==e['n'] and level==e['level']
        mad_comparison.append({'row':e['row'],'name':r['name'],'field':e['field'],'mode':r['mode'],'geographic_level':r['level'],'observed':observed,'median':median,'mad':spread,'z':z,'n':n,'external_level':e['level'],'recomputed_level':level,'match':match})
    # Match every orange historical row/field flag to an exact raw Git transition.
    with (path/'raw_candidate_events.csv').open() as f:events=list(csv.DictReader(f))
    for e in events:e['changes']=json.loads(e['changes'])
    revision=[]
    for e in read_export(ext,'method-revision'):
        if e['level']=='neutral':continue
        r=mapped[e['row']];field=LABELS.get(e['field'])
        if ': ' in e['field']:
            prefix,label=e['field'].split(': ',1);c=next((c for c,n in r['party_names'].items() if c.startswith('D' if prefix=='Erststimmen' else 'F') and n==label),None)
            field='party:'+c if c else None
        matches=[x for x in events if x['key']==r['key'] and field in x['changes'] and x['changes'][field]['before']==e['expected'] and x['changes'][field]['after']==e['observed']]
        same_delta=[x for x in events if x['key']==r['key'] and field in x['changes'] and x['changes'][field]['delta']==e['deviation']]
        revision.append({'external_row':e['row'],'field':e['field'],'mapped_field':field,'before':e['expected'],'after':e['observed'],'delta':e['deviation'],'external_event_at':e.get('event_at'),'exact_git_matches':[{'commit':x['commit'],'time':x['acquired_at_local'],'event':x['event']} for x in matches],'same_delta_git_matches':[x['commit'] for x in same_delta] if not matches else []})
    digit=[]
    for e in read_export(ext,'method-last-digits'):
        party=e['field'].split(':')[0];bins=collections.Counter()
        for r in raw.values():
            if r['level']!='WAHLBEZIRK' or r['valid_votes_zweit']<=400:continue
            c=next(c for c,n in r['party_names'].items() if c.startswith('F') and n==party)
            v=r['parties'][c]
            if v is not None and v>=100 and math.sqrt(v*(1-v/r['valid_votes_zweit']))>=10:bins[v%10]+=1
        digit.append({'party':party,'reported_n':e['n'],'recomputed_n':sum(bins.values()),'reported_counts':e['digit_counts'],'recomputed_counts':[bins[i] for i in range(10)],'counts_match':[bins[i] for i in range(10)]==e['digit_counts'],'external_q':e['q'],'simulations_reproduced':False})
    flags=[x for x in mad_comparison if x['recomputed_level']!='neutral']
    result={'external_url':'https://gfrei.news/wp-content/uploads/wahlbeobachtung/','external_version':cat['version'],'external_generated_at':cat['generated_at'],'external_software_commit':cat['commit'],'git_ref':s['ref'],'source_hash_comparison':source_comparison,'raw_rows_compared':len(erows),'numeric_comparisons':checks,'numeric_differences':diffs,'mad_checked':len(mad_comparison),'mad_mismatches':[x for x in mad_comparison if not x['match']],'mad_flags':dict(collections.Counter(x['recomputed_level'] for x in flags)),'mad_unique_flagged_areas':len({x['row'] for x in flags}),'mad_flagged_levels':dict(collections.Counter(x['geographic_level'] for x in flags)),'revision_flags':len(revision),'revision_exact_matches':sum(bool(x['exact_git_matches']) for x in revision),'revision_unmatched':[x for x in revision if not x['exact_git_matches']],'digit_checks':digit,'methods_catalogue':[{'id':x['id'],'title':x['title'],'counts':x['counts'],'total':x['total']} for x in cat['methods']]}
    write_json(path/'external_comparison.json',result);write_csv(path/'external_mad_recalculation.csv',mad_comparison);write_csv(path/'external_mad_flags.csv',flags);write_json(path/'external_revision_comparison.json',revision)
    print(json.dumps({k:result[k] for k in ['raw_rows_compared','numeric_comparisons','numeric_differences','mad_checked','mad_mismatches','mad_flags','mad_unique_flagged_areas','revision_flags','revision_exact_matches','revision_unmatched','digit_checks']},ensure_ascii=False,indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',required=True,type=Path);compare(p.parse_args().input)
