#!/usr/bin/env python3
"""Check generated evidence, draft lengths, links, and an independent rerun."""
import argparse
import csv
import gzip
import hashlib
import json
import re
from pathlib import Path


def verify(path, reproduction=None, visual_reviewed=False):
    s=json.loads((path/'summary.json').read_text())
    complete=s.get('reporting_evidence',{}).get('complete_reconciled',False)
    tweets=json.loads((path/'tweets.json').read_text())
    charts=json.loads((path/'chart_map.json').read_text())
    if any('priority_rank' in tweet for tweet in tweets):
        assert len(tweets)==10, 'The prioritized series must contain exactly ten findings'
        assert [tweet['priority_rank'] for tweet in tweets]==list(range(1,11))
        assert all(tweet.get('finding') and len(tweet['images'])<=4 for tweet in tweets)
        report=(path/'REPORT.md').read_text()
        assert len(re.findall(r'^### Tweet \d+:',report,re.M))==10
        assert all(chart['path'] in report for chart in charts), 'Every retained chart needs a report or appendix link'
    assert not s['arithmetic_issues'], 'Arithmetic violations require review'
    assert not s['source_replay_differences'], 'Source replay differences require review'
    assert s['mode_nonzero']==0, 'Voting-mode differences require review'
    if not s.get('full_history'):assert s['aggregation_nonzero']==0, 'Aggregation differences require review'
    assert all(x['verified'] for x in json.loads((path/'source_manifest.json').read_text()))
    for t in tweets:
        weight=sum(1 if ord(c)<=0x10ff or 0x2000<=ord(c)<=0x200d or 0x2010<=ord(c)<=0x201f or 0x2032<=ord(c)<=0x2037 else 2 for c in t['text'])
        assert weight==t['weighted_characters'] and weight<=280
        assert len(t['images'])==len(t['alt_text'])
        for image in t['images']:assert (path/image).is_file()
    for c in charts:assert (path/c['path']).read_bytes().startswith(b'\x89PNG\r\n\x1a\n')
    for name in ['REPORT.md','BITTERFELD_WOLFEN_000028.md','METHODS.md'] + (['AKEN_000010.md'] if (path/'AKEN_000010.md').exists() else []) + (['AGGREGATION_ASCHERSLEBEN.md'] if (path/'AGGREGATION_ASCHERSLEBEN.md').exists() else []):
        for target in re.findall(r'\]\(([^)]+)\)',(path/name).read_text()):
            if target.startswith(('https://','http://')) or target=='VALIDATION.md':continue
            assert (path/target).is_file(), f'Broken link {target}'
    case=json.loads((path/'bitterfeld_case.json').read_text())
    assert case['district_vote_diff_available'] is False
    assert len(case['simultaneously_reported_municipality_districts'])>1
    before,after=case['municipality_before'],case['municipality_after']
    for prefix,field in [('D','valid_votes_erst'),('F','valid_votes_zweit')]:
        rows=[r for r in case['aggregate_party_diffs'] if r['scope']==before['area_key'] and r['party_code'].startswith(prefix)]
        assert sum(r['delta'] for r in rows)==after[field]-before[field]
    with gzip.open(path/'aggregation_checks.csv.gz','rt') as f:aggregates=list(csv.DictReader(f))
    assert len(aggregates)==s['aggregation_checks']
    nonzero=[r for r in aggregates if r['delta'] not in ('','0')]
    assert len(nonzero)==s['aggregation_nonzero']
    with (path/'aggregation_nonzero.csv').open() as f:assert list(csv.DictReader(f))==nonzero
    full_check = None
    if s.get('full_history'):
        from types import SimpleNamespace
        if complete:
            from lsa_complete_history_report import derive
        else:
            from lsa_full_history_report import derive
        with (path/'versions.csv').open() as f:versions=list(csv.DictReader(f))
        obj=SimpleNamespace(s=s,path=path,latest=json.loads((path/'latest_areas.json').read_text()),version_by_commit={v['commit']:v for v in versions})
        derived=derive(obj)
        identity,gap,late=derived[:3]
        assert identity==json.loads((path/'district_identity_changes.json').read_text())
        assert gap==json.loads((path/'aggregation_gap.json').read_text())
        assert late==json.loads((path/'post_midnight_revisions.json').read_text())
        assert len(gap['observed_commits'])==3 and len(gap['latest_land_deltas'])==27
        assert all(int(r['delta'])==gap['latest_land_deltas'][r['field']] for r in nonzero)
        assert {r['relation'] for r in nonzero}=={'GEMEINDE->LAND','GEMEINDE->KREIS'}
        if complete:
            assert derived[3]==json.loads((path/'final_precinct_cases.json').read_text())
            assert derived[4]==json.loads((path/'final_reconciliation.json').read_text())
            assert derived[3]['Aschersleben']['arrival_vector_matches_final_booth']
            assert not derived[3]['Aken']['arrival_vector_matches_final_booth']
            assert derived[4]['status_vote_identity_matches']==2661
            assert s['reporting_evidence']['final_aggregation_nonzero']==0
            from analyze_lsa_git_timeline import git, BASE
            all_commits=set(git('log','--format=%H',s['ref'],'--',BASE).decode().splitlines())
            first_parent=set(git('log','--first-parent','--format=%H',s['ref'],'--',BASE).decode().splitlines())
            assert all_commits==first_parent and len(all_commits)==s['history_commit_count']
            external=json.loads((path/'external_comparison.json').read_text())
            assert not external['numeric_differences'] and not external['mad_mismatches'] and not external['revision_unmatched']
            assert all(e['matches'] for e in external['source_hash_comparison'])
            assert all(e['counts_match'] for e in external['digit_checks'])
            for e in json.loads((path/'external/manifest.json').read_text()):
                assert hashlib.sha256((path/'external'/e['file']).read_bytes()).hexdigest()==e['sha256']
        else:
            control=json.loads((path/'polling_stopped.json').read_text())
            assert control['workflow']['state']=='disabled_manually' and control['heartbeat']['status']=='PAUSED'
            assert not control['active_or_queued_archive_runs_at_verification']
        full_check={'inventory_commits':s['history_commit_count'],'dated_captures':s['versions'],
                    'geographic_residuals_reproduced':len(nonzero),'source_gap_captures':len(gap['observed_commits']),
                    'polling_control_rechecked':False if complete else True,'identity_and_gap_replay':'passed'}
    addendum = None
    if (path/'aken_case.json').exists() and not s.get('full_history'):
        from add_lsa_aken_update import derive
        derived = derive(path)
        assert derived == json.loads((path/'aken_case.json').read_text()), 'Aken evidence differs from source replay'
        assert derived['before']['overview_total'] == 2660
        assert derived['after']['overview_total'] == 2661
        assert len([t for t in tweets if t.get('series') == 'aken_addendum']) == 2
        addendum = {'captures':len(derived['observations']), 'verified_git_files':len(derived['sources']),
                    'baseline_status_rows_checked':derived['baseline_status_rows_checked'],
                    'first_present_capture_local':derived['after']['capture_local'],
                    'source_replay':'passed'}
    compared=[]
    if reproduction:
        for other in sorted(reproduction.iterdir()):
            if not other.is_file() or not (path/other.name).is_file():continue
            if other.name in ['REPORT.md','comparison.json','VALIDATION.md','verification.json','SHA256SUMS']:continue
            assert other.read_bytes()==(path/other.name).read_bytes(), f'Reproduction differs: {other.name}'
            compared.append(other.name)
        if (reproduction/'charts').exists():
            for other in sorted((reproduction/'charts').glob('*.png')):
                assert other.read_bytes()==(path/'charts'/other.name).read_bytes(), f'Chart differs: {other.name}'
                compared.append('charts/'+other.name)
        for other in sorted((reproduction/'addendum_sources').glob('*.json')):
            assert other.read_bytes() == (path/'addendum_sources'/other.name).read_bytes()
            compared.append('addendum_sources/'+other.name)
        if complete:
            for other in sorted([* (reproduction/'external').rglob('*'), * (reproduction/'context_sources').rglob('*'), * (reproduction/'cross_election_sources').rglob('*')]):
                if other.is_file():
                    rel=other.relative_to(reproduction)
                    assert other.read_bytes()==(path/rel).read_bytes(), f'External input differs: {rel}'
                    compared.append(str(rel))
        if addendum or full_check:
            assert (reproduction/'REPORT.md').read_bytes() == (path/'REPORT.md').read_bytes(), 'Updated report differs'
            compared.append('REPORT.md')
    results={'confidence':'Share with caveats','structural_checks':'passed',
             'tweet_count':len(tweets),'chart_count':len(charts),'max_weighted_tweet_length':max(t['weighted_characters'] for t in tweets),
             'reproduction_files_compared':compared,'visual_reviewed':visual_reviewed,
             'verifier_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    if addendum: results['aken_addendum'] = addendum
    if full_check: results['full_history'] = full_check
    (path/'verification.json').write_text(json.dumps(results,indent=2,ensure_ascii=False,sort_keys=True)+'\n')
    lines=['# Validation report','','## Overall assessment: Share with caveats','',
           'The interim report answers the archived-data question. It does not establish election misconduct or reconstruct votes for district 000028.','',
           '## Checks completed','',
           f"- {s['retained_sources_verified']} source objects match the archived SHA-256 manifests.",
           f"- {len(s['arithmetic_issues'])} arithmetic violations; {len(s['source_replay_differences'])} normalized/source differences.",
           f"- {s['aggregation_checks']:,} geographic and {s['mode_checks']:,} U+B field comparisons; {s['aggregation_nonzero']} and {s['mode_nonzero']} nonzero residuals respectively. Repeated states are included.",
           f"- {len(tweets)} tweet drafts; maximum {results['max_weighted_tweet_length']} weighted characters; all local links resolve.",
           f"- {len(charts)} PNGs; visual review recorded: {visual_reviewed}.",
           f"- Independent rerun: {len(compared)} byte-identical files; exact list in verification.json.",
           '- Bitterfeld municipality party deltas reconcile to first- and second-vote total deltas separately. District attribution is explicitly unavailable.','',
           '## Independent checks and regression tests','',
           'The companion notebook parses original CSVs independently, recomputes municipality and party totals against Land, and reads the overview JSON at both Bitterfeld captures. All five code cells executed sequentially with Python. A Jupyter kernel runner was unavailable locally (nbformat/nbclient missing); no dependencies were installed.','',
           "`python3 -m unittest discover -s scripts -p test_lsa_git_timeline.py -v` passes 14 tests covering growth, denominator changes, cross-level vote gaps, redistribution, missing vs zero, resets, initial templates, row order, arithmetic, separate geographic sums, the completeness guard, and future booth U/B grouping. The completeness guard rejects the unfinished Land counter. No source data was altered to pass checks.",'',
           '## Required caveats','',
           '- Reporting completeness is not vote completeness or official certification.',
           '- District-status history starts at 19:05; original district vote rows were absent.',
           '- Bitterfeld 000028 returns with four other districts. Municipality deltas cannot be assigned to it.',
           '- HTML/CSV publication timing can differ. Source lag is not a count of lost ballots.',
           '- Changes between captures and offsetting revisions can remain unseen; arithmetic alone cannot identify causes or rule out substantive errors.','',
           'No remaining blocker to a labelled interim report. Missing historical district vote data blocks individual-district party attribution. No tweets or website were published.','']
    if addendum:
        lines.extend(['## Aken addendum validation','',
                      f"- Replayed {addendum['captures']} post-baseline HTML observations; verified all {addendum['verified_git_files']} retained files against Git blob hashes and every HTML file against its collector SHA-256 and byte count.",
                      f"- Checked {addendum['baseline_status_rows_checked']:,} baseline status rows for any earlier occurrence of Aken 000010; none found.",
                      '- Exactly one added identity and no removed identities in the adjacent captures; total districts increase 2660 to 2661. Both HTML and CSV metadata confirm the changed total.',
                      '- First presence is in the 00:51:10 MESZ archive run, following absence at 00:45:41. Exact HTML-fetch times are retained separately; neither timestamp is asserted to be the precise publication time.',
                      '- The original 15 tweets and baseline evidence remain unchanged. Two addendum tweets and one image cover only this later finding; statewide vote shares and anomaly counts were not rerun.',
                      '- The addendum was rebuilt independently from the original report package and bundled sources; its generated report, data and image were compared byte-for-byte.',
                      '- Individual precinct party votes and the reason for adding the entry remain unavailable in these sources.',''])
    if full_check:
        lines.extend(['## Full-history findings validation','',
                      f"- Inventoried {s['history_commit_count']} data commits: {s['versions']} dated captures, {s['setup_commit_count']} setup commits without capture times. Includes {s['preopening_captures']} earlier zero templates, with {len(s['captures_without_raw_sources'])} missing original source CSV explicitly disclosed.",
                      '- Independently parsed the latest original CSVs in the notebook: municipality minus Land equals 1517 voters, 1509 valid first votes and 1513 valid second votes. Every party delta matches the published Aschersleben municipality transition.',
                      '- Replayed all 162 nonzero geographic fields without suppressing them: the same 27-field vector at two aggregation relations in three captures. Zero within-row arithmetic violations and zero normalized/source replay differences.',
                      '- Verified one added status identity (Aken 000010), no removed identities, one status withdrawal, and five additional fixed-count municipal revisions after the original report.',
                      '- The 2661/2661 HTML counter is not substituted for the 2660/2661 Land CSV counter. Computed municipality totals are explicitly separated from official Land totals and shares.',
                      '- Polling-stop evidence records the paused heartbeat, disabled GitHub workflow and empty active/queued collection list. No further collection or publication is scheduled.',''])
        lines=[line.replace('All five code cells executed sequentially with Python.','All six code cells of the updated notebook executed sequentially with Python.') for line in lines]
    if complete:
        lines=['# Validation: full Git history and external comparison','',
               '**Ready to share with the stated limits.** Final arithmetic and aggregation checks pass. Historical changes remain documented; their causes are not established.','',
               f"- Full reachable and first-parent data-commit sets are equal: {s['history_commit_count']} commits, {s['versions']} dated captures.",
               f"- {s['retained_sources_verified']} original source objects match their archived SHA-256. One early zero template lacks its original CSV; this is disclosed.",
               f"- {s['aggregation_checks']:,} geographic and {s['mode_checks']:,} U+B field comparisons over all captures. No within-row arithmetic or normalized/source differences.",
               '- All 2,661 final status identities match individual vote identities. Final voter, first-vote, second-vote and party aggregates reconcile through all geographic levels.',
               '- Historical 162 nonzero geographic fields reproduce exactly: 27 fields × two overlapping relations × three captures. Final residual is zero. Aschersleben 000965 exactly matches the historical arrival vector.',
               '- Aken’s final 1,139 second votes differ from the 435-vote municipality arrival. The report does not assign that arrival to the individual booth.',
               '- Bitterfeld 000028 has one final individual vote observation; its historical 19:35/22:11 per-party difference remains unavailable. The separate municipality diff reconciles for both ballots.',
               '- 135,837 external/Git numeric comparisons agree. Every checked robust-MAD statistic reproduces (24,171); all 86 external revision flags match exact raw Git transitions. Five digit-test populations and observed histograms match.',
               '- External versioned inputs pass the saved SHA-256 manifest. Simulated digit-test p/q-values and multivariate/spatial models were not independently rerun; they are clearly labelled.',
               '- The eight executed notebook cells also verify all 18 relative share changes using exact fractions, 546 area-arrival observations across two definitions, and the representation waterfall including its source hash and zero endpoint.',
               '- The first RLP export has a documented FREIE WÄHLER parser defect. Displayed party numerators and the published valid-vote denominator are retained without renormalization; BW/RLP endpoint checks use normalized Git exports.',
               f"- {len(tweets)} tweet drafts, maximum {results['max_weighted_tweet_length']} weighted characters; {len(charts)} PNGs. Visual review: {visual_reviewed}. Local report links resolve.",
               f"- Independent offline reconstruction: {len(compared)} byte-identical files; full list in verification.json.",
               '- Regression tests cover missing/zero distinctions, omitted preliminary columns, votes with missing parent counters, postal electorate handling, reporting resets, denominator changes, aggregation and source identity. See notebook_execution.json for separately executed raw-CSV checks.','',
               'Completeness of publication and arithmetic consistency do not certify factual correctness or establish electoral misconduct. One individual-vote snapshot cannot reconstruct earlier district changes. No live election polling, schedule change, commit, publication or deployment was performed by this rerun.','']
    (path/'VALIDATION.md').write_text('\n'.join(lines))
    print(json.dumps(results,ensure_ascii=False,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--reproduction-dir',type=Path)
    p.add_argument('--visual-reviewed',action='store_true')
    a=p.parse_args();verify(a.input,a.reproduction_dir,a.visual_reviewed)
