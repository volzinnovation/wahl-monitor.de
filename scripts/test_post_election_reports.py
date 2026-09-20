#!/usr/bin/env python3
"""Offline regressions for denominator, geography and completion safeguards."""
import copy
import unittest

import numpy as np

from analyze_post_election import ballot_corr,wcorr,weighted_midrank
from post_election_common import integer,raw_rows,raw_context,validate_capture
from prepare_post_election_demographics import number,ratio


class StatisticsTests(unittest.TestCase):
    def test_weighted_correlation_matches_frequency_expansion(self):
        x=np.array([1.,1.,4.,9.]);y=np.array([.2,.5,.4,.8]);n=np.array([2,5,3,4])
        self.assertAlmostEqual(wcorr(x,y,n),np.corrcoef(np.repeat(x,n),np.repeat(y,n))[0,1],places=12)

    def test_binary_context_matches_literal_ballots(self):
        x=np.array([1.,4.,9.]);k=np.array([1,2,5]);n=np.array([3,6,7])
        yy=np.concatenate([np.r_[np.ones(a),np.zeros(b-a)] for a,b in zip(k,n)])
        self.assertAlmostEqual(ballot_corr(x,k,n),np.corrcoef(np.repeat(x,n),yy)[0,1],places=12)

    def test_weighted_ties_and_constant_input(self):
        np.testing.assert_allclose(weighted_midrank([1,1,4],[2,4,2]),[.375,.375,.875])
        self.assertTrue(np.isnan(wcorr([1,1,1],[2,4,8],[1,1,1])))

    def test_suppression_is_not_zero(self):
        self.assertTrue(np.isnan(number('/')))
        self.assertTrue(np.isnan(ratio(10,0)))
        self.assertEqual(number('–'),0)
        self.assertIsNone(integer(''))
        self.assertEqual(integer('0'),0)


class GeographyTests(unittest.TestCase):
    def test_combined_mv_payload_headers_and_postal_amt(self):
        payload=('''# SOURCE: booth.csv
Berechnungsdatum;Ausgabe;Gemeinde;Gemeindename;Amt;Amtsname;Wahlkreis;Wahlbezirk;Wahlbezirksname;Wahlberechtigte;Wähler;Erst-/Zweitstimme;Gültige Stimmen;Ungültige Stimmen
20.09.2026;A;13071751;Briefwahl Demmin-Land;5151;Demmin-Land;14;901;901/Briefwahl;0;101;2;100;1
# SOURCE: land.csv
Berechnungsdatum;Ausgabe;Wahlkreis;Wahlkreisname/Land;Wahlberechtigte;Wähler;Erst-/Zweitstimme;Gültige Stimmen;Ungültige Stimmen
20.09.2026;A;99;Mecklenburg-Vorpommern;150;101;2;100;1
''').encode()
        rows=raw_rows('2026-mv',payload)
        self.assertEqual(len(rows),2)
        context=raw_context('2026-mv',rows)
        booth=context['mv:WAHLBEZIRK:13071751:14:901']
        self.assertEqual(booth['geo_id'],'130715151')
        self.assertEqual(booth['mode'],'Briefwahl')
        self.assertEqual(context['mv:LAND']['eligible'],150)

    def test_berlin_district_is_not_municipal_ags(self):
        row=dict(StimmArt='2',Gebietsart='Bezirk',Nummer='01',WberIns='100',Waehler='80',Gueltig='78',Unguelt='2',Gebietsname='Mitte',Datum='26.09.20',Zeit='23:00')
        c=raw_context('2026-be',[row])['berlin:GEMEINDE:11000001']
        self.assertEqual(c['geo_id'],'1101')


class CompletionTests(unittest.TestCase):
    def setUp(self):
        def area(key,level,n):return dict(row_key=key,gebietsart=level,reported_precincts=n,total_precincts=n,
            voters_total=10*n,valid_votes_erst=9*n,valid_votes_zweit=9*n)
        self.snapshots=[area('berlin:LAND','LAND',78),area('berlin:GEMEINDE:11000001','GEMEINDE',78)]
        self.snapshots += [area(f'berlin:WAHLKREIS:{i}','WAHLKREIS',1) for i in range(1,79)]
        self.parties=[dict(row_key=s['row_key'],vote_type=t,party_name='P',votes=s['valid_votes_zweit'])
                      for s in self.snapshots for t in ['Erststimmen','Zweitstimmen']]
        self.meta=dict(run_label='fixture',generated_at_utc='2026-09-20T21:00:00Z',csv_reported_precincts=78,csv_total_precincts=78)
        self.context={s['row_key']:dict(valid=s['valid_votes_zweit'],voters=s['voters_total'],invalid=s['voters_total']-s['valid_votes_zweit']) for s in self.snapshots}

    def status(self):return validate_capture('2026-be',self.meta,self.snapshots,self.parties,self.context)[0]

    def test_complete_reconciled_capture(self):self.assertTrue(self.status()['ready'])

    def test_land_counter_alone_is_not_enough(self):
        self.snapshots[-1]['reported_precincts']=0
        self.assertFalse(self.status()['ready'])

    def test_party_aggregation_mismatch_blocks_completion(self):
        self.parties[-1]['votes']-=1
        self.assertFalse(self.status()['ready'])
        self.assertFalse(self.status()['reconciliation_passed'])

    def test_missing_constituency_blocks_completion(self):
        removed=self.snapshots.pop()['row_key']
        self.parties=[p for p in self.parties if p['row_key']!=removed]
        self.assertIn('Missing constituencies',self.status()['issues'])

    def test_raw_capture_mismatch_blocks_completion(self):
        self.context['berlin:LAND']['valid']-=1
        self.assertFalse(self.status()['ready'])

    def test_duplicate_party_cannot_double_count(self):
        self.parties.append(copy.copy(self.parties[0]))
        with self.assertRaises(ValueError):self.status()


if __name__=='__main__':unittest.main()
