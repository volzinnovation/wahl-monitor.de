"""Regression cases for claims made by the election-night audit."""
import unittest

from analyze_lsa_git_timeline import compare_rows, number, overview_rows, aggregation_checks, arithmetic_issues, raw_vote_view, raw_rows, is_complete


def row(reported=1, total=2, votes=100, parties=None):
    return {"key": "a", "level": "GEMEINDE", "name": "A", "number": "15001000",
            "reported_precincts": reported, "total_precincts": total,
            "voters_total": votes, "valid_votes_erst": votes, "valid_votes_zweit": votes,
            "parties": parties or {"D1": votes, "F1": votes}}


class TimelineTests(unittest.TestCase):
    def test_omitted_counters_are_not_denominator_loss_or_vote_reset(self):
        old = row(2,2)
        new = {**old, 'reported_precincts':None, 'total_precincts':None}
        event = self.event(old,new)
        self.assertEqual(event['event'], 'value_became_missing')
        self.assertFalse(is_complete(new))
        self.assertEqual(new['valid_votes_zweit'],100)

    def test_preliminary_csv_without_counter_columns_preserves_votes(self):
        data = b'Satzart;Schl\xc3\xbcsselnummer;Name;Ergebnisart;B.W\xc3\xa4hler;F.G\xc3\xbcltige.Zweitstimmen;F01.CDU\nLAN;15;LSA;V;101;100;100\n'
        parsed = raw_rows(data)['lsa:LAND:15:TOTAL']
        self.assertIsNone(parsed['reported_precincts'])
        self.assertIsNone(parsed['total_precincts'])
        self.assertEqual(parsed['parties']['F1'],100)

    def test_vote_aggregation_works_without_parent_reporting_counters(self):
        land = {**row(votes=100),'key':'lsa:LAND:15','level':'LAND','reported_precincts':None,'total_precincts':None}
        checks=aggregation_checks({land['key']:land,'a':row(votes=100)})
        checks={x['field']:x for x in checks if x['relation']=='GEMEINDE->LAND'}
        self.assertIsNone(checks['reported_precincts']['delta'])
        self.assertEqual(checks['valid_votes_zweit']['delta'],0)

    def test_eligible_bound_does_not_treat_postal_zero_as_electorate(self):
        postal={**row(),'mode':'B','extra':{'eligible_voters':0}}
        self.assertEqual(arithmetic_issues({'a':postal}),[])
        local={**postal,'mode':'U','reported_precincts':2,'extra':{'eligible_voters':90}}
        self.assertEqual(arithmetic_issues({'a':local})[0]['check'],'voters_exceed_eligible')
        self.assertEqual(arithmetic_issues({'a':{**local,'reported_precincts':1}}),[])

    def event(self, old, new):
        return compare_rows({"a": old}, {"a": new})[0]

    def test_counting_growth_is_not_revision(self):
        e = self.event(row(1), row(2, votes=200))
        self.assertEqual(e["event"], "reporting_growth")
        self.assertFalse(e["candidate"])

    def test_added_district_after_complete_is_a_denominator_change(self):
        e = self.event(row(9,9,4200), row(10,10,4635))
        self.assertIn('denominator_change', e['event'])
        self.assertIn('revision_after_complete', e['event'])
        self.assertNotIn('revision_at_fixed_reporting_count', e['event'])

    def test_late_municipal_votes_remain_visible_as_aggregate_gap(self):
        land = {**row(votes=100), 'key':'lsa:LAND:15', 'level':'LAND', 'number':'15'}
        municipality = row(votes=125)
        checks = aggregation_checks({land['key']:land,municipality['key']:municipality})
        gap = next(c for c in checks if c['relation']=='GEMEINDE->LAND' and c['field']=='valid_votes_zweit')
        self.assertEqual(gap['children_sum'],125)
        self.assertEqual(gap['parent_value'],100)
        self.assertEqual(gap['delta'],25)

    def test_initial_template_denominator_is_not_a_revision(self):
        e = self.event(row(0,0,0), row(1,10,100))
        self.assertEqual(e["event"],"reporting_universe_initialized")
        self.assertFalse(e["candidate"])

    def test_complete_guard_requires_positive_denominator(self):
        self.assertFalse(is_complete(row(0,0,0)))
        self.assertFalse(is_complete(row(2614,2660)))
        self.assertTrue(is_complete(row(2660,2660)))

    def test_booth_modes_are_combined_once(self):
        u={**row(votes=70),"area_key":"booth","level":"WAHLBEZIRK","mode":"U"}
        b={**row(votes=30),"area_key":"booth","level":"WAHLBEZIRK","mode":"B"}
        result=raw_vote_view({"u":u,"b":b})["booth"]
        self.assertEqual(result["valid_votes_zweit"],100)
        self.assertEqual(result["parties"]["F1"],100)
        self.assertEqual(result["reported_precincts"],1)
        total={**row(votes=100),"area_key":"booth","level":"WAHLBEZIRK","mode":"TOTAL"}
        result=raw_vote_view({"u":u,"b":b,"total":total})["booth"]
        self.assertEqual(result["valid_votes_zweit"],100)

    def test_party_redistribution_with_unchanged_total_is_captured(self):
        e = self.event(row(2, parties={"D1":100,"F1":70,"F2":30}), row(2, parties={"D1":100,"F1":60,"F2":40}))
        self.assertIn("revision_after_complete", e["event"])
        self.assertIn("revision_at_fixed_reporting_count", e["event"])
        self.assertEqual(e["changes"]["party:F1"]["delta"], -10)

    def test_downward_correction_can_coincide_with_growth(self):
        e = self.event(row(1, parties={"F1":100}), row(2, votes=200, parties={"F1":99,"F2":101}))
        self.assertIn("numeric_decrease", e["event"])
        self.assertNotIn("revision_at_fixed_reporting_count", e["event"])

    def test_missing_and_zero_are_distinct(self):
        self.assertIsNone(number(""))
        self.assertEqual(number("0"), 0)
        newer = row(); newer["valid_votes_zweit"] = None
        e = self.event(row(), newer)
        self.assertIn("value_became_missing", e["event"])
        self.assertIsNone(e["changes"]["valid_votes_zweit"]["delta"])

    def test_disappearance_and_reset(self):
        self.assertTrue(compare_rows({"a":row()}, {})[0]["candidate"])
        e = self.event(row(), row(0, votes=0))
        self.assertIn("data_reset", e["event"])
        self.assertIn("reported_count_decrease", e["event"])

    def test_payload_timestamp_does_not_create_vote_revision(self):
        old, new = row(), row()
        old["source_time_local"], new["source_time_local"] = "18:00", "18:05"
        self.assertEqual(compare_rows({"a":old},{"a":new}), [])

    def test_bad_party_sum_detected(self):
        bad = row(parties={"D1":100,"F1":99})
        self.assertEqual(arithmetic_issues({"a":bad})[0]["delta"], -1)

    def test_levels_not_added_together(self):
        land = {**row(votes=100), "key":"lsa:LAND:15", "level":"LAND", "number":"15"}
        kreis = {**row(votes=100), "key":"k", "level":"KREIS", "number":"15001"}
        gem = row(votes=100)
        rows = {land["key"]:land,"k":kreis,"a":gem}
        checks = aggregation_checks(rows)
        matching = [c for c in checks if c["relation"] in ["GEMEINDE->LAND", "KREIS->LAND", "GEMEINDE->KREIS"]]
        self.assertTrue(all(c["delta"] == 0 for c in matching))

    def test_overview_identity_is_not_row_position(self):
        import json
        data = {"Wahlkreis":["01","01"],"Landkreis":["K","K"],"Gemeinde":["A","B"],
                "wbz":["001","001"],"wbz_ist":[1,0],"wbz_soll":[1,1]}
        def html(d):
            return ('<script type="application/json">'+json.dumps({"x":{"tag":{"attribs":{"data":d}}}})+'</script>').encode()
        first = overview_rows(html(data))
        second = overview_rows(html({k:v[::-1] for k,v in data.items()}))
        self.assertEqual(len(first),2)
        self.assertEqual(compare_rows(first,second), [])


if __name__ == "__main__":
    unittest.main()
