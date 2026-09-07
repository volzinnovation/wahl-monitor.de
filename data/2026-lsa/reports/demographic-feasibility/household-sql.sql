SELECT e.ags, e.name, e.kreis, e.valid_second_votes, e.F3_votes,
       h.HH_SIZE_NAT__1 AS one_person_households, h."0_Insgesamt_" AS private_households,
       100.0 * h.HH_SIZE_NAT__1 / h."0_Insgesamt_" AS single_household_share,
       100.0 * e.F3_votes / e.valid_second_votes AS F3_share
FROM municipal_vote_counts AS e JOIN census_household_counts AS h ON e.ags = h.ags
WHERE h."0_Insgesamt_" > 0 AND e.valid_second_votes > 0
ORDER BY e.ags
