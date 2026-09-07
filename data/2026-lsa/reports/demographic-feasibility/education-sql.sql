SELECT e.ags, e.name, e.kreis, e.valid_second_votes, e.F6_votes,
       s.SCHULABS_STP AS persons_15plus, s.SCHULABS_STP__24 AS persons_abitur,
       100.0 * s.SCHULABS_STP__24 / s.SCHULABS_STP AS abitur_share,
       100.0 * e.F6_votes / e.valid_second_votes AS F6_share
FROM municipal_vote_counts AS e JOIN census_school_counts AS s ON e.ags = s.ags
WHERE s.SCHULABS_STP > 0 AND s.SCHULABS_STP__24 IS NOT NULL AND e.valid_second_votes > 0
ORDER BY e.ags
