# RLP 2026 Final Booth Analysis

## Sources

- Final official booth download page: https://www.wahlen.rlp.de/landtagswahl/ergebnisse
- Final booth CSV: https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Ergebnisdateien/2026/LW_2026_Endergebnis_Stimmbezirksebene.csv
- Final booth XLSX: https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Ergebnisdateien/2026/LW_2026_Endergebnis_Stimmbezirksebene.xlsx
- Preliminary machine-readable CSV from repo snapshot: https://rlp-ltw26.wahlen.23degrees.eu/assets/json/Wahlergebnisse_Landtagswahl_2026.csv

## Core Findings

- Final official aggregate rows match the preliminary 23degrees hierarchy on all 2,878 comparable IDs after normalizing top-level leading zeros.
- Structural booth rows in the final official export: 4751.
- Effective individually measurable booth results: 4692.
- Booth rows aggregated away into other booths: 59 (receiver rows: 41).
- Effective booth count exactly matches the preliminary statewide precinct count (4692).
- Booth-sum validation: all 312 `ST` rows match exactly, all 52 Wahlkreise match exactly, and the land total matches exactly.

## Preliminary vs Final Delta

- Matched rows: 2878.
- Largest exact-code delta bucket by absolute CDU second-vote changes: WK.

## CDU Monte Carlo Consistency

- Statewide final CDU second-vote share: 30.961%.
- Statewide observed mean absolute deviation from final share on election evening: 0.229 pp.
- Probability of a random booth arrival order being at least this consistent: 1.0000.
- Wahlkreis simulations evaluated: 52.
- Median Wahlkreis consistency p-value: 0.8217.

## Generated Reports

- data/2026-rlp/reports/official_final_vs_preliminary_delta_rows.csv
- data/2026-rlp/reports/official_final_vs_preliminary_delta_summary.json
- data/2026-rlp/reports/official_final_effective_booth_rows.csv
- data/2026-rlp/reports/official_final_aggregated_or_missing_booth_rows.csv
- data/2026-rlp/reports/official_final_booth_aggregation_summary.json
- data/2026-rlp/reports/official_final_booth_validation_wahlkreis.csv
- data/2026-rlp/reports/official_final_booth_validation_summary.json
- data/2026-rlp/reports/official_final_cdu_land_monte_carlo_path.csv
- data/2026-rlp/reports/official_final_cdu_wahlkreis_monte_carlo_summary.csv
- data/2026-rlp/reports/official_final_cdu_monte_carlo_summary.json