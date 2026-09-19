# Mecklenburg-Vorpommern 2026 source inventory

Preparation status: 2026-09-07. Scheduled live collection starts at 19:00 CEST on 2026-09-20.

## Official sources

- Election hub: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2026/
- Result downloads page: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2026/Ergebnisse/
- Dataset description: https://www.laiv-mv.de/static/LAIV/Wahlen/2-Landtagswahlen/2026/Ergebnisse/dsb_l.pdf
- Candidate publication: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2026/
- Candidate workbook: https://www.laiv-mv.de/serviceassistent/download?id=1692101
- Musterstimmzettel for all 36 constituencies: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2026/
- Wahlkreis structure and officials: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2026/Wahlkreise-und-%E2%80%93leiter/
- Wahlkreis shape archive: https://www.laiv-mv.de/static/LAIV/Geoinformation/Dateien/Karten/LTwahl_Wahlkreise.zip
- 2021 final results page: https://www.laiv-mv.de/Wahlen/Landtagswahlen/2021/Ergebnisse/
- 2021 final results workbook: https://www.laiv-mv.de/serviceassistent/download?id=1651136
- 2021 mandate CSV: https://www.laiv-mv.de/static/LAIV/Wahlen/2-Landtagswahlen/2021/Ergebnisse/l_mandate.csv

## Result templates retained in this directory

- `l_wahlbezirke.csv`: polling-district level, absolute and percentage rows
- `l_gemeinden.csv`: municipality level, absolute and percentage rows
- `l_wahlkreise.csv`: constituency and state level, absolute and percentage rows
- `l_mandate.csv`: mandate allocation schema

The LAIV page states that the final live URLs will be published from calendar week 38. The poller therefore discovers the three result CSVs from the official downloads page rather than hard-coding a future result host.

## Local indexes

- `municipalities.csv`: municipality keys and names derived from the official municipality template
- `wahlkreis-mapping.csv`: constituency-to-municipality relations derived from the official polling-district template
- `wahlkreis-status.csv`: 36 constituencies initialized as `pending`
- `wahlkreise.geojson`: committed normalized GeoJSON geometry derived from the official shape archive
- `candidates.csv`: normalized 2026 candidate and candidacy records from the official candidate workbook

## Historical reference retained in `reference/2021`

- `areas.csv`: official 2021 state, constituency and municipality totals
- `party_results.csv`: official 2021 first- and second-vote party totals
- `wahlkreis_summary.csv`: official 2021 constituency winners and shares
- `seats.csv`: official 2021 final mandate totals
