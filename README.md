# Landtagswahl Baden-Wuerttemberg 2026 - Tracking Template

## Tracking Window

Automated tracking is scheduled to commence at **2026-03-08 18:00 CET**.
No official results are expected before **2026-03-08 18:00 CET**, so polling is intentionally disabled until then.

## Data Sources (Planned)

- `komm.one` municipality APIs (template: `https://wahlergebnisse.komm.one/lb/produktion/wahltermin-{wahltermin}/{ags}` + `/daten/api/...`)
- Statistik BW single CSV: `https://www.statistik-bw.de/fileadmin/user_upload/Wahlen/Landesdaten/ltw26_daten.csv` (fallback: `https://www.statistik-bw.de/fileadmin/user_upload/Presse/Pressemitteilungen/2026021_LTW26-Dummy-Datei.csv`)
- Wahlkreis geometry (GeoJSON ZIP): `https://www.statistik-bw.de/fileadmin/user_upload/medien/bilder/Karten_und_Geometrien_der_Wahlkreise/LTWahlkreise2026-BW_GEOJSON.zip`
- Wahlkreis geometry (SHP ZIP): `https://www.statistik-bw.de/fileadmin/user_upload/medien/bilder/Karten_und_Geometrien_der_Wahlkreise/LTWahlkreise2026-BW_SHP.zip`

## Wahlkreis Map

![Wahlkreis status map](data/ltw26/metadata/wahlkreis-status.svg)

Map file and status table are prepared from official published geometry in `data/ltw26/metadata/`.

## Party Totals (First and Second Votes)

| Vote Type | Party | `komm.one` Count | `komm.one` Share | `statla` Count | `statla` Share |
|---|---|---:|---:|---:|---:|
| Erststimmen | D1 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D2 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D3 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D4 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D5 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D6 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D7 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D8 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D9 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D11 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D12 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D13 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D16 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D17 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D20 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D21 | 0 | 0.00% | 0 | 0.00% |
| Erststimmen | D22 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F1 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F2 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F3 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F4 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F5 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F6 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F7 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F8 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F9 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F10 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F11 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F12 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F13 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F14 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F15 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F16 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F17 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F18 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F19 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F20 | 0 | 0.00% | 0 | 0.00% |
| Zweitstimmen | F21 | 0 | 0.00% | 0 | 0.00% |

## Operations

- Local run after start: `python scripts/poll_ltw26.py`
- SQLite history DB (local cache, not committed): `data/ltw26/history.sqlite`
- Rebuild SQLite from git deltas: `python scripts/rebuild_history_sqlite_from_git_deltas.py`
- Minute automation: `.github/workflows/poll.yml`
