# Berlin 2026 source inventory

Prepared: `2026-09-08T09:01:56Z`

## Election

- Election: Wahl zum 20. Abgeordnetenhaus von Berlin
- Election day: 20 September 2026
- Constituencies: 78, across 12 Bezirke

## Official sources

- [Berlin 2026 election information](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/)
- [Wahlgebietseinteilung](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/wahlgebietseinteilung/artikel.1600253.php)
- [Approved proposals](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/wahlvorschlaege/artikel.1600254.php)
- [Approved proposals publication (Amtsblatt Nr. 36)](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/allgemeine-informationen/abl_2026_36_2273_2644_98lwl.pdf?ts=1788850674)
- [Open Data geometry dataset](https://daten.berlin.de/datensaetze/geometrien-der-wahlkreise-fur-die-wahl-zum-20-abgeordnetenhaus-von-berlin-2026)
- [Open Data WFS dataset](https://daten.berlin.de/datensaetze/wahlgebiete-fur-die-wahl-zum-20-abgeordnetenhaus-von-berlin-2026-wfs-bc61142d)
- [WFS GetFeature source](https://gdi.berlin.de/services/wfs/wahlgebiete_agh2026?request=GetFeature&service=WFS&version=2.0.0&typeNames=wahlgebiete_agh2026%3Aagh2026_awk&outputFormat=application%2Fjson&srsName=EPSG%3A4326)

## Normalized files

- `wahlkreise.geojson`: 78 official WFS geometries, converted to EPSG:4326 and enriched with stable site keys.
- `wahlkreis-mapping.csv`: one official constituency-to-Berlin-district mapping row per constituency. `Wahlkreisnummer` is the statutory 1–78 number; `Wahlkreiscode` retains the WFS district/local code such as `0101`.
- `municipalities.csv`: the 12 Berlin Bezirke as pre-election drill-down entities.
- `parties.csv`: the 30 party/voter-group entries from the approved number sequence; the source also lists 11 individual candidates.
- `wahlkreis-status.csv`: all 78 constituencies marked `prestart` until result tracking begins.
