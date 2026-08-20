# Sachsen-Anhalt 2026 Setup Notes

Last checked: `2026-08-20`.

## Official Published Inputs
- Election information hub: <https://wahlen.sachsen-anhalt.de/zu-den-wahlen/landtagswahl>
- Official FAQ page for the 2026 election: <https://wahlen.sachsen-anhalt.de/zu-den-wahlen/landtagswahl/faqs-zur-landtagswahl-2026>
- Statistical election page: <https://statistik.sachsen-anhalt.de/themen/gebiet-und-wahlen/wahlen/landtagswahl-2026>
- Statistical Wahlkreis overview and downloads: <https://statistik.sachsen-anhalt.de/themen/gebiet-und-wahlen/wahlen/landtagswahl-2026-2/uebersicht-wahlkreiseinteilung>
- Official notice confirming the election date `2026-09-06`: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2025_09_29_Bek_der_Landeswahlleiterin_nach___28_Abs_2_LWO.pdf>
- Official 2026 Wahlkreiseinteilung PDF: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2026_Wahlkreiseinteilung_fuer_Landtagswahlen_in_Sachsen_Anhalt.pdf>
- Official 2026 Wahlkreiskarte PDF: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2026_Landtagswahl_Wahlkreiskarte.pdf>
- Official 2026 Wahlkreis geometry, GeoJSON ZIP: <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Wahlen/Wahlkreise/Wahlkreise_LTW_2026_geosjon-Datei.zip>
- Official 2026 Wahlkreis geometry, shapefile ZIP: <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Wahlen/Wahlkreise/Wahlkreise_LTW_2026.zip>
- Official 2026 Wahlkreis-to-municipality assignment, XLSX: <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Wahlen/Wahlkreise/Landtagswahl_2026_-_Wahlkreise___Gemeinden.xlsx>
- Official comparable historical results, XLSX: <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Wahlen/Vergleichbare_Wahlergebnisse/15000_Tabelle.xlsx>
- Official comparable seat distribution, XLSX: <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Themen/Wahlen/Vergleichbare_Wahlergebnisse/15_Vergleichbare_Wahlergebnisse_Sitzverteilung.xlsx>
- Official results portal root: <https://wahlergebnisse.sachsen-anhalt.de/>
- Published 2026 results portal/presentation: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/index.html>
- Published 2026 downloads page: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html>
- The downloads page now publishes two UTF-8 empty CSVs: land/districts/Wahlkreise and municipalities. They contain the complete structural rows, but all current vote values are zero until election day.
- Direct files: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads/Ergebnisse_Land_RKR_WKR_LT_2026.csv> and <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads/Ergebnisse_Gemeinden_LT_2026.csv>
- The former `erg_land.html` path currently returns HTTP 404; the current portal entry point is `lt26/index.html`.
- Verified historical 2021 results portal: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/index.php>
- Verified historical 2021 download page: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/and/lt.download.php>
- Official 2021 land and Wahlkreis final results CSV: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/erg/csv/lt21dat1.csv>
- Official 2021 municipality final results CSV: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/erg/csv/lt21dat2.csv>
- Official 2021 seat distribution CSV: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/sitz/sitzverteilung.csv>

## Confirmed setup inputs
- The statistical office publishes 41 Wahlkreis geometries in both GeoJSON and ESRI Shapefile ZIPs.
- The municipality assignment workbook contains 227 Wahlkreis–municipality relationships covering 218 unique AGS.
- The local derived files are `wahlkreise.geojson`, `wahlkreis-mapping.csv`, and `municipalities.csv`.
- The five split municipalities are recorded separately in `split_municipalities.csv`. The official page identifies the split municipality and Wahlkreis ranges; the workbook gives the exact district-level split only for Leuna and Petersberg.
- The normalized 2021 reference tables are in `data/2026-lsa/reference/2021/`. They cover the land, 14 districts, 41 Wahlkreise, and 218 municipalities, with long party-result rows and a compact Wahlkreis winner table.
- The 2026 landing-page map uses the official 2026 Wahlkreis geometry and colors each district by the 2021 official Zweitstimmen winner until 2026 results arrive.
- The current 2026 result drill-down therefore has four published levels: `LAND`, `KREIS`, `WAHLKREIS` and `GEMEINDE`. Individual `WAHLBEZIRK` rows are not included in the official LSA downloads; the static site exposes Landkreis pages and preserves all municipality-to-Wahlkreis links, including the five split municipalities.

## Population Reference
- Latest available official estimate used for preparation: `2,120,100` inhabitants on `2025-12-31`.
- Latest published official year-end count: `2,135,597` inhabitants on `2024-12-31`.
- Source for both figures: Statistisches Jahrbuch Sachsen-Anhalt 2025, <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Daten_und_Veroeffentlichungen/Veroeffentlichungen/Statistisches_Jahrbuch/6Z001_2025-A.pdf>

## Next Local Tasks
- The poller checks the downloads page automatically, combines both published LSA CSVs, and normalizes the current `Satzart`/`Schlüsselnummer`/`F01.CDU` schema into the existing result model.
- Resolve the municipal subareas for the split city administrations if the live source needs municipality-level reconstruction.
- Verify the eventual live result schema and party codebook before election night.
