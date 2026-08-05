# Sachsen-Anhalt 2026 Setup Notes

Last checked: `2026-08-05`.

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
- Published 2026 results portal: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/>
- Published 2026 land result presentation: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/erg_land.html>
- Published 2026 downloads page: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html>
- The downloads page currently states that empty files will be provided from the second half of August; it will publish interim results on election day and the preliminary result overnight.
- Verified historical 2021 results portal: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/index.php>
- Verified historical 2021 download page: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/and/lt.download.php>

## Confirmed setup inputs
- The statistical office publishes 41 Wahlkreis geometries in both GeoJSON and ESRI Shapefile ZIPs.
- The municipality assignment workbook contains 227 Wahlkreis–municipality relationships covering 218 unique AGS.
- The local derived files are `wahlkreise.geojson`, `wahlkreis-mapping.csv`, and `municipalities.csv`.
- The five split municipalities are recorded separately in `split_municipalities.csv`. The official page identifies the split municipality and Wahlkreis ranges; the workbook gives the exact district-level split only for Leuna and Petersberg.

## Population Reference
- Latest available official estimate used for preparation: `2,120,100` inhabitants on `2025-12-31`.
- Latest published official year-end count: `2,135,597` inhabitants on `2024-12-31`.
- Source for both figures: Statistisches Jahrbuch Sachsen-Anhalt 2025, <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Daten_und_Veroeffentlichungen/Veroeffentlichungen/Statistisches_Jahrbuch/6Z001_2025-A.pdf>

## Next Local Tasks
- The poller now checks the downloads page automatically and selects the best CSV matching the expected StatLA schema once the empty file(s) are published.
- Resolve the municipal subareas for the split city administrations if the live source needs municipality-level reconstruction.
- Verify the eventual live result schema and party codebook before election night.
