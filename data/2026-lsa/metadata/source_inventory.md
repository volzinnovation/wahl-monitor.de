# Sachsen-Anhalt 2026 Setup Notes

## Official Published Inputs
- Election information hub: <https://wahlen.sachsen-anhalt.de/zu-den-wahlen/landtagswahl>
- Official FAQ page for the 2026 election: <https://wahlen.sachsen-anhalt.de/zu-den-wahlen/landtagswahl/faqs-zur-landtagswahl-2026>
- Official notice confirming the election date `2026-09-06`: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2025_09_29_Bek_der_Landeswahlleiterin_nach___28_Abs_2_LWO.pdf>
- Official 2026 Wahlkreiseinteilung PDF: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2026_Wahlkreiseinteilung_fuer_Landtagswahlen_in_Sachsen_Anhalt.pdf>
- Official 2026 Wahlkreiskarte PDF: <https://wahlen.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MI/wahlen/PDF/2026_Landtagswahl_Wahlkreiskarte.pdf>
- Official results portal root: <https://wahlergebnisse.sachsen-anhalt.de/>
- Verified historical 2021 results portal: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/index.php>
- Verified historical 2021 download page: <https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt21/and/lt.download.php>

## Current Gaps
- No verified 2026 results portal path is published yet. `https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/index.php` returned `404` on `2026-03-23`.
- No verified 2026 CSV download endpoint is published yet.
- No official GeoJSON or SHP download for the 2026 Wahlkreise has been confirmed in this pass.
- Municipality seed data and Wahlkreis-to-municipality mapping still need to be assembled from official 2026 publications.

## Population Reference
- Latest available official estimate used for preparation: `2,120,100` inhabitants on `2025-12-31`.
- Latest published official year-end count: `2,135,597` inhabitants on `2024-12-31`.
- Source for both figures: Statistisches Jahrbuch Sachsen-Anhalt 2025, <https://statistik.sachsen-anhalt.de/fileadmin/Bibliothek/Landesaemter/StaLa/startseite/Daten_und_Veroeffentlichungen/Veroeffentlichungen/Statistisches_Jahrbuch/6Z001_2025-A.pdf>

## Next Local Tasks
- Confirm the 2026 live results portal pattern and CSV export once the Landeswahlleiter publishes it.
- Extract or build a semicolon-delimited `wahlkreis-mapping.csv` with `Wahlkreisnummer`, `Wahlkreisname`, `Gemeindekennziffer`, and `Gemeindename`.
- Add reusable Wahlkreis geometry once an official vector source is available or a clean derivation path is agreed.
- Build `municipalities.csv` from the official municipal inventory before election night.
