# Rheinland-Pfalz 2026 Setup Notes

## Official Published Inputs
- Election date: `2026-03-22` from [https://landtag-rlp.de/de/wahl-2026.htm](https://landtag-rlp.de/de/wahl-2026.htm).
- Official results landing page: [https://www.wahlen.rlp.de/landtagswahl/ergebnisse](https://www.wahlen.rlp.de/landtagswahl/ergebnisse).
- Official page says the detailed result portal starts about 30 minutes after polls close; a predictable portal pattern is [https://wahlen.rlp-ltw-2026.23degrees.eu/wk/0000000000000/overview](https://wahlen.rlp-ltw-2026.23degrees.eu/wk/0000000000000/overview).
- Official 2026 geodata ZIP: [https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Shapefiles/Geodaten_LW2026_RP.zip](https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Shapefiles/Geodaten_LW2026_RP.zip).
- Official 2021 machine-readable tree: [https://wahlen.rlp-ltw-2021.23degrees.eu/assets/wk-vec-tree.json](https://wahlen.rlp-ltw-2021.23degrees.eu/assets/wk-vec-tree.json).
- Official 2021 state workbook download: [https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/btw/csv/2021/LW_2021_GESAMT.xlsx](https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/btw/csv/2021/LW_2021_GESAMT.xlsx).

## Lessons From The 2026 BW Rollout
- Do not assume one municipality maps to one Wahlkreis. Split municipalities need to be identified before election night.
- Keep state-level and city-level sources separate. Secondary city portals are validation or drill-down sources, not silent replacements.
- Build the source inventory before the portal goes live. The missing work is discovery, not parsing.
- Avoid state-specific fallback logic. BW assumptions about source names, CSV shape, and HTML routes should not leak into RLP setup.

## Split Municipalities From The Official 2021 State Tree
- `11100000` Koblenz: Wahlkreise `108, 109`
- `21100000` Trier: Wahlkreise `224, 225`
- `31200000` Kaiserslautern: Wahlkreise `444, 445`
- `31400000` Ludwigshafen am Rhein: Wahlkreise `336, 337`
- `31500000` Mainz: Wahlkreise `327, 328, 329`

Only five AGS are split across multiple Wahlkreise in the official 2021 tree. Those are the main places where a secondary city source is operationally valuable.

## City Secondary Sources Found In This Pass
- `11100000` Koblenz: `partial`, [https://wahlen.koblenz.de/wahlpraesentation/landtagswahlen/](https://wahlen.koblenz.de/wahlpraesentation/landtagswahlen/)
  Scope: city archive and PDF reports. Verified archive landing page and 2021 PDF reports. No separate 2021 live HTML result app confirmed in this pass.
- `21100000` Trier: `found`, [https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021zweit.html](https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021zweit.html)
  Scope: citywide second vote plus WK24/WK25 first vote. City archive also links https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK25.html and https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK24.html.
- `31200000` Kaiserslautern: `found`, [https://wahlen.kaiserslautern.de/ltw202144_app.html](https://wahlen.kaiserslautern.de/ltw202144_app.html)
  Scope: WK44 and WK45 result apps. City page also links https://wahlen.kaiserslautern.de/ltw202145_app.html for the second split constituency.
- `31400000` Ludwigshafen am Rhein: `not_found`
  Scope: split city follow-up needed. Ludwigshafen is split across two Wahlkreise in the official tree, but this pass did not confirm a separate municipal 2021 result portal.
- `31500000` Mainz: `found`, [https://wahl.mainz.de/wahlapp/ltw2021wk27.html](https://wahl.mainz.de/wahlapp/ltw2021wk27.html)
  Scope: split city with citywide and constituency pages. Direct page verified. Mainz also used wk28 and wk29 pages and a citywide second-vote portal.
- `31800000` Speyer: `found`, [http://chamaeleon-hosting.de/sv_speyer/wahlen/app/ltw2021.html](http://chamaeleon-hosting.de/sv_speyer/wahlen/app/ltw2021.html)
  Scope: citywide result app. Speyer archive page also links state portal pages for the same election.
- `31900000` Worms: `found`, [https://wahlen.worms.de/webapp/ltw2021.html](https://wahlen.worms.de/webapp/ltw2021.html)
  Scope: citywide result app. Direct 2021 Worms portal verified.
- `32000000` Zweibruecken: `candidate`, [https://wahlen.zweibruecken.de/2021/ltw2021.html](https://wahlen.zweibruecken.de/2021/ltw2021.html)
  Scope: citywide candidate URL. Search results indicated a city portal, but certificate/path verification failed from this environment.
