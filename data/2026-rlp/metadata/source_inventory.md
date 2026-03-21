# Rheinland-Pfalz 2026 Setup Notes

## Official Published Inputs
- Election date: `2026-03-22` from [https://landtag-rlp.de/de/wahl-2026.htm](https://landtag-rlp.de/de/wahl-2026.htm).
- Official results landing page: [https://www.wahlen.rlp.de/landtagswahl/ergebnisse](https://www.wahlen.rlp.de/landtagswahl/ergebnisse).
- Official FAQ for the result portal: [https://www.wahlen.rlp.de/landtagswahl/ergebnisse/fragen-zu-den-ergebnissen](https://www.wahlen.rlp.de/landtagswahl/ergebnisse/fragen-zu-den-ergebnissen). It says the portal can be followed live from `2026-03-22 18:45 CET`, with first visible results expected between `18:30` and `18:45`.
- Official FAQ says interim results are refreshed every three minutes. The portal pattern still matches [https://wahlen.rlp-ltw-2026.23degrees.eu/wk/0000000000000/overview](https://wahlen.rlp-ltw-2026.23degrees.eu/wk/0000000000000/overview).
- Official FAQ says current counts can be downloaded as CSV from the portal menu and links the 2026 dataset description PDF: [https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/PDF/Datensatzbeschreibung_LTW_2026.pdf](https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/PDF/Datensatzbeschreibung_LTW_2026.pdf).
- Official FAQ says the final Stimmbezirk-level CSV will be published after the final result is established on `2026-04-02 10:00 CET`.
- Official 2026 geodata ZIP: [https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Shapefiles/Geodaten_LW2026_RP.zip](https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Shapefiles/Geodaten_LW2026_RP.zip).
- Official 2026 Strukturbericht workbook: [https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/LW_2026_Strukturbericht_Wahlkreise.xlsx](https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/LW_2026_Strukturbericht_Wahlkreise.xlsx).
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
- `11100000` Koblenz: `partial`, [https://wahlen.koblenz.de/](https://wahlen.koblenz.de/)
  Scope: active city election portal plus archive/result presentation section. The 2026 portal is live and says Koblenz results will appear on Sunday 22 March from 18:30. The same portal also exposes archived landtag result pages under /wahlpraesentation/landtagswahlen/.
- `21100000` Trier: `found`, [https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021zweit.html](https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021zweit.html)
  Scope: citywide second vote plus WK24/WK25 first vote. City archive also links https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK25.html and https://www.trier.de/systemstatic/Wahlen/ltw2021/ltw2021erst_WK24.html.
- `31200000` Kaiserslautern: `found`, [https://wahlen.kaiserslautern.de/ltw202144_app.html](https://wahlen.kaiserslautern.de/ltw202144_app.html)
  Scope: WK44 and WK45 result apps. City page also links https://wahlen.kaiserslautern.de/ltw202145_app.html for the second split constituency.
- `31400000` Ludwigshafen am Rhein: `partial`, [https://ludwigshafen.de/verwaltung-politik/landtagswahl-2026](https://ludwigshafen.de/verwaltung-politik/landtagswahl-2026)
  Scope: active split-city election information page. The city now publishes a dedicated 2026 election page with FAQ and Bekanntmachungen. A separate municipal live result app was not confirmed in this pass.
- `31500000` Mainz: `found`, [https://wahl.mainz.de/wahlapp/ltw2021wk27.html](https://wahl.mainz.de/wahlapp/ltw2021wk27.html)
  Scope: split city with citywide and constituency pages. Direct page verified. Mainz also used wk28 and wk29 pages and a citywide second-vote portal.
- `31800000` Speyer: `found`, [http://chamaeleon-hosting.de/sv_speyer/wahlen/app/ltw2021.html](http://chamaeleon-hosting.de/sv_speyer/wahlen/app/ltw2021.html)
  Scope: citywide result app. Speyer archive page also links state portal pages for the same election.
- `31900000` Worms: `found`, [https://wahlen.worms.de/webapp/ltw2021.html](https://wahlen.worms.de/webapp/ltw2021.html)
  Scope: citywide result app. Direct 2021 Worms portal verified.
- `32000000` Zweibruecken: `found`, [https://www.zweibruecken.de/de/verwaltung/politik-wahlen/wahlen/landtags-und-oberbuergermeisterwahl-2026/](https://www.zweibruecken.de/de/verwaltung/politik-wahlen/wahlen/landtags-und-oberbuergermeisterwahl-2026/)
  Scope: active city election page shared with the 2026 mayoral vote. The city now has a live 2026 election hub with Bekanntmachungen, Landeswahlleiter links, and brief-vote information.
