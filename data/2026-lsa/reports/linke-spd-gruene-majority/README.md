# Die Linke + SPD + GRÜNE: majorities in Sachsen-Anhalt precincts

Election: **Landtagswahl Sachsen-Anhalt, 6 September 2026**. Preliminary results,
downloaded from the statistical office on **9 September 2026, 13:42 CEST**.

**105 of 2,661 Wahlbezirke** have a combined majority of valid second votes:

| Municipality | In-person districts | Postal districts |
|---|---:|---:|
| Halle (Saale) | 33 of 126 | 30 of 60 |
| Magdeburg | 16 of 148 | 24 of 70 |
| Möckern | 1 of 28 | 0 of 2 |
| Dessau-Roßlau | 0 of 57 | 1 of 22 |
| Statewide, including all other municipalities | **50 of 2,150** | **55 of 511** |

## Maps and data

- `sachsen_anhalt_majority_map.png` / `.svg`: state overview and exact in-person
  precinct geometry in Halle and Magdeburg. Friedensau is represented by its
  polling-place location because a verified precinct boundary was unavailable.
- `halle_urnenwahl.png` / `.svg` and `magdeburg_urnenwahl.png` / `.svg`: enlarged
  maps with precinct identifiers and full-city context.
- `halle_briefwahl.png` / `.svg`: separate postal-catchment map. Actual official
  postal district boundaries are available for Halle. The other **25 qualifying
  postal districts (24 Magdeburg, 1 Dessau-Roßlau) have no verified geographic
  boundaries in this package** and are included in the CSV.
- `majority_precincts.csv`: all 105 matches, with party counts, denominator,
  unrounded percentage, municipality, constituency, precinct ID and U/B mode.
- `all_precincts.csv`: all 2,661 precincts and their classification.
- `halle_majority.geojson`: the 63 qualifying Halle in-person and postal polygons
  in WGS84. Filter by `wahlart` before mapping because the two modes overlap.
- `summary.json` and `source_metadata.json`: validation totals and provenance.

Hover over precinct polygons in the SVG files to see vote counts and percentages.
The Halle postal detail uses shortened labels (e.g. 59 means precinct 90059).
The maps use one highlight for strict majorities and neutral fill for all other
precincts. State overview markers indicate location, not precinct area or vote volume.

## Calculation and validation

The numerator is `F03.Die Linke + F04.SPD + F06.GRÜNE`; the denominator is
`F.Gültige.Zweitstimmen`. The exact integer test is `2 * numerator > denominator`.
Invalid votes are excluded. Rounded party percentages are never added. First votes
and voter turnout are not used. Urnenwahl (`U`) and Briefwahl (`B`) stay separate.

All 2,661 rows have distinct municipality/constituency/precinct keys and positive
denominators. Every row's complete set of party counts sums to its denominator.
The statewide sum is **1,315,315 valid second votes**, matching the repository's
LAND snapshot. The downloaded CSV has identical parsed records to the archived
7 September snapshot, with a different line-ending convention. All 186 Halle
geometry IDs join to the result file. See the Magdeburg geometry notes for its
PDF boundary extraction and identifier validation.

Friedensau (Möckern), precinct `000007`: **7 Linke + 35 SPD + 19 GRÜNE = 61/121,
or 50.4132%**. The marker represents the polling building at Ahornstraße 1 and
does not imply that only the building's residents contributed to the result.

## Sources

- [Statistisches Landesamt: preliminary results download page](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html)
- [Official precinct results CSV](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads/Ergebnisse_WBZ_LT_2026.csv),
  captured as `Ergebnisse_WBZ_LT_2026.csv`; SHA256 in `source_metadata.json`.
- [Official precinct data dictionary](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads/DSB_WBZ_LT_2026.pdf).
- [Halle 2026 in-person precinct polygons](https://webapp.halle.de/komgis30.hal.opendata/f977c527-18af-a33e-1095-607752b26ed0.html)
  and [postal precinct polygons](https://webapp.halle.de/komgis30.hal.opendata/f3adb8a7-6326-e10e-f75a-5e0ce46f2c2b.html).
  Attribution: **Datenquelle: Stadt Halle (Saale)**,
  [CC BY 3.0 DE](https://halle.de/verwaltung-stadtrat/stadtverwaltung/online-angebote/open-data-portal/nutzungsbedingungen-und-lizenzen).
  Original shapefile ZIPs in `sources/`; transformed from EPSG:2398 to EPSG:25832.
  Negligible numerical self-intersections in two nonqualifying polygons are
  repaired with Shapely `make_valid` before plotting.
- [Landeshauptstadt Magdeburg: official 2026 precinct map](https://www.magdeburg.de/loadDocument.phtml?Ext=PDF&ObjID=24496&ObjLa=1&ObjSvrID=698).
  Actual vector boundaries extracted in PDF page coordinates for the city inset;
  these are not a georeferenced geographic dataset.
  Labels suppressed on the 2026 map were recovered from the
  [official 2025 map](https://www.magdeburg.de/loadDocument.phtml?Ext=PDF&ObjID=22655&ObjLa=1&ObjSvrID=698)
  only after matching equivalent shapes. All 148 current polygons match with
  intersection-over-union above 0.99997; the selected 16 exceed 0.999984.
  Identifiers match the [current polling-place list](https://www.magdeburg.de/loadDocument.phtml?Ext=PDF&ObjID=23752&ObjLa=1&ObjSvrID=698).
  The sole identifier assigned by elimination, 3601, is not selected. Evidence
  for every mapping is recorded in `sources/magdeburg_mapping_evidence.json`.
- State outline: repository `data/2026-lsa/metadata/wahlkreise.geojson`, official
  2026 Wahlkreis data dissolved to the state boundary, EPSG:25832.
- Friedensau polling-building coordinates:
  [OpenStreetMap way 1137609885](https://www.openstreetmap.org/way/1137609885),
  © OpenStreetMap contributors, ODbL. Original API response in `friedensau_osm.json`.

## Reproduce

The renderer uses captured sources and needs no network requests:

```sh
python scripts/render_lsa_left_majority_map.py
```

Python dependencies: matplotlib, shapely (2.x), pyproj. The original Halle
conversion additionally needs pyshp. Analysis dependencies were installed in an
isolated temporary environment; no production dependency configuration changed.

These analysis outputs are versioned in the repository and are not added to the
generated website.
