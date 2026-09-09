# AfD above 50% in Sachsen-Anhalt Wahlbezirke

Companion to the Die Linke + SPD + GRÜNE map, using the **same preliminary 2026
Landtagswahl second-vote results**, captured on 9 September 2026 at 13:42 CEST.

**1,395 of 2,150 in-person Wahlbezirke qualify (64.9%). No postal district
qualifies (0 of 511).** The strict comparison excludes 17 districts at exactly
50%. Qualifying districts occur in 211 of the state's 218 municipalities.

## Files

- `sachsen_anhalt_afd_majority_map.png` / `.svg`: statewide overview and city maps.
- `halle_afd_urnenwahl.png` / `.svg`: 24 qualifying districts of 126.
- `magdeburg_afd_urnenwahl.png` / `.svg`: 30 qualifying districts of 148.
- `majority_precincts.csv`: all 1,395 qualifying districts with AfD votes, valid
  second votes, unrounded share, municipality, constituency and precinct ID.
- `all_precincts.csv`: all 2,661 districts, including postal districts and ties.
- `municipality_summary.csv`: counts of qualifying districts per municipality.
- `municipality_precinct_counts.geojson`: municipality geometry in WGS84 with
  those counts, not municipality-wide party vote shares.
- `summary.json`: totals, coverage and validation checks.

SVG polygons provide names, identifiers and exact results on hover.

## Geographic interpretation

The **statewide panel uses municipality boundaries**. Each municipality is
classified by how many of its constituent in-person districts satisfy AfD >50%:

| Color | Qualifying in-person districts | Municipalities |
|---|---|---:|
| Grey | None | 7 |
| Light blue | Some, but not all | 118 |
| Blue | All | 93 |

This is a summary of precinct-level classifications, **not a map of the AfD
vote share aggregated at municipality level**. Individual precinct boundaries
outside Halle and Magdeburg are not available in this package. The statewide
overview includes all 1,395 qualifying precincts in the municipality counts;
54 have exact boundaries in the city maps and 1,341 are represented only in the
municipality overview and complete result table.

The city panels use the same official 2026 precinct geometries as the previous
map. Halle's data are georeferenced. Magdeburg's shapes are the actual vector
paths from its official 2026 PDF, in local page coordinates. Identifiers were
recovered through equivalent-shape matching against the 2025 map and checked
against the current polling-place list. All 148 shapes match at intersection
over union >0.99997. See the companion package's source evidence.

## Calculation and validation

For each official row, calculate `2 * F02.AfD > F.Gültige.Zweitstimmen` using
integers. Invalid votes are excluded and percentages are not rounded before
comparison. U/B voting modes remain separate. Every source row's full set of
party counts equals its valid-vote denominator; all precinct keys are unique.
Statewide totals: **1,315,315 valid second votes**, **576,037 AfD votes**.
All 218 municipality geometries and all 274 Halle/Magdeburg in-person district
geometries join to the official identifiers.

## Sources and reproduction

- [Official preliminary results](https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads.html).
  Captured CSV and detailed retrieval metadata are in the adjacent
  `../linke-spd-gruene-majority/` package. SHA256:
  `8e85b855f2ef7ad793cafb787db9b363c565f1ef67e30f25b88ee807a3d4aac8`.
- [BKG VG250 municipality boundaries](https://gdz.bkg.bund.de/index.php/default/open-data/wfs-verwaltungsgebiete-1-250-000-stand-01-01-wfs-vg250.html),
  geography dated 1 January 2025; all 218 IDs match the election dataset.
  **© BKG 2026 dl-de/by-2-0**. Modification: Sachsen-Anhalt subset, colored by
  counts of qualifying election precincts. Exact URL, license and checksum in
  `sources/VG250_GEM_LSA.source.json`.
- City geometry: **Datenquelle: Stadt Halle (Saale), CC BY 3.0 DE**, and
  **Landeshauptstadt Magdeburg**. Source downloads, PDF label-matching evidence
  and licenses are documented in `../linke-spd-gruene-majority/README.md`.

Rebuild offline from repository root:

```sh
python scripts/render_lsa_afd_majority_map.py
```

Requires matplotlib, shapely 2.x and pyproj, and reuses the companion renderer's
geometry helpers. No production dependencies, existing map files, or published
site files were changed. These analysis outputs are versioned in the repository
and are not added to the generated website.
