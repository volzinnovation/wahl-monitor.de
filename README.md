# wahl-monitor.de

- A open source and open data project by Prof. Dr. Raphael Volz (University of Pforzheim)
- Contributors are welcome, Contact: raphael.volz@hs-pforzheim.de

## Purpose
Static election tracking, analysis, and publishing pipeline for German (state and federal) elections.

The repository is organized by election key in the form `<year>-<state>`, for example `2026-bw`. Each election gets its own config, metadata, latest normalized exports, reports, and generated static site output.

## Published Site

- Overview: `https://wahl-monitor.de/`
- Landtagswahl Baden-Württemberg 2026: `https://wahl-monitor.de/2026-bw/`
- Search Baden-Württemberg 2026: `https://wahl-monitor.de/2026-bw/search.html`
- Landtagswahl Rheinland-Pfalz 2026: `https://wahl-monitor.de/2026-rlp/`
- Search Rheinland-Pfalz 2026: `https://wahl-monitor.de/2026-rlp/search.html`
- Landtagswahl Sachsen-Anhalt 2026: `https://wahl-monitor.de/2026-lsa/`
- Search Sachsen-Anhalt 2026: `https://wahl-monitor.de/2026-lsa/search.html`

## Available Elections

- `2026-bw`: Landtagswahl Baden-Württemberg 2026
- `2026-rlp`: Landtagswahl Rheinland-Pfalz 2026
- `2026-lsa`: Landtagswahl Sachsen-Anhalt 2026

Current operational status:

- Live election currently configured for polling: none
- Next prepared election scaffold: `2026-lsa` for the Landtagswahl Sachsen-Anhalt on `2026-09-06`
- Active manual GitHub Actions workflows: `.github/workflows/`
- Archived scheduled GitHub Actions workflows: `.github/workflows-disabled/`

## Repository Layout

- `config/<election-key>.json`: election-specific configuration
- `data/<election-key>/metadata`: static inputs such as municipality mappings, dummy CSVs, geometry, and cached structure data
- `data/<election-key>/latest`: latest normalized exports committed to git
- `data/<election-key>/reports`: derived reports, charts, and event summaries
- `data/<election-key>/reference`: normalized historical reference data used before live results exist
- `data/<election-key>/raw`: raw fetch payloads for local inspection
- `data/<election-key>/history.sqlite`: local history cache, intentionally not committed
- `site/<election-key>`: generated static pages for GitHub Pages
- `site/<election-key>/search.html` and `search.json`: generated client-side search surface for Wahlkreise, Gemeinden, AGS, and Wahlbezirke
- `site/<election-key>/scenario.html` and `scenario-data.json`: generated what-if swing simulator with threshold, seat approximation, and coalition badges
- `site/index.html`: generated overview page for published elections
- `.github/workflows/`: active manual or CI workflows
- `.github/workflows-disabled/`: archived workflows, including old polling schedules

## Tools

### Polling and Ingestion

- `scripts/poll_election.py`: generic CLI entry point for the election poller
- `scripts/poll_election_core.py`: shared poller implementation, storage layer, normalization logic, and report generation
- `scripts/poll_ltw26.py`: backward-compatible wrapper around the generic poller
- `scripts/run_local_poll_loop.py`: runs the poller locally on a fixed interval
- `scripts/run_local_mock_poll.py`: runs the poller locally against mock or dummy data
- `scripts/refresh_statla_from_presentation.py`: refreshes StatLA-like outputs from the presentation fallback when direct source access is incomplete
- `scripts/maybe_disable_poll_schedule.py`: comments out the old poll workflow schedule after all Wahlkreise are complete

### Static Site and Publishing

- `scripts/generate_static_detail_pages.py`: main static-site generator for election overview, search, Wahlkreis, municipality, and booth pages
- `scripts/prepare_lsa_2021_reference.py`: normalizes the official Sachsen-Anhalt 2021 result downloads for the 2026 reference view
- `scripts/render_readme_html.py`: renders `README.md` to HTML with project styling

### Election Setup and Metadata

- `scripts/setup_rlp_2026_metadata.py`: builds Rheinland-Pfalz 2026 metadata from official published sources
- `scripts/build_rlp_zero_latest.py`: creates zero-result RLP latest exports from official metadata before live results exist
- `scripts/rlp_wahlkreis_structure.py`: parses the official RLP 2026 Wahlkreis structure workbook

### Seat Calculation and Electoral Law Helpers

- `scripts/calculate_bw_seats.py`: calculates Baden-Württemberg Landtag seats from the official Statistik BW CSV
- `scripts/calculate_seats.py`: BW seat estimator from normalized StatLA exports used by the static site
- `scripts/calculate_rlp_seats.py`: CLI for structured Rheinland-Pfalz seat-calculation payloads
- `scripts/rlp_seat_allocation.py`: RLP seat-law helper implementing threshold, majority safeguard, and balancing logic
- `scripts/rlp_interim_seat_summary.py`: official interim RLP seat summary used by published charts
- `scripts/render_rlp_landtag_half_square_interim.py`: renders the interim RLP half-square seat chart

### Maps and Charts

- `scripts/render_bw_municipality_coalition_map.py`: renders a BW municipality map by coalition majority
- `scripts/render_bw_municipality_second_vote_map.py`: renders a BW municipality map by second-vote winner share
- `scripts/render_bw_second_vote_representation_waterfall.py`: renders the political-representation waterfall chart for BW and RLP

### Analysis and Consistency Checks

- `scripts/analyze_statla_total_vote_timeline.py`: analyzes the git-tracked timeline of total valid votes
- `scripts/analyze_statla_vote_invalid_timeline.py`: analyzes the git-tracked timeline of invalid votes
- `scripts/analyze_statla_wahlkreis_consistency.py`: checks Wahlkreis-level consistency across git history
- `scripts/rebuild_history_sqlite_from_git_deltas.py`: rebuilds `history.sqlite` from git-tracked polling deltas

### Validation and Tests

- `scripts/validate_dummy_statla_result.py`: validates normalized outputs against the official dummy CSV
- `scripts/test_rlp_seat_allocation.py`: lightweight self-tests for the RLP seat allocator
- `scripts/test_map_against_schaubild8.py`: validates Wahlkreis geometry against the official reference map

## Local Workflow

Run one mock cycle from the Statistik BW dummy CSV:

```bash
python3 scripts/run_local_mock_poll.py --election-key 2026-bw --iterations 1 --limit-ags 10
```

Run a single live poll after an election opens:

```bash
python3 scripts/poll_election.py --election-key 2026-bw
```

Run the local minute loop from 18:00:

```bash
python3 scripts/run_local_poll_loop.py --election-key 2026-bw --start-at 18:00
```

Validate the dummy dataset integration:

```bash
python3 scripts/validate_dummy_statla_result.py --election-key 2026-bw
```

Print a read-only artifact status snapshot:

```bash
python3 scripts/election_status.py
python3 scripts/election_status.py --election-key 2026-bw --format json
```

Generate the static drill-down site:

```bash
python3 scripts/generate_static_detail_pages.py --election-key 2026-bw
```

Print the RLP seat-calculation input schema example:

```bash
python3 scripts/calculate_rlp_seats.py --print-example
```

Run the RLP seat-law self-tests:

```bash
python3 scripts/test_rlp_seat_allocation.py
```

Serve the generated site locally:

```bash
python3 -m http.server 8000
```

Then open:

- `http://localhost:8000/site/index.html`
- `http://localhost:8000/site/2026-bw/index.html`
- `http://localhost:8000/site/2026-bw/search.html`
- `http://localhost:8000/site/2026-rlp/index.html`
- `http://localhost:8000/site/2026-rlp/search.html`
- `http://localhost:8000/site/2026-lsa/index.html`
- `http://localhost:8000/site/2026-lsa/search.html`

## GitHub Pages Procedure

1. Generate `site/` locally with `python3 scripts/generate_static_detail_pages.py --election-key <election-key>`.
2. Inspect `site/<election-key>/index.html` and `site/<election-key>/search.html` locally.
3. Trigger `.github/workflows/pages.yml` manually on GitHub to rebuild, validate generated search indexes, and deploy the static site.

## Adding Another Election

1. Create `config/<year>-<state>.json`.
2. Add required metadata files under `data/<year>-<state>/metadata`.
3. Run the poller once with `--election-key <year>-<state>`.
4. Generate the static site with `scripts/generate_static_detail_pages.py`.
5. If scheduled polling should resume, adapt the archived workflow in `.github/workflows-disabled/` for the new election and move it back into `.github/workflows/`.

## Data Sources

- `komm.one` municipality HTML result pages
- Statistik BW CSV export from `wahlen.statistik-bw.de`
- official RLP result presentation CSV and JSON assets
- Statistik BW dummy CSV for pre-election and local mock runs
- official Wahlkreis geometry and mapping files
- cached `komm.one` 2021 structure data for municipality and polling-place drill-down

## Notes

- The HTML dashboard under `site/<election-key>/index.html` is the primary operational view.
- `README.md` is manual project documentation and is no longer rewritten by polling runs.
- Generated local artifacts such as `history.sqlite`, `data/*/raw`, `site/*`, and `README.html` do not need to be committed.
