#!/usr/bin/env python3
"""Prepare the Berlin 2026 pre-election metadata from official sources.

The Berlin Open Data WFS publishes the 78 Abgeordnetenhauswahlkreise as
EPSG:4326 GeoJSON.  This script keeps the source attributes and adds the
names/keys used by the static site and poller.  The approved proposals
publication is represented by a compact, reviewable party catalogue.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent))
import poll_election_core as core  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "2026-be" / "metadata"
DEFAULT_DATA_DIR = ROOT / "data" / "2026-be"
WFS_URL = (
    "https://gdi.berlin.de/services/wfs/wahlgebiete_agh2026"
    "?request=GetFeature&service=WFS&version=2.0.0"
    "&typeNames=wahlgebiete_agh2026%3Aagh2026_awk"
    "&outputFormat=application%2Fjson&srsName=EPSG%3A4326"
)
GEOMETRY_DATASET_URL = (
    "https://daten.berlin.de/datensaetze/"
    "geometrien-der-wahlkreise-fur-die-wahl-zum-20-abgeordnetenhaus-von-berlin-2026"
)
WFS_DATASET_URL = (
    "https://daten.berlin.de/datensaetze/"
    "wahlgebiete-fur-die-wahl-zum-20-abgeordnetenhaus-von-berlin-2026-wfs-bc61142d"
)
APPROVED_PROPOSALS_URL = (
    "https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/"
    "allgemeine-informationen/abl_2026_36_2273_2644_98lwl.pdf?ts=1788850674"
)

DISTRICTS = {
    "01": ("Mitte", "11000001"),
    "02": ("Friedrichshain-Kreuzberg", "11000002"),
    "03": ("Pankow", "11000003"),
    "04": ("Charlottenburg-Wilmersdorf", "11000004"),
    "05": ("Spandau", "11000005"),
    "06": ("Steglitz-Zehlendorf", "11000006"),
    "07": ("Tempelhof-Schöneberg", "11000007"),
    "08": ("Neukölln", "11000008"),
    "09": ("Treptow-Köpenick", "11000009"),
    "10": ("Marzahn-Hellersdorf", "11000010"),
    "11": ("Lichtenberg", "11000011"),
    "12": ("Reinickendorf", "11000012"),
}
DISTRICT_COUNTS = {
    "01": 7,
    "02": 5,
    "03": 9,
    "04": 7,
    "05": 5,
    "06": 7,
    "07": 7,
    "08": 6,
    "09": 7,
    "10": 6,
    "11": 6,
    "12": 6,
}

# Number sequence published by the Landeswahlausschuss.  The catalogue
# intentionally excludes the 11 Einzelbewerber; it is a party/group catalogue
# for the election UI, while the source publication remains linked below.
PARTIES = [
    (1, "Christlich Demokratische Union Deutschlands", "CDU", "Partei", True),
    (2, "Sozialdemokratische Partei Deutschlands", "SPD", "Partei", True),
    (3, "BÜNDNIS 90/DIE GRÜNEN", "GRÜNE", "Partei", True),
    (4, "Die Linke", "Die Linke", "Partei", True),
    (5, "Alternative für Deutschland", "AfD", "Partei", True),
    (6, "Freie Demokratische Partei", "FDP", "Partei", True),
    (7, "PARTEI MENSCH KLIMA TIERSCHUTZ", "Tierschutzpartei", "Partei", True),
    (
        8,
        "Partei für Arbeit, Rechtsstaat, Tierschutz, Elitenförderung und basisdemokratische Initiative",
        "Die PARTEI",
        "Partei",
        True,
    ),
    (9, "Volt Deutschland", "Volt", "Partei", True),
    (10, "FREIE WÄHLER", "FREIE WÄHLER", "Partei", False),
    (11, "Mieterpartei", "Mieterpartei", "Partei", False),
    (12, "Die Urbane. Eine HipHop Partei", "Die Urbane.", "Partei", True),
    (13, "Deutsche Kommunistische Partei", "DKP", "Partei", True),
    (14, "Ökologisch-Demokratische Partei", "ÖDP", "Partei", True),
    (15, "Die Heimat", "Die Heimat", "Partei", False),
    (16, "bergpartei, die überpartei", "bergpartei", "Partei", False),
    (17, "Sozialistische Gleichheitspartei, Vierte Internationale", "SGP", "Partei", True),
    (18, "Menschliche Welt", "Menschliche Welt", "Partei", False),
    (19, "Wählergemeinschaft: Antifaschistisches Bündnis Spandau", "AB Spandau", "Wählergemeinschaft", False),
    (20, "Wählergemeinschaft: Ausländer*rein! Ausländer für Berlin", "Ausländer für Berlin", "Wählergemeinschaft", False),
    (21, "Wählergemeinschaft: Ausländer* rein! Migrants for Neukölln", "Migrants for Neukölln", "Wählergemeinschaft", False),
    (22, "Wählergemeinschaft: Berliner:innen für eine Welt ohne Grenzen, Krieg und Ausbeutung", "Berliner:innen ohne Grenzen", "Wählergemeinschaft", False),
    (23, "Wählergemeinschaft: Berlin Retten - Treptow-Köpenick steht auf", "Berlin Retten", "Wählergemeinschaft", False),
    (24, "Bündnis Sahra Wagenknecht - Vernunft und Gerechtigkeit", "BSW", "Partei", True),
    (25, "Losdemokratie - Partei eine starke Bürgerschaft", "Losdemokratie", "Partei", False),
    (26, "MERA25 - Gemeinsam für Frieden, Solidarität und Freiheit", "MERA25", "Partei", False),
    (27, "Partei des Fortschritts", "PdF", "Partei", True),
    (28, "Wählergemeinschaft: Wählergemeinschaft: Jan Mihm: Bester Mann", "Jan Mihm: Bester Mann", "Wählergemeinschaft", False),
    (29, "Demokratische Linke", "Demokratische Linke", "Partei", False),
    (30, "Feministische Partei DIE FRAUEN", "DIE FRAUEN", "Partei", False),
]

SNAPSHOT_FIELDS = [
    "row_key",
    "ags",
    "municipality_name",
    "gebietsart",
    "gebietsnummer",
    "wahlkreisnummer",
    "wahlbezirk_name",
    "wahllokal",
    "reported_precincts",
    "total_precincts",
    "voters_total",
    "valid_votes_erst",
    "valid_votes_zweit",
    "voters_total_2021",
    "valid_votes_erst_2021",
    "valid_votes_zweit_2021",
    "delta_voters_total_vs_2021",
    "delta_valid_votes_erst_vs_2021",
    "delta_valid_votes_zweit_vs_2021",
    "payload_hash",
    "is_municipality_summary",
]
PARTY_FIELDS = [
    "row_key",
    "vote_type",
    "party_key",
    "party_name",
    "votes",
    "votes_2021",
    "share_percent_2021",
    "delta_votes_vs_2021",
    "delta_share_percent_vs_2021",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--geometry-input",
        type=Path,
        help="Local GeoJSON downloaded from the official Berlin WFS.",
    )
    parser.add_argument(
        "--geometry-url",
        default=WFS_URL,
        help="Official WFS URL used when --geometry-input is omitted.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def read_geometry(path: Path | None, url: str) -> Dict[str, Any]:
    if path:
        return json.loads(path.read_text(encoding="utf-8"))
    request = Request(url, headers={"User-Agent": "wahl-monitor.de metadata preparation"})
    with urlopen(request, timeout=120) as response:  # nosec B310 - official fixed URL
        return json.loads(response.read().decode("utf-8"))


def normalized_wahlkreis_code(raw: Any) -> str:
    digits = "".join(character for character in str(raw or "") if character.isdigit())
    if not digits:
        raise ValueError(f"missing Wahlkreis code: {raw!r}")
    return digits.zfill(4)[-4:]


def normalize_features(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    features = payload.get("features") or []
    normalized: List[Dict[str, Any]] = []
    for feature in features:
        source_props = feature.get("properties") or {}
        code = normalized_wahlkreis_code(source_props.get("awk"))
        district_code = code[:2]
        local_number = int(code[2:])
        district_name, ags = DISTRICTS.get(district_code, (f"Bezirk {district_code}", ""))
        global_number = sum(
            DISTRICT_COUNTS[previous_code]
            for previous_code in DISTRICTS
            if int(previous_code) < int(district_code)
        ) + local_number
        number = str(global_number)
        normalized.append(
            {
                "type": "Feature",
                "id": feature.get("id") or f"agh2026_awk.{number}",
                "geometry": feature.get("geometry"),
                "properties": {
                    "Nummer": number,
                    "WK Name": f"{district_name} {local_number}",
                    "wahlkreis_code": code,
                    "wahlkreis_nummer": global_number,
                    "bezirk_nummer": district_code,
                    "bezirk_name": district_name,
                    "wahlkreis_im_bezirk": local_number,
                    "ags": ags,
                    "source_id": source_props.get("id"),
                },
            }
        )
    normalized.sort(key=lambda feature: int(feature["properties"]["Nummer"]))
    if len(normalized) != 78:
        raise ValueError(f"expected 78 Berlin Wahlkreise, found {len(normalized)}")
    if len({feature["properties"]["Nummer"] for feature in normalized}) != 78:
        raise ValueError("Wahlkreis numbers are not unique")
    return normalized


def write_csv(
    path: Path,
    fieldnames: Iterable[str],
    rows: Iterable[Dict[str, Any]],
    *,
    delimiter: str = ",",
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


def write_zero_latest_exports(data_dir: Path, prepared_at: str) -> None:
    """Seed the static generator with an explicit, empty pre-election state."""
    latest_dir = data_dir / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "reports").mkdir(parents=True, exist_ok=True)
    write_csv(latest_dir / "statla_snapshots.csv", SNAPSHOT_FIELDS, [])
    write_csv(latest_dir / "kommone_snapshots.csv", ["ags", "municipality_name", "status"], [])
    write_csv(latest_dir / "kommone_party_results.csv", ["ags", "vote_type", "party", "votes", "percent"], [])

    party_rows = []
    for _number, _name, short_name, _proposal_type, _state_list in PARTIES:
        for vote_type, prefix in (("Erststimmen", "D"), ("Zweitstimmen", "F")):
            party_rows.append(
                {
                    "row_key": "berlin:LAND",
                    "vote_type": vote_type,
                    "party_key": f"{prefix}{_number}",
                    "party_name": short_name,
                    "votes": 0,
                    "votes_2021": "",
                    "share_percent_2021": "",
                    "delta_votes_vs_2021": "",
                    "delta_share_percent_vs_2021": "",
                }
            )
    write_csv(latest_dir / "statla_party_results.csv", PARTY_FIELDS, party_rows)
    write_csv(latest_dir / "official-results-source.csv", ["source", "url", "status"], [])
    write_csv(
        data_dir / "reports" / "latest_events.csv",
        ["event_time_utc", "source", "ags", "municipality_name", "event_type", "details_json"],
        [],
    )
    (latest_dir / "official_results_source_metadata.json").write_text(
        json.dumps(
            {
                "status": "not-published",
                "source_url": "https://www.wahlen-berlin.de/wahlen/Be2026/AFSPRAES/agh/",
                "note": "The official 2026 results portal is prepared for election day; no result file was available during metadata preparation.",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (latest_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "generated_at_utc": prepared_at,
                "run_label": "berlin-pre-election",
                "election_key": "2026-be",
                "election_date": "2026-09-20",
                "statla_mode": "PREPARED",
                "statla_url": "https://www.wahlen-berlin.de/wahlen/Be2026/AFSPRAES/agh/",
                "statla_error": "No official 2026 result file published yet; dashboard is in pre-election mode.",
                "overview_reported_precincts": 0,
                "overview_total_precincts": 0,
                "source_status": "pre-election",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def write_metadata(output_dir: Path, payload: Dict[str, Any], geometry_url: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    features = normalize_features(payload)
    geometry = {
        "type": "FeatureCollection",
        "name": "wahlkreise_agh2026_berlin",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": features,
    }
    (output_dir / "wahlkreise.geojson").write_text(
        json.dumps(geometry, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    municipality_rows = [
        {
            "ags": ags,
            "municipality_name": name,
            "source": "Amt für Statistik Berlin-Brandenburg / Berlin 2026 Bezirke",
        }
        for _district_code, (name, ags) in DISTRICTS.items()
    ]
    write_csv(output_dir / "municipalities.csv", ["ags", "municipality_name", "source"], municipality_rows)

    mapping_rows = []
    for feature in features:
        props = feature["properties"]
        mapping_rows.append(
            {
                "Wahlkreisnummer": props["Nummer"],
                "Wahlkreiscode": props["wahlkreis_code"],
                "Wahlkreisname": props["WK Name"],
                "Gemeindekennziffer": props["ags"],
                "Gemeindename": props["bezirk_name"],
                "Bezirk": props["bezirk_nummer"],
            }
        )
    write_csv(
        output_dir / "wahlkreis-mapping.csv",
        ["Wahlkreisnummer", "Wahlkreiscode", "Wahlkreisname", "Gemeindekennziffer", "Gemeindename", "Bezirk"],
        mapping_rows,
        delimiter=";",
    )

    party_rows = [
        {
            "nummer": number,
            "bezeichnung": name,
            "kurzbezeichnung": short_name,
            "wahlvorschlagstyp": proposal_type,
            "landesliste_zugelassen": "ja" if state_list else "nein",
            "quelle": "Amtsblatt für Berlin Nr. 36 vom 27. August 2026",
        }
        for number, name, short_name, proposal_type, state_list in PARTIES
    ]
    write_csv(
        output_dir / "parties.csv",
        [
            "nummer",
            "bezeichnung",
            "kurzbezeichnung",
            "wahlvorschlagstyp",
            "landesliste_zugelassen",
            "quelle",
        ],
        party_rows,
    )

    status_rows = [
        {
            "wahlkreisnummer": feature["properties"]["Nummer"],
            "wahlkreisname": feature["properties"]["WK Name"],
            "status": "prestart",
            "reported_precincts": "",
            "total_precincts": "",
            "municipalities_total": "1",
            "municipalities_complete": "0",
            "municipalities_pending": "0",
            "municipalities_no_data": "1",
        }
        for feature in features
    ]
    write_csv(
        output_dir / "wahlkreis-status.csv",
        list(status_rows[0].keys()),
        status_rows,
    )
    core.set_active_election(election_key="2026-be")
    core.load_config()
    core.render_wahlkreis_svg(features, status_rows)

    prepared_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    manifest = {
        "election_key": "2026-be",
        "election_name": "Wahl zum 20. Abgeordnetenhaus von Berlin 2026",
        "election_date": "2026-09-20",
        "prepared_at_utc": prepared_at,
        "status": "pre-election",
        "wahlkreise": 78,
        "bezirke": 12,
        "landeslisten": sum(1 for party in PARTIES if party[4]),
        "party_or_group_entries": len(PARTIES),
        "individual_candidates_in_source": 11,
        "geometry_source_url": geometry_url,
        "geometry_dataset_url": GEOMETRY_DATASET_URL,
        "wfs_dataset_url": WFS_DATASET_URL,
        "approved_proposals_url": APPROVED_PROPOSALS_URL,
        "geometry_crs": "EPSG:4326",
        "geometry_update_note": "Open Data catalog states that the April 2026 update changed topology only, not boundaries or attributes.",
    }
    (output_dir / "setup_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    inventory = f"""# Berlin 2026 source inventory

Prepared: `{prepared_at}`

## Election

- Election: Wahl zum 20. Abgeordnetenhaus von Berlin
- Election day: 20 September 2026
- Constituencies: 78, across 12 Bezirke

## Official sources

- [Berlin 2026 election information](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/)
- [Wahlgebietseinteilung](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/wahlgebietseinteilung/artikel.1600253.php)
- [Approved proposals](https://www.berlin.de/wahlen/wahlen/berliner-wahlen-2026/wahlvorschlaege/artikel.1600254.php)
- [Approved proposals publication (Amtsblatt Nr. 36)]({APPROVED_PROPOSALS_URL})
- [Open Data geometry dataset]({GEOMETRY_DATASET_URL})
- [Open Data WFS dataset]({WFS_DATASET_URL})
- [WFS GetFeature source]({geometry_url})

## Normalized files

- `wahlkreise.geojson`: 78 official WFS geometries, converted to EPSG:4326 and enriched with stable site keys.
- `wahlkreis-mapping.csv`: one official constituency-to-Berlin-district mapping row per constituency. `Wahlkreisnummer` is the statutory 1–78 number; `Wahlkreiscode` retains the WFS district/local code such as `0101`.
- `municipalities.csv`: the 12 Berlin Bezirke as pre-election drill-down entities.
- `parties.csv`: the 30 party/voter-group entries from the approved number sequence; the source also lists 11 individual candidates.
- `wahlkreis-status.csv`: all 78 constituencies marked `prestart` until result tracking begins.
"""
    (output_dir / "source_inventory.md").write_text(inventory, encoding="utf-8")
    write_zero_latest_exports(output_dir.parent, prepared_at)


def main() -> int:
    args = parse_args()
    payload = read_geometry(args.geometry_input, args.geometry_url)
    write_metadata(args.output_dir, payload, args.geometry_url)
    print(f"Prepared Berlin metadata in {args.output_dir}")
    print(f"Wahlkreise: 78; parties/groups: {len(PARTIES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
