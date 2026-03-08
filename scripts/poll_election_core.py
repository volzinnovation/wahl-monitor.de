#!/usr/bin/env python3
"""
Minute poller for BW Landtagswahl 2026.

It fetches:
- municipality-level interim results from komm.one (one source per AGS)
- statewide CSV from Statistik BW

Then it stores:
- immutable raw snapshots
- normalized snapshots + party results in SQLite
- change events (new, updated, removed, reverted)
- source diffs
- an auto-generated README dashboard with party and municipality drill-down
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import sqlite3
import subprocess
import tempfile
import unicodedata
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, request
from urllib.parse import urlsplit

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Python 3.9+ is required") from exc


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ELECTION_KEY = os.environ.get("WAHL_ELECTION_KEY", "2026-bw")
ACTIVE_ELECTION_KEY = DEFAULT_ELECTION_KEY
CONFIG_PATH = ROOT / "config" / f"{DEFAULT_ELECTION_KEY}.json"
DATA_DIR = ROOT / "data" / DEFAULT_ELECTION_KEY
SITE_OUTPUT_DIR = ROOT / "site" / DEFAULT_ELECTION_KEY
DB_PATH = DATA_DIR / "history.sqlite"
RAW_KOMMONE_DIR = DATA_DIR / "raw" / "kommone"
RAW_STATLA_DIR = DATA_DIR / "raw" / "statla"
LATEST_DIR = DATA_DIR / "latest"
REPORT_DIR = DATA_DIR / "reports"
META_DIR = DATA_DIR / "metadata"
README_PATH = ROOT / "README.md"
LOCAL_DUMMY_STATLA_PATH = META_DIR / "dummy-statla.csv"
WAHLKREIS_GEOJSON_PATH = META_DIR / "wahlkreise.geojson"
WAHLKREIS_MAPPING_PATH = META_DIR / "wahlkreis-mapping.csv"
WAHLKREIS_STATUS_MAP_PATH = META_DIR / "wahlkreis-status.svg"
WAHLKREIS_STATUS_CSV_PATH = META_DIR / "wahlkreis-status.csv"

STATLA_EXCLUDED_GEBIETSART = {
    "LAND",
    "WAHLKREIS",
    "URNENWAHLBEZIRK",
    "BRIEFWAHLBEZIRK",
}

STATLA_PARTY_CODEBOOK: Dict[str, List[Tuple[str, str]]] = {
    "Erststimmen": [
        ("D1", "GRÜNE"),
        ("D2", "CDU"),
        ("D3", "SPD"),
        ("D4", "FDP"),
        ("D5", "AfD"),
        ("D6", "Die Linke"),
        ("D7", "FREIE WÄHLER"),
        ("D8", "Die PARTEI"),
        ("D9", "dieBasis"),
        ("D11", "ÖDP"),
        ("D12", "Volt"),
        ("D13", "Bündnis C"),
        ("D16", "BSW"),
        ("D17", "Die Gerechtigkeitspartei"),
        ("D20", "Tierschutzpartei"),
        ("D21", "Werteunion"),
        ("D22", "Anderer Kreiswahlvorschlag"),
    ],
    "Zweitstimmen": [
        ("F1", "GRÜNE"),
        ("F2", "CDU"),
        ("F3", "SPD"),
        ("F4", "FDP"),
        ("F5", "AfD"),
        ("F6", "Die Linke"),
        ("F7", "FREIE WÄHLER"),
        ("F8", "Die PARTEI"),
        ("F9", "dieBasis"),
        ("F10", "KlimalisteBW"),
        ("F11", "ÖDP"),
        ("F12", "Volt"),
        ("F13", "Bündnis C"),
        ("F14", "PDH"),
        ("F15", "Verjüngungsforschung"),
        ("F16", "BSW"),
        ("F17", "Die Gerechtigkeitspartei"),
        ("F18", "PDR"),
        ("F19", "PdF"),
        ("F20", "Tierschutzpartei"),
        ("F21", "Werteunion"),
    ],
}

KOMMONE_HTML_PATH_SUFFIXES = (
    "landtagswahl_gemeinde_ohne_kwl",
    "landtagswahl_kwl_1_wk",
    "landtagswahl_kwl_mehrere_wk",
)
KOMMONE_RESULT_LINK_RE = re.compile(r'href="(ergebnisse_[^"#?]+\.html)"', re.IGNORECASE)
KOMMONE_TABLE_RE = re.compile(r'<table[^>]*class="[^"]*table-stimmen[^"]*"[^>]*>(.*?)</table>', re.IGNORECASE | re.DOTALL)
KOMMONE_SECTION_RE = re.compile(r"<(tbody|tfoot)[^>]*>(.*?)</\1>", re.IGNORECASE | re.DOTALL)
KOMMONE_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
KOMMONE_CELL_RE = re.compile(r"<t[hd][^>]*>(.*?)</t[hd]>", re.IGNORECASE | re.DOTALL)
KOMMONE_TAG_RE = re.compile(r"<[^>]+>")
KOMMONE_GEMEINDE_LINK_RE = re.compile(r"ergebnisse_gemeinde_(\d{8})\.html$", re.IGNORECASE)


@dataclass(frozen=True)
class Config:
    election_key: str
    election_name: str
    election_date: str
    tracking_start_local: str
    timezone: str
    kommone_wahltermin: str
    kommone_base_url_template: str
    statla_live_csv_url: str
    statla_dummy_csv_url: str
    wahlkreise_geojson_zip_url: str
    wahlkreise_shp_zip_url: str
    wahlkreise_mapping_csv_url: str
    legacy_city_source_csv: str
    local_dummy_statla_csv_filename: str
    local_wahlkreise_geojson_filename: str
    local_wahlkreise_mapping_csv_filename: str
    request_timeout_seconds: int
    max_workers: int


@dataclass
class HttpResult:
    url: str
    status_code: Optional[int]
    content: bytes
    error_message: Optional[str]


def config_path_for_election(election_key: str) -> Path:
    return ROOT / "config" / f"{election_key}.json"


def resolve_repo_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return ROOT / path


def repo_relative_path(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def set_active_election(*, election_key: Optional[str] = None, config_path: Optional[Path] = None) -> None:
    global ACTIVE_ELECTION_KEY
    global CONFIG_PATH
    global DATA_DIR
    global SITE_OUTPUT_DIR
    global DB_PATH
    global RAW_KOMMONE_DIR
    global RAW_STATLA_DIR
    global LATEST_DIR
    global REPORT_DIR
    global META_DIR
    global LOCAL_DUMMY_STATLA_PATH
    global WAHLKREIS_GEOJSON_PATH
    global WAHLKREIS_MAPPING_PATH
    global WAHLKREIS_STATUS_MAP_PATH
    global WAHLKREIS_STATUS_CSV_PATH

    resolved_config_path = Path(config_path) if config_path is not None else None
    if resolved_config_path is None:
        resolved_key = election_key or DEFAULT_ELECTION_KEY
        resolved_config_path = config_path_for_election(resolved_key)
    else:
        resolved_key = election_key or resolved_config_path.stem

    ACTIVE_ELECTION_KEY = resolved_key
    CONFIG_PATH = resolved_config_path
    DATA_DIR = ROOT / "data" / ACTIVE_ELECTION_KEY
    SITE_OUTPUT_DIR = ROOT / "site" / ACTIVE_ELECTION_KEY
    DB_PATH = DATA_DIR / "history.sqlite"
    RAW_KOMMONE_DIR = DATA_DIR / "raw" / "kommone"
    RAW_STATLA_DIR = DATA_DIR / "raw" / "statla"
    LATEST_DIR = DATA_DIR / "latest"
    REPORT_DIR = DATA_DIR / "reports"
    META_DIR = DATA_DIR / "metadata"
    LOCAL_DUMMY_STATLA_PATH = META_DIR / "dummy-statla.csv"
    WAHLKREIS_GEOJSON_PATH = META_DIR / "wahlkreise.geojson"
    WAHLKREIS_MAPPING_PATH = META_DIR / "wahlkreis-mapping.csv"
    WAHLKREIS_STATUS_MAP_PATH = META_DIR / "wahlkreis-status.svg"
    WAHLKREIS_STATUS_CSV_PATH = META_DIR / "wahlkreis-status.csv"


def apply_config_paths(config: Config) -> None:
    global ACTIVE_ELECTION_KEY
    global LOCAL_DUMMY_STATLA_PATH
    global WAHLKREIS_GEOJSON_PATH
    global WAHLKREIS_MAPPING_PATH

    if config.election_key != ACTIVE_ELECTION_KEY or CONFIG_PATH.stem != config.election_key:
        set_active_election(election_key=config.election_key, config_path=CONFIG_PATH)

    ACTIVE_ELECTION_KEY = config.election_key
    LOCAL_DUMMY_STATLA_PATH = META_DIR / config.local_dummy_statla_csv_filename
    WAHLKREIS_GEOJSON_PATH = META_DIR / config.local_wahlkreise_geojson_filename
    WAHLKREIS_MAPPING_PATH = META_DIR / config.local_wahlkreise_mapping_csv_filename


def ensure_directories() -> None:
    for directory in [RAW_KOMMONE_DIR, RAW_STATLA_DIR, LATEST_DIR, REPORT_DIR, META_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def load_config() -> Config:
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    dummy_filename = data.get("local_dummy_statla_csv_filename")
    if not dummy_filename:
        dummy_filename = Path(urlsplit(data["statla_dummy_csv_url"]).path).name or "dummy-statla.csv"

    config = Config(
        election_key=data.get("election_key", CONFIG_PATH.stem),
        election_name=data["election_name"],
        election_date=data["election_date"],
        tracking_start_local=data.get("tracking_start_local", "2026-03-08T18:00:00"),
        timezone=data.get("timezone", "Europe/Berlin"),
        kommone_wahltermin=data["kommone_wahltermin"],
        kommone_base_url_template=data["kommone_base_url_template"],
        statla_live_csv_url=data["statla_live_csv_url"],
        statla_dummy_csv_url=data["statla_dummy_csv_url"],
        wahlkreise_geojson_zip_url=data["wahlkreise_geojson_zip_url"],
        wahlkreise_shp_zip_url=data["wahlkreise_shp_zip_url"],
        wahlkreise_mapping_csv_url=data["wahlkreise_mapping_csv_url"],
        legacy_city_source_csv=data["legacy_city_source_csv"],
        local_dummy_statla_csv_filename=dummy_filename,
        local_wahlkreise_geojson_filename=data.get("local_wahlkreise_geojson_filename", "wahlkreise.geojson"),
        local_wahlkreise_mapping_csv_filename=data.get("local_wahlkreise_mapping_csv_filename", "wahlkreis-mapping.csv"),
        request_timeout_seconds=int(data.get("request_timeout_seconds", 4)),
        max_workers=int(data.get("max_workers", 48)),
    )
    apply_config_paths(config)
    return config


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def time_labels(tz_name: str) -> Tuple[str, str]:
    ts_utc = now_utc()
    ts_berlin = ts_utc.astimezone(ZoneInfo(tz_name))
    label_file = ts_berlin.strftime("%Y-%m-%d-%H-%M-%S")
    label_human = ts_berlin.strftime("%Y-%m-%d %H:%M:%S %Z")
    return label_file, label_human


def tracking_start_local_dt(config: Config) -> datetime:
    dt = datetime.fromisoformat(config.tracking_start_local)
    tz = ZoneInfo(config.timezone)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def format_local_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def decode_bytes(content: bytes) -> str:
    for enc in ("utf-8-sig", "latin-1"):
        try:
            return content.decode(enc)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def normalize_text(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", normalized.strip().lower())


def statla_party_name_from_code(vote_type: str, code: str) -> str:
    key = str(code).strip()
    for party_code, party_name in STATLA_PARTY_CODEBOOK.get(vote_type, []):
        if key == party_code:
            return party_name
    return key


def canonical_party_name(label: str, vote_type: Optional[str] = None) -> str:
    raw = str(label or "").strip()
    if not raw:
        return ""

    if vote_type:
        raw = statla_party_name_from_code(vote_type, raw)

    normalized = normalize_text(raw)
    normalized_simple = re.sub(r"[^a-z0-9]+", " ", normalized).strip()
    aliases = {
        "gruene": "GRÜNE",
        "grune": "GRÜNE",
        "bundnis 90 die grunen": "GRÜNE",
        "bundnis 90 die gruenen": "GRÜNE",
        "b 90 die grunen": "GRÜNE",
        "cdu": "CDU",
        "spd": "SPD",
        "fdp": "FDP",
        "afd": "AfD",
        "die linke": "Die Linke",
        "freie wahler": "FREIE WÄHLER",
        "die partei": "Die PARTEI",
        "diebasis": "dieBasis",
        "die basis": "dieBasis",
        "oedp": "ÖDP",
        "odp": "ÖDP",
        "volt": "Volt",
        "bundnis c": "Bündnis C",
        "bundnis c christen fur deutschland": "Bündnis C",
        "bsw": "BSW",
        "die gerechtigkeitspartei": "Die Gerechtigkeitspartei",
        "big": "Die Gerechtigkeitspartei",
        "tierschutzpartei": "Tierschutzpartei",
        "werteunion": "Werteunion",
        "klimalistebw": "KlimalisteBW",
        "klimaliste bw": "KlimalisteBW",
        "pdh": "PDH",
        "partei der humanisten": "PDH",
        "verjungungsforschung": "Verjüngungsforschung",
        "pdr": "PDR",
        "pdf": "PdF",
        "partei des fortschritts": "PdF",
        "anderer kreiswahlvorschlag": "Anderer Kreiswahlvorschlag",
    }
    if normalized_simple.startswith("eb ") or normalized.startswith("eb:"):
        return "Anderer Kreiswahlvorschlag"
    return aliases.get(normalized, aliases.get(normalized_simple, raw))


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace(".", "").replace(" ", "").replace("\u00a0", "")
    text = re.sub(r"[^0-9\-]", "", text)
    if text in {"", "-"}:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_float_percent(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", ".")
    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in {"", "-", "."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_float_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if text == "":
        return None
    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in {"", "-", "."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def csv_rows_from_text(content: str, delimiter: str = ";") -> List[Dict[str, str]]:
    reader = csv.DictReader(content.splitlines(), delimiter=delimiter)
    return [dict(row) for row in reader]


def http_get(url: str, timeout_seconds: int) -> HttpResult:
    req = request.Request(url, headers={"User-Agent": "wahl-monitor-poller/1.0"})
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            return HttpResult(
                url=url,
                status_code=int(response.status),
                content=response.read(),
                error_message=None,
            )
    except error.HTTPError as exc:
        body = exc.read() if hasattr(exc, "read") else b""
        return HttpResult(
            url=url,
            status_code=int(exc.code),
            content=body,
            error_message=f"HTTP {exc.code}",
        )
    except Exception as exc:  # pylint: disable=broad-except
        curl_fallback = http_get_with_curl(url, timeout_seconds)
        if curl_fallback is not None:
            return curl_fallback
        return HttpResult(
            url=url,
            status_code=None,
            content=b"",
            error_message=str(exc),
        )


def http_get_with_curl(url: str, timeout_seconds: int) -> Optional[HttpResult]:
    with tempfile.NamedTemporaryFile(delete=False) as handle:
        temp_path = handle.name
    try:
        completed = subprocess.run(
            [
                "curl",
                "-L",
                "--silent",
                "--show-error",
                "--max-time",
                str(timeout_seconds),
                "-o",
                temp_path,
                "-w",
                "%{http_code}",
                url,
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        body = Path(temp_path).read_bytes()
        status_code = parse_int(completed.stdout.strip())
        if completed.returncode == 0 or status_code is not None:
            return HttpResult(
                url=url,
                status_code=status_code,
                content=body,
                error_message=None if completed.returncode == 0 else (completed.stderr.strip() or None),
            )
        return None
    except FileNotFoundError:
        return None
    finally:
        try:
            Path(temp_path).unlink()
        except FileNotFoundError:
            pass


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS polls (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          polled_at_utc TEXT NOT NULL UNIQUE,
          polled_at_local TEXT NOT NULL,
          created_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_fetches (
          poll_id INTEGER NOT NULL,
          source TEXT NOT NULL,
          url TEXT NOT NULL,
          status_code INTEGER,
          content_hash TEXT,
          byte_count INTEGER NOT NULL,
          error_message TEXT,
          fetched_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS municipalities (
          ags TEXT PRIMARY KEY,
          municipality_name TEXT NOT NULL,
          source TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kommone_snapshots (
          poll_id INTEGER NOT NULL,
          ags TEXT NOT NULL,
          municipality_name TEXT NOT NULL,
          status TEXT NOT NULL,
          reported_precincts INTEGER,
          total_precincts INTEGER,
          voters_total INTEGER,
          valid_votes INTEGER,
          invalid_votes INTEGER,
          source_timestamp TEXT,
          payload_hash TEXT,
          error_message TEXT,
          PRIMARY KEY (poll_id, ags)
        );
        CREATE INDEX IF NOT EXISTS idx_kommone_snapshots_ags_poll
          ON kommone_snapshots (ags, poll_id DESC);
        CREATE TABLE IF NOT EXISTS kommone_party_results (
          poll_id INTEGER NOT NULL,
          ags TEXT NOT NULL,
          vote_type TEXT NOT NULL,
          party TEXT NOT NULL,
          votes INTEGER,
          percent REAL,
          PRIMARY KEY (poll_id, ags, vote_type, party)
        );
        CREATE TABLE IF NOT EXISTS statla_snapshots (
          poll_id INTEGER NOT NULL,
          row_key TEXT NOT NULL,
          ags TEXT,
          municipality_name TEXT,
          gebietsart TEXT,
          gebietsnummer TEXT,
          reported_precincts INTEGER,
          total_precincts INTEGER,
          voters_total INTEGER,
          valid_votes_erst INTEGER,
          valid_votes_zweit INTEGER,
          payload_hash TEXT NOT NULL,
          PRIMARY KEY (poll_id, row_key)
        );
        CREATE TABLE IF NOT EXISTS statla_party_results (
          poll_id INTEGER NOT NULL,
          row_key TEXT NOT NULL,
          vote_type TEXT NOT NULL,
          party_key TEXT NOT NULL,
          party_name TEXT NOT NULL,
          votes INTEGER,
          PRIMARY KEY (poll_id, row_key, vote_type, party_key)
        );
        CREATE TABLE IF NOT EXISTS source_diffs (
          poll_id INTEGER NOT NULL,
          ags TEXT NOT NULL,
          municipality_name TEXT NOT NULL,
          metric TEXT NOT NULL,
          kommone_value REAL,
          statla_value REAL,
          delta REAL,
          PRIMARY KEY (poll_id, ags, metric)
        );
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          poll_id INTEGER NOT NULL,
          event_time_utc TEXT NOT NULL,
          source TEXT NOT NULL,
          ags TEXT,
          municipality_name TEXT,
          event_type TEXT NOT NULL,
          details_json TEXT NOT NULL
        );
        """
    )
    conn.commit()


def create_poll(conn: sqlite3.Connection, polled_at_utc: str, polled_at_local: str) -> int:
    conn.execute(
        """
        INSERT INTO polls (polled_at_utc, polled_at_local, created_at_utc)
        VALUES (?, ?, ?)
        """,
        (polled_at_utc, polled_at_local, now_utc().isoformat()),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM polls WHERE polled_at_utc = ?", (polled_at_utc,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to create poll row")
    return int(row[0])


def read_csv_rows_from_file(path: Path, delimiter: str = ",") -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        content = decode_bytes(path.read_bytes())
    except Exception:  # pylint: disable=broad-except
        return []
    return csv_rows_from_text(content, delimiter=delimiter)


def parse_iso_datetime(value: str) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def seed_db_from_latest_exports(conn: sqlite3.Connection, config: Config) -> None:
    metadata_path = LATEST_DIR / "run_metadata.json"
    if not metadata_path.exists():
        return

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:  # pylint: disable=broad-except
        return

    polled_at_utc = str(metadata.get("generated_at_utc") or "").strip()
    parsed_polled_at = parse_iso_datetime(polled_at_utc)
    if parsed_polled_at is None:
        return

    polled_at_local = parsed_polled_at.astimezone(ZoneInfo(config.timezone)).strftime("%Y-%m-%d %H:%M:%S %Z")
    row = conn.execute("SELECT id FROM polls WHERE polled_at_utc = ?", (polled_at_utc,)).fetchone()
    if row is None:
        poll_id = create_poll(conn, polled_at_utc=polled_at_utc, polled_at_local=polled_at_local)
    else:
        poll_id = int(row[0])

    municipalities_seed: List[Dict[str, str]] = []
    seed_paths = [META_DIR / "municipalities.csv", resolve_repo_path(config.legacy_city_source_csv)]
    seen_seed_paths: set[Path] = set()
    for path in seed_paths:
        if path in seen_seed_paths:
            continue
        seen_seed_paths.add(path)
        for row in read_csv_rows_from_file(path, delimiter=","):
            ags = canonical_ags(row.get("ags") or row.get("AGS"))
            municipality_name = canonical_municipality_name(
                row.get("municipality_name") or row.get("Gemeindename") or row.get("Gemeinde")
            )
            source = str(row.get("source") or "git-latest-seed")
            if ags and municipality_name:
                municipalities_seed.append(
                    {
                        "ags": ags,
                        "municipality_name": municipality_name,
                        "source": source,
                    }
                )
    if municipalities_seed:
        unique = {(row["ags"], row["municipality_name"], row["source"]) for row in municipalities_seed}
        store_municipalities(
            conn,
            [{"ags": ags, "municipality_name": name, "source": source} for ags, name, source in sorted(unique)],
        )

    run_label = str(metadata.get("run_label") or "").strip()
    statla_url = str(metadata.get("statla_url") or "")
    statla_error = metadata.get("statla_error")
    statla_raw_path = RAW_STATLA_DIR / f"{run_label}-statla.csv" if run_label else None
    statla_hash = None
    statla_bytes = 0
    if statla_raw_path and statla_raw_path.exists():
        raw = statla_raw_path.read_bytes()
        statla_hash = sha256_bytes(raw)
        statla_bytes = len(raw)
    existing_statla_fetch = conn.execute(
        """
        SELECT 1
        FROM source_fetches
        WHERE poll_id = ?
          AND source = 'statla'
        LIMIT 1
        """,
        (poll_id,),
    ).fetchone()
    if existing_statla_fetch is None:
        conn.execute(
            """
            INSERT INTO source_fetches (
              poll_id, source, url, status_code, content_hash, byte_count, error_message, fetched_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                poll_id,
                "statla",
                statla_url,
                200 if statla_hash else None,
                statla_hash,
                statla_bytes,
                statla_error,
                polled_at_utc,
            ),
        )

    kommone_rows = read_csv_rows_from_file(LATEST_DIR / "kommone_snapshots.csv", delimiter=",")
    existing_kommone_rows = conn.execute(
        "SELECT 1 FROM kommone_snapshots WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if kommone_rows and existing_kommone_rows is None:
        conn.executemany(
            """
            INSERT INTO kommone_snapshots (
              poll_id, ags, municipality_name, status, reported_precincts, total_precincts,
              voters_total, valid_votes, invalid_votes, source_timestamp, payload_hash, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    canonical_ags(row.get("ags")),
                    canonical_municipality_name(row.get("municipality_name")),
                    str(row.get("status") or "NO_DATA"),
                    parse_int(row.get("reported_precincts")),
                    parse_int(row.get("total_precincts")),
                    parse_int(row.get("voters_total")),
                    parse_int(row.get("valid_votes")),
                    parse_int(row.get("invalid_votes")),
                    row.get("source_timestamp"),
                    row.get("payload_hash"),
                    row.get("error_message"),
                )
                for row in kommone_rows
                if canonical_ags(row.get("ags"))
            ],
        )

    party_rows = read_csv_rows_from_file(LATEST_DIR / "kommone_party_results.csv", delimiter=",")
    existing_kommone_party = conn.execute(
        "SELECT 1 FROM kommone_party_results WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if party_rows and existing_kommone_party is None:
        conn.executemany(
            """
            INSERT INTO kommone_party_results (poll_id, ags, vote_type, party, votes, percent)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    canonical_ags(row.get("ags")),
                    str(row.get("vote_type") or ""),
                    str(row.get("party") or ""),
                    parse_int(row.get("votes")),
                    parse_float_value(row.get("percent")),
                )
                for row in party_rows
                if canonical_ags(row.get("ags")) and str(row.get("party") or "")
            ],
        )

    statla_rows = read_csv_rows_from_file(LATEST_DIR / "statla_snapshots.csv", delimiter=",")
    existing_statla_rows = conn.execute(
        "SELECT 1 FROM statla_snapshots WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if statla_rows and existing_statla_rows is None:
        conn.executemany(
            """
            INSERT INTO statla_snapshots (
              poll_id, row_key, ags, municipality_name, gebietsart, gebietsnummer, reported_precincts,
              total_precincts, voters_total, valid_votes_erst, valid_votes_zweit, payload_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    str(row.get("row_key") or ""),
                    canonical_ags(row.get("ags")),
                    canonical_municipality_name(row.get("municipality_name")),
                    str(row.get("gebietsart") or ""),
                    str(row.get("gebietsnummer") or ""),
                    parse_int(row.get("reported_precincts")),
                    parse_int(row.get("total_precincts")),
                    parse_int(row.get("voters_total")),
                    parse_int(row.get("valid_votes_erst")),
                    parse_int(row.get("valid_votes_zweit")),
                    str(row.get("payload_hash") or ""),
                )
                for row in statla_rows
                if str(row.get("row_key") or "")
            ],
        )

    statla_party_rows = read_csv_rows_from_file(LATEST_DIR / "statla_party_results.csv", delimiter=",")
    existing_statla_party = conn.execute(
        "SELECT 1 FROM statla_party_results WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if statla_party_rows and existing_statla_party is None:
        conn.executemany(
            """
            INSERT INTO statla_party_results (poll_id, row_key, vote_type, party_key, party_name, votes)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    str(row.get("row_key") or ""),
                    str(row.get("vote_type") or ""),
                    str(row.get("party_key") or ""),
                    str(row.get("party_name") or ""),
                    parse_int(row.get("votes")),
                )
                for row in statla_party_rows
                if str(row.get("row_key") or "") and str(row.get("party_key") or "")
            ],
        )

    diff_rows = read_csv_rows_from_file(REPORT_DIR / "latest_source_diff.csv", delimiter=",")
    existing_diff_rows = conn.execute(
        "SELECT 1 FROM source_diffs WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if diff_rows and existing_diff_rows is None:
        conn.executemany(
            """
            INSERT INTO source_diffs (
              poll_id, ags, municipality_name, metric, kommone_value, statla_value, delta
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    canonical_ags(row.get("ags")),
                    canonical_municipality_name(row.get("municipality_name")),
                    str(row.get("metric") or ""),
                    parse_float_value(row.get("kommone_value")),
                    parse_float_value(row.get("statla_value")),
                    parse_float_value(row.get("delta")),
                )
                for row in diff_rows
                if canonical_ags(row.get("ags")) and str(row.get("metric") or "")
            ],
        )

    event_rows = read_csv_rows_from_file(REPORT_DIR / "latest_events.csv", delimiter=",")
    existing_event_rows = conn.execute(
        "SELECT 1 FROM events WHERE poll_id = ? LIMIT 1",
        (poll_id,),
    ).fetchone()
    if event_rows and existing_event_rows is None:
        conn.executemany(
            """
            INSERT INTO events (
              poll_id, event_time_utc, source, ags, municipality_name, event_type, details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    poll_id,
                    str(row.get("event_time_utc") or polled_at_utc),
                    str(row.get("source") or ""),
                    canonical_ags(row.get("ags")),
                    canonical_municipality_name(row.get("municipality_name")),
                    str(row.get("event_type") or ""),
                    str(row.get("details_json") or "{}"),
                )
                for row in event_rows
                if str(row.get("source") or "") and str(row.get("event_type") or "")
            ],
        )

    conn.commit()


def canonical_ags(raw_ags: Any) -> str:
    text = str(raw_ags or "").strip()
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return digits.zfill(8) if digits else ""


def canonical_municipality_name(raw_name: Any) -> str:
    text = str(raw_name or "").strip()
    text = text.replace("_", " ").replace("- ", "-")
    return re.sub(r"\s+", " ", text)


def build_municipality_master(config: Config, timeout_seconds: int) -> List[Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}

    legacy_path = resolve_repo_path(config.legacy_city_source_csv)
    if legacy_path.exists():
        try:
            text = decode_bytes(legacy_path.read_bytes())
            for row in csv_rows_from_text(text, delimiter=","):
                ags = canonical_ags(row.get("AGS") or row.get("ags") or "")
                name = canonical_municipality_name(row.get("Gemeindename") or row.get("municipality_name") or "")
                if ags and name:
                    merged[ags] = {
                        "ags": ags,
                        "municipality_name": name,
                        "source": "legacy-2025",
                    }
        except Exception:  # pylint: disable=broad-except
            pass

    dummy_result = http_get(config.statla_dummy_csv_url, timeout_seconds)
    if dummy_result.status_code == 200 and dummy_result.content:
        dummy_text = decode_bytes(dummy_result.content)
        for row in csv_rows_from_text(dummy_text, delimiter=";"):
            ags = canonical_ags(row.get("AGS", ""))
            name = canonical_municipality_name(row.get("Gemeindename", ""))
            gebietsart = str(row.get("Gebietsart", "")).strip().upper()
            bezirksnummer = str(row.get("Bezirksnummer", "")).strip()
            if not ags or not name:
                continue
            if bezirksnummer:
                continue
            if gebietsart in STATLA_EXCLUDED_GEBIETSART:
                continue
            merged[ags] = {
                "ags": ags,
                "municipality_name": name,
                "source": "statla-dummy",
            }

    municipalities = sorted(merged.values(), key=lambda item: item["ags"])
    output_path = META_DIR / "municipalities.csv"
    write_csv(
        output_path,
        ["ags", "municipality_name", "source"],
        municipalities,
    )
    return municipalities


def store_municipalities(conn: sqlite3.Connection, municipalities: Iterable[Dict[str, str]]) -> None:
    updated_at = now_utc().isoformat()
    rows = [
        (m["ags"], m["municipality_name"], m["source"], updated_at)
        for m in municipalities
        if m.get("ags") and m.get("municipality_name")
    ]
    conn.executemany(
        """
        INSERT INTO municipalities (ags, municipality_name, source, updated_at_utc)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ags) DO UPDATE SET
          municipality_name = excluded.municipality_name,
          source = excluded.source,
          updated_at_utc = excluded.updated_at_utc
        """,
        rows,
    )
    conn.commit()


def parse_kommone_progress(lines: List[str]) -> Tuple[Optional[int], Optional[int]]:
    for line in lines:
        match = re.search(r"(\d+)\s+von\s+(\d+)\s+Ergebnissen", line)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None


def normalize_party_label(label: str) -> str:
    if "," in label:
        return label.split(",")[-1].strip()
    return label.strip()


def parse_kommone_info_rows(info_rows: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    out = {
        "voters_total": None,
        "valid_votes": None,
        "invalid_votes": None,
    }
    for row in info_rows:
        label = normalize_text(str(((row or {}).get("label") or {}).get("labelKurz", "")))
        value = parse_int((row or {}).get("zahl"))
        if not label:
            continue
        if "wahler" in label or "wahlerinnen" in label or "waehler" in label:
            out["voters_total"] = value
        elif "ungultig" in label and "stimmen" in label:
            out["invalid_votes"] = value
        elif "gultig" in label and "stimmen" in label:
            out["valid_votes"] = value
    return out


def choose_preferred_vote_type(vote_types: List[str]) -> Optional[str]:
    if not vote_types:
        return None
    ranked = sorted(
        vote_types,
        key=lambda v: (
            0 if "zweit" in normalize_text(v) else 1 if "erst" in normalize_text(v) else 2,
            v,
        ),
    )
    return ranked[0]


def html_to_text(fragment: str) -> str:
    text = KOMMONE_TAG_RE.sub("", fragment)
    text = html.unescape(text).replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def make_fetch_record(source: str, result: HttpResult) -> Dict[str, Any]:
    return {
        "source": source,
        "url": result.url,
        "status_code": result.status_code,
        "content_hash": sha256_bytes(result.content) if result.content else None,
        "byte_count": len(result.content),
        "error_message": result.error_message,
    }


def kommone_regional_prefix(ags: str) -> str:
    ags = canonical_ags(ags)
    if len(ags) >= 3 and ags[2] in {"1", "2", "3", "4"}:
        return f"2{ags[2]}"
    return "21"


def kommone_county_root_ags(ags: str) -> str:
    ags = canonical_ags(ags)
    if len(ags) != 8:
        return ""
    return f"{ags[:5]}000"


def extract_kommone_result_links(page_html: str) -> List[str]:
    seen: set[str] = set()
    links: List[str] = []
    for href in KOMMONE_RESULT_LINK_RE.findall(page_html):
        if href not in seen:
            seen.add(href)
            links.append(href)
    return links


def discover_kommone_municipality_urls(
    config: Config,
    municipalities: List[Dict[str, str]],
    timeout_seconds: int,
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    county_roots = sorted(
        {
            kommone_county_root_ags(city.get("ags", ""))
            for city in municipalities
            if kommone_county_root_ags(city.get("ags", ""))
        }
    )
    queue: deque[str] = deque()
    queued: set[str] = set()
    visited: set[str] = set()
    municipality_urls: Dict[str, str] = {}
    fetches: List[Dict[str, Any]] = []

    for county_root in county_roots:
        regional_prefix = kommone_regional_prefix(county_root)
        county_id = str(int(county_root))
        for suffix in KOMMONE_HTML_PATH_SUFFIXES:
            url = (
                f"https://wahlergebnisse.komm.one/{regional_prefix}/produktion/"
                f"{county_id}/0/{config.kommone_wahltermin}/{suffix}/index.html"
            )
            if url not in queued:
                queue.append(url)
                queued.add(url)

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        result = http_get(url, timeout_seconds)
        fetches.append(make_fetch_record("kommone-discovery", result))
        if result.status_code != 200 or not result.content:
            continue

        page_html = decode_bytes(result.content)
        if "Landtagswahl Baden-W" not in page_html:
            continue

        base_url = url.rsplit("/", 1)[0]
        for link in extract_kommone_result_links(page_html):
            match = KOMMONE_GEMEINDE_LINK_RE.search(link)
            if match:
                municipality_urls.setdefault(match.group(1), f"{base_url}/{link}")
                continue
            if "stimmbezirk" in link.lower() or "briefwahlbezirk" in link.lower():
                continue
            child_url = f"{base_url}/{link}"
            if child_url not in queued and child_url not in visited:
                queue.append(child_url)
                queued.add(child_url)

    return municipality_urls, fetches


def extract_kommone_table_sections(page_html: str) -> Dict[str, str]:
    match = KOMMONE_TABLE_RE.search(page_html)
    if not match:
        return {}
    sections: Dict[str, str] = {}
    for section_name, section_html in KOMMONE_SECTION_RE.findall(match.group(1)):
        sections[section_name.lower()] = section_html
    return sections


def parse_kommone_footer_values(section_html: str) -> Dict[str, Optional[int]]:
    info = {
        "voters_total": None,
        "valid_votes": None,
        "invalid_votes": None,
    }
    for row_html in KOMMONE_ROW_RE.findall(section_html):
        cells = [html_to_text(cell) for cell in KOMMONE_CELL_RE.findall(row_html)]
        if len(cells) < 6:
            continue
        label = normalize_text(cells[0])
        first_value = parse_int(cells[2])
        second_value = parse_int(cells[5]) if len(cells) > 5 else None
        values = [value for value in [first_value, second_value] if isinstance(value, int)]
        best_value = max(values) if values else None
        if "wahler" in label or "waehler" in label:
            info["voters_total"] = best_value
        elif "ungultig" in label and "stimmen" in label:
            info["invalid_votes"] = best_value
        elif "gultig" in label and "stimmen" in label:
            info["valid_votes"] = best_value
    return info


def parse_kommone_party_rows_from_html(
    ags: str,
    municipality_name: str,
    section_html: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row_html in KOMMONE_ROW_RE.findall(section_html):
        cells = [html_to_text(cell) for cell in KOMMONE_CELL_RE.findall(row_html)]
        if len(cells) < 8:
            continue
        party = canonical_party_name(cells[0])
        if not party:
            continue
        rows.append(
            {
                "ags": ags,
                "municipality_name": municipality_name,
                "vote_type": "Erststimmen",
                "party": party,
                "votes": parse_int(cells[2]),
                "percent": parse_float_percent(cells[3]),
            }
        )
        rows.append(
            {
                "ags": ags,
                "municipality_name": municipality_name,
                "vote_type": "Zweitstimmen",
                "party": party,
                "votes": parse_int(cells[5]),
                "percent": parse_float_percent(cells[6]),
            }
        )
    return rows


def extract_kommone_status_text(page_html: str) -> Optional[str]:
    stand_matches = re.findall(r'<p[^>]*class="stand"[^>]*>(.*?)</p>', page_html, flags=re.IGNORECASE | re.DOTALL)
    cleaned = [html_to_text(item) for item in stand_matches if html_to_text(item)]
    if len(cleaned) >= 2:
        return cleaned[1]
    return cleaned[-1] if cleaned else None


def fetch_one_kommone_html_page(
    ags: str,
    municipality_name: str,
    municipality_url: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    result = http_get(municipality_url, timeout_seconds)
    fetches = [make_fetch_record("kommone", result)]

    if result.status_code != 200 or not result.content:
        return {
            "snapshot": {
                "ags": ags,
                "municipality_name": municipality_name,
                "status": "NO_DATA",
                "reported_precincts": None,
                "total_precincts": None,
                "voters_total": None,
                "valid_votes": None,
                "invalid_votes": None,
                "source_timestamp": None,
                "payload_hash": None,
                "error_message": result.error_message or f"HTTP {result.status_code}",
            },
            "party_rows": [],
            "fetches": fetches,
        }

    page_html = decode_bytes(result.content)
    payload_hash = sha256_bytes(result.content)
    status_text = extract_kommone_status_text(page_html)
    no_result_yet = "noch kein ergebnis eingegangen" in normalize_text(page_html) or (
        status_text is not None and normalize_text(status_text) == "kein eingang"
    )

    sections = extract_kommone_table_sections(page_html)
    footer_values = parse_kommone_footer_values(sections.get("tfoot", ""))
    party_rows = [] if no_result_yet else parse_kommone_party_rows_from_html(ags, municipality_name, sections.get("tbody", ""))

    snapshot = {
        "ags": ags,
        "municipality_name": municipality_name,
        "status": "NO_DATA" if no_result_yet else ("HAS_DATA" if party_rows else "NO_DATA"),
        "reported_precincts": None,
        "total_precincts": None,
        "voters_total": footer_values.get("voters_total"),
        "valid_votes": footer_values.get("valid_votes"),
        "invalid_votes": footer_values.get("invalid_votes"),
        "source_timestamp": None if no_result_yet else status_text,
        "payload_hash": payload_hash,
        "error_message": None if sections else "komm.one HTML result table missing",
    }
    return {"snapshot": snapshot, "party_rows": party_rows, "fetches": fetches}


def fetch_kommone_all(
    config: Config,
    municipalities: List[Dict[str, str]],
    timeout_seconds: int,
    max_workers: int,
    limit_ags: Optional[int] = None,
) -> Dict[str, Any]:
    selected = municipalities[: limit_ags or len(municipalities)]
    snapshots: List[Dict[str, Any]] = []
    party_rows: List[Dict[str, Any]] = []
    municipality_urls, discovery_fetches = discover_kommone_municipality_urls(config, selected, timeout_seconds)
    fetches: List[Dict[str, Any]] = list(discovery_fetches)

    missing_cities = [city for city in selected if city["ags"] not in municipality_urls]
    for city in missing_cities:
        snapshots.append(
            {
                "ags": city["ags"],
                "municipality_name": city["municipality_name"],
                "status": "NO_DATA",
                "reported_precincts": None,
                "total_precincts": None,
                "voters_total": None,
                "valid_votes": None,
                "invalid_votes": None,
                "source_timestamp": None,
                "payload_hash": None,
                "error_message": "No komm.one municipality page discovered for AGS",
            }
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_one_kommone_html_page,
                city["ags"],
                city["municipality_name"],
                municipality_urls[city["ags"]],
                timeout_seconds,
            ): city
            for city in selected
            if city["ags"] in municipality_urls
        }
        for future in as_completed(futures):
            result = future.result()
            snapshots.append(result["snapshot"])
            party_rows.extend(result["party_rows"])
            fetches.extend(result["fetches"])

    snapshots.sort(key=lambda row: row["ags"])
    party_rows.sort(key=lambda row: (row["party"], row["ags"], row["vote_type"]))
    return {"snapshots": snapshots, "party_rows": party_rows, "fetches": fetches}


def extract_statla_parties(row: Dict[str, str]) -> List[Dict[str, Any]]:
    parties: List[Dict[str, Any]] = []
    for key, raw_value in row.items():
        value = parse_int(raw_value)
        if value is None:
            continue

        if re.fullmatch(r"D\d+", key):
            vote_type = "Erststimmen"
            parties.append(
                {
                    "vote_type": vote_type,
                    "party_key": key,
                    "party_name": canonical_party_name(statla_party_name_from_code(vote_type, key), vote_type),
                    "votes": value,
                }
            )
            continue
        if re.fullmatch(r"F\d+", key):
            vote_type = "Zweitstimmen"
            parties.append(
                {
                    "vote_type": vote_type,
                    "party_key": key,
                    "party_name": canonical_party_name(statla_party_name_from_code(vote_type, key), vote_type),
                    "votes": value,
                }
            )
            continue

        if key.endswith("Erststimmen") and "gueltige" not in normalize_text(key) and "ungueltige" not in normalize_text(key):
            vote_type = "Erststimmen"
            parties.append(
                {
                    "vote_type": vote_type,
                    "party_key": key,
                    "party_name": canonical_party_name(key.replace(" Erststimmen", "").strip(), vote_type),
                    "votes": value,
                }
            )
        if key.endswith("Zweitstimmen") and "gueltige" not in normalize_text(key) and "ungueltige" not in normalize_text(key):
            vote_type = "Zweitstimmen"
            parties.append(
                {
                    "vote_type": vote_type,
                    "party_key": key,
                    "party_name": canonical_party_name(key.replace(" Zweitstimmen", "").strip(), vote_type),
                    "votes": value,
                }
            )
    return parties


def is_statla_municipality_row(row: Dict[str, str]) -> bool:
    ags = canonical_ags(row.get("AGS"))
    if not ags:
        return False

    name = canonical_municipality_name(row.get("Gemeindename"))
    if not name:
        return False

    bezirksnummer = str(row.get("Bezirksnummer", "")).strip()
    if bezirksnummer:
        return False

    gebietsart = str(row.get("Gebietsart", "")).strip().upper()
    if gebietsart in STATLA_EXCLUDED_GEBIETSART:
        return False
    return True


def parse_statla_csv_rows(csv_text: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    snapshots: List[Dict[str, Any]] = []
    party_rows: List[Dict[str, Any]] = []
    for idx, raw_row in enumerate(csv_rows_from_text(csv_text, delimiter=";")):
        row = dict(raw_row)
        canonical_row_json = json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
        row_hash = sha256_bytes(canonical_row_json)

        row_key = (
            f"{idx:06d}:"
            f"{str(row.get('Gebietsnummer', '')).strip() or '-'}:"
            f"{str(row.get('Bezirksnummer', '')).strip() or '-'}:"
            f"{canonical_ags(row.get('AGS')) or '-'}:"
            f"{str(row.get('Gebietsart', '')).strip() or '-'}"
        )
        snapshot = {
            "row_key": row_key,
            "ags": canonical_ags(row.get("AGS")),
            "municipality_name": canonical_municipality_name(row.get("Gemeindename")),
            "gebietsart": str(row.get("Gebietsart", "")).strip(),
            "gebietsnummer": str(row.get("Gebietsnummer", "")).strip(),
            "reported_precincts": parse_int(row.get("gemeldete Wahlbezirke")),
            "total_precincts": parse_int(row.get("Anzahl Wahlbezirke")),
            "voters_total": parse_int(row.get("Waehler gesamt (B)")),
            "valid_votes_erst": parse_int(row.get("Erststimmen gueltige (D)")),
            "valid_votes_zweit": parse_int(row.get("Zweitstimmen gueltige (F)")),
            "payload_hash": row_hash,
            "is_municipality_summary": is_statla_municipality_row(row),
        }
        snapshots.append(snapshot)
        for party in extract_statla_parties(row):
            party_rows.append({"row_key": row_key, **party})

    return snapshots, party_rows


def fetch_statla(config: Config, timeout_seconds: int, force_dummy: bool = False) -> Dict[str, Any]:
    live_result = http_get(config.statla_live_csv_url, timeout_seconds)
    selected_result = live_result
    selected_mode = "LIVE"
    selected_url = config.statla_live_csv_url
    fallback_used = False

    if force_dummy or live_result.status_code != 200 or not live_result.content:
        selected_result = http_get(config.statla_dummy_csv_url, timeout_seconds)
        selected_mode = "DUMMY"
        selected_url = config.statla_dummy_csv_url
        fallback_used = True

    fetches = [
        {
            "source": "statla",
            "url": config.statla_live_csv_url,
            "status_code": live_result.status_code,
            "content_hash": sha256_bytes(live_result.content) if live_result.content else None,
            "byte_count": len(live_result.content),
            "error_message": live_result.error_message,
        }
    ]
    if fallback_used:
        fetches.append(
            {
                "source": "statla",
                "url": config.statla_dummy_csv_url,
                "status_code": selected_result.status_code,
                "content_hash": sha256_bytes(selected_result.content) if selected_result.content else None,
                "byte_count": len(selected_result.content),
                "error_message": selected_result.error_message,
            }
        )

    if (selected_result.status_code != 200 or not selected_result.content) and force_dummy:
        local_dummy = LOCAL_DUMMY_STATLA_PATH
        if local_dummy.exists():
            content = local_dummy.read_bytes()
            selected_result = HttpResult(
                url=str(local_dummy),
                status_code=200,
                content=content,
                error_message=None,
            )
            selected_mode = "DUMMY"
            selected_url = str(local_dummy)

    if selected_result.status_code != 200 or not selected_result.content:
        return {
            "mode": "UNAVAILABLE",
            "url": selected_url,
            "status_code": selected_result.status_code,
            "content_hash": None,
            "raw_csv": "",
            "snapshots": [],
            "party_rows": [],
            "fetches": fetches,
            "error_message": selected_result.error_message or "No CSV available",
        }

    csv_text = decode_bytes(selected_result.content)
    snapshots, party_rows = parse_statla_csv_rows(csv_text)
    return {
        "mode": selected_mode,
        "url": selected_url,
        "status_code": selected_result.status_code,
        "content_hash": sha256_bytes(selected_result.content),
        "raw_csv": csv_text,
        "snapshots": snapshots,
        "party_rows": party_rows,
        "fetches": fetches,
        "error_message": None,
    }


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_list:
            writer.writerow({name: row.get(name) for name in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def previous_kommone_snapshot(conn: sqlite3.Connection, ags: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT status, payload_hash, reported_precincts, total_precincts
        FROM kommone_snapshots
        WHERE ags = ?
        ORDER BY poll_id DESC
        LIMIT 1
        """,
        (ags,),
    ).fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "payload_hash": row[1],
        "reported_precincts": row[2],
        "total_precincts": row[3],
    }


def insert_event(
    conn: sqlite3.Connection,
    poll_id: int,
    source: str,
    ags: Optional[str],
    municipality_name: Optional[str],
    event_type: str,
    details: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO events (
          poll_id, event_time_utc, source, ags, municipality_name, event_type, details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            poll_id,
            now_utc().isoformat(),
            source,
            ags,
            municipality_name,
            event_type,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
        ),
    )


def store_source_fetches(conn: sqlite3.Connection, poll_id: int, fetches: Iterable[Dict[str, Any]]) -> None:
    rows = [
        (
            poll_id,
            item["source"],
            item["url"],
            item["status_code"],
            item.get("content_hash"),
            int(item.get("byte_count") or 0),
            item.get("error_message"),
            now_utc().isoformat(),
        )
        for item in fetches
    ]
    conn.executemany(
        """
        INSERT INTO source_fetches (
          poll_id, source, url, status_code, content_hash, byte_count, error_message, fetched_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def store_kommone(
    conn: sqlite3.Connection,
    poll_id: int,
    snapshots: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
) -> None:
    for snapshot in snapshots:
        prev = previous_kommone_snapshot(conn, snapshot["ags"])
        conn.execute(
            """
            INSERT INTO kommone_snapshots (
              poll_id, ags, municipality_name, status, reported_precincts, total_precincts,
              voters_total, valid_votes, invalid_votes, source_timestamp, payload_hash, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                poll_id,
                snapshot["ags"],
                snapshot["municipality_name"],
                snapshot["status"],
                snapshot.get("reported_precincts"),
                snapshot.get("total_precincts"),
                snapshot.get("voters_total"),
                snapshot.get("valid_votes"),
                snapshot.get("invalid_votes"),
                snapshot.get("source_timestamp"),
                snapshot.get("payload_hash"),
                snapshot.get("error_message"),
            ),
        )

        current_status = snapshot.get("status")
        if prev is None and current_status == "HAS_DATA":
            insert_event(
                conn,
                poll_id,
                "kommone",
                snapshot["ags"],
                snapshot["municipality_name"],
                "RESULT_ADDED",
                {"reported_precincts": snapshot.get("reported_precincts"), "total_precincts": snapshot.get("total_precincts")},
            )
        elif prev:
            if prev.get("status") != "HAS_DATA" and current_status == "HAS_DATA":
                insert_event(
                    conn,
                    poll_id,
                    "kommone",
                    snapshot["ags"],
                    snapshot["municipality_name"],
                    "RESULT_ADDED",
                    {"reported_precincts": snapshot.get("reported_precincts"), "total_precincts": snapshot.get("total_precincts")},
                )
            if prev.get("status") == "HAS_DATA" and current_status != "HAS_DATA":
                insert_event(
                    conn,
                    poll_id,
                    "kommone",
                    snapshot["ags"],
                    snapshot["municipality_name"],
                    "RESULT_REMOVED",
                    {"previous_status": prev.get("status"), "current_status": current_status},
                )
            if (
                prev.get("status") == "HAS_DATA"
                and current_status == "HAS_DATA"
                and prev.get("payload_hash")
                and snapshot.get("payload_hash")
                and prev.get("payload_hash") != snapshot.get("payload_hash")
            ):
                insert_event(
                    conn,
                    poll_id,
                    "kommone",
                    snapshot["ags"],
                    snapshot["municipality_name"],
                    "RESULT_UPDATED",
                    {
                        "previous_hash": prev.get("payload_hash"),
                        "current_hash": snapshot.get("payload_hash"),
                    },
                )
            prev_rep = prev.get("reported_precincts")
            current_rep = snapshot.get("reported_precincts")
            if isinstance(prev_rep, int) and isinstance(current_rep, int):
                if current_rep > prev_rep:
                    insert_event(
                        conn,
                        poll_id,
                        "kommone",
                        snapshot["ags"],
                        snapshot["municipality_name"],
                        "PROGRESS_ADVANCED",
                        {"before": prev_rep, "after": current_rep},
                    )
                elif current_rep < prev_rep:
                    insert_event(
                        conn,
                        poll_id,
                        "kommone",
                        snapshot["ags"],
                        snapshot["municipality_name"],
                        "PROGRESS_REVERTED",
                        {"before": prev_rep, "after": current_rep},
                    )

    conn.executemany(
        """
        INSERT INTO kommone_party_results (poll_id, ags, vote_type, party, votes, percent)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                poll_id,
                row["ags"],
                row["vote_type"],
                row["party"],
                row.get("votes"),
                row.get("percent"),
            )
            for row in party_rows
        ],
    )
    conn.commit()


def previous_statla_hash(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        """
        SELECT content_hash
        FROM source_fetches
        WHERE source = 'statla'
          AND content_hash IS NOT NULL
        ORDER BY poll_id DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


def store_statla(
    conn: sqlite3.Connection,
    poll_id: int,
    snapshots: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
    current_file_hash: Optional[str],
) -> None:
    previous_hash = previous_statla_hash(conn)
    if previous_hash and current_file_hash and previous_hash != current_file_hash:
        insert_event(
            conn,
            poll_id,
            "statla",
            None,
            None,
            "FILE_UPDATED",
            {"previous_hash": previous_hash, "current_hash": current_file_hash},
        )

    conn.executemany(
        """
        INSERT INTO statla_snapshots (
          poll_id, row_key, ags, municipality_name, gebietsart, gebietsnummer, reported_precincts,
          total_precincts, voters_total, valid_votes_erst, valid_votes_zweit, payload_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                poll_id,
                row["row_key"],
                row.get("ags"),
                row.get("municipality_name"),
                row.get("gebietsart"),
                row.get("gebietsnummer"),
                row.get("reported_precincts"),
                row.get("total_precincts"),
                row.get("voters_total"),
                row.get("valid_votes_erst"),
                row.get("valid_votes_zweit"),
                row["payload_hash"],
            )
            for row in snapshots
        ],
    )
    conn.executemany(
        """
        INSERT INTO statla_party_results (poll_id, row_key, vote_type, party_key, party_name, votes)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                poll_id,
                row["row_key"],
                row["vote_type"],
                row["party_key"],
                row["party_name"],
                row.get("votes"),
            )
            for row in party_rows
        ],
    )
    conn.commit()


def latest_statla_municipality_rows(snapshots: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_ags: Dict[str, Dict[str, Any]] = {}
    for row in snapshots:
        if not row.get("is_municipality_summary"):
            continue
        ags = row.get("ags")
        if not ags:
            continue
        current_best = by_ags.get(ags)
        # Prefer rows with the highest number of total precincts as summary lines.
        current_total = row.get("total_precincts") or -1
        previous_total = (current_best or {}).get("total_precincts") or -1
        if current_best is None or current_total >= previous_total:
            by_ags[ags] = row
    return by_ags


def compute_source_diffs(
    poll_id: int,
    kommone_snapshots: List[Dict[str, Any]],
    statla_snapshots: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    kommone_by_ags = {row["ags"]: row for row in kommone_snapshots}
    statla_by_ags = latest_statla_municipality_rows(statla_snapshots)
    all_ags = sorted(set(kommone_by_ags) | set(statla_by_ags))

    diff_rows: List[Dict[str, Any]] = []
    for ags in all_ags:
        k = kommone_by_ags.get(ags, {})
        s = statla_by_ags.get(ags, {})
        municipality_name = k.get("municipality_name") or s.get("municipality_name") or ""

        metrics = [
            ("reported_precincts", k.get("reported_precincts"), s.get("reported_precincts")),
            ("total_precincts", k.get("total_precincts"), s.get("total_precincts")),
            ("voters_total", k.get("voters_total"), s.get("voters_total")),
            ("valid_votes", k.get("valid_votes"), s.get("valid_votes_zweit") or s.get("valid_votes_erst")),
        ]
        for metric, k_value, s_value in metrics:
            delta = None
            if isinstance(k_value, (int, float)) and isinstance(s_value, (int, float)):
                delta = float(k_value) - float(s_value)
            diff_rows.append(
                {
                    "poll_id": poll_id,
                    "ags": ags,
                    "municipality_name": municipality_name,
                    "metric": metric,
                    "kommone_value": k_value,
                    "statla_value": s_value,
                    "delta": delta,
                }
            )
    return diff_rows


def store_source_diffs(conn: sqlite3.Connection, diff_rows: List[Dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO source_diffs (
          poll_id, ags, municipality_name, metric, kommone_value, statla_value, delta
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["poll_id"],
                row["ags"],
                row["municipality_name"],
                row["metric"],
                row.get("kommone_value"),
                row.get("statla_value"),
                row.get("delta"),
            )
            for row in diff_rows
        ],
    )
    conn.commit()


def read_recent_events(conn: sqlite3.Connection, poll_id: int, limit: int = 2000) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT event_time_utc, source, ags, municipality_name, event_type, details_json
        FROM events
        WHERE poll_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (poll_id, limit),
    ).fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "event_time_utc": row[0],
                "source": row[1],
                "ags": row[2],
                "municipality_name": row[3],
                "event_type": row[4],
                "details_json": row[5],
            }
        )
    return out


def municipality_status(snapshot: Dict[str, Any]) -> str:
    if snapshot.get("status") != "HAS_DATA":
        return "no_data"
    reported = snapshot.get("reported_precincts")
    total = snapshot.get("total_precincts")
    if isinstance(reported, int) and isinstance(total, int):
        if total == 0:
            return "pending"
        if reported < total:
            return "pending"
        return "complete"
    return "pending"


def party_dashboard_rows(
    snapshots: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    status_by_ags = {row["ags"]: municipality_status(row) for row in snapshots}
    rows_by_ags: Dict[str, List[Dict[str, Any]]] = {}
    for row in party_rows:
        rows_by_ags.setdefault(row["ags"], []).append(row)

    selected_rows: List[Dict[str, Any]] = []
    for ags, rows in rows_by_ags.items():
        vote_types = sorted({row["vote_type"] for row in rows})
        preferred = choose_preferred_vote_type(vote_types)
        if preferred is None:
            continue
        for row in rows:
            if row["vote_type"] == preferred:
                selected_rows.append(row)

    totals: Dict[str, int] = {}
    detail_rows_by_party: Dict[str, List[Dict[str, Any]]] = {}
    for row in selected_rows:
        party = row["party"]
        votes = row.get("votes")
        if isinstance(votes, int):
            totals[party] = totals.get(party, 0) + votes
        detail_rows_by_party.setdefault(party, []).append(
            {
                "ags": row["ags"],
                "municipality_name": row["municipality_name"],
                "votes": votes,
                "percent": row.get("percent"),
                "status": status_by_ags.get(row["ags"], "no_data"),
            }
        )

    grand_total = sum(totals.values()) or 0
    summary_rows = []
    for party, votes in sorted(totals.items(), key=lambda item: item[1], reverse=True):
        share = (votes / grand_total * 100.0) if grand_total else 0.0
        summary_rows.append({"party": party, "votes": votes, "share_percent": share})

    for party, rows in detail_rows_by_party.items():
        rows.sort(key=lambda item: (item["votes"] is None, -(item["votes"] or 0), item["municipality_name"]))

    return summary_rows, detail_rows_by_party


def canonical_vote_type(label: str) -> str:
    normalized = normalize_text(label)
    if "zweit" in normalized:
        return "Zweitstimmen"
    if "erst" in normalized:
        return "Erststimmen"
    return label.strip() or "Unbekannt"


def source_party_totals(
    party_rows: List[Dict[str, Any]],
    party_field: str,
) -> Dict[str, Dict[str, int]]:
    totals_by_type: Dict[str, Dict[str, int]] = {}
    for row in party_rows:
        votes = row.get("votes")
        vote_type = canonical_vote_type(str(row.get("vote_type") or ""))
        party = canonical_party_name(str(row.get(party_field) or ""), vote_type=vote_type)
        if not party or not isinstance(votes, int):
            continue
        bucket = totals_by_type.setdefault(vote_type, {})
        bucket[party] = bucket.get(party, 0) + votes
    return totals_by_type


def fixed_party_order_by_vote_type() -> Dict[str, List[str]]:
    first_codes: List[str] = []
    second_codes: List[str] = []
    dummy_path = LOCAL_DUMMY_STATLA_PATH
    if dummy_path.exists():
        try:
            header_line = decode_bytes(dummy_path.read_bytes()).splitlines()[0]
            header = next(csv.reader([header_line], delimiter=";"))
            first_codes = sorted(
                [name for name in header if re.fullmatch(r"D\d+", name)],
                key=lambda name: int(name[1:]),
            )
            second_codes = sorted(
                [name for name in header if re.fullmatch(r"F\d+", name)],
                key=lambda name: int(name[1:]),
            )
        except Exception:  # pylint: disable=broad-except
            pass

    if not first_codes:
        first_codes = [code for code, _name in STATLA_PARTY_CODEBOOK["Erststimmen"]]
    if not second_codes:
        second_codes = [code for code, _name in STATLA_PARTY_CODEBOOK["Zweitstimmen"]]

    first: List[str] = []
    first_seen: set[str] = set()
    for code in first_codes:
        name = canonical_party_name(statla_party_name_from_code("Erststimmen", code), "Erststimmen")
        if name and name not in first_seen:
            first_seen.add(name)
            first.append(name)

    second: List[str] = []
    second_seen: set[str] = set()
    for code in second_codes:
        name = canonical_party_name(statla_party_name_from_code("Zweitstimmen", code), "Zweitstimmen")
        if name and name not in second_seen:
            second_seen.add(name)
            second.append(name)

    return {
        "Erststimmen": first,
        "Zweitstimmen": second,
    }


def party_summary_by_vote_type_sources(
    kommone_party_rows: List[Dict[str, Any]],
    statla_party_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    kommone_totals = source_party_totals(kommone_party_rows, party_field="party")
    statla_totals = source_party_totals(statla_party_rows, party_field="party_name")
    fixed_parties = fixed_party_order_by_vote_type()

    rows: List[Dict[str, Any]] = []
    ordered_vote_types = ["Erststimmen", "Zweitstimmen"]
    remaining_vote_types = sorted(
        vt for vt in set(kommone_totals.keys()) | set(statla_totals.keys()) if vt not in ordered_vote_types
    )

    for vote_type in ordered_vote_types + remaining_vote_types:
        k_party_totals = kommone_totals.get(vote_type, {})
        s_party_totals = statla_totals.get(vote_type, {})
        k_grand_total = sum(k_party_totals.values())
        s_grand_total = sum(s_party_totals.values())

        parties = list(fixed_parties.get(vote_type, []))
        extras = sorted(
            party
            for party in (set(k_party_totals.keys()) | set(s_party_totals.keys()))
            if party not in set(parties)
        )
        parties.extend(extras)
        if not parties:
            rows.append(
                {
                    "row_type": "TOTAL",
                    "vote_type": vote_type,
                    "party": "TOTAL",
                    "kommone_votes": 0,
                    "kommone_share_percent": 0.0,
                    "statla_votes": 0,
                    "statla_share_percent": 0.0,
                    "delta_votes": 0,
                    "delta_share_percent": 0.0,
                }
            )
            continue

        for party in parties:
            kommone_votes = k_party_totals.get(party, 0)
            statla_votes = s_party_totals.get(party, 0)
            kommone_share = (kommone_votes / k_grand_total * 100.0) if k_grand_total else 0.0
            statla_share = (statla_votes / s_grand_total * 100.0) if s_grand_total else 0.0
            rows.append(
                {
                    "row_type": "PARTY",
                    "vote_type": vote_type,
                    "party": party,
                    "kommone_votes": kommone_votes,
                    "kommone_share_percent": kommone_share,
                    "statla_votes": statla_votes,
                    "statla_share_percent": statla_share,
                    "delta_votes": kommone_votes - statla_votes,
                    "delta_share_percent": kommone_share - statla_share,
                }
            )
        kommone_total_share = 100.0 if k_grand_total else 0.0
        statla_total_share = 100.0 if s_grand_total else 0.0
        rows.append(
            {
                "row_type": "TOTAL",
                "vote_type": vote_type,
                "party": "TOTAL",
                "kommone_votes": k_grand_total,
                "kommone_share_percent": kommone_total_share,
                "statla_votes": s_grand_total,
                "statla_share_percent": statla_total_share,
                "delta_votes": k_grand_total - s_grand_total,
                "delta_share_percent": kommone_total_share - statla_total_share,
            }
        )
    return rows


def append_party_totals_tables(
    lines: List[str],
    vote_type_summary: List[Dict[str, Any]],
) -> None:
    lines.append("## Party Totals (First and Second Votes)")
    lines.append("")

    rows_by_vote_type: Dict[str, List[Dict[str, Any]]] = {}
    order_seen: List[str] = []
    for row in vote_type_summary:
        vote_type = str(row.get("vote_type") or "Unbekannt")
        if vote_type not in rows_by_vote_type:
            order_seen.append(vote_type)
        rows_by_vote_type.setdefault(vote_type, []).append(row)

    ordered_vote_types = [vote_type for vote_type in ["Erststimmen", "Zweitstimmen"] if vote_type in rows_by_vote_type]
    ordered_vote_types.extend(vote_type for vote_type in order_seen if vote_type not in ordered_vote_types)

    for vote_type in ordered_vote_types:
        lines.append(f"### {vote_type}")
        lines.append("")
        lines.append(
            "| Party | `komm.one` Count | `komm.one` Share | `statla` Count | `statla` Share | Delta Count (`komm.one`-`statla`) | Delta Share (`komm.one`-`statla`) |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for row in rows_by_vote_type.get(vote_type, []):
            is_total = str(row.get("row_type") or "") == "TOTAL"
            party = "**TOTAL**" if is_total else str(row.get("party") or "-")
            lines.append(
                (
                    f"| {party} | {int(row.get('kommone_votes') or 0)} | {float(row.get('kommone_share_percent') or 0.0):.2f}% | "
                    f"{int(row.get('statla_votes') or 0)} | {float(row.get('statla_share_percent') or 0.0):.2f}% | "
                    f"{int(row.get('delta_votes') or 0):+d} | {float(row.get('delta_share_percent') or 0.0):+.2f}% |"
                )
            )
        lines.append("")


def normalize_wahlkreis_nummer(value: Any) -> str:
    number = parse_int(value)
    if number is None:
        return ""
    return str(number)


def load_wahlkreis_features() -> List[Dict[str, Any]]:
    if not WAHLKREIS_GEOJSON_PATH.exists():
        return []
    try:
        payload = json.loads(WAHLKREIS_GEOJSON_PATH.read_text(encoding="utf-8"))
    except Exception:  # pylint: disable=broad-except
        return []
    return payload.get("features", []) or []


def load_wahlkreis_mapping() -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    if not WAHLKREIS_MAPPING_PATH.exists():
        return mapping

    lines: List[str] = []
    with WAHLKREIS_MAPPING_PATH.open("r", encoding="latin-1", newline="") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or line.startswith("#"):
                continue
            # Statistik BW includes a delimiter-only spacer row before the actual header.
            if stripped.replace(";", "") == "":
                continue
            lines.append(line)

    reader = csv.DictReader(lines, delimiter=";")
    for row in reader:
        wk = normalize_wahlkreis_nummer(row.get("Wahlkreisnummer"))
        if not wk:
            continue
        ags = canonical_ags(row.get("Gemeindekennziffer"))
        wk_name = canonical_municipality_name(row.get("Wahlkreisname"))
        bucket = mapping.setdefault(wk, {"wahlkreis_name": wk_name or f"Wahlkreis {wk}", "ags_set": set()})
        if wk_name:
            bucket["wahlkreis_name"] = wk_name
        if ags:
            bucket["ags_set"].add(ags)
    return mapping


def iter_exterior_rings(geometry: Dict[str, Any]) -> Iterable[List[List[float]]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geom_type == "Polygon":
        if coords:
            yield coords[0]
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            if polygon:
                yield polygon[0]


def project_point(
    lon: float,
    lat: float,
    min_lon: float,
    min_lat: float,
    scale: float,
    pad: float,
    height: float,
) -> Tuple[float, float]:
    x = pad + (lon - min_lon) * scale
    y = height - pad - (lat - min_lat) * scale
    return x, y


def statla_wahlkreis_status_map(statla_snapshots: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_wk: Dict[str, Dict[str, Any]] = {}
    for row in statla_snapshots:
        if str(row.get("gebietsart", "")).strip().upper() != "WAHLKREIS":
            continue
        wk = normalize_wahlkreis_nummer(row.get("gebietsnummer") or row.get("row_key"))
        if not wk:
            continue
        by_wk[wk] = {
            "reported_precincts": row.get("reported_precincts"),
            "total_precincts": row.get("total_precincts"),
        }
    return by_wk


def compute_wahlkreis_status_rows(
    features: List[Dict[str, Any]],
    mapping: Dict[str, Dict[str, Any]],
    kommone_snapshots: List[Dict[str, Any]],
    statla_snapshots: List[Dict[str, Any]],
    prestart: bool,
) -> List[Dict[str, Any]]:
    status_by_ags = {row["ags"]: municipality_status(row) for row in kommone_snapshots}
    statla_by_wk = statla_wahlkreis_status_map(statla_snapshots)

    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for feature in features:
        props = feature.get("properties") or {}
        wk = normalize_wahlkreis_nummer(props.get("Nummer"))
        if not wk:
            continue
        seen.add(wk)
        name = str(props.get("WK Name") or f"Wahlkreis {wk}").strip()
        ags_set = mapping.get(wk, {}).get("ags_set", set())
        total_municipalities = len(ags_set)
        complete_municipalities = 0
        pending_municipalities = 0
        no_data_municipalities = 0
        for ags in ags_set:
            status = status_by_ags.get(ags, "no_data")
            if status == "complete":
                complete_municipalities += 1
            elif status == "pending":
                pending_municipalities += 1
            else:
                no_data_municipalities += 1

        statla = statla_by_wk.get(wk, {})
        reported = statla.get("reported_precincts")
        total = statla.get("total_precincts")

        if prestart:
            status = "prestart"
        elif isinstance(total, int) and total > 0:
            if isinstance(reported, int) and reported >= total:
                status = "complete"
            else:
                status = "pending"
        elif complete_municipalities == total_municipalities and total_municipalities > 0:
            status = "complete"
        elif complete_municipalities > 0 or pending_municipalities > 0:
            status = "pending"
        else:
            status = "no_data"

        rows.append(
            {
                "wahlkreisnummer": wk,
                "wahlkreisname": mapping.get(wk, {}).get("wahlkreis_name") or name,
                "status": status,
                "reported_precincts": reported,
                "total_precincts": total,
                "municipalities_total": total_municipalities,
                "municipalities_complete": complete_municipalities,
                "municipalities_pending": pending_municipalities,
                "municipalities_no_data": no_data_municipalities,
            }
        )

    for wk, entry in mapping.items():
        if wk in seen:
            continue
        ags_set = entry.get("ags_set", set())
        rows.append(
            {
                "wahlkreisnummer": wk,
                "wahlkreisname": entry.get("wahlkreis_name") or f"Wahlkreis {wk}",
                "status": "prestart" if prestart else "no_data",
                "reported_precincts": None,
                "total_precincts": None,
                "municipalities_total": len(ags_set),
                "municipalities_complete": 0,
                "municipalities_pending": 0,
                "municipalities_no_data": len(ags_set),
            }
        )

    rows.sort(key=lambda row: int(row["wahlkreisnummer"]))
    return rows


def render_wahlkreis_svg(features: List[Dict[str, Any]], status_rows: List[Dict[str, Any]]) -> None:
    if not features:
        WAHLKREIS_STATUS_MAP_PATH.write_text(
            "<svg xmlns='http://www.w3.org/2000/svg' width='800' height='200'><text x='20' y='40'>No Wahlkreis geometry available.</text></svg>",
            encoding="utf-8",
        )
        return

    status_by_wk = {row["wahlkreisnummer"]: row["status"] for row in status_rows}
    colors = {
        "prestart": "#d1d5db",
        "no_data": "#e5e7eb",
        "pending": "#f59e0b",
        "complete": "#16a34a",
    }

    all_points: List[Tuple[float, float]] = []
    for feature in features:
        for ring in iter_exterior_rings(feature.get("geometry") or {}):
            for point in ring:
                if len(point) >= 2:
                    all_points.append((float(point[0]), float(point[1])))
    if not all_points:
        return

    min_lon = min(p[0] for p in all_points)
    max_lon = max(p[0] for p in all_points)
    min_lat = min(p[1] for p in all_points)
    max_lat = max(p[1] for p in all_points)

    width = 1000.0
    height = 1300.0
    pad = 40.0
    scale_x = (width - 2 * pad) / max(max_lon - min_lon, 1e-9)
    scale_y = (height - 2 * pad) / max(max_lat - min_lat, 1e-9)
    scale = min(scale_x, scale_y)

    path_nodes: List[str] = []
    for feature in features:
        props = feature.get("properties") or {}
        wk = normalize_wahlkreis_nummer(props.get("Nummer"))
        name = str(props.get("WK Name") or f"Wahlkreis {wk}").strip()
        status = status_by_wk.get(wk, "no_data")
        fill = colors.get(status, colors["no_data"])
        d_parts: List[str] = []
        for ring in iter_exterior_rings(feature.get("geometry") or {}):
            if len(ring) < 3:
                continue
            projected = [
                project_point(
                    float(pt[0]),
                    float(pt[1]),
                    min_lon=min_lon,
                    min_lat=min_lat,
                    scale=scale,
                    pad=pad,
                    height=height,
                )
                for pt in ring
            ]
            seg = "M " + " L ".join(f"{x:.2f} {y:.2f}" for x, y in projected) + " Z"
            d_parts.append(seg)
        if not d_parts:
            continue
        title = html.escape(f"{wk} {name} ({status})")
        path_nodes.append(
            f"<path d=\"{' '.join(d_parts)}\" fill=\"{fill}\" stroke=\"#111827\" stroke-width=\"0.6\"><title>{title}</title></path>"
        )

    status_counts = {"prestart": 0, "no_data": 0, "pending": 0, "complete": 0}
    for row in status_rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    legend_nodes: List[str] = []
    legend_items = [
        ("prestart", "Tracking not started"),
        ("no_data", "No data"),
        ("pending", "Pending"),
        ("complete", "Complete"),
    ]
    legend_y = 18
    for status, label in legend_items:
        count = status_counts.get(status, 0)
        legend_nodes.append(
            (
                f"<rect x='20' y='{legend_y}' width='14' height='14' fill='{colors[status]}' stroke='#111827' stroke-width='0.4' />"
                f"<text x='40' y='{legend_y + 12}' font-size='12' fill='#111827'>{html.escape(label)}: {count}</text>"
            )
        )
        legend_y += 20

    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {int(width)} {int(height)}'>"
        "<rect width='100%' height='100%' fill='#ffffff'/>"
        f"{''.join(path_nodes)}"
        "<g>"
        f"{''.join(legend_nodes)}"
        "</g>"
        f"<text x='20' y='1245' font-size='11' fill='#374151'>Source: Statistik BW {html.escape(WAHLKREIS_GEOJSON_PATH.name)} geometry</text>"
        "</svg>"
    )
    WAHLKREIS_STATUS_MAP_PATH.write_text(svg, encoding="utf-8")


def generate_wahlkreis_map(
    kommone_snapshots: List[Dict[str, Any]],
    statla_snapshots: List[Dict[str, Any]],
    prestart: bool,
) -> List[Dict[str, Any]]:
    features = load_wahlkreis_features()
    mapping = load_wahlkreis_mapping()
    status_rows = compute_wahlkreis_status_rows(
        features=features,
        mapping=mapping,
        kommone_snapshots=kommone_snapshots,
        statla_snapshots=statla_snapshots,
        prestart=prestart,
    )
    write_csv(
        WAHLKREIS_STATUS_CSV_PATH,
        [
            "wahlkreisnummer",
            "wahlkreisname",
            "status",
            "reported_precincts",
            "total_precincts",
            "municipalities_total",
            "municipalities_complete",
            "municipalities_pending",
            "municipalities_no_data",
        ],
        status_rows,
    )
    render_wahlkreis_svg(features, status_rows)
    return status_rows


def generate_readme(
    config: Config,
    polled_at_local: str,
    municipalities: List[Dict[str, str]],
    kommone_snapshots: List[Dict[str, Any]],
    party_rows: List[Dict[str, Any]],
    statla_party_rows: List[Dict[str, Any]],
    statla_mode: str,
    statla_url: str,
    diff_rows: List[Dict[str, Any]],
    wahlkreis_status_rows: List[Dict[str, Any]],
) -> None:
    tracking_start = tracking_start_local_dt(config)
    data_dir_rel = repo_relative_path(DATA_DIR)
    latest_dir_rel = repo_relative_path(LATEST_DIR)
    metadata_dir_rel = repo_relative_path(META_DIR)
    site_dir_rel = repo_relative_path(SITE_OUTPUT_DIR)
    status_counts = {"complete": 0, "pending": 0, "no_data": 0}
    status_by_ags: Dict[str, str] = {}
    for snapshot in kommone_snapshots:
        status = municipality_status(snapshot)
        status_counts[status] += 1
        status_by_ags[snapshot["ags"]] = status

    missing_ags = [city["ags"] for city in municipalities if city["ags"] not in status_by_ags]
    status_counts["no_data"] += len(missing_ags)

    party_summary, party_details = party_dashboard_rows(kommone_snapshots, party_rows)
    vote_type_summary = party_summary_by_vote_type_sources(
        kommone_party_rows=party_rows,
        statla_party_rows=statla_party_rows,
    )

    statla_diff_summary: Dict[str, Dict[str, float]] = {}
    for row in diff_rows:
        metric = row["metric"]
        bucket = statla_diff_summary.setdefault(metric, {"count_with_delta": 0, "abs_delta_sum": 0.0})
        if isinstance(row.get("delta"), (int, float)):
            bucket["count_with_delta"] += 1
            bucket["abs_delta_sum"] += abs(float(row["delta"]))

    pending_rows_all = sorted(
        kommone_snapshots,
        key=lambda item: (
            municipality_status(item) != "pending",
            item.get("reported_precincts") is None,
            item.get("total_precincts") is None,
            item["municipality_name"],
        ),
    )
    pending_rows = [row for row in pending_rows_all if municipality_status(row) != "complete"]
    max_pending_rows = 200

    lines: List[str] = []
    lines.append(f"# {config.election_name} ({config.election_key}) - Tracking Template")
    lines.append("")
    lines.append(f"Last poll: **{polled_at_local}**")
    lines.append("")
    lines.append("## Tracking Window")
    lines.append("")
    lines.append(
        f"- Tracking starts at **{format_local_dt(tracking_start)}**. "
        "Before this point, official result collection is intentionally disabled."
    )
    lines.append("")
    lines.append("## Data Sources")
    lines.append("")
    lines.append("- `komm.one` municipality result pages (current 2026 HTML structure, discovered recursively per county/wahlkreis)")
    lines.append(f"- Statistik BW single CSV (current mode: **{statla_mode}**) at `{statla_url}`")
    lines.append("")
    lines.append("## Operations")
    lines.append("")
    lines.append(f"- Local run: `python scripts/poll_election.py --election-key {config.election_key}`")
    lines.append(
        f"- Local minute loop: `python scripts/run_local_poll_loop.py --election-key {config.election_key} --start-at 18:00`"
    )
    lines.append(
        f"- Local mock run (Statistik BW dummy CSV only): `python scripts/run_local_mock_poll.py --election-key {config.election_key} --iterations 1 --limit-ags 10`"
    )
    lines.append(
        f"- Validate dummy StatLA result: `python scripts/validate_dummy_statla_result.py --election-key {config.election_key}`"
    )
    lines.append(
        f"- Generate static drill-down pages: `python scripts/generate_static_detail_pages.py --election-key {config.election_key}`"
    )
    lines.append(f"- Site index for this election: `{site_dir_rel}/index.html`")
    lines.append(f"- SQLite history DB (local cache, not committed): `{data_dir_rel}/history.sqlite`")
    lines.append(
        f"- Rebuild SQLite from git deltas: `python scripts/rebuild_history_sqlite_from_git_deltas.py --election-key {config.election_key}`"
    )
    lines.append("- GitHub Pages deploy workflow (manual): `.github/workflows/pages.yml`")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Municipalities tracked: **{len(municipalities)}**")
    lines.append(f"- `komm.one` complete: **{status_counts['complete']}**")
    lines.append(f"- `komm.one` pending: **{status_counts['pending']}**")
    lines.append(f"- `komm.one` no data: **{status_counts['no_data']}**")
    lines.append("")
    wahlkreis_counts = {"prestart": 0, "no_data": 0, "pending": 0, "complete": 0}
    for row in wahlkreis_status_rows:
        wahlkreis_counts[row["status"]] = wahlkreis_counts.get(row["status"], 0) + 1

    lines.append("## Wahlkreis Map")
    lines.append("")
    lines.append(f"![Wahlkreis status map]({metadata_dir_rel}/wahlkreis-status.svg)")
    lines.append("")
    lines.append(f"- Wahlkreise complete: **{wahlkreis_counts['complete']}**")
    lines.append(f"- Wahlkreise pending: **{wahlkreis_counts['pending']}**")
    lines.append(f"- Wahlkreise no data: **{wahlkreis_counts['no_data']}**")
    lines.append(f"- Status table: `{metadata_dir_rel}/wahlkreis-status.csv`")
    lines.append(f"- Geometry source ZIP: `{config.wahlkreise_geojson_zip_url}`")
    lines.append(f"- SHP source ZIP: `{config.wahlkreise_shp_zip_url}`")
    lines.append("")
    append_party_totals_tables(lines, vote_type_summary)
    lines.append("## Party Dashboard (Municipality Drill-Down)")
    lines.append("")
    if not party_summary:
        lines.append("No party data available yet.")
    else:
        lines.append("| Party | Votes | Share |")
        lines.append("|---|---:|---:|")
        for row in party_summary:
            lines.append(f"| {row['party']} | {row['votes']} | {row['share_percent']:.2f}% |")
    lines.append("")

    for row in party_summary:
        party = row["party"]
        details = party_details.get(party, [])
        lines.append(f"<details><summary>{party}</summary>")
        lines.append("")
        lines.append("| AGS | Municipality | Votes | Percent | Status |")
        lines.append("|---|---|---:|---:|---|")
        for item in details:
            votes = "" if item["votes"] is None else str(item["votes"])
            percent = "" if item["percent"] is None else f"{item['percent']:.2f}%"
            lines.append(
                f"| {item['ags']} | {item['municipality_name']} | {votes} | {percent} | {item['status']} |"
            )
        lines.append("")
        lines.append("</details>")
        lines.append("")

    lines.append("## Pending Results")
    lines.append("")
    lines.append(
        f"Showing {min(len(pending_rows), max_pending_rows)} of {len(pending_rows)} rows. "
        f"Full export: `{latest_dir_rel}/kommone_snapshots.csv`."
    )
    lines.append("")
    lines.append("<details><summary>Open pending municipalities</summary>")
    lines.append("")
    lines.append("| AGS | Municipality | `komm.one` reported/total | Status |")
    lines.append("|---|---|---:|---|")
    for row in pending_rows[:max_pending_rows]:
        status = municipality_status(row)
        rep = row.get("reported_precincts")
        total = row.get("total_precincts")
        rep_total = (
            ""
            if rep is None or total is None
            else f"{rep}/{total}"
        )
        lines.append(f"| {row['ags']} | {row['municipality_name']} | {rep_total} | {status} |")
    lines.append("")
    lines.append("</details>")
    lines.append("")

    lines.append("## Source Difference Summary")
    lines.append("")
    lines.append("| Metric | Rows with Delta | Sum(|delta|) |")
    lines.append("|---|---:|---:|")
    for metric in ["reported_precincts", "total_precincts", "voters_total", "valid_votes"]:
        bucket = statla_diff_summary.get(metric, {"count_with_delta": 0, "abs_delta_sum": 0.0})
        lines.append(
            f"| {metric} | {int(bucket['count_with_delta'])} | {bucket['abs_delta_sum']:.2f} |"
        )
    lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- Polling is designed for minute-level snapshots and immutable timing of updates/removals.")
    lines.append(f"- No official results are expected before **{format_local_dt(tracking_start)}**.")
    lines.append("- Statistik BW live data is now published from `wahlen.statistik-bw.de`; fallback still uses the provided dummy CSV when needed.")
    lines.append("- `komm.one` is polled from the current public HTML result pages because the legacy `/daten/api/...` path is no longer available on the 2026 site.")
    lines.append("- Statistik BW coded party columns (`D*`, `F*`) are resolved using the official Hinweise party codebook.")
    lines.append(f"- Election storage is keyed by `{config.election_key}` under `{data_dir_rel}/` and `{site_dir_rel}/`.")
    lines.append("")

    README_PATH.write_text("\n".join(lines), encoding="utf-8")


def write_prestart_readme(config: Config) -> None:
    tracking_start = tracking_start_local_dt(config)
    data_dir_rel = repo_relative_path(DATA_DIR)
    metadata_dir_rel = repo_relative_path(META_DIR)
    site_dir_rel = repo_relative_path(SITE_OUTPUT_DIR)
    lines: List[str] = []
    lines.append(f"# {config.election_name} ({config.election_key}) - Tracking Template")
    lines.append("")
    lines.append("## Tracking Window")
    lines.append("")
    lines.append(
        f"Automated tracking is scheduled to commence at **{format_local_dt(tracking_start)}**."
    )
    lines.append(
        f"No official results are expected before **{format_local_dt(tracking_start)}**, "
        "so polling is intentionally disabled until then."
    )
    lines.append("")
    lines.append("## Data Sources (Planned)")
    lines.append("")
    lines.append("- `komm.one` municipality result pages (current 2026 HTML structure, discovered recursively per county/wahlkreis)")
    lines.append(
        f"- Statistik BW single CSV: `{config.statla_live_csv_url}` (fallback: `{config.statla_dummy_csv_url}`)"
    )
    lines.append(f"- Wahlkreis geometry (GeoJSON ZIP): `{config.wahlkreise_geojson_zip_url}`")
    lines.append(f"- Wahlkreis geometry (SHP ZIP): `{config.wahlkreise_shp_zip_url}`")
    lines.append("")
    lines.append("## Wahlkreis Map")
    lines.append("")
    lines.append(f"![Wahlkreis status map]({metadata_dir_rel}/wahlkreis-status.svg)")
    lines.append("")
    lines.append(
        f"Map file and status table are prepared from official published geometry in `{metadata_dir_rel}/`."
    )
    lines.append("")
    append_party_totals_tables(lines, party_summary_by_vote_type_sources([], []))
    lines.append("## Operations")
    lines.append("")
    lines.append(f"- Local run after start: `python scripts/poll_election.py --election-key {config.election_key}`")
    lines.append(
        f"- Local minute loop after start: `python scripts/run_local_poll_loop.py --election-key {config.election_key} --start-at 18:00`"
    )
    lines.append(
        f"- Local mock run after start: `python scripts/run_local_mock_poll.py --election-key {config.election_key} --iterations 1 --limit-ags 10`"
    )
    lines.append(
        f"- Validate dummy StatLA result: `python scripts/validate_dummy_statla_result.py --election-key {config.election_key}`"
    )
    lines.append(
        f"- Generate static drill-down pages: `python scripts/generate_static_detail_pages.py --election-key {config.election_key}`"
    )
    lines.append(f"- Site index for this election: `{site_dir_rel}/index.html`")
    lines.append(f"- SQLite history DB (local cache, not committed): `{data_dir_rel}/history.sqlite`")
    lines.append(
        f"- Rebuild SQLite from git deltas: `python scripts/rebuild_history_sqlite_from_git_deltas.py --election-key {config.election_key}`"
    )
    lines.append("- GitHub Pages deploy workflow (manual): `.github/workflows/pages.yml`")
    lines.append("")
    README_PATH.write_text("\n".join(lines), encoding="utf-8")


def persist_files(
    label_file: str,
    kommone_snapshots: List[Dict[str, Any]],
    kommone_party_rows: List[Dict[str, Any]],
    statla: Dict[str, Any],
    diff_rows: List[Dict[str, Any]],
    events_rows: List[Dict[str, Any]],
) -> None:
    vote_type_summary = party_summary_by_vote_type_sources(
        kommone_party_rows=kommone_party_rows,
        statla_party_rows=statla.get("party_rows", []),
    )

    # Raw snapshots
    write_json(RAW_KOMMONE_DIR / f"{label_file}-kommone.json", {"snapshots": kommone_snapshots, "party_rows": kommone_party_rows})
    if statla.get("raw_csv"):
        (RAW_STATLA_DIR / f"{label_file}-statla.csv").write_text(statla["raw_csv"], encoding="utf-8")

    # Latest normalized views
    write_csv(
        LATEST_DIR / "kommone_snapshots.csv",
        [
            "ags",
            "municipality_name",
            "status",
            "reported_precincts",
            "total_precincts",
            "voters_total",
            "valid_votes",
            "invalid_votes",
            "source_timestamp",
            "payload_hash",
            "error_message",
        ],
        kommone_snapshots,
    )
    write_csv(
        LATEST_DIR / "kommone_party_results.csv",
        ["ags", "municipality_name", "vote_type", "party", "votes", "percent"],
        kommone_party_rows,
    )
    write_csv(
        LATEST_DIR / "party_vote_type_summary.csv",
        [
            "row_type",
            "vote_type",
            "party",
            "kommone_votes",
            "kommone_share_percent",
            "statla_votes",
            "statla_share_percent",
            "delta_votes",
            "delta_share_percent",
        ],
        vote_type_summary,
    )
    write_csv(
        LATEST_DIR / "statla_snapshots.csv",
        [
            "row_key",
            "ags",
            "municipality_name",
            "gebietsart",
            "gebietsnummer",
            "reported_precincts",
            "total_precincts",
            "voters_total",
            "valid_votes_erst",
            "valid_votes_zweit",
            "payload_hash",
            "is_municipality_summary",
        ],
        statla.get("snapshots", []),
    )
    write_csv(
        LATEST_DIR / "statla_party_results.csv",
        ["row_key", "vote_type", "party_key", "party_name", "votes"],
        statla.get("party_rows", []),
    )

    write_csv(
        REPORT_DIR / "latest_source_diff.csv",
        ["poll_id", "ags", "municipality_name", "metric", "kommone_value", "statla_value", "delta"],
        diff_rows,
    )
    write_csv(
        REPORT_DIR / "latest_events.csv",
        ["event_time_utc", "source", "ags", "municipality_name", "event_type", "details_json"],
        events_rows,
    )

    write_json(
        LATEST_DIR / "run_metadata.json",
        {
            "run_label": label_file,
            "generated_at_utc": now_utc().isoformat(),
            "statla_mode": statla.get("mode"),
            "statla_url": statla.get("url"),
            "statla_error": statla.get("error_message"),
            "kommone_municipalities_polled": len(kommone_snapshots),
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll election data sources and build the dashboard.")
    parser.add_argument(
        "--election-key",
        default=DEFAULT_ELECTION_KEY,
        help="Election storage key, for example 2026-bw. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Optional explicit config path. Defaults to config/<election-key>.json.",
    )
    parser.add_argument(
        "--limit-ags",
        type=int,
        default=None,
        help="Optional cap for municipality polling (useful for local dry runs).",
    )
    parser.add_argument(
        "--force-run",
        action="store_true",
        help="Run even before tracking_start_local (for testing with dummy data).",
    )
    parser.add_argument(
        "--use-dummy-statla",
        action="store_true",
        help="Force Statistik BW dummy CSV instead of live CSV.",
    )
    parser.add_argument(
        "--skip-kommone",
        action="store_true",
        help="Skip all komm.one network polling and use empty municipality snapshots.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    set_active_election(
        election_key=args.election_key,
        config_path=Path(args.config_path) if args.config_path else None,
    )
    ensure_directories()
    config = load_config()
    now_local = now_utc().astimezone(ZoneInfo(config.timezone))
    if now_local < tracking_start_local_dt(config) and not args.force_run:
        generate_wahlkreis_map(kommone_snapshots=[], statla_snapshots=[], prestart=True)
        write_prestart_readme(config)
        return

    label_file, label_human = time_labels(config.timezone)
    polled_at_utc = now_utc().isoformat()

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        seed_db_from_latest_exports(conn, config)
        poll_id = create_poll(conn, polled_at_utc=polled_at_utc, polled_at_local=label_human)

        municipalities = build_municipality_master(config, config.request_timeout_seconds)
        store_municipalities(conn, municipalities)

        if args.skip_kommone:
            selected_municipalities = municipalities[: args.limit_ags] if args.limit_ags is not None else municipalities
            kommone = {
                "snapshots": [
                    {
                        "ags": city["ags"],
                        "municipality_name": city["municipality_name"],
                        "status": "NO_DATA",
                        "reported_precincts": None,
                        "total_precincts": None,
                        "voters_total": None,
                        "valid_votes": None,
                        "invalid_votes": None,
                        "source_timestamp": None,
                        "payload_hash": None,
                        "error_message": "komm.one polling skipped by --skip-kommone",
                    }
                    for city in selected_municipalities
                ],
                "party_rows": [],
                "fetches": [],
            }
        else:
            kommone = fetch_kommone_all(
                config=config,
                municipalities=municipalities,
                timeout_seconds=config.request_timeout_seconds,
                max_workers=config.max_workers,
                limit_ags=args.limit_ags,
            )
        statla = fetch_statla(config, config.request_timeout_seconds, force_dummy=args.use_dummy_statla)

        all_fetches = list(kommone["fetches"]) + list(statla["fetches"])
        store_source_fetches(conn, poll_id, all_fetches)
        store_kommone(conn, poll_id, kommone["snapshots"], kommone["party_rows"])
        store_statla(conn, poll_id, statla["snapshots"], statla["party_rows"], statla.get("content_hash"))
        wahlkreis_status_rows = generate_wahlkreis_map(
            kommone_snapshots=kommone["snapshots"],
            statla_snapshots=statla["snapshots"],
            prestart=False,
        )

        diffs = compute_source_diffs(poll_id, kommone["snapshots"], statla["snapshots"])
        store_source_diffs(conn, diffs)
        events = read_recent_events(conn, poll_id)

        persist_files(
            label_file=label_file,
            kommone_snapshots=kommone["snapshots"],
            kommone_party_rows=kommone["party_rows"],
            statla=statla,
            diff_rows=diffs,
            events_rows=events,
        )
        generate_readme(
            config=config,
            polled_at_local=label_human,
            municipalities=municipalities if args.limit_ags is None else municipalities[: args.limit_ags],
            kommone_snapshots=kommone["snapshots"],
            party_rows=kommone["party_rows"],
            statla_party_rows=statla["party_rows"],
            statla_mode=statla.get("mode", "UNAVAILABLE"),
            statla_url=statla.get("url", ""),
            diff_rows=diffs,
            wahlkreis_status_rows=wahlkreis_status_rows,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
