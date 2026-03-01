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
import re
import sqlite3
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, request

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Python 3.9+ is required") from exc


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "ltw26_config.json"
DB_PATH = ROOT / "data" / "ltw26" / "history.sqlite"
RAW_KOMMONE_DIR = ROOT / "data" / "ltw26" / "raw" / "kommone"
RAW_STATLA_DIR = ROOT / "data" / "ltw26" / "raw" / "statla"
LATEST_DIR = ROOT / "data" / "ltw26" / "latest"
REPORT_DIR = ROOT / "data" / "ltw26" / "reports"
META_DIR = ROOT / "data" / "ltw26" / "metadata"
README_PATH = ROOT / "README.md"
WAHLKREIS_GEOJSON_PATH = META_DIR / "LTWahlkreise2026-BW.geojson"
WAHLKREIS_MAPPING_PATH = META_DIR / "LTWahlkreise2026-BW-wkr_kr_gem.csv"
WAHLKREIS_STATUS_MAP_PATH = META_DIR / "wahlkreis-status.svg"
WAHLKREIS_STATUS_CSV_PATH = META_DIR / "wahlkreis-status.csv"

STATLA_EXCLUDED_GEBIETSART = {
    "LAND",
    "WAHLKREIS",
    "URNENWAHLBEZIRK",
    "BRIEFWAHLBEZIRK",
}


@dataclass(frozen=True)
class Config:
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
    request_timeout_seconds: int
    max_workers: int


@dataclass
class HttpResult:
    url: str
    status_code: Optional[int]
    content: bytes
    error_message: Optional[str]


def ensure_directories() -> None:
    for directory in [RAW_KOMMONE_DIR, RAW_STATLA_DIR, LATEST_DIR, REPORT_DIR, META_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def load_config() -> Config:
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return Config(
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
        request_timeout_seconds=int(data.get("request_timeout_seconds", 4)),
        max_workers=int(data.get("max_workers", 48)),
    )


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
    req = request.Request(url, headers={"User-Agent": "ltw26-template-poller/1.0"})
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
        return HttpResult(
            url=url,
            status_code=None,
            content=b"",
            error_message=str(exc),
        )


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
    for path in [META_DIR / "municipalities.csv", META_DIR / "ltw26-bw-gemeinden.csv"]:
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

    legacy_path = ROOT / config.legacy_city_source_csv
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


def fetch_one_kommone_ags(
    config: Config,
    ags: str,
    municipality_name: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    base_url = config.kommone_base_url_template.format(wahltermin=config.kommone_wahltermin, ags=ags)
    termin_url = f"{base_url}/daten/api/termin.json"
    fetches: List[Dict[str, Any]] = []

    termin_result = http_get(termin_url, timeout_seconds)
    fetches.append(
        {
            "source": "kommone",
            "url": termin_url,
            "status_code": termin_result.status_code,
            "content_hash": sha256_bytes(termin_result.content) if termin_result.content else None,
            "byte_count": len(termin_result.content),
            "error_message": termin_result.error_message,
        }
    )

    if termin_result.status_code != 200 or not termin_result.content:
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
                "error_message": termin_result.error_message or f"HTTP {termin_result.status_code}",
            },
            "party_rows": [],
            "fetches": fetches,
        }

    try:
        termin_payload = json.loads(decode_bytes(termin_result.content))
    except json.JSONDecodeError:
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
                "error_message": "Invalid JSON in termin payload",
            },
            "party_rows": [],
            "fetches": fetches,
        }

    entries = termin_payload.get("wahleintraege") or []
    if not entries:
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
                "payload_hash": sha256_bytes(termin_result.content),
                "error_message": None,
            },
            "party_rows": [],
            "fetches": fetches,
        }

    all_party_rows: List[Dict[str, Any]] = []
    payloads_for_hash: List[Dict[str, Any]] = []
    best_info: Dict[str, Any] = {}
    source_timestamp: Optional[str] = None

    for entry in entries:
        wahl = entry.get("wahl") or {}
        stimmentyp = entry.get("stimmentyp") or {}
        gebiet_link = entry.get("gebiet_link") or {}
        wahl_id = wahl.get("id")
        stimmentyp_id = stimmentyp.get("id")
        vote_type = str(stimmentyp.get("titel") or f"type-{stimmentyp_id}")
        link_id = gebiet_link.get("id")
        if wahl_id is None or stimmentyp_id is None or not link_id:
            continue

        result_url = f"{base_url}/daten/api/wahl_{wahl_id}/ergebnis_{link_id}_{stimmentyp_id}.json"
        result = http_get(result_url, timeout_seconds)
        fetches.append(
            {
                "source": "kommone",
                "url": result_url,
                "status_code": result.status_code,
                "content_hash": sha256_bytes(result.content) if result.content else None,
                "byte_count": len(result.content),
                "error_message": result.error_message,
            }
        )
        if result.status_code != 200 or not result.content:
            continue

        try:
            payload = json.loads(decode_bytes(result.content))
        except json.JSONDecodeError:
            continue

        payloads_for_hash.append(payload)
        source_timestamp = str(payload.get("zeitstempel") or source_timestamp or "")

        info = (((payload.get("Komponente") or {}).get("info") or {}).get("tabelle") or {}).get("zeilen") or []
        info_numbers = parse_kommone_info_rows(info)
        hint_lines = (((payload.get("Komponente") or {}).get("info") or {}).get("hinweis") or [])
        if isinstance(hint_lines, str):
            hint_lines = [hint_lines]
        reported, total = parse_kommone_progress([str(item) for item in hint_lines])

        prev_total = best_info.get("total_precincts") or -1
        if total is not None and total >= prev_total:
            best_info = {
                "reported_precincts": reported,
                "total_precincts": total,
                **info_numbers,
            }

        rows = (((payload.get("Komponente") or {}).get("tabelle") or {}).get("zeilen") or [])
        for row in rows:
            party_label = str(((row or {}).get("label") or {}).get("labelKurz") or "").strip()
            party = normalize_party_label(party_label)
            all_party_rows.append(
                {
                    "ags": ags,
                    "municipality_name": municipality_name,
                    "vote_type": vote_type,
                    "party": party,
                    "votes": parse_int((row or {}).get("zahl")),
                    "percent": parse_float_percent((row or {}).get("prozent")),
                }
            )

    payload_hash = None
    status = "NO_DATA"
    if payloads_for_hash:
        status = "HAS_DATA"
        canonical_payload = json.dumps(payloads_for_hash, sort_keys=True, ensure_ascii=False).encode("utf-8")
        payload_hash = sha256_bytes(canonical_payload)

    snapshot = {
        "ags": ags,
        "municipality_name": municipality_name,
        "status": status,
        "reported_precincts": best_info.get("reported_precincts"),
        "total_precincts": best_info.get("total_precincts"),
        "voters_total": best_info.get("voters_total"),
        "valid_votes": best_info.get("valid_votes"),
        "invalid_votes": best_info.get("invalid_votes"),
        "source_timestamp": source_timestamp,
        "payload_hash": payload_hash,
        "error_message": None,
    }
    return {"snapshot": snapshot, "party_rows": all_party_rows, "fetches": fetches}


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
    fetches: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_one_kommone_ags,
                config,
                city["ags"],
                city["municipality_name"],
                timeout_seconds,
            ): city
            for city in selected
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
            parties.append(
                {
                    "vote_type": "Erststimmen",
                    "party_key": key,
                    "party_name": key,
                    "votes": value,
                }
            )
            continue
        if re.fullmatch(r"F\d+", key):
            parties.append(
                {
                    "vote_type": "Zweitstimmen",
                    "party_key": key,
                    "party_name": key,
                    "votes": value,
                }
            )
            continue

        if key.endswith("Erststimmen") and "gueltige" not in normalize_text(key) and "ungueltige" not in normalize_text(key):
            parties.append(
                {
                    "vote_type": "Erststimmen",
                    "party_key": key,
                    "party_name": key.replace(" Erststimmen", "").strip(),
                    "votes": value,
                }
            )
        if key.endswith("Zweitstimmen") and "gueltige" not in normalize_text(key) and "ungueltige" not in normalize_text(key):
            parties.append(
                {
                    "vote_type": "Zweitstimmen",
                    "party_key": key,
                    "party_name": key.replace(" Zweitstimmen", "").strip(),
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
        local_dummy = META_DIR / "2026021_LTW26-Dummy-Datei.csv"
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
        party = str(row.get(party_field) or "").strip()
        if not party or not isinstance(votes, int):
            continue
        vote_type = canonical_vote_type(str(row.get("vote_type") or ""))
        bucket = totals_by_type.setdefault(vote_type, {})
        bucket[party] = bucket.get(party, 0) + votes
    return totals_by_type


def fixed_party_order_by_vote_type() -> Dict[str, List[str]]:
    first: List[str] = []
    second: List[str] = []
    dummy_path = META_DIR / "2026021_LTW26-Dummy-Datei.csv"
    if dummy_path.exists():
        try:
            header_line = decode_bytes(dummy_path.read_bytes()).splitlines()[0]
            header = next(csv.reader([header_line], delimiter=";"))
            first = sorted(
                [name for name in header if re.fullmatch(r"D\d+", name)],
                key=lambda name: int(name[1:]),
            )
            second = sorted(
                [name for name in header if re.fullmatch(r"F\d+", name)],
                key=lambda name: int(name[1:]),
            )
        except Exception:  # pylint: disable=broad-except
            pass
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
                    "vote_type": vote_type,
                    "party": "",
                    "kommone_votes": 0,
                    "kommone_share_percent": 0.0,
                    "statla_votes": 0,
                    "statla_share_percent": 0.0,
                }
            )
            continue

        for party in parties:
            kommone_votes = k_party_totals.get(party, 0)
            statla_votes = s_party_totals.get(party, 0)
            rows.append(
                {
                    "vote_type": vote_type,
                    "party": party,
                    "kommone_votes": kommone_votes,
                    "kommone_share_percent": (kommone_votes / k_grand_total * 100.0) if k_grand_total else 0.0,
                    "statla_votes": statla_votes,
                    "statla_share_percent": (statla_votes / s_grand_total * 100.0) if s_grand_total else 0.0,
                }
            )
    return rows


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
            if not line.strip() or line.startswith("#"):
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
        "<text x='20' y='1245' font-size='11' fill='#374151'>Source: Statistik BW LTWahlkreise2026-BW geometry</text>"
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
    lines.append(f"# {config.election_name} - Tracking Template")
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
    lines.append(
        f"- `komm.one` municipality APIs (template: `{config.kommone_base_url_template}` + `/daten/api/...`)"
    )
    lines.append(f"- Statistik BW single CSV (current mode: **{statla_mode}**) at `{statla_url}`")
    lines.append("")
    lines.append("## Operations")
    lines.append("")
    lines.append("- Local run: `python scripts/poll_ltw26.py`")
    lines.append("- SQLite history DB (local cache, not committed): `data/ltw26/history.sqlite`")
    lines.append("- Rebuild SQLite from git deltas: `python scripts/rebuild_history_sqlite_from_git_deltas.py`")
    lines.append("- Minute automation: `.github/workflows/poll.yml`")
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
    lines.append("![Wahlkreis status map](data/ltw26/metadata/wahlkreis-status.svg)")
    lines.append("")
    lines.append(f"- Wahlkreise complete: **{wahlkreis_counts['complete']}**")
    lines.append(f"- Wahlkreise pending: **{wahlkreis_counts['pending']}**")
    lines.append(f"- Wahlkreise no data: **{wahlkreis_counts['no_data']}**")
    lines.append("- Status table: `data/ltw26/metadata/wahlkreis-status.csv`")
    lines.append(f"- Geometry source ZIP: `{config.wahlkreise_geojson_zip_url}`")
    lines.append(f"- SHP source ZIP: `{config.wahlkreise_shp_zip_url}`")
    lines.append("")
    lines.append("## Party Totals (First and Second Votes)")
    lines.append("")
    lines.append("| Vote Type | Party | `komm.one` Count | `komm.one` Share | `statla` Count | `statla` Share |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for row in vote_type_summary:
        party_label = row["party"] or "-"
        lines.append(
            (
                f"| {row['vote_type']} | {party_label} | {int(row['kommone_votes'])} | "
                f"{float(row['kommone_share_percent']):.2f}% | {int(row['statla_votes'])} | "
                f"{float(row['statla_share_percent']):.2f}% |"
            )
        )
    lines.append("")
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
        "Full export: `data/ltw26/latest/kommone_snapshots.csv`."
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
    lines.append("- `komm.one` is expected to publish first. Statistik BW may start later; fallback currently uses the provided dummy CSV.")
    lines.append("- If Statistik BW keeps coded party columns (e.g. `D1`, `F1`), cross-source party mapping requires an external codebook.")
    lines.append("")

    README_PATH.write_text("\n".join(lines), encoding="utf-8")


def write_prestart_readme(config: Config) -> None:
    tracking_start = tracking_start_local_dt(config)
    lines: List[str] = []
    lines.append(f"# {config.election_name} - Tracking Template")
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
    lines.append(
        f"- `komm.one` municipality APIs (template: `{config.kommone_base_url_template}` + `/daten/api/...`)"
    )
    lines.append(
        f"- Statistik BW single CSV: `{config.statla_live_csv_url}` (fallback: `{config.statla_dummy_csv_url}`)"
    )
    lines.append(f"- Wahlkreis geometry (GeoJSON ZIP): `{config.wahlkreise_geojson_zip_url}`")
    lines.append(f"- Wahlkreis geometry (SHP ZIP): `{config.wahlkreise_shp_zip_url}`")
    lines.append("")
    lines.append("## Wahlkreis Map")
    lines.append("")
    lines.append("![Wahlkreis status map](data/ltw26/metadata/wahlkreis-status.svg)")
    lines.append("")
    lines.append(
        "Map file and status table are prepared from official published geometry in `data/ltw26/metadata/`."
    )
    lines.append("")
    lines.append("## Party Totals (First and Second Votes)")
    lines.append("")
    lines.append("| Vote Type | Party | `komm.one` Count | `komm.one` Share | `statla` Count | `statla` Share |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for row in party_summary_by_vote_type_sources([], []):
        party_label = row["party"] or "-"
        lines.append(
            (
                f"| {row['vote_type']} | {party_label} | {int(row['kommone_votes'])} | "
                f"{float(row['kommone_share_percent']):.2f}% | {int(row['statla_votes'])} | "
                f"{float(row['statla_share_percent']):.2f}% |"
            )
        )
    lines.append("")
    lines.append("## Operations")
    lines.append("")
    lines.append("- Local run after start: `python scripts/poll_ltw26.py`")
    lines.append("- SQLite history DB (local cache, not committed): `data/ltw26/history.sqlite`")
    lines.append("- Rebuild SQLite from git deltas: `python scripts/rebuild_history_sqlite_from_git_deltas.py`")
    lines.append("- Minute automation: `.github/workflows/poll.yml`")
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
            "vote_type",
            "party",
            "kommone_votes",
            "kommone_share_percent",
            "statla_votes",
            "statla_share_percent",
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
    parser = argparse.ArgumentParser(description="Poll BW LTW26 data sources and build dashboard.")
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
