"""Strict, lossless acquisition of the official Sachsen-Anhalt overview and downloads.

Each file is parsed with its own header. Failed attempts remain in ignored raw
storage; only a complete, validated result may replace the published exports.
"""

from __future__ import annotations

import csv
import html
import io
import json
import re
from collections import Counter, defaultdict
from urllib.parse import urljoin, urlsplit

import poll_election_core as core


def parse_overview_page(content: bytes) -> dict[str, int | str]:
    """Parse the official overview's embedded polling-district status table."""
    text = core.decode_bytes(content)
    if not core.looks_like_html_document(text):
        raise ValueError("LSA overview response is not HTML")

    for attrs, body in re.findall(r"<script([^>]*)>(.*?)</script>", text, re.S | re.I):
        if "application/json" not in attrs.lower() or "data-for" not in attrs.lower():
            continue
        try:
            widget = json.loads(body)
        except json.JSONDecodeError:
            continue
        data = (
            widget.get("x", {})
            .get("tag", {})
            .get("attribs", {})
            .get("data", {})
        )
        if not isinstance(data, dict) or not {"wbz_ist", "wbz_soll"}.issubset(data):
            continue
        reported = data["wbz_ist"]
        total = data["wbz_soll"]
        if not isinstance(reported, list) or not isinstance(total, list) or len(reported) != len(total):
            raise ValueError("LSA overview has invalid polling-district arrays")
        if any(
            isinstance(actual, bool)
            or isinstance(expected, bool)
            or not isinstance(actual, (int, float))
            or not isinstance(expected, (int, float))
            or actual < 0
            or expected < 0
            or actual > expected
            for actual, expected in zip(reported, total)
        ):
            raise ValueError("LSA overview has invalid polling-district counts")
        return {
            "reported_precincts": int(sum(reported)),
            "total_precincts": int(sum(total)),
            "rows": len(reported),
            "reported_rows": sum(1 for value in reported if value > 0),
            "content_hash": core.sha256_bytes(content),
        }
    raise ValueError("LSA overview contains no polling-district table")


def parse_dynamic_results_page(content: bytes) -> dict:
    """Parse party totals from the dynamic ``Ergebnisse`` Wahlkreise page.

    The overview renders its result links with JavaScript.  The linked page
    embeds the complete Wahlkreise table as a Reactable JSON widget; its
    ``anzahl.wj.x`` values are second votes and ``anzahl.wj.y`` values are
    first votes.
    """
    text = core.decode_bytes(content)
    if not core.looks_like_html_document(text):
        raise ValueError("LSA dynamic result response is not HTML")

    widget_data = None
    for attrs, body in re.findall(r"<script([^>]*)>(.*?)</script>", text, re.S | re.I):
        if not re.search(r"data-for\s*=\s*[\"']ergtable[\"']", attrs, re.I):
            continue
        try:
            widget = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError("LSA dynamic result table contains invalid JSON") from exc
        widget_data = (
            widget.get("x", {})
            .get("tag", {})
            .get("attribs", {})
            .get("data", {})
        )
        break
    if not isinstance(widget_data, dict):
        raise ValueError("LSA dynamic result page contains no Wahlkreise table")

    required = ("id", "merkmal", "partei_pos", "anzahl.wj.x", "anzahl.wj.y")
    if any(key not in widget_data or not isinstance(widget_data[key], list) for key in required):
        raise ValueError("LSA dynamic result table is missing party columns")
    length = len(widget_data["id"])
    if any(len(widget_data[key]) != length for key in required):
        raise ValueError("LSA dynamic result table has inconsistent column lengths")

    party_rows = []
    seen = set()
    wahlkreis_ids = set()
    for index in range(length):
        position = core.parse_int(widget_data["partei_pos"][index])
        if position is None or position <= 0:
            continue
        number = str(widget_data["id"][index] or "").strip()
        if not re.fullmatch(r"\d{1,3}", number):
            raise ValueError(f"LSA dynamic result has invalid Wahlkreis id: {number!r}")
        number = number.zfill(3)
        wahlkreis_ids.add(number)
        label = html.unescape(str(widget_data["merkmal"][index] or ""))
        label = re.sub(r"<[^>]+>", "", label).replace("\u00ad", "").strip()
        if not label:
            raise ValueError(f"LSA dynamic result has an empty party label for {number}")
        if core.normalize_text(label) == "einzelbewerber/in":
            label = "Anderer Kreiswahlvorschlag"

        for column, vote_type, prefix in (
            ("anzahl.wj.x", "Zweitstimmen", "F"),
            ("anzahl.wj.y", "Erststimmen", "D"),
        ):
            votes = core.parse_int(widget_data[column][index])
            if votes is None:
                continue
            if votes < 0:
                raise ValueError(f"LSA dynamic result has negative party votes: {number}/{label}")
            party_key = f"{prefix}{position}"
            row = {
                "row_key": f"lsa:WAHLKREIS:{number}",
                "vote_type": vote_type,
                "party_key": party_key,
                "party_name": core.canonical_party_name(label, vote_type),
                "votes": votes,
            }
            identity = (row["row_key"], row["vote_type"], row["party_key"])
            if identity in seen:
                raise ValueError(f"LSA dynamic result has duplicate party row: {identity}")
            seen.add(identity)
            party_rows.append(row)

    if len(wahlkreis_ids) != 41:
        raise ValueError(f"LSA dynamic result covers {len(wahlkreis_ids)} Wahlkreise; expected 41")
    if not party_rows:
        raise ValueError("LSA dynamic result contains no numeric party totals")
    return {
        "party_rows": sorted(party_rows, key=lambda row: (row["row_key"], row["vote_type"], row["party_key"])),
        "wahlkreis_count": len(wahlkreis_ids),
        "party_row_count": len(party_rows),
        "content_hash": core.sha256_bytes(content),
    }


def read_rows(content: bytes) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(core.decode_bytes(content)), delimiter=";")
    fields = reader.fieldnames or []
    if not fields or len(fields) != len(set(fields)):
        raise ValueError("LSA CSV has missing or duplicate column names")
    rows = list(reader)
    for number, row in enumerate(rows, 2):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"LSA CSV row {number} has the wrong number of columns")
        for key, value in row.items():
            if re.match(r"^(?:[A-F](?:\.|\d)|(?:Soll|Ist)\.Wahlbezirke$)", key):
                if value.strip() and not re.fullmatch(r"\d+", value.strip()):
                    raise ValueError(f"LSA CSV row {number}: invalid integer in {key}: {value!r}")
    return rows


def validate_results(snapshots: list[dict], parties: list[dict], previous: list[dict]) -> None:
    keys = [row["row_key"] for row in snapshots]
    key_set = set(keys)
    if len(keys) != len(key_set):
        raise ValueError("LSA CSV contains duplicate result areas")
    levels = Counter(row["gebietsart"] for row in snapshots)
    for level, count in (("LAND", 1), ("KREIS", 14), ("WAHLKREIS", 41)):
        if levels[level] != count:
            raise ValueError(f"LSA coverage: {level} has {levels[level]} rows; expected {count}")
    municipalities = {row["ags"] for row in snapshots if row["gebietsart"] == "GEMEINDE"}
    if len(municipalities) != 218 or any(not re.fullmatch(r"15\d{6}", ags) for ags in municipalities):
        raise ValueError(f"LSA coverage: expected 218 municipality AGS; got {len(municipalities)}")
    missing = {row["row_key"] for row in previous} - key_set
    if missing:
        raise ValueError(f"LSA coverage lost {len(missing)} previously collected areas: {sorted(missing)[:3]}")

    totals = defaultdict(int)
    party_keys = set()
    for row in parties:
        key = (row["row_key"], row["vote_type"], row["party_key"])
        if key in party_keys or row["row_key"] not in key_set:
            raise ValueError(f"LSA duplicate or orphan party result: {key}")
        party_keys.add(key)
        votes = row.get("votes")
        if not isinstance(votes, int) or votes < 0:
            raise ValueError(f"LSA invalid party vote count: {key}")
        totals[key[:2]] += votes
    for row in snapshots:
        reported, total = row.get("reported_precincts"), row.get("total_precincts")
        if reported is not None and total is not None and not 0 <= reported <= total:
            raise ValueError(f"LSA invalid reporting progress: {row['row_key']}")
        for vote_type, field in (("Erststimmen", "valid_votes_erst"), ("Zweitstimmen", "valid_votes_zweit")):
            valid = row.get(field) or 0
            if valid != totals[(row["row_key"], vote_type)]:
                raise ValueError(f"LSA party sum differs from {field}: {row['row_key']}")
            if row.get("voters_total") is not None and valid > row["voters_total"]:
                raise ValueError(f"LSA valid votes exceed voters: {row['row_key']}")
    # Official corrections may lower votes or reporting counts. Do not impose
    # monotonic totals or compare independently refreshed files to each other.


def fetch_lsa(config: core.Config, timeout_seconds: int) -> dict:
    attempt = core.RAW_STATLA_DIR / core.now_utc().strftime("%Y%m%dT%H%M%S.%fZ")
    attempt.mkdir(parents=True, exist_ok=False)
    payloads = []
    fetches = []

    def fetch(url: str, filename: str) -> core.HttpResult:
        result = core.statla_http_get(url, timeout_seconds, show_progress=False)
        (attempt / filename).write_bytes(result.content)
        fetches.append({
            "source": "statla", "url": url, "status_code": result.status_code,
            "content_hash": core.sha256_bytes(result.content), "byte_count": len(result.content),
            "error_message": result.error_message, "filename": filename,
            "fetched_at_utc": core.now_utc().isoformat(),
        })
        core.write_json(attempt / "manifest.json", {"fetches": fetches})
        if result.status_code != 200 or result.error_message or not result.content:
            raise ValueError(f"LSA source unavailable: {url}: HTTP {result.status_code}; {result.error_message}")
        payloads.append({"filename": filename, "content": result.content})
        return result

    overview_url = str(config.statla_live_csv_url or "").strip()
    if not overview_url:
        raise ValueError("No official LSA overview URL configured")
    overview = fetch(overview_url, "overview.html")
    overview_summary = parse_overview_page(overview.content)

    # The overview's rendered "Ergebnisse" cells navigate to this dynamic
    # Wahlkreise page.  Keep the exact response and use its embedded party
    # totals when they reconcile with the CSV snapshots below.
    dynamic_results_summary = None
    dynamic_results_url = str(getattr(config, "statla_live_results_url", "") or "").strip()
    if dynamic_results_url:
        dynamic_results = fetch(dynamic_results_url, "erg_wkr.html")
        dynamic_results_summary = parse_dynamic_results_page(dynamic_results.content)

    downloads_url = str(config.statla_downloads_url or "").strip()
    if not downloads_url:
        raise ValueError("No official LSA downloads URL configured")
    page = fetch(downloads_url, "downloads.html")
    urls = set()
    for href in re.findall(r"href\s*=\s*[\"']([^\"']+)[\"']", core.decode_bytes(page.content), re.I):
        url = urljoin(downloads_url, html.unescape(href).strip())
        if urlsplit(url).path.lower().endswith(".csv"):
            if urlsplit(url).netloc != urlsplit(downloads_url).netloc:
                raise ValueError(f"Unexpected external LSA CSV link: {url}")
            urls.add(url)
    if not urls:
        raise ValueError("Official LSA downloads page contains no CSV links")

    snapshots, parties, summary_rows, booth_texts = [], [], [], []
    result_urls, booth_urls = [], []
    for url in sorted(urls):
        filename = core.sha256_bytes(url.encode())[:16] + ".csv"
        result = fetch(url, filename)
        rows = read_rows(result.content)
        text = core.decode_bytes(result.content)
        if core.looks_like_statla_wahlbezirk_csv(text):
            parsed, parsed_parties = core.parse_statla_wahlbezirk_csv_rows(text)
            if not parsed:
                raise ValueError(f"Published LSA Wahlbezirk CSV is empty: {url}")
            booth_texts.append(text)
            booth_urls.append(url)
        elif core.looks_like_statla_csv(text):
            parsed, parsed_parties = core.parse_statla_csv_rows(text)
            summary_rows.extend(rows)
            result_urls.append(url)
        else:
            if core.looks_like_html_document(text):
                raise ValueError(f"LSA CSV link returned HTML: {url}")
            # Seat distribution and other auxiliary CSVs are retained verbatim.
            continue
        snapshots.extend(parsed)
        parties.extend(parsed_parties)

    if dynamic_results_summary:
        dynamic_party_rows = dynamic_results_summary["party_rows"]
        snapshots_by_key = {row["row_key"]: row for row in snapshots}
        dynamic_keys = {row["row_key"] for row in dynamic_party_rows}
        totals = defaultdict(int)
        for row in dynamic_party_rows:
            totals[(row["row_key"], row["vote_type"])] += row["votes"]
        dynamic_matches_csv = all(
            key in snapshots_by_key
            and totals[(key, "Erststimmen")] == (snapshots_by_key[key].get("valid_votes_erst") or 0)
            and totals[(key, "Zweitstimmen")] == (snapshots_by_key[key].get("valid_votes_zweit") or 0)
            for key in dynamic_keys
        ) and len(dynamic_keys) == 41
        if dynamic_matches_csv:
            parties = [row for row in parties if row["row_key"] not in dynamic_keys]
            parties.extend(dynamic_party_rows)
        else:
            dynamic_results_summary["used_for_normalized_results"] = False
    if dynamic_results_summary and "used_for_normalized_results" not in dynamic_results_summary:
        dynamic_results_summary["used_for_normalized_results"] = True

    validate_results(snapshots, parties, core.load_latest_statla_exports()["snapshots"])
    # Serialize by column name, never concatenate bodies under a foreign header.
    fieldnames = list(dict.fromkeys(key for row in summary_rows for key in row))
    combined = io.StringIO(newline="")
    writer = csv.DictWriter(combined, fieldnames=fieldnames, delimiter=";")
    writer.writeheader()
    writer.writerows(summary_rows)
    digest = core.sha256_bytes(json.dumps(
        [(item["url"], item["content_hash"]) for item in fetches],
        sort_keys=True,
    ).encode())
    source_urls = [overview_url]
    if dynamic_results_url:
        source_urls.append(dynamic_results_url)
    source_urls.extend(result_urls)
    source_urls.extend(booth_urls)
    return {
        "mode": "LIVE_OVERVIEW_HTML_WITH_CSV_DOWNLOAD", "url": ";".join(source_urls),
        "status_code": 200, "content_hash": digest, "raw_csv": combined.getvalue(),
        "raw_wahlbezirk_csv": "\n".join(booth_texts),
        "source_copy_text": core.decode_bytes(overview.content), "source_copy_url": overview.url,
        "source_copy_filename": "official-results-source.html",
        "source_copy_status_code": 200, "source_copy_error": None,
        "source_copy_hash": core.sha256_bytes(overview.content),
        "overview_summary": overview_summary,
        "dynamic_results_summary": {
            **(dynamic_results_summary or {}),
            "url": dynamic_results_url or None,
        },
        "wahlbezirk_source_url": ";".join(booth_urls) or None,
        "wahlbezirk_source_error": None if booth_urls else "Wahlbezirk CSV not published yet",
        "snapshots": sorted(snapshots, key=lambda row: row["row_key"]),
        "party_rows": sorted(parties, key=lambda row: (row["row_key"], row["vote_type"], row["party_key"])),
        "fetches": fetches, "error_message": None,
        "source_payloads": payloads,
        "source_manifest": {"election_key": config.election_key, "content_hash": digest, "fetches": fetches},
    }
