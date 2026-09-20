"""Shared, read-only inputs for the Berlin/MV post-election report ports."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ELECTIONS = {"2026-be": "Berlin", "2026-mv": "Mecklenburg-Vorpommern"}
POPULATION = {
    "2026-be": (3700577, "https://www.statistik-berlin-brandenburg.de/presse/2026/73-bevoelkerungsfortschreibung-2025-berlin/"),
    "2026-mv": (1573685, "https://www.laiv-mv.de/Pressemitteilungen/?id=221343&processor=processor.sa.pressemitteilung"),
}
BERLIN_CENSUS_URL = "https://download.statistik-berlin-brandenburg.de/a9cf3ac18d95cdc0/d6af0d4cfde4/Zensus2022_Basistabelle_Berlin.xlsx"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def read_csv(path):
    return list(csv.DictReader(path.read_text(encoding="utf-8-sig").splitlines()))


def write_csv(path, rows, fields=None):
    rows = list(rows)
    fields = fields or list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def integer(value):
    if value is None or str(value).strip() in {"", "x", "-", "."}:
        return None
    return int(str(value).replace(" ", "").replace("\xa0", ""))


def raw_rows(key, payload):
    """MV's retained payload concatenates three CSVs with different headers."""
    text = payload.decode("utf-8-sig")
    if key == "2026-be":
        return list(csv.DictReader(io.StringIO(text), delimiter=";"))
    from poll_election_core import mv_csv_rows_from_text
    sections = text.split("# SOURCE: ")[1:]
    if not sections:
        sections = ["single\n" + text]
    return [r for section in sections
            for r in mv_csv_rows_from_text(section.split("\n", 1)[1])
            if r.get("Ausgabe") == "A"]


def capture(key, source_dir=None):
    """Copy a coherent local capture; never combine current and historical bytes."""
    latest = Path(source_dir) if source_dir else ROOT / "data" / key / "latest"
    names = ["run_metadata.json", "statla_snapshots.csv", "statla_party_results.csv"]
    content = {name: (latest / name).read_bytes() for name in names}
    meta = json.loads(content["run_metadata.json"])
    raw = latest / 'source_results.csv' if source_dir else ROOT / "data" / key / "raw/statla" / (meta["run_label"] + "-statla.csv")
    if not raw.exists():
        raise ValueError(f"Matching raw capture missing: {raw}; obtain a fresh local poll before building")
    content["source_results.csv"] = raw.read_bytes()
    if any((latest / name).read_bytes() != content[name] for name in names):
        raise ValueError("Collector changed latest during capture; retry")
    if meta.get("statla_error") or meta.get("statla_mode") != "LIVE_CSV_DOWNLOAD":
        raise ValueError("A successful live CSV capture is required")
    snapshots = list(csv.DictReader(io.StringIO(content["statla_snapshots.csv"].decode())))
    parties = list(csv.DictReader(io.StringIO(content["statla_party_results.csv"].decode())))
    for row in snapshots:
        for field in ["reported_precincts", "total_precincts", "voters_total", "valid_votes_erst", "valid_votes_zweit"]:
            row[field] = integer(row[field])
    for row in parties:
        row["votes"] = integer(row["votes"])
    return meta, snapshots, parties, raw_rows(key, content["source_results.csv"]), content


def raw_context(key, rows):
    """Recover electorate, geographical identifiers and voting mode from source."""
    result = {}
    for r in rows:
        if key == "2026-be":
            if r["StimmArt"] != "2":
                continue
            level = r["Gebietsart"]
            if level == "Bundesland":
                row_key = "berlin:LAND"
            elif level == "Bezirk":
                row_key = f"berlin:GEMEINDE:110000{int(r['Nummer']):02d}"
            else:
                # Constituency numbers in the normalised files are sequential.
                continue
            result[row_key] = dict(eligible=integer(r["WberIns"]),
                                   voters=integer(r["Waehler"]), valid=integer(r["Gueltig"]),
                                   invalid=integer(r["Unguelt"]), name=r["Gebietsname"],
                                   geo_id=f"11{int(r['Nummer']):02d}", source_time=r["Datum"] + " " + r["Zeit"])
        else:
            if r.get("Erst-/Zweitstimme") != "2":
                continue
            if "Wahlbezirksname" in r:
                row_key = f"mv:WAHLBEZIRK:{r['Gemeinde']}:{int(r['Wahlkreis'])}:{r['Wahlbezirk']}"
            elif "Wahlkreisname/Land" in r:
                row_key = "mv:LAND" if int(r["Wahlkreis"]) == 99 else f"mv:WAHLKREIS:{int(r['Wahlkreis'])}"
            else:
                row_key = "mv:GEMEINDE:" + r["Gemeinde"]
            amt = integer(r.get("Amt"))
            geo_id = (r["Gemeinde"][:5] + str(amt).zfill(4) if amt and amt >= 5000
                      else r.get("Gemeinde", ""))
            result[row_key] = dict(eligible=integer(r["Wahlberechtigte"]),
                                   voters=integer(r["Wähler"]), valid=integer(r["Gültige Stimmen"]),
                                   invalid=integer(r["Ungültige Stimmen"]),
                                   name=r.get("Amtsname") or r.get("Wahlkreisname/Land", ""),
                                   geo_id=geo_id, source_time=r["Berechnungsdatum"],
                                   mode=("Briefwahl" if "brief" in r.get("Wahlbezirksname", "").lower()
                                         else "Urnenwahl") if "Wahlbezirksname" in r else "Gesamt")
    return result


def validate_capture(key, meta, snapshots, parties, context):
    by_key = {r["row_key"]: r for r in snapshots}
    if len(by_key) != len(snapshots):
        raise ValueError("Duplicate snapshot keys")
    lands = [r for r in snapshots if r["gebietsart"] == "LAND"]
    if len(lands) != 1:
        raise ValueError("Expected exactly one Land total")
    land = lands[0]
    issues = []
    party_map = {}
    party_sums = {}
    for r in parties:
        identity = (r["row_key"], r["vote_type"], r["party_name"])
        if identity in party_map or r["votes"] is None or r["votes"] < 0 or r["row_key"] not in by_key:
            raise ValueError(f"Invalid/duplicate party row: {identity}")
        party_map[identity] = r["votes"]
        group = identity[:2]
        party_sums[group] = party_sums.get(group, 0) + r["votes"]
    checks = []
    for s in snapshots:
        for vote, field in [("Erststimmen", "valid_votes_erst"), ("Zweitstimmen", "valid_votes_zweit")]:
            value = s[field]
            total = party_sums.get((s["row_key"], vote), 0)
            if value is not None and value != total:
                issues.append(f"Party sum mismatch: {s['row_key']} {vote}: {total} != {value}")
        c = context.get(s["row_key"])
        if c and (s["valid_votes_zweit"] != c["valid"] or s["voters_total"] != c["voters"]):
            issues.append(f"Raw/normalised mismatch: {s['row_key']}")
        if c and c["valid"] is not None and c["invalid"] is not None and c["valid"] + c["invalid"] != c["voters"]:
            issues.append(f"Valid + invalid != voters: {s['row_key']}")
    for level in ["GEMEINDE", "WAHLKREIS", "WAHLBEZIRK"]:
        group = [r for r in snapshots if r["gebietsart"] == level]
        if not group:
            continue
        for field in ["voters_total", "valid_votes_erst", "valid_votes_zweit"]:
            total = sum(r[field] or 0 for r in group)
            checks.append(dict(level=level, field=field, sum=total, land=land[field], delta=total - (land[field] or 0)))
        for vote in ["Erststimmen", "Zweitstimmen"]:
            for (rk, vt, party), value in party_map.items():
                if rk != land["row_key"] or vt != vote:
                    continue
                total = sum(party_map.get((r["row_key"], vote, party), 0) for r in group)
                checks.append(dict(level=level, field=vote + ":" + party, sum=total, land=value, delta=total-value))
    issues.extend(f"Aggregation mismatch: {r['level']} {r['field']} delta={r['delta']}" for r in checks if r['delta'])
    for field, meta_field in [("reported_precincts", "csv_reported_precincts"), ("total_precincts", "csv_total_precincts")]:
        if land[field] != meta[meta_field]:
            issues.append(f"Metadata mismatch: {field}")
    if land["valid_votes_zweit"] is None or land["valid_votes_zweit"] <= 0:
        issues.append("No positive second-vote result")
    expected_constituencies = 78 if key == "2026-be" else 36
    if sum(r["gebietsart"] == "WAHLKREIS" for r in snapshots) != expected_constituencies:
        issues.append("Missing constituencies")
    reported, expected = land["reported_precincts"], land["total_precincts"]
    counters_complete = bool(expected and reported == expected)
    # Counter completeness must also hold below Land. MV's booth counter is inferred
    # from nonzero votes by the collector; use explicit constituency/municipal counters.
    counters_complete &= all(r["total_precincts"] and r["reported_precincts"] == r["total_precincts"]
                             for r in snapshots if r["gebietsart"] in {"WAHLKREIS", "GEMEINDE"})
    return dict(election_key=key, capture=meta["run_label"], as_of=meta["generated_at_utc"],
                reported_precincts=reported, total_precincts=expected,
                counting_complete=bool(counters_complete), reconciliation_passed=not issues,
                ready=bool(counters_complete and not issues), issues=issues, checks=checks), land, party_map
