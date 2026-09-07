#!/usr/bin/env python3
"""Offline audit of immutable LSA result versions; never fetches or edits inputs.

Two independent views are retained: the published normalized exports and the
original official CSVs (including U/B subtotals). Missing is never zero. Git
first-parent order defines observations; acquisition times label observations.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
BASE = "data/2026-lsa/latest/"
BERLIN = ZoneInfo("Europe/Berlin")
METRICS = ["reported_precincts", "total_precincts", "voters_total",
           "valid_votes_erst", "valid_votes_zweit"]
RAW_METRICS = {
    "Ist.Wahlbezirke": "reported_precincts", "Soll.Wahlbezirke": "total_precincts",
    "B.Wähler": "voters_total", "A.Wahlberechtigte": "eligible_voters",
    "D.Gültige.Erststimmen": "valid_votes_erst", "F.Gültige.Zweitstimmen": "valid_votes_zweit",
    "C.Ungültige.Erststimmen": "invalid_votes_erst", "E.Ungültige.Zweitstimmen": "invalid_votes_zweit",
}
LEVELS = {"LAN": "LAND", "KRS": "KREIS", "WKR": "WAHLKREIS", "GEM": "GEMEINDE"}


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def number(value):
    if value is None or str(value).strip() == "":
        return None
    if not re.fullmatch(r"-?\d+", str(value).strip()):
        raise ValueError(f"Not an integer: {value!r}")
    return int(value)


def csv_rows(content, delimiter=","):
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), delimiter=delimiter)
    fields = reader.fieldnames or []
    if len(fields) != len(set(fields)):
        raise ValueError("Duplicate CSV columns")
    rows = list(reader)
    if any(None in r or any(v is None for v in r.values()) for r in rows):
        raise ValueError("Truncated or overlong CSV row")
    return rows


def json_text(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def write_json(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def write_csv(path, rows, fields=None):
    fields = fields or list(dict.fromkeys(k for r in rows for k in r))
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    content = buffer.getvalue().encode()
    if path.suffix == ".gz":
        content = gzip.compress(content, mtime=0)
    path.write_bytes(content)


def numeric_values(row):
    return {**{k: row.get(k) for k in METRICS}, **row.get("extra", {}),
            **{f"party:{k}": v for k, v in row.get("parties", {}).items()}}


def is_complete(row):
    return row.get("total_precincts") is not None and row["total_precincts"] > 0 and row.get("reported_precincts") == row["total_precincts"]


def has_data(row):
    return any((row.get(k) or 0) > 0 for k in ("reported_precincts", "voters_total", "valid_votes_zweit", "valid_votes_erst"))


def compare_rows(previous, current):
    """Return semantic transitions, including disappeared and reappearing keys."""
    events = []
    for key in sorted(previous.keys() | current.keys()):
        old, new = previous.get(key), current.get(key)
        if old is None:
            events.append({"key": key, "event": "row_appeared", "changes": {}, "candidate": False})
            continue
        if new is None:
            events.append({"key": key, "event": "row_disappeared", "changes": {}, "candidate": has_data(old)})
            continue
        before, after = numeric_values(old), numeric_values(new)
        changes = {f: {"before": before.get(f), "after": after.get(f),
                       "delta": after[f] - before[f] if before.get(f) is not None and after.get(f) is not None else None}
                   for f in sorted(before.keys() | after.keys()) if before.get(f) != after.get(f)}
        if not changes:
            continue
        if not has_data(old) and not old.get("total_precincts") and (new.get("total_precincts") or 0) > 0:
            events.append({"key": key, "event": "reporting_universe_initialized", "changes": changes, "candidate": False})
            continue
        flags = []
        if any(v["before"] is not None and v["after"] is None for v in changes.values()):
            flags.append("value_became_missing")
        if has_data(old) and not has_data(new):
            flags.append("data_reset")
        if any(v["delta"] is not None and v["delta"] < 0 for v in changes.values()):
            flags.append("numeric_decrease")
        if "reported_precincts" in changes and (changes["reported_precincts"]["delta"] or 0) < 0:
            flags.append("reported_count_decrease")
        if old.get("total_precincts") is not None and new.get("total_precincts") is not None and old["total_precincts"] != new["total_precincts"]:
            flags.append("denominator_change")
        vote_change = any(f not in ("reported_precincts", "total_precincts", "eligible_voters") for f in changes)
        if vote_change and old.get("reported_precincts") == new.get("reported_precincts") and old.get("reported_precincts") is not None and has_data(old):
            flags.append("revision_at_fixed_reporting_count")
        if vote_change and is_complete(old):
            flags.append("revision_after_complete")
        events.append({"key": key, "event": "|".join(flags) or "reporting_growth",
                       "changes": changes, "candidate": bool(flags)})
    return events


def overview_rows(content):
    for attrs, body in re.findall(r"<script([^>]*)>(.*?)</script>", content.decode("utf-8-sig"), re.S | re.I):
        if "application/json" not in attrs.lower():
            continue
        widget = json.loads(body)
        data = widget.get("x", {}).get("tag", {}).get("attribs", {}).get("data", {})
        if not isinstance(data, dict) or "wbz_ist" not in data:
            continue
        required = ["Wahlkreis", "Landkreis", "Gemeinde", "wbz", "wbz_ist", "wbz_soll"]
        n = len(data["wbz_ist"])
        if any(k not in data or len(data[k]) != n for k in required):
            raise ValueError("Inconsistent overview schema")
        result = {}
        for i in range(n):
            r = {k: v[i] for k, v in data.items() if isinstance(v, list) and len(v) == n}
            # The overview publishes no AGS. Retain all four geographic labels;
            # never key on the polling-place number alone or on row position.
            key = json_text([r[k] for k in ["Wahlkreis", "Landkreis", "Gemeinde", "wbz"]])
            if key in result:
                raise ValueError(f"Duplicate overview identity {key}")
            result[key] = {"key": key, "name": f'{r["Gemeinde"]} / {r["wbz"]}',
                           "level": "WAHLBEZIRK_STATUS", "reported_precincts": number(r["wbz_ist"]),
                           "total_precincts": number(r["wbz_soll"]), **r}
        return result
    raise ValueError("No district status table in archived overview")


def raw_rows(content):
    result = {}
    for r in csv_rows(content, ";"):
        if "Satzart" in r:
            level = LEVELS.get(r["Satzart"])
            if level is None:
                continue
            ident = r["Schlüsselnummer"]
            area_key = f"lsa:{level}:{ident}"
            name = r["Name"]
        elif {"Wahlkreisnummer", "Gemeindeschlüssel", "Wahlbezirk"}.issubset(r):
            level = "WAHLBEZIRK"
            ident = r["Wahlbezirk"]
            area_key = f'lsa:WAHLBEZIRK:{r["Gemeindeschlüssel"]}:{int(r["Wahlkreisnummer"])}:{ident}'
            name = r.get("Gemeindename", "") + " / " + ident
        else:
            continue
        mode = r.get("Wahllokal", "").strip() or "TOTAL"
        key = area_key + ":" + mode
        if key in result:
            raise ValueError(f"Duplicate raw key: {key}")
        values = {v: number(r.get(k)) for k, v in RAW_METRICS.items()}
        if level == "WAHLBEZIRK":
            values.update(reported_precincts=1, total_precincts=1)
        parties, labels = {}, {}
        for k, value in r.items():
            match = re.fullmatch(r"([DF])(\d+)\.(.+)", k)
            if match:
                code = match[1] + str(int(match[2]))
                parties[code] = number(value)
                labels[code] = match[3]
        result[key] = {"key": key, "area_key": area_key, "level": level, "name": name,
                       "number": ident, "mode": mode, "result_type": r.get("Ergebnisart"),
                       "source_time_local": r.get("Datum", "") + " " + r.get("Uhrzeit", ""),
                       **{k: values[k] for k in METRICS},
                       "extra": {k: v for k, v in values.items() if k not in METRICS},
                       "parties": parties, "party_names": labels}
    return result


def normalized_rows(snapshots, parties):
    result = {}
    for r in csv_rows(snapshots):
        k = r["row_key"]
        if k in result:
            raise ValueError(f"Duplicate normalized area {k}")
        result[k] = {"key": k, "level": r["gebietsart"], "name": r["municipality_name"],
                     "number": r["gebietsnummer"], "ags": r["ags"],
                     **{f: number(r.get(f)) for f in METRICS}, "parties": {}, "party_names": {}}
    for r in csv_rows(parties):
        k, code = r["row_key"], r["party_key"]
        if k not in result or code in result[k]["parties"]:
            raise ValueError(f"Orphan/duplicate normalized party {k}/{code}")
        result[k]["parties"][code] = number(r["votes"])
        result[k]["party_names"][code] = r["party_name"]
    return result


def raw_vote_view(raw):
    """Choose combined rows, or combine U/B only for the separate booth schema."""
    grouped = defaultdict(list)
    for row in raw.values():
        grouped[row["area_key"]].append(row)
    result = {}
    for key, rows in grouped.items():
        combined = [r for r in rows if r["mode"] == "TOTAL"]
        if combined:
            result[key] = combined[0]
        elif rows[0]["level"] == "WAHLBEZIRK":
            codes = set().union(*(r["parties"] for r in rows))
            def sum_known(values):
                return sum(v for v in values if v is not None) if any(v is not None for v in values) else None
            result[key] = {**rows[0], "reported_precincts": 1, "total_precincts": 1,
                           **{f: sum_known([r[f] for r in rows]) for f in METRICS[2:]},
                           "parties": {c: sum_known([r["parties"].get(c) for r in rows]) for c in codes}}
        else:
            raise ValueError(f"Missing combined area row: {key}")
    return result


def arithmetic_issues(rows):
    issues = []
    for k, r in rows.items():
        vals = numeric_values(r)
        for field, v in vals.items():
            if v is not None and v < 0:
                issues.append({"key": k, "check": "negative", "field": field, "delta": v})
        actual, total = r.get("reported_precincts"), r.get("total_precincts")
        if actual is not None and total is not None and actual > total:
            issues.append({"key": k, "check": "reported_exceeds_total", "field": "reported_precincts", "delta": actual-total})
        eligible = r.get('extra', {}).get('eligible_voters')
        if eligible and r.get('mode') != 'B' and is_complete(r) and r.get('voters_total') is not None and r['voters_total'] > eligible:
            issues.append({'key': k, 'check': 'voters_exceed_eligible', 'field': 'voters_total', 'delta': r['voters_total']-eligible})
        for prefix, vote in [("D", "erst"), ("F", "zweit")]:
            valid = r.get("valid_votes_" + vote)
            values = [v for p, v in r["parties"].items() if p.startswith(prefix) and v is not None]
            if valid is not None and sum(values) != valid:
                issues.append({"key": k, "check": "party_sum", "field": vote, "delta": sum(values)-valid})
            voters = r.get("voters_total")
            invalid = r.get("extra", {}).get("invalid_votes_" + vote)
            if valid is not None and voters is not None and valid > voters:
                issues.append({"key": k, "check": "valid_exceeds_voters", "field": vote, "delta": valid-voters})
            if valid is not None and voters is not None and invalid is not None and valid+invalid != voters:
                issues.append({"key": k, "check": "ballot_identity", "field": vote, "delta": valid+invalid-voters})
    return issues


def aggregation_checks(rows):
    """Return every comparison, including zero residuals; overlapping levels never sum."""
    checks = []
    land = rows.get("lsa:LAND:15")
    if not land:
        return checks
    groups = [(f"{level}->LAND", land, [r for r in rows.values() if r["level"] == level])
              for level in ("GEMEINDE", "KREIS", "WAHLKREIS")]
    for parent in rows.values():
        if parent["level"] == "KREIS":
            children = [r for r in rows.values() if r["level"] == "GEMEINDE" and r["number"].startswith(parent["number"])]
            groups.append(("GEMEINDE->KREIS", parent, children))
    booths = [r for r in rows.values() if r["level"] == "WAHLBEZIRK"]
    if booths:
        groups.append(("WAHLBEZIRK->LAND", land, booths))
        for parent in rows.values():
            if parent["level"] == "GEMEINDE":
                children = [r for r in booths if r["key"].split(":")[2] == parent["number"]]
            elif parent["level"] == "WAHLKREIS":
                children = [r for r in booths if int(r["key"].split(":")[3]) == int(parent["number"])]
            else:
                continue
            groups.append(("WAHLBEZIRK->"+parent["level"], parent, children))
    for relation, parent, children in groups:
        for f, target in numeric_values(parent).items():
            if f in ("eligible_voters", "invalid_votes_erst", "invalid_votes_zweit"):
                continue
            values = [numeric_values(c).get(f) for c in children]
            missing = sum(v is None for v in values)
            # A blank candidate cell means not standing, but a missing metric
            # cannot be interpreted as zero. Party absence is counted explicitly.
            delta = sum(v or 0 for v in values) - target if target is not None and (not missing or f.startswith("party:")) else None
            checks.append({"relation": relation, "parent": parent["key"], "name": parent["name"],
                           "field": f, "child_count": len(children), "missing_children": missing,
                           "children_sum": sum(v or 0 for v in values), "parent_value": target, "delta": delta})
    return checks


def run(ref="HEAD", output=None, require_complete=False, full_history=False):
    end_ref = git("rev-parse", ref + "^{commit}").decode().strip()
    output = Path(output or ROOT / "data/2026-lsa/reports/git-timeline" / end_ref[:8]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    logs = git("log", "--first-parent", "--reverse", "--format=%H\t%cI\t%s", end_ref, "--", BASE).decode().splitlines()
    versions, provenance, normalized_timeline, party_timeline, raw_timeline, status_timeline = [], [], [], [], [], []
    events, raw_events, status_events, issues, aggregations, mode_checks, replay_diffs = [], [], [], [], [], [], []
    previous, previous_raw, previous_status = None, None, None
    previous_info, previous_status_info = None, None
    previously_seen, previously_seen_status = set(), set()
    unique_payloads, unique_raw_payloads = set(), set()
    latest_raw, latest_status = {}, {}
    inventory, captures_without_raw, schema_events, turnout_denominators = [], [], [], []
    previous_schemas = {}
    for line in logs:
        commit, committed, subject = line.split("\t", 2)
        files = set(git("ls-tree", "-r", "--name-only", commit, BASE).decode().splitlines())
        if BASE + "run_metadata.json" not in files:
            inventory.append({"commit":commit,"commit_time":committed,"phase":"no_metadata","included":False})
            continue
        read = lambda path: git("show", f"{commit}:{BASE}{path}")
        meta = json.loads(read("run_metadata.json"))
        acquired = meta.get("generated_at_utc")
        preopening = bool(acquired and datetime.fromisoformat(acquired).astimezone(BERLIN) < datetime(2026, 9, 6, 18, tzinfo=BERLIN))
        record = {"commit":commit,"commit_time":committed,"acquired_at_utc":acquired,
                  "phase":"setup" if not acquired else "pre_opening" if preopening else "election_night",
                  "included":bool(acquired and (full_history or not preopening)),"note":meta.get("statla_error")}
        inventory.append(record)
        inventory_record = record
        if not acquired or (preopening and not full_history):
            continue
        if meta.get("statla_error"):
            raise ValueError(f"Archived failed capture at {commit}: {meta['statla_error']}")
        snap_bytes, party_bytes = read("statla_snapshots.csv"), read("statla_party_results.csv")
        current = normalized_rows(snap_bytes, party_bytes)
        record["normalized_area_rows"] = len(current)
        record["areas_with_data"] = sum(has_data(r) for r in current.values())
        info = {"commit": commit, "acquired_at_utc": acquired,
                "acquired_at_local": datetime.fromisoformat(acquired).astimezone(BERLIN).isoformat(),
                "commit_time": committed, "subject": subject}
        manifest_path = "official_sources/manifest.json"
        manifest = json.loads(read(manifest_path)) if BASE+manifest_path in files else {"fetches": []}
        raw, status, source_content_hashes = {}, None, []
        for fetch in manifest["fetches"]:
            filename = fetch["filename"]
            file_path = "official_sources/" + filename
            retained = BASE + file_path in files
            record = {**info, **fetch, "retained": retained}
            provenance.append(record)
            if not retained:
                record["verified"] = False
                continue
            data = read(file_path)
            digest = hashlib.sha256(data).hexdigest()
            record["verified"] = digest == fetch["content_hash"]
            if not record["verified"]:
                raise ValueError(f"Source checksum mismatch at {commit}/{filename}")
            source_content_hashes.append(digest)
            if filename.endswith(".csv"):
                records = csv_rows(data, ";")
                schema = {"columns": list(records[0]) if records else [],
                          "result_types": sorted({r.get("Ergebnisart", "") for r in records})}
                old_schema = previous_schemas.get(filename)
                if old_schema != schema:
                    schema_events.append({**info, "filename": filename, "url": fetch.get("url"),
                                          "first_observation": old_schema is None,
                                          "before": old_schema, "after": schema,
                                          "rows": len(records)})
                previous_schemas[filename] = schema
                parsed = raw_rows(data)
                if raw.keys() & parsed.keys():
                    raise ValueError("Overlapping official source identities")
                raw.update(parsed)
            if filename == "overview.html":
                status = overview_rows(data)
        inventory_record["retained_raw_area_rows"] = len(raw)
        if not raw and not (full_history and preopening and not any(has_data(r) for r in current.values())):
            raise ValueError(f"No replayable official results at {commit}")
        if not raw:
            captures_without_raw.append({**info,"reason":"Pre-election zero template: normalized exports retained, original CSV source bytes not retained."})
        unique_raw_payloads.add(hashlib.sha256(json_text(raw).encode()).hexdigest())
        unique_payloads.add(hashlib.sha256(json_text(current).encode()).hexdigest())
        land = current["lsa:LAND:15"]
        version = {**info, **{f: land[f] for f in METRICS},
                   "coverage_percent": 100*land["reported_precincts"]/land["total_precincts"] if land["total_precincts"] else None,
                   "area_rows": len(current), "raw_rows": len(raw),
                   "overview_rows": len(status) if status is not None else None,
                   "overview_reported": sum(r["reported_precincts"] for r in status.values()) if status is not None else None,
                   "overview_minus_csv": sum(r["reported_precincts"] for r in status.values())-land["reported_precincts"] if status is not None and land["reported_precincts"] is not None else None,
                   "wahlbezirk_vote_rows": sum(r["level"] == "WAHLBEZIRK" for r in current.values()),
                   "mode": meta.get("statla_mode"), "dynamic_results_used": meta.get("dynamic_results_used_for_normalized_results")}
        versions.append(version)
        for key, row in current.items():
            normalized_timeline.append({**info, **{k: v for k, v in row.items() if k not in ("parties", "party_names")}})
            for code, votes in row["parties"].items():
                party_timeline.append({"commit": commit, "key": key, "party_code": code,
                                       "party": row["party_names"][code], "votes": votes})
        raw_timeline.extend({"commit": commit, **row} for row in raw.values())
        for source_name, rows in [("normalized", current), ("official_csv", raw)]:
            issues.extend({**info, "source": source_name, **i} for i in arithmetic_issues(rows))
        for row in raw.values():
            eligible = row.get('extra', {}).get('eligible_voters')
            if eligible and row['mode'] != 'B' and row.get('voters_total') is not None and row['voters_total'] > eligible:
                turnout_denominators.append({**info,'key':row['key'],'name':row['name'],'mode':row['mode'],
                    'reported':row['reported_precincts'],'total':row['total_precincts'],
                    'eligible_published':eligible,'voters_published':row['voters_total'],'complete':is_complete(row),
                    'interpretation':'Partial reporting may combine postal voters with the electorate of only the reported in-person booths; B/A is not a full-electorate turnout rate.'})
        aggregations.extend({**info, **a} for a in aggregation_checks(current))
        for key, row in raw.items():
            if row["mode"] != "TOTAL":
                continue
            u, b = raw.get(row["area_key"]+":U"), raw.get(row["area_key"]+":B")
            if u and b:
                for f, total in numeric_values(row).items():
                    a, z = numeric_values(u).get(f), numeric_values(b).get(f)
                    if total is not None and a is not None and z is not None:
                        mode_checks.append({"commit": commit, "key": row["area_key"], "field": f, "delta": a+z-total})
        source_view = raw_vote_view(raw)
        for key in sorted(current.keys() | source_view.keys()) if raw else []:
            normalized, row = current.get(key), source_view.get(key)
            if normalized is None or row is None:
                replay_diffs.append({**info, "key": key, "field": "area_presence", "raw": row is not None, "normalized": normalized is not None})
            else:
                replay_fields = set(METRICS) | {"party:"+p for p in normalized["parties"].keys() | row["parties"].keys()}
                for f in sorted(replay_fields):
                    v = numeric_values(normalized).get(f)
                    expected = numeric_values(row).get(f)
                    if expected != v:
                        replay_diffs.append({**info, "key": key, "field": f, "raw": expected, "normalized": v,
                                             "dynamic_results_used": version["dynamic_results_used"]})
        if previous is not None:
            for source, before, after, target in [("normalized", previous, current, events), ("official_csv", previous_raw, raw, raw_events)]:
                if source == "official_csv" and (not before or not after):
                    continue
                for e in compare_rows(before, after):
                    row = after.get(e["key"], before.get(e["key"]))
                    if source == "normalized" and e["event"] == "row_appeared" and e["key"] in previously_seen:
                        e["event"] = "row_reappeared"
                    target.append({**info, "previous_commit": previous_info["commit"], "previous_acquired_at_local": previous_info["acquired_at_local"],
                                   "source": source, "level": row["level"], "name": row["name"],
                                   "mode": row.get("mode", "TOTAL"), "previous_reported": before.get(e["key"], {}).get("reported_precincts"),
                                   "reported": row.get("reported_precincts"), "total": row.get("total_precincts"), **e})
        if status is not None:
            if previous_status is not None:
                for e in compare_rows(previous_status, status):
                    row = status.get(e["key"], previous_status.get(e["key"]))
                    if e["event"] == "row_appeared" and e["key"] in previously_seen_status:
                        e["event"] = "row_reappeared"
                    status_events.append({**info, "previous_commit": previous_status_info["commit"],
                                          "previous_acquired_at_local": previous_status_info["acquired_at_local"], "name": row["name"], **e})
            status_timeline.extend({"commit": commit, **row} for row in status.values())
            previous_status, previous_status_info = status, info
            previously_seen_status.update(status)
            latest_status = status
        previous, previous_raw, previous_info = current, raw, info
        previously_seen.update(current)
        latest_raw = raw
    if not versions:
        raise ValueError("No election-night versions in selected history")
    end, land = versions[-1], previous["lsa:LAND:15"]
    major_codes = sorted([p for p, v in land["parties"].items() if p.startswith("F") and v is not None and v/land["valid_votes_zweit"] > .05],
                         key=lambda p: land["parties"][p], reverse=True)
    selected = [{"code": p, "party": land["party_names"][p], "votes": land["parties"][p],
                 "share_percent": 100*land["parties"][p]/land["valid_votes_zweit"]} for p in major_codes]
    candidate_events = [e for e in events if e["candidate"]]
    revisions = [e for e in events if "revision_at_fixed_reporting_count" in e["event"]]
    municipality_revisions = [e for e in revisions if e["level"] == "GEMEINDE"]
    missing = [{**r, "commit": end["commit"]} for r in latest_status.values() if r["reported_precincts"] < r["total_precincts"]]
    overview_versions = [v for v in versions if v["overview_rows"] is not None]
    final_checks = aggregation_checks(previous)
    municipalities = [r for r in previous.values() if r['level'] == 'GEMEINDE']
    booth_count = sum(r['level'] == 'WAHLBEZIRK' for r in previous.values())
    evidence = {
        'land_csv_reported': land['reported_precincts'], 'land_csv_total': land['total_precincts'],
        'html_reported': sum(r['reported_precincts'] for r in latest_status.values()),
        'html_total': sum(r['total_precincts'] for r in latest_status.values()),
        'municipality_reported': sum(r['reported_precincts'] for r in municipalities),
        'municipality_total': sum(r['total_precincts'] for r in municipalities),
        'individual_vote_rows': booth_count,
        'final_aggregation_nonzero': sum(a['delta'] not in (0, None) for a in final_checks),
        'final_vote_comparisons_unavailable': sum(a['delta'] is None for a in final_checks if a['field'] not in METRICS[:2]),
    }
    evidence['complete_reconciled'] = bool(evidence['html_total'] > 0 and
        evidence['html_reported'] == evidence['html_total'] == evidence['municipality_reported'] == evidence['municipality_total'] == booth_count and
        evidence['final_aggregation_nonzero'] == evidence['final_vote_comparisons_unavailable'] == 0)
    status_losses = []
    for e in status_events:
        if "reported_count_decrease" not in e["event"]:
            continue
        observations = [r for r in status_timeline if r["key"] == e["key"]]
        event_index = next(i for i, r in enumerate(observations) if r["commit"] == e["commit"])
        restored = next((r for r in observations[event_index+1:] if r["reported_precincts"] > 0), None)
        restored_info = next((v for v in versions if restored and v["commit"] == restored["commit"]), None)
        status_losses.append({**e, "restored_capture": restored_info})
    summary = {
        "election": "Landtagswahl Sachsen-Anhalt 2026", "ref": end_ref,
        "full_history":full_history,"history_commit_count":len(logs),
        "setup_commit_count":sum(r['phase']=='setup' for r in inventory),
        "preopening_captures":sum(r['phase']=='pre_opening' and r['included'] for r in inventory),
        "election_night_captures":sum(r['phase']=='election_night' for r in inventory),
        "captures_without_raw_sources":captures_without_raw,
        "first_capture": versions[0], "last_capture": end, "versions": len(versions),
        "unique_normalized_versions": len(unique_payloads), "unique_raw_versions": len(unique_raw_payloads),
        "levels_latest": dict(Counter(r["level"] for r in previous.values())),
        "selected_parties": selected, "selection": "Strictly >5% of valid Land second votes at frozen cutoff; fixed cohort throughout timeline",
        "status_observations": len(overview_versions), "status_first_capture": overview_versions[0] if overview_versions else None,
        "status_last_capture": overview_versions[-1] if overview_versions else None,
        "status_candidate_events": [e for e in status_events if e["candidate"]],
        "status_losses": status_losses,
        "status_identity_additions":[e for e in status_events if e['event']=='row_appeared'],
        "status_identity_removals":[e for e in status_events if e['event']=='row_disappeared'],
        "status_identity_reappearances":[e for e in status_events if e['event']=='row_reappeared'],
        "denominator_changes":[e for e in events if 'denominator_change' in e['event']],
        "overview_csv_comparable_captures": sum(v["overview_minus_csv"] is not None for v in overview_versions),
        "overview_csv_mismatching_captures": sum(v["overview_minus_csv"] not in (0, None) for v in overview_versions),
        "area_candidate_count": len(candidate_events), "area_revision_fixed_count": len(revisions),
        "municipality_revisions": municipality_revisions,
        "raw_municipality_candidates": [e for e in raw_events if e["candidate"] and e["level"] == "GEMEINDE" and e["mode"] == "TOTAL"],
        "area_disappearances": [e for e in events if e["event"] == "row_disappeared"],
        "area_resets": [e for e in events if "data_reset" in e["event"]],
        "area_reporting_decreases": [e for e in events if "reported_count_decrease" in e["event"]],
        "area_value_missing": [e for e in events if "value_became_missing" in e["event"]],
        "arithmetic_issues": issues, "source_replay_differences": replay_diffs,
        "aggregation_checks": len(aggregations), "aggregation_nonzero": sum(a["delta"] not in (None, 0) for a in aggregations),
        "mode_checks": len(mode_checks), "mode_nonzero": sum(a["delta"] != 0 for a in mode_checks),
        "retained_sources_verified": sum(p.get("verified", False) for p in provenance),
        "unretained_source_records": sum(not p["retained"] for p in provenance),
        "missing_status_rows": len(missing), "missing_precincts": land["total_precincts"]-land["reported_precincts"] if land['total_precincts'] is not None and land['reported_precincts'] is not None else None,
        "reporting_evidence": evidence,
        "turnout_denominator_observations":len(turnout_denominators),
        "turnout_denominator_areas":len({x['key'] for x in turnout_denominators}),
        "max_capture_gap_minutes": max((datetime.fromisoformat(b["acquired_at_utc"])-datetime.fromisoformat(a["acquired_at_utc"])).total_seconds()/60 for a,b in zip(versions, versions[1:])),
        "analysis_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python_version": sys.version.split()[0],
    }
    write_json(output/"summary.json", summary)
    write_json(output/"source_manifest.json", provenance)
    write_json(output/"source_schema_events.json", schema_events)
    write_json(output/"turnout_denominator_observations.json", turnout_denominators)
    write_csv(output/"history_inventory.csv", inventory)
    write_json(output/"latest_areas.json", previous)
    write_json(output/"latest_official_rows.json", latest_raw)
    write_csv(output/"versions.csv", versions)
    write_csv(output/"area_timeline.csv.gz", normalized_timeline)
    write_csv(output/"party_timeline.csv.gz", party_timeline)
    for filename, rows in [("raw_timeline.jsonl.gz", raw_timeline), ("status_timeline.jsonl.gz", status_timeline)]:
        (output/filename).write_bytes(gzip.compress(("\n".join(json_text(r) for r in rows)+"\n").encode(), mtime=0))
    for filename, rows in [("area_events.csv", events), ("candidate_events.csv", candidate_events), ("raw_candidate_events.csv", [e for e in raw_events if e["candidate"]]), ("status_events.csv", status_events)]:
        write_csv(output/filename, [{**e, "changes": json_text(e["changes"])} for e in rows],
                  fields=None if rows else ["commit", "previous_commit", "key", "event", "changes"])
    write_csv(output/"aggregation_checks.csv.gz", aggregations)
    write_csv(output/"aggregation_nonzero.csv", [a for a in aggregations if a["delta"] not in (None,0)], list(aggregations[0]))
    write_csv(output/"mode_nonzero.csv", [a for a in mode_checks if a["delta"] != 0], ["commit", "key", "field", "delta"])
    write_csv(output/"missing_wahlbezirke.csv", missing, list(next(iter(latest_status.values())))+["commit"] if latest_status else ["commit", "key"])
    print(json.dumps({k: summary[k] for k in ["ref", "versions", "levels_latest", "selected_parties", "area_candidate_count", "area_revision_fixed_count", "missing_precincts", "aggregation_nonzero", "mode_nonzero", "retained_sources_verified"]}, ensure_ascii=False, indent=2))
    print(f"Report evidence: {output}")
    if require_complete and not (is_complete(land) or evidence['complete_reconciled']):
        raise SystemExit(f"Incomplete snapshot: {land['reported_precincts']}/{land['total_precincts']}. Evidence retained; no 100% label allowed.")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", default="HEAD", help="Immutable commit or ref resolved once at start")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-complete", action="store_true", help="Exit nonzero below 100%; does not imply certified final results")
    parser.add_argument("--full-history", action="store_true", help="Inventory all LSA history and include pre-election templates; setup commits without captures stay in the inventory")
    args = parser.parse_args()
    run(args.ref, args.output, args.require_complete, args.full_history)


if __name__ == "__main__":
    main()
