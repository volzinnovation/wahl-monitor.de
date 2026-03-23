#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
BERLIN = ZoneInfo("Europe/Berlin")
BOOTH_GEBIETSART = {"URNENWAHLBEZIRK", "BRIEFWAHLBEZIRK"}


@dataclass(frozen=True)
class CommitInfo:
    commit: str
    committed_at_utc: str
    subject: str


def git_text(commit: str, rel_path: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"{commit}:{rel_path}"],
        cwd=ROOT,
        text=True,
    )


def load_csv_at_commit(commit: str, rel_path: str) -> List[Dict[str, str]]:
    return list(csv.DictReader(io.StringIO(git_text(commit, rel_path))))


def git_poll_commits(election_key: str) -> List[CommitInfo]:
    rel_paths = [
        f"data/{election_key}/latest/run_metadata.json",
        f"data/{election_key}/latest/statla_snapshots.csv",
        f"data/{election_key}/latest/statla_party_results.csv",
    ]
    raw = subprocess.check_output(
        ["git", "log", "--reverse", "--format=%H\t%cI\t%s", "--", *rel_paths],
        cwd=ROOT,
        text=True,
    )
    prefix = f"{election_key} poll "
    commits: List[CommitInfo] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        if not line.strip():
            continue
        commit, committed_at_utc, subject = line.split("\t", 2)
        if commit in seen or not subject.startswith(prefix):
            continue
        seen.add(commit)
        commits.append(CommitInfo(commit=commit, committed_at_utc=committed_at_utc, subject=subject))
    return commits


def local_label(utc_iso: str) -> str:
    dt = datetime.fromisoformat(utc_iso).astimezone(BERLIN)
    suffix = "MEZ" if dt.utcoffset() == timedelta(hours=1) else "MESZ"
    return dt.strftime(f"%Y-%m-%d %H:%M {suffix}")


def local_now_label() -> str:
    dt = datetime.now(BERLIN)
    suffix = "MEZ" if dt.utcoffset() == timedelta(hours=1) else "MESZ"
    return dt.strftime(f"%Y-%m-%d %H:%M {suffix}")


def normalized_ags(value: str) -> str:
    value = str(value or "").strip()
    if not value or value == "BW":
        return ""
    return value.zfill(8)


def load_dummy_mapping(election_key: str) -> Dict[Tuple[str, str, str], str]:
    path = ROOT / "data" / election_key / "metadata" / "2026021_LTW26-Dummy-Datei.csv"
    mapping: Dict[Tuple[str, str, str], str] = {}
    with path.open(encoding="latin1", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            key = (
                normalized_ags(row.get("AGS", "")),
                str(row.get("Gebietsart") or "").strip(),
                str(row.get("Gebietsnummer") or "").strip(),
            )
            mapping[key] = str(row.get("Wahlkreisnummer") or "").strip()
    return mapping


def load_latest_raw_mapping(election_key: str) -> Dict[Tuple[str, str, str], str]:
    raw_dir = ROOT / "data" / election_key / "raw" / "statla"
    raw_files = sorted(raw_dir.glob("*.csv"))
    if not raw_files:
        return {}
    path = raw_files[-1]
    mapping: Dict[Tuple[str, str, str], str] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            key = (
                normalized_ags(row.get("AGS", "")),
                str(row.get("Gebietsart") or "").strip(),
                str(row.get("Gebietsnummer") or "").strip(),
            )
            mapping[key] = str(row.get("Wahlkreisnummer") or "").strip()
    return mapping


def load_ags_to_wahlkreise(election_key: str) -> Dict[str, set[str]]:
    path = ROOT / "data" / election_key / "metadata" / "LTWahlkreise2026-BW-wkr_kr_gem.csv"
    ags_to_wks: Dict[str, set[str]] = defaultdict(set)
    with path.open(encoding="latin1") as handle:
        lines = [line for line in handle if not line.startswith("#") and line.strip() and line.strip() != ";;;;;;"]
    for row in csv.DictReader(lines, delimiter=";"):
        ags = normalized_ags(row.get("Gemeindekennziffer", ""))
        wk = str(row.get("Wahlkreisnummer") or "").strip()
        if ags and wk:
            ags_to_wks[ags].add(wk)
    return ags_to_wks


def load_wahlkreis_names(election_key: str) -> Dict[str, str]:
    path = ROOT / "data" / election_key / "metadata" / "LTWahlkreise2026-BW-wkr_kr_gem.csv"
    names: Dict[str, str] = {}
    with path.open(encoding="latin1") as handle:
        lines = [line for line in handle if not line.startswith("#") and line.strip() and line.strip() != ";;;;;;"]
    for row in csv.DictReader(lines, delimiter=";"):
        wk = str(row.get("Wahlkreisnummer") or "").strip()
        name = str(row.get("Wahlkreisname") or "").strip()
        if wk and name and wk not in names:
            names[wk] = name
    return names


def booth_wahlkreis(
    ags: str,
    gebietsart: str,
    gebietsnummer: str,
    key_to_wk: Dict[Tuple[str, str, str], str],
    ags_to_wks: Dict[str, set[str]],
) -> str:
    wk = key_to_wk.get((ags, gebietsart, gebietsnummer), "")
    if wk:
        return wk
    options = ags_to_wks.get(ags, set())
    if len(options) == 1:
        return next(iter(options))
    return ""


def build_party_index(rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, int]]:
    out: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    for row in rows:
        out[(row["row_key"], row["vote_type"])][row["party_name"]] = int(row["votes"] or 0)
    return out


def max_gap_party_name(aggregate: Dict[str, int], booth: Dict[str, int]) -> Tuple[str, int]:
    best_party = ""
    best_gap = 0
    for party in sorted(set(aggregate) | set(booth)):
        gap = aggregate.get(party, 0) - booth.get(party, 0)
        if abs(gap) > abs(best_gap):
            best_party = party
            best_gap = gap
    return best_party, best_gap


def sum_abs_gap(aggregate: Dict[str, int], booth: Dict[str, int]) -> int:
    return sum(abs(aggregate.get(party, 0) - booth.get(party, 0)) for party in set(aggregate) | set(booth))


def analyze_commit(
    commit: CommitInfo,
    election_key: str,
    key_to_wk: Dict[Tuple[str, str, str], str],
    ags_to_wks: Dict[str, set[str]],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    snapshots = load_csv_at_commit(commit.commit, f"data/{election_key}/latest/statla_snapshots.csv")
    party_rows = load_csv_at_commit(commit.commit, f"data/{election_key}/latest/statla_party_results.csv")
    party_index = build_party_index(party_rows)

    booth_valid_by_wk: Dict[str, Dict[str, int]] = defaultdict(lambda: {"erst": 0, "zweit": 0})
    booth_party_by_wk_vote: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    wk_rows: Dict[str, Dict[str, object]] = {}
    wk_party_by_vote: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    mapped_booth_rows_by_wk: Counter[str] = Counter()
    unmapped_booth_rows = 0

    for snapshot in snapshots:
        gebietsart = str(snapshot.get("gebietsart") or "").strip()
        if gebietsart == "WAHLKREIS":
            wk = str(snapshot.get("gebietsnummer") or "").strip()
            wk_rows[wk] = {
                "reported_precincts": int(snapshot["reported_precincts"] or 0),
                "total_precincts": int(snapshot["total_precincts"] or 0),
                "agg_valid_votes_erst": int(snapshot["valid_votes_erst"] or 0),
                "agg_valid_votes_zweit": int(snapshot["valid_votes_zweit"] or 0),
            }
            wk_party_by_vote[(wk, "Erststimmen")] = party_index.get((snapshot["row_key"], "Erststimmen"), {})
            wk_party_by_vote[(wk, "Zweitstimmen")] = party_index.get((snapshot["row_key"], "Zweitstimmen"), {})
            continue

        if gebietsart not in BOOTH_GEBIETSART:
            continue

        ags = normalized_ags(snapshot.get("ags") or "")
        wk = booth_wahlkreis(
            ags=ags,
            gebietsart=gebietsart,
            gebietsnummer=str(snapshot.get("gebietsnummer") or "").strip(),
            key_to_wk=key_to_wk,
            ags_to_wks=ags_to_wks,
        )
        if not wk:
            unmapped_booth_rows += 1
            continue

        mapped_booth_rows_by_wk[wk] += 1
        booth_valid_by_wk[wk]["erst"] += int(snapshot["valid_votes_erst"] or 0)
        booth_valid_by_wk[wk]["zweit"] += int(snapshot["valid_votes_zweit"] or 0)
        for party_name, votes in party_index.get((snapshot["row_key"], "Erststimmen"), {}).items():
            booth_party_by_wk_vote[(wk, "Erststimmen")][party_name] += votes
        for party_name, votes in party_index.get((snapshot["row_key"], "Zweitstimmen"), {}).items():
            booth_party_by_wk_vote[(wk, "Zweitstimmen")][party_name] += votes

    rows: List[Dict[str, object]] = []
    party_gap_rows: List[Dict[str, object]] = []
    commit_time_local = local_label(commit.committed_at_utc)
    for wk, wk_row in sorted(wk_rows.items(), key=lambda item: int(item[0])):
        aggregate_erst = wk_party_by_vote.get((wk, "Erststimmen"), {})
        aggregate_zweit = wk_party_by_vote.get((wk, "Zweitstimmen"), {})
        booth_erst = booth_party_by_wk_vote.get((wk, "Erststimmen"), {})
        booth_zweit = booth_party_by_wk_vote.get((wk, "Zweitstimmen"), {})

        abs_party_gap_erst = sum_abs_gap(aggregate_erst, booth_erst)
        abs_party_gap_zweit = sum_abs_gap(aggregate_zweit, booth_zweit)
        max_party_gap_erst_party, max_party_gap_erst = max_gap_party_name(aggregate_erst, booth_erst)
        max_party_gap_zweit_party, max_party_gap_zweit = max_gap_party_name(aggregate_zweit, booth_zweit)

        gap_erst = int(wk_row["agg_valid_votes_erst"]) - booth_valid_by_wk[wk]["erst"]
        gap_zweit = int(wk_row["agg_valid_votes_zweit"]) - booth_valid_by_wk[wk]["zweit"]

        rows.append(
            {
                "commit": commit.commit,
                "commit_time_utc": commit.committed_at_utc,
                "commit_time_local": commit_time_local,
                "subject": commit.subject,
                "wahlkreis": wk,
                "reported_precincts": int(wk_row["reported_precincts"]),
                "total_precincts": int(wk_row["total_precincts"]),
                "mapped_booth_rows": mapped_booth_rows_by_wk[wk],
                "unmapped_booth_rows_commit_total": unmapped_booth_rows,
                "booth_valid_votes_erst": booth_valid_by_wk[wk]["erst"],
                "agg_valid_votes_erst": int(wk_row["agg_valid_votes_erst"]),
                "valid_gap_erst": gap_erst,
                "booth_valid_votes_zweit": booth_valid_by_wk[wk]["zweit"],
                "agg_valid_votes_zweit": int(wk_row["agg_valid_votes_zweit"]),
                "valid_gap_zweit": gap_zweit,
                "abs_party_gap_erst": abs_party_gap_erst,
                "abs_party_gap_zweit": abs_party_gap_zweit,
                "max_party_gap_erst_party": max_party_gap_erst_party,
                "max_party_gap_erst": max_party_gap_erst,
                "max_party_gap_zweit_party": max_party_gap_zweit_party,
                "max_party_gap_zweit": max_party_gap_zweit,
                "is_inconsistent": int(
                    gap_erst != 0
                    or gap_zweit != 0
                    or abs_party_gap_erst != 0
                    or abs_party_gap_zweit != 0
                ),
            }
        )
        if gap_erst != 0 or gap_zweit != 0 or abs_party_gap_erst != 0 or abs_party_gap_zweit != 0:
            for vote_type, aggregate_votes, booth_votes in (
                ("Erststimmen", aggregate_erst, booth_erst),
                ("Zweitstimmen", aggregate_zweit, booth_zweit),
            ):
                for party_name in sorted(set(aggregate_votes) | set(booth_votes)):
                    party_gap = aggregate_votes.get(party_name, 0) - booth_votes.get(party_name, 0)
                    if party_gap == 0:
                        continue
                    party_gap_rows.append(
                        {
                            "commit": commit.commit,
                            "commit_time_utc": commit.committed_at_utc,
                            "commit_time_local": commit_time_local,
                            "subject": commit.subject,
                            "wahlkreis": wk,
                            "vote_type": vote_type,
                            "party_name": party_name,
                            "aggregate_votes": aggregate_votes.get(party_name, 0),
                            "booth_votes": booth_votes.get(party_name, 0),
                            "party_gap": party_gap,
                        }
                    )
    return rows, party_gap_rows


def summarize_wahlkreise(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    latest_by_wk: Dict[str, Dict[str, object]] = {}
    for row in rows:
        wk = str(row["wahlkreis"])
        latest_by_wk[wk] = row
        if int(row["is_inconsistent"]):
            grouped[wk].append(row)

    summary: List[Dict[str, object]] = []
    for wk, items in sorted(grouped.items(), key=lambda item: int(item[0])):
        latest = latest_by_wk[wk]
        summary.append(
            {
                "wahlkreis": wk,
                "inconsistent_commit_count": len(items),
                "first_inconsistent_time_local": items[0]["commit_time_local"],
                "last_inconsistent_time_local": items[-1]["commit_time_local"],
                "max_abs_valid_gap_erst": max(abs(int(item["valid_gap_erst"])) for item in items),
                "max_abs_valid_gap_zweit": max(abs(int(item["valid_gap_zweit"])) for item in items),
                "max_abs_party_gap_erst": max(int(item["abs_party_gap_erst"]) for item in items),
                "max_abs_party_gap_zweit": max(int(item["abs_party_gap_zweit"]) for item in items),
                "latest_valid_gap_erst": int(latest["valid_gap_erst"]),
                "latest_valid_gap_zweit": int(latest["valid_gap_zweit"]),
                "latest_abs_party_gap_erst": int(latest["abs_party_gap_erst"]),
                "latest_abs_party_gap_zweit": int(latest["abs_party_gap_zweit"]),
            }
        )
    return summary


def top_events(rows: List[Dict[str, object]], limit: int = 15) -> List[Dict[str, object]]:
    inconsistent = [row for row in rows if int(row["is_inconsistent"])]
    return sorted(
        inconsistent,
        key=lambda row: (
            max(abs(int(row["valid_gap_erst"])), abs(int(row["valid_gap_zweit"]))),
            int(row["abs_party_gap_erst"]) + int(row["abs_party_gap_zweit"]),
        ),
        reverse=True,
    )[:limit]


def largest_event(rows: List[Dict[str, object]]) -> Dict[str, object] | None:
    inconsistent = [row for row in rows if int(row["is_inconsistent"])]
    if not inconsistent:
        return None
    return max(
        inconsistent,
        key=lambda row: (
            max(abs(int(row["valid_gap_erst"])), abs(int(row["valid_gap_zweit"]))),
            int(row["abs_party_gap_erst"]) + int(row["abs_party_gap_zweit"]),
        ),
    )


def party_rows_for_event(
    party_gap_rows: List[Dict[str, object]],
    event_row: Dict[str, object] | None,
    vote_type: str,
) -> List[Dict[str, object]]:
    if event_row is None:
        return []
    rows = [
        row
        for row in party_gap_rows
        if row["commit"] == event_row["commit"]
        and row["wahlkreis"] == event_row["wahlkreis"]
        and row["vote_type"] == vote_type
    ]
    return sorted(rows, key=lambda row: abs(int(row["party_gap"])), reverse=True)


def latex_escape(value: object) -> str:
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def build_report_tex(
    rows: List[Dict[str, object]],
    summary_rows: List[Dict[str, object]],
    top_rows: List[Dict[str, object]],
    party_gap_rows: List[Dict[str, object]],
    election_key: str,
    wahlkreis_names: Dict[str, str],
) -> str:
    total_commits = len({str(row["commit"]) for row in rows})
    commits_with_inconsistency = len({str(row["commit"]) for row in rows if int(row["is_inconsistent"])})
    affected_wahlkreise = len(summary_rows)
    latest_rows = [row for row in rows if row["commit"] == rows[-1]["commit"]]
    latest_inconsistent = sum(int(row["is_inconsistent"]) for row in latest_rows)
    max_gap_row = largest_event(rows)
    max_gap_text = "Keine Inkonsistenzen gefunden."
    if max_gap_row is not None:
        max_gap_value = max(abs(int(max_gap_row["valid_gap_erst"])), abs(int(max_gap_row["valid_gap_zweit"])))
        max_gap_text = (
            f"Die groesste beobachtete Luecke lag bei {format_int(max_gap_value)} Stimmen in Wahlkreis "
            f"{latex_escape(max_gap_row['wahlkreis'])} am {latex_escape(max_gap_row['commit_time_local'])}."
        )

    summary_table = [
        r"\begingroup\small",
        r"\begin{longtable}{@{}rlrrrll@{}}",
        r"\toprule",
        r"WK & Name & Commits & Max Gap E & Max Gap Z & Erste Inkonsistenz & Letzte Inkonsistenz \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        r"WK & Name & Commits & Max Gap E & Max Gap Z & Erste Inkonsistenz & Letzte Inkonsistenz \\",
        r"\midrule",
        r"\endhead",
    ]
    for row in summary_rows:
        wk = str(row["wahlkreis"])
        summary_table.append(
            " & ".join(
                [
                    latex_escape(wk.zfill(2)),
                    latex_escape(wahlkreis_names.get(wk, "")),
                    latex_escape(row["inconsistent_commit_count"]),
                    latex_escape(format_int(int(row["max_abs_valid_gap_erst"]))),
                    latex_escape(format_int(int(row["max_abs_valid_gap_zweit"]))),
                    latex_escape(row["first_inconsistent_time_local"]),
                    latex_escape(row["last_inconsistent_time_local"]),
                ]
            )
            + r" \\"
        )
    summary_table.extend([r"\bottomrule", r"\end{longtable}", r"\endgroup"])

    top_table = [
        r"\begingroup\small",
        r"\begin{longtable}{@{}llrrrr@{}}",
        r"\toprule",
        r"Zeit & WK & Gap E & Gap Z & abs. Party Gap E & abs. Party Gap Z \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        r"Zeit & WK & Gap E & Gap Z & abs. Party Gap E & abs. Party Gap Z \\",
        r"\midrule",
        r"\endhead",
    ]
    for row in top_rows:
        top_table.append(
            " & ".join(
                [
                    latex_escape(row["commit_time_local"]),
                    latex_escape(str(row["wahlkreis"]).zfill(2)),
                    latex_escape(format_int(int(row["valid_gap_erst"]))),
                    latex_escape(format_int(int(row["valid_gap_zweit"]))),
                    latex_escape(format_int(int(row["abs_party_gap_erst"]))),
                    latex_escape(format_int(int(row["abs_party_gap_zweit"]))),
                ]
            )
            + r" \\"
        )
    top_table.extend([r"\bottomrule", r"\end{longtable}", r"\endgroup"])

    party_tables: List[str] = []
    if max_gap_row is not None:
        for vote_type, title in (
            ("Erststimmen", "Partei-Gaps beim groessten Ereignis (Erststimmen)"),
            ("Zweitstimmen", "Partei-Gaps beim groessten Ereignis (Zweitstimmen)"),
        ):
            event_party_rows = party_rows_for_event(party_gap_rows, max_gap_row, vote_type)
            if not event_party_rows:
                continue
            party_tables.extend(
                [
                    rf"\subsection*{{{title}}}",
                    r"\begingroup\small",
                    r"\begin{longtable}{@{}lrrr@{}}",
                    r"\toprule",
                    r"Partei & Aggregat & Booth-Summe & Gap \\",
                    r"\midrule",
                    r"\endfirsthead",
                    r"\toprule",
                    r"Partei & Aggregat & Booth-Summe & Gap \\",
                    r"\midrule",
                    r"\endhead",
                ]
            )
            for row in event_party_rows:
                party_tables.append(
                    " & ".join(
                        [
                            latex_escape(row["party_name"]),
                            latex_escape(format_int(int(row["aggregate_votes"]))),
                            latex_escape(format_int(int(row["booth_votes"]))),
                            latex_escape(format_int(int(row["party_gap"]))),
                        ]
                    )
                    + r" \\"
                )
            party_tables.extend([r"\bottomrule", r"\end{longtable}", r"\endgroup"])

    generated_at = local_now_label()
    extra_conclusion = ""
    if affected_wahlkreise == 1 and summary_rows:
        wk = str(summary_rows[0]["wahlkreis"])
        wk_name = wahlkreis_names.get(wk, "")
        extra_conclusion = (
            f"Betroffen war ausschliesslich Wahlkreis {latex_escape(wk.zfill(2))}"
            f" ({latex_escape(wk_name)}), und zwar nur zum Zeitpunkt "
            f"{latex_escape(summary_rows[0]['first_inconsistent_time_local'])}."
        )
    return "\n".join(
        [
            r"\documentclass[11pt,a4paper]{article}",
            r"\usepackage[utf8]{inputenc}",
            r"\usepackage[T1]{fontenc}",
            r"\usepackage[ngerman]{babel}",
            r"\usepackage{lmodern}",
            r"\usepackage{geometry}",
            r"\usepackage{booktabs}",
            r"\usepackage{longtable}",
            r"\usepackage{array}",
            r"\usepackage{hyperref}",
            r"\geometry{margin=2.2cm}",
            r"\setlength{\parindent}{0pt}",
            r"\setlength{\parskip}{0.7em}",
            r"\begin{document}",
            r"\begin{center}",
            r"{\LARGE Tempor\"are Inkonsistenzen zwischen Wahlkreisaggregaten und Einzelzeilen in den StatLA-Daten}\\[0.6em]",
            rf"{{\large Wahl: {latex_escape(election_key)}}}\\[0.4em]",
            rf"{{\small Stand des Reports: {latex_escape(generated_at)}}}",
            r"\end{center}",
            r"\section*{Fragestellung}",
            (
                "Untersucht wird, ob es in der Git-Historie der StatLA-Exporte zeitweise Inkonsistenzen zwischen "
                "Wahlkreisaggregaten und den zugeordneten Einzelzeilen gab. Als Inkonsistenz gilt hier, dass die "
                "gueltigen Erst- oder Zweitstimmen eines Wahlkreisaggregats nicht mit der Summe der zugeordneten "
                "Urnen- und Briefwahlbezirke uebereinstimmen oder dass sich die Parteisummen unterscheiden."
            ),
            r"\section*{Methode}",
            (
                "Fuer jeden Poll-Commit wurden die historisierten StatLA-Snapshots und Parteitabellen geladen. "
                "Die Zuordnung von Wahlbezirken zu Wahlkreisen erfolgt ueber die StatLA-Dummy-Datei, die letzte "
                "lokal verfuegbare rohe StatLA-CSV und die offizielle Gemeindenzuordnung der Wahlkreise. "
                "Fuer jeden Wahlkreis wurden die gueltigen Stimmen und Parteisummen der Einzelzeilen aufaddiert "
                "und mit dem jeweiligen Wahlkreisaggregat verglichen."
            ),
            r"\section*{Kurzfazit}",
            (
                f"Analysiert wurden {format_int(total_commits)} Poll-Commits. In {format_int(commits_with_inconsistency)} "
                f"Commits trat mindestens ein inkonsistenter Wahlkreis auf. Insgesamt waren {format_int(affected_wahlkreise)} "
                f"Wahlkreise zeitweise betroffen. Im letzten analysierten Commit waren noch {format_int(latest_inconsistent)} "
                "Wahlkreise inkonsistent."
            ),
            extra_conclusion,
            max_gap_text,
            r"\section*{Betroffene Wahlkreise}",
            *summary_table,
            r"\section*{Groesste Einzelereignisse}",
            *top_table,
            r"\section*{Partei-Aufschluesselung}",
            *party_tables,
            r"\section*{Dateien}",
            (
                "Der Report wird zusammen mit drei CSV-Dateien geschrieben: einer Detaildatei ueber alle "
                "Commit-Wahlkreis-Kombinationen, einer verdichteten Summary-Datei je Wahlkreis und einer "
                "Partei-Aufschluesselung fuer alle inkonsistenten Ereignisse."
            ),
            r"\end{document}",
        ]
    )


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--election-key", default="2026-bw")
    parser.add_argument("--output-detail-csv")
    parser.add_argument("--output-summary-csv")
    parser.add_argument("--output-party-csv")
    parser.add_argument("--output-tex")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report_dir = ROOT / "data" / args.election_key / "reports"
    detail_csv = Path(args.output_detail_csv) if args.output_detail_csv else report_dir / "statla_wahlkreis_consistency_over_time.csv"
    summary_csv = Path(args.output_summary_csv) if args.output_summary_csv else report_dir / "statla_wahlkreis_consistency_summary.csv"
    party_csv = Path(args.output_party_csv) if args.output_party_csv else report_dir / "statla_wahlkreis_consistency_party_breakdown.csv"
    output_tex = Path(args.output_tex) if args.output_tex else report_dir / "statla_wahlkreis_consistency_report.tex"

    key_to_wk = load_dummy_mapping(args.election_key)
    key_to_wk.update(load_latest_raw_mapping(args.election_key))
    ags_to_wks = load_ags_to_wahlkreise(args.election_key)
    wahlkreis_names = load_wahlkreis_names(args.election_key)

    all_rows: List[Dict[str, object]] = []
    all_party_gap_rows: List[Dict[str, object]] = []
    for commit in git_poll_commits(args.election_key):
        commit_rows, commit_party_gap_rows = analyze_commit(commit, args.election_key, key_to_wk, ags_to_wks)
        all_rows.extend(commit_rows)
        all_party_gap_rows.extend(commit_party_gap_rows)

    if not all_rows:
        raise SystemExit("No poll commits found.")

    summary_rows = summarize_wahlkreise(all_rows)
    for row in summary_rows:
        row["wahlkreis_name"] = wahlkreis_names.get(str(row["wahlkreis"]), "")
    top_rows = top_events(all_rows)
    report_tex = build_report_tex(
        all_rows,
        summary_rows,
        top_rows,
        all_party_gap_rows,
        args.election_key,
        wahlkreis_names,
    )

    write_csv(
        detail_csv,
        all_rows,
        [
            "commit",
            "commit_time_utc",
            "commit_time_local",
            "subject",
            "wahlkreis",
            "reported_precincts",
            "total_precincts",
            "mapped_booth_rows",
            "unmapped_booth_rows_commit_total",
            "booth_valid_votes_erst",
            "agg_valid_votes_erst",
            "valid_gap_erst",
            "booth_valid_votes_zweit",
            "agg_valid_votes_zweit",
            "valid_gap_zweit",
            "abs_party_gap_erst",
            "abs_party_gap_zweit",
            "max_party_gap_erst_party",
            "max_party_gap_erst",
            "max_party_gap_zweit_party",
            "max_party_gap_zweit",
            "is_inconsistent",
        ],
    )
    write_csv(
        summary_csv,
        summary_rows,
        [
            "wahlkreis",
            "wahlkreis_name",
            "inconsistent_commit_count",
            "first_inconsistent_time_local",
            "last_inconsistent_time_local",
            "max_abs_valid_gap_erst",
            "max_abs_valid_gap_zweit",
            "max_abs_party_gap_erst",
            "max_abs_party_gap_zweit",
            "latest_valid_gap_erst",
            "latest_valid_gap_zweit",
            "latest_abs_party_gap_erst",
            "latest_abs_party_gap_zweit",
        ],
    )
    write_csv(
        party_csv,
        all_party_gap_rows,
        [
            "commit",
            "commit_time_utc",
            "commit_time_local",
            "subject",
            "wahlkreis",
            "vote_type",
            "party_name",
            "aggregate_votes",
            "booth_votes",
            "party_gap",
        ],
    )
    output_tex.parent.mkdir(parents=True, exist_ok=True)
    output_tex.write_text(report_tex, encoding="utf-8")

    print(
        json.dumps(
            {
                "detail_csv": str(detail_csv),
                "summary_csv": str(summary_csv),
                "party_csv": str(party_csv),
                "tex": str(output_tex),
                "rows": len(all_rows),
                "affected_wahlkreise": len(summary_rows),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
