#!/usr/bin/env python3
"""Analyze the official 2026 RLP result source against the aggregated interim output.

This script fetches the official 23degrees assets, reconstructs the finest
publicly visible units in the official Wahlkreis tree, and writes reports about:

- how much of the state is still only published in grouped form
- where the tree stops breaking results down before the lowest visible nodes
- how the official source compares to the repo's cached interim aggregation
- a Monte Carlo check for the seemingly "constant" CDU share in grouped units
"""

from __future__ import annotations

import argparse
import io
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "2026-rlp"
REPORT_DIR = DATA_DIR / "reports"
HISTORY_DB = DATA_DIR / "history.sqlite"

OFFICIAL_RESULTS_CSV_URL = "https://rlp-ltw26.wahlen.23degrees.eu/assets/json/Wahlergebnisse_Landtagswahl_2026.csv"
OFFICIAL_WK_TREE_URL = "https://rlp-ltw26.wahlen.23degrees.eu/assets/wk-vec-tree.json"
OFFICIAL_GLOBAL_URL = "https://rlp-ltw26.wahlen.23degrees.eu/assets/json/global.json"
OFFICIAL_FAQ_URL = "https://www.wahlen.rlp.de/landtagswahl/ergebnisse/fragen-zu-den-ergebnissen"


@dataclass(frozen=True)
class TreeNode:
    node_id: str
    name: str
    level: int
    parent: str
    children: tuple[str, ...]


def fetch_text(url: str) -> str:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return response.text


def fetch_json(url: str) -> Any:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return response.json()


def read_official_csv() -> pd.DataFrame:
    text = fetch_text(OFFICIAL_RESULTS_CSV_URL)
    frame = pd.read_csv(io.StringIO(text), sep=";", dtype=str).fillna("")
    frame["id"] = frame["Identifikationsschlüssel"].astype(str).str.zfill(13)
    for column in [
        "Anzahl der Wahlbezirke",
        "Anzahl ausgezählt",
        "gültige Landesstimmen",
        "CDU.2",
        "gültige Wahlkreisstimmen",
        "CDU",
    ]:
        frame[column] = pd.to_numeric(
            frame[column]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False),
            errors="coerce",
        )
    return frame


def build_wk_tree() -> dict[str, TreeNode]:
    payload = fetch_json(OFFICIAL_WK_TREE_URL)
    tree: dict[str, TreeNode] = {}
    for raw in payload:
        node_id = str(raw.get("bezeichnung") or "").zfill(13)
        children = tuple(str(child.get("bezeichnung") or "").zfill(13) for child in raw.get("children") or [])
        tree[node_id] = TreeNode(
            node_id=node_id,
            name=str(raw.get("name") or ""),
            level=int(raw.get("level") or 0),
            parent=str(raw.get("parent") or "").zfill(13) if raw.get("parent") else "",
            children=children,
        )
    return tree


def canonical_ags(raw: Any) -> str:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    try:
        return f"{int(float(text)):08d}"
    except Exception:
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits.zfill(8) if digits else ""


def safe_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    try:
        value = float(raw)
    except Exception:
        return 0.0
    return 0.0 if np.isnan(value) else value


def is_wahlkreis_row(node_id: str) -> bool:
    return node_id.endswith("0000000000") and node_id != "0000000000000" and not node_id.startswith(("100", "200", "300", "400"))


def build_lowest_published_units(
    official_rows: pd.DataFrame,
    wk_tree: Mapping[str, TreeNode],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    row_by_id = official_rows.set_index("id").to_dict("index")

    unit_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []

    for node_id, node in wk_tree.items():
        if node_id not in row_by_id:
            continue

        current = row_by_id[node_id]
        child_ids = [child_id for child_id in node.children if child_id in row_by_id]
        ags = node_id[3:11]

        if not child_ids:
            precincts = safe_float(current.get("Anzahl der Wahlbezirke"))
            valid = safe_float(current.get("gültige Landesstimmen"))
            cdu = safe_float(current.get("CDU.2"))
            if precincts <= 0 and valid <= 0 and cdu <= 0:
                continue
            unit_rows.append(
                {
                    "wk": node_id[:3],
                    "ags": ags,
                    "node_id": node_id,
                    "node_name": node.name,
                    "node_level": node.level,
                    "unit_type": "single_precinct_leaf" if precincts == 1 else "multi_precinct_leaf",
                    "precincts": int(precincts),
                    "valid_zweit": int(valid),
                    "cdu_zweit": int(cdu),
                    "cdu_share_zweit": (cdu / valid) if valid else np.nan,
                }
            )
            continue

        child_precincts = sum(safe_float(row_by_id[child_id].get("Anzahl der Wahlbezirke")) for child_id in child_ids)
        child_valid = sum(safe_float(row_by_id[child_id].get("gültige Landesstimmen")) for child_id in child_ids)
        child_cdu = sum(safe_float(row_by_id[child_id].get("CDU.2")) for child_id in child_ids)

        parent_precincts = safe_float(current.get("Anzahl der Wahlbezirke"))
        parent_valid = safe_float(current.get("gültige Landesstimmen"))
        parent_cdu = safe_float(current.get("CDU.2"))

        residual_precincts = parent_precincts - child_precincts
        residual_valid = parent_valid - child_valid
        residual_cdu = parent_cdu - child_cdu

        if residual_precincts > 0 or residual_valid > 0 or residual_cdu > 0:
            residual_row = {
                "wk": node_id[:3],
                "ags": ags,
                "node_id": node_id,
                "node_name": node.name,
                "node_level": node.level,
                "unit_type": "parent_residual",
                "precincts": int(residual_precincts),
                "valid_zweit": int(residual_valid),
                "cdu_zweit": int(residual_cdu),
                "cdu_share_zweit": (residual_cdu / residual_valid) if residual_valid else np.nan,
                "child_row_count": len(child_ids),
                "child_precincts": int(child_precincts),
                "child_valid_zweit": int(child_valid),
                "child_cdu_zweit": int(child_cdu),
            }
            unit_rows.append(residual_row)
            residual_rows.append(residual_row)

    units = pd.DataFrame(unit_rows).sort_values(
        ["wk", "ags", "unit_type", "node_name", "node_id"],
        ignore_index=True,
    )
    residuals = pd.DataFrame(residual_rows).sort_values(
        ["precincts", "valid_zweit", "node_name"],
        ascending=[False, False, True],
        ignore_index=True,
    )
    return units, residuals


def build_official_rows_not_in_wk_tree(
    official_rows: pd.DataFrame,
    wk_tree: Mapping[str, TreeNode],
) -> pd.DataFrame:
    wk_ids = set(wk_tree.keys())
    extra = official_rows.loc[~official_rows["id"].isin(wk_ids)].copy()
    extra["wk"] = extra["id"].str[:3]
    extra["ags"] = extra["id"].str[3:11]
    return extra[
        [
            "id",
            "wk",
            "ags",
            "Bezeichnung",
            "Anzahl der Wahlbezirke",
            "Anzahl ausgezählt",
            "gültige Landesstimmen",
            "CDU.2",
            "Datum letzte Änderung",
        ]
    ].rename(
        columns={
            "Bezeichnung": "name",
            "Anzahl der Wahlbezirke": "precincts",
            "Anzahl ausgezählt": "counted_precincts",
            "gültige Landesstimmen": "valid_zweit",
            "CDU.2": "cdu_zweit",
            "Datum letzte Änderung": "last_changed",
        }
    )


def read_public_rows(poll_id: int, gebietsart: str) -> pd.DataFrame:
    with sqlite3.connect(HISTORY_DB) as connection:
        snapshots = pd.read_sql_query(
            """
            SELECT row_key, ags, municipality_name, gebietsart, gebietsnummer,
                   reported_precincts, valid_votes_zweit
            FROM statla_snapshots
            WHERE poll_id = ? AND gebietsart = ?
            """,
            connection,
            params=(poll_id, gebietsart),
        )
        party = pd.read_sql_query(
            """
            SELECT row_key, votes AS cdu_zweit
            FROM statla_party_results
            WHERE poll_id = ? AND vote_type = 'Zweitstimmen' AND party_name = 'CDU'
            """,
            connection,
            params=(poll_id,),
        )
    frame = snapshots.merge(party, on="row_key", how="left")
    frame["ags"] = frame["ags"].map(canonical_ags)
    frame["public_valid_zweit"] = pd.to_numeric(frame["valid_votes_zweit"], errors="coerce").fillna(0).astype(int)
    frame["public_cdu_zweit"] = pd.to_numeric(frame["cdu_zweit"], errors="coerce").fillna(0).astype(int)
    frame["public_precincts"] = pd.to_numeric(frame["reported_precincts"], errors="coerce").fillna(0).astype(int)
    return frame


def compare_to_public_interim(
    official_rows: pd.DataFrame,
    lowest_units: pd.DataFrame,
    poll_id: int,
) -> dict[str, pd.DataFrame]:
    with sqlite3.connect(HISTORY_DB) as connection:
        public_wahlkreise = pd.read_sql_query(
            """
            SELECT s.row_key, s.gebietsnummer, s.reported_precincts, s.valid_votes_zweit, p.votes AS public_cdu_zweit
            FROM statla_snapshots AS s
            LEFT JOIN statla_party_results AS p
              ON p.poll_id = s.poll_id
             AND p.row_key = s.row_key
             AND p.vote_type = 'Zweitstimmen'
             AND p.party_name = 'CDU'
            WHERE s.poll_id = ? AND s.gebietsart = 'WAHLKREIS'
            """,
            connection,
            params=(poll_id,),
        )
        public_land = pd.read_sql_query(
            """
            SELECT s.row_key, s.reported_precincts, s.valid_votes_zweit, p.votes AS public_cdu_zweit
            FROM statla_snapshots AS s
            LEFT JOIN statla_party_results AS p
              ON p.poll_id = s.poll_id
             AND p.row_key = s.row_key
             AND p.vote_type = 'Zweitstimmen'
             AND p.party_name = 'CDU'
            WHERE s.poll_id = ? AND s.gebietsart = 'LAND'
            """,
            connection,
            params=(poll_id,),
        )

    public_municipalities = read_public_rows(poll_id, "GEMEINDE")

    official_wahlkreise = official_rows.loc[official_rows["id"].map(is_wahlkreis_row)].copy()
    official_wahlkreise["wk"] = official_wahlkreise["id"].str[:3]
    official_wahlkreise["official_precincts"] = official_wahlkreise["Anzahl der Wahlbezirke"].fillna(0).astype(int)
    official_wahlkreise["official_valid_zweit"] = official_wahlkreise["gültige Landesstimmen"].fillna(0).astype(int)
    official_wahlkreise["official_cdu_zweit"] = official_wahlkreise["CDU.2"].fillna(0).astype(int)

    wahlkreis_compare = public_wahlkreise.rename(columns={"gebietsnummer": "wk"}).copy()
    wahlkreis_compare["wk"] = wahlkreis_compare["wk"].astype(str).str.zfill(3)
    wahlkreis_compare["public_precincts"] = pd.to_numeric(wahlkreis_compare["reported_precincts"], errors="coerce").fillna(0).astype(int)
    wahlkreis_compare["public_valid_zweit"] = pd.to_numeric(wahlkreis_compare["valid_votes_zweit"], errors="coerce").fillna(0).astype(int)
    wahlkreis_compare["public_cdu_zweit"] = pd.to_numeric(wahlkreis_compare["public_cdu_zweit"], errors="coerce").fillna(0).astype(int)
    wahlkreis_compare = wahlkreis_compare[["wk", "public_precincts", "public_valid_zweit", "public_cdu_zweit"]].merge(
        official_wahlkreise[["wk", "official_precincts", "official_valid_zweit", "official_cdu_zweit"]],
        on="wk",
        how="outer",
    ).fillna(0)
    wahlkreis_compare["valid_diff"] = wahlkreis_compare["official_valid_zweit"] - wahlkreis_compare["public_valid_zweit"]
    wahlkreis_compare["cdu_diff"] = wahlkreis_compare["official_cdu_zweit"] - wahlkreis_compare["public_cdu_zweit"]
    wahlkreis_compare["precinct_diff"] = wahlkreis_compare["official_precincts"] - wahlkreis_compare["public_precincts"]

    municipality_official = (
        lowest_units.groupby("ags", as_index=False)[["precincts", "valid_zweit", "cdu_zweit"]]
        .sum()
        .rename(columns={"precincts": "official_precincts", "valid_zweit": "official_valid_zweit", "cdu_zweit": "official_cdu_zweit"})
    )
    municipality_compare = public_municipalities[["ags", "public_precincts", "public_valid_zweit", "public_cdu_zweit"]].merge(
        municipality_official,
        on="ags",
        how="outer",
    ).fillna(0)
    municipality_compare["valid_diff"] = municipality_compare["official_valid_zweit"] - municipality_compare["public_valid_zweit"]
    municipality_compare["cdu_diff"] = municipality_compare["official_cdu_zweit"] - municipality_compare["public_cdu_zweit"]
    municipality_compare["precinct_diff"] = municipality_compare["official_precincts"] - municipality_compare["public_precincts"]

    official_land = official_rows.loc[official_rows["id"] == "0000000000000"].iloc[0]
    land_compare = pd.DataFrame(
        [
            {
                "public_precincts": int(safe_float(pd.to_numeric(public_land.iloc[0]["reported_precincts"], errors="coerce"))),
                "public_valid_zweit": int(safe_float(pd.to_numeric(public_land.iloc[0]["valid_votes_zweit"], errors="coerce"))),
                "public_cdu_zweit": int(safe_float(pd.to_numeric(public_land.iloc[0]["public_cdu_zweit"], errors="coerce"))),
                "official_precincts": int(safe_float(official_land["Anzahl der Wahlbezirke"])),
                "official_valid_zweit": int(safe_float(official_land["gültige Landesstimmen"])),
                "official_cdu_zweit": int(safe_float(official_land["CDU.2"])),
            }
        ]
    )
    land_compare["valid_diff"] = land_compare["official_valid_zweit"] - land_compare["public_valid_zweit"]
    land_compare["cdu_diff"] = land_compare["official_cdu_zweit"] - land_compare["public_cdu_zweit"]
    land_compare["precinct_diff"] = land_compare["official_precincts"] - land_compare["public_precincts"]

    return {
        "land": land_compare,
        "wahlkreis": wahlkreis_compare.sort_values("wk", ignore_index=True),
        "municipality": municipality_compare.sort_values(["valid_diff", "cdu_diff", "ags"], ascending=[False, False, True], ignore_index=True),
    }


def run_cdu_monte_carlo(lowest_units: pd.DataFrame, reps: int, seed: int) -> dict[str, Any]:
    units = lowest_units.copy()
    units = units[(units["precincts"] > 0) & (units["valid_zweit"] > 0)].copy()
    units["share"] = units["cdu_zweit"] / units["valid_zweit"]

    wk_totals = units.groupby("wk", as_index=False)[["valid_zweit", "cdu_zweit"]].sum()
    wk_totals["wk_share"] = wk_totals["cdu_zweit"] / wk_totals["valid_zweit"]
    wk_share = wk_totals.set_index("wk")["wk_share"]
    units = units.merge(wk_share, on="wk")

    summary = units.groupby("wk").agg(
        single=("precincts", lambda series: int((series == 1).sum())),
        multi=("precincts", lambda series: int((series > 1).sum())),
    )
    covered_wks = summary.loc[(summary["single"] > 0) & (summary["multi"] > 0)].index.tolist()
    covered = units.loc[units["wk"].isin(covered_wks)].copy()

    observed_multi = covered.loc[covered["precincts"] > 1].copy()
    observed_dev = observed_multi["share"] - observed_multi["wk_share"]
    observed_mean = np.average(observed_dev, weights=observed_multi["valid_zweit"])
    observed_sd = float(
        np.sqrt(
            np.average(
                (observed_dev - observed_mean) ** 2,
                weights=observed_multi["valid_zweit"],
            )
        )
    )

    by_wk_single = {
        wk: group.loc[group["precincts"] == 1, ["valid_zweit", "cdu_zweit"]].to_numpy(dtype=float)
        for wk, group in covered.groupby("wk")
    }
    by_wk_multi_sizes = {
        wk: group.loc[group["precincts"] > 1, "precincts"].to_numpy(dtype=int)
        for wk, group in covered.groupby("wk")
    }
    by_wk_share = wk_share.to_dict()

    rng = np.random.default_rng(seed)
    simulated_sd = np.empty(reps, dtype=float)
    for rep in range(reps):
        devs: list[float] = []
        weights: list[float] = []
        for wk in covered_wks:
            single = by_wk_single[wk]
            sizes = by_wk_multi_sizes[wk]
            for size in sizes:
                sampled_idx = rng.integers(0, len(single), size=int(size))
                sampled_valid = float(single[sampled_idx, 0].sum())
                sampled_cdu = float(single[sampled_idx, 1].sum())
                if sampled_valid <= 0:
                    continue
                devs.append(sampled_cdu / sampled_valid - by_wk_share[wk])
                weights.append(sampled_valid)
        dev_array = np.asarray(devs, dtype=float)
        weight_array = np.asarray(weights, dtype=float)
        simulated_mean = np.average(dev_array, weights=weight_array)
        simulated_sd[rep] = np.sqrt(np.average((dev_array - simulated_mean) ** 2, weights=weight_array))

    less_equal = int(np.count_nonzero(simulated_sd <= observed_sd))
    return {
        "covered_wahlkreise": len(covered_wks),
        "covered_multi_rows": int(len(observed_multi)),
        "covered_multi_precincts": int(observed_multi["precincts"].sum()),
        "observed_weighted_sd_pp": observed_sd * 100.0,
        "simulated_mean_sd_pp": float(simulated_sd.mean() * 100.0),
        "simulated_std_sd_pp": float(simulated_sd.std() * 100.0),
        "simulated_quantiles_sd_pp": {
            "p01": float(np.quantile(simulated_sd, 0.01) * 100.0),
            "p05": float(np.quantile(simulated_sd, 0.05) * 100.0),
            "p10": float(np.quantile(simulated_sd, 0.10) * 100.0),
            "p50": float(np.quantile(simulated_sd, 0.50) * 100.0),
            "p90": float(np.quantile(simulated_sd, 0.90) * 100.0),
            "p95": float(np.quantile(simulated_sd, 0.95) * 100.0),
            "p99": float(np.quantile(simulated_sd, 0.99) * 100.0),
        },
        "simulated_p_le_observed": float((less_equal + 1) / (reps + 1)),
        "simulated_repetitions": reps,
        "seed": seed,
        "metric": (
            "Weighted within-Wahlkreis standard deviation of CDU second-vote share across "
            "multi-precinct lowest published units; simulated by regrouping published single-precinct "
            "units within the same Wahlkreis."
        ),
        "note": (
            "A smaller observed SD means the grouped published units are more internally homogeneous "
            "than random same-size regroupings of the visible single-precinct units."
        ),
    }


def write_dataframe(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poll-id", type=int, default=12, help="Cached interim poll to compare against.")
    parser.add_argument("--reps", type=int, default=10000, help="Monte Carlo repetitions.")
    parser.add_argument("--seed", type=int, default=0, help="Monte Carlo RNG seed.")
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    official_rows = read_official_csv()
    wk_tree = build_wk_tree()
    global_meta = fetch_json(OFFICIAL_GLOBAL_URL)

    lowest_units, residuals = build_lowest_published_units(official_rows, wk_tree)
    official_extra = build_official_rows_not_in_wk_tree(official_rows, wk_tree)
    public_compare = compare_to_public_interim(official_rows, lowest_units, poll_id=args.poll_id)
    monte_carlo = run_cdu_monte_carlo(lowest_units, reps=args.reps, seed=args.seed)

    publication_summary = {
        "lowest_units_total": int(len(lowest_units)),
        "single_precinct_units": int((lowest_units["unit_type"] == "single_precinct_leaf").sum()),
        "single_precinct_precincts": int(lowest_units.loc[lowest_units["unit_type"] == "single_precinct_leaf", "precincts"].sum()),
        "multi_precinct_units": int((lowest_units["unit_type"] == "multi_precinct_leaf").sum()),
        "multi_precinct_precincts": int(lowest_units.loc[lowest_units["unit_type"] == "multi_precinct_leaf", "precincts"].sum()),
        "residual_parent_units": int((lowest_units["unit_type"] == "parent_residual").sum()),
        "residual_parent_precincts": int(lowest_units.loc[lowest_units["unit_type"] == "parent_residual", "precincts"].sum()),
    }
    total_precincts = int(lowest_units["precincts"].sum())
    publication_summary["total_precincts_accounted_for"] = total_precincts
    publication_summary["individual_precinct_share_percent"] = publication_summary["single_precinct_precincts"] / total_precincts * 100.0
    publication_summary["aggregated_precinct_share_percent"] = (
        (publication_summary["multi_precinct_precincts"] + publication_summary["residual_parent_precincts"])
        / total_precincts
        * 100.0
    )

    municipality_compare = public_compare["municipality"]
    municipality_diff = municipality_compare.loc[
        (municipality_compare["valid_diff"] != 0)
        | (municipality_compare["cdu_diff"] != 0)
        | (municipality_compare["precinct_diff"] != 0)
    ].copy()
    wahlkreis_diff = public_compare["wahlkreis"].loc[
        (public_compare["wahlkreis"]["valid_diff"] != 0)
        | (public_compare["wahlkreis"]["cdu_diff"] != 0)
        | (public_compare["wahlkreis"]["precinct_diff"] != 0)
    ].copy()
    land_diff = public_compare["land"].loc[
        (public_compare["land"]["valid_diff"] != 0)
        | (public_compare["land"]["cdu_diff"] != 0)
        | (public_compare["land"]["precinct_diff"] != 0)
    ].copy()

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "official_sources": {
            "results_csv_url": OFFICIAL_RESULTS_CSV_URL,
            "wk_tree_url": OFFICIAL_WK_TREE_URL,
            "global_url": OFFICIAL_GLOBAL_URL,
            "faq_url": OFFICIAL_FAQ_URL,
            "global_state": global_meta.get("state"),
            "global_timestamp_utc": global_meta.get("timestamp"),
        },
        "publication_structure": publication_summary,
        "public_tree_residual_parent_nodes": residuals[
            ["node_id", "node_name", "precincts", "valid_zweit", "cdu_zweit", "cdu_share_zweit", "child_row_count", "child_precincts"]
        ].to_dict(orient="records"),
        "official_rows_not_in_wk_tree_count": int(len(official_extra)),
        "interim_comparison": {
            "poll_id": args.poll_id,
            "land_diff_rows": int(len(land_diff)),
            "wahlkreis_diff_rows": int(len(wahlkreis_diff)),
            "municipality_diff_rows": int(len(municipality_diff)),
            "municipality_diff_ags": municipality_diff["ags"].tolist(),
        },
        "cdu_monte_carlo": monte_carlo,
    }

    write_dataframe(lowest_units, REPORT_DIR / "official_publication_units.csv")
    write_dataframe(residuals, REPORT_DIR / "official_publication_gaps.csv")
    write_dataframe(official_extra, REPORT_DIR / "official_rows_not_in_wk_tree.csv")
    write_dataframe(public_compare["wahlkreis"], REPORT_DIR / "official_vs_interim_wahlkreis_diff.csv")
    write_dataframe(municipality_compare, REPORT_DIR / "official_vs_interim_municipality_diff.csv")
    write_dataframe(public_compare["land"], REPORT_DIR / "official_vs_interim_land_diff.csv")
    (REPORT_DIR / "official_vs_interim_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
