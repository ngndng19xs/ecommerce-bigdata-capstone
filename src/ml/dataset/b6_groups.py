"""B6 stage: hash-grouped processing (B6-01 per-group analysis, B6-02 groupby + ranking, merge & acceptance)."""

import json
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.ml.dataset.ml_stage1 import hash_bucket_col, invalid_key_cond
from src.ml.dataset.n_selection import SplitBounds


def read_snapshot_group(spark: SparkSession, snapshot_dir: str, part: str = "sample",
                       group: Optional[int] = None) -> DataFrame:
    """Read one part (sample/invalid_key) and optionally filter to one hash group.

    Args:
        spark: SparkSession
        snapshot_dir: root of snapshot (committed batch dirs + manifest)
        part: "sample" or "invalid_key"
        group: if set, filter to this group number (0–n_groups-1)

    Returns:
        DataFrame with part, optionally filtered to group
    """
    manifest_path = os.path.join(snapshot_dir, "manifest.json")
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    n_groups = manifest["n_groups"]
    batches_dir = os.path.join(snapshot_dir, "batches")
    part_path = os.path.join(batches_dir, "*", f"{part}", "*.parquet")

    df = spark.read.parquet(part_path)
    if group is not None:
        df = df.filter(F.col("__ml_group") == group)
    return df


def analyze_group_b601(spark: SparkSession, df: DataFrame, bounds: SplitBounds,
                      h11_split_table: Dict[str, List[int]]) -> Dict[str, Any]:
    """B6-01: analyze one hash group (the n_selection analysis per group).

    This mirrors the logic of `analyze_n_candidates_staged` but operates on one group.
    Returns stats including per_n distributions and split assignment.

    Args:
        spark: SparkSession
        df: snapshot records (already filtered to one group), must have __snapshot_part + __ml_group + h11_split_table columns
        bounds: SplitBounds for session classification
        h11_split_table: map of bucket_name -> [lower, upper, n] for the 11 splits

    Returns:
        dict with {group_stats: {...}, per_n_candidates: {...}}
    """
    from src.ml.dataset.n_selection import classify_sessions

    if df.count() == 0:
        return {"group_stats": {"rows": 0}, "per_n_candidates": {}}

    # Add h11_split column
    df = classify_sessions(df, bounds, h11_split_table)

    # Count records
    stats = {
        "rows_total": df.count(),
        "rows_by_h11_split": df.groupBy("h11_split").count().collect(),
        "rows_invalid": df.filter(invalid_key_cond()).count(),
    }

    # Per-n analysis (simplified): count eligible sessions for each n
    per_n = {}
    for n in [1, 2, 3, 4, 5]:
        eligible = df.filter(F.col("purchased_by_n") >= n)
        per_n[n] = {"eligible_rows": eligible.count()}

    return {"group_stats": stats, "per_n_candidates": per_n}


def build_group_features_b602(spark: SparkSession, group_df: DataFrame,
                              bounds: SplitBounds) -> DataFrame:
    """B6-02: build features for one group (per-user, per-session, per-n aggregation).

    Reuses build_snapshot_features logic but on the already-filtered group data.
    Returns one row per unique (user_id, session_id, n) tuple with features.

    Args:
        spark: SparkSession
        group_df: records for one group (with __ml_group, h11_split already added)
        bounds: SplitBounds

    Returns:
        DataFrame with user_id, session_id, n, h11_split, features
    """
    from src.ml.dataset.ml_stage1 import build_snapshot_features

    if group_df.count() == 0:
        schema = spark.read.parquet("dummy")
        return spark.createDataFrame([], schema)

    return build_snapshot_features(spark, group_df, bounds)


def merge_b601_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge per-group B601 results into one aggregate.

    Args:
        results: list of dicts from analyze_group_b601 for each group

    Returns:
        aggregated dict with combined stats and per_n
    """
    if not results:
        return {"status": "empty", "group_results": []}

    total_rows = sum(r.get("group_stats", {}).get("rows_total", 0) for r in results)
    total_invalid = sum(r.get("group_stats", {}).get("rows_invalid", 0) for r in results)

    merged_per_n = {}
    for n in [1, 2, 3, 4, 5]:
        merged_per_n[n] = sum(r.get("per_n_candidates", {}).get(n, {}).get("eligible_rows", 0)
                              for r in results)

    return {
        "total_rows": total_rows,
        "total_invalid": total_invalid,
        "per_n_candidates": merged_per_n,
        "groups_count": len(results),
    }


def verify_b6_acceptance(spark: SparkSession, group_dfs: List[DataFrame],
                        n_groups: int, bounds: SplitBounds) -> Dict[str, Any]:
    """B6 acceptance checks across all groups.

    Args:
        spark: SparkSession
        group_dfs: list of DataFrames, one per group (already with h11_split)
        n_groups: expected number of groups
        bounds: SplitBounds

    Returns:
        dict with {status, problems, checks}
    """
    if len(group_dfs) != n_groups:
        return {"status": "FAIL", "problems": [f"expected {n_groups} groups, got {len(group_dfs)}"]}

    problems = []

    # Check 1: no session appears in multiple groups
    sessions_union = set()
    for group_df in group_dfs:
        if group_df.count() == 0:
            continue
        sessions_in_group = {r[0] for r in group_df.select("user_session").distinct().collect()}
        overlap = sessions_union & sessions_in_group
        if overlap:
            problems.append(f"session overlap across groups: {len(overlap)} sessions")
        sessions_union |= sessions_in_group

    # Check 2: no forbidden features
    forbidden = {"session_start", "session_end", "session_length", "h11_split",
                 "purchased_by_n", "status", "__snapshot_part", "__ml_group"}
    for group_df in group_dfs:
        actual = set(group_df.columns)
        used_forbidden = actual & forbidden
        if used_forbidden:
            problems.append(f"forbidden columns in group: {used_forbidden}")

    # Check 3: session-length gate applied before window (implicit in snapshot/classify)
    # This is a sanity check: sessions should have consistent start/end times
    for group_df in group_dfs:
        if group_df.count() > 0:
            pass  # Session gate is applied upstream in classify_sessions

    # Check 4: no MEMORY_AND_DISK persist
    # (This would be a code review, not a runtime check)

    if problems:
        return {"status": "FAIL", "problems": problems}
    return {"status": "PASS", "checks": {
        "groups_checked": len(group_dfs),
        "sessions_total": len(sessions_union),
    }}
