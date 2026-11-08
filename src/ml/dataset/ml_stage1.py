"""P5-B-B6-01 / ML Giai doan 1: snapshot 455, ban sao long va kiem tra.

Quy tac (chi tiet trong docs/qa/P5_ML_STAGE1_runbook.md):
- Hash session: pmod(xxhash64(user_session), 10000). Ban sao long: 10 subset 57 subset 455.
- Session hop le: user_session IS NOT NULL AND trim(user_session) <> '' (dung quy tac engine).
- Chi write_snapshot() va build_snapshot_frame() duoc doc nguon (HDFS). Moi ham khac chi doc
  snapshot local. Khong goi sample_whole_sessions trong duong snapshot.

Ham thuan (khong can Spark) nam o cuoi file de test nhanh.
"""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ml.session_features import build_session_features
from src.ml.dataset.n_selection import (
    SplitBounds,
    assert_session_utc,
    classify_sessions,
    h11_split_table,
    spark_of,
    utc_ts_literal,
    valid_session_events,
)

SNAPSHOT_THRESHOLD = 455
PILOT_THRESHOLD = 57
CHECK_THRESHOLD = 10
HASH_BUCKETS = 10000
SOURCE_ROWS_REFERENCE = 109950743
PART_SAMPLE = "sample"
PART_INVALID = "invalid_key"
PART_COLUMN = "part"
ALT_PART_COLUMN = "snapshot_part"
HASH_EXPR = "pmod(xxhash64(user_session), {buckets})"
WINDOW_LO = datetime(2019, 10, 1)
WINDOW_HI = datetime(2019, 12, 1)
INVALID_CAUSES = ("null", "empty", "whitespace_only")
REQUIRED_SOURCE_COLUMNS = [
    "user_session",
    "event_time",
    "event_type",
    "event_type_rank",
    "event_sort_key",
    "product_id",
    "category_id",
    "brand",
    "price",
]


class DecisionRequiredError(RuntimeError):
    """A business decision is missing (for example the is_valid rule). Mapped to WAITING_DECISION."""


class SessionLengthGateError(RuntimeError):
    """Longest session is above the configured gate. Nothing is dropped silently."""

    def __init__(self, message: str, max_len: int, threshold: int):
        super().__init__(message)
        self.max_len = max_len
        self.threshold = threshold


# --- Hash, validity and partition helpers ---------------------------------------------------


def check_threshold(threshold: int) -> int:
    if not (1 <= int(threshold) <= HASH_BUCKETS):
        raise ValueError(f"threshold must be in [1, {HASH_BUCKETS}], got {threshold}")
    return int(threshold)


def hash_bucket_col():
    """Session bucket in [0, 10000). Same expression as lr_b6_smoke.session_bucket_expr()."""
    return F.expr(HASH_EXPR.format(buckets=HASH_BUCKETS))


def invalid_key_cond():
    """Session key is NULL or blank under the engine rule (trim removes spaces only)."""
    key = F.col("user_session")
    return key.isNull() | (F.trim(key) == "")


def invalid_cause_col():
    key = F.col("user_session")
    return (
        F.when(key.isNull(), F.lit("null"))
        .when(key == "", F.lit("empty"))
        .when(F.trim(key) == "", F.lit("whitespace_only"))
    )


def partition_column_for(columns: Sequence[str]) -> str:
    """Name of the partition column. Renamed when the source already has a column called part."""
    return ALT_PART_COLUMN if PART_COLUMN in columns else PART_COLUMN


def file_uri(path: str) -> str:
    """file:// URI for a LOCAL path. Never percent-encodes (partition names contain '=').

    Paths without a scheme are resolved by Spark against fs.defaultFS (HDFS on the VM), so every
    local path given to Spark must go through this function.
    """
    posix = Path(path).absolute().as_posix()
    if not posix.startswith("/"):
        posix = "/" + posix  # Windows drive letter: C:/x -> /C:/x
    return "file://" + posix


# --- Source read (ONE action) -----------------------------------------------------------------


def check_source_columns(columns: Sequence[str]) -> List[str]:
    return [c for c in REQUIRED_SOURCE_COLUMNS if c not in columns]


def build_snapshot_frame(raw: DataFrame, threshold: int = SNAPSHOT_THRESHOLD) -> Tuple[DataFrame, str]:
    """Tag every row once: 'invalid_key' (checked first), 'sample' (hash < threshold), else dropped.

    No action is triggered here. Whole sessions are kept because the decision depends only on
    user_session and no row is filtered by any other column.
    """
    threshold = check_threshold(threshold)
    part_col = partition_column_for(raw.columns)
    tag = (
        F.when(invalid_key_cond(), F.lit(PART_INVALID))
        .when(hash_bucket_col() < F.lit(threshold), F.lit(PART_SAMPLE))
    )
    frame = raw.withColumn(part_col, tag).filter(F.col(part_col).isNotNull())
    return frame, part_col


def write_snapshot(raw: DataFrame, out_dir: str, threshold: int = SNAPSHOT_THRESHOLD, coalesce_n: int = 16) -> str:
    """Single write of the snapshot. Returns the partition column name used."""
    frame, part_col = build_snapshot_frame(raw, threshold)
    frame.coalesce(int(coalesce_n)).write.mode("errorifexists").partitionBy(part_col).parquet(out_dir)
    return part_col


def subset_frame(sample: DataFrame, threshold: int) -> DataFrame:
    """Nested subset of an already materialised sample (no source read)."""
    threshold = check_threshold(threshold)
    return sample.filter(hash_bucket_col() < F.lit(threshold))


def write_subset(sample: DataFrame, out_dir: str, threshold: int, coalesce_n: int = 4) -> None:
    subset_frame(sample, threshold).coalesce(int(coalesce_n)).write.mode("errorifexists").parquet(out_dir)


# --- Statistics computed from disk (never from the source) ------------------------------------


def _counts(df: DataFrame, col: str) -> Dict[str, int]:
    return {str(r[0]): int(r[1]) for r in df.groupBy(col).count().collect()}


def compute_part_stats(spark, part_dir: str, bounds: SplitBounds, threshold: Optional[int] = None) -> Dict[str, Any]:
    """Statistics of a part directory on local disk. threshold=None skips the hash check."""
    assert_session_utc(spark)
    df = spark.read.parquet(file_uri(part_dir))
    stats: Dict[str, Any] = {"schema_names": sorted(df.columns), "rows": int(df.count())}
    stats["distinct_sessions"] = int(df.select("user_session").distinct().count())
    stats["invalid_key_rows"] = int(df.filter(invalid_key_cond()).count())
    if threshold is not None:
        stats["hash_violations"] = int(df.filter(hash_bucket_col() >= F.lit(int(threshold))).count())
    lo, hi = utc_ts_literal(WINDOW_LO), utc_ts_literal(WINDOW_HI)
    stats["time_out_of_window_rows"] = int(
        df.filter(F.col("event_time").isNull() | (F.col("event_time") < lo) | (F.col("event_time") >= hi)).count()
    )
    stats["month_counts"] = _counts(df.select(F.date_format("event_time", "yyyy-MM").alias("month")), "month")
    buckets = classify_sessions(valid_session_events(df), bounds)
    stats["h11_bucket_sessions"] = _counts(buckets, "bucket")
    return stats


def compute_invalid_stats(spark, invalid_dir: str) -> Dict[str, Any]:
    df = spark.read.parquet(file_uri(invalid_dir))
    causes = _counts(df.select(invalid_cause_col().alias("cause")).filter(F.col("cause").isNotNull()), "cause")
    return {
        "schema_names": sorted(df.columns),
        "rows": int(df.count()),
        "by_cause": {c: int(causes.get(c, 0)) for c in INVALID_CAUSES},
        "rows_not_invalid": int(df.filter(~invalid_key_cond()).count()),
    }


def max_session_events(events: DataFrame) -> int:
    row = events.groupBy("user_session").count().agg(F.max("count").alias("m")).collect()[0]
    return int(row["m"] or 0)


# --- Files, hashes and manifests (pure) ------------------------------------------------------


def sha256_file(path: str, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def list_files(root: str) -> List[Dict[str, Any]]:
    """Relative path and size of every file under root, sorted. Used by manifests and verify."""
    base = Path(root)
    out = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            out.append({"path": p.relative_to(base).as_posix(), "bytes": p.stat().st_size})
    return out


def write_json(path: str, obj: Any) -> None:
    path = str(path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
    os.replace(tmp, path)


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def compare_stats(expected: Dict[str, Any], actual: Dict[str, Any], keys: Optional[Sequence[str]] = None) -> List[str]:
    """Problems where actual differs from expected. Lists are compared as sorted lists."""
    problems = []
    for key in keys or expected.keys():
        if key not in expected:
            continue
        e, a = expected[key], actual.get(key)
        if isinstance(e, list) and isinstance(a, list):
            e, a = sorted(e), sorted(a)
        if e != a:
            problems.append(f"{key}: manifest={e} disk={a}")
    return problems


def check_input_manifest(df: DataFrame, manifest: Dict[str, Any]) -> List[str]:
    """Schema and row count of the input must match its manifest (used by B6-01/B6-02 snapshot mode)."""
    problems = []
    if sorted(df.columns) != sorted(manifest.get("schema_names", [])):
        problems.append("schema differs from manifest")
    if PART_COLUMN in df.columns or ALT_PART_COLUMN in df.columns:
        problems.append("partition column leaked into the data; read the part=sample directory")
    rows = int(df.count())
    if rows != int(manifest.get("rows", -1)):
        problems.append(f"rows={rows} differ from manifest rows={manifest.get('rows')}")
    return problems


def manifest_scope_fields(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Fields every output of the snapshot path must carry (never called Full)."""
    return {
        "source_scope": "SAMPLED_SESSIONS",
        "sample_threshold": int(manifest.get("threshold", -1)),
        "input_kind": manifest.get("kind"),
        "input_name": manifest.get("name", "snapshot"),
        "input_manifest_sha256": manifest.get("manifest_sha256"),
    }


# --- B6-02 snapshot mode (core) ---------------------------------------------------------------


def build_snapshot_features(
    events: DataFrame,
    n: int,
    bounds: SplitBounds,
    is_valid_rule: Optional[str] = None,
    max_session_events_gate: Optional[int] = None,
) -> Tuple[DataFrame, Dict[str, Any]]:
    """B6-02 for the snapshot path. Returns (persisted dataset with h11_split, stats).

    - Split by h11_split_table (B6-01 classify_sessions). No sampling function is called here.
    - Only train, validation and test sessions are ranked; other buckets are counted.
    - Dataset holds eligible sessions only (same rule as build_session_features).
    - Raises DecisionRequiredError when the snapshot has is_valid and no rule was chosen.
    - Raises SessionLengthGateError when the longest session exceeds max_session_events_gate.
    The caller writes the dataset and unpersists it.
    """
    spark = spark_of(events)
    assert_session_utc(spark)
    has_is_valid = "is_valid" in events.columns
    if has_is_valid and is_valid_rule is None:
        raise DecisionRequiredError("snapshot has column is_valid: choose --is-valid-rule ignore|apply")
    rule = is_valid_rule if has_is_valid else "ignore"

    stats: Dict[str, Any] = {
        "n": int(n),
        "has_is_valid": has_is_valid,
        "is_valid_rule": rule if has_is_valid else None,
    }
    if has_is_valid:
        stats["is_valid_false_rows"] = int(events.filter(F.col("is_valid") == F.lit(False)).count())

    longest = max_session_events(events)
    stats["max_session_events"] = longest
    stats["max_session_events_gate"] = max_session_events_gate
    if max_session_events_gate is not None and longest > int(max_session_events_gate):
        raise SessionLengthGateError(
            f"longest session has {longest} events, gate is {max_session_events_gate}; not started",
            longest,
            int(max_session_events_gate),
        )

    keys = h11_split_table(events, bounds)
    stats["h11_sessions_by_split"] = _counts(keys, "h11_split")
    if not stats["h11_sessions_by_split"]:
        raise ValueError("no session in train, validation or test under H11 windows")

    dataset, audit, invalid_rows = build_session_features(
        events, n, is_valid_rule=rule, session_keys=keys.select("user_session")
    )
    dataset = dataset.join(keys, on="user_session", how="inner").withColumn("prediction_n", F.lit(int(n)))
    dataset = dataset.persist(StorageLevel.DISK_ONLY)

    audit_by_split = (
        audit.join(keys, on="user_session", how="inner").groupBy("h11_split", "status").count().collect()
    )
    stats["audit_status_by_split"] = {}
    for r in audit_by_split:
        stats["audit_status_by_split"].setdefault(str(r["h11_split"]), {})[str(r["status"])] = int(r["count"])

    label_rows = dataset.groupBy("h11_split", "label").count().collect()
    stats["label_counts_by_split"] = {}
    for r in label_rows:
        stats["label_counts_by_split"].setdefault(str(r["h11_split"]), {})[str(r["label"])] = int(r["count"])

    stats["dataset_rows_by_split"] = _counts(dataset, "h11_split")
    stats["invalid_key_event_rows"] = int(invalid_rows.count())
    return dataset, stats
