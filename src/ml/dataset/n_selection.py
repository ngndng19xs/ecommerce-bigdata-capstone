"""P5-B-B6-01: N candidate analysis for the official prediction point.

Reuses the accepted B6 logic:
- ranking by (event_time UTC, event_type_rank, event_sort_key) via ml.features.rank_session_events;
- exclusion statuses in the same priority as ml.session_features.build_session_features:
  too_short -> purchased_by_n -> eligible;
- label = 1 iff a purchase exists with event_idx > N, else 0 (within the session).

Temporal policy (chronological, no test labels):
- A session belongs to train/validation only if ALL its events lie inside that split window
  (session_start in window AND session_end < window end, end exclusive). Otherwise it is
  censored from that split and counted separately. This guarantees that no label of a train
  session uses validation/test events, and no label of a validation session uses test events.
- Sessions starting in the test window are counted but never ranked and never labelled.
  Only event_time is read for them, never purchase or label information.
- Invalid sessions (NULL or blank user_session) are counted as EVENTS, not sessions.

Output is a plain dict (JSON-serialisable). The caller decides where to write it.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

from ml.features import rank_session_events

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"
SPARK_TS_FMT = "yyyy-MM-dd HH:mm:ss"
SPARK_ISO_UTC_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
UTC_SESSION_ZONES = ("UTC", "Etc/UTC")


def utc_ts_literal(dt: datetime):
    """Timestamp literal for a UTC wall-clock value, independent of the Python/JVM/OS time zone.

    F.lit(datetime) is NOT used: PySpark converts a naive datetime with the local time zone of the
    Python process, which shifted every window boundary by the OS offset (seen with KST, +9h).
    Here the value is sent as a string and parsed by Spark in the session time zone, which
    assert_session_utc() requires to be UTC. Timezone-aware inputs are converted to UTC first.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return F.to_timestamp(F.lit(dt.strftime("%Y-%m-%d %H:%M:%S")), SPARK_TS_FMT)


def spark_of(df: DataFrame):
    """SparkSession of a DataFrame (Spark 3.3+ attribute, with the 3.1 fallback)."""
    session = getattr(df, "sparkSession", None)
    if session is None:
        session = df.sql_ctx.sparkSession
    return session


def assert_session_utc(spark) -> str:
    """Fail fast unless the Spark session time zone is UTC. Returns the configured zone."""
    tz = spark.conf.get("spark.sql.session.timeZone")
    if tz not in UTC_SESSION_ZONES:
        raise ValueError(f"spark.sql.session.timeZone must be UTC for P5-B-B6-01, got {tz!r}")
    return tz
SPLIT_BUCKETS = ("train", "validation")
ALL_BUCKETS = (
    "train",
    "censored_train",
    "validation",
    "censored_validation",
    "test_range",
    "after_test_end",
    "before_train",
    "excluded_null_event_time",
    "gap",
)


@dataclass(frozen=True)
class SplitBounds:
    """UTC windows. Start inclusive, end exclusive."""

    train_start: datetime
    train_end: datetime
    val_start: datetime
    val_end: datetime
    test_start: datetime
    test_end: datetime

    @staticmethod
    def from_iso(cfg: Dict[str, str]) -> "SplitBounds":
        def p(key: str) -> datetime:
            return datetime.strptime(cfg[key], ISO_FMT)

        b = SplitBounds(
            train_start=p("train_start"),
            train_end=p("train_end_exclusive"),
            val_start=p("validation_start"),
            val_end=p("validation_end_exclusive"),
            test_start=p("test_start"),
            test_end=p("test_end_exclusive"),
        )
        b.validate()
        return b

    def validate(self) -> None:
        if not (self.train_start < self.train_end <= self.val_start < self.val_end <= self.test_start < self.test_end):
            raise ValueError(
                "Windows must be ordered and non-overlapping: train < validation < test "
                "(train_end <= validation_start, validation_end <= test_start)."
            )


def parse_n_candidates(values: Iterable[int]) -> List[int]:
    ns = sorted({int(v) for v in values})
    if not ns or any(n < 1 for n in ns):
        raise ValueError("N candidates must be positive integers")
    return ns


def valid_session_events(events: DataFrame) -> DataFrame:
    """Events with a usable user_session (NULL and blank are excluded)."""
    return events.filter(F.col("user_session").isNotNull() & (F.trim(F.col("user_session")) != ""))


def classify_sessions(valid: DataFrame, b: SplitBounds) -> DataFrame:
    """One row per session: user_session, session_start, session_end, n_events, bucket.

    Uses only event_time. Does not read event_type, so it cannot leak purchase information.
    """
    sess = valid.groupBy("user_session").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count(F.lit(1)).alias("n_events"),
        F.sum(F.when(F.col("event_time").isNull(), 1).otherwise(0)).alias("n_null_time"),
    )
    s = F.col("session_start")
    e = F.col("session_end")

    def inside(lo: datetime, hi: datetime):
        return (s >= utc_ts_literal(lo)) & (s < utc_ts_literal(hi))

    # Order matters. A session with ANY NULL event_time cannot be ordered reliably, so it is
    # excluded first and counted separately (it never reaches ranking). Sessions starting at or
    # after the test end are outside the analysis and counted separately. "gap" is reserved
    # for sessions that no rule explains; QA treats any gap as a FAILURE, never as a silent drop.
    bucket = (
        F.when(F.col("n_null_time") > 0, "excluded_null_event_time")
        .when(s.isNull(), "excluded_null_event_time")
        .when(inside(b.train_start, b.train_end) & (e < utc_ts_literal(b.train_end)), "train")
        .when(inside(b.train_start, b.train_end), "censored_train")
        .when(inside(b.val_start, b.val_end) & (e < utc_ts_literal(b.val_end)), "validation")
        .when(inside(b.val_start, b.val_end), "censored_validation")
        .when(inside(b.test_start, b.test_end), "test_range")
        .when(s >= utc_ts_literal(b.test_end), "after_test_end")
        .when(s < utc_ts_literal(b.train_start), "before_train")
        .otherwise("gap")
    )
    return sess.withColumn("bucket", bucket)


H11_SPLIT_OF_BUCKET = {"train": "train", "validation": "validation", "test_range": "test"}


def h11_split_table(events: DataFrame, b: SplitBounds) -> DataFrame:
    """One row per valid session that belongs to train, validation or test under H11.

    Uses classify_sessions() (the B6-01 rule) and only renames its buckets:
    train -> train, validation -> validation, test_range -> test. Every other bucket
    (censored_*, before_train, after_test_end, excluded_null_event_time, gap) is NOT returned;
    callers count them through session_buckets. Columns: user_session, h11_split.
    """
    assert_session_utc(spark_of(events))
    sessions = classify_sessions(valid_session_events(events), b)
    split = (
        F.when(F.col("bucket") == "train", F.lit("train"))
        .when(F.col("bucket") == "validation", F.lit("validation"))
        .when(F.col("bucket") == "test_range", F.lit("test"))
    )
    return (
        sessions.withColumn("h11_split", split)
        .filter(F.col("h11_split").isNotNull())
        .select("user_session", "h11_split")
    )


def _split_end_expr(b: SplitBounds):
    return F.when(F.col("bucket") == "train", utc_ts_literal(b.train_end)).otherwise(utc_ts_literal(b.val_end))


def _n_aggregates(ns: Sequence[int], length_col: str = "length") -> List[Any]:
    """Conditional aggregates per N (shared by the legacy and staged engines)."""
    aggs = []
    for n in ns:
        length = F.col(length_col)
        first = F.col("first_purchase_idx")
        short = length < F.lit(n)
        early = (length >= F.lit(n)) & first.isNotNull() & (first <= F.lit(n))
        eligible = (length >= F.lit(n)) & ~early
        aggs += [
            F.sum(F.when(short, 1).otherwise(0)).alias(f"n{n}_too_short"),
            F.sum(F.when(early, 1).otherwise(0)).alias(f"n{n}_early_purchase"),
            F.sum(F.when(eligible, 1).otherwise(0)).alias(f"n{n}_eligible"),
            F.sum(F.when(eligible & first.isNotNull(), 1).otherwise(0)).alias(f"n{n}_label1"),
            F.sum(F.when(eligible & first.isNull(), 1).otherwise(0)).alias(f"n{n}_label0"),
            F.sum(F.when(length == F.lit(n), 1).otherwise(0)).alias(f"n{n}_len_equals_n"),
        ]
    return aggs


def _build_per_n(ns: Sequence[int], by_bucket: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Assemble the per-N table for train, validation and train+validation from bucket rows."""
    per_n: Dict[str, Dict[str, Any]] = {}
    for n in ns:
        per_n[str(n)] = {}
        for split in SPLIT_BUCKETS:
            row = by_bucket.get(split, {})
            short = int(row.get(f"n{n}_too_short") or 0)
            early = int(row.get(f"n{n}_early_purchase") or 0)
            elig = int(row.get(f"n{n}_eligible") or 0)
            l1 = int(row.get(f"n{n}_label1") or 0)
            l0 = int(row.get(f"n{n}_label0") or 0)
            scope = short + early + elig
            per_n[str(n)][split] = {
                "in_scope_sessions": scope,
                "too_short": short,
                "early_purchase": early,
                "eligible": elig,
                "label_0": l0,
                "label_1": l1,
                "coverage_ratio": round(elig / scope, 6) if scope else None,
                "positive_rate": round(l1 / elig, 6) if elig else None,
                "len_equals_n": int(row.get(f"n{n}_len_equals_n") or 0),
                "checks": {
                    "partition_ok": scope == short + early + elig,
                    "label_sum_ok": elig == l0 + l1,
                },
            }
        tr = per_n[str(n)]["train"]
        va = per_n[str(n)]["validation"]
        per_n[str(n)]["train_plus_validation"] = {
            k: tr[k] + va[k]
            for k in ("in_scope_sessions", "too_short", "early_purchase", "eligible", "label_0", "label_1", "len_equals_n")
        }
        tv = per_n[str(n)]["train_plus_validation"]
        tv["coverage_ratio"] = round(tv["eligible"] / tv["in_scope_sessions"], 6) if tv["in_scope_sessions"] else None
        tv["positive_rate"] = round(tv["label_1"] / tv["eligible"], 6) if tv["eligible"] else None
    return per_n


def _bounds_utc(b: SplitBounds) -> Dict[str, List[str]]:
    return {
        "train": [b.train_start.strftime(ISO_FMT), b.train_end.strftime(ISO_FMT)],
        "validation": [b.val_start.strftime(ISO_FMT), b.val_end.strftime(ISO_FMT)],
        "test": [b.test_start.strftime(ISO_FMT), b.test_end.strftime(ISO_FMT)],
    }


def analyze_n_candidates(events: DataFrame, n_candidates: Sequence[int], bounds: SplitBounds) -> Dict[str, Any]:
    """Compute the N candidate table for train and validation, plus boundary and leakage QA.

    Parameters
    ----------
    events : DataFrame
        Curated-like events with user_session, event_time, event_type, event_type_rank, event_sort_key.
        Pass a whole-session sample for testing; the full run must be explicitly approved.
    n_candidates : sequence of int
    bounds : SplitBounds
    """
    ns = parse_n_candidates(n_candidates)
    assert_session_utc(spark_of(events))

    total_events = int(events.count())
    valid = valid_session_events(events)
    invalid_events = total_events - int(valid.count())

    sessions = classify_sessions(valid, bounds).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        bucket_rows = sessions.groupBy("bucket").agg(F.count(F.lit(1)).alias("sessions")).collect()
        buckets = {r["bucket"]: {"sessions": int(r["sessions"])} for r in bucket_rows}
        for name in ALL_BUCKETS:
            buckets.setdefault(name, {"sessions": 0})
        null_time_events = int(sessions.agg(F.sum("n_null_time")).collect()[0][0] or 0)

        cand = sessions.filter(F.col("bucket").isin(list(SPLIT_BUCKETS))).select(
            "user_session", "bucket", "session_end"
        )
        cand_events = valid.join(cand.select("user_session"), on="user_session", how="inner").select(
            "user_session", "event_time", "event_type", "event_type_rank", "event_sort_key"
        )

        # QA: no candidate event lies at or after its own split end (must be 0 by construction).
        leak_rows = (
            cand_events.join(cand.select("user_session", "bucket"), on="user_session", how="inner")
            .filter(F.col("event_time") >= _split_end_expr(bounds))
            .count()
        )

        ranked = rank_session_events(cand_events)
        per_session = (
            ranked.groupBy("user_session")
            .agg(
                F.count(F.lit(1)).alias("length"),
                F.min(F.when(F.col("event_type") == "purchase", F.col("event_idx"))).alias("first_purchase_idx"),
            )
            .join(cand.select("user_session", "bucket"), on="user_session", how="inner")
        )

        aggs = [F.count(F.lit(1)).alias("in_scope_all")]
        for n in ns:
            length = F.col("length")
            first = F.col("first_purchase_idx")
            short = length < F.lit(n)
            early = (length >= F.lit(n)) & first.isNotNull() & (first <= F.lit(n))
            eligible = (length >= F.lit(n)) & ~early
            aggs += [
                F.sum(F.when(short, 1).otherwise(0)).alias(f"n{n}_too_short"),
                F.sum(F.when(early, 1).otherwise(0)).alias(f"n{n}_early_purchase"),
                F.sum(F.when(eligible, 1).otherwise(0)).alias(f"n{n}_eligible"),
                F.sum(F.when(eligible & first.isNotNull(), 1).otherwise(0)).alias(f"n{n}_label1"),
                F.sum(F.when(eligible & first.isNull(), 1).otherwise(0)).alias(f"n{n}_label0"),
                F.sum(F.when(length == F.lit(n), 1).otherwise(0)).alias(f"n{n}_len_equals_n"),
            ]
        grouped = per_session.groupBy("bucket").agg(*aggs).collect()
        by_bucket = {r["bucket"]: r.asDict() for r in grouped}
        ranked_buckets = sorted(by_bucket.keys())
        ranked_outside = sum(
            int(v["in_scope_all"]) for k, v in by_bucket.items() if k not in SPLIT_BUCKETS
        )
    finally:
        sessions.unpersist()

    per_n = _build_per_n(ns, by_bucket)

    result = {
        "n_candidates": ns,
        "bounds_utc": _bounds_utc(bounds),
        "events": {
            "total": total_events,
            "invalid_session_events": invalid_events,
            "null_event_time_events_in_valid_sessions": null_time_events,
        },
        "session_buckets": buckets,
        "boundary_qa": {
            "train_censored_sessions": buckets["censored_train"]["sessions"],
            "validation_censored_sessions": buckets["censored_validation"]["sessions"],
            "test_range_sessions_not_labelled": buckets["test_range"]["sessions"],
            "after_test_end_sessions": buckets["after_test_end"]["sessions"],
            "excluded_null_event_time_sessions": buckets["excluded_null_event_time"]["sessions"],
            "gap_sessions": buckets["gap"]["sessions"],
        },
        "leakage_qa": {
            "candidate_events_at_or_after_own_split_end": int(leak_rows),
            "sessions_ranked_by_bucket": sorted(ranked_buckets),
            "ranked_sessions_outside_train_validation": int(ranked_outside),
        },
        "per_n": per_n,
    }
    return result


def diagnose_session_buckets(events: DataFrame, bounds: SplitBounds) -> Dict[str, Any]:
    """Explain every session that is not in train/validation. No ranking, no labels.

    Reads only user_session, event_time. Produces counts per bucket, the start-date distribution
    of out-of-scope sessions, and the number of events with NULL event_time. Any session still in
    'gap' is reported as UNEXPLAINED with its start range, so it can be investigated.
    """
    assert_session_utc(spark_of(events))
    total_events = int(events.count())
    valid = valid_session_events(events)
    null_time_events = int(valid.filter(F.col("event_time").isNull()).count())
    sessions = classify_sessions(valid, bounds).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # Timestamps are formatted INSIDE Spark (session time zone UTC). collect() of a timestamp
        # would convert it to the local time zone of the Python process.
        rows = sessions.groupBy("bucket").agg(
            F.count(F.lit(1)).alias("sessions"),
            F.date_format(F.min("session_start"), SPARK_ISO_UTC_FMT).alias("min_start"),
            F.date_format(F.max("session_start"), SPARK_ISO_UTC_FMT).alias("max_start"),
        ).collect()
        buckets = {
            r["bucket"]: {
                "sessions": int(r["sessions"]),
                "min_start_utc": r["min_start"],
                "max_start_utc": r["max_start"],
            }
            for r in rows
        }
        for name in ALL_BUCKETS:
            buckets.setdefault(name, {"sessions": 0, "min_start_utc": None, "max_start_utc": None})

        after = sessions.filter(F.col("bucket") == "after_test_end")
        by_day = [
            {"date": str(r[0]), "sessions": int(r[1])}
            for r in after.groupBy(F.to_date("session_start").alias("d")).count().orderBy("d").limit(31).collect()
        ]
        after_events = int(
            valid.join(after.select("user_session"), on="user_session", how="inner").count()
        )
        null_start_sessions = int(sessions.filter(F.col("session_start").isNull()).count())
    finally:
        sessions.unpersist()

    gap = buckets["gap"]["sessions"]
    return _diagnosis_dict(total_events, null_time_events, buckets, null_start_sessions, after_events, by_day, gap)


def _diagnosis_dict(total_events, null_time_events, buckets, null_start_sessions, after_events, by_day, gap):
    return {
        "total_events": total_events,
        "null_event_time_events_in_valid_sessions": null_time_events,
        "session_buckets": buckets,
        "sessions_with_null_start": null_start_sessions,
        "after_test_end_events": after_events,
        "after_test_end_sessions_by_day_first_31": by_day,
        "unexplained_gap_sessions": gap,
        "diagnosis": (
            "no unexplained sessions" if gap == 0 else
            f"{gap} sessions are not explained by any rule; investigate before any full run"
        ),
    }


# =============================================================================================
# Staged engine: Phase A (session summary) -> pre-window gate -> Phase B (S2 ranking).
# The legacy analyze_n_candidates above is kept unchanged as the oracle for equivalence tests.
# =============================================================================================

RANKING_INPUTS = ("semi_join", "time_filter")
MATERIALIZE_MODES = ("disk_persist", "staging")
RANK_COLUMNS = ["user_session", "event_time", "event_type", "event_type_rank", "event_sort_key"]
INVALID_KEY_BUCKET = "invalid_session_key"


class WindowGroupGateError(RuntimeError):
    """Raised between Phase A and Phase B when the largest window group exceeds the threshold."""

    def __init__(self, message: str, gate: Dict[str, Any]):
        super().__init__(message)
        self.gate = gate


def capture_plan(df: DataFrame) -> str:
    """Physical plan text of a DataFrame (explain(mode='formatted') prints to stdout in 3.1 and 4.x)."""
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


def _conf_or(spark, key: str, default: str = "unset") -> str:
    try:
        return spark.conf.get(key)
    except Exception:  # noqa: BLE001 - unknown key in this Spark version
        return default


def session_summary(events: DataFrame, b: SplitBounds) -> DataFrame:
    """Phase A input: one row per user_session key over ALL events (including invalid keys).

    Grouping before filtering lets one aggregate also count invalid-session events, so no separate
    count() scan is needed. Invalid keys (NULL or blank) get bucket INVALID_KEY_BUCKET and are never
    treated as sessions. Bucket rules for valid keys are exactly classify_sessions().
    """
    key = F.col("user_session")
    invalid_key = key.isNull() | (F.trim(key) == "")
    grouped = events.groupBy("user_session").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count(F.lit(1)).alias("n_events"),
        F.sum(F.when(F.col("event_time").isNull(), 1).otherwise(0)).alias("n_null_time"),
        # Events Phase B would rank with ranking_input='time_filter' (non-null and before validation end).
        F.sum(F.when(F.col("event_time") < utc_ts_literal(b.val_end), 1).otherwise(0)).alias("n_before_val_end"),
    )
    s = F.col("session_start")
    e = F.col("session_end")

    def inside(lo: datetime, hi: datetime):
        return (s >= utc_ts_literal(lo)) & (s < utc_ts_literal(hi))

    bucket = (
        F.when(invalid_key, INVALID_KEY_BUCKET)
        .when(F.col("n_null_time") > 0, "excluded_null_event_time")
        .when(s.isNull(), "excluded_null_event_time")
        .when(inside(b.train_start, b.train_end) & (e < utc_ts_literal(b.train_end)), "train")
        .when(inside(b.train_start, b.train_end), "censored_train")
        .when(inside(b.val_start, b.val_end) & (e < utc_ts_literal(b.val_end)), "validation")
        .when(inside(b.val_start, b.val_end), "censored_validation")
        .when(inside(b.test_start, b.test_end), "test_range")
        .when(s >= utc_ts_literal(b.test_end), "after_test_end")
        .when(s < utc_ts_literal(b.train_start), "before_train")
        .otherwise("gap")
    )
    return grouped.withColumn("bucket", bucket)


def analyze_n_candidates_staged(
    events: DataFrame,
    n_candidates: Sequence[int],
    bounds: SplitBounds,
    ranking_input: str = "semi_join",
    materialize: str = "disk_persist",
    staging_dir: str = None,
    max_window_group: int = None,
    capture_plans: bool = True,
) -> Dict[str, Any]:
    """Same business result as analyze_n_candidates, executed as Phase A -> gate -> Phase B.

    Phase A: session summary over all events, materialised (DISK_ONLY persist or staging Parquet),
             then ONE aggregate action: buckets, event counts, session-length statistics, leak QA.
    Gate   : largest window group that Phase B will buffer. If max_window_group is set and exceeded,
             WindowGroupGateError is raised and Phase B never starts.
    Phase B: S2 ranking (ml.features.rank_session_events, full key with event_sort_key) on
             - semi_join  : events of train/validation sessions only (LEFT SEMI JOIN with Phase A), or
             - time_filter: events with event_time < validation_end (test-range events never enter);
             then session-level join with Phase A and ONE aggregate action.
    Test sealed: test-range sessions are never ranked; purchase information of test-range events is
             never computed. QA verifies that no test-range session reached Phase B.
    """
    ns = parse_n_candidates(n_candidates)
    if ranking_input not in RANKING_INPUTS:
        raise ValueError(f"ranking_input must be one of {RANKING_INPUTS}")
    if materialize not in MATERIALIZE_MODES:
        raise ValueError(f"materialize must be one of {MATERIALIZE_MODES}")
    if materialize == "staging" and not staging_dir:
        raise ValueError("materialize='staging' requires staging_dir")
    spark = spark_of(events)
    session_tz = assert_session_utc(spark)

    actions: List[str] = []
    plans: Dict[str, str] = {}
    summary_all = session_summary(events, bounds)
    staging_path = None
    persisted = None
    if materialize == "staging":
        staging_path = staging_dir.rstrip("/") + "/session_summary"
        summary_all.write.mode("errorifexists").parquet(staging_path)
        actions.append("A0_write_session_summary_staging")
        summary_all = spark.read.parquet(staging_path)
    else:
        summary_all = summary_all.persist(StorageLevel.DISK_ONLY)
        persisted = summary_all

    try:
        split_list = list(SPLIT_BUCKETS)
        is_split = F.col("bucket").isin(split_list)
        leak = (
            ((F.col("bucket") == "train") & (F.col("session_end") >= utc_ts_literal(bounds.train_end)))
            | ((F.col("bucket") == "validation") & (F.col("session_end") >= utc_ts_literal(bounds.val_end)))
        )
        phase_a = summary_all.groupBy("bucket").agg(
            F.count(F.lit(1)).alias("sessions"),
            F.sum("n_events").alias("events"),
            F.sum("n_null_time").alias("null_time_events"),
            F.max("n_events").alias("max_n_events"),
            F.max("n_before_val_end").alias("max_n_before_val_end"),
            F.sum(F.when(leak, 1).otherwise(0)).alias("leak_sessions"),
            F.expr("percentile_approx(n_events, array(0.5, 0.99, 0.999), 10000)").alias("n_events_pct"),
        )
        if capture_plans:
            plans["phase_a"] = capture_plan(phase_a)
        a_rows = {r["bucket"]: r.asDict() for r in phase_a.collect()}
        actions.append("A1_session_summary_aggregate")

        total_events = sum(int(r["events"] or 0) for r in a_rows.values())
        invalid_events = int((a_rows.get(INVALID_KEY_BUCKET) or {}).get("events") or 0)
        null_time_events = sum(
            int(r["null_time_events"] or 0) for k, r in a_rows.items() if k != INVALID_KEY_BUCKET
        )
        buckets = {name: {"sessions": int((a_rows.get(name) or {}).get("sessions") or 0)} for name in ALL_BUCKETS}
        for k, r in a_rows.items():
            if k not in ALL_BUCKETS and k != INVALID_KEY_BUCKET:
                buckets[k] = {"sessions": int(r["sessions"] or 0)}
        leak_sessions = sum(int(r["leak_sessions"] or 0) for r in a_rows.values())

        if ranking_input == "semi_join":
            group_max = max([int(a_rows[k]["max_n_events"] or 0) for k in split_list if k in a_rows] or [0])
            metric = "max n_events over train/validation sessions (window groups with semi_join)"
        else:
            group_max = max(
                [int(r["max_n_before_val_end"] or 0) for k, r in a_rows.items() if k != INVALID_KEY_BUCKET] or [0]
            )
            metric = "max events before validation_end over all valid sessions (window groups with time_filter)"
        gate = {
            "ranking_input": ranking_input,
            "window_group_max": group_max,
            "metric": metric,
            "threshold": max_window_group,
            "mode": "record_only" if max_window_group is None else "enforce",
            "max_n_events_by_bucket": {k: int(r["max_n_events"] or 0) for k, r in a_rows.items()},
            "n_events_p50_p99_p999_by_split": {
                k: [int(x) for x in (a_rows[k]["n_events_pct"] or [])] for k in split_list if k in a_rows
            },
        }
        if max_window_group is not None and group_max > max_window_group:
            raise WindowGroupGateError(
                f"largest window group {group_max} exceeds threshold {max_window_group}; Phase B not started",
                gate,
            )

        valid = valid_session_events(events)
        if ranking_input == "semi_join":
            cand_ids = summary_all.filter(is_split).select("user_session")
            rank_in = valid.join(cand_ids, on="user_session", how="left_semi").select(*RANK_COLUMNS)
        else:
            rank_in = valid.filter(
                F.col("event_time").isNotNull() & (F.col("event_time") < utc_ts_literal(bounds.val_end))
            ).select(*RANK_COLUMNS)

        ranked = rank_session_events(rank_in)
        per_session = ranked.groupBy("user_session").agg(
            F.count(F.lit(1)).alias("ranked_length"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_idx"))).alias("first_purchase_idx"),
        )
        joined = per_session.join(
            summary_all.filter(F.col("bucket") != INVALID_KEY_BUCKET).select("user_session", "bucket", "n_events"),
            on="user_session",
            how="inner",
        ).withColumn("length", F.col("n_events"))
        mismatch = F.when(F.col("bucket").isin(split_list) & (F.col("ranked_length") != F.col("n_events")), 1).otherwise(0)
        phase_b = joined.groupBy("bucket").agg(
            F.count(F.lit(1)).alias("in_scope_all"),
            F.sum(mismatch).alias("length_mismatch"),
            *_n_aggregates(ns, "length"),
        )
        if capture_plans:
            plans["phase_b"] = capture_plan(phase_b)
        by_bucket = {r["bucket"]: r.asDict() for r in phase_b.collect()}
        actions.append("B1_rank_and_aggregate")
    finally:
        if persisted is not None:
            persisted.unpersist()

    ranked_by_bucket = {k: int(v["in_scope_all"] or 0) for k, v in by_bucket.items()}
    length_mismatch = sum(int(v["length_mismatch"] or 0) for v in by_bucket.values())
    per_n = _build_per_n(ns, {k: v for k, v in by_bucket.items() if k in SPLIT_BUCKETS})

    return {
        "n_candidates": ns,
        "bounds_utc": _bounds_utc(bounds),
        "events": {
            "total": total_events,
            "invalid_session_events": invalid_events,
            "null_event_time_events_in_valid_sessions": null_time_events,
        },
        "session_buckets": buckets,
        "boundary_qa": {
            "train_censored_sessions": buckets["censored_train"]["sessions"],
            "validation_censored_sessions": buckets["censored_validation"]["sessions"],
            "test_range_sessions_not_labelled": buckets["test_range"]["sessions"],
            "after_test_end_sessions": buckets["after_test_end"]["sessions"],
            "excluded_null_event_time_sessions": buckets["excluded_null_event_time"]["sessions"],
            "gap_sessions": buckets["gap"]["sessions"],
        },
        "leakage_qa": {
            "candidate_sessions_ending_at_or_after_own_split_end": leak_sessions,
            "ranked_test_range_sessions": ranked_by_bucket.get("test_range", 0),
            "ranked_sessions_by_bucket": ranked_by_bucket,
            "length_mismatch_between_summary_and_ranking": length_mismatch,
        },
        "per_n": per_n,
        "execution": {
            "engine": "staged",
            "ranking_input": ranking_input,
            "materialize": materialize,
            "staging_path": staging_path,
            "spark_actions": actions,
            "pre_window_gate": gate,
            "spark_conf": {
                "spark.sql.session.timeZone": session_tz,
                "spark.sql.shuffle.partitions": _conf_or(spark, "spark.sql.shuffle.partitions"),
                "spark.sql.adaptive.enabled": _conf_or(spark, "spark.sql.adaptive.enabled"),
                "spark.sql.windowExec.buffer.in.memory.threshold": _conf_or(
                    spark, "spark.sql.windowExec.buffer.in.memory.threshold"),
                "spark.sql.windowExec.buffer.spill.threshold": _conf_or(
                    spark, "spark.sql.windowExec.buffer.spill.threshold"),
                "spark.driver.memory": _conf_or(spark, "spark.driver.memory"),
                "spark.master": _conf_or(spark, "spark.master"),
                "spark.local.dir": _conf_or(spark, "spark.local.dir"),
            },
            "plans": plans,
        },
    }
