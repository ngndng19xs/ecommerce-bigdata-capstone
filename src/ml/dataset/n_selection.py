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


def _split_end_expr(b: SplitBounds):
    return F.when(F.col("bucket") == "train", utc_ts_literal(b.train_end)).otherwise(utc_ts_literal(b.val_end))


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
