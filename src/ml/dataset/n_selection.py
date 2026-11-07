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
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

from ml.features import rank_session_events

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"
SPLIT_BUCKETS = ("train", "validation")
ALL_BUCKETS = (
    "train",
    "censored_train",
    "validation",
    "censored_validation",
    "test_range",
    "gap",
    "before_train",
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
    )
    s = F.col("session_start")
    e = F.col("session_end")

    def inside(lo: datetime, hi: datetime):
        return (s >= F.lit(lo)) & (s < F.lit(hi))

    bucket = (
        F.when(inside(b.train_start, b.train_end) & (e < F.lit(b.train_end)), "train")
        .when(inside(b.train_start, b.train_end), "censored_train")
        .when(inside(b.val_start, b.val_end) & (e < F.lit(b.val_end)), "validation")
        .when(inside(b.val_start, b.val_end), "censored_validation")
        .when(inside(b.test_start, b.test_end), "test_range")
        .when(s < F.lit(b.train_start), "before_train")
        .otherwise("gap")
    )
    return sess.withColumn("bucket", bucket)


def _split_end_expr(b: SplitBounds):
    return F.when(F.col("bucket") == "train", F.lit(b.train_end)).otherwise(F.lit(b.val_end))


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

    total_events = int(events.count())
    valid = valid_session_events(events)
    invalid_events = total_events - int(valid.count())

    sessions = classify_sessions(valid, bounds).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        bucket_rows = sessions.groupBy("bucket").agg(F.count(F.lit(1)).alias("sessions")).collect()
        buckets = {r["bucket"]: {"sessions": int(r["sessions"])} for r in bucket_rows}
        for name in ALL_BUCKETS:
            buckets.setdefault(name, {"sessions": 0})

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
        "bounds_utc": {
            "train": [bounds.train_start.isoformat(), bounds.train_end.isoformat()],
            "validation": [bounds.val_start.isoformat(), bounds.val_end.isoformat()],
            "test": [bounds.test_start.isoformat(), bounds.test_end.isoformat()],
        },
        "events": {"total": total_events, "invalid_session_events": invalid_events},
        "session_buckets": buckets,
        "boundary_qa": {
            "train_censored_sessions": buckets["censored_train"]["sessions"],
            "validation_censored_sessions": buckets["censored_validation"]["sessions"],
            "test_range_sessions_not_labelled": buckets["test_range"]["sessions"],
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
