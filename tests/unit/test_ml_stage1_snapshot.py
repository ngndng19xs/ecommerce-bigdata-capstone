"""P5 ML stage 1: snapshot builder, nested subsets and statistics (Spark, small fixtures only)."""

import sys
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset import ml_stage1 as m  # noqa: E402
from src.ml.dataset.n_selection import SplitBounds  # noqa: E402
from src.ml.training.lr_b6_smoke import session_bucket_expr  # noqa: E402

BOUNDS = SplitBounds(
    train_start=datetime(2019, 10, 1), train_end=datetime(2019, 11, 13),
    val_start=datetime(2019, 11, 13), val_end=datetime(2019, 11, 22),
    test_start=datetime(2019, 11, 22), test_end=datetime(2019, 12, 1),
)

SCHEMA = StructType([
    StructField("user_session", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_type_rank", IntegerType(), True),
    StructField("event_sort_key", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
])
RANK = {"view": 1, "cart": 2, "remove_from_cart": 3, "purchase": 4}


@pytest.fixture(scope="module")
def spark():
    session = get_spark("TestMLStage1Snapshot")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_events(spark, rows):
    """rows: (session, 'YYYY-MM-DD HH:MM:SS' UTC, event_type, sort_key). Parsed in session time zone UTC."""
    data = []
    for session, when, kind, key in rows:
        data.append((session, when, kind, RANK[kind], key, "P1", "C1", "A", 10.0))
    df = spark.createDataFrame(data, SCHEMA)
    return df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))


def buckets_of(spark, keys):
    rows = spark.createDataFrame([(k,) for k in keys], "user_session string").select(
        "user_session", session_bucket_expr().alias("b")
    ).collect()
    return {r["user_session"]: int(r["b"]) for r in rows}


def key_below(spark, threshold, prefix="below"):
    keys = [f"{prefix}-{i:04d}" for i in range(300)]
    b = buckets_of(spark, keys)
    return next(k for k in keys if b[k] < threshold)


def key_at_or_above(spark, threshold, prefix="above"):
    keys = [f"{prefix}-{i:04d}" for i in range(300)]
    b = buckets_of(spark, keys)
    return next(k for k in keys if b[k] >= threshold)


# --- hash, validity and partitions --------------------------------------------------------------


def test_hash_expression_is_the_engine_expression(spark):
    keys = [f"k-{i}" for i in range(200)]
    df = spark.createDataFrame([(k,) for k in keys], "user_session string")
    got = df.select("user_session", m.hash_bucket_col().alias("a"), session_bucket_expr().alias("b")).collect()
    assert all(r["a"] == r["b"] for r in got)
    assert all(0 <= r["a"] < 10000 for r in got)


def test_invalid_keys_follow_the_engine_rule(spark):
    rows = [
        ("", "2019-10-05 10:00:00", "view", "e1"),
        ("   ", "2019-10-05 10:00:00", "view", "e2"),
        ("\t", "2019-10-05 10:00:00", "view", "e3"),
        ("ok-session", "2019-10-05 10:00:00", "view", "e4"),
    ]
    raw = make_events(spark, rows)
    # NULL key is written through a separate row because a NULL cannot be produced from a tuple of str
    null_row = make_events(spark, [("x", "2019-10-05 10:00:00", "view", "e5")]).withColumn(
        "user_session", F.lit(None).cast("string")
    )
    frame, part = m.build_snapshot_frame(raw.unionByName(null_row), 455)
    tagged = {r["event_sort_key"]: r[part] for r in frame.select("event_sort_key", part).collect()}
    assert tagged.get("e1") == "invalid_key"
    assert tagged.get("e2") == "invalid_key"
    assert tagged.get("e5") == "invalid_key"
    assert tagged.get("e3") != "invalid_key"  # a tab is not trimmed by the engine rule, so the key is valid


def test_invalid_cause_labels(spark):
    raw = make_events(spark, [("", "2019-10-05 10:00:00", "view", "a"), ("  ", "2019-10-05 10:00:00", "view", "b")])
    causes = {r["event_sort_key"]: r["cause"] for r in raw.select(
        "event_sort_key", m.invalid_cause_col().alias("cause")).collect()}
    assert causes == {"a": "empty", "b": "whitespace_only"}


def test_partition_column_is_renamed_when_source_has_part(spark):
    raw = make_events(spark, [("s1", "2019-10-05 10:00:00", "view", "a")]).withColumn("part", F.lit("x"))
    _, part = m.build_snapshot_frame(raw, 455)
    assert part == "snapshot_part"


def test_building_the_frame_triggers_no_action(spark):
    raw = make_events(spark, [("s1", "2019-10-05 10:00:00", "view", "a"), ("s2", "2019-10-05 10:00:00", "cart", "b")])
    with mock.patch("pyspark.sql.DataFrame.count", side_effect=AssertionError("action on source")), \
            mock.patch("pyspark.sql.DataFrame.collect", side_effect=AssertionError("action on source")):
        frame, part = m.build_snapshot_frame(raw, 455)
    assert part == "part"
    assert frame is not None


def test_threshold_out_of_range_is_refused():
    with pytest.raises(ValueError):
        m.check_threshold(0)
    with pytest.raises(ValueError):
        m.check_threshold(10001)


# --- whole sessions and nested subsets ----------------------------------------------------------


def test_whole_session_is_kept_across_days(spark):
    inside = key_below(spark, 455, "in")
    rows = [
        (inside, "2019-10-31 23:59:00", "view", "w1"),
        (inside, "2019-11-01 00:01:00", "cart", "w2"),
        (inside, "2019-11-02 09:00:00", "purchase", "w3"),
    ]
    raw = make_events(spark, rows)
    frame, part = m.build_snapshot_frame(raw, 455)
    kept = frame.filter(F.col(part) == "sample").count()
    assert kept == 3, "every event of a selected session must be kept, even across a day boundary"


def test_nested_subsets_10_inside_57_inside_455(spark):
    """Keys are chosen from their REAL buckets, so the test does not depend on luck."""
    candidates = [f"nest-{i:05d}" for i in range(20000)]
    b = buckets_of(spark, candidates)
    in10 = [k for k in candidates if b[k] < 10][:3]
    in57 = [k for k in candidates if 10 <= b[k] < 57][:3]
    in455 = [k for k in candidates if 57 <= b[k] < 455][:3]
    assert len(in10) == len(in57) == len(in455) == 3
    rows = []
    for key in in10 + in57 + in455 + [k for k in candidates if b[k] >= 455][:2]:
        rows.append((key, "2019-10-05 10:00:00", "view", f"{key}-a"))
        rows.append((key, "2019-10-05 10:01:00", "cart", f"{key}-b"))
    raw = make_events(spark, rows)
    frame, part = m.build_snapshot_frame(raw, 455)
    sample = frame.filter(F.col(part) == "sample").drop(part)
    s10 = {r[0] for r in m.subset_frame(sample, 10).select("user_session").distinct().collect()}
    s57 = {r[0] for r in m.subset_frame(sample, 57).select("user_session").distinct().collect()}
    s455 = {r[0] for r in sample.select("user_session").distinct().collect()}
    assert s10 == set(in10)
    assert s57 == set(in10 + in57)
    assert s455 == set(in10 + in57 + in455)
    assert s10 <= s57 <= s455


def test_write_snapshot_and_recompute_stats(spark, tmp_path):
    inside = key_below(spark, 455, "snap-in")
    outside = key_at_or_above(spark, 455, "snap-out")
    rows = [
        (inside, "2019-10-05 10:00:00", "view", "s1"),
        (inside, "2019-11-15 10:00:00", "purchase", "s2"),
        (outside, "2019-10-06 10:00:00", "view", "s3"),
        ("", "2019-10-06 10:00:00", "view", "s4"),
    ]
    raw = make_events(spark, rows)
    snap = tmp_path / "snapshot"
    part = m.write_snapshot(raw, m.file_uri(str(snap)), 455, coalesce_n=2)
    assert part == "part"
    stats = m.compute_part_stats(spark, str(snap / "part=sample"), BOUNDS, 455)
    assert stats["rows"] == 2
    assert stats["distinct_sessions"] == 1
    assert stats["hash_violations"] == 0
    assert stats["invalid_key_rows"] == 0
    # starts in train but ends on 2019-11-15, after train_end: censored, never labelled as train
    assert stats["h11_bucket_sessions"] == {"censored_train": 1}
    inv = m.compute_invalid_stats(spark, str(snap / "part=invalid_key"))
    assert inv["rows"] == 1
    assert inv["by_cause"]["empty"] == 1


def test_subset_matches_direct_count(spark, tmp_path):
    keys = [f"sub-{i:04d}" for i in range(250)]
    rows = [(k, "2019-10-05 10:00:00", "view", f"{k}-x") for k in keys]
    raw = make_events(spark, rows)
    frame, part = m.build_snapshot_frame(raw, 455)
    sample = frame.filter(F.col(part) == "sample").drop(part)
    expected = raw.join(
        spark.createDataFrame([(k,) for k in keys], "user_session string").select(
            "user_session", session_bucket_expr().alias("b")
        ).filter(F.col("b") < 10).select("user_session"),
        on="user_session",
    ).count()
    out = tmp_path / "s10"
    m.write_subset(sample, m.file_uri(str(out)), 10, coalesce_n=1)
    assert spark.read.parquet(m.file_uri(str(out))).count() == expected


def test_compare_stats_reports_differences():
    expected = {"rows": 5, "schema_names": ["b", "a"], "month_counts": {"2019-10": 5}}
    assert m.compare_stats(expected, {"rows": 5, "schema_names": ["a", "b"], "month_counts": {"2019-10": 5}}) == []
    problems = m.compare_stats(expected, {"rows": 4, "schema_names": ["a", "b"], "month_counts": {"2019-10": 4}})
    assert any(p.startswith("rows") for p in problems)
    assert any(p.startswith("month_counts") for p in problems)


def test_manifest_scope_is_never_full():
    fields = m.manifest_scope_fields({"kind": "subset", "threshold": 10, "name": "check_10"})
    assert fields["source_scope"] == "SAMPLED_SESSIONS"
    assert fields["sample_threshold"] == 10
