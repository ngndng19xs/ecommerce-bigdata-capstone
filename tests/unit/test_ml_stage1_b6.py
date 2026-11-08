"""P5 ML stage 1: H11 split table, B6-02 snapshot mode, is_valid rules, gates and B6-01 cross counts."""

import importlib.util
import inspect
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.session_features import build_session_features  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset import ml_stage1 as m  # noqa: E402
from src.ml.dataset.n_selection import (  # noqa: E402
    SplitBounds,
    analyze_n_candidates_staged,
    classify_sessions,
    h11_split_table,
    valid_session_events,
)
from src.ml.training.lr_b6_smoke import FORBIDDEN_FEATURE_COLUMNS, NUMERIC_FEATURES  # noqa: E402

BOUNDS = SplitBounds(
    train_start=datetime(2019, 10, 1), train_end=datetime(2019, 11, 13),
    val_start=datetime(2019, 11, 13), val_end=datetime(2019, 11, 22),
    test_start=datetime(2019, 11, 22), test_end=datetime(2019, 12, 1),
)
RANK = {"view": 1, "cart": 2, "remove_from_cart": 3, "purchase": 4}
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
SCHEMA_IV = StructType(SCHEMA.fields + [StructField("is_valid", BooleanType(), True)])


@pytest.fixture(scope="module")
def spark():
    session = get_spark("TestMLStage1B6")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _sessions():
    """Hand-made sessions covering every H11 bucket and every B6 status (N = 3)."""
    s = []

    def add(name, times_kinds):
        for i, (when, kind) in enumerate(times_kinds):
            s.append((name, when, kind, f"{name}-{i:02d}"))

    add("A", [("2019-10-05 10:00:00", "view"), ("2019-10-05 10:01:00", "view"),
              ("2019-10-05 10:02:00", "view"), ("2019-10-05 10:03:00", "purchase")])     # train, eligible, label 1
    add("B", [("2019-10-05 11:00:00", "view"), ("2019-10-05 11:01:00", "view")])          # train, too_short
    add("C", [("2019-10-06 10:00:00", "view"), ("2019-10-06 10:01:00", "view"),
              ("2019-10-06 10:02:00", "view")])                                           # train, eligible, label 0
    add("D", [("2019-11-12 23:00:00", "view"), ("2019-11-13 01:00:00", "view")])          # censored_train
    add("E", [("2019-11-15 10:00:00", "view"), ("2019-11-15 10:01:00", "view"),
              ("2019-11-15 10:02:00", "view"), ("2019-11-15 10:03:00", "purchase")])     # validation, label 1
    add("F", [("2019-11-16 10:00:00", "view"), ("2019-11-16 10:01:00", "view"),
              ("2019-11-16 10:02:00", "purchase"), ("2019-11-16 10:03:00", "view")])     # validation, purchased_by_n
    add("G", [("2019-11-25 10:00:00", "view"), ("2019-11-25 10:01:00", "view"),
              ("2019-11-25 10:02:00", "view"), ("2019-11-25 10:03:00", "purchase")])     # test_range, label 1
    add("H", [("2019-09-30 10:00:00", "view"), ("2019-09-30 10:01:00", "view"),
              ("2019-09-30 10:02:00", "view")])                                           # before_train
    return s


def _df(spark, rows, with_is_valid=False):
    data = []
    for row in rows:
        session, when, kind, key = row[:4]
        base = (session, when, kind, RANK[kind], key, "P1", "C1", "A", 10.0)
        data.append(base + ((row[4],) if with_is_valid else ()))
    schema = SCHEMA_IV if with_is_valid else SCHEMA
    df = spark.createDataFrame(data, schema)
    return df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))


@pytest.fixture(scope="module")
def events(spark):
    return _df(spark, _sessions())


def _counts_by(df, col):
    return {str(r[0]): int(r[1]) for r in df.groupBy(col).count().collect()}


# --- H11 split table -----------------------------------------------------------------------------


def test_h11_split_table_matches_classify_sessions(spark, events):
    keys = h11_split_table(events, BOUNDS)
    assert _counts_by(keys, "h11_split") == {"train": 3, "validation": 2, "test": 1}
    buckets = _counts_by(classify_sessions(valid_session_events(events), BOUNDS), "bucket")
    assert buckets["censored_train"] == 1 and buckets["before_train"] == 1
    # the table must hold exactly the sessions of the three split buckets, nothing else
    names = {r[0] for r in keys.select("user_session").collect()}
    assert names == {"A", "B", "C", "E", "F", "G"}


def test_h11_split_table_excludes_censored_and_before_train(spark, events):
    names = {r[0] for r in h11_split_table(events, BOUNDS).select("user_session").collect()}
    assert "D" not in names and "H" not in names


# --- B6-02 core ----------------------------------------------------------------------------------


def test_build_snapshot_features_counts_match_b6_01(spark, events):
    """Same fixture, same N: B6-02 eligible and label counts equal the B6-01 per-N table."""
    dataset, stats = m.build_snapshot_features(events, 3, BOUNDS, is_valid_rule=None)
    result = analyze_n_candidates_staged(events, [3], BOUNDS, capture_plans=False)
    per_n = result["per_n"]["3"]
    dataset_by = _counts_by(dataset, "h11_split")
    assert dataset_by.get("train", 0) == per_n["train"]["eligible"] == 2
    assert dataset_by.get("validation", 0) == per_n["validation"]["eligible"] == 1
    labels = stats["label_counts_by_split"]
    assert labels["train"] == {"0": 1, "1": 1}
    assert per_n["train"]["label_1"] == 1 and per_n["validation"]["label_1"] == 1
    assert stats["dataset_rows_by_split"] == {"train": 2, "validation": 1, "test": 1}
    dataset.unpersist()


def test_dataset_holds_only_eligible_sessions_of_the_three_splits(spark, events):
    dataset, _ = m.build_snapshot_features(events, 3, BOUNDS, is_valid_rule=None)
    names = {r[0] for r in dataset.select("user_session").collect()}
    assert names == {"A", "C", "E", "G"}  # B too short, F purchased by N, D censored, H before train
    dataset.unpersist()


def test_session_keys_restriction_gives_the_same_rows(spark, events):
    full = build_session_features(events, 3)[0]
    keys = spark.createDataFrame([("A",), ("C",)], "user_session string")
    part = build_session_features(events, 3, session_keys=keys)[0]
    cols = ["user_session", "n_view", "n_cart", "avg_price", "prefix_duration_seconds", "last_event_type", "label"]
    a = sorted(tuple(r) for r in full.select(*cols).filter(F.col("user_session").isin("A", "C")).collect())
    b = sorted(tuple(r) for r in part.select(*cols).collect())
    assert a == b and len(b) == 2


def test_default_call_keeps_the_legacy_signature(spark, events):
    dataset, audit, invalid = build_session_features(events, 3)
    # eligible without any split restriction: A, C, E, G and H (H is before train but has 3 views)
    assert dataset.count() == 5 and audit.count() == 8 and invalid.count() == 0


def test_gate_refuses_long_sessions_before_ranking(spark):
    rows = _sessions() + [("L", f"2019-10-07 10:0{i}:00", "view", f"L-{i:02d}") for i in range(6)]
    long_events = _df(spark, rows)
    with pytest.raises(m.SessionLengthGateError) as info:
        m.build_snapshot_features(long_events, 3, BOUNDS, is_valid_rule=None, max_session_events_gate=5)
    assert info.value.max_len == 6
    dataset, stats = m.build_snapshot_features(long_events, 3, BOUNDS, is_valid_rule=None, max_session_events_gate=None)
    assert stats["max_session_events"] == 6
    dataset.unpersist()


# --- is_valid rules -----------------------------------------------------------------------------


def _iv_rows():
    rows = [(s, w, k, key, True) for (s, w, k, key) in _sessions()]
    rows += [("I", f"2019-10-08 10:0{i}:00", "view", f"I-{i:02d}", False) for i in range(3)]
    return rows


def test_is_valid_without_a_rule_is_a_waiting_decision(spark):
    events = _df(spark, _iv_rows(), with_is_valid=True)
    with pytest.raises(m.DecisionRequiredError):
        m.build_snapshot_features(events, 3, BOUNDS, is_valid_rule=None)


def test_is_valid_ignore_keeps_flagged_events_like_b6_01(spark):
    events = _df(spark, _iv_rows(), with_is_valid=True)
    dataset, stats = m.build_snapshot_features(events, 3, BOUNDS, is_valid_rule="ignore")
    assert "I" in {r[0] for r in dataset.select("user_session").collect()}
    assert stats["is_valid_false_rows"] == 3
    dataset.unpersist()


def test_is_valid_apply_removes_flagged_events(spark):
    events = _df(spark, _iv_rows(), with_is_valid=True)
    dataset, _ = m.build_snapshot_features(events, 3, BOUNDS, is_valid_rule="apply")
    assert "I" not in {r[0] for r in dataset.select("user_session").collect()}
    dataset.unpersist()


# --- guards -------------------------------------------------------------------------------------


def test_features_never_use_split_or_audit_columns():
    assert {"h11_split", "session_start", "session_end", "session_length", "purchased_by_n", "status"} \
        <= FORBIDDEN_FEATURE_COLUMNS
    assert not (set(NUMERIC_FEATURES) & FORBIDDEN_FEATURE_COLUMNS)


def test_snapshot_path_never_calls_sample_whole_sessions():
    assert "sample_whole_sessions" not in inspect.getsource(m.build_snapshot_features)
    spec = importlib.util.spec_from_file_location("run_b6_session_features",
                                                  str(ROOT / "scripts" / "run" / "run_b6_session_features.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert "sample_whole_sessions" not in inspect.getsource(module.run_snapshot)


def test_snapshot_mode_refuses_a_root_directory(tmp_path):
    spec = importlib.util.spec_from_file_location("run_b6_session_features_2",
                                                  str(ROOT / "scripts" / "run" / "run_b6_session_features.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = tmp_path / "snapshot_manifest.json"
    manifest.write_text('{"kind": "snapshot", "threshold": 455}', encoding="utf-8")
    args = module.parse_args([
        "--input-kind", "snapshot", "--n", "3", "--snapshot-dir", str(tmp_path / "snapshot"),
        "--snapshot-manifest", str(manifest), "--output-path", str(tmp_path / "out"),
        "--stats-dir", str(tmp_path / "stats"),
    ])
    (tmp_path / "snapshot").mkdir()
    assert module.run_snapshot(args) == 2  # root (not part=sample) is refused before Spark starts
