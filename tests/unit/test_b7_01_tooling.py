"""Tests for the P4-B-B7-01 acceptance tooling: source manifest, evidence checker, whole-session sampling."""

import importlib.util
import sys
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.training.lr_b6_smoke import sample_whole_sessions  # noqa: E402


def _load_script(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sm = _load_script("source_manifest_under_test", "scripts/run/source_manifest.py")
ck = _load_script("check_evidence_under_test", "scripts/run/check_b7_01_evidence.py")


# --- Source manifest ----------------------------------------------------------


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "x.py").write_text("print('x')\n", encoding="utf-8")
    (tmp_path / "b.txt").write_text("hello", encoding="utf-8")
    monkeypatch.setattr(sm, "ROOT", str(tmp_path))
    monkeypatch.setattr(sm, "KEY_FILES", ["a/x.py", "b.txt"])
    return tmp_path


def test_manifest_roundtrip_pass(fake_repo):
    assert sm.cmd_make(Namespace()) == 0
    assert sm.cmd_verify(Namespace(out=None)) == 0


def test_manifest_detects_content_change_fail(fake_repo):
    sm.cmd_make(Namespace())
    (fake_repo / "b.txt").write_text("changed", encoding="utf-8")
    assert sm.cmd_verify(Namespace(out=None)) == 1


def test_manifest_missing_is_blocked_not_pass(fake_repo):
    assert sm.cmd_verify(Namespace(out=None)) == 2


def test_manifest_contains_no_env_or_identity_fields(fake_repo):
    sm.cmd_make(Namespace())
    text = (fake_repo / sm.MANIFEST_REL).read_text(encoding="utf-8").lower()
    for forbidden in ("token", "password", "secret", "@gmail", "user_email"):
        assert forbidden not in text


def test_manifest_key_files_exist_in_repo():
    for rel in sm.KEY_FILES:
        assert (ROOT / rel).is_file(), rel


# --- Evidence checker ---------------------------------------------------------


def _good_inputs():
    smoke = {
        "smoke_status": "PASS",
        "exit_code": 0,
        "prediction_n": 2,
        "split": {
            "train_dates": ["2019-10-01", "2019-10-02"],
            "val_dates": ["2019-10-03"],
            "dates_not_read": 4,
            "distinct_session_dates_total": 7,
        },
        "train": {"pos": 10, "neg": 20},
        "validation": {
            "validation_rows_in": 5, "validation_rows_out": 5,
            "bad_prob_size": 0, "bad_prob_value": 0, "bad_features": 0, "null_prediction": 0,
        },
    }
    writer = {
        "output_session_rows": 30,
        "output_label_counts": {"0": 20, "1": 10},
        "sampled_distinct_sessions": 100,
        "prediction_n": 2,
    }
    tc16 = {"status": "PASS", "files_matched": 3, "files_total": 3}
    return smoke, writer, tc16


def test_checker_pass_when_all_evidence_consistent():
    crit, overall = ck.evaluate(*_good_inputs())
    assert overall == "PASS"
    assert all(c["status"] == "PASS" for c in crit)


def test_checker_does_not_hardcode_dates_not_read_one():
    smoke, writer, tc16 = _good_inputs()
    # 61 distinct dates with 42 train + 9 val leaves 10 not read: must PASS by identity, not by value 1.
    smoke["split"] = {
        "train_dates": [f"d{i:02d}" for i in range(42)],
        "val_dates": [f"d{i:02d}" for i in range(42, 51)],
        "dates_not_read": 10,
        "distinct_session_dates_total": 61,
    }
    _, overall = ck.evaluate(smoke, writer, tc16)
    assert overall == "PASS"


def test_checker_fails_when_date_identity_breaks():
    smoke, writer, tc16 = _good_inputs()
    smoke["split"]["dates_not_read"] = 3  # 7 != 2 + 1 + 3
    _, overall = ck.evaluate(smoke, writer, tc16)
    assert overall == "FAIL"


def test_checker_fails_when_source_not_verified():
    smoke, writer, tc16 = _good_inputs()
    tc16["status"] = "BLOCKED"
    _, overall = ck.evaluate(smoke, writer, tc16)
    assert overall == "FAIL"


def test_checker_fails_when_smoke_failed():
    smoke, writer, tc16 = _good_inputs()
    smoke.update(smoke_status="FAIL", exit_code=1)
    _, overall = ck.evaluate(smoke, writer, tc16)
    assert overall == "FAIL"


def test_checker_fails_when_writer_has_single_class():
    smoke, writer, tc16 = _good_inputs()
    writer["output_label_counts"] = {"0": 30}
    _, overall = ck.evaluate(smoke, writer, tc16)
    assert overall == "FAIL"


# --- Whole-session sampling ---------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    session = get_spark("TestB7Tooling")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _sessions_df(spark):
    schema = StructType([
        StructField("user_session", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_type_rank", IntegerType(), True),
        StructField("event_sort_key", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    base = datetime(2019, 10, 31, 23, 59, 0)  # crosses midnight on purpose
    rows = []
    for s in range(40):
        for step in range(1 + s % 4):
            rows.append((f"sess_{s:03d}", base + timedelta(seconds=30 * step), "view", 1, f"{s:064x}", 1.0))
    return spark.createDataFrame(rows, schema)


def test_sampling_keeps_all_events_of_each_selected_session(spark):
    df = _sessions_df(spark)
    original = {r[0]: r[1] for r in df.groupBy("user_session").count().collect()}
    sampled = sample_whole_sessions(df, 50.0)
    kept = {r[0]: r[1] for r in sampled.groupBy("user_session").count().collect()}
    assert kept, "sample must not be empty for 40 sessions at 50 percent"
    assert len(kept) < len(original)
    for sid, n_events in kept.items():
        assert n_events == original[sid], f"session {sid} was cut"


def test_sampling_is_reproducible(spark):
    df = _sessions_df(spark)
    first = sorted(r[0] for r in sample_whole_sessions(df, 50.0).select("user_session").distinct().collect())
    second = sorted(r[0] for r in sample_whole_sessions(df, 50.0).select("user_session").distinct().collect())
    assert first == second


def test_sampling_rejects_invalid_percent(spark):
    df = _sessions_df(spark)
    with pytest.raises(ValueError):
        sample_whole_sessions(df, 0.0)
    with pytest.raises(ValueError):
        sample_whole_sessions(df, 150.0)
    with pytest.raises(ValueError):
        sample_whole_sessions(df, 0.001)  # rounds to 0 buckets


def _ids_df(spark, ids):
    schema = StructType([StructField("user_session", StringType(), True)])
    return spark.createDataFrame([(i,) for i in ids], schema)


def test_bucket_is_non_negative_for_negative_raw_hashes(spark):
    """xxhash64 can be negative; pmod must map every value into [0, 10000)."""
    ids = [f"sess-{i:05d}" for i in range(3000)]
    rows = _ids_df(spark, ids).select(
        F.expr("xxhash64(user_session)").alias("h"),
        F.expr("pmod(xxhash64(user_session), 10000)").alias("b"),
    ).collect()
    assert any(r["h"] < 0 for r in rows), "fixture must contain negative raw hashes"
    buckets = [r["b"] for r in rows]
    assert min(buckets) >= 0
    assert max(buckets) < 10000
    # Negative raw hashes must land in the same bucket as Python's non-negative modulo.
    for r in rows:
        assert r["b"] == r["h"] % 10000


def test_bucket_positive_hashes_and_threshold_selection(spark):
    ids = [f"pos-{i:05d}" for i in range(3000)]
    rows = _ids_df(spark, ids).select(
        F.expr("xxhash64(user_session)").alias("h"),
        F.expr("pmod(xxhash64(user_session), 10000)").alias("b"),
    ).collect()
    assert any(r["h"] > 0 for r in rows)
    kept = _ids_df(spark, ids).filter(F.expr("pmod(xxhash64(user_session), 10000)") < 500).count()
    expected = sum(1 for r in rows if r["b"] < 500)
    assert kept == expected
    assert 0 < kept < len(ids)


def test_null_session_decision_is_deterministic_and_does_not_fail(spark):
    """Null user_session rows are hashed like any value; B6 later drops them as invalid."""
    df = _ids_df(spark, [None, None, "a", "b"])
    first = sorted((r["user_session"], r["b"]) for r in df.select(
        "user_session", F.expr("pmod(xxhash64(user_session), 10000)").alias("b")).collect()
        if r["user_session"] is None)
    second = sorted((r["user_session"], r["b"]) for r in df.select(
        "user_session", F.expr("pmod(xxhash64(user_session), 10000)").alias("b")).collect()
        if r["user_session"] is None)
    assert first == second
    assert all(0 <= b < 10000 for _, b in first)
    sampled = sample_whole_sessions(df, 100.0).count()
    assert sampled == 4  # at 100 percent every row (including the two null-session rows) is kept


def test_sampling_repeatable_across_partitionings(spark):
    ids = [f"rep-{i:05d}" for i in range(400)]
    df = _ids_df(spark, ids)
    a = sorted(r[0] for r in sample_whole_sessions(df, 10.0).collect())
    b = sorted(r[0] for r in sample_whole_sessions(df.repartition(7), 10.0).collect())
    c = sorted(r[0] for r in sample_whole_sessions(df, 10.0).collect())
    assert a == b == c
    assert 0 < len(a) < len(ids)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
