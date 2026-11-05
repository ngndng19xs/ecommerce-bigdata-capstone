"""
Unit tests for Temporal Split module (Task S2 / ML Contract v1.0 Section 9.3).

Tests cover:
1. Cutoff date parsing and format validation (UTC YYYY-MM-DD).
2. Strict ordering validation: train_end_utc < validation_end_utc.
3. Edge cases: equal dates, inverted dates, non-date strings, None/null inputs.
4. Chronological partition disjointness: Train < Val < Test with zero session overlap.
5. PySpark integration test (executed on VM-B).
"""

from datetime import date, datetime
import pytest
from src.ml.dataset.split import validate_cutoff_dates


def test_validate_cutoff_dates_valid():
    """Valid YYYY-MM-DD UTC strings in strict ascending order must parse cleanly."""
    t_date, v_date = validate_cutoff_dates("2019-11-01", "2019-11-15")
    assert t_date == date(2019, 11, 1)
    assert v_date == date(2019, 11, 15)
    assert t_date < v_date


def test_validate_cutoff_dates_with_whitespace():
    """Cutoff dates with leading/trailing whitespace must be trimmed and parsed."""
    t_date, v_date = validate_cutoff_dates("  2019-10-15  ", "  2019-10-25  ")
    assert t_date == date(2019, 10, 15)
    assert v_date == date(2019, 10, 25)


def test_validate_cutoff_dates_rejects_equal_dates():
    """Equal train and val cutoffs must be rejected (Train < Val required)."""
    with pytest.raises(ValueError, match="strictly earlier"):
        validate_cutoff_dates("2019-11-10", "2019-11-10")


def test_validate_cutoff_dates_rejects_inverted_dates():
    """Inverted dates (train > val) must be rejected."""
    with pytest.raises(ValueError, match="strictly earlier"):
        validate_cutoff_dates("2019-11-20", "2019-11-10")


def test_validate_cutoff_dates_rejects_invalid_format():
    """Invalid date format (e.g. DD/MM/YYYY, text) must raise ValueError."""
    with pytest.raises(ValueError, match="Cutoff dates must be in UTC YYYY-MM-DD format"):
        validate_cutoff_dates("10-11-2019", "2019-11-15")

    with pytest.raises(ValueError, match="Cutoff dates must be in UTC YYYY-MM-DD format"):
        validate_cutoff_dates("2019-11-01", "not-a-date")


def test_validate_cutoff_dates_rejects_none():
    """None or non-string inputs must raise ValueError."""
    with pytest.raises(ValueError, match="Cutoff dates must be in UTC YYYY-MM-DD format"):
        validate_cutoff_dates(None, "2019-11-15")


def test_temporal_split_disjoint_logic_simulation():
    """Simulate temporal cutoff splitting to prove zero session leakage across sets.

    Guarantees:
    1. Every session is assigned to exactly ONE split.
    2. Train max date <= train_end_utc.
    3. Validation min date > train_end_utc and max date <= validation_end_utc.
    4. Test min date > validation_end_utc.
    5. Sets are mutually disjoint.
    """
    train_end = "2019-10-20"
    val_end = "2019-10-25"

    sessions = [
        {"session_id": f"s_{day}_{i}", "session_start_date": f"2019-10-{day:02d}"}
        for day in range(1, 31)
        for i in range(2)
    ]

    train_set = [s for s in sessions if s["session_start_date"] <= train_end]
    val_set = [
        s
        for s in sessions
        if train_end < s["session_start_date"] <= val_end
    ]
    test_set = [s for s in sessions if s["session_start_date"] > val_end]

    train_ids = set(s["session_id"] for s in train_set)
    val_ids = set(s["session_id"] for s in val_set)
    test_ids = set(s["session_id"] for s in test_set)

    # All sessions accounted for
    assert len(train_ids) + len(val_ids) + len(test_ids) == len(sessions)

    # Mutual disjointness (anti-leakage)
    assert len(train_ids.intersection(val_ids)) == 0
    assert len(val_ids.intersection(test_ids)) == 0
    assert len(train_ids.intersection(test_ids)) == 0

    # Chronological boundaries
    assert max(s["session_start_date"] for s in train_set) <= train_end
    assert min(s["session_start_date"] for s in val_set) > train_end
    assert max(s["session_start_date"] for s in val_set) <= val_end
    assert min(s["session_start_date"] for s in test_set) > val_end


# ---------------------------------------------------------------------------
# PySpark Integration Test (Executed on VM-B)
# ---------------------------------------------------------------------------


def test_pyspark_split_by_cutoff_dates():
    """Full PySpark test for split_by_cutoff_dates."""
    pyspark = pytest.importorskip("pyspark", reason="PySpark is required for split_by_cutoff_dates")
    from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
    from src.common.spark_session import get_spark
    from src.ml.dataset.split import split_by_cutoff_dates

    spark = get_spark("TestTemporalSplitCutoffs")
    try:
        schema = StructType(
            [
                StructField("user_session", StringType(), True),
                StructField("session_start_date", DateType(), True),
                StructField("label", IntegerType(), True),
            ]
        )
        rows = [
            ("s1", date(2019, 10, 10), 0),
            ("s2", date(2019, 10, 15), 1),
            ("s3", date(2019, 10, 22), 0),
            ("s4", date(2019, 10, 24), 1),
            ("s5", date(2019, 10, 26), 0),
            ("s6", date(2019, 10, 28), 1),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        train_df, val_df, test_df, meta = split_by_cutoff_dates(
            df, train_end_utc="2019-10-20", validation_end_utc="2019-10-25"
        )
        assert train_df.count() == 2
        assert val_df.count() == 2
        assert test_df.count() == 2
        assert meta["train"]["total_sessions"] == 2
        assert meta["val"]["total_sessions"] == 2
        assert meta["test"]["total_sessions"] == 2
    finally:
        spark.stop()
