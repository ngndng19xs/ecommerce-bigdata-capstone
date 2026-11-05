"""Disk persistence and retry tests for the local B4 sample sink."""

import pytest

pytest.importorskip("fcntl", reason="Local ledger sink requires POSIX")
pytest.importorskip("pyspark", reason="Requires PySpark; run on VM B")

from pyspark.sql import SparkSession

from src.streaming.consumer import ledger_sink as sink


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("B4LedgerSinkTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = """
    kafka_topic string,
    kafka_partition int,
    kafka_offset long,
    kafka_key string,
    kafka_value string,
    user_session string,
    is_valid boolean
"""

OLD = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
NEW = ("ecommerce_events", 0, 11, None, "{broken", None, False)


def make_df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def read_back(spark, root):
    return sink.read_local_ledger(spark, root, make_df(spark, []).schema)


def test_write_and_read_back(spark, tmp_path):
    root = tmp_path / "ledger"
    assert sink.commit_local_ledger_batch(
        make_df(spark, [OLD, NEW]), 0, root
    ) == 2

    result = read_back(spark, root)
    assert result.count() == 2
    assert result.filter("is_valid = false").count() == 1
    assert result.filter("user_session IS NULL").count() == 1


def test_retry_after_publication_adds_nothing(spark, tmp_path):
    root = tmp_path / "ledger"
    batch = make_df(spark, [OLD])

    assert sink.commit_local_ledger_batch(batch, 0, root) == 1
    # Simulate callback retry after data was published.
    assert sink.commit_local_ledger_batch(batch, 0, root) == 0
    assert read_back(spark, root).count() == 1


def test_new_batch_id_does_not_duplicate_old_offset(spark, tmp_path):
    root = tmp_path / "ledger"
    batch = make_df(spark, [OLD])

    assert sink.commit_local_ledger_batch(batch, 0, root) == 1
    assert sink.commit_local_ledger_batch(batch, 99, root) == 0
    assert read_back(spark, root).count() == 1


def test_overlapping_batch_inserts_only_new_message(spark, tmp_path):
    root = tmp_path / "ledger"

    assert sink.commit_local_ledger_batch(
        make_df(spark, [OLD]), 0, root
    ) == 1
    assert sink.commit_local_ledger_batch(
        make_df(spark, [OLD, NEW]), 1, root
    ) == 1
    assert read_back(spark, root).count() == 2


def test_conflict_does_not_modify_saved_ledger(spark, tmp_path):
    root = tmp_path / "ledger"
    sink.commit_local_ledger_batch(make_df(spark, [OLD]), 0, root)

    changed = (
        "ecommerce_events", 0, 10, "s1", "changed", "s1", True
    )

    with pytest.raises(ValueError, match="Conflicting payloads"):
        sink.commit_local_ledger_batch(
            make_df(spark, [changed]), 1, root
        )

    result = read_back(spark, root).collect()
    assert len(result) == 1
    assert result[0]["kafka_value"] == "payload1"


def test_failure_before_publish_can_be_retried(
    spark, tmp_path, monkeypatch
):
    root = tmp_path / "ledger"
    batch = make_df(spark, [OLD])

    def fail_publish(*args, **kwargs):
        raise OSError("simulated publish failure")

    with monkeypatch.context() as patch:
        patch.setattr(sink.os, "rename", fail_publish)
        with pytest.raises(OSError, match="simulated publish failure"):
            sink.commit_local_ledger_batch(batch, 0, root)

    # Complete staging data remains invisible until publication.
    assert read_back(spark, root).count() == 0
    assert any((root / "_staging").iterdir())

    assert sink.commit_local_ledger_batch(batch, 0, root) == 1
    assert read_back(spark, root).count() == 1


def test_incomplete_published_commit_is_rejected(spark, tmp_path):
    root = tmp_path / "ledger"
    (root / "commits" / "broken").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="Incomplete published commit"):
        read_back(spark, root)


def test_empty_batch_creates_no_commit(spark, tmp_path):
    root = tmp_path / "ledger"

    assert sink.commit_local_ledger_batch(
        make_df(spark, []), 0, root
    ) == 0

    assert list((root / "commits").iterdir()) == []
