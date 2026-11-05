"""Small Spark tests for B4 micro-batch preparation."""

import pytest

pytest.importorskip("pyspark", reason="Requires PySpark; run on VM B")

from pyspark.sql import SparkSession
from src.streaming.consumer.event_ledger import prepare_ledger_batch


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("B4LedgerTests")
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


def make_df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_duplicate_offset_is_counted_once(spark):
    row = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
    result = prepare_ledger_batch(make_df(spark, [row, row]))
    assert result.count() == 1


def test_same_payload_at_different_offsets_is_kept(spark):
    rows = [
        ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True),
        ("ecommerce_events", 0, 11, "s1", "payload1", "s1", True),
    ]
    assert prepare_ledger_batch(make_df(spark, rows)).count() == 2


def test_same_offset_in_different_partitions_is_kept(spark):
    rows = [
        ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True),
        ("ecommerce_events", 1, 10, "s1", "payload1", "s1", True),
    ]
    assert prepare_ledger_batch(make_df(spark, rows)).count() == 2


def test_same_offset_in_different_topics_is_kept(spark):
    rows = [
        ("topic_a", 0, 10, "s1", "payload1", "s1", True),
        ("topic_b", 0, 10, "s1", "payload1", "s1", True),
    ]
    assert prepare_ledger_batch(make_df(spark, rows)).count() == 2


def test_conflicting_payload_is_rejected(spark):
    rows = [
        ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True),
        ("ecommerce_events", 0, 10, "s1", "payload2", "s1", True),
    ]
    with pytest.raises(ValueError, match="Conflicting payloads"):
        prepare_ledger_batch(make_df(spark, rows))


def test_conflicting_kafka_key_is_rejected(spark):
    rows = [
        ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True),
        ("ecommerce_events", 0, 10, "s2", "payload1", "s1", True),
    ]
    with pytest.raises(ValueError, match="Conflicting payloads"):
        prepare_ledger_batch(make_df(spark, rows))


@pytest.mark.parametrize("identity", [
    (None, 0, 10),
    ("", 0, 10),
    ("   ", 0, 10),
    ("ecommerce_events", None, 10),
    ("ecommerce_events", -1, 10),
    ("ecommerce_events", 0, None),
    ("ecommerce_events", 0, -1),
])
def test_invalid_identity_is_rejected(spark, identity):
    row = identity + ("s1", "payload1", "s1", True)
    with pytest.raises(ValueError, match="invalid Kafka ledger identity"):
        prepare_ledger_batch(make_df(spark, [row]))


def test_null_session_and_invalid_payload_are_preserved(spark):
    rows = [
        ("ecommerce_events", 0, 10, None, "payload1", None, True),
        ("ecommerce_events", 0, 11, None, "{broken", None, False),
    ]
    result = prepare_ledger_batch(make_df(spark, rows))
    assert result.count() == 2
    assert result.filter("user_session IS NULL").count() == 2
    assert result.filter("is_valid = false").count() == 1


def test_missing_required_column_is_rejected(spark):
    row = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
    df = make_df(spark, [row]).drop("kafka_value")
    with pytest.raises(ValueError, match="Missing ledger columns"):
        prepare_ledger_batch(df)


def test_empty_batch_is_supported(spark):
    result = prepare_ledger_batch(make_df(spark, []))
    assert result.count() == 0


def test_replayed_batch_adds_no_rows(spark):
    from src.streaming.consumer.event_ledger import select_new_ledger_rows

    row = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
    committed = make_df(spark, [row])
    incoming = make_df(spark, [row, row])

    assert select_new_ledger_rows(incoming, committed).count() == 0


def test_overlapping_batch_keeps_only_new_offset(spark):
    from src.streaming.consumer.event_ledger import select_new_ledger_rows

    old = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
    new = ("ecommerce_events", 0, 11, "s1", "payload2", "s1", True)

    result = select_new_ledger_rows(
        make_df(spark, [old, new]),
        make_df(spark, [old]),
    )

    offsets = [row["kafka_offset"] for row in result.collect()]
    assert offsets == [11]


def test_conflict_with_committed_offset_is_rejected(spark):
    from src.streaming.consumer.event_ledger import select_new_ledger_rows

    old = ("ecommerce_events", 0, 10, "s1", "payload1", "s1", True)
    changed = ("ecommerce_events", 0, 10, "s1", "changed", "s1", True)

    with pytest.raises(ValueError, match="Conflicting payloads"):
        select_new_ledger_rows(
            make_df(spark, [changed]),
            make_df(spark, [old]),
        )
