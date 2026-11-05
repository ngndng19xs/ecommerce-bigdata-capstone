"""B4 session funnel tests against S2 ordering rules."""

from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="Requires PySpark; run on VM B")

from pyspark.sql import SparkSession
from src.streaming.consumer.session_funnel import build_session_funnel


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("B4SessionFunnelTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = """
    user_session string,
    is_valid boolean,
    event_type string,
    event_time timestamp,
    event_type_rank int,
    event_sort_key string
"""

RANKS = {"view": 1, "cart": 2, "remove_from_cart": 3, "purchase": 4}


def event(kind, second=0, session="s1", valid=True, key="a", day=1):
    timestamp = datetime(2019, 10, day, 0, 0, second)
    return (session, valid, kind, timestamp, RANKS[kind], key * 64)


def frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def result(spark, rows):
    return build_session_funnel(frame(spark, rows)).collect()


def flags(row):
    return (row["has_view"], row["has_cart"], row["has_purchase"])


@pytest.mark.parametrize("sequence,expected", [
    (["view", "cart", "purchase"], (True, True, True)),
    (["cart", "view", "cart", "purchase"], (True, True, True)),
    (["cart", "view", "purchase"], (True, False, False)),
    (["view", "purchase", "cart"], (True, True, False)),
    (["cart", "purchase"], (False, False, False)),
    (["view"], (True, False, False)),
])
def test_funnel_sequences(spark, sequence, expected):
    rows = [event(kind, second=i) for i, kind in enumerate(sequence)]
    # Reverse input order: physical arrival order must not define funnel.
    output = result(spark, list(reversed(rows)))
    assert len(output) == 1
    assert flags(output[0]) == expected


def test_equal_timestamp_uses_rank(spark):
    rows = [event("purchase"), event("cart"), event("view")]
    output = result(spark, rows)[0]

    assert flags(output) == (True, True, True)
    assert output["view_order"]["event_type_rank"] == 1
    assert output["cart_order"]["event_type_rank"] == 2
    assert output["purchase_order"]["event_type_rank"] == 4


def test_same_time_and_rank_uses_sort_key(spark):
    rows = [
        event("view", key="f"),
        event("view", key="1"),
        event("cart", second=1),
    ]
    output = result(spark, rows)[0]

    assert output["view_order"]["event_sort_key"] == "1" * 64
    assert flags(output) == (True, True, False)


def test_day_is_first_view_utc_day(spark):
    view = (
        "s1", True, "view",
        datetime(2019, 10, 1, 23, 59, 59), 1, "a" * 64
    )
    rows = [
        event("purchase", second=1, day=2),
        event("cart", day=2),
        view,
    ]
    output = result(spark, rows)[0]

    assert flags(output) == (True, True, True)
    assert str(output["funnel_date"]) == "2019-10-01"


def test_null_sessions_and_invalid_events_are_excluded(spark):
    rows = [
        event("view"),
        event("cart", second=1),
        event("purchase", second=2, valid=False),
        event("view", session=None),
        event("cart", second=1, session=None),
        event("purchase", second=2, session=None),
        event("view", session="invalid-only", valid=False),
    ]
    output = result(spark, rows)

    assert len(output) == 1
    assert output[0]["user_session"] == "s1"
    assert flags(output[0]) == (True, True, False)


def test_recompute_after_late_cart(spark):
    first_snapshot = [event("view"), event("purchase", second=2)]
    assert flags(result(spark, first_snapshot)[0]) == (True, False, False)

    complete_snapshot = first_snapshot + [event("cart", second=1)]
    assert flags(result(spark, complete_snapshot)[0]) == (True, True, True)


def test_identical_events_do_not_create_extra_sessions(spark):
    view = event("view")
    rows = [view, view, event("cart", 1), event("purchase", 2)]
    output = result(spark, rows)

    assert len(output) == 1
    assert flags(output[0]) == (True, True, True)


def test_sessions_are_independent(spark):
    rows = [
        event("view", session="s1"),
        event("cart", 1, session="s1"),
        event("purchase", 2, session="s2"),
    ]
    output = {row["user_session"]: flags(row) for row in result(spark, rows)}

    assert output == {
        "s1": (True, True, False),
        "s2": (False, False, False),
    }


def test_empty_input(spark):
    assert result(spark, []) == []


def test_null_order_field_fails_explicitly(spark):
    row = ("s1", True, "view", None, 1, "a" * 64)
    with pytest.raises(ValueError, match="null ordering field"):
        build_session_funnel(frame(spark, [row]))
