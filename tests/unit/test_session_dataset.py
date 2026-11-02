"""Unit tests for session-level dataset building and anti-leakage guarantees."""

from datetime import date, datetime
from decimal import Decimal
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.common.schema import RAW_EVENT_SCHEMA, normalize_events
from src.common.spark_session import get_spark
from src.ml.dataset.build_session_dataset import build_session_dataset


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a configured local SparkSession for unit testing."""
    session = get_spark("TestSessionDataset")
    yield session
    session.stop()


def _create_events_df(spark: SparkSession, rows: list):
    """Helper to create a normalized events DataFrame from mock tuples."""
    schema = StructType(
        [
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
            StructField("event_date", DateType(), True),
        ]
    )
    formatted_rows = []
    for r in rows:
        # Expected tuple: (session, event_time_str, event_type, product_id, cat_code, brand, price)
        session, time_str, event_type, product_id, cat_code, brand, price = r
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        formatted_rows.append(
            (
                dt,
                event_type,
                int(product_id),
                2053013555631882655,
                cat_code,
                brand,
                float(price),
                12345,
                session,
                dt.date(),
            )
        )
    return spark.createDataFrame(formatted_rows, schema=schema)


def test_purchase_in_head_dropped(spark: SparkSession):
    """Rule: Sessions containing a purchase at rn <= 5 must be dropped."""
    # Add an anchor session on earlier date and a later date to establish max date
    rows = [
        # Anchor session (eligible, label=0)
        ("s_anchor", "2019-11-01 08:00:00", "view", 101, "c1", "b1", 10.0),
        ("s_anchor", "2019-11-01 08:01:00", "view", 102, "c1", "b1", 10.0),
        ("s_anchor", "2019-11-01 08:02:00", "view", 103, "c1", "b1", 10.0),
        ("s_anchor", "2019-11-01 08:03:00", "view", 104, "c1", "b1", 10.0),
        ("s_anchor", "2019-11-01 08:04:00", "view", 105, "c1", "b1", 10.0),
        ("s_anchor", "2019-11-01 08:05:00", "view", 106, "c1", "b1", 10.0),
        # Target session s1 with purchase at rn=3
        ("s1", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:01:00", "view", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:02:00", "purchase", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:03:00", "view", 103, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:04:00", "view", 104, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:05:00", "view", 105, "c1", "b1", 10.0),
        # Max date anchor to avoid s1 or s_anchor being right-censored
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5)
    sessions = [row["user_session"] for row in res.collect()]
    assert "s1" not in sessions
    assert "s_anchor" in sessions


def test_purchase_in_tail_label_one(spark: SparkSession):
    """Rule: Purchase at rn > 5 yields label=1; 2 purchases in tail still yields label=1."""
    rows = [
        # Session s1: 1 purchase in tail (rn=6)
        ("s1", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:01:00", "view", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:02:00", "cart", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:03:00", "view", 103, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:04:00", "cart", 103, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:05:00", "purchase", 102, "c1", "b1", 10.0),
        # Session s2: 2 purchases in tail (rn=6, rn=7)
        ("s2", "2019-11-01 09:10:00", "view", 201, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:11:00", "view", 202, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:12:00", "view", 203, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:13:00", "view", 204, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:14:00", "cart", 204, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:15:00", "purchase", 204, "c1", "b1", 10.0),
        ("s2", "2019-11-01 09:16:00", "purchase", 201, "c1", "b1", 10.0),
        # Max date anchor
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5).collect()
    label_dict = {row["user_session"]: row["label"] for row in res}
    assert label_dict["s1"] == 1
    assert label_dict["s2"] == 1


def test_no_purchase_in_tail_label_zero(spark: SparkSession):
    """Rule: Events after k but no purchase must produce label = 0."""
    rows = [
        ("s1", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:01:00", "view", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:02:00", "view", 103, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:03:00", "view", 104, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:04:00", "view", 105, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:05:00", "view", 106, "c1", "b1", 10.0),
        ("s1", "2019-11-01 09:06:00", "cart", 106, "c1", "b1", 10.0),
        # Max date anchor
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5).collect()
    label_dict = {row["user_session"]: row["label"] for row in res}
    assert label_dict["s1"] == 0


def test_session_le_k_events_dropped(spark: SparkSession):
    """Rule: Sessions with exactly 5 events or fewer must be excluded."""
    rows = [
        # s_short_3: 3 events
        ("s_short_3", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 10.0),
        ("s_short_3", "2019-11-01 09:01:00", "view", 102, "c1", "b1", 10.0),
        ("s_short_3", "2019-11-01 09:02:00", "view", 103, "c1", "b1", 10.0),
        # s_exact_5: exactly 5 events
        ("s_exact_5", "2019-11-01 09:10:00", "view", 201, "c1", "b1", 10.0),
        ("s_exact_5", "2019-11-01 09:11:00", "view", 202, "c1", "b1", 10.0),
        ("s_exact_5", "2019-11-01 09:12:00", "view", 203, "c1", "b1", 10.0),
        ("s_exact_5", "2019-11-01 09:13:00", "view", 204, "c1", "b1", 10.0),
        ("s_exact_5", "2019-11-01 09:14:00", "view", 205, "c1", "b1", 10.0),
        # s_valid_6: 6 events
        ("s_valid_6", "2019-11-01 09:20:00", "view", 301, "c1", "b1", 10.0),
        ("s_valid_6", "2019-11-01 09:21:00", "view", 302, "c1", "b1", 10.0),
        ("s_valid_6", "2019-11-01 09:22:00", "view", 303, "c1", "b1", 10.0),
        ("s_valid_6", "2019-11-01 09:23:00", "view", 304, "c1", "b1", 10.0),
        ("s_valid_6", "2019-11-01 09:24:00", "view", 305, "c1", "b1", 10.0),
        ("s_valid_6", "2019-11-01 09:25:00", "view", 306, "c1", "b1", 10.0),
        # Max date anchor
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5).collect()
    kept = [row["user_session"] for row in res]
    assert "s_short_3" not in kept
    assert "s_exact_5" not in kept
    assert "s_valid_6" in kept


def test_event_type_tie_breaking_order(spark: SparkSession):
    """Rule: Same-second events must order view < cart < remove_from_cart < purchase.

    If alphabetical ordering were mistakenly used:
    'purchase' < 'view', so purchase would get rn=5 (head), causing session to be dropped!
    With type_rank:
    view (1) < purchase (4), so view gets rn=5 (head), purchase gets rn=6 (tail),
    retaining the session and yielding label=1!
    """
    rows = [
        ("s1", "2019-11-01 10:00:00", "view", 101, "c1", "b1", 10.0),
        ("s1", "2019-11-01 10:01:00", "view", 102, "c1", "b1", 10.0),
        ("s1", "2019-11-01 10:02:00", "view", 103, "c1", "b1", 10.0),
        ("s1", "2019-11-01 10:03:00", "view", 104, "c1", "b1", 10.0),
        # Exactly identical timestamp at 10:04:00: one view and one purchase
        ("s1", "2019-11-01 10:04:00", "purchase", 105, "c1", "b1", 10.0),
        ("s1", "2019-11-01 10:04:00", "view", 105, "c1", "b1", 10.0),
        # Max date anchor
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5).collect()
    sessions = {row["user_session"]: row for row in res}
    assert "s1" in sessions, "s1 must not be dropped because view must precede purchase"
    assert sessions["s1"]["label"] == 1
    assert sessions["s1"]["last_event_type"] == "view"


def test_right_censoring_last_day_dropped(spark: SparkSession):
    """Rule: Sessions starting on the maximum event date of the dataset must be dropped."""
    rows = [
        # Session starting on day 1 (eligible)
        ("s_day1", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 10.0),
        ("s_day1", "2019-11-01 09:01:00", "view", 102, "c1", "b1", 10.0),
        ("s_day1", "2019-11-01 09:02:00", "view", 103, "c1", "b1", 10.0),
        ("s_day1", "2019-11-01 09:03:00", "view", 104, "c1", "b1", 10.0),
        ("s_day1", "2019-11-01 09:04:00", "view", 105, "c1", "b1", 10.0),
        ("s_day1", "2019-11-01 09:05:00", "view", 106, "c1", "b1", 10.0),
        # Session starting on day 2 (max date of dataset -> must be dropped)
        ("s_day2", "2019-11-02 09:00:00", "view", 201, "c1", "b1", 10.0),
        ("s_day2", "2019-11-02 09:01:00", "view", 202, "c1", "b1", 10.0),
        ("s_day2", "2019-11-02 09:02:00", "view", 203, "c1", "b1", 10.0),
        ("s_day2", "2019-11-02 09:03:00", "view", 204, "c1", "b1", 10.0),
        ("s_day2", "2019-11-02 09:04:00", "view", 205, "c1", "b1", 10.0),
        ("s_day2", "2019-11-02 09:05:00", "view", 206, "c1", "b1", 10.0),
    ]
    df = _create_events_df(spark, rows)
    res = build_session_dataset(df, k=5).collect()
    kept = [row["user_session"] for row in res]
    assert "s_day1" in kept
    assert "s_day2" not in kept


def test_tail_mutation_does_not_affect_features(spark: SparkSession):
    """Rule: Changing contents of tail events (rn > 5) must not alter prefix features."""
    head_rows = [
        ("s1", "2019-11-01 09:00:00", "view", 101, "c1", "b1", 15.0),
        ("s1", "2019-11-01 09:01:00", "view", 102, "c2", "b2", 25.0),
        ("s1", "2019-11-01 09:02:00", "cart", 102, "c2", "b2", 25.0),
        ("s1", "2019-11-01 09:03:00", "view", 103, "c3", "b3", 35.0),
        ("s1", "2019-11-01 09:04:00", "cart", 103, "c3", "b3", 35.0),
    ]
    # Tail 1: 2 views with low prices
    tail_1 = [
        ("s1", "2019-11-01 09:05:00", "view", 888, "c8", "b8", 1.0),
        ("s1", "2019-11-01 09:06:00", "view", 889, "c8", "b8", 2.0),
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]
    # Tail 2: 2 remove_from_cart events with extreme prices and different categories
    tail_2 = [
        ("s1", "2019-11-01 09:05:00", "remove_from_cart", 999, "c9", "b9", 9999.0),
        ("s1", "2019-11-01 09:06:00", "remove_from_cart", 999, "c9", "b9", 9999.0),
        ("s_max", "2019-11-02 08:00:00", "view", 999, "c1", "b1", 10.0),
    ]

    df1 = _create_events_df(spark, head_rows + tail_1)
    df2 = _create_events_df(spark, head_rows + tail_2)

    res1 = build_session_dataset(df1, k=5).filter("user_session == 's1'").collect()[0]
    res2 = build_session_dataset(df2, k=5).filter("user_session == 's1'").collect()[0]

    feature_cols = [
        "view_count",
        "cart_count",
        "remove_count",
        "has_cart",
        "unique_product_count",
        "unique_category_count",
        "unique_brand_count",
        "avg_price",
        "max_price",
        "min_price",
        "interaction_duration",
        "hour_of_day",
        "day_of_week",
        "last_event_type",
        "prev_event_type",
    ]
    for col in feature_cols:
        assert res1[col] == res2[col], f"Feature {col} leaked information from tail!"


def test_normalize_events_consistency(spark: SparkSession):
    """Rule: normalize_events produces identical schemas and values from String UTC and Timestamp sources."""
    # Source A: CSV-like with string timestamp and double price
    schema_csv = StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )
    data_csv = [
        (
            "2019-11-05 14:30:00 UTC",
            "view",
            1001,
            205301,
            "electronics.audio",
            "sony",
            199.99,
            555,
            "sess_abc",
        )
    ]
    df_csv = spark.createDataFrame(data_csv, schema=schema_csv)

    # Source B: Parquet-like with native TimestampType, Decimal price, and pre-existing event_date
    schema_parquet = StructType(
        [
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DecimalType(18, 2), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("event_date", DateType(), True),
        ]
    )
    from datetime import timezone
    dt = datetime(2019, 11, 5, 14, 30, 0, tzinfo=timezone.utc)
    data_parquet = [
        (
            dt,
            "view",
            1001,
            205301,
            "electronics.audio",
            "sony",
            Decimal("199.99"),
            555,
            "sess_abc",
            "nov.parquet",
            dt.date(),
        )
    ]
    df_parquet = spark.createDataFrame(data_parquet, schema=schema_parquet)

    norm_csv = normalize_events(df_csv)
    norm_parquet = normalize_events(df_parquet)

    # Validate schema column types
    assert isinstance(norm_csv.schema["event_time"].dataType, TimestampType)
    assert isinstance(norm_parquet.schema["event_time"].dataType, TimestampType)
    assert isinstance(norm_csv.schema["event_date"].dataType, DateType)
    assert isinstance(norm_parquet.schema["event_date"].dataType, DateType)
    assert isinstance(norm_csv.schema["price"].dataType, DoubleType)
    assert isinstance(norm_parquet.schema["price"].dataType, DoubleType)

    # Validate exact value equivalence
    row_csv = norm_csv.select("event_time", "event_date", "price").collect()[0]
    row_parquet = norm_parquet.select("event_time", "event_date", "price").collect()[0]

    assert row_csv["event_time"] == row_parquet["event_time"]
    assert row_csv["event_date"] == row_parquet["event_date"]
    assert row_csv["price"] == row_parquet["price"]
