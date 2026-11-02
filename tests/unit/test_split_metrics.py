"""Unit tests for time-based splitting and feature pipeline stages."""

from datetime import date
import pytest
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from src.common.spark_session import get_spark
from src.ml.dataset.split import split_by_time
from src.ml.features.feature_pipeline import get_feature_pipeline_stages


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing local SparkSession."""
    session = get_spark("TestSplitFeatures")
    yield session
    session.stop()


def _create_mock_session_df(spark: SparkSession, rows: list):
    schema = StructType(
        [
            StructField("user_session", StringType(), True),
            StructField("session_start_date", DateType(), True),
            StructField("label", IntegerType(), True),
            StructField("view_count", LongType(), True),
            StructField("cart_count", LongType(), True),
            StructField("remove_count", LongType(), True),
            StructField("has_cart", IntegerType(), True),
            StructField("unique_product_count", IntegerType(), True),
            StructField("unique_category_count", IntegerType(), True),
            StructField("unique_brand_count", IntegerType(), True),
            StructField("avg_price", DoubleType(), True),
            StructField("max_price", DoubleType(), True),
            StructField("min_price", DoubleType(), True),
            StructField("interaction_duration", LongType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("last_event_type", StringType(), True),
            StructField("prev_event_type", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_split_by_time_ordering_and_disjoint(spark: SparkSession):
    """Test that train/val/test splits strictly respect chronological order with no overlap."""
    rows = []
    # Generate 10 days of sessions, 2 per day, with label 1 present every day
    for day in range(1, 11):
        d = date(2019, 11, day)
        rows.append((f"s_{day}_1", d, 1, 5, 1, 0, 1, 3, 2, 2, 50.0, 80.0, 20.0, 120, 10, 2, "view", "cart"))
        rows.append((f"s_{day}_2", d, 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 11, 2, "view", "view"))

    df = _create_mock_session_df(spark, rows)
    train_df, val_df, test_df, meta = split_by_time(df, ratios={"train": 0.70, "val": 0.15, "test": 0.15})

    train_dates = set(meta["train"]["dates"])
    val_dates = set(meta["val"]["dates"])
    test_dates = set(meta["test"]["dates"])

    # Disjointness
    assert len(train_dates.intersection(val_dates)) == 0
    assert len(val_dates.intersection(test_dates)) == 0
    assert len(train_dates.intersection(test_dates)) == 0

    # Chronological ordering
    assert max(train_dates) < min(val_dates)
    assert max(val_dates) < min(test_dates)

    # All partitions have positive labels
    assert meta["train"]["pos_label_count"] > 0
    assert meta["val"]["pos_label_count"] > 0
    assert meta["test"]["pos_label_count"] > 0


def test_split_by_time_raises_on_too_few_dates(spark: SparkSession):
    """Test that split_by_time raises ValueError when fewer than 3 distinct dates are present."""
    rows = [
        ("s1", date(2019, 11, 1), 1, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
        ("s2", date(2019, 11, 2), 1, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
    ]
    df = _create_mock_session_df(spark, rows)
    with pytest.raises(ValueError, match="at least 3"):
        split_by_time(df)


def test_split_by_time_raises_on_no_positive_train(spark: SparkSession):
    """Test that split_by_time raises ValueError when train set has zero positive labels."""
    rows = [
        # Days 1 to 7 have only label 0
        ("s1", date(2019, 11, 1), 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
        ("s2", date(2019, 11, 2), 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
        ("s3", date(2019, 11, 3), 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
        ("s4", date(2019, 11, 4), 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
        # Day 5 (val/test) has label 1
        ("s5", date(2019, 11, 5), 1, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 10, 2, "view", "view"),
    ]
    df = _create_mock_session_df(spark, rows)
    with pytest.raises(ValueError, match="positive labels"):
        split_by_time(df, ratios={"train": 0.70, "val": 0.15, "test": 0.15})


def test_feature_pipeline_stages_execution(spark: SparkSession):
    """Test that feature preprocessing pipeline fits and transforms DataFrame producing features vector."""
    rows = [
        ("s1", date(2019, 11, 1), 1, 5, 1, 0, 1, 3, 2, 2, 50.0, 80.0, 20.0, 120, 10, 2, "view", "cart"),
        ("s2", date(2019, 11, 1), 0, 5, 0, 0, 0, 2, 1, 1, 30.0, 30.0, 30.0, 60, 11, 2, "view", "view"),
        ("s3", date(2019, 11, 2), 1, 4, 1, 1, 1, 4, 3, 2, 100.0, 150.0, 50.0, 300, 14, 3, "cart", "remove_from_cart"),
    ]
    df = _create_mock_session_df(spark, rows)
    stages = get_feature_pipeline_stages()

    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df)
    transformed_df = model.transform(df)

    assert "features" in transformed_df.columns
    # Check that features vector exists and has non-zero size
    first_feat = transformed_df.select("features").first()["features"]
    assert first_feat.size > 13
