"""Schema definitions and event normalization utilities for clickstream data."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_EVENT_SCHEMA = StructType(
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


def normalize_events(df: DataFrame) -> DataFrame:
    """Normalize clickstream events across disparate data sources (CSV sample or curated Parquet).

    Standard output requirements:
    - event_time: TimestampType in UTC.
    - event_date: DateType derived from event_time in UTC.
    - price: DoubleType for ML feature compatibility.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame from either raw CSV (event_time as string) or clean Parquet.

    Returns
    -------
    DataFrame
        Normalized DataFrame with consistent column types.
    """
    event_time_data_type = df.schema["event_time"].dataType

    # If event_time is StringType, parse "yyyy-MM-dd HH:mm:ss UTC" to TimestampType
    if isinstance(event_time_data_type, StringType):
        normalized_df = df.withColumn(
            "event_time",
            F.when(
                F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$"),
                F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"),
            ).otherwise(F.to_timestamp("event_time")),
        )
    elif not isinstance(event_time_data_type, TimestampType):
        normalized_df = df.withColumn("event_time", F.col("event_time").cast(TimestampType()))
    else:
        normalized_df = df

    # Ensure event_date is DateType (UTC)
    normalized_df = normalized_df.withColumn("event_date", F.to_date("event_time"))

    # Ensure price is cast to DoubleType
    normalized_df = normalized_df.withColumn("price", F.col("price").cast(DoubleType()))

    return normalized_df
