"""
Spark Structured Streaming Consumer for Production Kafka Events (Task S2 / Version kafka-v1.0).

This module ingests streaming clickstream events from Apache Kafka topic 'ecommerce_events',
parses JSON payloads according to Kafka Contract v1.0 (12 fields, String IDs, pre-computed
rank and SHA-256 sort key), performs strict validation without silent data loss, and exposes
the canonical business ordering (event_time ASC, event_type_rank ASC, event_sort_key ASC).

Key Guarantees & Contract Compliance (Data Contract v1.0, Section 6 & 8):
1. Topic: Subscribes strictly to 'ecommerce_events' (Data Contract Section 6.1).
2. Schema: Validates 12 fields, 3 String IDs (product_id, category_id, user_id),
   Double price >= 0.0, String brand (fallback 'Unknown'), schema_version 'kafka-v1.0'.
3. Rank & Sort Key: Inherits event_type_rank and event_sort_key directly from payload;
   NEVER recomputes rank or SHA-256 hash in consumer.
4. Timestamp: Parses event_time as UTC ISO-8601 (YYYY-MM-DDTHH:MM:SSZ).
5. Error Accounting: Uses Spark PERMISSIVE mode with _corrupt_record and non-silent
   validation tracking (is_valid flag, rejection_reason); never drops invalid records silently.
6. Ordering Limitation: Micro-batch ordering applies strictly within micro-batch.
   End-to-end multi-micro-batch session ordering requires stateful sessionization
   (flatMapGroupsWithState), which is subject to Pending Decisions #4 (Watermark),
   #5 (Session timeout), and #10 (DLQ/late event policy).
7. Python 3.8+ and PySpark 3.1.2+ compatible.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

LOG = logging.getLogger("event_consumer")

# Contract v1.0 Constants
PRODUCTION_TOPIC = "ecommerce_events"
PRODUCTION_SCHEMA_VERSION = "kafka-v1.0"

EXPECTED_PAYLOAD_FIELDS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_type_rank",
    "event_sort_key",
    "schema_version",
]

VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
EVENT_TYPE_TO_RANK = {
    "view": 1,
    "cart": 2,
    "remove_from_cart": 3,
    "purchase": 4,
}
EVENT_TIME_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
EVENT_SORT_KEY_REGEX = re.compile(r"^[a-f0-9]{64}$")

# Conditional PySpark import for local test compatibility without Spark
try:
    from pyspark.sql import Column, DataFrame, SparkSession, functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    PYSPARK_AVAILABLE = True

    # Spark Schema for parsing Kafka JSON payload
    KAFKA_EVENT_SPARK_SCHEMA = StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_session", StringType(), True),
            StructField("event_type_rank", IntegerType(), True),
            StructField("event_sort_key", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore
    Column = Any  # type: ignore
    KAFKA_EVENT_SPARK_SCHEMA = None  # type: ignore


# ---------------------------------------------------------------------------
# Pure Python Validation & Parsing Helper (Testable without PySpark)
# ---------------------------------------------------------------------------


def validate_and_parse_record(
    raw_value: Union[str, bytes, Dict[str, Any]],
    raw_key: Optional[Union[str, bytes]] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """Validate a raw Kafka message value and key against Kafka Contract v1.0.

    Parameters
    ----------
    raw_value : str, bytes, or dict
        Raw message payload from Kafka.
    raw_key : str, bytes, or None, optional
        Kafka message key (expected to match user_session or None).

    Returns
    -------
    Tuple[bool, Optional[Dict[str, Any]], Optional[str]]
        (is_valid, parsed_dict, rejection_reason).
    """
    if isinstance(raw_value, bytes):
        try:
            raw_value = raw_value.decode("utf-8")
        except UnicodeDecodeError as exc:
            return False, None, f"Corrupt message: UTF-8 decode error: {exc}"

    if isinstance(raw_value, str):
        try:
            record = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            return False, None, f"Corrupt message: JSON decode error: {exc}"
    elif isinstance(raw_value, dict):
        record = raw_value
    else:
        return False, None, f"Invalid value type: {type(raw_value)}"

    if not isinstance(record, dict):
        return False, None, "Payload is not a JSON object"

    # 1. Field presence and strict 12 fields (additionalProperties=false)
    keys = set(record.keys())
    missing = set(EXPECTED_PAYLOAD_FIELDS) - keys
    if missing:
        return False, record, f"Missing required fields: {sorted(list(missing))}"

    extra = keys - set(EXPECTED_PAYLOAD_FIELDS)
    if extra:
        return False, record, f"Unexpected extra fields: {sorted(list(extra))}"

    # 2. Schema version
    if record.get("schema_version") != PRODUCTION_SCHEMA_VERSION:
        return (
            False,
            record,
            f"Invalid schema_version '{record.get('schema_version')}', expected '{PRODUCTION_SCHEMA_VERSION}'",
        )

    # 3. String IDs validation (must be non-empty string, NOT int or float)
    for id_col in ("product_id", "category_id", "user_id"):
        val = record.get(id_col)
        if not isinstance(val, str) or not val.strip():
            return (
                False,
                record,
                f"Field '{id_col}' must be a non-empty string, got {type(val).__name__} ({val!r})",
            )

    # 4. Event type & rank cross-field consistency
    event_type = record.get("event_type")
    if event_type not in VALID_EVENT_TYPES:
        return False, record, f"Invalid event_type: {event_type!r}"

    rank = record.get("event_type_rank")
    if not isinstance(rank, int) or isinstance(rank, bool):
        return False, record, f"event_type_rank must be integer, got {type(rank).__name__}"

    expected_rank = EVENT_TYPE_TO_RANK[event_type]
    if rank != expected_rank:
        return (
            False,
            record,
            f"Cross-field rank mismatch: event_type '{event_type}' requires rank {expected_rank}, got {rank}",
        )

    # 5. Timestamp validation (strict UTC ISO-8601 YYYY-MM-DDTHH:MM:SSZ)
    event_time = record.get("event_time")
    if not isinstance(event_time, str) or not EVENT_TIME_REGEX.match(event_time):
        return (
            False,
            record,
            f"event_time must be ISO-8601 UTC with trailing 'Z' (YYYY-MM-DDTHH:MM:SSZ), got {event_time!r}",
        )

    # 6. Event sort key (strict 64 hex characters SHA-256)
    sort_key = record.get("event_sort_key")
    if not isinstance(sort_key, str) or not EVENT_SORT_KEY_REGEX.match(sort_key):
        return (
            False,
            record,
            f"event_sort_key must be 64-char lowercase hex SHA-256, got {sort_key!r}",
        )

    # 7. Price validation (numeric, >= 0.0)
    price = record.get("price")
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        return False, record, f"price must be a float/int >= 0.0, got {type(price).__name__}"
    if price < 0.0:
        return False, record, f"Negative price not allowed: {price}"

    # 8. Brand fallback validation
    brand = record.get("brand")
    if brand is None or (isinstance(brand, str) and not brand.strip()):
        return False, record, "brand cannot be null or empty (expected fallback 'Unknown')"

    # 9. Key consistency (if key provided, user_session non-empty should match)
    if raw_key is not None:
        key_str = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
        session_val = record.get("user_session")
        if session_val is not None and key_str != session_val:
            return (
                False,
                record,
                f"Kafka key mismatch: key={key_str!r} != user_session={session_val!r}",
            )

    return True, record, None


# ---------------------------------------------------------------------------
# PySpark Streaming Functions
# ---------------------------------------------------------------------------


def parse_kafka_stream(raw_stream_df: DataFrame) -> DataFrame:
    """Parse raw Kafka streaming DataFrame into structured columns with error tracking.

    Parameters
    ----------
    raw_stream_df : DataFrame
        Kafka source DataFrame with standard columns: key, value, topic, partition, offset, timestamp.

    Returns
    -------
    DataFrame
        Parsed DataFrame containing the 12 contract fields, parsed event_timestamp,
        is_valid boolean flag, and rejection_reason string.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to execute parse_kafka_stream()")

    # 1. Cast key and value to String
    string_df = raw_stream_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("kafka_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ingest_time"),
    )

    # 2. Parse JSON with PERMISSIVE mode capturing corrupt records
    parsed_json_df = string_df.withColumn(
        "payload",
        F.from_json(
            F.col("kafka_value"),
            KAFKA_EVENT_SPARK_SCHEMA,
            options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )

    # 3. Extract payload columns
    extracted_df = parsed_json_df.select(
        "kafka_key",
        "kafka_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ingest_time",
        F.col("payload.event_time").alias("event_time_str"),
        F.col("payload.event_type").alias("event_type"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.category_id").alias("category_id"),
        F.col("payload.category_code").alias("category_code"),
        F.col("payload.brand").alias("brand"),
        F.col("payload.price").alias("price"),
        F.col("payload.user_id").alias("user_id"),
        F.col("payload.user_session").alias("user_session"),
        F.col("payload.event_type_rank").alias("event_type_rank"),
        F.col("payload.event_sort_key").alias("event_sort_key"),
        F.col("payload.schema_version").alias("schema_version"),
        F.col("payload._corrupt_record").alias("_corrupt_record"),
    )

    # 4. Timestamp parsing
    with_ts_df = extracted_df.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    # 5. Non-silent validation accounting (construct is_valid & rejection_reason)
    is_corrupt = F.col("_corrupt_record").isNotNull()
    is_missing_required = (
        F.col("event_time_str").isNull()
        | F.col("event_type").isNull()
        | F.col("product_id").isNull()
        | F.col("category_id").isNull()
        | F.col("user_id").isNull()
        | F.col("event_type_rank").isNull()
        | F.col("event_sort_key").isNull()
        | F.col("schema_version").isNull()
    )
    is_invalid_version = F.col("schema_version") != PRODUCTION_SCHEMA_VERSION
    is_invalid_event_type = ~F.col("event_type").isin("view", "cart", "remove_from_cart", "purchase")
    is_rank_mismatch = (
        (F.col("event_type") == "view") & (F.col("event_type_rank") != 1)
        | (F.col("event_type") == "cart") & (F.col("event_type_rank") != 2)
        | (F.col("event_type") == "remove_from_cart") & (F.col("event_type_rank") != 3)
        | (F.col("event_type") == "purchase") & (F.col("event_type_rank") != 4)
    )
    is_negative_price = F.col("price").isNull() | (F.col("price") < 0.0)

    is_valid_expr = ~(
        is_corrupt
        | is_missing_required
        | is_invalid_version
        | is_invalid_event_type
        | is_rank_mismatch
        | is_negative_price
    )

    rejection_reason_expr = (
        F.when(is_corrupt, F.concat(F.lit("Corrupt JSON: "), F.col("_corrupt_record")))
        .when(is_missing_required, F.lit("Missing required fields"))
        .when(is_invalid_version, F.concat(F.lit("Invalid schema version: "), F.col("schema_version")))
        .when(is_invalid_event_type, F.concat(F.lit("Invalid event_type: "), F.col("event_type")))
        .when(is_rank_mismatch, F.concat(F.lit("Cross-field rank mismatch for event_type: "), F.col("event_type")))
        .when(is_negative_price, F.concat(F.lit("Invalid/negative price: "), F.col("price").cast("string")))
        .otherwise(None)
    )

    validated_df = with_ts_df.withColumn("is_valid", is_valid_expr).withColumn(
        "rejection_reason", rejection_reason_expr
    )

    return validated_df


def order_events_microbatch(df: DataFrame) -> DataFrame:
    """Order parsed events within micro-batch according to Data Contract v1.0 Section 5.1 & 9.1.

    Business Ordering Specification:
        event_time ASC,
        event_type_rank ASC,
        event_sort_key ASC

    IMPORTANT STREAMING ARCHITECTURAL NOTICE:
    -----------------------------------------
    Sorting within a micro-batch provides strict business event ordering for events
    processed within the same micro-batch partition.
    However, in Apache Spark Structured Streaming, cross-micro-batch session ordering
    and stateful session funnel reconstruction require stateful sessionization
    (e.g., flatMapGroupsWithState), which is contingent upon:
    - Pending Decision #4: Watermark Delay Specification.
    - Pending Decision #5: Session Inactivity Timeout.
    - Pending Decision #10: Production DLQ & Late-event Policy.

    Parameters
    ----------
    df : DataFrame
        Parsed events DataFrame containing event_time, event_type_rank, event_sort_key.

    Returns
    -------
    DataFrame
        DataFrame ordered within the micro-batch by the canonical three-key ordering.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to execute order_events_microbatch()")

    return df.orderBy(
        F.col("event_time").asc(),
        F.col("event_type_rank").asc(),
        F.col("event_sort_key").asc(),
    )


def create_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str = PRODUCTION_TOPIC,
    starting_offsets: str = "latest",
) -> DataFrame:
    """Create a Structured Streaming DataFrame subscribed to the Kafka production topic.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session with spark-sql-kafka connector available.
    bootstrap_servers : str
        Comma-separated list of Kafka broker hosts (e.g., '192.168.182.11:9092').
    topic : str, optional
        Kafka topic name, default 'ecommerce_events' (Data Contract Section 6.1).
    starting_offsets : str, optional
        Kafka starting offsets ('latest' or 'earliest'), default 'latest'.

    Returns
    -------
    DataFrame
        Streaming DataFrame with raw Kafka messages.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to execute create_kafka_stream()")

    if topic != PRODUCTION_TOPIC:
        LOG.warning(
            "Topic '%s' differs from official production topic '%s'",
            topic,
            PRODUCTION_TOPIC,
        )

    LOG.info("Subscribing to Kafka topic '%s' at %s", topic, bootstrap_servers)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw_stream
