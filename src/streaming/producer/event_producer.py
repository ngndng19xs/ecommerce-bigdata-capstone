"""
Kafka Event Producer for Production Curated Parquet Events (Task S2 / Version kafka-v1.0).

This module reads normalized clickstream events from Curated Parquet storage (/data/curated/events),
validates each record against the official Kafka Contract v1.0 specification (12 fields, string IDs,
pre-computed event_type_rank and event_sort_key), and streams them to Apache Kafka topic 'ecommerce_events'.

Key Guarantees:
1. Source: /data/curated/events (never raw CSV or temporary events_cleaned in production).
2. Inherits event_type_rank and event_sort_key directly from Curated Parquet; never recomputes rank or SHA-256.
3. Produces strictly 12 JSON payload fields (no event_date, no reject_reason, additionalProperties=false).
4. Ensures product_id, category_id, user_id are formatted as non-empty JSON Strings.
5. Formats event_time strictly as UTC ISO-8601 with trailing 'Z' (YYYY-MM-DDTHH:MM:SSZ).
6. Message key: user_session as UTF-8 bytes when non-null; true None (null wire protocol) when user_session is null.
7. Two execution modes:
   - Dry-run (default): reads, maps, validates against schema v1.0, logs statistics, never sends to Kafka.
   - Send: explicitly activated via --send flag, verifies topic, produces with ACK verification and error accounting.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

LOG = logging.getLogger("event_producer")

# Contract v1.0 Constants
PRODUCTION_TOPIC = "ecommerce_events"
PRODUCTION_SCHEMA_VERSION = "kafka-v1.0"
CURATED_PRODUCTION_PATH = "/data/curated/events"

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


def format_iso8601_utc(dt_val: Any) -> str:
    """Format datetime, timestamp, or string into strict ISO-8601 UTC with Z suffix.

    Parameters
    ----------
    dt_val : datetime, str, or timestamp
        Input timestamp.

    Returns
    -------
    str
        Timestamp string formatted as YYYY-MM-DDTHH:MM:SSZ.

    Raises
    ------
    ValueError
        If dt_val cannot be parsed into a valid UTC timestamp.
    """
    if isinstance(dt_val, datetime):
        if dt_val.tzinfo is None:
            # Assume naive datetime from Parquet is in UTC as required by Data Contract
            dt_utc = dt_val.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_val.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(dt_val, str):
        cleaned = dt_val.strip()
        # Handle "YYYY-MM-DD HH:mm:ss UTC"
        if cleaned.endswith(" UTC"):
            cleaned = cleaned[:-4].strip()
        # Handle "YYYY-MM-DD HH:mm:ss"
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                parsed = datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
                return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

    raise ValueError(f"Cannot format '{dt_val}' as ISO-8601 UTC timestamp")


def row_to_kafka_event(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a Curated Parquet row into official Kafka Contract v1.0 JSON payload.

    Parameters
    ----------
    row : dict
        Dictionary of row attributes from Curated Parquet.

    Returns
    -------
    dict
        Validated 12-field dictionary ready for JSON serialization.

    Raises
    ------
    ValueError
        If required fields are missing or cannot be normalized according to contract.
    """
    # 1. event_time
    raw_time = row.get("event_time")
    if raw_time is None:
        raise ValueError("Missing required field 'event_time'")
    event_time_str = format_iso8601_utc(raw_time)

    # 2. event_type
    event_type = str(row.get("event_type", "")).strip().lower()
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Invalid event_type '{event_type}'")

    # 3. 3 Identifiers: product_id, category_id, user_id (Must be non-empty String)
    for id_field in ("product_id", "category_id", "user_id"):
        id_val = row.get(id_field)
        if id_val is None:
            raise ValueError(f"Required identifier '{id_field}' is NULL")
        str_val = str(id_val).strip()
        if not str_val:
            raise ValueError(f"Identifier '{id_field}' cannot be empty")

    product_id_str = str(row["product_id"]).strip()
    category_id_str = str(row["category_id"]).strip()
    user_id_str = str(row["user_id"]).strip()

    # 4. category_code (nullable string)
    raw_cat_code = row.get("category_code")
    category_code = str(raw_cat_code).strip() if raw_cat_code is not None and str(raw_cat_code).strip() else None

    # 5. brand (non-empty string, normalized to 'Unknown' if missing/empty)
    raw_brand = row.get("brand")
    brand = str(raw_brand).strip() if raw_brand is not None and str(raw_brand).strip() else "Unknown"

    # 6. price (non-negative number >= 0.0)
    raw_price = row.get("price")
    if raw_price is None:
        raise ValueError("Required field 'price' is NULL")
    try:
        price = float(raw_price)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid price value '{raw_price}': {exc}")
    if price < 0.0:
        raise ValueError(f"Negative price '{price}' is rejected by contract")

    # 7. user_session (nullable string)
    raw_session = row.get("user_session")
    user_session = str(raw_session).strip() if raw_session is not None and str(raw_session).strip() else None

    # 8. event_type_rank (must be inherited directly from Curated Parquet)
    raw_rank = row.get("event_type_rank")
    if raw_rank is None:
        raise ValueError("Missing 'event_type_rank'; producer must inherit pre-computed rank from Curated Parquet")
    try:
        rank = int(raw_rank)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid event_type_rank '{raw_rank}'")

    expected_rank = EVENT_TYPE_TO_RANK[event_type]
    if rank != expected_rank:
        raise ValueError(f"Cross-field rank mismatch: event_type '{event_type}' requires rank {expected_rank}, got {rank}")

    # 9. event_sort_key (must be inherited directly from Curated Parquet, 64 hex lowercase)
    raw_sort_key = row.get("event_sort_key")
    if raw_sort_key is None:
        raise ValueError("Missing 'event_sort_key'; producer must inherit pre-computed hash from Curated Parquet")
    sort_key = str(raw_sort_key).strip().lower()
    if not EVENT_SORT_KEY_REGEX.match(sort_key):
        raise ValueError(f"Invalid event_sort_key '{sort_key}': must be 64-char lowercase hex SHA-256")

    # Assemble strictly 12 fields (no event_date, no reject_reason)
    return {
        "event_time": event_time_str,
        "event_type": event_type,
        "product_id": product_id_str,
        "category_id": category_id_str,
        "category_code": category_code,
        "brand": brand,
        "price": price,
        "user_id": user_id_str,
        "user_session": user_session,
        "event_type_rank": rank,
        "event_sort_key": sort_key,
        "schema_version": PRODUCTION_SCHEMA_VERSION,
    }


def serialize_kafka_message(event: Dict[str, Any]) -> Tuple[Optional[bytes], bytes]:
    """Serialize event into Kafka key and value bytes.

    Parameters
    ----------
    event : dict
        Validated Kafka contract event dictionary.

    Returns
    -------
    tuple (key_bytes, value_bytes)
        - key_bytes: user_session as UTF-8 bytes, or None if user_session is null.
        - value_bytes: compact UTF-8 JSON bytes of 12 fields.
    """
    user_session = event.get("user_session")
    key_bytes = user_session.encode("utf-8") if user_session is not None else None

    # Compact JSON serialization with exact field formatting
    value_json = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    value_bytes = value_json.encode("utf-8")

    return key_bytes, value_bytes


def read_curated_records_pyspark(spark: Any, path: str) -> Iterator[Dict[str, Any]]:
    """Iterate over Curated Parquet records using PySpark without loading all data into driver RAM.

    Parameters
    ----------
    spark : SparkSession
        Configured SparkSession instance.
    path : str
        HDFS or local path to Curated Parquet.

    Yields
    ------
    dict
        Row dictionary.
    """
    df = spark.read.parquet(path)
    # Using toLocalIterator() to stream records partition by partition without full collect()
    for row in df.toLocalIterator():
        yield row.asDict()


class EventProducer:
    """Production Kafka Producer orchestrator implementing Contract v1.0."""

    def __init__(
        self,
        bootstrap_servers: str = "127.0.0.1:9092",
        topic: str = PRODUCTION_TOPIC,
        schema_path: Optional[str] = None,
        dry_run: bool = True,
        rate_limit: float = 0.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dry_run = dry_run
        self.rate_limit = rate_limit
        self.producer = None

        self.stats = {
            "scanned": 0,
            "valid": 0,
            "malformed": 0,
            "sent_ack": 0,
            "send_failed": 0,
        }

        # Optional JSON Schema Draft 7 validator
        self.validator = None
        if schema_path:
            schema_file = Path(schema_path)
            if schema_file.is_file():
                try:
                    from jsonschema import Draft7Validator
                    with open(schema_file, "r", encoding="utf-8") as f:
                        schema_dict = json.load(f)
                    Draft7Validator.check_schema(schema_dict)
                    self.validator = Draft7Validator(schema_dict)
                    LOG.info("Loaded JSON Schema validator from: %s", schema_path)
                except Exception as exc:
                    LOG.warning("Could not initialize JSON Schema validator: %s", exc)

    def init_kafka_producer(self) -> None:
        """Initialize real KafkaProducer client when send mode is active."""
        if self.dry_run:
            LOG.info("DRY-RUN mode: KafkaProducer will NOT be initialized.")
            return

        if self.topic != PRODUCTION_TOPIC:
            raise ValueError(f"Production Producer must publish strictly to topic '{PRODUCTION_TOPIC}', got '{self.topic}'")

        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                f"Missing kafka-python library in runtime environment: {exc}"
            ) from exc

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in self.bootstrap_servers.split(",")],
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            LOG.info("KafkaProducer initialized successfully on servers: %s", self.bootstrap_servers)
        except Exception as exc:
            raise RuntimeError(f"Failed to connect to Kafka Broker: {exc}") from exc

    def process_record(self, raw_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate, map, and optionally send a single event record.

        Parameters
        ----------
        raw_row : dict
            Input record from Curated Parquet.

        Returns
        -------
        dict or None
            Mapped event dictionary if valid, None if malformed.
        """
        self.stats["scanned"] += 1

        try:
            event = row_to_kafka_event(raw_row)
        except ValueError as exc:
            self.stats["malformed"] += 1
            if self.stats["malformed"] <= 5:
                LOG.warning("MALFORMED_RECORD #%d reason=%s", self.stats["scanned"], exc)
            return None

        # Verify with JSON Schema validator if available
        if self.validator is not None:
            errors = list(self.validator.iter_errors(event))
            if errors:
                self.stats["malformed"] += 1
                LOG.warning("SCHEMA_VALIDATION_ERROR: %s", errors[0].message)
                return None

        self.stats["valid"] += 1
        key_bytes, value_bytes = serialize_kafka_message(event)

        if not self.dry_run:
            if self.producer is None:
                raise RuntimeError("Producer not initialized in SEND mode")

            try:
                future = self.producer.send(self.topic, key=key_bytes, value=value_bytes)
                # Synchronous wait for ACK to guarantee delivery without silent record loss
                future.get(timeout=20)
                self.stats["sent_ack"] += 1
            except Exception as exc:
                self.stats["send_failed"] += 1
                LOG.error("KAFKA_SEND_FAILURE record #%d: %s", self.stats["scanned"], exc)
                raise RuntimeError(f"Kafka send failed: {exc}") from exc

        return event

    def run(self, records_iterator: Iterator[Dict[str, Any]], limit: int = 0) -> Dict[str, int]:
        """Execute streaming workflow over an iterator of records.

        Parameters
        ----------
        records_iterator : iterator
            Stream of raw row dictionaries.
        limit : int, optional
            Max records to process (0 = all).

        Returns
        -------
        dict
            Processing summary statistics.
        """
        start_time = time.monotonic()
        LOG.info(
            "START EventProducer mode=%s topic=%s limit=%d",
            "DRY-RUN" if self.dry_run else "SEND",
            self.topic,
            limit,
        )

        try:
            self.init_kafka_producer()
            for record in records_iterator:
                self.process_record(record)

                if limit > 0 and self.stats["valid"] >= limit:
                    LOG.info("Reached target limit of %d valid records.", limit)
                    break

                if self.rate_limit > 0:
                    time.sleep(1.0 / self.rate_limit)

        finally:
            if self.producer is not None:
                try:
                    self.producer.flush(timeout=30)
                    self.producer.close(timeout=30)
                except Exception as exc:
                    LOG.error("Failed to close KafkaProducer cleanly: %s", exc)

            elapsed = time.monotonic() - start_time
            LOG.info(
                "SUMMARY scanned=%d valid=%d malformed=%d sent_ack=%d send_failed=%d elapsed=%.2fs",
                self.stats["scanned"],
                self.stats["valid"],
                self.stats["malformed"],
                self.stats["sent_ack"],
                self.stats["send_failed"],
                elapsed,
            )

        return self.stats


def arguments():
    parser = argparse.ArgumentParser(
        description="Production Kafka Event Producer (Task S2 / Contract v1.0)"
    )
    parser.add_argument(
        "--input", default=CURATED_PRODUCTION_PATH,
        help=f"Path to Curated Parquet dataset (default: {CURATED_PRODUCTION_PATH})"
    )
    parser.add_argument(
        "--topic", default=PRODUCTION_TOPIC,
        help=f"Kafka topic (must be '{PRODUCTION_TOPIC}' for production)"
    )
    parser.add_argument(
        "--bootstrap-servers", default="127.0.0.1:9092",
        help="Kafka bootstrap servers host:port[,host:port]"
    )
    parser.add_argument(
        "--schema", default="src/streaming/schemas/kafka_event_schema_v1.json",
        help="Path to JSON Schema Draft 7 validation file"
    )
    parser.add_argument(
        "--send", action="store_true",
        help="Explicitly enable real Kafka transmission (defaults to DRY-RUN mode)"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max valid records to process (0 = all records)"
    )
    parser.add_argument(
        "--rate", type=float, default=0.0,
        help="Max messages per second pacing (0.0 = unlimited)"
    )
    return parser


def main(argv=None) -> int:
    args = arguments().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.send and args.topic != PRODUCTION_TOPIC:
        LOG.error("Production transmission must use topic '%s', got '%s'", PRODUCTION_TOPIC, args.topic)
        return 1

    producer = EventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        schema_path=args.schema,
        dry_run=not args.send,
        rate_limit=args.rate,
    )

    # Initialize Spark if running in Spark environment
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("KafkaEventProducer").getOrCreate()
        records_iter = read_curated_records_pyspark(spark, args.input)
    except Exception as exc:
        LOG.error("Failed to initialize Spark reader for input '%s': %s", args.input, exc)
        return 1

    try:
        producer.run(records_iter, limit=args.limit)
    except Exception as exc:
        LOG.error("Producer execution failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
