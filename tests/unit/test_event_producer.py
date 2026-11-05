"""
Unit tests for Production Kafka Event Producer (Task S2 / Version kafka-v1.0).
"""

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import sys
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.streaming.producer.event_producer import (
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_TOPIC,
    EventProducer,
    format_iso8601_utc,
    row_to_kafka_event,
    serialize_kafka_message,
)


class EventProducerTest(unittest.TestCase):
    """Test suite for event_producer mapping, validation, and serialization."""

    def setUp(self):
        self.valid_parquet_row = {
            "event_time": datetime(2019, 10, 1, 0, 0, 0, tzinfo=timezone.utc),
            "event_type": "view",
            "product_id": 44600062,
            "category_id": 2103807459595387724,
            "category_code": "electronics.smartphone",
            "brand": "shiseido",
            "price": 35.79,
            "user_id": 541312140,
            "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
            "event_date": "2019-10-01",                     # Should be excluded
            "reject_reason": "",                            # Should be excluded
            "event_type_rank": 1,
            "event_sort_key": "b472e987c67e914eb16bb2153b6fa286e4ea5da11cf5ea9d6fc578c78d59dc6b",
        }

    # 1. Mapping đúng 12 trường và loại bỏ các cột không được phép
    def test_01_row_to_kafka_event_valid_12_fields(self):
        event = row_to_kafka_event(self.valid_parquet_row)

        self.assertEqual(len(event), 12)
        self.assertNotIn("event_date", event)
        self.assertNotIn("reject_reason", event)

        # 3 ID must be string
        self.assertEqual(event["product_id"], "44600062")
        self.assertIsInstance(event["product_id"], str)
        self.assertEqual(event["category_id"], "2103807459595387724")
        self.assertIsInstance(event["category_id"], str)
        self.assertEqual(event["user_id"], "541312140")
        self.assertIsInstance(event["user_id"], str)

        # Time, version, rank, sort_key
        self.assertEqual(event["event_time"], "2019-10-01T00:00:00Z")
        self.assertEqual(event["schema_version"], PRODUCTION_SCHEMA_VERSION)
        self.assertEqual(event["event_type_rank"], 1)
        self.assertEqual(
            event["event_sort_key"],
            "b472e987c67e914eb16bb2153b6fa286e4ea5da11cf5ea9d6fc578c78d59dc6b",
        )

    # 2. Timestamp UTC formatting
    def test_02_timestamp_utc_formatting(self):
        dt_naive = datetime(2019, 10, 15, 12, 30, 45)
        self.assertEqual(format_iso8601_utc(dt_naive), "2019-10-15T12:30:45Z")

        str_space = "2019-11-05 14:20:10"
        self.assertEqual(format_iso8601_utc(str_space), "2019-11-05T14:20:10Z")

        str_utc = "2019-11-05 14:20:10 UTC"
        self.assertEqual(format_iso8601_utc(str_utc), "2019-11-05T14:20:10Z")

    # 3. ID là NULL hoặc rỗng bị từ chối rõ ràng
    def test_03_null_or_empty_ids_rejected(self):
        for id_field in ("product_id", "category_id", "user_id"):
            with self.subTest(field=id_field):
                # None
                bad_row_none = dict(self.valid_parquet_row, **{id_field: None})
                with self.assertRaises(ValueError) as ctx:
                    row_to_kafka_event(bad_row_none)
                self.assertIn("NULL", str(ctx.exception))

                # Empty string
                bad_row_empty = dict(self.valid_parquet_row, **{id_field: "   "})
                with self.assertRaises(ValueError) as ctx:
                    row_to_kafka_event(bad_row_empty)
                self.assertIn("empty", str(ctx.exception))

    # 4. Kiểm tra chéo rank vs event_type
    def test_04_cross_field_rank_mismatch_rejected(self):
        mismatched_row = dict(self.valid_parquet_row, event_type="cart", event_type_rank=1)
        with self.assertRaises(ValueError) as ctx:
            row_to_kafka_event(mismatched_row)
        self.assertIn("rank mismatch", str(ctx.exception).lower())

    # 5. Giá trị price >= 0.0 (chấp nhận 0.0, từ chối âm)
    def test_05_price_validation(self):
        # 0.0 is valid
        zero_price_row = dict(self.valid_parquet_row, price=0.0)
        event = row_to_kafka_event(zero_price_row)
        self.assertEqual(event["price"], 0.0)

        # negative price raises ValueError
        neg_row = dict(self.valid_parquet_row, price=-0.01)
        with self.assertRaises(ValueError):
            row_to_kafka_event(neg_row)

    # 6. Brand fallback về 'Unknown'
    def test_06_brand_fallback_to_unknown(self):
        row_null_brand = dict(self.valid_parquet_row, brand=None)
        event1 = row_to_kafka_event(row_null_brand)
        self.assertEqual(event1["brand"], "Unknown")

        row_empty_brand = dict(self.valid_parquet_row, brand="")
        event2 = row_to_kafka_event(row_empty_brand)
        self.assertEqual(event2["brand"], "Unknown")

    # 7. Serialization của Kafka message (Key và Value)
    def test_07_serialize_kafka_message(self):
        event = row_to_kafka_event(self.valid_parquet_row)
        key_bytes, val_bytes = serialize_kafka_message(event)

        # Non-null user_session -> key_bytes is UTF-8
        self.assertEqual(key_bytes, b"72d76fde-8bb3-4e00-8c23-a032dfed738c")
        val_parsed = json.loads(val_bytes.decode("utf-8"))
        self.assertEqual(val_parsed["product_id"], "44600062")

        # Null user_session -> key_bytes is None
        event_null_session = dict(event, user_session=None)
        key_null, _ = serialize_kafka_message(event_null_session)
        self.assertIsNone(key_null)

    # 8. Producer chạy ở chế độ Dry-run (mặc định) không gửi Kafka
    def test_08_producer_dry_run_does_not_send(self):
        producer = EventProducer(dry_run=True)
        records = [dict(self.valid_parquet_row) for _ in range(3)]

        stats = producer.run(iter(records))
        self.assertEqual(stats["scanned"], 3)
        self.assertEqual(stats["valid"], 3)
        self.assertEqual(stats["malformed"], 0)
        self.assertEqual(stats["sent_ack"], 0)
        self.assertIsNone(producer.producer)

    # 9. Send mode yêu cầu đúng topic 'ecommerce_events'
    def test_09_send_mode_requires_production_topic(self):
        producer_wrong_topic = EventProducer(topic="wrong_topic", dry_run=False)
        with self.assertRaises(ValueError) as ctx:
            producer_wrong_topic.init_kafka_producer()
        self.assertIn(PRODUCTION_TOPIC, str(ctx.exception))

    # 10. Báo cáo chính xác số lượng bản ghi hợp lệ và lỗi
    def test_10_error_accounting_on_malformed_records(self):
        bad_row = dict(self.valid_parquet_row, product_id=None)
        records = [self.valid_parquet_row, bad_row, self.valid_parquet_row]

        producer = EventProducer(dry_run=True)
        stats = producer.run(iter(records))

        self.assertEqual(stats["scanned"], 3)
        self.assertEqual(stats["valid"], 2)
        self.assertEqual(stats["malformed"], 1)


if __name__ == "__main__":
    unittest.main()
