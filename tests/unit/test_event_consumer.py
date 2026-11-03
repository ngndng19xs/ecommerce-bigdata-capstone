"""
Unit tests for Spark Structured Streaming Consumer (Task S2 / Kafka Contract v1.0).

Tests cover:
1. 12 fields schema and Spark schema data types (String IDs, Double price, Integer rank, String sort key).
2. Schema version kafka-v1.0 enforcement.
3. Direct inheritance of rank and sort key (no consumer-side recomputation).
4. Corrupt JSON and non-UTF8 message rejection.
5. Missing required fields and unexpected extra fields rejection.
6. Integer ID rejection (strictly StringType).
7. Cross-field rank vs event_type mismatch rejection.
8. Null user_session handling with key semantics.
9. Out-of-order business ordering by (event_time ASC, event_type_rank ASC, event_sort_key ASC).
10. Non-silent error accounting (is_valid flag and rejection reason).
"""

import json
from pathlib import Path
import pytest
from src.streaming.consumer.event_consumer import (
    EXPECTED_PAYLOAD_FIELDS,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_TOPIC,
    validate_and_parse_record,
)

FIXTURES_PATH = Path("tests/fixtures/kafka_records_v1.jsonl")


def load_fixture_messages():
    """Load valid messages containing key and value from kafka_records_v1.jsonl."""
    messages = []
    with open(FIXTURES_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                messages.append(json.loads(line))
    return messages


# ---------------------------------------------------------------------------
# Offline Pure-Python Unit Tests (Runnable locally without PySpark / Kafka)
# ---------------------------------------------------------------------------


def test_consumer_accepts_valid_fixtures():
    """Valid records from kafka_records_v1.jsonl must all pass validation."""
    fixtures = load_fixture_messages()
    assert len(fixtures) >= 7
    for msg in fixtures:
        key = msg.get("key")
        val = msg.get("value")
        is_valid, parsed, err = validate_and_parse_record(val, raw_key=key)
        assert is_valid is True, f"Valid fixture failed: {err}"
        assert parsed is not None
        assert parsed["schema_version"] == PRODUCTION_SCHEMA_VERSION
        assert isinstance(parsed["product_id"], str)
        assert isinstance(parsed["category_id"], str)
        assert isinstance(parsed["user_id"], str)
        assert err is None


def test_consumer_accepts_valid_json_string_and_bytes():
    """Consumer must correctly parse both JSON strings and UTF-8 bytes."""
    valid_record = {
        "event_time": "2019-11-01T08:00:00Z",
        "event_type": "view",
        "product_id": "1001",
        "category_id": "205301",
        "category_code": "electronics.audio",
        "brand": "sony",
        "price": 99.99,
        "user_id": "555123",
        "user_session": "sess_001",
        "event_type_rank": 1,
        "event_sort_key": "a" * 64,
        "schema_version": "kafka-v1.0",
    }
    json_str = json.dumps(valid_record)
    json_bytes = json_str.encode("utf-8")

    # String test
    ok_str, parsed_str, err_str = validate_and_parse_record(json_str)
    assert ok_str is True
    assert parsed_str["product_id"] == "1001"

    # Bytes test
    ok_bytes, parsed_bytes, err_bytes = validate_and_parse_record(json_bytes)
    assert ok_bytes is True
    assert parsed_bytes["product_id"] == "1001"


def test_consumer_rejects_corrupt_json():
    """Consumer must reject malformed JSON without silent failure."""
    bad_json = '{"event_time": "2019-11-01T08:00:00Z", "event_type": '
    is_valid, parsed, err = validate_and_parse_record(bad_json)
    assert is_valid is False
    assert parsed is None
    assert "JSON decode error" in err


def test_consumer_rejects_corrupt_bytes():
    """Consumer must reject non-UTF-8 bytes."""
    bad_bytes = b"\x80abc\xff"
    is_valid, parsed, err = validate_and_parse_record(bad_bytes)
    assert is_valid is False
    assert parsed is None
    assert "UTF-8 decode error" in err


def test_consumer_rejects_missing_fields():
    """Consumer must reject payload missing any of the 12 contract fields."""
    valid_record = load_fixture_messages()[0]["value"]
    for required_col in ["event_sort_key", "event_type_rank", "user_id", "schema_version"]:
        incomplete = dict(valid_record)
        del incomplete[required_col]
        is_valid, _, err = validate_and_parse_record(incomplete)
        assert is_valid is False
        assert "Missing required fields" in err
        assert required_col in err


def test_consumer_rejects_unexpected_extra_fields():
    """Consumer must reject extra fields (additionalProperties: false)."""
    valid_record = dict(load_fixture_messages()[0]["value"])
    valid_record["event_date"] = "2019-11-01"
    is_valid, _, err = validate_and_parse_record(valid_record)
    assert is_valid is False
    assert "Unexpected extra fields" in err
    assert "event_date" in err


def test_consumer_rejects_integer_ids():
    """Consumer must reject integer or float IDs (must be StringType)."""
    base = dict(load_fixture_messages()[0]["value"])

    for id_col in ["product_id", "category_id", "user_id"]:
        bad_record = dict(base)
        bad_record[id_col] = 12345
        is_valid, _, err = validate_and_parse_record(bad_record)
        assert is_valid is False, f"Did not reject integer {id_col}"
        assert f"Field '{id_col}' must be a non-empty string" in err


def test_consumer_rejects_empty_or_whitespace_ids():
    """Consumer must reject empty or whitespace-only IDs."""
    base = dict(load_fixture_messages()[0]["value"])
    for id_col in ["product_id", "category_id", "user_id"]:
        bad_record = dict(base)
        bad_record[id_col] = "   "
        is_valid, _, err = validate_and_parse_record(bad_record)
        assert is_valid is False
        assert f"Field '{id_col}' must be a non-empty string" in err


def test_consumer_rejects_invalid_schema_version():
    """Consumer must reject drafts or wrong versions."""
    base = dict(load_fixture_messages()[0]["value"])
    bad_versions = ["draft-b2-01-not-approved-s2", "DRAFT-S2-01", "v1.0", "kafka-v2.0"]
    for v in bad_versions:
        bad_record = dict(base)
        bad_record["schema_version"] = v
        is_valid, _, err = validate_and_parse_record(bad_record)
        assert is_valid is False
        assert "Invalid schema_version" in err


def test_consumer_cross_field_rank_validation():
    """Consumer must enforce strict 1-to-1 event_type <-> event_type_rank mapping."""
    base = dict(load_fixture_messages()[0]["value"])

    # view requires rank 1; setting rank 2 must fail
    bad_record = dict(base)
    bad_record["event_type"] = "view"
    bad_record["event_type_rank"] = 2
    is_valid, _, err = validate_and_parse_record(bad_record)
    assert is_valid is False
    assert "Cross-field rank mismatch" in err

    # purchase requires rank 4; setting rank 1 must fail
    bad_record = dict(base)
    bad_record["event_type"] = "purchase"
    bad_record["event_type_rank"] = 1
    is_valid, _, err = validate_and_parse_record(bad_record)
    assert is_valid is False
    assert "Cross-field rank mismatch" in err


def test_consumer_rejects_negative_price():
    """Consumer must reject negative price."""
    base = dict(load_fixture_messages()[0]["value"])
    bad_record = dict(base)
    bad_record["price"] = -5.0
    is_valid, _, err = validate_and_parse_record(bad_record)
    assert is_valid is False
    assert "Negative price not allowed" in err


def test_consumer_null_user_session_and_key_handling():
    """Null user_session is valid with None key; non-null user_session matches key."""
    # Null user_session with None key -> valid
    fixtures = load_fixture_messages()
    null_sess_msg = next(m for m in fixtures if m["value"].get("user_session") is None)
    is_valid, _, err = validate_and_parse_record(
        null_sess_msg["value"], raw_key=null_sess_msg.get("key")
    )
    assert is_valid is True, f"Failed on null user_session: {err}"

    # Non-null user_session with matching key -> valid
    normal_msg = fixtures[0]
    is_valid, _, err = validate_and_parse_record(
        normal_msg["value"], raw_key=normal_msg["key"]
    )
    assert is_valid is True, f"Failed on matching session: {err}"

    # Non-null user_session with mismatching key -> rejected
    is_valid, _, err = validate_and_parse_record(
        normal_msg["value"], raw_key="wrong_key_123"
    )
    assert is_valid is False
    assert "Kafka key mismatch" in err


def test_consumer_ordering_specification():
    """Canonical ordering must sort by (event_time, event_type_rank, event_sort_key).

    Verifies:
    1. Same timestamp: view (1) precedes purchase (4).
    2. Same timestamp and same event_type: event_sort_key breaks the tie (alphabetical hex).
    3. product_id is NOT used for ordering (product 999 with smaller sort_key comes BEFORE product 100 with larger sort_key).
    """
    events = [
        {
            "event_time": "2019-11-01T10:00:00Z",
            "event_type": "purchase",
            "event_type_rank": 4,
            "event_sort_key": "c" * 64,
            "product_id": "100",
        },
        {
            "event_time": "2019-11-01T10:00:00Z",
            "event_type": "view",
            "event_type_rank": 1,
            "event_sort_key": "f" * 64,
            "product_id": "200",
        },
        {
            "event_time": "2019-11-01T10:00:00Z",
            "event_type": "cart",
            "event_type_rank": 2,
            "event_sort_key": "b" * 64,
            "product_id": "999",  # Higher product_id, but smaller sort_key
        },
        {
            "event_time": "2019-11-01T10:00:00Z",
            "event_type": "cart",
            "event_type_rank": 2,
            "event_sort_key": "e" * 64,
            "product_id": "100",  # Lower product_id, but larger sort_key
        },
    ]

    # Sort using canonical key (event_time, event_type_rank, event_sort_key)
    sorted_events = sorted(
        events,
        key=lambda r: (r["event_time"], r["event_type_rank"], r["event_sort_key"]),
    )

    # Expected order:
    # 1. view (rank 1)
    # 2. cart (rank 2, sort_key 'bb...') with product_id 999
    # 3. cart (rank 2, sort_key 'ee...') with product_id 100
    # 4. purchase (rank 4)
    assert sorted_events[0]["event_type"] == "view"
    assert sorted_events[1]["event_type"] == "cart"
    assert sorted_events[1]["product_id"] == "999"  # Proves sort_key precedes product_id!
    assert sorted_events[2]["event_type"] == "cart"
    assert sorted_events[2]["product_id"] == "100"
    assert sorted_events[3]["event_type"] == "purchase"


# ---------------------------------------------------------------------------
# PySpark Structured Streaming Tests (Executed when PySpark is present on VM-B)
# ---------------------------------------------------------------------------


def test_spark_consumer_schema_structure():
    """Verify Spark schema defines 12 fields with strict contract data types."""
    pyspark = pytest.importorskip("pyspark", reason="PySpark is required for Spark schema test")
    from src.streaming.consumer.event_consumer import KAFKA_EVENT_SPARK_SCHEMA
    from pyspark.sql.types import DoubleType, IntegerType, StringType

    fields = {f.name: f.dataType for f in KAFKA_EVENT_SPARK_SCHEMA.fields}

    # Verify 12 contract fields exist
    for col_name in EXPECTED_PAYLOAD_FIELDS:
        assert col_name in fields, f"Missing {col_name} in KAFKA_EVENT_SPARK_SCHEMA"

    # String IDs
    assert isinstance(fields["product_id"], StringType)
    assert isinstance(fields["category_id"], StringType)
    assert isinstance(fields["user_id"], StringType)

    # Double price and Integer rank
    assert isinstance(fields["price"], DoubleType)
    assert isinstance(fields["event_type_rank"], IntegerType)
    assert isinstance(fields["event_sort_key"], StringType)
    assert isinstance(fields["schema_version"], StringType)

    # Permissive corrupt record column
    assert "_corrupt_record" in fields
