#!/usr/bin/env python3
"""
Task P2-B-S2-01: Offline validation utility for Kafka JSON Contract (DRAFT-S2-01).

Validates simulated Kafka record batches (JSONL format) without requiring
active Kafka brokers, Zookeeper, Spark, or HDFS connectivity.

Usage:
    python scripts/validate_kafka_contract.py tests/fixtures/kafka_records_DRAFT.jsonl
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Contract Constants (DRAFT-S2-01)
DRAFT_SCHEMA_VERSION = "DRAFT-S2-01"
VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
EVENT_TYPE_TO_RANK = {
    "view": 1,
    "cart": 2,
    "remove_from_cart": 3,
    "purchase": 4,
}
EVENT_TIME_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
EVENT_SORT_KEY_REGEX = re.compile(r"^[a-f0-9]{64}$")

EXPECTED_VALUE_FIELDS = {
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
}


class ValidationError(ValueError):
    """Raised when a Kafka record fails contract constraints."""
    pass


def validate_event_value(value, schema_dict=None):
    """Validate the 'value' payload against DRAFT-S2-01 contract specifications.

    Parameters
    ----------
    value : dict
        Deserialized JSON payload representing the event record.
    schema_dict : dict, optional
        Optional loaded JSON schema. If jsonschema package is available,
        it will be validated against this schema dictionary as well.

    Returns
    -------
    list of str
        List of validation error messages. Empty list if record is valid.
    """
    errors = []

    if not isinstance(value, dict):
        return ["Payload 'value' must be a JSON object (dict)."]

    # 1. Check for extra or missing properties (additionalProperties: false)
    present_keys = set(value.keys())
    missing_keys = EXPECTED_VALUE_FIELDS - present_keys
    extra_keys = present_keys - EXPECTED_VALUE_FIELDS

    if missing_keys:
        errors.append(f"Missing required fields: {sorted(missing_keys)}")
    if extra_keys:
        errors.append(f"Unexpected extra fields (additionalProperties=false): {sorted(extra_keys)}")

    # 2. event_time
    if "event_time" in value:
        t = value["event_time"]
        if not isinstance(t, str):
            errors.append("event_time must be a string.")
        elif not EVENT_TIME_REGEX.match(t):
            errors.append(f"event_time '{t}' does not match ISO-8601 UTC format (YYYY-MM-DDTHH:MM:SSZ).")

    # 3. event_type
    event_type = value.get("event_type")
    if "event_type" in value:
        if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
            errors.append(f"Invalid event_type '{event_type}'. Must be one of {sorted(VALID_EVENT_TYPES)}.")

    # 4. event_type_rank
    if "event_type_rank" in value:
        rank = value["event_type_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            errors.append("event_type_rank must be an integer.")
        elif rank not in {1, 2, 3, 4}:
            errors.append(f"event_type_rank {rank} must be between 1 and 4.")
        elif event_type in EVENT_TYPE_TO_RANK and rank != EVENT_TYPE_TO_RANK[event_type]:
            errors.append(
                f"event_type_rank mismatch: event_type '{event_type}' requires rank {EVENT_TYPE_TO_RANK[event_type]}, got {rank}."
            )

    # 5. product_id
    if "product_id" in value:
        pid = value["product_id"]
        if not isinstance(pid, int) or isinstance(pid, bool) or pid < 0:
            errors.append("product_id must be a non-negative integer (Long).")

    # 6. category_id
    if "category_id" in value:
        cid = value["category_id"]
        if not isinstance(cid, int) or isinstance(cid, bool) or cid < 0:
            errors.append("category_id must be a non-negative integer (Long).")

    # 7. category_code
    if "category_code" in value:
        cc = value["category_code"]
        if cc is not None and not isinstance(cc, str):
            errors.append("category_code must be string or null.")

    # 8. brand (In curated data, null/empty must be normalized to 'Unknown')
    if "brand" in value:
        brand = value["brand"]
        if not isinstance(brand, str):
            errors.append("brand must be a non-null string.")
        elif not brand.strip():
            errors.append("brand in Curated data cannot be empty or whitespace (must be normalized to 'Unknown').")

    # 9. price
    if "price" in value:
        price = value["price"]
        if isinstance(price, bool) or not isinstance(price, (int, float)):
            errors.append("price must be a number (Double).")
        elif price < 0.0:
            errors.append(f"price cannot be negative (got {price}).")

    # 10. user_id
    if "user_id" in value:
        uid = value["user_id"]
        if not isinstance(uid, int) or isinstance(uid, bool) or uid < 0:
            errors.append("user_id must be a non-negative integer (Long).")

    # 11. user_session
    if "user_session" in value:
        us = value["user_session"]
        if us is not None and not isinstance(us, str):
            errors.append("user_session must be string or null.")

    # 12. event_sort_key
    if "event_sort_key" in value:
        k = value["event_sort_key"]
        if not isinstance(k, str):
            errors.append("event_sort_key must be a string.")
        elif not EVENT_SORT_KEY_REGEX.match(k):
            errors.append(f"event_sort_key '{k}' must be a 64-character lowercase hexadecimal SHA-256 string.")

    # 13. schema_version
    if "schema_version" in value:
        sv = value["schema_version"]
        if sv != DRAFT_SCHEMA_VERSION:
            errors.append(f"schema_version '{sv}' does not match expected draft '{DRAFT_SCHEMA_VERSION}'.")

    # Optional: jsonschema library validation if schema_dict provided
    if schema_dict:
        try:
            import jsonschema
            validator = jsonschema.Draft7Validator(schema_dict)
            for err in validator.iter_errors(value):
                errors.append(f"JSONSchema validation: {err.message}")
        except ImportError:
            pass

    return errors


def validate_kafka_record(record, schema_dict=None):
    """Validate an entire simulated Kafka record containing 'key' and 'value'.

    Parameters
    ----------
    record : dict
        Simulated Kafka message with 'key' and 'value'.
    schema_dict : dict, optional
        Loaded JSON schema dict.

    Returns
    -------
    list of str
        List of validation error messages.
    """
    errors = []
    if not isinstance(record, dict):
        return ["Record must be a JSON object containing 'key' and 'value'."]

    if "key" not in record or "value" not in record:
        return ["Record missing 'key' or 'value' attributes."]

    key = record["key"]
    value = record["value"]

    # Validate value
    value_errors = validate_event_value(value, schema_dict=schema_dict)
    errors.extend(value_errors)

    # Validate key semantics against user_session
    if isinstance(value, dict) and "user_session" in value:
        user_session = value["user_session"]
        if user_session is None:
            if key is not None:
                errors.append(
                    f"When user_session is NULL, Kafka message key must be null (None), but got: {repr(key)}."
                )
        else:
            if key != user_session:
                errors.append(
                    f"Kafka message key must equal user_session ('{user_session}'), but got: {repr(key)}."
                )
            if key in ("null", "None"):
                errors.append(f"Kafka message key must not be literal string '{key}'.")

    return errors


def validate_jsonl_file(file_path: Path, schema_path: Path = None):
    """Validate a .jsonl file containing simulated Kafka records line by line.

    Parameters
    ----------
    file_path : Path
        Path to the JSONL file.
    schema_path : Path, optional
        Path to the kafka_event_schema_DRAFT.json file.

    Returns
    -------
    tuple (int, int, list)
        (total_lines, valid_lines, failure_details)
    """
    schema_dict = None
    if schema_path and schema_path.exists():
        with open(schema_path, "r", encoding="utf-8") as sf:
            schema_dict = json.load(sf)

    total_lines = 0
    valid_lines = 0
    failures = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line_str = line.strip()
            if not line_str:
                continue

            total_lines += 1
            try:
                record = json.loads(line_str)
            except json.JSONDecodeError as exc:
                failures.append((line_num, [f"JSON parsing error: {exc}"]))
                continue

            record_errors = validate_kafka_record(record, schema_dict=schema_dict)
            if record_errors:
                failures.append((line_num, record_errors))
            else:
                valid_lines += 1

    return total_lines, valid_lines, failures


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Offline validator for Kafka JSON Contract (Task P2-B-S2-01)."
    )
    parser.add_argument(
        "jsonl_file",
        help="Path to .jsonl file containing simulated Kafka records."
    )
    parser.add_argument(
        "--schema",
        default=str(Path(__file__).resolve().parents[1] / "src" / "streaming" / "schemas" / "kafka_event_schema_DRAFT.json"),
        help="Optional path to draft JSON Schema file."
    )
    args = parser.parse_args(argv)

    jsonl_path = Path(args.jsonl_file)
    schema_path = Path(args.schema)

    if not jsonl_path.exists():
        print(f"FAIL: File not found: {jsonl_path}", file=sys.stderr)
        return 1

    print(f"=== Validating Kafka Contract: {jsonl_path.name} ===")
    total, valid, failures = validate_jsonl_file(jsonl_path, schema_path=schema_path)

    print(f"Scanned lines: {total}")
    print(f"Valid records: {valid}")
    print(f"Failed records: {len(failures)}")

    if failures:
        print("\n--- Failure Details ---", file=sys.stderr)
        for line_num, errs in failures:
            print(f"Line {line_num}:", file=sys.stderr)
            for err in errs:
                print(f"  - {err}", file=sys.stderr)
        print("\nRESULT: FAIL", file=sys.stderr)
        return 1

    print("\nRESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())

