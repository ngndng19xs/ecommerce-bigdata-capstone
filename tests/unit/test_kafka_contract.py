"""
Nhiệm vụ P2-B-S2-01: Bộ kiểm thử đơn vị cho Hợp đồng Kafka JSON (DRAFT-S2-01).

Kiểm tra tính hợp lệ của các ràng buộc schema, định dạng timestamp UTC, thứ tự ưu tiên sự kiện,
quy tắc khóa phân vùng (Kafka message key), sự tuân thủ JSON Schema Draft 7, tính toàn vẹn và
sự tồn tại của đường dẫn schema.
Hoạt động hoàn toàn ngoại tuyến (offline), không yêu cầu kết nối Kafka Broker, Zookeeper hay Spark.
"""

import json
import tempfile
import unittest
from pathlib import Path

# Add project root to path for imports
import sys
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.validate_kafka_contract import (
    DRAFT_SCHEMA_VERSION,
    EXPECTED_VALUE_FIELDS,
    load_json_schema,
    main as validator_main,
    validate_event_value,
    validate_kafka_record,
    validate_jsonl_file,
)


class KafkaContractTest(unittest.TestCase):
    """Bộ kiểm thử đơn vị thẩm định Hợp đồng Kafka JSON."""

    def setUp(self):
        """Chuẩn bị bản ghi mẫu hợp lệ làm cơ sở cho các ca kiểm thử biến dị (mutation)."""
        self.fixtures_dir = REPO_ROOT / "tests" / "fixtures"
        self.fixture_file = self.fixtures_dir / "kafka_records_DRAFT.jsonl"
        self.schema_file = (
            REPO_ROOT / "src" / "streaming" / "schemas" / "kafka_event_schema_DRAFT.json"
        )

        self.valid_value = {
            "event_time": "2019-10-01T00:00:00Z",
            "event_type": "view",
            "product_id": 1004856,
            "category_id": 2053013555631882655,
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": 520088429,
            "user_session": "d6c3d980-d281-4203-bc85-999ac65daec9",
            "event_type_rank": 1,
            "event_sort_key": "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92a",
            "schema_version": "DRAFT-S2-01",
        }

        self.valid_record = {
            "key": "d6c3d980-d281-4203-bc85-999ac65daec9",
            "value": dict(self.valid_value),
        }

    # 1. JSON hợp lệ & baseline
    def test_01_valid_record_passes(self):
        errors = validate_kafka_record(self.valid_record)
        self.assertEqual(errors, [], f"Expected 0 errors on valid record, got: {errors}")

    # 2. Schema đủ các trường bắt buộc
    def test_02_missing_required_fields_fails(self):
        for field in EXPECTED_VALUE_FIELDS:
            with self.subTest(missing_field=field):
                mutated = dict(self.valid_value)
                del mutated[field]
                record = {"key": mutated.get("user_session"), "value": mutated}
                errors = validate_kafka_record(record)
                self.assertTrue(
                    any(f"Missing required fields" in err and field in err for err in errors),
                    f"Expected missing field error for '{field}', got: {errors}",
                )

    # 3. Timestamp đúng UTC đề xuất
    def test_03_valid_utc_timestamp_passes(self):
        valid_times = [
            "2019-10-01T00:00:00Z",
            "2019-11-30T23:59:59Z",
            "2019-10-15T12:30:45Z",
        ]
        for t in valid_times:
            with self.subTest(time=t):
                mutated = dict(self.valid_value, event_time=t)
                errors = validate_event_value(mutated)
                self.assertEqual(errors, [])

    # 4. Từ chối timestamp sai định dạng
    def test_04_invalid_timestamp_rejected(self):
        invalid_times = [
            "2019-10-01 00:00:00 UTC",   # CSV format, not ISO-8601 Z
            "2019-10-01T00:00:00",       # Missing Z
            "2019-10-01T00:00:00+07:00", # Non-UTC offset
            "2019/10/01 00:00:00",       # Slash delimiter
            "invalid-timestamp-string",
            1569888000,                  # Epoch integer
        ]
        for t in invalid_times:
            with self.subTest(invalid_time=t):
                mutated = dict(self.valid_value, event_time=t)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("event_time" in err for err in errors),
                    f"Expected timestamp rejection for '{t}', got {errors}",
                )

    # 5. event_type hợp lệ
    def test_05_valid_event_types_pass(self):
        valid_types = [
            ("view", 1),
            ("cart", 2),
            ("remove_from_cart", 3),
            ("purchase", 4),
        ]
        for etype, rank in valid_types:
            with self.subTest(event_type=etype):
                mutated = dict(self.valid_value, event_type=etype, event_type_rank=rank)
                errors = validate_event_value(mutated)
                self.assertEqual(errors, [])

    # 6. event_type_rank khớp event_type
    def test_06_event_type_rank_mismatch_rejected(self):
        mismatches = [
            ("view", 2),
            ("cart", 1),
            ("purchase", 2),
            ("remove_from_cart", 4),
            ("view", 99),
        ]
        for etype, rank in mismatches:
            with self.subTest(etype=etype, rank=rank):
                mutated = dict(self.valid_value, event_type=etype, event_type_rank=rank)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("event_type_rank" in err for err in errors),
                    f"Expected rank mismatch error for {etype} with rank {rank}, got {errors}",
                )

    # 7. user_session khác NULL có key tương ứng
    def test_07_non_null_user_session_matches_key(self):
        sess = "custom-session-uuid-12345"
        record = {
            "key": sess,
            "value": dict(self.valid_value, user_session=sess),
        }
        errors = validate_kafka_record(record)
        self.assertEqual(errors, [])

        # Mismatch key
        record_mismatch = {
            "key": "wrong-key",
            "value": dict(self.valid_value, user_session=sess),
        }
        errors_mismatch = validate_kafka_record(record_mismatch)
        self.assertTrue(
            any("Kafka message key must equal user_session" in err for err in errors_mismatch),
            f"Expected key mismatch error, got {errors_mismatch}",
        )

    # 8. user_session NULL có key null
    def test_08_null_user_session_requires_null_key(self):
        record_null = {
            "key": None,
            "value": dict(self.valid_value, user_session=None),
        }
        errors_null = validate_kafka_record(record_null)
        self.assertEqual(errors_null, [])

        # Negative test: String 'null' or non-null key when user_session is null
        bad_keys = ["null", "None", "", "dummy-key"]
        for bad_key in bad_keys:
            with self.subTest(bad_key=bad_key):
                bad_record = {
                    "key": bad_key,
                    "value": dict(self.valid_value, user_session=None),
                }
                errs = validate_kafka_record(bad_record)
                self.assertTrue(
                    any("must be null (None)" in e for e in errs),
                    f"Expected rejection for key {bad_key} with null session, got {errs}",
                )

    # 9. price = 0 hợp lệ
    def test_09_zero_price_is_valid(self):
        mutated = dict(self.valid_value, price=0.0)
        errors = validate_event_value(mutated)
        self.assertEqual(errors, [], f"Expected price=0.0 to be valid, got {errors}")

    # 10. price âm không hợp lệ
    def test_10_negative_price_rejected(self):
        negative_prices = [-0.01, -1.0, -100.5]
        for p in negative_prices:
            with self.subTest(price=p):
                mutated = dict(self.valid_value, price=p)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("price cannot be negative" in err for err in errors),
                    f"Expected rejection for negative price {p}, got {errors}",
                )

    # 11. brand rỗng không hợp lệ với dữ liệu Curated
    def test_11_empty_brand_rejected(self):
        invalid_brands = ["", "   ", "\t"]
        for b in invalid_brands:
            with self.subTest(brand=repr(b)):
                mutated = dict(self.valid_value, brand=b)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("brand in Curated data cannot be empty" in err for err in errors),
                    f"Expected empty brand rejection, got {errors}",
                )

    # 12. event_sort_key đúng định dạng SHA-256 (64 hex lowercase)
    def test_12_event_sort_key_format_validated(self):
        valid_key = "a" * 64
        mutated = dict(self.valid_value, event_sort_key=valid_key)
        self.assertEqual(validate_event_value(mutated), [])

        invalid_keys = [
            "A" * 64,       # Uppercase not allowed
            "a" * 63,       # Length 63
            "a" * 65,       # Length 65
            "g" * 64,       # Non-hex character 'g'
            "",             # Empty
            12345,          # Non-string
        ]
        for k in invalid_keys:
            with self.subTest(invalid_key=k):
                bad_mutated = dict(self.valid_value, event_sort_key=k)
                errors = validate_event_value(bad_mutated)
                self.assertTrue(
                    any("event_sort_key" in err for err in errors),
                    f"Expected event_sort_key rejection for '{k}', got {errors}",
                )

    # 13. schema_version DRAFT tồn tại và đúng định dạng đề xuất
    def test_13_schema_version_draft_validated(self):
        self.assertEqual(self.valid_value["schema_version"], DRAFT_SCHEMA_VERSION)
        mutated = dict(self.valid_value, schema_version="v1.0-official")
        errors = validate_event_value(mutated)
        self.assertTrue(
            any("schema_version" in err for err in errors),
            f"Expected schema_version mismatch, got {errors}",
        )

    # 14. Không chấp nhận trường JSON ngoài danh sách trong bản DRAFT (additionalProperties=false)
    def test_14_additional_properties_rejected(self):
        mutated = dict(self.valid_value, unauthorized_extra_field="malicious_payload")
        errors = validate_event_value(mutated)
        self.assertTrue(
            any("Unexpected extra fields" in err for err in errors),
            f"Expected additional properties rejection, got {errors}",
        )

    # 15. Kiểm thử file fixture chuẩn
    def test_15_fixture_file_all_records_pass(self):
        self.assertTrue(self.fixture_file.exists(), f"Fixture file not found: {self.fixture_file}")
        total, valid, failures = validate_jsonl_file(self.fixture_file, schema_path=self.schema_file)
        self.assertGreater(total, 0, "Fixture file has 0 records.")
        self.assertEqual(
            failures, [],
            f"Fixture records failed validation: {failures}",
        )
        self.assertEqual(total, valid)

    # 16. Kiểm tra phát hiện đường dẫn Schema sai (FileNotFoundError)
    def test_16_schema_file_not_found_raises(self):
        non_existent = Path("non_existent_schema_file.json")
        with self.assertRaises(FileNotFoundError):
            load_json_schema(non_existent)

        with self.assertRaises(FileNotFoundError):
            validate_jsonl_file(self.fixture_file, schema_path=non_existent)

    # 17. Kiểm tra phát hiện Schema không hợp lệ bằng Draft7Validator.check_schema()
    def test_17_invalid_json_schema_fails_check_schema(self):
        from jsonschema import Draft7Validator

        # 17.1. Đảm bảo schema chính thức của dự án thỏa mãn Draft7Validator.check_schema
        schema_dict = load_json_schema(self.schema_file)
        Draft7Validator.check_schema(schema_dict)

        # 17.2. Schema vi phạm Draft 7 specification (ví dụ: type không hợp lệ)
        invalid_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "invalid_type_name_that_does_not_exist",
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
            json.dump(invalid_schema, tf)
            tf_path = Path(tf.name)

        try:
            with self.assertRaises(ValueError) as ctx:
                load_json_schema(tf_path)
            self.assertIn("Draft 7 specification violation", str(ctx.exception))
        finally:
            tf_path.unlink(missing_ok=True)

    # 18. Kiểm tra phát hiện file Schema bị lỗi cú pháp JSON
    def test_18_malformed_json_schema_file_raises(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
            tf.write("{\n  malformed_json: true,\n")
            tf_path = Path(tf.name)

        try:
            with self.assertRaises(ValueError) as ctx:
                load_json_schema(tf_path)
            self.assertIn("Failed to parse JSON Schema", str(ctx.exception))
        finally:
            tf_path.unlink(missing_ok=True)

    # 19. CLI Validator trả về mã lỗi != 0 khi đường dẫn Schema không tồn tại
    def test_19_cli_fails_on_missing_schema_file(self):
        exit_code = validator_main([
            str(self.fixture_file),
            "--schema",
            "non_existent_schema_path.json",
        ])
        self.assertNotEqual(exit_code, 0, "CLI must return non-zero exit code when schema is missing.")

    # 20. CLI Validator trả về mã lỗi != 0 khi file Schema không hợp lệ (Draft 7 check failed)
    def test_20_cli_fails_on_invalid_schema_file(self):
        invalid_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": 12345,  # Type must be a string or array of strings in Draft 7
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
            json.dump(invalid_schema, tf)
            tf_path = Path(tf.name)

        try:
            exit_code = validator_main([
                str(self.fixture_file),
                "--schema",
                str(tf_path),
            ])
            self.assertNotEqual(exit_code, 0, "CLI must return non-zero exit code when schema is invalid.")
        finally:
            tf_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
