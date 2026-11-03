"""
Nhiệm vụ Task S2 / P2-B-S2-01: Bộ kiểm thử đơn vị cho Hợp đồng Kafka JSON v1.0.

Kiểm tra tính hợp lệ của các ràng buộc schema, định dạng timestamp UTC, thứ tự ưu tiên sự kiện,
quy tắc khóa phân vùng (Kafka message key), 3 trường ID kiểu String, sự tuân thủ JSON Schema Draft 7,
tính toàn vẹn và sự tồn tại của đường dẫn schema production v1.0.
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
    KAFKA_SCHEMA_VERSION,
    EXPECTED_VALUE_FIELDS,
    load_json_schema,
    main as validator_main,
    validate_event_value,
    validate_kafka_record,
    validate_jsonl_file,
)


class KafkaContractV1Test(unittest.TestCase):
    """Bộ kiểm thử đơn vị thẩm định Hợp đồng Kafka JSON v1.0."""

    def setUp(self):
        """Chuẩn bị bản ghi mẫu hợp lệ theo hợp đồng v1.0 làm cơ sở cho các ca kiểm thử biến dị (mutation)."""
        self.fixtures_dir = REPO_ROOT / "tests" / "fixtures"
        self.fixture_file = self.fixtures_dir / "kafka_records_v1.jsonl"
        self.schema_file = (
            REPO_ROOT / "src" / "streaming" / "schemas" / "kafka_event_schema_v1.json"
        )

        self.valid_value = {
            "event_time": "2019-10-01T00:00:00Z",
            "event_type": "view",
            "product_id": "1004856",
            "category_id": "2053013555631882655",
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": "520088429",
            "user_session": "d6c3d980-d281-4203-bc85-999ac65daec9",
            "event_type_rank": 1,
            "event_sort_key": "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92a",
            "schema_version": "kafka-v1.0",
        }

        self.valid_record = {
            "key": "d6c3d980-d281-4203-bc85-999ac65daec9",
            "value": dict(self.valid_value),
        }

    # 1. JSON hợp lệ & baseline
    def test_01_valid_record_passes(self):
        errors = validate_kafka_record(self.valid_record)
        self.assertEqual(errors, [], f"Expected 0 errors on valid record, got: {errors}")

    # 2. Schema đủ 12 trường bắt buộc
    def test_02_missing_required_fields_fails(self):
        for field in EXPECTED_VALUE_FIELDS:
            with self.subTest(missing_field=field):
                mutated = dict(self.valid_value)
                del mutated[field]
                record = {"key": mutated.get("user_session"), "value": mutated}
                errors = validate_kafka_record(record)
                self.assertTrue(
                    any("Missing required fields" in err and field in err for err in errors),
                    f"Expected missing field error for '{field}', got: {errors}",
                )

    # 3. Timestamp đúng UTC ISO-8601
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

    # 5. Cả 4 event_type hợp lệ với đúng rank tương ứng
    def test_05_valid_event_types_and_ranks_pass(self):
        valid_pairs = [
            ("view", 1),
            ("cart", 2),
            ("remove_from_cart", 3),
            ("purchase", 4),
        ]
        for etype, rank in valid_pairs:
            with self.subTest(event_type=etype, rank=rank):
                mutated = dict(self.valid_value, event_type=etype, event_type_rank=rank)
                errors = validate_event_value(mutated)
                self.assertEqual(errors, [])

    # 6. Từ chối event_type không hợp lệ
    def test_06_invalid_event_type_rejected(self):
        invalid_types = ["click", "hover", "checkout", "VIEW", "Cart", "", None]
        for itype in invalid_types:
            with self.subTest(invalid_type=itype):
                mutated = dict(self.valid_value, event_type=itype)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("event_type" in err for err in errors),
                    f"Expected event_type rejection for '{itype}', got {errors}",
                )

    # 7. Kiểm tra chéo (Cross-field Validation): Từ chối khi event_type và event_type_rank không khớp
    def test_07_cross_field_rank_mismatch_rejected(self):
        mismatches = [
            ("view", 2),
            ("cart", 1),
            ("cart", 3),
            ("remove_from_cart", 4),
            ("purchase", 1),
        ]
        for etype, wrong_rank in mismatches:
            with self.subTest(event_type=etype, wrong_rank=wrong_rank):
                mutated = dict(self.valid_value, event_type=etype, event_type_rank=wrong_rank)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("event_type_rank mismatch" in err or "JSONSchema validation" in err for err in errors),
                    f"Expected rank mismatch rejection for {etype} with rank {wrong_rank}, got {errors}",
                )

    # 8. Ba ID dạng String hợp lệ (product_id, category_id, user_id)
    def test_08_string_ids_accepted(self):
        mutated = dict(
            self.valid_value,
            product_id="44600062",
            category_id="2103807459595387724",
            user_id="541312140",
        )
        errors = validate_event_value(mutated)
        self.assertEqual(errors, [])

    # 9. Từ chối khi ba ID là số nguyên (Integer IDs must be rejected in contract v1.0)
    def test_09_integer_ids_rejected(self):
        for field in ("product_id", "category_id", "user_id"):
            with self.subTest(field=field):
                mutated = dict(self.valid_value, **{field: 12345678})
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("must be a string" in err for err in errors),
                    f"Expected string ID rejection for {field} as int, got {errors}",
                )

    # 10. Từ chối khi ba ID là rỗng hoặc NULL
    def test_10_empty_or_null_ids_rejected(self):
        for field in ("product_id", "category_id", "user_id"):
            for bad_val in ("", "   ", None):
                with self.subTest(field=field, val=bad_val):
                    mutated = dict(self.valid_value, **{field: bad_val})
                    errors = validate_event_value(mutated)
                    self.assertTrue(
                        len(errors) > 0,
                        f"Expected rejection for {field} with value {repr(bad_val)}",
                    )

    # 11. Xử lý giá tiền (price >= 0.0, chấp nhận 0.0, từ chối giá âm hoặc NULL)
    def test_11_price_validation(self):
        # price = 0.0 hợp lệ (quà tặng / khuyến mãi)
        mutated = dict(self.valid_value, price=0.0)
        self.assertEqual(validate_event_value(mutated), [])

        # price là số nguyên >= 0 hợp lệ
        mutated = dict(self.valid_value, price=50)
        self.assertEqual(validate_event_value(mutated), [])

        # price âm bị từ chối
        mutated = dict(self.valid_value, price=-1.0)
        errors = validate_event_value(mutated)
        self.assertTrue(any("price" in err for err in errors))

        # price là null bị từ chối
        mutated = dict(self.valid_value, price=None)
        errors = validate_event_value(mutated)
        self.assertTrue(any("price" in err for err in errors))

    # 12. Xử lý brand (chấp nhận "Unknown", từ chối rỗng hoặc NULL)
    def test_12_brand_validation(self):
        # brand = "Unknown" hợp lệ
        mutated = dict(self.valid_value, brand="Unknown")
        self.assertEqual(validate_event_value(mutated), [])

        # brand rỗng bị từ chối
        mutated = dict(self.valid_value, brand="")
        errors = validate_event_value(mutated)
        self.assertTrue(any("brand" in err for err in errors))

        # brand null bị từ chối
        mutated = dict(self.valid_value, brand=None)
        errors = validate_event_value(mutated)
        self.assertTrue(any("brand" in err for err in errors))

    # 13. Khóa Kafka (Key) và phiên truy cập (user_session)
    def test_13_kafka_key_and_session_semantics(self):
        # Case A: session có giá trị -> key phải bằng session
        record_valid = {
            "key": "d6c3d980-d281-4203-bc85-999ac65daec9",
            "value": dict(self.valid_value),
        }
        self.assertEqual(validate_kafka_record(record_valid), [])

        # Case B: session khác key -> từ chối
        record_mismatch = {
            "key": "different-key-uuid",
            "value": dict(self.valid_value),
        }
        errors = validate_kafka_record(record_mismatch)
        self.assertTrue(any("Kafka message key must equal user_session" in err for err in errors))

        # Case C: session là NULL -> key phải là None (null)
        val_null_session = dict(self.valid_value, user_session=None)
        record_null_key = {
            "key": None,
            "value": val_null_session,
        }
        self.assertEqual(validate_kafka_record(record_null_key), [])

        # Case D: session là NULL nhưng key là chuỗi "null" hoặc "None" -> từ chối kịch liệt
        for fake_null in ("null", "None", ""):
            with self.subTest(fake_null=fake_null):
                record_fake = {
                    "key": fake_null,
                    "value": val_null_session,
                }
                errors = validate_kafka_record(record_fake)
                self.assertTrue(any("When user_session is NULL" in err for err in errors))

    # 14. category_code cho phép NULL
    def test_14_category_code_nullable(self):
        mutated = dict(self.valid_value, category_code=None)
        self.assertEqual(validate_event_value(mutated), [])

    # 15. event_sort_key đúng định dạng 64 ký tự hex lowercase
    def test_15_event_sort_key_validation(self):
        # 64 hex hợp lệ
        valid_keys = [
            "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92a",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ]
        for k in valid_keys:
            with self.subTest(key=k):
                mutated = dict(self.valid_value, event_sort_key=k)
                self.assertEqual(validate_event_value(mutated), [])

        # Sai định dạng: 63 ký tự, 65 ký tự, chữ hoa, ký tự ngoài hex
        invalid_keys = [
            "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92",   # 63 chars
            "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92aa", # 65 chars
            "1E2CFBFFBD40220C027A1D6AE279D4586CE479677B6916C7BF1E431BB6DCA92A", # UPPERCASE
            "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92z", # 'z' non-hex
            "",
            None,
        ]
        for k in invalid_keys:
            with self.subTest(invalid_key=k):
                mutated = dict(self.valid_value, event_sort_key=k)
                errors = validate_event_value(mutated)
                self.assertTrue(len(errors) > 0, f"Expected rejection for key {k}")

    # 16. schema_version phải bằng "kafka-v1.0"
    def test_16_schema_version_v1_validated(self):
        self.assertEqual(self.valid_value["schema_version"], KAFKA_SCHEMA_VERSION)

        # Các version khác (kể cả draft cũ) đều bị từ chối
        invalid_versions = ["DRAFT-S2-01", "v1.0", "kafka-v2.0", "", None]
        for iv in invalid_versions:
            with self.subTest(version=iv):
                mutated = dict(self.valid_value, schema_version=iv)
                errors = validate_event_value(mutated)
                self.assertTrue(
                    any("schema_version" in err for err in errors),
                    f"Expected schema_version mismatch for {iv}, got {errors}",
                )

    # 17. Từ chối trường ngoài danh mục (additionalProperties: false)
    def test_17_additional_properties_rejected(self):
        mutated = dict(self.valid_value, reject_reason="should_not_exist_in_kafka")
        errors = validate_event_value(mutated)
        self.assertTrue(
            any("Unexpected extra fields" in err or "additionalProperties" in err for err in errors),
            f"Expected additional properties rejection, got {errors}",
        )

        mutated_date = dict(self.valid_value, event_date="2019-10-01")
        errors_date = validate_event_value(mutated_date)
        self.assertTrue(
            any("Unexpected extra fields" in err or "additionalProperties" in err for err in errors_date),
            f"Expected rejection for event_date in Kafka payload, got {errors_date}",
        )

    # 18. Thẩm định file fixtures chuẩn kafka_records_v1.jsonl
    def test_18_fixture_file_all_records_pass(self):
        self.assertTrue(self.fixture_file.exists(), f"Fixture file not found: {self.fixture_file}")
        total, valid, failures = validate_jsonl_file(self.fixture_file, schema_path=self.schema_file)
        self.assertGreater(total, 0, "Fixture file has 0 records.")
        self.assertEqual(
            failures, [],
            f"Fixture records failed validation: {failures}",
        )
        self.assertEqual(total, valid)

    # 19. Kiểm tra phát hiện Schema không hợp lệ hoặc lỗi cú pháp
    def test_19_schema_syntax_validation(self):
        from jsonschema import Draft7Validator

        # 19.1. Đảm bảo schema chính thức của dự án thỏa mãn Draft7Validator.check_schema
        schema_dict = load_json_schema(self.schema_file)
        Draft7Validator.check_schema(schema_dict)

        # 19.2. Bắt lỗi khi schema bị lỗi định dạng JSON
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
            tf.write("{\n  malformed_json: true,\n")
            tf_path = Path(tf.name)

        try:
            with self.assertRaises(ValueError) as ctx:
                load_json_schema(tf_path)
            self.assertIn("Failed to parse JSON Schema", str(ctx.exception))
        finally:
            tf_path.unlink(missing_ok=True)

    # 20. CLI Validator hoạt động chính xác với exit code
    def test_20_cli_validator_exit_codes(self):
        # 20.1. File fixture hợp lệ -> return 0
        exit_code_ok = validator_main([str(self.fixture_file)])
        self.assertEqual(exit_code_ok, 0, "CLI must return 0 on valid fixture.")

        # 20.2. Schema không tồn tại -> return 1
        exit_code_err = validator_main([
            str(self.fixture_file),
            "--schema",
            "non_existent_schema_path.json",
        ])
        self.assertNotEqual(exit_code_err, 0, "CLI must return non-zero when schema missing.")


if __name__ == "__main__":
    unittest.main()
