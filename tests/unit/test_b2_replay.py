
"""Kiểm thử B2-01 ở chế độ ngoại tuyến (offline), không yêu cầu Kafka."""

import csv
import tempfile
import unittest
from pathlib import Path

from src.streaming.producer.replay import (
    COLUMNS,
    draft_event,
    arguments,
    replay,
)


class ReplayTest(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

        self.csv_path = (
            Path(self.temp.name) / "part-00000.csv"
        )

        self.rows = [
            [
                "2019-10-01 00:00:00 UTC",
                "view", "1", "10", "", "",
                "20.50", "99", "session-1"
            ],
            [
                "2019-10-01 00:00:01 UTC",
                "cart", "1", "10", "", "",
                "20.50", "99", "session-1"
            ],
            [
                "2019-10-02 00:00:00 UTC",
                "purchase", "2", "11", "", "",
                "30.00", "99", ""
            ],
        ]

        self.write_rows(self.rows)

    def write_rows(self, rows, header=COLUMNS):
        with self.csv_path.open(
            "w", encoding="utf-8", newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)

    def params(self, *extra):
        return arguments().parse_args(
            [
                "--input", self.temp.name,
                "--rate", "100000",
                "--preview", "0",
            ] + list(extra)
        )

    def test_utc_and_draft_marker(self):
        day, obj = draft_event(
            dict(zip(COLUMNS, self.rows[0]))
        )

        self.assertEqual(str(day), "2019-10-01")
        self.assertEqual(
            obj["event_time"],
            "2019-10-01T00:00:00Z"
        )
        self.assertEqual(
            obj["schema_version"],
            "draft-b2-01-not-approved-s2"
        )
        self.assertEqual(obj["price"], "20.50")
        self.assertEqual(obj["user_session"], "session-1")

    def test_null_session_is_preserved(self):
        _, obj = draft_event(
            dict(zip(COLUMNS, self.rows[2]))
        )
        self.assertIsNone(obj["user_session"])

    def test_limit(self):
        result = replay(
            self.params(
                "--limit", "2",
                "--chunk-size", "1"
            )
        )

        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["selected"], 2)
        self.assertEqual(result["sent_ack"], 0)

    def test_inclusive_date_bounds(self):
        result = replay(
            self.params(
                "--start-date", "2019-10-02",
                "--end-date", "2019-10-02"
            )
        )

        self.assertEqual(result["selected"], 1)
        self.assertEqual(result["filtered_date"], 2)

    def test_malformed_row_is_counted(self):
        invalid_row = [
            "BAD", "view", "1", "10", "",
            "", "1", "9", "x"
        ]

        self.write_rows(
            [self.rows[0], invalid_row]
        )

        result = replay(
            self.params("--limit", "0")
        )

        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["malformed"], 1)
        self.assertEqual(result["selected"], 1)

    def test_wrong_header_rejected(self):
        self.write_rows(
            self.rows,
            header=["incorrect"]
        )

        with self.assertRaisesRegex(
            ValueError, "header"
        ):
            replay(self.params())

    def test_test_topic_safety(self):
        with self.assertRaisesRegex(
            ValueError, "b2_test_"
        ):
            replay(
                self.params(
                    "--send-test",
                    "--topic",
                    "ecommerce.clickstream.raw"
                )
            )

    def test_unlimited_send_blocked(self):
        with self.assertRaisesRegex(
            ValueError, "--limit"
        ):
            replay(
                self.params(
                    "--send-test",
                    "--limit", "0"
                )
            )

    def test_bad_date_range_blocked(self):
        with self.assertRaisesRegex(
            ValueError, "--start-date"
        ):
            replay(
                self.params(
                    "--start-date", "2019-10-03",
                    "--end-date", "2019-10-01"
                )
            )


if __name__ == "__main__":
    unittest.main()
