import sys
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ml.features import (
    rank_session_events,
    select_prediction_prefix,
)


class TestB601Edges(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("B6-01-Edge-Tests")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.columns = [
            "user_session",
            "event_time",
            "event_type",
            "event_type_rank",
            "event_sort_key",
        ]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_events(self, rows):
        return self.spark.createDataFrame(
            rows,
            self.columns
        )

    def prefix_values(self, rows, n):
        df = self.make_events(rows)

        ranked = rank_session_events(df)
        prefix = select_prediction_prefix(ranked, n)

        return [
            (r.event_type, r.event_sort_key)
            for r in (
                prefix.orderBy("event_idx")
                .select("event_type", "event_sort_key")
                .collect()
            )
        ]

    def test_07_sort_key_tiebreaker(self):
        t = datetime(2019, 10, 1, 12, 0, 0)

        rows = [
            ("SKEY", t, "view", 1, "b" * 64),
            ("SKEY", t, "view", 1, "a" * 64),
        ]

        actual = self.prefix_values(rows, 2)

        self.assertEqual(
            actual,
            [
                ("view", "a" * 64),
                ("view", "b" * 64),
            ]
        )

    def test_08_identical_events_stable(self):
        t1 = datetime(2019, 10, 1, 12, 0, 0)
        t2 = datetime(2019, 10, 1, 12, 0, 10)

        rows = [
            ("SDUP", t1, "view", 1, "a" * 64),
            ("SDUP", t2, "cart", 2, "b" * 64),
            ("SDUP", t2, "cart", 2, "b" * 64),
        ]

        first = self.prefix_values(rows, 2)
        second = self.prefix_values(
            list(reversed(rows)), 2
        )

        self.assertEqual(first, second)

    def test_09_future_event_does_not_change_prefix(self):
        t1 = datetime(2019, 10, 1, 12, 0, 0)
        t2 = datetime(2019, 10, 1, 12, 0, 10)
        t3 = datetime(2019, 10, 1, 12, 0, 20)

        base = [
            ("SFUT", t1, "view", 1, "a" * 64),
            ("SFUT", t2, "cart", 2, "b" * 64),
        ]

        extended = base + [
            ("SFUT", t3, "purchase", 4, "c" * 64)
        ]

        self.assertEqual(
            self.prefix_values(base, 2),
            self.prefix_values(extended, 2)
        )


if __name__ == "__main__":
    result = unittest.main(
        verbosity=2,
        exit=False
    ).result

    if result.wasSuccessful():
        print("B6_01_EDGE_TESTS=PASS")
    else:
        print("B6_01_EDGE_TESTS=FAIL")
        sys.exit(1)
