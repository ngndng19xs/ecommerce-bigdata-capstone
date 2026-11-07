import sys
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

from ml.features import (
    rank_session_events,
    select_prediction_prefix
)


class TestB601(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("B6-01-Unit-Tests")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        schema = StructType([
            StructField("user_session", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_type_rank", IntegerType(), True),
            StructField("event_sort_key", StringType(), True)
        ])

        t1 = datetime(2019, 10, 1, 14, 12, 34)
        t2 = datetime(2019, 10, 1, 14, 12, 48)
        t3 = datetime(2019, 10, 1, 14, 13, 11)

        # Input co tinh dat Purchase truoc Cart.
        # Sort keys duoc tao gia chi de test ranking.
        rows = [
            ("S001", t3, "purchase", 4, "c" * 64),
            ("S001", t1, "view", 1, "a" * 64),
            ("S001", t2, "cart", 2, "b" * 64),

            ("S002", t1, "cart", 2, "b" * 64),
            ("S002", t1, "view", 1, "a" * 64),

            (None, t1, "view", 1, "a" * 64),
            (" ", t1, "view", 1, "a" * 64)
        ]

        cls.events = cls.spark.createDataFrame(rows, schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_01_event_order(self):
        ranked = rank_session_events(self.events)

        rows = (
            ranked.filter(F.col("user_session") == "S001")
            .orderBy("event_idx")
            .select("event_type")
            .collect()
        )

        actual = [r.event_type for r in rows]

        self.assertEqual(
            actual,
            ["view", "cart", "purchase"]
        )

    def test_02_prediction_prefix(self):
        ranked = rank_session_events(self.events)
        prefix = select_prediction_prefix(ranked, 2)

        rows = (
            prefix.filter(F.col("user_session") == "S001")
            .orderBy("event_idx")
            .select("event_type")
            .collect()
        )

        self.assertEqual(
            [r.event_type for r in rows],
            ["view", "cart"]
        )

    def test_03_same_timestamp(self):
        ranked = rank_session_events(self.events)

        rows = (
            ranked.filter(F.col("user_session") == "S002")
            .orderBy("event_idx")
            .select("event_type")
            .collect()
        )

        self.assertEqual(
            [r.event_type for r in rows],
            ["view", "cart"]
        )

    def test_04_null_session(self):
        ranked = rank_session_events(self.events)

        self.assertEqual(ranked.count(), 5)

    def test_05_invalid_prediction_n(self):
        ranked = rank_session_events(self.events)

        with self.assertRaises(ValueError):
            select_prediction_prefix(ranked, 0)

    def test_06_independent_session_index(self):
        ranked = rank_session_events(self.events)

        rows = (
            ranked.groupBy("user_session")
            .agg(F.min("event_idx").alias("first_idx"))
            .collect()
        )

        self.assertTrue(
            all(r.first_idx == 1 for r in rows)
        )


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        TestB601
    )

    result = unittest.TextTestRunner(
        verbosity=2
    ).run(suite)

    if result.wasSuccessful():
        print("B6_01_UNIT_TESTS=PASS")
    else:
        print("B6_01_UNIT_TESTS=FAIL")
        sys.exit(1)
