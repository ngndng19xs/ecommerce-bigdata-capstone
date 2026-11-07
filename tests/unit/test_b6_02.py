import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    IntegerType, DoubleType, BooleanType,
)
from ml.session_features import build_session_features


RANK = {
    "view": 1,
    "cart": 2,
    "remove_from_cart": 3,
    "purchase": 4,
}

BASE = datetime(2019, 10, 1, 12, 0, 0)


def event(
    session, step, kind,
    product="P1", category="C1",
    brand="A", price=10.0
):
    return (
        session,
        BASE + timedelta(seconds=10 * step),
        kind,
        RANK[kind],
        "{:064x}".format(step + 1),
        product,
        category,
        brand,
        float(price),
        True,
    )


class TestB602(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("B6-02-Unit-Tests")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

        schema = StructType([
            StructField("user_session", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_type_rank", IntegerType(), True),
            StructField("event_sort_key", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("is_valid", BooleanType(), True),
        ])

        rows = [
            # Positive: purchase sau N=2.
            event("S_POS", 2, "purchase", "P3", "C3", "B", 30),
            event("S_POS", 0, "view"),
            event("S_POS", 1, "cart", "P2", "C2", "", 20),

            # Negative: cung prefix, khac suffix.
            event("S_NEG", 0, "view"),
            event("S_NEG", 1, "cart", "P2", "C2", "", 20),
            event("S_NEG", 2, "view", "P3", "C3", "C", 70),

            # Purchase xay ra tai event N.
            event("S_EARLY", 0, "view"),
            event("S_EARLY", 1, "purchase"),
            event("S_EARLY", 2, "cart"),

            # Phien ngan hon N.
            event("S_SHORT", 0, "view"),

            # Phien dung N event, khong mua.
            event("S_EXACT", 0, "view"),
            event("S_EXACT", 1, "cart"),

            # Session khong hop le.
            event(None, 0, "view"),
            event("  ", 0, "cart"),
        ]

        cls.events = cls.spark.createDataFrame(
            rows, schema
        )

        cls.dataset, cls.audit, cls.invalid = (
            build_session_features(cls.events, 2)
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_01_eligible_count(self):
        self.assertEqual(self.dataset.count(), 3)
        self.assertEqual(
            self.dataset.select("user_session").distinct().count(),
            3
        )

    def test_02_labels(self):
        labels = {
            r["user_session"]: r["label"]
            for r in self.dataset.select(
                "user_session", "label"
            ).collect()
        }

        self.assertEqual(
            labels,
            {"S_POS": 1, "S_NEG": 0, "S_EXACT": 0}
        )

    def test_03_short_session(self):
        self.assertEqual(
            self.audit.filter("status = 'too_short'").count(),
            1
        )

    def test_04_purchase_by_n(self):
        rows = (
            self.audit
            .filter("status = 'purchased_by_n'")
            .select("user_session")
            .collect()
        )

        self.assertEqual(
            [r[0] for r in rows],
            ["S_EARLY"]
        )

    def test_05_invalid_session(self):
        self.assertEqual(self.invalid.count(), 2)

    def test_06_prefix_features(self):
        r = (
            self.dataset
            .filter("user_session = 'S_POS'")
            .first()
        )

        self.assertEqual(
            (r["n_view"], r["n_cart"],
             r["n_remove_from_cart"]),
            (1, 1, 0)
        )

        self.assertEqual(
            (r["n_unique_products"],
             r["n_unique_categories"],
             r["n_unique_brands"]),
            (2, 2, 2)
        )

        self.assertEqual(
            r["brands_seen"],
            ["A", "Unknown"]
        )

        self.assertAlmostEqual(r["avg_price"], 15)
        self.assertEqual(r["max_price"], 20)
        self.assertEqual(r["last_event_type"], "cart")
        self.assertEqual(
            r["prefix_duration_seconds"], 10
        )

    def test_07_no_future_leakage(self):
        positive = (
            self.dataset
            .filter("user_session = 'S_POS'")
            .first().asDict()
        )

        negative = (
            self.dataset
            .filter("user_session = 'S_NEG'")
            .first().asDict()
        )

        for key in positive:
            if key not in ("user_session", "label"):
                self.assertEqual(
                    positive[key], negative[key], key
                )

        self.assertNotEqual(
            positive["label"],
            negative["label"]
        )

    def test_08_audit_reconciles(self):
        self.assertEqual(self.audit.count(), 5)

        counts = {
            r["status"]: r["count"]
            for r in (
                self.audit.groupBy("status")
                .count().collect()
            )
        }

        self.assertEqual(
            counts,
            {
                "eligible": 3,
                "too_short": 1,
                "purchased_by_n": 1,
            }
        )

    def test_09_no_future_columns(self):
        forbidden = {
            "session_length",
            "purchased_by_n",
            "event_idx",
            "event_type",
            "kafka_offset",
            "rejection_reason",
        }

        self.assertFalse(
            forbidden.intersection(
                self.dataset.columns
            )
        )

    def test_10_invalid_n(self):
        with self.assertRaises(ValueError):
            build_session_features(self.events, 0)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        TestB602
    )

    result = unittest.TextTestRunner(
        verbosity=2
    ).run(suite)

    if result.wasSuccessful():
        print("B6_02_UNIT_TESTS=PASS")
    else:
        print("B6_02_UNIT_TESTS=FAIL")
        sys.exit(1)
