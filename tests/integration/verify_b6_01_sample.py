import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.features import (
    rank_session_events,
    select_prediction_prefix,
)


LEDGER = Path(
    "/home/student/b4_runtime/ledger_restart_v1/commits"
)

SESSION = "001226df-03c7-41e6-a9c4-7d0233348bd5"
TOPIC = "ecommerce_events_b4_restart_v1"
N = 2


def main():
    spark = (
        SparkSession.builder
        .appName("P3-B-B6-01-Integration")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Chi doc ledger commits da cong bo.
        commits = sorted(
            p for p in LEDGER.iterdir()
            if p.is_dir() and (p / "_SUCCESS").exists()
        )

        files = [
            f.resolve().as_uri()
            for commit in commits
            for f in sorted(commit.rglob("*.parquet"))
        ]

        assert len(commits) == 2
        assert len(files) == 4

        print("LEDGER_COMMITS =", len(commits))
        print("PARQUET_FILES =", len(files))

        # 2. Doc du lieu REES46 thuc te.
        df = spark.read.parquet(*files)

        required = {
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "user_session",
            "event_time",
            "event_type",
            "event_type_rank",
            "event_sort_key",
            "is_valid",
        }

        assert required.issubset(set(df.columns))

        total = df.count()
        assert total == 3, (
            "Unexpected total rows: {}".format(total)
        )

        valid = df.filter(
            F.col("is_valid") == F.lit(True)
        )

        assert valid.count() == 3

        assert valid.select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset"
        ).distinct().count() == 3

        print("SOURCE_TOTAL_ROWS =", total)
        print("VALID_ROWS = 3")
        print("SOURCE_CHECK=PASS")

        # 3. Xac minh thu tu Kafka.
        arrival_rows = (
            valid.orderBy("kafka_offset")
            .select(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "event_type"
            )
            .collect()
        )

        assert all(
            r["kafka_topic"] == TOPIC
            and r["kafka_partition"] == 0
            for r in arrival_rows
        )

        kafka_order = [
            (r["kafka_offset"], r["event_type"])
            for r in arrival_rows
        ]

        assert kafka_order == [
            (0, "view"),
            (1, "purchase"),
            (2, "cart"),
        ]

        print("KAFKA_ORDER =", kafka_order)
        print("KAFKA_ORDER_CHECK=PASS")

        # 4. Chay ranking B6-01.
        ranked = rank_session_events(valid)

        assert ranked.select(
            "user_session"
        ).distinct().count() == 1

        assert (
            ranked.select("user_session").first()[0]
            == SESSION
        )

        ranked_rows = (
            ranked.select(
                "event_idx",
                "event_type",
                F.date_format(
                    "event_time",
                    "yyyy-MM-dd HH:mm:ss"
                ).alias("utc")
            )
            .orderBy("event_idx")
            .collect()
        )

        actual_order = [
            (
                r["event_idx"],
                r["event_type"],
                r["utc"]
            )
            for r in ranked_rows
        ]

        expected_order = [
            (1, "view", "2019-10-01 14:12:34"),
            (2, "cart", "2019-10-01 14:12:48"),
            (3, "purchase", "2019-10-01 14:13:11"),
        ]

        assert actual_order == expected_order, (
            actual_order
        )

        print("BUSINESS_ORDER =", actual_order)
        print("RANKING_CHECK=PASS")

        # 5. Kiem tra prediction prefix N=2.
        prefix = select_prediction_prefix(ranked, N)

        prefix_rows = (
            prefix.orderBy("event_idx")
            .select("event_type")
            .collect()
        )

        prefix_types = [
            r["event_type"]
            for r in prefix_rows
        ]

        assert prefix.count() == 2
        assert prefix_types == ["view", "cart"]

        print("PREDICTION_N =", N)
        print("PREFIX_EVENTS =", prefix_types)
        print("PREFIX_CHECK=PASS")

        print("B6_01_INTEGRATION=PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
