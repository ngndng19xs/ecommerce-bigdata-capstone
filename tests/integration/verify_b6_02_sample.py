import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.session_features import build_session_features


LEDGER = Path(
    "/home/student/b4_runtime/ledger_restart_v1/commits"
)

SESSION = "001226df-03c7-41e6-a9c4-7d0233348bd5"


def main():
    spark = (
        SparkSession.builder
        .appName("B6-02-REES46")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        commits = sorted(
            p for p in LEDGER.iterdir()
            if p.is_dir()
            and (p / "_SUCCESS").exists()
        )

        files = [
            f.resolve().as_uri()
            for p in commits
            for f in sorted(p.rglob("*.parquet"))
        ]

        assert len(commits) == 2
        assert len(files) == 4

        source = spark.read.parquet(*files)

        assert source.count() == 3

        valid = source.filter(
            F.col("is_valid") == True
        )

        assert valid.count() == 3

        dataset, audit, invalid_rows = (
            build_session_features(source, 2)
        )

        assert invalid_rows.count() == 0
        assert audit.count() == 1

        a = audit.first()

        assert a["status"] == "eligible"
        assert a["label"] == 1
        assert a["session_length"] == 3

        assert dataset.count() == 1

        row = dataset.first()

        assert row["user_session"] == SESSION
        assert row["label"] == 1

        assert (
            row["n_view"],
            row["n_cart"],
            row["n_remove_from_cart"]
        ) == (1, 1, 0)

        assert row["last_event_type"] == "cart"
        assert row["prefix_duration_seconds"] == 14

        pred = dataset.select(
            F.date_format(
                "prediction_time",
                "yyyy-MM-dd HH:mm:ss"
            ).alias("utc")
        ).first()["utc"]

        assert pred == "2019-10-01 14:12:48"

        # Khong dua cac truong audit vao ML dataset.
        assert "session_length" not in dataset.columns
        assert "purchased_by_n" not in dataset.columns

        print("LEDGER_COMMITS=2 PARQUET_FILES=4")
        print("SOURCE_ROWS=3 ELIGIBLE_SESSIONS=1")
        print("PREDICTION_N=2 LABEL=1")
        print("PREFIX_COUNTS=view:1 cart:1 remove:0")
        print("PREFIX_DURATION_SECONDS=14")
        print("B6_02_INTEGRATION=PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
