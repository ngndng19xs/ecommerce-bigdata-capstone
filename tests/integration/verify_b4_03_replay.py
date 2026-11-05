import os
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SESSION = "001226df-03c7-41e6-a9c4-7d0233348bd5"

snap_root = Path(os.environ["SNAP"]) / "commits"
ledger_root = Path(os.environ["LEDGER"]) / "commits"

snapshots = sorted(
    p for p in snap_root.iterdir()
    if p.is_dir()
    and (p / "metadata.json").is_file()
    and (p / "sessions" / "_SUCCESS").is_file()
)

assert len(snapshots) == 4, (
    "Expected Run1, Run3 and two replay snapshots",
    len(snapshots)
)

ledger_commits = sorted(
    p.name for p in ledger_root.iterdir()
    if p.is_dir() and (p / "_SUCCESS").is_file()
)

assert len(ledger_commits) == 2

expected_meta = {
    "total_messages": 3,
    "unique_offsets": 3,
    "duplicate_offsets": 0,
    "valid_messages": 3,
    "funnel_sessions": 1,
    "view_sessions": 1,
    "cart_sessions": 1,
    "purchase_sessions": 1,
}

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("B4_03_Verify_Replay")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    results = []

    # Snapshot Run3 va hai snapshot Run4.
    for snapshot in snapshots[-3:]:
        with (snapshot / "metadata.json").open() as f:
            meta = json.load(f)

        for key, expected in expected_meta.items():
            assert meta.get(key) == expected, (
                snapshot.name, key, meta.get(key)
            )

        assert sorted(meta["source_commits"]) == ledger_commits

        df = spark.read.parquet(
            (snapshot / "sessions").as_uri()
        )

        rows = (
            df.select(
                "user_session",
                "has_view",
                "has_cart",
                "has_purchase",
                F.col("view_order.event_time")
                 .cast("string").alias("view_utc"),
                F.col("cart_order.event_time")
                 .cast("string").alias("cart_utc"),
                F.col("purchase_order.event_time")
                 .cast("string").alias("purchase_utc"),
                F.col("funnel_date")
                 .cast("string").alias("funnel_date")
            ).collect()
        )

        assert len(rows) == 1
        row = rows[0].asDict()

        assert row["user_session"] == SESSION
        assert (
            row["has_view"],
            row["has_cart"],
            row["has_purchase"]
        ) == (True, True, True)

        assert (
            row["view_utc"],
            row["cart_utc"],
            row["purchase_utc"]
        ) == (
            "2019-10-01 14:12:34",
            "2019-10-01 14:12:48",
            "2019-10-01 14:13:11"
        )

        assert row["funnel_date"] == "2019-10-01"

        results.append(row)
        print("SNAPSHOT_PASS =", snapshot.name)

    assert results[0] == results[1] == results[2]

    print("RUN4_METADATA_CHECK=PASS")
    print("RUN4_FUNNEL_UNCHANGED=PASS")
    print("RUN4_VERIFICATION=PASS")

finally:
    spark.stop()
