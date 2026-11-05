import os
import json
from pathlib import Path
from pyspark.sql import SparkSession

SESSION = "001226df-03c7-41e6-a9c4-7d0233348bd5"

root = Path(os.environ["SNAP"]) / "commits"

commits = sorted(
    p for p in root.iterdir()
    if p.is_dir()
    and (p / "metadata.json").is_file()
    and (p / "sessions" / "_SUCCESS").is_file()
)

assert len(commits) == 2, (
    "Expected Run 1 and Run 3 snapshots",
    len(commits)
)

latest = commits[-1]

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("B4_03_Verify_Run3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    df = spark.read.parquet(
        (latest / "sessions").as_uri()
    )

    print("=== SNAPSHOT SCHEMA ===")
    df.printSchema()

    print("=== FUNNEL DATA ===")
    df.show(truncate=False)

    rows = df.collect()

    assert len(rows) == 1

    row = rows[0].asDict(recursive=True)

    assert row["user_session"] == SESSION

    assert (
        row["has_view"],
        row["has_cart"],
        row["has_purchase"]
    ) == (True, True, True)

    print("RUN3_FUNNEL_FLAGS_CHECK=PASS")
    print("RUN3_SESSION_CHECK=PASS")

finally:
    spark.stop()
