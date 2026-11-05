import os
import json
from pathlib import Path
from pyspark.sql import SparkSession

SESSION_ID = "001226df-03c7-41e6-a9c4-7d0233348bd5"

root = Path(os.environ["SNAP"]) / "commits"

commits = sorted(
    p for p in root.iterdir()
    if p.is_dir()
    and (p / "metadata.json").is_file()
    and (p / "sessions" / "_SUCCESS").is_file()
)

assert len(commits) == 1, (
    "Expected exactly one published snapshot",
    commits
)

snapshot = commits[0]

with (snapshot / "metadata.json").open() as f:
    meta = json.load(f)

expected = {
    "total_messages": 2,
    "unique_offsets": 2,
    "duplicate_offsets": 0,
    "valid_messages": 2,
    "funnel_sessions": 1,
    "view_sessions": 1,
    "cart_sessions": 0,
    "purchase_sessions": 0,
}

for key, value in expected.items():
    assert meta.get(key) == value, (
        key, meta.get(key), value
    )

ledger_commits = sorted(
    p.name
    for p in (Path(os.environ["LEDGER"]) / "commits").iterdir()
    if p.is_dir() and (p / "_SUCCESS").is_file()
)

assert sorted(meta["source_commits"]) == ledger_commits

print("METADATA_CHECK=PASS")
print("SOURCE_COMMITS_CHECK=PASS")

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("B4_03_Verify_Run1")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

try:
    df = spark.read.parquet(
        (snapshot / "sessions").as_uri()
    )

    rows = df.collect()

    assert len(rows) == 1, len(rows)

    result = rows[0].asDict()

    print("SESSION =", result["user_session"])
    print("HAS_VIEW =", result["has_view"])
    print("HAS_CART =", result["has_cart"])
    print("HAS_PURCHASE =", result["has_purchase"])

    assert result["user_session"] == SESSION_ID

    assert (
        result["has_view"],
        result["has_cart"],
        result["has_purchase"]
    ) == (True, False, False)

    print("RUN1_FUNNEL_CHECK=PASS")
    print("RUN1_VERIFICATION=PASS")

finally:
    spark.stop()
