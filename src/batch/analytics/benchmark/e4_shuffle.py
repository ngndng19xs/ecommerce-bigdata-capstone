
import os
import time
import hashlib
from statistics import median

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "hdfs://localhost:9000/data/curated/events"

RESULT_DIR = os.path.expanduser(
    "~/capstone/benchmark/results"
)
os.makedirs(RESULT_DIR, exist_ok=True)

CSV_OUTPUT = os.path.join(RESULT_DIR, "e4_results.csv")
SUMMARY_OUTPUT = os.path.join(RESULT_DIR, "e4_summary.txt")


def make_spark(partitions):
    return (
        SparkSession.builder
        .appName(f"A9_E4_Shuffle_{partitions}")
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .getOrCreate()
    )


def make_query(spark):
    df = spark.read.parquet(INPUT)

    df = df.filter(
        (F.col("event_date") >= "2019-10-01") &
        (F.col("event_date") < "2019-11-01")
    )

    return (
        df.groupBy("event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("price"), 4).alias("avg_price")
        )
        .orderBy("event_type")
    )


def hash_result(rows):
    content = "|".join(
        f"{r['event_type']}:{r['event_count']}:{r['avg_price']}"
        for r in rows
    )
    return hashlib.md5(content.encode()).hexdigest()


results = []

for partitions in [12, 24, 48]:
    spark = make_spark(partitions)

    try:
        print(f"\n===== E4: SHUFFLE PARTITIONS = {partitions} =====")
        print(
            "Configured partitions:",
            spark.conf.get("spark.sql.shuffle.partitions")
        )

        query = make_query(spark)

        # EXPLAIN được lưu trong log đầy đủ
        query.explain("formatted")

        # Warm-up
        query.collect()
        print("Warm-up: completed")

        times = []
        final_rows = None

        for i in range(1, 4):
            start = time.perf_counter()
            rows = query.collect()
            elapsed = time.perf_counter() - start

            times.append(elapsed)
            final_rows = rows

            print(f"Run {i}: {elapsed:.3f}s")

        med = median(times)
        digest = hash_result(final_rows)

        print(f"Median: {med:.3f}s")
        print("Hash:", digest)

        results.append((partitions, times, med, digest))

    finally:
        spark.stop()


# Tổng hợp kết quả
hashes = [item[3] for item in results]
all_match = len(set(hashes)) == 1

with open(CSV_OUTPUT, "w") as f:
    f.write(
        "shuffle_partitions,run1,run2,run3,median_seconds,result_hash\n"
    )

    for partitions, times, med, digest in results:
        f.write(
            f"{partitions},"
            f"{times[0]:.6f},"
            f"{times[1]:.6f},"
            f"{times[2]:.6f},"
            f"{med:.6f},"
            f"{digest}\n"
        )


with open(SUMMARY_OUTPUT, "w") as f:
    f.write("E4 - SHUFFLE PARTITIONS BENCHMARK\n")
    f.write("=================================\n")
    f.write("Input: /data/curated/events\n")
    f.write("Date range: 2019-10-01 to 2019-11-01\n")
    f.write("Master: local[4]\n")
    f.write("Driver memory: 3g\n")
    f.write("Timezone: UTC\n")
    f.write("Warm-up: 1 run per configuration\n")
    f.write("Measured runs: 3 per configuration\n\n")

    for partitions, times, med, digest in results:
        f.write(f"Shuffle partitions: {partitions}\n")
        f.write(f"Run 1: {times[0]:.3f}s\n")
        f.write(f"Run 2: {times[1]:.3f}s\n")
        f.write(f"Run 3: {times[2]:.3f}s\n")
        f.write(f"Median: {med:.3f}s\n")
        f.write(f"Hash: {digest}\n\n")

    f.write(f"ALL RESULTS MATCH: {all_match}\n")

print("\n===== E4 COMPLETED =====")
print("ALL RESULTS MATCH:", all_match)
print("CSV:", CSV_OUTPUT)
print("SUMMARY:", SUMMARY_OUTPUT)

