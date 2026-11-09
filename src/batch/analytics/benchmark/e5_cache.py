import os
import time
import hashlib
from statistics import median

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

INPUT = "hdfs://localhost:9000/data/curated/events"

RESULT_DIR = os.path.expanduser(
    "~/capstone/benchmark/results"
)
os.makedirs(RESULT_DIR, exist_ok=True)

CSV_OUTPUT = os.path.join(RESULT_DIR, "e5_results.csv")
SUMMARY_OUTPUT = os.path.join(RESULT_DIR, "e5_summary.txt")

spark = (
    SparkSession.builder
    .appName("A9_E5_Cache_Benchmark")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_data():
    return (
        spark.read.parquet(INPUT)
        .filter(
            (F.col("event_date") >= "2019-10-01") &
            (F.col("event_date") < "2019-11-01")
        )
        .select("event_type", "price")
    )


def make_query(df):
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


def benchmark(label, query):
    print(f"\n===== {label} =====")

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

    return times, med, digest


results = []

try:
    # E5-A: Không cache
    df_a = load_data()
    query_a = make_query(df_a)

    times_a, median_a, hash_a = benchmark(
        "E5-A: WITHOUT CACHE",
        query_a
    )

    results.append(
        ("E5-A", "Without cache", times_a, median_a, hash_a, 0)
    )

    # E5-B: Có cache
    df_b = load_data()

    start_cache = time.perf_counter()

    df_b.persist(StorageLevel.MEMORY_AND_DISK)
    df_b.count()  # Materialize cache

    cache_time = time.perf_counter() - start_cache

    print("\n===== CACHE PREPARATION =====")
    print(f"Cache preparation time: {cache_time:.3f}s")

    query_b = make_query(df_b)

    times_b, median_b, hash_b = benchmark(
        "E5-B: WITH MEMORY_AND_DISK CACHE",
        query_b
    )

    results.append(
        ("E5-B", "With MEMORY_AND_DISK", times_b,
         median_b, hash_b, cache_time)
    )

    df_b.unpersist(blocking=True)

finally:
    spark.stop()


# Kiểm tra kết quả và lưu file
all_match = len(set(item[4] for item in results)) == 1

with open(CSV_OUTPUT, "w") as f:
    f.write(
        "variant,run1,run2,run3,median_seconds,"
        "cache_preparation_seconds,result_hash\n"
    )

    for label, description, times, med, digest, prep in results:
        f.write(
            f"{label},"
            f"{times[0]:.6f},"
            f"{times[1]:.6f},"
            f"{times[2]:.6f},"
            f"{med:.6f},"
            f"{prep:.6f},"
            f"{digest}\n"
        )


with open(SUMMARY_OUTPUT, "w") as f:
    f.write("E5 - CACHE BENCHMARK\n")
    f.write("====================\n")
    f.write("Input: /data/curated/events\n")
    f.write("Date range: 2019-10-01 to 2019-11-01\n")
    f.write("Master: local[4]\n")
    f.write("Driver memory: 3g\n")
    f.write("Shuffle partitions: 24\n")
    f.write("Timezone: UTC\n")
    f.write("Warm-up: 1 run per configuration\n")
    f.write("Measured runs: 3 per configuration\n\n")

    for label, description, times, med, digest, prep in results:
        f.write(f"{label}: {description}\n")
        f.write(f"Run 1: {times[0]:.3f}s\n")
        f.write(f"Run 2: {times[1]:.3f}s\n")
        f.write(f"Run 3: {times[2]:.3f}s\n")
        f.write(f"Median: {med:.3f}s\n")

        if label == "E5-B":
            f.write(f"Cache preparation: {prep:.3f}s\n")

        f.write(f"Hash: {digest}\n\n")

    f.write(f"ALL RESULTS MATCH: {all_match}\n")

print("\n===== E5 COMPLETED =====")
print("ALL RESULTS MATCH:", all_match)
print("CSV:", CSV_OUTPUT)
print("SUMMARY:", SUMMARY_OUTPUT)
