from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time
import csv
import hashlib
import json
import os
import statistics

spark = (
    SparkSession.builder
    .appName("A9_E2_Partition_Pruning")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

source = "/data/curated/events"
output_path = os.path.expanduser(
    "~/capstone/benchmark/results/e2_results.csv"
)

# Đọc cùng một nguồn Parquet
base_df = spark.read.parquet(source)

# E2-A: Chỉ lọc theo event_time
query_a = (
    base_df
    .filter(
        (col("event_time") >= "2019-10-01 00:00:00")
        & (col("event_time") < "2019-11-01 00:00:00")
    )
    .groupBy("event_type")
    .agg(count("*").alias("event_count"))
    .orderBy("event_type")
)

# E2-B: Giữ nguyên event_time, bổ sung event_date
query_b = (
    base_df
    .filter(
        (col("event_time") >= "2019-10-01 00:00:00")
        & (col("event_time") < "2019-11-01 00:00:00")
        & (col("event_date") >= "2019-10-01")
        & (col("event_date") < "2019-11-01")
    )
    .groupBy("event_type")
    .agg(count("*").alias("event_count"))
    .orderBy("event_type")
)

def get_result(df):
    rows = df.collect()
    values = [(r["event_type"], r["event_count"]) for r in rows]
    payload = json.dumps(values, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return values, digest

results = []

for name, query in [
    ("E2_A_event_time_only", query_a),
    ("E2_B_event_time_and_event_date", query_b)
]:
    print(f"\n===== {name} =====", flush=True)

    print(f"\n===== {name} EXPLAIN FORMATTED =====", flush=True)
    query.explain("formatted")

    print(f"\n===== {name} WARMUP =====", flush=True)
    warm_result, warm_hash = get_result(query)
    print(f"WARMUP rows={len(warm_result)} result={warm_result}", flush=True)

    durations = []
    final_hash = None

    for run in range(1, 4):
        start = time.perf_counter()
        values, digest = get_result(query)
        elapsed = time.perf_counter() - start

        durations.append(elapsed)
        final_hash = digest

        results.append({
            "variant": name,
            "run": run,
            "seconds": round(elapsed, 3),
            "output_rows": len(values),
            "total_events": sum(x[1] for x in values),
            "output_sha256": digest
        })

        print(
            f"{name} RUN_{run}: {elapsed:.3f}s | "
            f"rows={len(values)} | total={sum(x[1] for x in values)}",
            flush=True
        )

    print(
        f"{name} MEDIAN: {statistics.median(durations):.3f}s",
        flush=True
    )

# Kiểm tra kết quả hai phiên bản
result_a, hash_a = get_result(query_a)
result_b, hash_b = get_result(query_b)

print("\n===== E2 OUTPUT PARITY =====")
print("E2-A:", result_a)
print("E2-B:", result_b)
print("MATCH:", result_a == result_b)
print("HASH MATCH:", hash_a == hash_b)

if result_a != result_b:
    raise RuntimeError("E2 output mismatch!")

os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "variant", "run", "seconds",
            "output_rows", "total_events", "output_sha256"
        ]
    )
    writer.writeheader()
    writer.writerows(results)

print(f"\nSaved: {output_path}", flush=True)
spark.stop()
