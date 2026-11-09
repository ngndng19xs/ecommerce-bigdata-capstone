import os
import time
import hashlib
from statistics import median

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "hdfs://localhost:9000/data/curated/events"
OUTPUT = os.path.expanduser(
    "~/capstone/benchmark/results/e3_results.csv"
)

spark = (
    SparkSession.builder
    .appName("A9_E3_Projection_Pushdown")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def build_query(project_first):
    df = spark.read.parquet(INPUT)

    if project_first:
        df = df.select("event_type", "price", "event_date")

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

def result_hash(rows):
    content = "|".join(
        f"{r['event_type']}:{r['event_count']}:{r['avg_price']}"
        for r in rows
    )
    return hashlib.md5(content.encode()).hexdigest()

def benchmark(label, query):
    print(f"\n===== {label} =====")
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
    digest = result_hash(final_rows)

    print("Median: {:.3f}s".format(med))
    print("Hash:", digest)
    print("Result:", [r.asDict() for r in final_rows])

    return med, digest

try:
    query_a = build_query(project_first=False)
    query_b = build_query(project_first=True)

    time_a, hash_a = benchmark("E3-A: FULL COLUMNS", query_a)
    time_b, hash_b = benchmark("E3-B: SELECT REQUIRED COLUMNS", query_b)

    print("\n===== E3 COMPARISON =====")
    print("A median:", time_a)
    print("B median:", time_b)
    print("Result match:", hash_a == hash_b)
    print("A hash:", hash_a)
    print("B hash:", hash_b)

    with open(OUTPUT, "w") as f:
        f.write("variant,median_seconds,result_hash\n")
        f.write(f"E3-A,{time_a:.6f},{hash_a}\n")
        f.write(f"E3-B,{time_b:.6f},{hash_b}\n")

    print("\nSaved:", OUTPUT)

finally:
    spark.stop()
