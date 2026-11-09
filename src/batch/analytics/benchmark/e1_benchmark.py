from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_timestamp,
    count, avg
)
from pyspark.sql.types import StructType, StructField, StringType
import time
import csv
import hashlib
import statistics
import json
import os

spark = (
    SparkSession.builder
    .appName("A9_E1_CSV_vs_Parquet_Benchmark")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_path = "/data/raw/ecommerce_events/2019-Oct.csv"
parquet_path = "/data/curated/events"
output_path = os.path.expanduser(
    "~/capstone/benchmark/results/e1_results.csv"
)

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True)
])

# CSV: áp dụng cùng quy tắc chuẩn hóa và lọc như ETL
csv_df = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv(raw_path)
    .withColumn(
        "event_time",
        to_timestamp(
            regexp_replace(trim(col("event_time")), r"\s+UTC$", ""),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn("event_type", lower(trim(col("event_type"))))
    .withColumn("price", col("price").cast("double"))
)

valid_events = ["view", "cart", "remove_from_cart", "purchase"]

csv_df = (
    csv_df
    .filter(
        col("event_time").isNotNull()
        & col("event_type").isNotNull()
        & col("event_type").isin(valid_events)
        & col("price").isNotNull()
        & (col("price") >= 0)
        & col("user_id").isNotNull()
    )
    .withColumn("event_date", col("event_time").cast("date"))
)

# Parquet: chỉ lấy tháng 10/2019
parquet_df = (
    spark.read.parquet(parquet_path)
    .filter(
        (col("event_date") >= "2019-10-01")
        & (col("event_date") < "2019-11-01")
    )
)

def build_queries(df):
    return {
        "Q1_event_count_by_type": (
            df.groupBy("event_type")
              .agg(count("*").alias("event_count"))
              .orderBy("event_type")
        ),
        "Q2_count_avg_price_by_type": (
            df.groupBy("event_type")
              .agg(
                  count("*").alias("event_count"),
                  avg("price").alias("avg_price")
              )
              .orderBy("event_type")
        ),
        "Q3_daily_event_count": (
            df.groupBy("event_date")
              .agg(count("*").alias("event_count"))
              .orderBy("event_date")
        )
    }

def collect_result(query_df):
    rows = query_df.collect()
    normalized = []

    for row in rows:
        values = []
        for value in row:
            if isinstance(value, float):
                values.append(round(value, 6))
            elif value is None:
                values.append(None)
            else:
                values.append(str(value))
        normalized.append(values)

    payload = json.dumps(
        normalized, ensure_ascii=False, separators=(",", ":")
    )
    signature = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return normalized, signature

results = []

sources = [
    ("CSV", csv_df),
    ("Parquet", parquet_df)
]

all_signatures = {}

for source_name, df in sources:
    print(f"\n===== SOURCE: {source_name} =====", flush=True)

    queries = build_queries(df)

    for query_name, query_df in queries.items():
        print(f"\n--- {query_name}: warm-up ---", flush=True)

        # Warm-up: không đưa vào kết quả đo
        collect_result(query_df)

        durations = []
        final_signature = None
        output_rows = None

        for run_number in range(1, 4):
            start = time.perf_counter()
            normalized, signature = collect_result(query_df)
            elapsed = time.perf_counter() - start

            durations.append(elapsed)
            output_rows = len(normalized)
            final_signature = signature

            results.append({
                "source": source_name,
                "query": query_name,
                "run": run_number,
                "seconds": round(elapsed, 3),
                "output_rows": output_rows,
                "output_sha256": signature
            })

            print(
                f"{source_name} | {query_name} | "
                f"Run {run_number}: {elapsed:.3f}s",
                flush=True
            )

        all_signatures[(source_name, query_name)] = final_signature

        print(
            f"Median {source_name} | {query_name}: "
            f"{statistics.median(durations):.3f}s",
            flush=True
        )

# Kiểm tra kết quả CSV và Parquet có giống nhau
print("\n===== OUTPUT PARITY =====")
for query_name in [
    "Q1_event_count_by_type",
    "Q2_count_avg_price_by_type",
    "Q3_daily_event_count"
]:
    csv_sig = all_signatures[("CSV", query_name)]
    parquet_sig = all_signatures[("Parquet", query_name)]
    matched = csv_sig == parquet_sig

    print(f"{query_name}: MATCH = {matched}")

    if not matched:
        raise RuntimeError(
            f"Output mismatch for {query_name}"
        )

os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "source", "query", "run", "seconds",
            "output_rows", "output_sha256"
        ]
    )
    writer.writeheader()
    writer.writerows(results)

print(f"\nSaved benchmark results: {output_path}")
spark.stop()
