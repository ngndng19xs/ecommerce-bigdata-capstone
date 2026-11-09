from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_timestamp
)
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("A9_E1_Parity_Check")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_path = "/data/raw/ecommerce_events/2019-Oct.csv"
parquet_path = "/data/curated/events"

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

# CSV: áp dụng quy tắc chuẩn hóa và lọc valid theo ETL
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

csv_valid = (
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
parquet_valid = (
    spark.read.parquet(parquet_path)
    .filter(
        (col("event_date") >= "2019-10-01")
        & (col("event_date") < "2019-11-01")
    )
)

# Truy vấn đối chiếu: số sự kiện theo event_type
csv_result = (
    csv_valid.groupBy("event_type")
    .count()
    .orderBy("event_type")
    .collect()
)

parquet_result = (
    parquet_valid.groupBy("event_type")
    .count()
    .orderBy("event_type")
    .collect()
)

csv_values = [(r["event_type"], r["count"]) for r in csv_result]
parquet_values = [(r["event_type"], r["count"]) for r in parquet_result]

print("===== CSV RESULT =====")
for row in csv_values:
    print(row)

print("===== PARQUET RESULT =====")
for row in parquet_values:
    print(row)

print("===== PARITY CHECK =====")
print("CSV total:", sum(x[1] for x in csv_values))
print("Parquet total:", sum(x[1] for x in parquet_values))
print("MATCH:", csv_values == parquet_values)

if csv_values != parquet_values:
    raise RuntimeError("CSV và Parquet chưa khớp kết quả!")

spark.stop()
