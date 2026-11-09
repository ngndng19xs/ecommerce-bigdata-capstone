from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, to_timestamp, count
from pyspark.sql.types import StructType, StructField, StringType
import time

spark = (
    SparkSession.builder
    .appName("A9_E1_Timing_Check")
    .master("local[4]")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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

csv_df = (
    spark.read.schema(schema).option("header", "true")
    .csv("/data/raw/ecommerce_events/2019-Oct.csv")
    .withColumn(
        "event_time",
        to_timestamp(
            regexp_replace(trim(col("event_time")), r"\s+UTC$", ""),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn("event_type", lower(trim(col("event_type"))))
    .withColumn("price", col("price").cast("double"))
    .filter(
        col("event_time").isNotNull()
        & col("event_type").isin(
            "view", "cart", "remove_from_cart", "purchase"
        )
        & col("price").isNotNull()
        & (col("price") >= 0)
        & col("user_id").isNotNull()
    )
)

parquet_df = (
    spark.read.parquet("/data/curated/events")
    .filter(
        (col("event_date") >= "2019-10-01")
        & (col("event_date") < "2019-11-01")
    )
)

for name, df in [("CSV", csv_df), ("PARQUET", parquet_df)]:
    print(f"\n===== {name} PLAN =====")
    query = df.groupBy("event_type").agg(
        count("*").alias("event_count")
    ).orderBy("event_type")

    query.explain("formatted")

    for run in ["WARMUP", "RUN_1", "RUN_2"]:
        start = time.perf_counter()
        result = query.collect()
        elapsed = time.perf_counter() - start
        print(
            f"{name} {run}: {elapsed:.3f}s | "
            f"rows={len(result)} | result={result}",
            flush=True
        )

spark.stop()
