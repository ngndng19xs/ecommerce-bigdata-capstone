
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "/data/curated/events"
OUTPUT_PATH = "/data/analytics/brand_analysis/aggregate"

spark = (
    SparkSession.builder
    .appName("P6_A7_Brand_Analysis")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "12")
    .getOrCreate()
)

# 1. Read partitioned curated data
df = spark.read.parquet(INPUT_PATH)

# 2. Normalize brand
df = df.withColumn(
    "brand",
    F.when(
        F.col("brand").isNull() | (F.trim(F.col("brand")) == ""),
        "Unknown"
    ).otherwise(F.trim(F.col("brand")))
)

# 3. Aggregate event counts and unique sessions by brand
brand_agg = (
    df.groupBy("brand")
    .agg(
        F.count("*").alias("total_events"),

        F.sum(
            F.when(F.col("event_type") == "view", 1).otherwise(0)
        ).alias("view_events"),

        F.sum(
            F.when(F.col("event_type") == "cart", 1).otherwise(0)
        ).alias("cart_events"),

        F.sum(
            F.when(F.col("event_type") == "purchase", 1).otherwise(0)
        ).alias("purchase_events"),

        F.sum(
            F.when(F.col("event_type") == "remove_from_cart", 1)
            .otherwise(0)
        ).alias("remove_from_cart_events"),

        F.countDistinct(
            F.when(
                F.col("event_type") == "view",
                F.col("user_session")
            )
        ).alias("view_sessions"),

        F.countDistinct(
            F.when(
                F.col("event_type") == "purchase",
                F.col("user_session")
            )
        ).alias("purchase_sessions"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                F.col("price")
            ).otherwise(0)
        ).alias("purchase_event_value")
    )
)

# 4. Calculate session conversion rate
brand_agg = brand_agg.withColumn(
    "session_conversion_rate",
    F.when(
        F.col("view_sessions") > 0,
        F.round(
            F.col("purchase_sessions") * 100.0 /
            F.col("view_sessions"), 2
        )
    ).otherwise(0.0)
)

# 5. Save aggregate result
(
    brand_agg
    .orderBy(F.desc("view_sessions"))
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Brand Analytics completed!")
print("Output:", OUTPUT_PATH)

spark.stop()

