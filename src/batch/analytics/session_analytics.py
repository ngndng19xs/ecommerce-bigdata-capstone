from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================================
# 1. KHOI TAO SPARK
# =====================================================
spark = (
    SparkSession.builder
    .appName("Session_Analytics_A6")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# 2. KHAI BAO DUONG DAN
# =====================================================
input_path = "/data/curated/events"
output_path = "/data/analytics/session_summary"

print("========== SESSION ANALYTICS A6 ==========")
print("Input :", input_path)
print("Output:", output_path)

# =====================================================
# 3. DOC DU LIEU CURATED
# =====================================================
df = spark.read.parquet(input_path)

# Chi lay cac cot can thiet
events = (
    df.select(
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "price",
        "user_session"
    )
    .filter(
        F.col("user_session").isNotNull()
        & (F.trim(F.col("user_session")) != "")
    )
    .withColumn(
        "price_numeric",
        F.col("price").cast("double")
    )
)

print("\n========== INPUT SCHEMA ==========")
events.printSchema()

# =====================================================
# 4. TONG HOP THEO USER_SESSION
# =====================================================
session_summary = (
    events
    .groupBy("user_session")
    .agg(
        # Thoi gian session
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),

        # Tong so su kien
        F.count("*").alias("total_events"),

        # So luong tung loai su kien
        F.sum(
            F.when(F.col("event_type") == "view", 1).otherwise(0)
        ).alias("view_count"),

        F.sum(
            F.when(F.col("event_type") == "cart", 1).otherwise(0)
        ).alias("cart_count"),

        F.sum(
            F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)
        ).alias("remove_from_cart_count"),

        F.sum(
            F.when(F.col("event_type") == "purchase", 1).otherwise(0)
        ).alias("purchase_count"),

        # San pham va danh muc
        F.countDistinct("product_id").alias("distinct_products"),
        F.countDistinct("category_id").alias("distinct_categories"),

        # Thong ke gia
        F.avg("price_numeric").alias("avg_price"),
        F.min("price_numeric").alias("min_price"),
        F.max("price_numeric").alias("max_price"),

        # Kiem tra du lieu thoi gian
        F.sum(
            F.when(F.col("event_time").isNull(), 1).otherwise(0)
        ).alias("null_event_time_count")
    )
)

# =====================================================
# 5. TAO CAC CHI TIEU SESSION
# =====================================================
session_summary = (
    session_summary
    .withColumn(
        "duration_seconds",
        F.when(
            F.col("session_start").isNotNull()
            & F.col("session_end").isNotNull(),
            F.unix_timestamp("session_end")
            - F.unix_timestamp("session_start")
        )
    )
    .withColumn(
        "session_date",
        F.to_date("session_start")
    )
    .withColumn(
        "has_purchase",
        F.when(F.col("purchase_count") > 0, 1).otherwise(0)
    )
)

# =====================================================
# 6. SAP XEP CAC COT
# =====================================================
result = session_summary.select(
    "user_session",
    "session_date",
    "session_start",
    "session_end",
    "duration_seconds",
    "total_events",
    "view_count",
    "cart_count",
    "remove_from_cart_count",
    "purchase_count",
    "distinct_products",
    "distinct_categories",
    "avg_price",
    "min_price",
    "max_price",
    "has_purchase",
    "null_event_time_count"
)

# =====================================================
# 7. HIEN THI KET QUA MAU
# =====================================================
print("\n========== SESSION SUMMARY SAMPLE ==========")
result.show(10, truncate=False)

# =====================================================
# 8. THONG KE TONG QUAN
# =====================================================
print("\n========== SESSION SUMMARY STATISTICS ==========")

result.select(
    F.count("*").alias("total_sessions"),
    F.sum("has_purchase").alias("purchase_sessions"),
    F.sum(
        F.when(F.col("has_purchase") == 0, 1).otherwise(0)
    ).alias("non_purchase_sessions"),
    F.avg("duration_seconds").alias("avg_duration_seconds"),
    F.avg("total_events").alias("avg_events_per_session")
).show(truncate=False)

# =====================================================
# 9. LUU KET QUA PARQUET
# =====================================================
print("\n========== WRITING SESSION SUMMARY ==========")

(
    result
    .write
    .mode("overwrite")
    .partitionBy("session_date")
    .parquet(output_path)
)

print("SUCCESS: Session summary saved to", output_path)

spark.stop()
