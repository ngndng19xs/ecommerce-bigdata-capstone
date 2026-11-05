from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Khoi tao Spark
spark = (
    SparkSession.builder
    .appName("Session_Comparison_A6")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Duong dan
input_path = "/data/analytics/session_summary"
output_path = "/data/analytics/session_summary_analysis"

print("========== SESSION COMPARISON A6-02 ==========")
print("Input :", input_path)
print("Output:", output_path)

# 3. Doc session summary
df = spark.read.parquet(input_path)

# 4. Gan nhan hai nhom
sessions = (
    df.withColumn(
        "session_group",
        F.when(F.col("has_purchase") == 1, "purchase")
         .otherwise("non_purchase")
    )
)

# 5. Thong ke tong quan hai nhom
group_summary = (
    sessions
    .groupBy("session_group")
    .agg(
        F.count("*").alias("total_sessions"),

        F.avg("duration_seconds").alias("avg_duration_seconds"),
        F.expr(
            "percentile_approx(duration_seconds, 0.5, 10000)"
        ).alias("median_duration_seconds"),
        F.expr(
            "percentile_approx(duration_seconds, 0.9, 10000)"
        ).alias("p90_duration_seconds"),

        F.avg("total_events").alias("avg_events_per_session"),
        F.expr(
            "percentile_approx(total_events, 0.5, 10000)"
        ).alias("median_events_per_session"),
        F.expr(
            "percentile_approx(total_events, 0.9, 10000)"
        ).alias("p90_events_per_session"),

        F.avg("distinct_products").alias("avg_distinct_products"),
        F.avg("distinct_categories").alias("avg_distinct_categories"),

        F.avg("view_count").alias("avg_views"),
        F.avg("cart_count").alias("avg_carts"),
        F.avg("purchase_count").alias("avg_purchases")
    )
    .orderBy("session_group")
)

# 6. Tao cac nhom phan phoi so su kien/session
sessions_bucket = (
    sessions
    .withColumn(
        "event_bucket",
        F.when(F.col("total_events") == 1, "01_event")
         .when(F.col("total_events").between(2, 3), "02-03_events")
         .when(F.col("total_events").between(4, 5), "04-05_events")
         .when(F.col("total_events").between(6, 10), "06-10_events")
         .when(F.col("total_events").between(11, 20), "11-20_events")
         .otherwise("over_20_events")
    )
)

event_distribution = (
    sessions_bucket
    .groupBy("session_group", "event_bucket")
    .agg(
        F.count("*").alias("session_count")
    )
)

# Tinh ty le trong tung nhom session
group_totals = (
    sessions
    .groupBy("session_group")
    .agg(F.count("*").alias("group_total"))
)

event_distribution = (
    event_distribution
    .join(group_totals, "session_group")
    .withColumn(
        "percentage",
        F.round(
            F.col("session_count") / F.col("group_total") * 100,
            2
        )
    )
    .drop("group_total")
    .orderBy("session_group", "event_bucket")
)

# 7. Hien thi ket qua
print("\n========== PURCHASE VS NON-PURCHASE ==========")
group_summary.show(truncate=False)

print("\n========== EVENT DISTRIBUTION ==========")
event_distribution.show(30, truncate=False)

# 8. Luu hai bang ket qua
(
    group_summary
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_path + "/group_summary")
)

(
    event_distribution
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_path + "/event_distribution")
)

print("\nSUCCESS: Session comparison saved to", output_path)

spark.stop()
