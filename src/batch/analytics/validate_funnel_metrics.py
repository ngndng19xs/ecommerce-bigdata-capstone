from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Validate_Funnel_Metrics")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/data/analytics/conversion_funnel")

# Kiểm tra số ngày và ngày bị trùng
total_days = df.count()
duplicate_days = (
    df.groupBy("funnel_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Kiểm tra số liệu và tỷ lệ
checked = df.withColumn(
    "invalid_counts",
    (F.col("view_sessions") <= 0)
    | (F.col("cart_sessions") > F.col("view_sessions"))
    | (F.col("purchase_sessions") > F.col("cart_sessions"))
).withColumn(
    "invalid_rates",
    (F.abs(
        F.col("view_to_cart_rate")
        - F.round(F.col("cart_sessions") / F.col("view_sessions") * 100, 2)
    ) > 0.001)
    | (F.abs(
        F.col("cart_to_purchase_rate")
        - F.round(F.col("purchase_sessions") / F.col("cart_sessions") * 100, 2)
    ) > 0.001)
    | (F.abs(
        F.col("view_to_purchase_rate")
        - F.round(F.col("purchase_sessions") / F.col("view_sessions") * 100, 2)
    ) > 0.001)
)

invalid_count = checked.filter(
    F.col("invalid_counts") | F.col("invalid_rates")
).count()

print("=== FUNNEL VALIDATION ===")
print("Total days:", total_days)
print("Duplicate dates:", duplicate_days)
print("Invalid daily records:", invalid_count)

if total_days == 31 and duplicate_days == 0 and invalid_count == 0:
    print("VALIDATION RESULT: PASS")
else:
    print("VALIDATION RESULT: CHECK REQUIRED")

spark.stop()

