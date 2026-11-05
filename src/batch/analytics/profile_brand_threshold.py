from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("A7_Brand_Threshold_Profile")
    .getOrCreate()
)

df = spark.read.parquet(
    "/data/analytics/brand_analysis/aggregate"
)

# Chỉ khảo sát các brand có tên xác định
known = df.filter(F.col("brand") != "Unknown")

total = known.count()

print("\n===== BRAND DISTRIBUTION =====")
print("Known brands:", total)

quantiles = known.approxQuantile(
    "view_sessions",
    [0.25, 0.50, 0.75, 0.90],
    0.01
)

print("View sessions quantiles:")
print("25%, 50%, 75%, 90%:", quantiles)

print("\n===== CANDIDATE MINIMUM VIEW THRESHOLDS =====")

thresholds = [1000, 5000, 10000, 25000, 50000, 100000]

for threshold in thresholds:
    retained = known.filter(
        F.col("view_sessions") >= threshold
    ).count()

    print(
        "Threshold:", threshold,
        "| Retained:", retained,
        "| Excluded:", total - retained,
        "| Retained rate:", round(retained * 100 / total, 2), "%"
    )

print("\n===== TOP 15 KNOWN BRANDS =====")

known.orderBy(
    F.desc("view_sessions")
).select(
    "brand",
    "view_sessions",
    "purchase_sessions",
    "session_conversion_rate"
).show(15, truncate=False)

spark.stop()
