from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH = "/data/analytics/brand_analysis/aggregate"

OUTPUT_PATH = (
    "/data/analytics/brand_analysis/"
    "top_brands_min_view_10000"
)

MIN_VIEW_SESSIONS = 10000

spark = (
    SparkSession.builder
    .appName("P6_A7_Top_Brand_Ranking")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

df = spark.read.parquet(INPUT_PATH)

# Loại Unknown khỏi bảng xếp hạng thương hiệu
# nhưng vẫn giữ Unknown trong bảng tổng hợp A7-01.
known_brands = df.filter(
    (F.col("brand") != "Unknown") &
    (F.col("view_sessions") >= MIN_VIEW_SESSIONS)
)

total_known = df.filter(
    F.col("brand") != "Unknown"
).count()

retained = known_brands.count()
excluded = total_known - retained

# Xếp hạng theo số view sessions giảm dần
window_spec = Window.orderBy(
    F.desc("view_sessions"),
    F.asc("brand")
)

ranking = (
    known_brands
    .withColumn(
        "rank",
        F.row_number().over(window_spec)
    )
    .withColumn(
        "min_view_sessions",
        F.lit(MIN_VIEW_SESSIONS)
    )
    .select(
        "rank",
        "brand",
        "total_events",
        "view_events",
        "cart_events",
        "purchase_events",
        "view_sessions",
        "purchase_sessions",
        "session_conversion_rate",
        "min_view_sessions"
    )
)

ranking = ranking.orderBy("rank")

ranking.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\n===== A7-02 BRAND RANKING =====")
print("Minimum view sessions:", MIN_VIEW_SESSIONS)
print("Total known brands:", total_known)
print("Retained brands:", retained)
print("Excluded brands:", excluded)

if total_known > 0:
    print(
        "Retained rate:",
        round(retained * 100.0 / total_known, 2),
        "%"
    )

ranking.show(30, truncate=False)

print("Output:", OUTPUT_PATH)

spark.stop()

