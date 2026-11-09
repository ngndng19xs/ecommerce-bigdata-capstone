from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Check_A7_Brand_Analysis")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

df = spark.read.parquet(
    "/data/analytics/brand_analysis/aggregate"
)

print("\n===== TOP 20 BRANDS THEO VIEW SESSIONS =====")
df.orderBy(F.desc("view_sessions")).show(20, truncate=False)

print("\n===== KIEM TRA UNKNOWN =====")
df.filter(F.col("brand") == "Unknown").show(
    truncate=False
)

print("\n===== TONG SO BRAND =====")
print("Total brands:", df.count())

spark.stop()
