from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Check_Funnel_Output")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

df = spark.read.parquet("/data/analytics/conversion_funnel")

print("=== OUTPUT SCHEMA ===")
df.printSchema()

print("=== OUTPUT RECORD COUNT ===")
print("Total daily records:", df.count())

print("=== OUTPUT DATA ===")
df.orderBy("funnel_date").show(40, truncate=False)

spark.stop()
