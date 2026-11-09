from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("A9-Inspect-Parquet-Schema")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "12")
    .getOrCreate()
)

df = spark.read.parquet("hdfs:///data/curated/events")

print("\n===== PARQUET SCHEMA =====")
df.printSchema()

print("\n===== PARTITION COLUMN =====")
print("event_date" in df.columns)

print("\n===== NUMBER OF INPUT FILES =====")
print(len(df.inputFiles()))

print("\n===== SAMPLE QUERY PLAN =====")
df.select("event_type", "price", "user_id", "event_date") \
  .filter("event_date = '2019-10-01'") \
  .explain("formatted")

spark.stop()
