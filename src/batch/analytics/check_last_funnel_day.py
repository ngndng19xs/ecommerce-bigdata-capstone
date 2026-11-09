from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckLastFunnelDay")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

path = "/data/analytics/conversion_funnel_full_tmp/funnel_date=2019-11-30"

df = spark.read.parquet(path)
df.show(truncate=False)

spark.stop()
