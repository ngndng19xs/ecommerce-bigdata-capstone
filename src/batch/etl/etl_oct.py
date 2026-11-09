
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# ==========================================
# PHAN 1: KHOI TAO SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("ETL_Oct_2019") \
    .master("local[4]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.shuffle.partitions", "24") \
    .config("spark.sql.parquet.enableDictionary", "false") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# ==========================================
# PHAN 2: KHAI BAO SCHEMA VA DOC DU LIEU
# ==========================================

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True)
])

raw_path = "/data/raw/ecommerce_events/2019-Oct.csv"

df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .csv(raw_path)

print("===== RAW DATA =====", flush=True)

df.printSchema()
df.show(5, truncate=False)

# ==========================================
# PHAN 3: LAM SACH VA BIEN DOI DU LIEU
# ==========================================

# 3.1. Chuan hoa event_time
df = df.withColumn(
    "event_time",
    to_timestamp(
        regexp_replace(
            trim(col("event_time")),
            r"\s+UTC$",
            ""
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# 3.2. Chuan hoa event_type
df = df.withColumn(
    "event_type",
    lower(trim(col("event_type")))
)

# 3.3. Chuan hoa price
df = df.withColumn(
    "price",
    col("price").cast("double")
)

# 3.4. Xu ly brand
df = df.withColumn(
    "brand",
    when(
        col("brand").isNull() |
        (trim(col("brand")) == ""),
        "Unknown"
    ).otherwise(trim(col("brand")))
)

# 3.5. Tao event_date
df = df.withColumn(
    "event_date",
    to_date(col("event_time"))
)

# ==========================================
# PHAN 4: KIEM TRA CHAT LUONG DU LIEU
# ==========================================

valid_events = [
    "view",
    "cart",
    "remove_from_cart",
    "purchase"
]

invalid_condition = (
    col("event_time").isNull() |
    col("event_type").isNull() |
    (~col("event_type").isin(valid_events)) |
    col("price").isNull() |
    (col("price") < 0) |
    col("user_id").isNull()
)

# Ghi nhan ly do ban ghi bi tu choi
df = df.withColumn(
    "reject_reason",
    concat_ws(
        "; ",
        when(
            col("event_time").isNull(),
            "invalid_event_time"
        ),
        when(
            col("event_type").isNull() |
            (~col("event_type").isin(valid_events)),
            "invalid_event_type"
        ),
        when(
            col("price").isNull(),
            "invalid_price_null"
        ),
        when(
            col("price") < 0,
            "invalid_price_negative"
        ),
        when(
            col("user_id").isNull(),
            "invalid_user_id"
        )
    )
)

# ==========================================
# PHAN 5: TACH VALID VA REJECTED
# ==========================================

rejected_df = df.filter(invalid_condition)
valid_df = df.filter(~invalid_condition)

# ==========================================
# TASK 2.1: TAO EVENT_TYPE_RANK
# ==========================================

valid_df = valid_df.withColumn(
    "event_type_rank",
    when(col("event_type") == "view", 1)
    .when(col("event_type") == "cart", 2)
    .when(col("event_type") == "remove_from_cart", 3)
    .when(col("event_type") == "purchase", 4)
)

# ==========================================
# TASK 2.2: TAO EVENT_SORT_KEY
# ==========================================

normalized_fields = struct(
    date_format(
        col("event_time"),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("event_time"),

    trim(col("event_type")).alias("event_type"),

    trim(col("product_id")).alias("product_id"),

    trim(col("category_id")).alias("category_id"),

    trim(col("category_code")).alias("category_code"),

    trim(col("brand")).alias("brand"),

    col("price").cast("decimal(20,6)").cast("string").alias("price"),

    trim(col("user_id")).alias("user_id"),

    trim(col("user_session")).alias("user_session")
)

valid_df = valid_df.withColumn(
    "event_sort_key",
    sha2(
        to_json(
            normalized_fields,
            {"ignoreNullFields": "false"}
        ),
        256
    )
)

# ==========================================
# TASK 2.3: KIEM TRA KET QUA
# ==========================================

print("===== EVENT TYPE RANK =====", flush=True)

valid_df.groupBy(
    "event_type",
    "event_type_rank"
).count().orderBy(
    "event_type_rank"
).show(truncate=False)

print("===== EVENT SORT KEY =====", flush=True)

valid_df.select(
    "event_time",
    "event_type",
    "event_type_rank",
    "event_sort_key"
).show(20, truncate=False)

# Kiem tra gia tri NULL
print("===== CHECK NULL VALUES =====", flush=True)

valid_df.select(
    count(
        when(col("event_type_rank").isNull(), 1)
    ).alias("null_rank"),

    count(
        when(col("event_sort_key").isNull(), 1)
    ).alias("null_sort_key")
).show()

# Kiem tra do dai SHA-256
print("===== CHECK SHA-256 LENGTH =====", flush=True)

valid_df.select(
    length(col("event_sort_key")).alias("key_length")
).groupBy(
    "key_length"
).count().show()

invalid_sha_count = valid_df.filter(
    (col("event_sort_key").isNull()) |
    (length(col("event_sort_key")) != 64)
).count()

print(
    "So ban ghi co SHA-256 khong hop le:",
    invalid_sha_count,
    flush=True
)

# ==========================================
# PHAN 6: THONG KE SO LUONG BAN GHI
# ==========================================

valid_count = valid_df.count()
rejected_count = rejected_df.count()

print("===== DATA QUALITY SUMMARY =====", flush=True)
print("Valid records:", valid_count)
print("Rejected records:", rejected_count)
print("Total processed:", valid_count + rejected_count)

# Thong ke ly do tu choi
print("===== REJECT REASONS =====", flush=True)

rejected_df.groupBy("reject_reason") \
    .count() \
    .orderBy(desc("count")) \
    .show(truncate=False)

# Kiem tra du lieu hop le
print("===== VALID DATAFRAME =====", flush=True)

valid_df.printSchema()
valid_df.show(10, truncate=False)

print("So ban ghi Valid:", valid_count)

# Kiem tra du lieu khong hop le
print("===== REJECTED DATAFRAME =====", flush=True)

rejected_df.printSchema()

rejected_df.select(
    "event_time",
    "event_type",
    "price",
    "user_id",
    "reject_reason"
).show(20, truncate=False)

print("So ban ghi Rejected:", rejected_count)

# ==========================================
# PHAN 7: GHI DU LIEU XUONG HDFS
# ==========================================

valid_output_path = "/data/curated/events_cleaned"
rejected_output_path = "/data/curated/_quarantine/invalid_events"

print("===== GHI VALID DATA =====", flush=True)

valid_df.write \
    .mode("overwrite") \
    .parquet(valid_output_path)

print("===== GHI REJECTED DATA =====", flush=True)

rejected_df.write \
    .mode("overwrite") \
    .parquet(rejected_output_path)

print("===== ETL WRITE COMPLETED =====", flush=True)
print("Valid output:", valid_output_path)
print("Rejected output:", rejected_output_path)

# ==========================================
# TASK A303: GHI PARQUET VA PARTITION
# ==========================================

# 1. Kiem tra du lieu dau vao
print("===== A303: KIEM TRA VALID_DF =====", flush=True)

valid_count = valid_df.count()

print("Tong so ban ghi hop le:", valid_count, flush=True)

valid_df.printSchema()

# 2. Kiem tra event_date
date_summary = valid_df.select(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date"),
    F.countDistinct("event_date").alias("total_days"),
    F.count(
        F.when(F.col("event_date").isNull(), 1)
    ).alias("null_dates")
)

date_summary.show(truncate=False)

# 3. Duong dan HDFS
output_path = "/data/curated/events"

# 4. Ghi Parquet partition
print("===== A303: BAT DAU GHI PARQUET =====", flush=True)

try:
    (
        valid_df
        .repartition(24)
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("event_date")
        .save(output_path)
    )

    print(
        "===== A303: GHI PARQUET THANH CONG =====",
        flush=True
    )

except Exception as e:
    print(
        "===== A303: GHI PARQUET THAT BAI =====",
        flush=True
    )
    print("Chi tiet loi:", repr(e), flush=True)
    raise

# 5. Doc lai du lieu Parquet
print("===== A303: KIEM TRA PARQUET =====", flush=True)

parquet_df = spark.read.parquet(output_path)

parquet_count = parquet_df.count()

print("So dong valid_df:", valid_count)
print("So dong parquet_df:", parquet_count)

# 6. Doi chieu so dong
if valid_count == parquet_count:
    print("PASS: So dong khop nhau", flush=True)
else:
    print("FAIL: So dong khong khop", flush=True)

# 7. Kiem tra schema
parquet_df.printSchema()

# 8. Kiem tra partition
parquet_df.select(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date"),
    F.countDistinct("event_date").alias("total_days"),
    F.count(
        F.when(F.col("event_date").isNull(), 1)
    ).alias("null_dates")
).show(truncate=False)

# ==========================================
# PHAN CUOI: DONG SPARK SESSION
# ==========================================

spark.stop()

print("===== ETL HOAN TAT =====", flush=True)
