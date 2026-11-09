
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# ==========================================
# PHAN 1: KHOI TAO SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("ETL_Nov_2019") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.sql.parquet.enableDictionary", "false") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

try:

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

    raw_path = "/data/raw/ecommerce_events/2019-Nov.csv"

    df = spark.read \
        .schema(schema) \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .csv(raw_path)

    print("===== NOV RAW DATA =====", flush=True)
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

    # 3.3. Chuyen doi price
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

    missing_event_type = (
        col("event_type").isNull() |
        (trim(col("event_type")) == "")
    )

    missing_user_id = (
        col("user_id").isNull() |
        (trim(col("user_id")) == "")
    )

    invalid_condition = (
        col("event_time").isNull() |
        missing_event_type |
        (~col("event_type").isin(valid_events)) |
        col("price").isNull() |
        (col("price") < 0) |
        missing_user_id
    )

    # ==========================================
    # PHAN 5: GHI NHAN LY DO REJECTED
    # ==========================================

    df = df.withColumn(
        "reject_reason",
        concat_ws(
            "; ",
            when(
                col("event_time").isNull(),
                "invalid_event_time"
            ),
            when(
                missing_event_type |
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
                missing_user_id,
                "invalid_user_id"
            )
        )
    )

    # ==========================================
    # PHAN 6: TACH VALID VA REJECTED
    # ==========================================

    rejected_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    # 6.1. Event type rank
    valid_df = valid_df.withColumn(
        "event_type_rank",
        when(col("event_type") == "view", 1)
        .when(col("event_type") == "cart", 2)
        .when(col("event_type") == "remove_from_cart", 3)
        .when(col("event_type") == "purchase", 4)
    )

    # 6.2. Event sort key
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

        col("price")
        .cast("decimal(20,6)")
        .cast("string")
        .alias("price"),

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
    # PHAN 7: KIEM TRA VA THONG KE
    # ==========================================

    print("===== EVENT TYPE RANK =====", flush=True)

    valid_df.groupBy(
        "event_type",
        "event_type_rank"
    ).count().orderBy(
        "event_type_rank"
    ).show(truncate=False)

    print("===== CHECK SHA-256 =====", flush=True)

    valid_df.select(
        length("event_sort_key").alias("key_length")
    ).groupBy("key_length").count().show()

    print("===== DATA QUALITY SUMMARY =====", flush=True)

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    print("Valid records:", valid_count, flush=True)
    print("Rejected records:", rejected_count, flush=True)
    print(
        "Total processed:",
        valid_count + rejected_count,
        flush=True
    )

    print("===== REJECT REASONS =====", flush=True)

    rejected_df.groupBy(
        "reject_reason"
    ).count().orderBy(
        desc("count")
    ).show(truncate=False)

    # ==========================================
    # PHAN 8: GHI VALID VA REJECTED
    # ==========================================

    valid_output_path = (
        "/data/curated/events_cleaned/month=2019-11"
    )

    rejected_output_path = (
        "/data/curated/_quarantine/"
        "invalid_events/month=2019-11"
    )

    print("===== WRITE VALID DATA =====", flush=True)

    valid_df.write \
        .mode("overwrite") \
        .parquet(valid_output_path)

    print("===== WRITE REJECTED DATA =====", flush=True)

    rejected_df.write \
        .mode("overwrite") \
        .parquet(rejected_output_path)

    # ==========================================
    # PHAN 9: TASK A303 - GHI PARQUET PARTITION
    # ==========================================

    print("===== A303: CHECK EVENT DATE =====", flush=True)

    valid_df.select(
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date"),
        F.countDistinct("event_date").alias("total_days"),
        F.count(
            F.when(F.col("event_date").isNull(), 1)
        ).alias("null_dates")
    ).show(truncate=False)

    output_path = "/data/curated/events"

    print("===== A303: WRITE PARTITIONED PARQUET =====", flush=True)

    (
        valid_df
        .repartition(12, "event_date")
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("event_date")
        .save(output_path)
    )

    print("===== A303: WRITE COMPLETED =====", flush=True)

    # ==========================================
    # PHAN 10: DOC LAI VA DOI CHIEU
    # ==========================================

    parquet_df = spark.read.parquet(output_path)

    parquet_count = parquet_df.count()

    print("===== A303: ROW COUNT CHECK =====", flush=True)
    print("Valid records:", valid_count, flush=True)
    print("Parquet records:", parquet_count, flush=True)

    if valid_count == parquet_count:
        print("PASS: RECORD COUNTS MATCH", flush=True)
    else:
        print("FAIL: RECORD COUNTS DO NOT MATCH", flush=True)

    print("===== A303: SCHEMA CHECK =====", flush=True)
    parquet_df.printSchema()

    print("===== A303: PARTITION CHECK =====", flush=True)

    parquet_df.select(
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date"),
        F.countDistinct("event_date").alias("total_days"),
        F.count(
            F.when(F.col("event_date").isNull(), 1)
        ).alias("null_dates")
    ).show(truncate=False)

    print("===== ETL NOV COMPLETED =====", flush=True)

finally:
    spark.stop()
