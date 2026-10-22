from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel


def main():
    spark = (
        SparkSession.builder
        .appName("CleanEcommerceEvents")
        .master("local[2]")
        .config("spark.sql.files.maxPartitionBytes", "32m")
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
        .config("spark.sql.shuffle.partitions", "8")
        # to_date(timestamp) sẽ lấy ngày theo UTC.
        .config("spark.sql.session.timeZone", "UTC")
        # Giá trị cast không hợp lệ thành null để mình phân loại.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .getOrCreate()
    )

    columns = [
        "event_time", "event_type", "product_id",
        "category_id", "category_code", "brand",
        "price", "user_id", "user_session",
    ]

    # Đọc dạng string trước để giữ được giá trị gốc khi có lỗi.
    schema = T.StructType([
        T.StructField(name, T.StringType(), True)
        for name in columns + ["_corrupt_record"]
    ])

    raw = (
        spark.read
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv("data/raw/archive/2019-Nov.csv")
        .withColumn("source_file", F.input_file_name())
    )

    # Giữ dữ liệu gốc để truy vết bản ghi bị loại.
    df = raw.withColumn(
        "raw_record",
        F.struct(*[F.col(name) for name in columns])
    )

    # Chuỗi rỗng hoặc toàn khoảng trắng -> null.
    for name in columns:
        value = F.trim(F.col(name))
        df = df.withColumn(
            name,
            F.when(F.length(value) > 0, value)
        )

    df = df.withColumn("event_type", F.lower("event_type"))

    # Ví dụ đầu vào: 2019-11-01 00:00:00 UTC.
    # Chỉ nhận chuỗi có hậu tố UTC đúng như định dạng nguồn.
    df = df.withColumn(
        "event_time",
        F.when(
            F.col("event_time").rlike(
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$"
            ),
            F.to_timestamp(
                "event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"
            )
        )
    )

    for name in ["product_id", "category_id", "user_id"]:
        df = df.withColumn(
            name,
            F.when(
                F.col(name).rlike(r"^[0-9]+$"),
                F.col(name).cast("long")
            )
        )

    df = (
        df.withColumn(
            "category_id",
            F.when(F.col("category_id") > 0, F.col("category_id"))
        )
        .withColumn("price", F.col("price").cast("decimal(18,2)"))
        .withColumn("event_date", F.to_date("event_time"))
    )

    # Một bản ghi có thể mang nhiều lý do lỗi.
    rules = [
        (F.col("_corrupt_record").isNotNull(), "malformed_csv"),
        (F.col("event_time").isNull(), "invalid_event_time"),
        (
            F.col("event_type").isNull()
            | ~F.col("event_type").isin("view", "cart", "purchase", "remove_from_cart"),
            "invalid_event_type",
        ),
        (
            F.col("product_id").isNull() | (F.col("product_id") <= 0),
            "invalid_product_id",
        ),
        (
            F.col("user_id").isNull() | (F.col("user_id") <= 0),
            "invalid_user_id",
        ),
        (
            F.col("price").isNull() | (F.col("price") < 0),
            "invalid_price",
        ),
        (F.col("user_session").isNull(), "missing_user_session"),
    ]

    df = df.withColumn(
        "error_reason",
        F.concat_ws(
            ";",
            *[F.when(condition, reason) for condition, reason in rules]
        )
    ).persist(StorageLevel.DISK_ONLY)

    try:
        # Materialize để CSV và cột corrupt được đọc đầy đủ.
        total = df.count()
        if total == 0:
            raise ValueError("File đầu vào không có bản ghi dữ liệu.")

        clean = (
            df.filter(F.col("error_reason") == "")
            .drop("error_reason", "_corrupt_record", "raw_record")
        )
        rejected = df.filter(F.col("error_reason") != "")

        rejected_count = rejected.count()
        print(
            f"Input={total}, "
            f"Valid={total - rejected_count}, "
            f"Rejected={rejected_count}"
        )

        clean.printSchema()
        clean.select("event_time", "event_date").show(5, truncate=False)
        rejected.groupBy("error_reason").count().show(truncate=False)

        clean.write.mode("errorifexists").partitionBy(
            "event_date"
        ).option("compression", "snappy").parquet(
            "data/processed/nov_2019_run_01/events"
        )

        rejected.write.mode("errorifexists").option(
            "compression", "snappy"
        ).parquet(
            "data/processed/nov_2019_run_01/rejected_events"
        )
    finally:
        df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
