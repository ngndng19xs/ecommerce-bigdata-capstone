from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Khởi tạo Spark
spark = (
    SparkSession.builder
    .appName("Batch_Conversion_Funnel")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "32")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Khai báo đường dẫn
input_path = "/data/curated/events"
output_path = "/data/analytics/conversion_funnel"

print("=== BATCH CONVERSION FUNNEL ===")
print("Input:", input_path)
print("Output:", output_path)

# 3. Đọc dữ liệu đã ETL
df = spark.read.parquet(input_path)

# Chỉ lấy các cột cần thiết và các loại sự kiện thuộc Funnel
events = (
    df.select(
        "event_time",
        "event_type",
        "event_type_rank",
        "event_sort_key",
        "user_session"
    )
    .filter(
        F.col("user_session").isNotNull()
        & (F.trim(F.col("user_session")) != "")
        & F.col("event_time").isNotNull()
        & F.col("event_type_rank").isNotNull()
        & F.col("event_sort_key").isNotNull()
        & F.col("event_type").isin("view", "cart", "purchase")
    )
)

# 4. Đánh số thứ tự sự kiện trong từng session
session_window = Window.partitionBy("user_session")

order_window = (
    Window.partitionBy("user_session")
    .orderBy(
        F.col("event_time"),
        F.col("event_type_rank"),
        F.col("event_sort_key")
    )
)

ordered = events.withColumn(
    "event_position",
    F.row_number().over(order_window)
)

# 5. Tìm vị trí và thời gian View đầu tiên
with_view = (
    ordered
    .withColumn(
        "first_view_position",
        F.min(
            F.when(
                F.col("event_type") == "view",
                F.col("event_position")
            )
        ).over(session_window)
    )
    .withColumn(
        "first_view_time",
        F.min(
            F.when(
                F.col("event_type") == "view",
                F.col("event_time")
            )
        ).over(session_window)
    )
)

# 6. Tìm Cart đầu tiên xảy ra sau View đầu tiên
with_cart = with_view.withColumn(
    "first_cart_position",
    F.min(
        F.when(
            (F.col("event_type") == "cart")
            & (F.col("event_position") > F.col("first_view_position")),
            F.col("event_position")
        )
    ).over(session_window)
)

# 7. Tìm Purchase đầu tiên xảy ra sau Cart đầu tiên
with_purchase = with_cart.withColumn(
    "first_purchase_position",
    F.min(
        F.when(
            (F.col("event_type") == "purchase")
            & (F.col("event_position") > F.col("first_cart_position")),
            F.col("event_position")
        )
    ).over(session_window)
)

# 8. Tổng hợp về một dòng cho mỗi session
sessions = (
    with_purchase
    .groupBy("user_session")
    .agg(
        F.max("first_view_time").alias("first_view_time"),
        F.max("first_view_position").alias("first_view_position"),
        F.max("first_cart_position").alias("first_cart_position"),
        F.max("first_purchase_position").alias("first_purchase_position")
    )
    .filter(F.col("first_view_position").isNotNull())
    .withColumn("funnel_date", F.to_date("first_view_time"))
    .withColumn(
        "has_view",
        F.lit(1)
    )
    .withColumn(
        "has_cart",
        F.when(F.col("first_cart_position").isNotNull(), 1).otherwise(0)
    )
    .withColumn(
        "has_purchase",
        F.when(F.col("first_purchase_position").isNotNull(), 1).otherwise(0)
    )
)

# 9. Tính Funnel theo ngày View đầu tiên
daily_funnel = (
    sessions
    .groupBy("funnel_date")
    .agg(
        F.sum("has_view").alias("view_sessions"),
        F.sum("has_cart").alias("cart_sessions"),
        F.sum("has_purchase").alias("purchase_sessions")
    )
    .withColumn(
        "view_to_cart_rate",
        F.round(
            F.col("cart_sessions") / F.col("view_sessions") * 100, 2
        )
    )
    .withColumn(
        "cart_to_purchase_rate",
        F.when(
            F.col("cart_sessions") > 0,
            F.round(
                F.col("purchase_sessions") / F.col("cart_sessions") * 100, 2
            )
        ).otherwise(0)
    )
    .withColumn(
        "view_to_purchase_rate",
        F.round(
            F.col("purchase_sessions") / F.col("view_sessions") * 100, 2
        )
    )
    .orderBy("funnel_date")
)

# 10. Hiển thị kết quả
print("=== DAILY CONVERSION FUNNEL ===")
daily_funnel.show(40, truncate=False)

# 11. Lưu kết quả Parquet, phân vùng theo ngày
(
    daily_funnel
    .write
    .mode("overwrite")
    .partitionBy("funnel_date")
    .parquet(output_path)
)

print("=== FUNNEL COMPLETED ===")
print("Output:", output_path)

spark.stop()
