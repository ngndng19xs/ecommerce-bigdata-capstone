from pyspark.sql import functions as F
from pyspark.sql.window import Window


ORDER_COLUMNS = [
    "event_time",
    "event_type_rank",
    "event_sort_key",
]


def rank_session_events(events):
    required = {
        "user_session",
        "event_time",
        "event_type_rank",
        "event_sort_key",
        "event_type",
    }

    missing = required - set(events.columns)

    if missing:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(sorted(missing))
        )

    valid = events.filter(
        F.col("user_session").isNotNull()
        & (F.trim(F.col("user_session")) != "")
    )

    order_window = (
        Window
        .partitionBy("user_session")
        .orderBy(
            F.col("event_time").asc(),
            F.col("event_type_rank").asc(),
            F.col("event_sort_key").asc(),
        )
    )

    ranked = valid.withColumn(
        "event_idx",
        F.row_number().over(order_window)
    )

    return ranked


def select_prediction_prefix(ranked, n):
    if n < 1:
        raise ValueError("Prediction N must be >= 1")

    return ranked.filter(
        F.col("event_idx") <= F.lit(n)
    )
