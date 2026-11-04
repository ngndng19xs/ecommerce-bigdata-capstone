"""Compute session funnels from a complete committed ledger snapshot."""

from pyspark.sql import functions as F


def build_session_funnel(ledger_df):
    """Return one row per valid non-null session, using S2 event order."""
    spark = ledger_df.sql_ctx.sparkSession
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Funnel requires Spark session timezone UTC")

    required = [
        "user_session", "is_valid", "event_type",
        "event_time", "event_type_rank", "event_sort_key",
    ]
    missing = [c for c in required if c not in ledger_df.columns]
    if missing:
        raise ValueError("Missing funnel columns: {}".format(missing))

    events = ledger_df.filter(
        (F.col("is_valid") == True)
        & F.col("user_session").isNotNull()
    )

    invalid_order = (
        F.col("event_time").isNull()
        | F.col("event_type_rank").isNull()
        | F.col("event_sort_key").isNull()
    )
    if events.filter(invalid_order).limit(1).count():
        raise ValueError("Valid session event has a null ordering field")

    events = events.withColumn(
        "_event_order",
        F.struct("event_time", "event_type_rank", "event_sort_key"),
    )

    views = events.groupBy("user_session").agg(
        F.min(
            F.when(F.col("event_type") == "view", F.col("_event_order"))
        ).alias("view_order")
    )

    carts = (
        events.join(views, "user_session", "inner")
        .groupBy("user_session")
        .agg(
            F.min(
                F.when(
                    (F.col("event_type") == "cart")
                    & (F.col("_event_order") > F.col("view_order")),
                    F.col("_event_order"),
                )
            ).alias("cart_order")
        )
    )

    purchases = (
        events.join(carts, "user_session", "inner")
        .groupBy("user_session")
        .agg(
            F.min(
                F.when(
                    (F.col("event_type") == "purchase")
                    & (F.col("_event_order") > F.col("cart_order")),
                    F.col("_event_order"),
                )
            ).alias("purchase_order")
        )
    )

    return (
        views.join(carts, "user_session", "inner")
        .join(purchases, "user_session", "inner")
        .withColumn("has_view", F.col("view_order").isNotNull())
        .withColumn("has_cart", F.col("cart_order").isNotNull())
        .withColumn("has_purchase", F.col("purchase_order").isNotNull())
        .withColumn("funnel_date", F.to_date(F.col("view_order.event_time")))
    )
