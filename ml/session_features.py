from pyspark.sql import functions as F

from ml.features import (
    rank_session_events,
    select_prediction_prefix,
)


def build_session_features(events, n, is_valid_rule=None, session_keys=None):
    """
    Returns:
      dataset: one row per eligible session
      audit: session-level exclusion audit
      invalid_session_rows: events with null/blank session

    Optional arguments (defaults keep the historical behaviour):
      is_valid_rule: None = filter is_valid == True when the column exists (legacy, used by the
                     B7-01/B7-02 evidence); 'apply' = same filter explicitly; 'ignore' = keep every
                     event, as B6-01 does.
      session_keys:  DataFrame with a user_session column. When given, only those sessions are
                     ranked (left semi join BEFORE ranking). Ranking is per session, so the rows of
                     the kept sessions are identical to ranking the whole source.
    """

    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise ValueError(
            "prediction N must be a positive integer"
        )

    required = {
        "user_session",
        "event_time",
        "event_type",
        "event_type_rank",
        "event_sort_key",
        "product_id",
        "category_id",
        "brand",
        "price",
    }

    missing = required - set(events.columns)

    if missing:
        raise ValueError(
            "Missing columns: "
            + ", ".join(sorted(missing))
        )

    # 1. Quy tac is_valid. None = hanh vi cu (loc khi co cot is_valid).
    if is_valid_rule not in (None, "apply", "ignore"):
        raise ValueError("is_valid_rule must be None, 'apply' or 'ignore'")
    source = events

    if "is_valid" in source.columns and is_valid_rule != "ignore":
        source = source.filter(
            F.col("is_valid") == F.lit(True)
        )

    # 2. Luu rieng so event khong co session hop le (truoc khi gioi han session).
    invalid_session_rows = source.filter(
        F.col("user_session").isNull()
        | (F.trim(F.col("user_session")) == "")
    )

    # 2b. Gioi han session TRUOC khi xep hang (semi join). Xep hang theo session nen ket qua
    #     cua cac session duoc giu giong het khi xep hang toan bo source.
    if session_keys is not None:
        source = source.join(session_keys, on="user_session", how="left_semi")

    # 3. Rank theo S2 / B6-01.
    ranked = rank_session_events(source)

    # 4. Thong ke toan phien CHI de tao label
    #    va xet dieu kien loai.
    audit = (
        ranked.groupBy("user_session")
        .agg(
            F.count(F.lit(1)).alias("session_length"),

            F.max(
                F.when(
                    (F.col("event_idx") <= F.lit(n))
                    & (F.col("event_type") == "purchase"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("purchased_by_n"),

            F.max(
                F.when(
                    (F.col("event_idx") > F.lit(n))
                    & (F.col("event_type") == "purchase"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("label"),
        )
        .withColumn(
            "status",
            F.when(
                F.col("session_length") < F.lit(n),
                F.lit("too_short")
            )
            .when(
                F.col("purchased_by_n") == 1,
                F.lit("purchased_by_n")
            )
            .otherwise(F.lit("eligible"))
        )
    )

    # 5. Chi lay N event dau tien de tao features.
    prefix = select_prediction_prefix(
        ranked, n
    )

    # Chuan hoa brand theo quy tac Unknown.
    prefix = prefix.withColumn(
        "brand_clean",
        F.when(
            F.col("brand").isNull()
            | (F.trim(F.col("brand")) == ""),
            F.lit("Unknown")
        ).otherwise(F.trim(F.col("brand")))
    )

    # 6. Tao features CHI tu prefix.
    features = prefix.groupBy("user_session").agg(

        F.sum(
            F.when(F.col("event_type") == "view", 1)
            .otherwise(0)
        ).alias("n_view"),

        F.sum(
            F.when(F.col("event_type") == "cart", 1)
            .otherwise(0)
        ).alias("n_cart"),

        F.sum(
            F.when(
                F.col("event_type") == "remove_from_cart",
                1
            ).otherwise(0)
        ).alias("n_remove_from_cart"),

        F.countDistinct("product_id")
            .alias("n_unique_products"),

        F.countDistinct("category_id")
            .alias("n_unique_categories"),

        F.countDistinct("brand_clean")
            .alias("n_unique_brands"),

        F.sort_array(
            F.collect_set("brand_clean")
        ).alias("brands_seen"),

        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price"),

        F.min("event_time").alias("prefix_start"),

        F.max(
            F.when(
                F.col("event_idx") == F.lit(n),
                F.col("event_time")
            )
        ).alias("prediction_time"),

        F.max(
            F.when(
                F.col("event_idx") == F.lit(n),
                F.col("event_type")
            )
        ).alias("last_event_type"),
    )

    features = features.withColumn(
        "prefix_duration_seconds",
        F.col("prediction_time").cast("long")
        - F.col("prefix_start").cast("long")
    )

    # 7. Chi lay label cua session du dieu kien.
    eligible_labels = (
        audit.filter(F.col("status") == "eligible")
        .select(
            "user_session",
            F.col("label").cast("int").alias("label")
        )
    )

    # 8. Ket qua ML: mot dong cho moi session.
    dataset = features.join(
        eligible_labels,
        on="user_session",
        how="inner"
    )

    return dataset, audit, invalid_session_rows
