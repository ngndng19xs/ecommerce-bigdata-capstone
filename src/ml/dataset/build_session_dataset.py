"""Session-level dataset generation module for purchase prediction.

This module processes raw/curated clickstream events and builds a session-level
dataset suitable for binary classification (predicting whether a purchase occurs
in the session after observing the first K events).

Strict Anti-Data-Leakage Design:
1. Only the first K events (head: rn <= k) are used to extract features.
2. The remaining events (tail: rn > k) are used solely to determine the binary label:
   label = 1 if at least one 'purchase' event occurs in tail, else 0.
3. Sessions containing any 'purchase' event within the first K events are dropped.
4. Sessions starting on the final date of the dataset are excluded (right-censoring).
5. Explicit Exclusions:
   - Neither 'user_session', 'user_id', 'source_file', 'n_events', nor any overall session
     duration/timestamps are used as predictive features.
   - Tail events never leak into feature calculations.
"""

import os
from typing import Dict, Optional
from pyspark.sql import DataFrame, functions as F, Window
from src.common.logger import get_logger
from src.common.schema import RAW_EVENT_SCHEMA, normalize_events
from src.common.spark_session import get_spark, load_config

logger = get_logger("build_session_dataset")


def build_session_dataset(events_df: DataFrame, k: int = 5) -> DataFrame:
    """Build session-level feature and label DataFrame according to T010 specifications.

    Parameters
    ----------
    events_df : DataFrame
        Normalized clickstream events DataFrame containing at least:
        event_time, event_type, product_id, category_code, brand, price, user_session.
    k : int, optional
        Number of prefix events to observe before making a prediction, default 5.

    Returns
    -------
    DataFrame
        Dataset containing user_session, session_start_date, label, and prefix features.
    """
    logger.info("Starting build_session_dataset with K = %d", k)

    # 1. Clean: drop null user_session and null event_time; impute null brand and category_code
    cleaned_df = events_df.filter(
        F.col("user_session").isNotNull() & F.col("event_time").isNotNull()
    )
    cleaned_df = cleaned_df.withColumn(
        "brand",
        F.when(F.col("brand").isNull(), "Unknown").otherwise(F.col("brand")),
    ).withColumn(
        "category_code",
        F.when(F.col("category_code").isNull(), "Unknown").otherwise(F.col("category_code")),
    )

    # Calculate global max date for right-censoring filter
    max_dataset_date_row = cleaned_df.select(F.max(F.to_date("event_time"))).collect()
    max_dataset_date = max_dataset_date_row[0][0] if max_dataset_date_row else None
    logger.info("Global dataset maximum event date (for right-censoring): %s", max_dataset_date)

    # 2. Deterministic ordering: event_time ASC, type_rank ASC, product_id ASC
    # type_rank: view=1 < cart=2 < remove_from_cart=3 < purchase=4, other=99
    type_rank_col = (
        F.when(F.col("event_type") == "view", 1)
        .when(F.col("event_type") == "cart", 2)
        .when(F.col("event_type") == "remove_from_cart", 3)
        .when(F.col("event_type") == "purchase", 4)
        .otherwise(99)
    )

    order_win = Window.partitionBy("user_session").orderBy(
        F.col("event_time").asc(),
        type_rank_col.asc(),
        F.col("product_id").asc(),
    )
    ranked_df = cleaned_df.withColumn("rn", F.row_number().over(order_win))

    # 3. Session-level eligibility and labeling
    session_summary = ranked_df.groupBy("user_session").agg(
        F.max("rn").alias("n_events"),
        F.min(F.to_date("event_time")).alias("session_start_date"),
        F.max(
            F.when((F.col("rn") <= k) & (F.col("event_type") == "purchase"), 1).otherwise(0)
        ).alias("has_purchase_head"),
        F.max(
            F.when((F.col("rn") > k) & (F.col("event_type") == "purchase"), 1).otherwise(0)
        ).alias("label"),
    )

    # Apply eligibility filters:
    # - n_events > k
    # - no purchase in head (has_purchase_head == 0)
    # - session_start_date != max_dataset_date (right-censoring)
    valid_sessions = session_summary.filter(
        (F.col("n_events") > k)
        & (F.col("has_purchase_head") == 0)
        & (F.col("session_start_date") != max_dataset_date)
    ).select("user_session", "session_start_date", "label")

    # 4. Extract features strictly from head (rn <= k)
    head_df = ranked_df.filter(F.col("rn") <= k)

    head_features = head_df.groupBy("user_session").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_count"),
        F.size(F.collect_set("product_id")).alias("unique_product_count"),
        F.size(F.collect_set("category_code")).alias("unique_category_count"),
        F.size(F.collect_set("brand")).alias("unique_brand_count"),
        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
        (
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None)).cast("long")
            - F.min(F.when(F.col("rn") == 1, F.col("event_time")).otherwise(None)).cast("long")
        ).alias("interaction_duration"),
        F.hour(
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None))
        ).alias("hour_of_day"),
        F.dayofweek(
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None))
        ).alias("day_of_week"),
        F.max(F.when(F.col("rn") == k, F.col("event_type")).otherwise(None)).alias("last_event_type"),
        F.max(F.when(F.col("rn") == k - 1, F.col("event_type")).otherwise(None)).alias("prev_event_type"),
    ).withColumn("has_cart", F.when(F.col("cart_count") > 0, 1).otherwise(0))

    # Join valid session metadata and labels with head features
    dataset_df = valid_sessions.join(head_features, on="user_session", how="inner")

    return dataset_df


def run_eda_and_save(events_df: DataFrame, k: int, output_path: str) -> DataFrame:
    """Run full session dataset creation, log EDA statistics, and save to Parquet.

    Parameters
    ----------
    events_df : DataFrame
        Normalized clickstream events.
    k : int
        Prefix length.
    output_path : str
        Parquet output path.

    Returns
    -------
    DataFrame
        Created session dataset.
    """
    cleaned_df = events_df.filter(
        F.col("user_session").isNotNull() & F.col("event_time").isNotNull()
    )
    cleaned_df = cleaned_df.withColumn(
        "brand",
        F.when(F.col("brand").isNull(), "Unknown").otherwise(F.col("brand")),
    ).withColumn(
        "category_code",
        F.when(F.col("category_code").isNull(), "Unknown").otherwise(F.col("category_code")),
    )

    max_dataset_date_row = cleaned_df.select(F.max(F.to_date("event_time"))).collect()
    max_dataset_date = max_dataset_date_row[0][0] if max_dataset_date_row else None

    type_rank_col = (
        F.when(F.col("event_type") == "view", 1)
        .when(F.col("event_type") == "cart", 2)
        .when(F.col("event_type") == "remove_from_cart", 3)
        .when(F.col("event_type") == "purchase", 4)
        .otherwise(99)
    )

    order_win = Window.partitionBy("user_session").orderBy(
        F.col("event_time").asc(),
        type_rank_col.asc(),
        F.col("product_id").asc(),
    )
    ranked_df = cleaned_df.withColumn("rn", F.row_number().over(order_win))

    session_summary = ranked_df.groupBy("user_session").agg(
        F.max("rn").alias("n_events"),
        F.min(F.to_date("event_time")).alias("session_start_date"),
        F.max(
            F.when((F.col("rn") <= k) & (F.col("event_type") == "purchase"), 1).otherwise(0)
        ).alias("has_purchase_head"),
        F.max(
            F.when((F.col("rn") > k) & (F.col("event_type") == "purchase"), 1).otherwise(0)
        ).alias("label"),
    ).cache()

    # Calculate EDA metrics
    total_sessions = session_summary.count()
    le_k_count = session_summary.filter(F.col("n_events") <= k).count()
    gt_k_df = session_summary.filter(F.col("n_events") > k)
    pur_head_count = gt_k_df.filter(F.col("has_purchase_head") == 1).count()
    no_pur_head_df = gt_k_df.filter(F.col("has_purchase_head") == 0)
    last_day_count = no_pur_head_df.filter(F.col("session_start_date") == max_dataset_date).count()
    valid_sessions = no_pur_head_df.filter(F.col("session_start_date") != max_dataset_date).cache()

    remaining_sessions = valid_sessions.count()
    pos_label_count = valid_sessions.filter(F.col("label") == 1).count()
    neg_label_count = remaining_sessions - pos_label_count

    logger.info("================ EDA STATISTICS (K = %d) ================", k)
    logger.info("1. Total distinct sessions: %d", total_sessions)
    logger.info(
        "2. Excluded (n_events <= %d): %d (%.2f%%)",
        k,
        le_k_count,
        (le_k_count / total_sessions * 100.0) if total_sessions else 0.0,
    )
    logger.info(
        "3. Excluded (purchase in head rn <= %d): %d (%.2f%%)",
        k,
        pur_head_count,
        (pur_head_count / total_sessions * 100.0) if total_sessions else 0.0,
    )
    logger.info(
        "4. Excluded (right-censoring at %s): %d (%.2f%%)",
        max_dataset_date,
        last_day_count,
        (last_day_count / total_sessions * 100.0) if total_sessions else 0.0,
    )
    logger.info(
        "5. Remaining eligible sessions: %d (%.2f%%)",
        remaining_sessions,
        (remaining_sessions / total_sessions * 100.0) if total_sessions else 0.0,
    )
    logger.info(
        "6. Target label distribution: label=1: %d (%.2f%%), label=0: %d (%.2f%%)",
        pos_label_count,
        (pos_label_count / remaining_sessions * 100.0) if remaining_sessions else 0.0,
        neg_label_count,
        (neg_label_count / remaining_sessions * 100.0) if remaining_sessions else 0.0,
    )

    # Group by session_start_date
    date_dist = (
        valid_sessions.groupBy("session_start_date")
        .agg(
            F.count("user_session").alias("total_sessions"),
            F.sum("label").alias("pos_sessions"),
        )
        .orderBy("session_start_date")
        .collect()
    )

    logger.info("7. Session distribution across session_start_date:")
    for row in date_dist:
        logger.info(
            "   Date: %s | Sessions: %d | Purchase: %d (%.2f%%)",
            row["session_start_date"],
            row["total_sessions"],
            row["pos_sessions"],
            (row["pos_sessions"] / row["total_sessions"] * 100.0) if row["total_sessions"] else 0.0,
        )
    logger.info("==========================================================")

    # Extract head features and assemble final dataset
    head_df = ranked_df.filter(F.col("rn") <= k)
    head_features = head_df.groupBy("user_session").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_count"),
        F.size(F.collect_set("product_id")).alias("unique_product_count"),
        F.size(F.collect_set("category_code")).alias("unique_category_count"),
        F.size(F.collect_set("brand")).alias("unique_brand_count"),
        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
        (
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None)).cast("long")
            - F.min(F.when(F.col("rn") == 1, F.col("event_time")).otherwise(None)).cast("long")
        ).alias("interaction_duration"),
        F.hour(
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None))
        ).alias("hour_of_day"),
        F.dayofweek(
            F.max(F.when(F.col("rn") == k, F.col("event_time")).otherwise(None))
        ).alias("day_of_week"),
        F.max(F.when(F.col("rn") == k, F.col("event_type")).otherwise(None)).alias("last_event_type"),
        F.max(F.when(F.col("rn") == k - 1, F.col("event_type")).otherwise(None)).alias("prev_event_type"),
    ).withColumn("has_cart", F.when(F.col("cart_count") > 0, 1).otherwise(0))

    final_dataset = valid_sessions.select("user_session", "session_start_date", "label").join(
        head_features, on="user_session", how="inner"
    )

    logger.info("Writing session dataset to Parquet: %s", output_path)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    final_dataset.write.mode("overwrite").parquet(output_path)
    logger.info("Session dataset successfully saved to %s", output_path)

    return final_dataset


def main() -> None:
    """CLI entrypoint for building session dataset."""
    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="BuildSessionDataset", cfg=cfg)

    ml_cfg = cfg.get("ml", {})
    sample_csv = ml_cfg.get("sample_csv", "data/raw/sample_nov_1pct.csv")
    output_path = ml_cfg.get("session_features_path", "data/processed/ml/session_features")
    k = int(ml_cfg.get("k_events", 5))

    logger.info("Loading raw events from: %s", sample_csv)
    raw_df = spark.read.option("header", True).schema(RAW_EVENT_SCHEMA).csv(sample_csv)
    normalized_df = normalize_events(raw_df)

    run_eda_and_save(events_df=normalized_df, k=k, output_path=output_path)
    spark.stop()


if __name__ == "__main__":
    main()
