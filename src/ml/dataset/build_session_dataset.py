"""Session-level dataset generation module for purchase prediction (Task S2 / ML Contract v1.0).

This module processes curated clickstream events from /data/curated/events and builds a session-level
dataset suitable for binary classification (predicting whether a purchase occurs in the session after
observing the first N events).

Strict Anti-Data-Leakage Design (Data Contract v1.0, Section 9):
1. Ordering: Strictly ordered by (event_time ASC, event_type_rank ASC, event_sort_key ASC).
   Inherits event_type_rank and event_sort_key directly from Curated Parquet; never uses product_id as tie-breaker.
2. Prediction Point N: Head consists of events with row_number rn <= N (including event N).
   Tail consists of events with row_number rn > N.
3. Feature Isolation: All features are extracted strictly from Head (rn <= N).
   Tail events and overall session properties (total duration, total events, session end time) never leak into features.
4. Target Label: Binary label in {0, 1} determined solely from Tail (rn > N):
   label = 1 if at least one 'purchase' event occurs in Tail, else 0 (for complete sessions).
5. Early Purchase Exclusion: Sessions with any 'purchase' event occurring at or before event N (rn <= N)
   are strictly excluded from the ML dataset.
6. Short Sessions Exclusion: Sessions with fewer than N events (n_events < N) are strictly excluded.
7. Clean Session ID: Records with null, empty, or whitespace-only user_session are excluded.
"""

import os
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, Window, functions as F
from src.common.logger import get_logger
from src.common.spark_session import get_spark, load_config

logger = get_logger("build_session_dataset")

# Curated Parquet Production Path per Data Contract S2
CURATED_PRODUCTION_PATH = "/data/curated/events"


def build_session_dataset(
    events_df: DataFrame,
    k: int = 5,
    exclude_equal_k: bool = True,
    apply_h02_right_censoring: bool = False,
) -> DataFrame:
    """Build session-level feature and label DataFrame according to Data Contract v1.0 specifications.

    Parameters
    ----------
    events_df : DataFrame
        Curated clickstream events DataFrame containing at least:
        event_time, event_type, product_id, category_code, brand, price, user_session.
        Optionally contains pre-computed event_type_rank and event_sort_key.
    k : int, optional
        Number of prefix events N to observe before making a prediction, default 5 (TBD parameter).
    exclude_equal_k : bool, optional
        Whether to exclude sessions with exactly k events (n_events == k).
        Contract v1.0 Section 9.4 marks n_events == k as Pending Decision #1 (TBD).
        Default True (excludes sessions lacking tail observation window; can be set False).
    apply_h02_right_censoring : bool, optional
        Whether to apply H02 legacy heuristic right-censoring by dropping the dataset's max date.
        Default False (Contract v1.0 Section 9.5 marks final date dropping as non-production).

    Returns
    -------
    DataFrame
        Dataset containing user_session, session_start_date, label, and prefix features.
    """
    logger.info("Starting build_session_dataset with K = %d", k)

    # 1. Clean: drop null, empty, or whitespace user_session, and null event_time
    cleaned_df = events_df.filter(
        F.col("user_session").isNotNull()
        & (F.trim(F.col("user_session")) != "")
        & F.col("event_time").isNotNull()
    )

    # Impute missing brand to 'Unknown' per Curated Contract
    cleaned_df = cleaned_df.withColumn(
        "brand",
        F.when(F.col("brand").isNull() | (F.trim(F.col("brand")) == ""), "Unknown").otherwise(F.trim(F.col("brand"))),
    )

    # 2. Deterministic ordering: (event_time ASC, event_type_rank ASC, event_sort_key ASC)
    # Check if pre-computed event_type_rank and event_sort_key exist in DataFrame
    cols = cleaned_df.columns

    if "event_type_rank" in cols:
        rank_col = F.col("event_type_rank").asc()
    else:
        # Fallback for H02 raw CSV mock tests missing Curated Parquet columns
        rank_col = (
            F.when(F.col("event_type") == "view", 1)
            .when(F.col("event_type") == "cart", 2)
            .when(F.col("event_type") == "remove_from_cart", 3)
            .when(F.col("event_type") == "purchase", 4)
            .otherwise(99)
        ).asc()

    if "event_sort_key" in cols:
        sort_key_col = F.col("event_sort_key").asc()
    else:
        # Fallback: strictly deterministic fallback without violating sort key principle
        sort_key_col = F.sha2(
            F.concat_ws("||", F.col("event_time").cast("string"), F.col("event_type"), F.col("product_id").cast("string")),
            256,
        ).asc()

    # Strict Data Contract Ordering: NO product_id tie-breaker
    order_win = Window.partitionBy("user_session").orderBy(
        F.col("event_time").asc(),
        rank_col,
        sort_key_col,
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

    # 4. Filter sessions based on contract exclusion rules:
    # Rule 1: Short sessions n_events < k must be dropped
    length_filter = (F.col("n_events") > k) if exclude_equal_k else (F.col("n_events") >= k)

    # Rule 2: Early purchase in head (rn <= k) must be dropped
    eligibility_cond = length_filter & (F.col("has_purchase_head") == 0)

    # Optional H02 heuristic right-censoring if explicitly requested
    if apply_h02_right_censoring:
        max_date_row = cleaned_df.select(F.max(F.to_date("event_time"))).collect()
        if max_date_row and max_date_row[0][0]:
            max_date = max_date_row[0][0]
            logger.info("Applying H02 right-censoring: excluding date %s", max_date)
            eligibility_cond = eligibility_cond & (F.col("session_start_date") != max_date)

    valid_sessions = session_summary.filter(eligibility_cond).select(
        "user_session", "session_start_date", "label"
    )

    # 5. Extract 15 features strictly from Head (rn <= k)
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

    # Assemble final session dataset
    final_dataset = valid_sessions.join(head_features, on="user_session", how="inner")
    return final_dataset


def run_eda_and_save(events_df: DataFrame, k: int, output_path: str) -> DataFrame:
    """Run exploratory dataset statistics and save session dataset to Parquet."""
    dataset = build_session_dataset(events_df, k=k)
    logger.info("Writing session dataset to Parquet: %s", output_path)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    dataset.write.mode("overwrite").parquet(output_path)
    logger.info("Session dataset successfully saved to %s", output_path)
    return dataset


def main() -> None:
    """CLI entrypoint for building session dataset from Curated Parquet."""
    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="BuildSessionDataset", cfg=cfg)

    ml_cfg = cfg.get("ml", {})
    curated_path = cfg.get("hdfs", {}).get("curated", {}).get("events", CURATED_PRODUCTION_PATH)
    output_path = ml_cfg.get("session_features_path", "data/processed/ml/session_features")
    k = int(ml_cfg.get("k_events", 5))

    logger.info("Loading curated events from Parquet: %s", curated_path)
    curated_df = spark.read.parquet(curated_path)

    run_eda_and_save(events_df=curated_df, k=k, output_path=output_path)
    spark.stop()


if __name__ == "__main__":
    main()
