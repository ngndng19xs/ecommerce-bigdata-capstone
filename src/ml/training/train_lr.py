"""Logistic Regression training and evaluation pipeline for session purchase prediction."""

import os
from typing import Any, Dict, Optional, Tuple
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import DataFrame, functions as F
from src.common.logger import get_logger
from src.common.spark_session import get_spark, load_config
from src.ml.dataset.split import split_by_time
from src.ml.evaluation.metrics import evaluate_model
from src.ml.features.feature_pipeline import get_feature_pipeline_stages

logger = get_logger("train_lr")


def train_logistic_regression(
    dataset_df: DataFrame,
    cfg: Dict[str, Any],
) -> Tuple[PipelineModel, Dict[str, Any]]:
    """Train weighted Logistic Regression with Spark MLlib Pipeline and evaluate on Val/Test sets.

    Parameters
    ----------
    dataset_df : DataFrame
        Session dataset containing features, session_start_date, and label.
    cfg : Dict[str, Any]
        Configuration loaded from YAML.

    Returns
    -------
    Tuple[PipelineModel, Dict[str, Any]]
        Fitted PipelineModel and complete metrics dictionary.
    """
    ml_cfg = cfg.get("ml", {})
    split_cfg = ml_cfg.get("split", {"train": 0.70, "val": 0.15, "test": 0.15})
    lr_cfg = ml_cfg.get("lr", {"max_iter": 50, "reg_param": 0.01})
    threshold_grid_cfg = ml_cfg.get("threshold_grid", {"start": 0.05, "stop": 0.95, "step": 0.01})
    model_save_path = ml_cfg.get("model_path", "data/processed/ml/lr_model")
    metrics_path = ml_cfg.get("metrics_path", "results/metrics/lr_metrics.json")
    k_events = int(ml_cfg.get("k_events", 5))

    # 1. Split dataset chronologically
    logger.info("Splitting dataset by time (train/val/test)...")
    train_df, val_df, test_df, split_metadata = split_by_time(dataset_df, ratios=split_cfg)

    # 2. Compute class weights solely from train set to address class imbalance
    train_neg = split_metadata["train"]["total_sessions"] - split_metadata["train"]["pos_label_count"]
    train_pos = split_metadata["train"]["pos_label_count"]
    pos_weight = float(train_neg / train_pos) if train_pos > 0 else 1.0
    logger.info(
        "Train set class weight for label=1: neg/pos = %d / %d = %.4f",
        train_neg,
        train_pos,
        pos_weight,
    )

    train_weighted = train_df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1, F.lit(pos_weight)).otherwise(F.lit(1.0)),
    )

    # 3. Construct Pipeline: Feature preprocessing stages + LogisticRegression classifier
    feature_stages = get_feature_pipeline_stages()
    lr_classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=int(lr_cfg.get("max_iter", 50)),
        regParam=float(lr_cfg.get("reg_param", 0.01)),
    )

    pipeline = Pipeline(stages=feature_stages + [lr_classifier])

    # 4. Fit pipeline on train set
    logger.info("Fitting Logistic Regression Pipeline on Train set...")
    model: PipelineModel = pipeline.fit(train_weighted)
    logger.info("Model training completed.")

    # 5. Save model to disk
    logger.info("Saving trained PipelineModel to: %s", model_save_path)
    os.makedirs(os.path.dirname(os.path.abspath(model_save_path)), exist_ok=True)
    model.write().overwrite().save(model_save_path)
    logger.info("Model saved successfully.")

    # 6. Transform Validation and Test sets
    logger.info("Generating predictions on Validation and Test sets...")
    val_preds = model.transform(val_df)
    test_preds = model.transform(test_df)

    # 7. Evaluate model performance and save metrics
    metrics = evaluate_model(
        val_preds=val_preds,
        test_preds=test_preds,
        threshold_grid_cfg=threshold_grid_cfg,
        split_metadata=split_metadata,
        k_events=k_events,
        metrics_output_path=metrics_path,
    )

    return model, metrics


def main() -> None:
    """CLI entrypoint for training and evaluating Logistic Regression."""
    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="TrainLogisticRegression", cfg=cfg)

    session_features_path = cfg.get("ml", {}).get(
        "session_features_path", "data/processed/ml/session_features"
    )
    logger.info("Loading session features from Parquet: %s", session_features_path)
    dataset_df = spark.read.parquet(session_features_path)

    train_logistic_regression(dataset_df=dataset_df, cfg=cfg)
    spark.stop()


if __name__ == "__main__":
    main()
