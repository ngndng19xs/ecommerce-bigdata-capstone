"""B7–B9: Logistic Regression training, B8 threshold tuning, B9 final test & artifacts."""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Feature columns: 9 numeric + deterministic last_event_type flags
NUMERIC_FEATURES = [
    "event_count", "purchased_flag", "purchased_count", "view_count", "cart_count",
    "avg_price", "distinct_brands", "avg_price_squared", "price_stddev",
]
CATEGORICAL_FEATURES = [
    "last_event_type_view", "last_event_type_cart", "last_event_type_purchase",
]


class TrainRejectedError(Exception):
    """Training failed due to insufficient data or class imbalance."""
    pass


class ModelFrozenError(Exception):
    """Attempt to retrain or modify a frozen model."""
    pass


def train_b7_lr(spark: SparkSession, train_df: DataFrame, val_df: DataFrame,
               max_iter: int = 20, reg_param: float = 0.1, elastic_net: float = 0.0,
               ) -> Dict[str, Any]:
    """B7: Train Logistic Regression on train_df, validate on val_df.

    Args:
        spark: SparkSession
        train_df: training data with features + label column
        val_df: validation data
        max_iter: max iterations for LR
        reg_param: regularization parameter
        elastic_net: elastic net mixing (0=L2, 1=L1)

    Returns:
        dict with {model_path, metrics, model_metadata}
    """
    if train_df.count() == 0:
        raise TrainRejectedError("train_df is empty")

    # Check class diversity
    label_counts = train_df.groupBy("label").count().collect()
    if len(label_counts) < 2:
        raise TrainRejectedError("train_df has only one class")

    # Impute nulls in numeric features
    imputer = Imputer(inputCols=NUMERIC_FEATURES, outputCols=NUMERIC_FEATURES, strategy="median")
    imputed = imputer.fit(train_df).transform(train_df)

    # Assemble features (numeric + categorical flags)
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + CATEGORICAL_FEATURES, outputCol="features_raw")
    assembled = assembler.transform(imputed)

    # Standardize
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    scaled = scaler.fit(assembled).transform(assembled)

    # Train LR
    lr = LogisticRegression(maxIter=max_iter, regParam=reg_param, elasticNetParam=elastic_net,
                           labelCol="label", featuresCol="features")
    model = lr.fit(scaled)

    # Validate
    val_imputed = imputer.fit(train_df).transform(val_df)
    val_assembled = assembler.transform(val_imputed)
    val_scaled = scaler.transform(val_assembled)
    predictions = model.transform(val_scaled)

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(predictions)

    return {
        "model": model,
        "pipeline": {
            "imputer": imputer,
            "assembler": assembler,
            "scaler": scaler,
        },
        "metrics": {"auc": auc},
        "model_metadata": {
            "max_iter": max_iter,
            "reg_param": reg_param,
            "elastic_net": elastic_net,
            "train_rows": train_df.count(),
            "train_label_distribution": {int(r.label): int(r.count) for r in label_counts},
            "validation_auc": auc,
        },
    }


def tune_threshold_b8(spark: SparkSession, val_df: DataFrame, model: PipelineModel,
                     pipeline_dict: Dict[str, Any],
                     thresholds: Optional[List[float]] = None) -> Dict[str, Any]:
    """B8: Grid-search optimal threshold on validation data.

    Args:
        spark: SparkSession
        val_df: validation data
        model: trained LR model
        pipeline_dict: {imputer, assembler, scaler}
        thresholds: list of thresholds to try; default [0.3, 0.4, 0.5, 0.6, 0.7]

    Returns:
        dict with {best_threshold, best_f1, metrics_per_threshold, frozen_manifest}
    """
    if thresholds is None:
        thresholds = [0.3, 0.4, 0.5, 0.6, 0.7]

    # Apply pipeline and get predictions with probability
    imputer = pipeline_dict["imputer"]
    assembler = pipeline_dict["assembler"]
    scaler = pipeline_dict["scaler"]

    val_imputed = imputer.fit(val_df).transform(val_df)
    val_assembled = assembler.transform(val_imputed)
    val_scaled = scaler.transform(val_assembled)
    predictions = model.transform(val_scaled)

    # Extract positive class probability (column 1 of probability vector)
    predictions = predictions.withColumn("prob_positive", F.col("probability")[1])

    metrics_per_threshold = {}
    best_threshold = thresholds[0]
    best_f1 = 0.0

    for threshold in thresholds:
        pred_binary = predictions.withColumn(
            "prediction_at_threshold",
            F.when(F.col("prob_positive") >= threshold, 1).otherwise(0)
        )

        # Compute metrics at this threshold
        tp = pred_binary.filter((F.col("prediction_at_threshold") == 1) & (F.col("label") == 1)).count()
        fp = pred_binary.filter((F.col("prediction_at_threshold") == 1) & (F.col("label") == 0)).count()
        tn = pred_binary.filter((F.col("prediction_at_threshold") == 0) & (F.col("label") == 0)).count()
        fn = pred_binary.filter((F.col("prediction_at_threshold") == 0) & (F.col("label") == 1)).count()

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        metrics_per_threshold[threshold] = {
            "tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "precision": precision, "recall": recall, "f1": f1,
        }

        if f1 > best_f1 or (f1 == best_f1 and threshold < best_threshold):
            best_f1 = f1
            best_threshold = threshold

    return {
        "best_threshold": best_threshold,
        "best_f1": best_f1,
        "metrics_per_threshold": metrics_per_threshold,
        "frozen_manifest": {
            "model_class": "LogisticRegression",
            "threshold": best_threshold,
            "status": "MODEL_AND_THRESHOLD_FROZEN",
        },
    }


def verify_split_no_leakage(spark: SparkSession, train_df: DataFrame, val_df: DataFrame,
                           test_df: DataFrame) -> Dict[str, Any]:
    """B9 gate: Check that Train/Val/Test are disjoint via user_session projection.

    Args:
        spark: SparkSession
        train_df, val_df, test_df: the three splits

    Returns:
        dict with {status, problems}
    """
    problems = []

    sessions_train = {r[0] for r in train_df.select("user_session").distinct().collect() if r[0] is not None}
    sessions_val = {r[0] for r in val_df.select("user_session").distinct().collect() if r[0] is not None}
    sessions_test = {r[0] for r in test_df.select("user_session").distinct().collect() if r[0] is not None}

    if sessions_train & sessions_val:
        problems.append(f"leakage: {len(sessions_train & sessions_val)} sessions in both train and val")
    if sessions_train & sessions_test:
        problems.append(f"leakage: {len(sessions_train & sessions_test)} sessions in both train and test")
    if sessions_val & sessions_test:
        problems.append(f"leakage: {len(sessions_val & sessions_test)} sessions in both val and test")

    if problems:
        return {"status": "LEAKAGE_DETECTED", "problems": problems}
    return {"status": "PASS", "checks": {
        "sessions_train": len(sessions_train),
        "sessions_val": len(sessions_val),
        "sessions_test": len(sessions_test),
    }}


def test_b9_final(spark: SparkSession, test_df: DataFrame, model: PipelineModel,
                 pipeline_dict: Dict[str, Any], threshold: float) -> Dict[str, Any]:
    """B9: Compute final metrics on test split using frozen model + threshold.

    Args:
        spark: SparkSession
        test_df: test data
        model: trained LR model
        pipeline_dict: {imputer, assembler, scaler}
        threshold: frozen threshold from B8

    Returns:
        dict with final metrics
    """
    imputer = pipeline_dict["imputer"]
    assembler = pipeline_dict["assembler"]
    scaler = pipeline_dict["scaler"]

    test_imputed = imputer.fit(test_df).transform(test_df)
    test_assembled = assembler.transform(test_imputed)
    test_scaled = scaler.transform(test_assembled)
    predictions = model.transform(test_scaled)
    predictions = predictions.withColumn("prob_positive", F.col("probability")[1])
    predictions = predictions.withColumn(
        "prediction_at_threshold",
        F.when(F.col("prob_positive") >= threshold, 1).otherwise(0)
    )

    tp = predictions.filter((F.col("prediction_at_threshold") == 1) & (F.col("label") == 1)).count()
    fp = predictions.filter((F.col("prediction_at_threshold") == 1) & (F.col("label") == 0)).count()
    tn = predictions.filter((F.col("prediction_at_threshold") == 0) & (F.col("label") == 0)).count()
    fn = predictions.filter((F.col("prediction_at_threshold") == 0) & (F.col("label") == 1)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0

    return {
        "status": "PROVISIONAL_PENDING_A_REVIEW",
        "threshold": threshold,
        "test_metrics": {
            "tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "precision": precision, "recall": recall, "f1": f1, "accuracy": accuracy,
        },
    }
