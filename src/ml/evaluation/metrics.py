"""Evaluation metrics module for session purchase classification."""

import json
import os
from typing import Any, Dict, List, Optional, Tuple
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, functions as F
from src.common.logger import get_logger

logger = get_logger("metrics")


def evaluate_threshold_grid(
    val_preds: DataFrame,
    threshold_grid_cfg: Dict[str, float],
) -> Tuple[float, float, Dict[float, Dict[str, float]]]:
    """Find the optimal threshold on validation set maximizing F1 using a single Spark aggregation.

    Parameters
    ----------
    val_preds : DataFrame
        Validation predictions with 'probability' and 'label' columns.
    threshold_grid_cfg : Dict[str, float]
        Dictionary containing 'start', 'stop', and 'step'.

    Returns
    -------
    Tuple[float, float, Dict[float, Dict[str, float]]]
        optimal_threshold, best_f1_val, and full validation grid results.
    """
    start = float(threshold_grid_cfg.get("start", 0.05))
    stop = float(threshold_grid_cfg.get("stop", 0.95))
    step = float(threshold_grid_cfg.get("step", 0.01))

    # Generate discrete threshold grid values rounded to 4 decimals
    thresholds: List[float] = []
    curr = start
    while curr <= stop + 1e-6:
        thresholds.append(round(curr, 4))
        curr += step

    df_p1 = val_preds.withColumn("p1", vector_to_array("probability")[1])

    # Build single aggregation query across all thresholds
    agg_exprs = []
    for idx, t in enumerate(thresholds):
        agg_exprs.extend(
            [
                F.sum(
                    F.when((F.col("label") == 1) & (F.col("p1") >= t), 1).otherwise(0)
                ).alias(f"tp_{idx}"),
                F.sum(
                    F.when((F.col("label") == 0) & (F.col("p1") >= t), 1).otherwise(0)
                ).alias(f"fp_{idx}"),
                F.sum(
                    F.when((F.col("label") == 1) & (F.col("p1") < t), 1).otherwise(0)
                ).alias(f"fn_{idx}"),
                F.sum(
                    F.when((F.col("label") == 0) & (F.col("p1") < t), 1).otherwise(0)
                ).alias(f"tn_{idx}"),
            ]
        )

    agg_row = df_p1.agg(*agg_exprs).collect()[0]

    best_threshold = thresholds[0]
    best_f1 = -1.0
    grid_results = {}

    for idx, t in enumerate(thresholds):
        tp = int(agg_row[f"tp_{idx}"] or 0)
        fp = int(agg_row[f"fp_{idx}"] or 0)
        fn = int(agg_row[f"fn_{idx}"] or 0)
        tn = int(agg_row[f"tn_{idx}"] or 0)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (
            (2.0 * precision * recall) / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        grid_results[t] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }

        if f1 > best_f1:
            best_f1 = f1
            best_threshold = t

    logger.info(
        "Threshold grid search on validation: Best Threshold = %.2f with F1 = %.4f",
        best_threshold,
        best_f1,
    )
    return best_threshold, best_f1, grid_results


def compute_metrics_at_threshold(
    df_preds: DataFrame,
    threshold: float,
) -> Dict[str, Any]:
    """Compute precision, recall, F1, and confusion matrix at a fixed threshold using Spark aggregation.

    Parameters
    ----------
    df_preds : DataFrame
        Predictions DataFrame.
    threshold : float
        Decision threshold.

    Returns
    -------
    Dict[str, Any]
        Dictionary with tp, fp, fn, tn, precision, recall, f1.
    """
    df_p1 = df_preds.withColumn("p1", vector_to_array("probability")[1])

    agg_row = df_p1.agg(
        F.sum(
            F.when((F.col("label") == 1) & (F.col("p1") >= threshold), 1).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when((F.col("label") == 0) & (F.col("p1") >= threshold), 1).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when((F.col("label") == 1) & (F.col("p1") < threshold), 1).otherwise(0)
        ).alias("fn"),
        F.sum(
            F.when((F.col("label") == 0) & (F.col("p1") < threshold), 1).otherwise(0)
        ).alias("tn"),
    ).collect()[0]

    tp = int(agg_row["tp"] or 0)
    fp = int(agg_row["fp"] or 0)
    fn = int(agg_row["fn"] or 0)
    tn = int(agg_row["tn"] or 0)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (
        (2.0 * precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    return {
        "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def evaluate_model(
    val_preds: DataFrame,
    test_preds: DataFrame,
    threshold_grid_cfg: Dict[str, float],
    split_metadata: Dict[str, Any],
    k_events: int = 5,
    metrics_output_path: Optional[str] = "results/metrics/lr_metrics.json",
) -> Dict[str, Any]:
    """Execute complete model evaluation, calculate AUC and threshold metrics, and save JSON report.

    Parameters
    ----------
    val_preds : DataFrame
        Validation predictions.
    test_preds : DataFrame
        Test predictions.
    threshold_grid_cfg : Dict[str, float]
        Grid search configuration.
    split_metadata : Dict[str, Any]
        Metadata detailing session counts and dates per partition.
    k_events : int
        Prefix length K.
    metrics_output_path : Optional[str]
        File path to save JSON metrics.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing all evaluation metrics.
    """
    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="label",
        metricName="areaUnderROC",
    )

    auc_val = round(float(evaluator.evaluate(val_preds)), 4)
    auc_test = round(float(evaluator.evaluate(test_preds)), 4)

    logger.info("ROC-AUC: Validation = %.4f | Test = %.4f", auc_val, auc_test)

    # Sanity checks for potential data leakage or model malfunction
    if auc_test < 0.55:
        logger.warning(
            "WARNING: Test AUC (%.4f) is lower than 0.55! Possible model underfitting or defective features.",
            auc_test,
        )
    elif auc_test > 0.98:
        logger.warning(
            "WARNING: Test AUC (%.4f) exceeds 0.98! Strongly suspected data leakage.",
            auc_test,
        )

    # 1. Select optimal threshold strictly on validation set
    optimal_threshold, f1_val, _ = evaluate_threshold_grid(val_preds, threshold_grid_cfg)

    # 2. Evaluate performance on test set using optimal threshold selected from validation
    test_metrics = compute_metrics_at_threshold(test_preds, optimal_threshold)

    results: Dict[str, Any] = {
        "AUC_val": auc_val,
        "AUC_test": auc_test,
        "threshold": optimal_threshold,
        "F1_val": round(f1_val, 4),
        "F1_test": test_metrics["f1"],
        "precision_test": test_metrics["precision"],
        "recall_test": test_metrics["recall"],
        "confusion": test_metrics["confusion"],
        "split_dates": {
            "train": {
                "start": split_metadata["train"]["start_date"],
                "end": split_metadata["train"]["end_date"],
            },
            "val": {
                "start": split_metadata["val"]["start_date"],
                "end": split_metadata["val"]["end_date"],
            },
            "test": {
                "start": split_metadata["test"]["start_date"],
                "end": split_metadata["test"]["end_date"],
            },
        },
        "k_events": k_events,
        "session_counts": {
            "train": split_metadata["train"]["total_sessions"],
            "val": split_metadata["val"]["total_sessions"],
            "test": split_metadata["test"]["total_sessions"],
        },
        "pos_label_rates": {
            "train": round(split_metadata["train"]["pos_rate"], 4),
            "val": round(split_metadata["val"]["pos_rate"], 4),
            "test": round(split_metadata["test"]["pos_rate"], 4),
        },
    }

    if metrics_output_path:
        os.makedirs(os.path.dirname(os.path.abspath(metrics_output_path)), exist_ok=True)
        with open(metrics_output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        logger.info("Evaluation metrics successfully saved to %s", metrics_output_path)

    return results
