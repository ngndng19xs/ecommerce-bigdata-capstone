"""P4-B-B7-01: Logistic Regression smoke pipeline on B6 Session Features.

This module is a SMOKE run, not the official B7/B8/B9 pipeline.

Input contract: the output schema of ``ml.session_features.build_session_features``
plus a ``prediction_n`` column written by ``scripts/run/run_b6_session_features.py``.

Guarantees enforced here:
- Only the train and validation date ranges are read. The remaining (test-range)
  dates are never filtered, counted, transformed or evaluated.
- Preprocessing stages and LogisticRegression are fitted on the train set only.
- Class weights are computed from the train set only.
- Any NULL / NaN / infinite numeric value, invalid label, or unexpected categorical
  value raises ValueError instead of being silently dropped or passed to the model.
- Validation is transformed once for shape / finiteness checks and a sanity AUC.
  It is NOT used for threshold selection (that is B8).
- Nothing is written to official model or metrics paths (see assert_smoke_output_path).
"""

import math
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, functions as F

from src.common.logger import get_logger

logger = get_logger("lr_b6_smoke")

# --- B6 schema contract -------------------------------------------------------

B6_REQUIRED_COLUMNS: List[str] = [
    "user_session",
    "label",
    "prefix_start",
    "prediction_time",
    "prediction_n",
    "prefix_duration_seconds",
    "n_view",
    "n_cart",
    "n_remove_from_cart",
    "n_unique_products",
    "n_unique_categories",
    "n_unique_brands",
    "avg_price",
    "max_price",
    "last_event_type",
]

# Numeric prefix features (B6 spec section 2.4). Only prefix-derived values.
NUMERIC_FEATURES: List[str] = [
    "n_view",
    "n_cart",
    "n_remove_from_cart",
    "n_unique_products",
    "n_unique_categories",
    "n_unique_brands",
    "avg_price",
    "max_price",
    "prefix_duration_seconds",
]

CATEGORICAL_FEATURES: List[str] = ["last_event_type"]

# The event at prediction point N can never be purchase for an eligible session
# (purchased_by_n sessions are excluded by B6), so these are the only valid values.
LAST_EVENT_TYPE_VALUES = ("view", "cart", "remove_from_cart")

# Columns that must never enter the feature vector.
FORBIDDEN_FEATURE_COLUMNS = {
    "user_session",
    "label",
    "prefix_start",
    "prediction_time",
    "prediction_n",
    "session_start_date",
    "brands_seen",
    "class_weight",
}

# Official paths that a smoke run must never write to (config/config.example.yaml,
# acceptance docs). Matched as exact path or as a parent directory prefix.
OFFICIAL_OUTPUT_PREFIXES: Tuple[str, ...] = (
    "/data/curated",
    "/data/ml/features",
    "/data/ml/datasets",
    "/data/ml/models",
    "/data/ml/evaluation",
    "/data/analytics",
    "/data/raw",
    "data/processed/ml",
    "results/metrics",
)


HASH_BUCKETS = 10000


def sample_whole_sessions(events: DataFrame, percent: float) -> DataFrame:
    """Keep ALL events of a deterministic subset of sessions (never a subset of rows).

    The decision depends only on user_session, so every event of a selected session is kept
    and no session is cut. Sessions crossing a date boundary are kept whole because the
    filter does not look at event_time. Reproducible: same input and percent give same sessions.
    """
    if not (0.0 < percent <= 100.0):
        raise ValueError("sample percent must be in (0, 100]")
    threshold = int(round(percent * HASH_BUCKETS / 100.0))
    if threshold < 1:
        raise ValueError("sample percent too small for hash buckets; increase percent")
    # pmod is not in the PySpark 3.1.2 Python API (verified in its source). The SQL function is
    # available in Spark 3.1.2 and returns a non-negative result for negative hash values.
    return events.filter(session_bucket_expr() < F.lit(threshold))


def session_bucket_expr():
    """Column expression for the deterministic session bucket in [0, HASH_BUCKETS)."""
    return F.expr(f"pmod(xxhash64(user_session), {HASH_BUCKETS})")


def assert_smoke_output_path(path: str) -> None:
    """Raise ValueError if ``path`` points at an official output location."""
    norm = path.replace("\\", "/").rstrip("/")
    for prefix in OFFICIAL_OUTPUT_PREFIXES:
        if norm == prefix or norm.startswith(prefix + "/"):
            raise ValueError(
                f"Refusing to write smoke output into official path '{path}' (matches '{prefix}')."
            )


# --- Contract validation ------------------------------------------------------


def validate_b6_schema(df: DataFrame) -> None:
    """Fail fast if the input is not the B6 Session Features schema."""
    missing = [c for c in B6_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"B6 schema contract violated, missing columns: {missing}")


def _label_stats(df: DataFrame) -> Dict[str, int]:
    row = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("pos"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("neg"),
    ).collect()[0]
    total = int(row["total"] or 0)
    pos = int(row["pos"] or 0)
    neg = int(row["neg"] or 0)
    return {"total": total, "pos": pos, "neg": neg, "invalid": total - pos - neg}


def _invalid_numeric_counts(df: DataFrame) -> Dict[str, int]:
    exprs = []
    for col in NUMERIC_FEATURES:
        x = F.col(col).cast("double")
        bad = x.isNull() | F.isnan(x) | (F.abs(x) == F.lit(float("inf")))
        exprs.append(F.sum(F.when(bad, 1).otherwise(0)).alias(col))
    row = df.agg(*exprs).collect()[0]
    return {col: int(row[col] or 0) for col in NUMERIC_FEATURES}


def _invalid_categorical_count(df: DataFrame) -> int:
    allowed = F.col("last_event_type").isin(list(LAST_EVENT_TYPE_VALUES))
    return int(df.filter(~allowed | F.col("last_event_type").isNull()).count())


def validate_split_partition(df: DataFrame, name: str) -> Dict[str, int]:
    """Validate labels, numeric values and categories of one partition (train or val).

    Raises ValueError on any violation. Returns label stats for the report.
    """
    stats = _label_stats(df)
    if stats["invalid"] > 0:
        raise ValueError(f"{name}: {stats['invalid']} rows have label outside {{0, 1}} or NULL.")

    invalid_numeric = _invalid_numeric_counts(df)
    bad = {c: n for c, n in invalid_numeric.items() if n > 0}
    if bad:
        raise ValueError(f"{name}: NULL/NaN/infinite numeric feature values found: {bad}")

    bad_cat = _invalid_categorical_count(df)
    if bad_cat > 0:
        raise ValueError(
            f"{name}: {bad_cat} rows have last_event_type outside {LAST_EVENT_TYPE_VALUES} or NULL."
        )
    return stats


def compute_train_class_weight(train_neg: int, train_pos: int) -> float:
    """Class weight for label=1 computed from the TRAIN set only: neg / pos."""
    if train_pos == 0:
        raise ValueError("Train set contains no positive labels (label=1); cannot fit smoke model.")
    if train_neg == 0:
        raise ValueError("Train set contains no negative labels (label=0); cannot fit smoke model.")
    weight = float(train_neg) / float(train_pos)
    if not math.isfinite(weight) or weight <= 0:
        raise ValueError(f"Invalid class weight computed from train: {weight}")
    return weight


# --- Split (train/validation only) -------------------------------------------


def split_train_val_dates(
    distinct_dates: Sequence[Any],
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
) -> Tuple[List[Any], List[Any], int]:
    """Choose train and validation dates with the same cutoff rule as the official split.

    Returns (train_dates, val_dates, n_dates_not_read). The last dates are the test range
    of the official 70/15/15 split; they are returned only as a count and never read.
    """
    dates = sorted(distinct_dates)
    total = len(dates)
    if total < 2:
        raise ValueError(f"Smoke split needs at least 2 distinct session_start_date values, got {total}.")

    n_train = max(1, int(total * train_ratio))
    n_val = max(1, round(total * val_ratio))
    if n_train + n_val > total:
        n_train = total - 1
        n_val = 1

    train_dates = dates[:n_train]
    val_dates = dates[n_train : n_train + n_val]
    n_not_read = total - n_train - n_val
    return train_dates, val_dates, n_not_read


# --- Pipeline ----------------------------------------------------------------


def build_b6_feature_stages() -> List[Any]:
    """Preprocessing stages for B6 features. Fitted on train only via Pipeline.fit.

    - StringIndexer / OneHotEncoder with handleInvalid='keep': unseen categories map to
      a dedicated bucket instead of failing at transform time.
    - VectorAssembler with handleInvalid='error': any NULL/NaN that slipped through
      validation raises instead of entering the vector.
    - StandardScaler(withMean=False): keeps sparse vectors; fitted on train only.
    """
    indexer = StringIndexer(
        inputCols=CATEGORICAL_FEATURES,
        outputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
        handleInvalid="keep",
    )
    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
        outputCols=[f"{c}_vec" for c in CATEGORICAL_FEATURES],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + [f"{c}_vec" for c in CATEGORICAL_FEATURES],
        outputCol="features_unscaled",
        handleInvalid="error",
    )
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=False,
        withStd=True,
    )
    return [indexer, encoder, assembler, scaler]


def _validation_output_checks(val_preds: DataFrame, expected_rows: int) -> Dict[str, Any]:
    prob = vector_to_array("probability")
    feats = vector_to_array("features")
    row = val_preds.agg(
        F.count(F.lit(1)).alias("n_out"),
        F.sum(F.when(F.size(prob) != 2, 1).otherwise(0)).alias("bad_prob_size"),
        F.sum(
            F.when(
                F.exists(prob, lambda x: F.isnan(x) | (x < 0.0) | (x > 1.0)),
                1,
            ).otherwise(0)
        ).alias("bad_prob_value"),
        F.sum(
            F.when(
                F.exists(feats, lambda x: F.isnan(x) | (F.abs(x) == F.lit(float("inf")))),
                1,
            ).otherwise(0)
        ).alias("bad_features"),
        F.sum(F.when(F.col("prediction").isNull(), 1).otherwise(0)).alias("null_prediction"),
    ).collect()[0]

    result = {
        "validation_rows_in": expected_rows,
        "validation_rows_out": int(row["n_out"] or 0),
        "bad_prob_size": int(row["bad_prob_size"] or 0),
        "bad_prob_value": int(row["bad_prob_value"] or 0),
        "bad_features": int(row["bad_features"] or 0),
        "null_prediction": int(row["null_prediction"] or 0),
    }
    if result["validation_rows_out"] != expected_rows:
        raise ValueError(
            f"Validation row count changed after transform: in={expected_rows}, out={result['validation_rows_out']}"
        )
    for key in ("bad_prob_size", "bad_prob_value", "bad_features", "null_prediction"):
        if result[key] != 0:
            raise ValueError(f"Validation output check failed: {key}={result[key]}")
    return result


def run_b6_lr_smoke(
    dataset_df: DataFrame,
    output_dir: str,
    max_iter: int = 50,
    reg_param: float = 0.01,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    save_model: bool = True,
    input_path: Optional[str] = None,
) -> Tuple[PipelineModel, Dict[str, Any]]:
    """Fit LR on train, transform validation once, and return a smoke report.

    Parameters
    ----------
    dataset_df : DataFrame
        B6 Session Features with the schema validated by ``validate_b6_schema``.
    output_dir : str
        Smoke-only output directory (model is saved under ``output_dir/lr_smoke_model``).
    save_model : bool
        Save the smoke PipelineModel. Never saved to official paths.

    Returns
    -------
    (model, report)
        Fitted PipelineModel and a JSON-serialisable report. ``model`` is always returned;
        it is also saved to disk when ``save_model`` is True.
    """
    assert_smoke_output_path(output_dir)
    if input_path:
        norm_in = input_path.replace("\\", "/").rstrip("/")
        norm_out = output_dir.replace("\\", "/").rstrip("/")
        if norm_out == norm_in or norm_out.startswith(norm_in + "/"):
            raise ValueError("Smoke output directory must not be the input directory or inside it.")

    validate_b6_schema(dataset_df)

    prediction_ns = [
        int(r[0]) for r in dataset_df.select("prediction_n").distinct().collect() if r[0] is not None
    ]
    if len(prediction_ns) != 1:
        raise ValueError(f"Expected exactly one prediction_n value in input, found {prediction_ns}.")
    prediction_n = prediction_ns[0]

    df = dataset_df.withColumn("session_start_date", F.to_date(F.col("prefix_start")))

    distinct_dates = [r[0] for r in df.select("session_start_date").distinct().collect()]
    train_dates, val_dates, n_not_read = split_train_val_dates(distinct_dates, train_ratio, val_ratio)
    logger.info(
        "Smoke split: train dates=%d (%s..%s), val dates=%d (%s..%s), dates not read=%d",
        len(train_dates), train_dates[0], train_dates[-1],
        len(val_dates), val_dates[0], val_dates[-1],
        n_not_read,
    )

    # Only train and validation partitions are referenced below. The test range is never filtered.
    train_df = df.filter(F.col("session_start_date").isin(train_dates))
    val_df = df.filter(F.col("session_start_date").isin(val_dates))

    train_stats = validate_split_partition(train_df, "train")
    val_stats = validate_split_partition(val_df, "validation")

    n_train_distinct = int(train_df.select("user_session").distinct().count())
    if n_train_distinct != train_stats["total"]:
        raise ValueError("Train set has duplicate user_session rows; B6 must produce one row per session.")
    n_val_distinct = int(val_df.select("user_session").distinct().count())
    if n_val_distinct != val_stats["total"]:
        raise ValueError("Validation set has duplicate user_session rows; B6 must produce one row per session.")

    null_session = int(train_df.filter(F.col("user_session").isNull()).count()) + int(
        val_df.filter(F.col("user_session").isNull()).count()
    )
    if null_session:
        raise ValueError(f"Found {null_session} rows with NULL user_session in train/validation.")

    pos_weight = compute_train_class_weight(train_stats["neg"], train_stats["pos"])
    logger.info(
        "Train class weight (label=1) = neg/pos = %d / %d = %.6f",
        train_stats["neg"], train_stats["pos"], pos_weight,
    )

    train_weighted = train_df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1, F.lit(pos_weight)).otherwise(F.lit(1.0)),
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=int(max_iter),
        regParam=float(reg_param),
    )
    stages = build_b6_feature_stages()
    pipeline = Pipeline(stages=stages + [lr])

    logger.info("Fitting smoke Pipeline on train set only (%d rows)...", train_stats["total"])
    model: PipelineModel = pipeline.fit(train_weighted)

    lr_model = model.stages[-1]
    coefs = [float(v) for v in lr_model.coefficients.toArray()]
    intercept = float(lr_model.intercept)
    if not all(math.isfinite(c) for c in coefs) or not math.isfinite(intercept):
        raise ValueError("Logistic Regression produced non-finite coefficients or intercept.")

    indexer_labels = {
        col: list(labels)
        for col, labels in zip(CATEGORICAL_FEATURES, model.stages[0].labelsArray)
    }

    model_path = None
    if save_model:
        model_path = os.path.join(output_dir, "lr_smoke_model").replace("\\", "/")
        assert_smoke_output_path(model_path)
        model.write().overwrite().save(model_path)
        logger.info("Smoke model saved to %s (not an official model).", model_path)

    # Validation is transformed exactly once here. No test-range data is involved.
    val_preds = model.transform(val_df)
    output_checks = _validation_output_checks(val_preds, expected_rows=val_stats["total"])

    auc_val: Optional[float] = None
    if val_stats["pos"] > 0 and val_stats["neg"] > 0:
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC"
        )
        auc_val = round(float(evaluator.evaluate(val_preds)), 6)
    else:
        logger.warning("Validation set lacks one class; AUC sanity check skipped.")

    report: Dict[str, Any] = {
        "input_path": input_path,
        "input_columns": dataset_df.columns,
        "prediction_n": prediction_n,
        "split": {
            "train_ratio": train_ratio,
            "val_ratio": val_ratio,
            "train_dates": [str(d) for d in train_dates],
            "val_dates": [str(d) for d in val_dates],
            "dates_not_read": n_not_read,
            "distinct_session_dates_total": len(distinct_dates),
        },
        "train": {
            "sessions": train_stats["total"],
            "pos": train_stats["pos"],
            "neg": train_stats["neg"],
            "pos_rate": round(train_stats["pos"] / train_stats["total"], 6),
        },
        "validation": {
            "sessions": val_stats["total"],
            "pos": val_stats["pos"],
            "neg": val_stats["neg"],
            "pos_rate": round(val_stats["pos"] / val_stats["total"], 6),
            "auc_sanity_only": auc_val,
            **output_checks,
        },
        "class_weight_label1": pos_weight,
        "lr": {"max_iter": int(max_iter), "reg_param": float(reg_param)},
        "pipeline_stages": [type(s).__name__ for s in model.stages],
        "feature_columns_numeric": NUMERIC_FEATURES,
        "feature_columns_categorical": CATEGORICAL_FEATURES,
        "forbidden_feature_columns": sorted(FORBIDDEN_FEATURE_COLUMNS),
        "indexer_labels": indexer_labels,
        "coefficients_count": len(coefs),
        "coefficient_l2_norm": round(math.sqrt(sum(c * c for c in coefs)), 10),
        "intercept": intercept,
        "model_path": model_path,
    }
    return model, report
