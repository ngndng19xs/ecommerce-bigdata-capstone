"""Feature engineering and preprocessing pipeline for session classification."""

from typing import List
from pyspark.ml.base import Estimator, Transformer
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)

NUMERIC_FEATURE_COLS = [
    "view_count",
    "cart_count",
    "remove_count",
    "has_cart",
    "unique_product_count",
    "unique_category_count",
    "unique_brand_count",
    "avg_price",
    "max_price",
    "min_price",
    "interaction_duration",
    "hour_of_day",
    "day_of_week",
]

CATEGORICAL_COLS = [
    "last_event_type",
    "prev_event_type",
]


def get_feature_pipeline_stages() -> List[Estimator | Transformer]:
    """Build and return feature preprocessing stages.

    Stages:
    1. StringIndexer for last_event_type and prev_event_type (handleInvalid='keep')
    2. OneHotEncoder for indexed event types (handleInvalid='keep')
    3. VectorAssembler combining numeric features and one-hot encoded vectors
    4. StandardScaler (withMean=False, withStd=True) to normalize feature scale while
       preserving sparse vector representations.

    Returns
    -------
    List[Union[Estimator, Transformer]]
        List of PySpark ML Pipeline stages (classifier excluded).
    """
    indexer = StringIndexer(
        inputCols=CATEGORICAL_COLS,
        outputCols=[f"{col}_idx" for col in CATEGORICAL_COLS],
        handleInvalid="keep",
    )

    encoder = OneHotEncoder(
        inputCols=[f"{col}_idx" for col in CATEGORICAL_COLS],
        outputCols=[f"{col}_vec" for col in CATEGORICAL_COLS],
        handleInvalid="keep",
    )

    assembler_inputs = NUMERIC_FEATURE_COLS + [f"{col}_vec" for col in CATEGORICAL_COLS]
    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="features_unscaled",
        handleInvalid="keep",
    )

    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=False,
        withStd=True,
    )

    return [indexer, encoder, assembler, scaler]
