"""Unit and end-to-end tests for P4-B-B7-01 LR smoke on B6 Session Features.

Synthetic events are turned into Session Features by the real B6 function
(ml.session_features.build_session_features), so these tests also cover B6 -> B7 compatibility.

Layout (N = 2, 6 consecutive days, official-style 70/15/15 date cutoff):
- Days 1-4 : train. Day 5 : validation. Day 6 : test range (holdout, never read).
- 12 sessions per day; session i=11 is purchase-before-N and is excluded by B6.
- Label 1 sessions: view, cart, view, purchase (purchase after N).
- Label 0 sessions: view,cart,view,view / view,remove_from_cart,view (day 5 only) / view,view,cart.
- Day 6 contains a session with NULL prices. If the pipeline ever touched the holdout range,
  the NULL would be detected and the run would fail.
"""

import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.session_features import build_session_features  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.ml.training.lr_b6_smoke import (  # noqa: E402
    FORBIDDEN_FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    assert_smoke_output_path,
    compute_train_class_weight,
    run_b6_lr_smoke,
    split_train_val_dates,
    validate_b6_schema,
)

PREDICTION_N = 2
RANK = {"view": 1, "cart": 2, "remove_from_cart": 3, "purchase": 4}
HOLDOUT_POISON_SESSION = "d6_s00"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = get_spark("TestB7LRSmoke")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _pattern(day: int, i: int, drop_train_positives: bool) -> list:
    if i == 11:
        return ["cart", "purchase", "view"]  # purchase at event 2 -> purchased_by_n, excluded
    r = i % 3
    if r == 0:
        if drop_train_positives and day <= 4:
            return ["view", "cart", "view", "view"]
        return ["view", "cart", "view", "purchase"]
    if r == 1:
        if day == 5:
            return ["view", "remove_from_cart", "view"]  # last event at N = remove_from_cart (val only)
        return ["view", "cart", "view", "view"]
    return ["view", "view", "cart"]


def make_events(
    spark: SparkSession,
    drop_train_positives: bool = False,
    null_price_sessions: tuple = (HOLDOUT_POISON_SESSION,),
) -> DataFrame:
    schema = StructType(
        [
            StructField("user_session", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_type_rank", IntegerType(), True),
            StructField("event_sort_key", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    )
    rows = []
    counter = 0
    for day in range(1, 7):
        for i in range(12):
            sid = f"d{day}_s{i:02d}"
            base = datetime(2019, 11, day, 10, 0, 0) + timedelta(minutes=i)
            for step, kind in enumerate(_pattern(day, i, drop_train_positives)):
                counter += 1
                price = None if sid in null_price_sessions else 10.0 + i
                rows.append(
                    (
                        sid,
                        base + timedelta(seconds=10 * step),
                        kind,
                        RANK[kind],
                        "{:064x}".format(counter),
                        f"P{i % 4}",
                        f"C{i % 3}",
                        "A" if i % 2 == 0 else "",
                        price,
                    )
                )
    return spark.createDataFrame(rows, schema)


def make_b6_dataset(spark: SparkSession, **kwargs) -> DataFrame:
    events = make_events(spark, **kwargs)
    dataset, _, _ = build_session_features(events, PREDICTION_N)
    return dataset.withColumn("prediction_n", F.lit(PREDICTION_N))


@pytest.fixture(scope="module")
def dataset(spark: SparkSession) -> DataFrame:
    return make_b6_dataset(spark).cache()


@pytest.fixture(scope="module")
def smoke_run(spark: SparkSession, dataset: DataFrame, tmp_path_factory):
    out = tmp_path_factory.mktemp("b7_smoke_run")
    return run_b6_lr_smoke(dataset, output_dir=str(out), save_model=True, input_path=None)


# --- TC03 / TC04: contract ----------------------------------------------------


def test_tc03_b6_schema_contract_accepts_b6_output(dataset):
    validate_b6_schema(dataset)  # no exception


def test_tc03_b6_schema_contract_rejects_missing_column(dataset):
    with pytest.raises(ValueError, match="missing columns"):
        validate_b6_schema(dataset.drop("n_cart"))


def test_tc04_non_binary_label_raises(spark, dataset, tmp_path):
    bad = dataset.withColumn(
        "label", F.when(F.col("user_session") == "d5_s01", F.lit(2)).otherwise(F.col("label"))
    )
    with pytest.raises(ValueError, match="outside"):
        run_b6_lr_smoke(bad, output_dir=str(tmp_path / "o"), save_model=False)


# --- Split and output guards --------------------------------------------------


def test_split_train_val_dates_uses_ratio_and_reports_unread_dates():
    dates = [f"2019-11-0{d}" for d in range(1, 7)]
    train, val, not_read = split_train_val_dates(dates, 0.70, 0.15)
    assert train == dates[:4]
    assert val == dates[4:5]
    assert not_read == 1


def test_split_train_val_dates_rejects_single_date():
    with pytest.raises(ValueError, match="at least 2"):
        split_train_val_dates(["2019-11-01"])


@pytest.mark.parametrize(
    "path",
    ["/data/ml/models/lr", "/data/curated/events", "results/metrics/lr_metrics.json", "data/processed/ml/lr_model"],
)
def test_official_output_paths_are_refused(path):
    with pytest.raises(ValueError, match="official path"):
        assert_smoke_output_path(path)


def test_smoke_output_path_allowed_outside_official_locations():
    assert_smoke_output_path("/data/ml/smoke/p4_b_b7_01/run1/model")
    assert_smoke_output_path("reports/P4_B_B7_01/run1")


def test_output_dir_inside_input_is_refused(dataset, tmp_path):
    with pytest.raises(ValueError, match="input directory"):
        run_b6_lr_smoke(
            dataset,
            output_dir="/data/ml/smoke/x/session_features/sub",
            save_model=False,
            input_path="/data/ml/smoke/x/session_features",
        )


# --- End-to-end smoke ---------------------------------------------------------


def test_tc12_tc13_fit_and_validation_transform_succeed(smoke_run):
    model, report = smoke_run
    assert report["pipeline_stages"] == [
        "StringIndexerModel",
        "OneHotEncoderModel",
        "VectorAssembler",
        "StandardScalerModel",
        "LogisticRegressionModel",
    ]
    val = report["validation"]
    assert val["validation_rows_out"] == val["validation_rows_in"] == val["sessions"]
    assert val["bad_prob_size"] == 0
    assert val["bad_prob_value"] == 0
    assert val["bad_features"] == 0
    assert val["null_prediction"] == 0
    # Version-independent: the coefficient vector must match the LR model's own feature width.
    # (One-hot width with handleInvalid='keep' + dropLast differs between Spark 3.1 and 4.x,
    # so the formula is not asserted.)
    lr_model = model.stages[-1]
    assert report["coefficients_count"] == lr_model.numFeatures
    assert math.isfinite(report["intercept"])


def test_tc05_train_contains_both_classes(smoke_run):
    _, report = smoke_run
    assert report["train"]["pos"] > 0
    assert report["train"]["neg"] > 0


def test_tc14_holdout_range_is_never_read(smoke_run):
    """Day 6 holds a NULL-price session. If read, the run would fail or change counts."""
    _, report = smoke_run
    assert report["split"]["dates_not_read"] == 1
    assert report["split"]["train_dates"] == [f"2019-11-0{d}" for d in range(1, 5)]
    assert report["split"]["val_dates"] == ["2019-11-05"]
    # 11 eligible sessions per day: train 4 days = 44, validation 1 day = 11, holdout excluded.
    assert report["train"]["sessions"] == 44
    assert report["validation"]["sessions"] == 11
    assert "test" not in report
    assert "test" not in report["split"]


def test_tc08_preprocessing_fitted_on_train_only(smoke_run):
    """remove_from_cart occurs as last event only in validation, so train vocabulary must lack it."""
    model, report = smoke_run
    labels = set(model.stages[0].labelsArray[0])
    assert labels == {"view", "cart"}
    assert "remove_from_cart" not in report["indexer_labels"]["last_event_type"]
    # Unseen validation category must not crash transform and must keep row count.
    assert report["validation"]["validation_rows_out"] == report["validation"]["sessions"]


def test_tc09_tc10_feature_columns_exclude_ids_labels_and_future_info(smoke_run):
    model, report = smoke_run
    assembler = model.stages[2]
    inputs = set(assembler.getInputCols())
    assert "user_session" not in inputs
    assert "label" not in inputs
    assert not inputs & FORBIDDEN_FEATURE_COLUMNS
    for col in ("session_length", "purchased_by_n", "event_idx", "prediction_time", "prefix_start"):
        assert col not in inputs
    assert set(NUMERIC_FEATURES).issubset(inputs)
    lr_stage = model.stages[-1]
    assert isinstance(lr_stage, LogisticRegressionModel)
    assert lr_stage.getFeaturesCol() == "features"
    assert lr_stage.getLabelCol() == "label"


def test_tc11_class_weight_from_train_only(spark, dataset, smoke_run):
    _, report = smoke_run
    # Independent recount from B6 output, restricted to train days 1-4 (sessions named d1_..d4_).
    train_rows = dataset.filter(F.col("user_session").rlike("^d[1-4]_"))
    pos = train_rows.filter(F.col("label") == 1).count()
    neg = train_rows.filter(F.col("label") == 0).count()
    expected = neg / pos
    assert report["class_weight_label1"] == pytest.approx(expected)
    assert report["class_weight_label1"] == pytest.approx(report["train"]["neg"] / report["train"]["pos"])


def test_tc15_repeatability_same_input_same_coefficients(spark, dataset):
    _, first = run_b6_lr_smoke(dataset, output_dir="/data/ml/smoke/unit_tc15/a", save_model=False)
    _, second = run_b6_lr_smoke(dataset, output_dir="/data/ml/smoke/unit_tc15/b", save_model=False)
    assert first["coefficient_l2_norm"] == pytest.approx(second["coefficient_l2_norm"], abs=1e-9)
    assert first["intercept"] == pytest.approx(second["intercept"], abs=1e-9)


# --- Negative cases -----------------------------------------------------------


def test_tc06_train_single_class_raises_explicitly(spark, tmp_path):
    ds = make_b6_dataset(spark, drop_train_positives=True, null_price_sessions=())
    with pytest.raises(ValueError, match="no positive labels"):
        run_b6_lr_smoke(ds, output_dir=str(tmp_path / "o"), save_model=False)


def test_tc07_null_numeric_in_train_raises(spark, tmp_path):
    ds = make_b6_dataset(spark, null_price_sessions=("d1_s01",))
    with pytest.raises(ValueError, match="avg_price"):
        run_b6_lr_smoke(ds, output_dir=str(tmp_path / "o"), save_model=False)


def test_tc07_nan_numeric_in_train_raises(spark, tmp_path):
    ds = make_b6_dataset(spark, null_price_sessions=()).withColumn(
        "avg_price",
        F.when(F.col("user_session") == "d1_s02", F.lit(float("nan"))).otherwise(F.col("avg_price")),
    )
    with pytest.raises(ValueError, match="NaN|avg_price"):
        run_b6_lr_smoke(ds, output_dir=str(tmp_path / "o"), save_model=False)


def test_compute_train_class_weight_edge_cases():
    assert compute_train_class_weight(train_neg=7, train_pos=4) == pytest.approx(1.75)
    with pytest.raises(ValueError, match="no positive"):
        compute_train_class_weight(train_neg=5, train_pos=0)
    with pytest.raises(ValueError, match="no negative"):
        compute_train_class_weight(train_neg=0, train_pos=5)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
