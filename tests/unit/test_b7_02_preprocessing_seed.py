"""P4-B-B7-02: preprocessing pipeline and seed/repeatability tests.

Tests the REAL stage builder used by the B7 smoke path (build_b6_feature_stages) and the
real split helper. Version-dependent behaviour (unknown/null one-hot encoding with
handleInvalid='keep' and dropLast) is checked through invariants, not a hardcoded formula,
because Spark 3.1.2 and Spark 4.x differ there.

Test IDs match docs/qa/P4_B_B7_02_acceptance.md.
"""

import inspect
import random
import re
import statistics
import sys
from pathlib import Path

import pytest
import yaml
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.training import lr_b6_smoke as smoke  # noqa: E402
from src.ml.training.lr_b6_smoke import (  # noqa: E402
    CATEGORICAL_FEATURES,
    FORBIDDEN_FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    build_b6_feature_stages,
    split_train_val_dates,
)

LONG_COLS = {"n_view", "n_cart", "n_remove_from_cart", "n_unique_products",
             "n_unique_categories", "n_unique_brands", "prefix_duration_seconds"}
DOUBLE_COLS = {"avg_price", "max_price"}
KNOWN = ("view", "cart")


@pytest.fixture(scope="module")
def spark():
    session = get_spark("TestB7PreprocessingSeed")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _schema():
    fields = [StructField("user_session", StringType(), True), StructField("last_event_type", StringType(), True)]
    for c in NUMERIC_FEATURES:
        fields.append(StructField(c, DoubleType() if c in DOUBLE_COLS else LongType(), True))
    return StructType(fields)


def _row(i, cat, scale=1, session_prefix="s"):
    """Deterministic numeric values; distinct per column so feature order is observable."""
    values = [
        (i % 7 + 1) * scale,           # n_view
        (i % 5 + 2) * scale,           # n_cart
        (i % 3) * scale,               # n_remove_from_cart
        (i % 4 + 1) * scale,           # n_unique_products
        (i % 2 + 1) * scale,           # n_unique_categories
        (i % 6 + 1) * scale,           # n_unique_brands
        float(10 + i % 5) * scale,     # avg_price
        float(20 + i % 9) * scale,     # max_price
        (30 + i % 11) * scale,         # prefix_duration_seconds
    ]
    return (f"{session_prefix}{i}", cat) + tuple(values)


def train_df(spark):
    rows = [_row(i, KNOWN[i % 2]) for i in range(60)]
    return spark.createDataFrame(rows, _schema())


def val_df(spark):
    # Validation-only categories and large numeric values: detects fitting on validation.
    cats = ["view", "cart", "remove_from_cart", "unseen_category", None]
    rows = [_row(1000 + i, cats[i % 5], scale=1000, session_prefix="v") for i in range(25)]
    return spark.createDataFrame(rows, _schema())


@pytest.fixture(scope="module")
def fitted(spark):
    return Pipeline(stages=build_b6_feature_stages()).fit(train_df(spark))


def _vec(v):
    return [float(x) for x in v.toArray()] if hasattr(v, "toArray") else list(v)


# --- TC01-TC04: StringIndexer ------------------------------------------------


def test_tc01_indexer_labels_learned_from_train_only(fitted, spark):
    labels_before = list(fitted.stages[0].labelsArray[0])
    assert set(labels_before) == set(KNOWN), labels_before
    assert "remove_from_cart" not in labels_before
    assert "unseen_category" not in labels_before
    fitted.transform(val_df(spark)).collect()
    assert list(fitted.stages[0].labelsArray[0]) == labels_before, "validation transform changed indexer labels"


def test_tc02_unknown_category_keeps_rows_and_is_not_mapped_to_known(fitted, spark):
    val = val_df(spark)
    out = fitted.transform(val)
    assert out.count() == val.count(), "rows silently dropped"
    labels = list(fitted.stages[0].labelsArray[0])
    unknown_rows = out.filter(F.col("last_event_type") == "unseen_category").select("last_event_type_idx").collect()
    assert unknown_rows, "fixture must contain unknown category rows"
    assert all(int(r[0]) == len(labels) for r in unknown_rows), "unknown must map to keep bucket index"


def test_tc03_null_category_keeps_rows(fitted, spark):
    val = val_df(spark)
    out = fitted.transform(val)
    assert out.count() == val.count()
    null_rows = out.filter(F.col("last_event_type").isNull()).select("last_event_type_idx").collect()
    assert null_rows, "fixture must contain null category rows"
    assert all(int(r[0]) == len(fitted.stages[0].labelsArray[0]) for r in null_rows)


def test_tc04_known_category_mapping_stable_across_transforms(fitted, spark):
    def mapping(df):
        out = fitted.transform(df).select("last_event_type", "last_event_type_idx").distinct().collect()
        return {r[0]: int(r[1]) for r in out if r[0] in KNOWN}

    m_train = mapping(train_df(spark))
    m_val = mapping(val_df(spark))
    m_again = mapping(train_df(spark))
    assert m_train == m_val == m_again
    assert sorted(m_train.values()) == list(range(len(KNOWN)))


# --- TC05-TC07: OneHotEncoder and encoder invariants -------------------------


def test_tc05_vector_dimension_consistent_and_fixed_after_fit(fitted, spark):
    """Every row of train and validation gets the same vector width, and the width equals
    numeric count + one-hot width observed on the same fitted encoder (version-independent)."""
    def widths(df):
        out = fitted.transform(df)
        return {r[0] for r in out.select(F.size(vector_to_array("features_unscaled")).alias("n")).collect()}, \
               {r[0] for r in out.select(F.size(vector_to_array("last_event_type_vec")).alias("w")).collect()}

    unscaled_train, onehot_train = widths(train_df(spark))
    unscaled_val, onehot_val = widths(val_df(spark))
    assert len(unscaled_train) == 1 and unscaled_train == unscaled_val, (unscaled_train, unscaled_val)
    assert len(onehot_train) == 1 and onehot_train == onehot_val, (onehot_train, onehot_val)
    assert unscaled_train.pop() == len(NUMERIC_FEATURES) + onehot_train.pop()


def test_tc06_encoder_config_and_unknown_vector_distinct_from_known(fitted, spark):
    enc = fitted.stages[1]
    assert enc.getOrDefault("handleInvalid") == "keep"
    assert enc.getOrDefault("dropLast") is True
    out = fitted.transform(val_df(spark))
    known_vecs = {}
    for r in out.filter(F.col("last_event_type").isin(list(KNOWN))).select(
            "last_event_type", vector_to_array("last_event_type_vec").alias("v")).collect():
        known_vecs.setdefault(r[0], tuple(r[1]))
    unknown = out.filter(F.col("last_event_type") == "unseen_category").select(
        vector_to_array("last_event_type_vec").alias("v")).collect()
    for r in unknown:
        v = tuple(r[0])
        assert sum(1 for x in v if x != 0) <= 1, "unknown must not light more than one slot"
        assert v not in known_vecs.values(), "unknown encoded identically to a known category"


def test_tc07_encoder_metadata_not_refit_on_validation(fitted, spark):
    before_labels = list(fitted.stages[0].labelsArray[0])
    before_sizes = list(fitted.stages[1].categorySizes)
    fitted.transform(val_df(spark)).collect()
    assert list(fitted.stages[0].labelsArray[0]) == before_labels
    assert list(fitted.stages[1].categorySizes) == before_sizes


# --- TC08-TC11: VectorAssembler and leakage ----------------------------------


def test_tc08_assembler_inputs_are_exact_pipeline_features(fitted):
    assembler = fitted.stages[2]
    assert list(assembler.getInputCols()) == NUMERIC_FEATURES + ["last_event_type_vec"]
    assert list(CATEGORICAL_FEATURES) == ["last_event_type"]


def test_tc09_feature_order_is_positional(fitted, spark):
    row = _row(7, "cart", scale=1)
    df = spark.createDataFrame([row], _schema())
    unscaled = _vec(fitted.transform(df).select("features_unscaled").first()[0])
    expected = [float(v) for v in row[2:2 + len(NUMERIC_FEATURES)]]
    assert unscaled[:len(NUMERIC_FEATURES)] == pytest.approx(expected), "numeric order differs from pipeline order"


def test_tc10_no_forbidden_column_enters_features(fitted):
    inputs = set(fitted.stages[2].getInputCols())
    assert not inputs & FORBIDDEN_FEATURE_COLUMNS, inputs & FORBIDDEN_FEATURE_COLUMNS
    assert "user_session" not in inputs and "label" not in inputs


def test_tc11_null_numeric_is_rejected_not_passed_to_model(fitted, spark):
    rows = [_row(1, "view")]
    rows[0] = rows[0][:8] + (None,) + rows[0][9:]  # null avg_price
    df = spark.createDataFrame(rows, _schema())
    with pytest.raises(Exception):
        fitted.transform(df).collect()


# --- TC12-TC13: StandardScaler and persistence -------------------------------


def test_tc12_scaler_statistics_are_train_only(fitted, spark):
    train_unscaled = [
        _vec(r[0]) for r in fitted.transform(train_df(spark)).select("features_unscaled").collect()
    ]
    val_unscaled = [
        _vec(r[0]) for r in fitted.transform(val_df(spark)).select("features_unscaled").collect()
    ]
    std_model = list(fitted.stages[3].std.toArray())
    for j, s in enumerate(std_model):
        col_train = [v[j] for v in train_unscaled]
        assert s == pytest.approx(statistics.stdev(col_train), rel=1e-6, abs=1e-9), f"feature {j} not train std"
        combined = statistics.stdev(col_train + [v[j] for v in val_unscaled])
        # Fixture sanity: validation must be able to change the statistic, otherwise the test is blind.
        assert abs(combined - s) > 1e-6, f"feature {j}: fixture does not separate train and train+val std"


def test_tc13_save_load_consistency(fitted, spark, tmp_path):
    path = str(tmp_path / "pipeline_model")
    fitted.write().overwrite().save(path)
    loaded = PipelineModel.load(path)
    val = val_df(spark)
    a = [_vec(r[0]) for r in fitted.transform(val).select("features").orderBy("user_session").collect()]
    b = [_vec(r[0]) for r in loaded.transform(val).select("features").orderBy("user_session").collect()]
    assert len(a) == len(b)
    assert all(len(x) == len(y) for x, y in zip(a, b))
    for x, y in zip(a, b):
        assert x == pytest.approx(y, abs=1e-9)


# --- TC14-TC18: seed, repeatability and configuration ------------------------

B7_PATH_FILES = [
    "src/ml/training/lr_b6_smoke.py",
    "scripts/run/run_b6_session_features.py",
    "scripts/run/run_b7_lr_smoke.py",
    "ml/session_features.py",
    "ml/features.py",
]
RANDOM_TOKENS = re.compile(r"randomSplit|\.sample\(|\brand\(|\brandn\(|\bshuffle\(|import random\b|from random\b|monotonically_increasing_id")


def test_tc14_randomness_audit_on_b7_path():
    hits = {}
    for rel in B7_PATH_FILES:
        text = (ROOT / rel).read_text(encoding="utf-8")
        for n, line in enumerate(text.splitlines(), 1):
            code = line.split("#", 1)[0]
            if RANDOM_TOKENS.search(code):
                hits.setdefault(rel, []).append((n, line.strip()))
    assert not hits, f"unexpected randomness on B7 path: {hits}"


def test_tc15_chronological_split_deterministic_and_order_independent():
    dates = [f"2019-11-{d:02d}" for d in range(1, 11)]
    a = split_train_val_dates(dates)
    b = split_train_val_dates(list(reversed(dates)))
    c = split_train_val_dates(dates)
    assert a == b == c
    train, val, _ = a
    assert max(train) < min(val)


def test_tc15_membership_identical_across_partitionings(spark):
    from datetime import date
    rows = [(f"sess{i}", date(2019, 11, (i % 6) + 1)) for i in range(120)]
    schema = StructType([StructField("user_session", StringType(), True), StructField("d", DateType(), True)])
    df = spark.createDataFrame(rows, schema)
    train, val, _ = split_train_val_dates(sorted({r[1] for r in rows}))
    keys = {}
    for parts in (2, 7):
        d = df.repartition(parts)
        keys[parts] = (
            sorted(r[0] for r in d.filter(F.col("d").isin(train)).collect()),
            sorted(r[0] for r in d.filter(F.col("d").isin(val)).collect()),
        )
    assert keys[2] == keys[7]
    assert keys[2][0] and keys[2][1], "both partitions must be non-empty"


def test_tc16_no_hidden_python_randomness_in_fit(spark):
    """Different Python-side random seeds must not change the fitted model (no seed is used)."""
    df = train_df(spark).withColumn("label", F.when(F.col("last_event_type") == "cart", 1).otherwise(0))
    coefs = []
    for seed in (1, 999):
        random.seed(seed)
        m = Pipeline(stages=build_b6_feature_stages() + [
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01)
        ]).fit(df)
        coefs.append(list(m.stages[-1].coefficients.toArray()))
    assert coefs[0] == pytest.approx(coefs[1], abs=1e-12)


def test_tc17_repeatability_across_partitionings(spark):
    from pyspark.ml.classification import LogisticRegression
    base = train_df(spark).withColumn("label", F.when(F.col("last_event_type") == "cart", 1).otherwise(0))
    results = []
    for parts in (2, 7):
        d = base.repartition(parts)
        m = Pipeline(stages=build_b6_feature_stages() + [
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=50, regParam=0.01)
        ]).fit(d)
        results.append((
            list(m.stages[0].labelsArray[0]),
            list(m.stages[3].std.toArray()),
            list(m.stages[-1].coefficients.toArray()),
        ))
    assert results[0][0] == results[1][0]
    assert results[0][1] == pytest.approx(results[1][1], rel=1e-9)
    assert results[0][2] == pytest.approx(results[1][2], rel=1e-6, abs=1e-9)


def test_tc18_hyperparameter_config_matches_code_defaults():
    cfg = yaml.safe_load((ROOT / "configs" / "b7_smoke_hyperparams.example.yaml").read_text(encoding="utf-8"))
    sig = inspect.signature(smoke.run_b6_lr_smoke).parameters
    assert cfg["lr"]["max_iter"] == sig["max_iter"].default
    assert cfg["lr"]["reg_param"] == sig["reg_param"].default
    assert cfg["split"]["train_ratio"] == sig["train_ratio"].default
    assert cfg["split"]["val_ratio"] == sig["val_ratio"].default
    assert cfg["prediction_n"] == 2
    assert cfg["sampling"]["sample_percent_default"] == 0.1
    assert cfg["seed_policy"]["random_seed_used_by_code"] is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
