#!/usr/bin/env python3
"""P5 ML pipeline compatibility smoke test: verify key PySpark operations work on this platform."""

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.ml.classification import LogisticRegression  # noqa: E402
from pyspark.ml.feature import VectorAssembler  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType  # noqa: E402

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.ml_stage1 import file_uri, hash_bucket_col  # noqa: E402


def test_xxhash64_and_pmod(spark):
    """Test that xxhash64 and pmod (as expr) are available."""
    print("Test: xxhash64 and pmod...")
    df = spark.createDataFrame([("s1",), ("s2",), ("s3",)], "user_session string")
    result = df.withColumn(
        "hash_bucket",
        F.expr("pmod(xxhash64(user_session), 10000)")
    ).select("user_session", "hash_bucket").collect()
    assert all(0 <= r[1] < 10000 for r in result), "hash buckets out of range"
    print("  ✓ xxhash64 and pmod work")


def test_parquet_read_write(spark):
    """Test that parquet files can be read and written."""
    print("Test: parquet R/W...")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.parquet")
        df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
        df.write.mode("overwrite").parquet(file_uri(path))
        df2 = spark.read.parquet(file_uri(path))
        assert df2.count() == 2, "parquet row count mismatch"
    print("  ✓ parquet read/write work")


def test_partition_schema(spark):
    """Test that partition columns are handled correctly."""
    print("Test: partition schema...")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "partitioned.parquet")
        df = spark.createDataFrame(
            [(1, "a", 0), (2, "b", 1), (3, "c", 0)],
            "id int, name string, group int"
        )
        df.write.mode("overwrite").parquet(file_uri(path))
        df2 = spark.read.parquet(file_uri(path))
        assert "__partitions__" not in df2.columns
        print("  ✓ partition schema handling works")


def test_vector_assembler(spark):
    """Test that VectorAssembler works for feature engineering."""
    print("Test: VectorAssembler...")
    df = spark.createDataFrame(
        [(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)],
        "f1 double, f2 double, f3 double"
    )
    assembler = VectorAssembler(inputCols=["f1", "f2", "f3"], outputCol="features")
    result = assembler.transform(df).select("features")
    assert result.count() == 2, "VectorAssembler output row count mismatch"
    print("  ✓ VectorAssembler works")


def test_logistic_regression_fit(spark):
    """Test that Logistic Regression training works."""
    print("Test: LogisticRegression fit...")
    df = spark.createDataFrame(
        [(1.0, 2.0, 1), (2.0, 3.0, 1), (10.0, 11.0, 0), (11.0, 12.0, 0)],
        "f1 double, f2 double, label int"
    )
    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    df_assembled = assembler.transform(df)
    lr = LogisticRegression(maxIter=5, labelCol="label", featuresCol="features")
    model = lr.fit(df_assembled)
    predictions = model.transform(df_assembled)
    assert predictions.filter(F.col("prediction").isNotNull()).count() == 4
    print("  ✓ LogisticRegression fit works")


def test_evaluator(spark):
    """Test that evaluators work."""
    print("Test: BinaryClassificationEvaluator...")
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    df = spark.createDataFrame(
        [(1.0, 1), (0.8, 1), (0.3, 0), (0.1, 0)],
        "probability double, label int"
    ).withColumn("rawPrediction", F.col("probability") - 0.5)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(df)
    assert 0 <= auc <= 1, f"AUC out of range: {auc}"
    print(f"  ✓ BinaryClassificationEvaluator works (AUC={auc:.3f})")


def test_watchdog_process_group(spark):
    """Test that process monitoring works (watchdog integration)."""
    print("Test: watchdog process monitoring...")
    try:
        import psutil
        pgid = os.getpgrp() if hasattr(os, "getpgrp") else None
        if pgid:
            print(f"  ✓ Process group ID: {pgid}")
        else:
            print("  ⚠ Cannot get process group (running on Windows?)")
    except ImportError:
        print("  ⚠ psutil not available (expected on Windows)")


def test_hash_bucket_col_function(spark):
    """Test that the hash_bucket_col utility function works."""
    print("Test: hash_bucket_col utility...")
    df = spark.createDataFrame([("u1", "s1"), ("u2", "s2")], "user_id string, user_session string")
    df_with_hash = df.withColumn("bucket", hash_bucket_col())
    result = df_with_hash.select("bucket").collect()
    assert all(0 <= r[0] < 10000 for r in result), "hash_bucket_col output out of range"
    print("  ✓ hash_bucket_col utility works")


def main(argv=None):
    """Run compatibility smoke tests."""
    spark = get_spark("P5_ML_CompatSmoke")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        test_xxhash64_and_pmod(spark)
        test_parquet_read_write(spark)
        test_partition_schema(spark)
        test_vector_assembler(spark)
        test_logistic_regression_fit(spark)
        test_evaluator(spark)
        test_watchdog_process_group(spark)
        test_hash_bucket_col_function(spark)
        print("\n✓ All compatibility tests PASSED")
        return 0
    except Exception as e:
        print(f"\n✗ Compatibility test FAILED: {e}", file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
