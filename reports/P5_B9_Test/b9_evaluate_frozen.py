import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.spark_session import get_spark, load_config
from src.ml.training.lr_b6_smoke import (
    validate_b6_schema,
    validate_split_partition,
)

b7_root = Path(sys.argv[1]).resolve()
b8_root = Path(sys.argv[2]).resolve()
features_root = Path(sys.argv[3]).resolve()
output_root = Path(sys.argv[4]).resolve()

def sha256_tree(root):
    h = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_file():
            rel = path.relative_to(root).as_posix()
            h.update(rel.encode("utf-8"))
            h.update(b"\0")
            with path.open("rb") as f:
                for block in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(block)
    return h.hexdigest()

# GATE 1: Chỉ kiểm tra model và metadata đã đóng băng.
b7 = json.loads(
    (b7_root / "b7_train_report.json").read_text()
)
frozen = json.loads(
    (b8_root / "frozen_threshold.json").read_text()
)
b8 = json.loads(
    (b8_root / "b8_threshold_report.json").read_text()
)

assert b7["status"] == "B7_EXPERIMENT_PASS"
assert b8["status"] == "B8_EXPERIMENT_PASS"
assert frozen["selection_frozen_for_this_run"] is True
assert frozen["prediction_n"] == 3
assert frozen["selection_metric"] == "positive_class_f1"
assert frozen["test_evaluated"] is False

threshold = float(frozen["selected_threshold"])
assert math.isfinite(threshold) and 0 < threshold < 1
assert threshold == float(b8["selected_threshold"])

model_path = b7_root / "model"
assert model_path.resolve() == Path(
    frozen["model_path"]
).resolve()

actual_sha = sha256_tree(model_path)
assert actual_sha == frozen["model_sha256"], (
    "MODEL_SHA256_MISMATCH"
)

print("FROZEN_MODEL_SHA256_PASS", flush=True)
print(f"FROZEN_THRESHOLD={threshold:.2f}", flush=True)

assert not output_root.exists(), "B9 output already exists"
assert (features_root / "_SUCCESS").is_file()

test_path = features_root / "h11_split=test"
assert test_path.is_dir()

# GATE 2: Chỉ sau khi model và threshold đã được xác minh,
# mới bắt đầu đánh giá trên Test.
output_root.mkdir(parents=True)

spark = get_spark(
    app_name="P5-B9-Frozen-Test-Evaluation",
    cfg=load_config("configs/config.example.yaml"),
)

try:
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load("file://" + str(model_path))
    test_df = spark.read.parquet("file://" + str(test_path))

    validate_b6_schema(test_df)
    stats = validate_split_partition(test_df, "test")

    assert stats["total"] == 19297
    assert stats["pos"] == 1561
    assert stats["neg"] == 17736
    assert stats["invalid"] == 0

    invalid_n = test_df.filter(
        F.col("prediction_n").isNull()
        | (F.col("prediction_n") != 3)
    ).count()
    assert invalid_n == 0, "INVALID_PREDICTION_N"

    distinct_sessions = (
        test_df.select("user_session").distinct().count()
    )
    assert distinct_sessions == 19297, "DUPLICATE_TEST_SESSIONS"

    print("B9_TEST_DATA_QA_PASS", flush=True)

    # Transform bằng PipelineModel đã fit từ Train.
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc_roc = evaluator.evaluate(predictions)

    evaluator.setMetricName("areaUnderPR")
    auc_pr = evaluator.evaluate(predictions)

    rows = predictions.select(
        F.col("label").cast("int").alias("label"),
        vector_to_array("probability")[1].alias("score"),
    ).collect()

    records = [
        (float(r["score"]), int(r["label"]))
        for r in rows
    ]

    assert len(records) == 19297
    assert all(
        math.isfinite(score) and 0 <= score <= 1
        and label in (0, 1)
        for score, label in records
    )

finally:
    spark.stop()

# GATE 3: Chỉ dùng threshold đã đóng băng.
tp = fp = tn = fn = 0

for score, label in records:
    pred = int(score >= threshold)
    if pred == 1 and label == 1:
        tp += 1
    elif pred == 1 and label == 0:
        fp += 1
    elif pred == 0 and label == 0:
        tn += 1
    else:
        fn += 1

assert tp + fn == 1561
assert tn + fp == 17736

precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = (
    2 * precision * recall / (precision + recall)
    if precision + recall else 0.0
)
accuracy = (tp + tn) / len(records)
specificity = tn / (tn + fp) if tn + fp else 0.0

metrics = {
    "status": "B9_PROVISIONAL_PASS",
    "acceptance_status": "PENDING_A_REVIEW",
    "evaluation_type": "ONE_TIME_FROZEN_TEST",
    "test_rows": len(records),
    "prediction_n": 3,
    "threshold": threshold,
    "tp": tp,
    "fp": fp,
    "tn": tn,
    "fn": fn,
    "precision": precision,
    "recall": recall,
    "f1": f1,
    "accuracy": accuracy,
    "specificity": specificity,
    "auc_roc": auc_roc,
    "auc_pr": auc_pr,
    "model_sha256": actual_sha,
    "training_performed": False,
    "threshold_retuned_on_test": False,
    "evaluated_utc": datetime.now(timezone.utc).isoformat(),
}

(output_root / "test_metrics.json").write_text(
    json.dumps(metrics, indent=2),
    encoding="utf-8",
)

# Confusion Matrix
matrix = [[tn, fp], [fn, tp]]
fig, ax = plt.subplots(figsize=(6, 5))
im = ax.imshow(matrix)
fig.colorbar(im, ax=ax)
ax.set_xticks([0, 1], ["Predicted 0", "Predicted 1"])
ax.set_yticks([0, 1], ["Actual 0", "Actual 1"])
ax.set_title(f"B9 Test Confusion Matrix (t={threshold:.2f})")

for i in range(2):
    for j in range(2):
        ax.text(
            j, i, str(matrix[i][j]),
            ha="center", va="center",
            color="white" if matrix[i][j] > max(tp, fp, tn, fn)/2
            else "black",
        )

fig.tight_layout()
fig.savefig(output_root / "confusion_matrix.png", dpi=300)
plt.close(fig)

# ROC & Precision-Recall trên Test: chỉ để báo cáo,
# không dùng để chọn lại threshold.
ordered = sorted(records, reverse=True)
P = 1561
N = 17736
running_tp = running_fp = 0

fprs, tprs = [0.0], [0.0]
recalls, precisions = [0.0], [1.0]

for _, group in groupby(ordered, key=lambda x: x[0]):
    for _, label in group:
        if label:
            running_tp += 1
        else:
            running_fp += 1
    fprs.append(running_fp / N)
    tprs.append(running_tp / P)
    recalls.append(running_tp / P)
    precisions.append(
        running_tp / (running_tp + running_fp)
    )

fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(fprs, tprs, label=f"AUC-ROC = {auc_roc:.4f}")
ax.plot([0, 1], [0, 1], "--", label="Random baseline")
ax.set(
    xlabel="False Positive Rate",
    ylabel="True Positive Rate",
    title="B9 - Test ROC Curve",
)
ax.legend()
ax.grid(alpha=0.25)
fig.tight_layout()
fig.savefig(output_root / "test_roc_curve.png", dpi=300)
plt.close(fig)

fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(recalls, precisions, label=f"Spark AUC-PR = {auc_pr:.4f}")
ax.axhline(P / (P + N), linestyle="--", label="Positive rate")
ax.set(
    xlabel="Recall",
    ylabel="Precision",
    title="B9 - Test Precision-Recall Curve",
)
ax.legend()
ax.grid(alpha=0.25)
fig.tight_layout()
fig.savefig(output_root / "test_precision_recall_curve.png", dpi=300)
plt.close(fig)

print(json.dumps(metrics, indent=2), flush=True)
print("B9_PROVISIONAL_PASS", flush=True)
