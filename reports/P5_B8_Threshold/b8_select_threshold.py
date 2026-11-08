import csv
import hashlib
import json
import math
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


b7_root = Path(sys.argv[1])
out = Path(sys.argv[2])

scores_path = b7_root / "evidence" / "validation_scores.csv"
report_path = b7_root / "b7_train_report.json"
summary_path = b7_root / "evidence" / "validation_chart_summary.json"
model_path = b7_root / "model"


def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


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


# Kiểm tra nguồn B7 trước khi tạo kết quả.
for path in (scores_path, report_path, summary_path):
    if not path.is_file():
        raise RuntimeError(f"Missing input: {path}")

if not (model_path / "metadata" / "part-00000").is_file():
    raise RuntimeError("Saved B7 PipelineModel missing")

b7 = json.loads(report_path.read_text(encoding="utf-8"))
summary = json.loads(summary_path.read_text(encoding="utf-8"))

assert b7["status"] == "B7_EXPERIMENT_PASS"
assert b7["prediction_n"] == 3
assert b7["test_evaluated"] is False

print("===== LOAD VALIDATION SCORES =====", flush=True)

records = []

with scores_path.open("r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)

    assert reader.fieldnames == [
        "probability_positive", "label"
    ], "Unexpected CSV schema"

    for row in reader:
        score = float(row["probability_positive"])
        label = int(row["label"])

        if not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Invalid probability")

        if label not in (0, 1):
            raise ValueError("Invalid label")

        records.append((score, label))


total = len(records)
positive = sum(label for _, label in records)
negative = total - positive

assert total == 37848
assert positive == 2755
assert negative == 35093

assert summary["validation_rows"] == total
assert summary["positive_rows"] == positive
assert summary["negative_rows"] == negative

print(f"VALIDATION_ROWS={total}", flush=True)
print(f"POSITIVE={positive}", flush=True)
print(f"NEGATIVE={negative}", flush=True)
print("VALIDATION_INPUT_QA_PASS", flush=True)


print("===== THRESHOLD GRID SEARCH =====", flush=True)

metrics = []

# 0.05 ... 0.95, bước 0.01
for i in range(5, 96):
    threshold = i / 100.0

    tp = fp = tn = fn = 0

    for score, label in records:
        prediction = 1 if score >= threshold else 0

        if prediction == 1 and label == 1:
            tp += 1
        elif prediction == 1 and label == 0:
            fp += 1
        elif prediction == 0 and label == 0:
            tn += 1
        else:
            fn += 1

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )

    accuracy = (tp + tn) / total

    metrics.append({
        "threshold": round(threshold, 2),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
        "predicted_positive": tp + fp,
    })


# Tối đa hóa F1; nếu bằng nhau thì ưu tiên threshold nhỏ.
best = max(
    metrics,
    key=lambda row: (row["f1"], -row["threshold"])
)

assert len(metrics) == 91
assert best["tp"] + best["fn"] == positive
assert best["tn"] + best["fp"] == negative

print(f"BEST_THRESHOLD={best['threshold']:.2f}")
print(f"BEST_PRECISION={best['precision']:.6f}")
print(f"BEST_RECALL={best['recall']:.6f}")
print(f"BEST_F1={best['f1']:.6f}")


# Không ghi đè kết quả từ các lần chạy trước.
out.mkdir(parents=True, exist_ok=False)

# 1. CSV chứa chỉ số của từng threshold.
csv_path = out / "threshold_metrics.csv"

with csv_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(metrics[0]))
    writer.writeheader()
    writer.writerows(metrics)


# 2. Biểu đồ threshold.
x = [r["threshold"] for r in metrics]

fig, ax = plt.subplots(figsize=(9, 5.5))

for key, name in [
    ("precision", "Precision"),
    ("recall", "Recall"),
    ("f1", "F1-score"),
]:
    ax.plot(
        x,
        [r[key] for r in metrics],
        label=name,
        linewidth=2,
    )

ax.axvline(
    best["threshold"],
    linestyle="--",
    label=f"Best threshold = {best['threshold']:.2f}",
)

ax.set(
    xlabel="Classification threshold",
    ylabel="Metric value",
    title="B8 - Validation Threshold Selection",
    xlim=(0.05, 0.95),
    ylim=(0, 1.02),
)

ax.grid(alpha=0.25)
ax.legend()
fig.tight_layout()
fig.savefig(out / "threshold_curve.png", dpi=300)
plt.close(fig)


# 3. Đóng băng ngưỡng cho lần thực nghiệm này.
frozen = {
    "task": "P5-B8",
    "status": "PROVISIONAL_PENDING_A_REVIEW",
    "selection_frozen_for_this_run": True,
    "model_path": str(model_path),
    "model_sha256": sha256_tree(model_path),
    "validation_scores_path": str(scores_path),
    "validation_scores_sha256": sha256_file(scores_path),
    "prediction_n": 3,
    "selection_metric": "positive_class_f1",
    "tie_break": "smaller_threshold",
    "threshold_grid": {
        "start": 0.05,
        "stop": 0.95,
        "step": 0.01,
    },
    "selected_threshold": best["threshold"],
    "validation_metrics": best,
    "test_evaluated": False,
    "created_utc": datetime.now(timezone.utc).isoformat(),
}

(out / "frozen_threshold.json").write_text(
    json.dumps(frozen, indent=2, ensure_ascii=False),
    encoding="utf-8"
)


# 4. Báo cáo B8.
b8_report = {
    "status": "B8_EXPERIMENT_PASS",
    "acceptance_status": "PENDING_A_REVIEW",
    "validation_rows": total,
    "validation_positive": positive,
    "validation_negative": negative,
    "thresholds_evaluated": len(metrics),
    "selection_metric": "positive_class_f1",
    "selected_threshold": best["threshold"],
    "best_validation_metrics": best,
    "b7_validation_auc_roc": b7["validation_auc_roc"],
    "b7_validation_auc_pr": b7["validation_auc_pr"],
    "test_access": "NONE",
}

(out / "b8_threshold_report.json").write_text(
    json.dumps(b8_report, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

# Lưu kèm đúng mã nguồn đã tạo kết quả.
shutil.copy2(__file__, out / "b8_select_threshold.py")

print("===== B8 OUTPUT =====")
print(json.dumps(b8_report, indent=2))
print("B8_EXPERIMENT_PASS", flush=True)
