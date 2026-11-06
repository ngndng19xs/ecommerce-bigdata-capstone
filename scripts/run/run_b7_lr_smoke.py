"""P4-B-B7-01 smoke run: Logistic Regression on B6 Session Features (train/validation only).

Writes a JSON evidence report even when the run fails, so failures are recorded with
their root cause instead of being lost in terminal output. Exit code 0 = PASS, 1 = FAIL.

Usage (run from repository root on VM B):
    python3 scripts/run/run_b7_lr_smoke.py \
        --input-path /data/ml/smoke/p4_b_b7_01/<run_id>/session_features \
        --output-dir /data/ml/smoke/p4_b_b7_01/<run_id>/model \
        --report-dir reports/P4_B_B7_01/<run_id>
"""

import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark, load_config  # noqa: E402
from src.ml.training.lr_b6_smoke import run_b6_lr_smoke, validate_b6_schema  # noqa: E402

logger = get_logger("run_b7_lr_smoke")


def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        )
        return out.stdout.strip()
    except Exception:  # noqa: BLE001 - evidence field, not control flow
        return "UNKNOWN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="P4-B-B7-01 LR smoke test on B6 features.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-dir", required=True, help="Smoke-only output (model), not official.")
    parser.add_argument("--report-dir", required=True, help="Local directory for smoke_report.json.")
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--reg-param", type=float, default=0.01)
    parser.add_argument("--train-ratio", type=float, default=0.70)
    parser.add_argument("--val-ratio", type=float, default=0.15)
    parser.add_argument("--no-save-model", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc).isoformat()
    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="P4B-B7-01-LRSmoke", cfg=cfg)
    spark.sparkContext.setLogLevel("WARN")

    report = {
        "task": "P4-B-B7-01",
        "scope": "LR smoke on B6 Session Features; train and validation only; test never read",
        "git_commit": _git_commit(),
        "spark_version": spark.version,
        "python_version": sys.version.split()[0],
        "started_utc": started,
        "args": vars(args),
    }
    exit_code = 1
    try:
        dataset_df = spark.read.parquet(args.input_path)
        validate_b6_schema(dataset_df)
        report["input_schema"] = [
            {"name": f.name, "type": f.dataType.simpleString()} for f in dataset_df.schema.fields
        ]

        _, smoke = run_b6_lr_smoke(
            dataset_df=dataset_df,
            output_dir=args.output_dir,
            max_iter=args.max_iter,
            reg_param=args.reg_param,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
            save_model=not args.no_save_model,
            input_path=args.input_path,
        )
        report.update(smoke)
        report["smoke_status"] = "PASS"
        exit_code = 0
    except Exception as exc:  # noqa: BLE001 - recorded as FAIL evidence, then re-signalled via exit code
        report["smoke_status"] = "FAIL"
        report["error_type"] = type(exc).__name__
        report["error_message"] = str(exc).splitlines()[0] if str(exc) else ""
        report["traceback_tail"] = traceback.format_exc().splitlines()[-20:]
        logger.error("Smoke run FAILED: %s", exc)
    finally:
        report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        report["exit_code"] = exit_code
        os.makedirs(args.report_dir, exist_ok=True)
        report_path = os.path.join(args.report_dir, "smoke_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        logger.info("Smoke report written to %s", report_path)
        spark.stop()

    print(json.dumps({k: report.get(k) for k in ("smoke_status", "exit_code", "git_commit")}, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
