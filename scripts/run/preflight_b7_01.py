"""Preflight (E3) for P4-B-B7-01: check the Curated input WITHOUT scanning rows.

Checks, all metadata-only:
- The path exists and contains Parquet part files (listing via inputFiles()).
- The schema contains every column B6 needs (schema only, no data read).
- Reports the number of files (the official Curated count is 774, but this is informational).

It does not count rows, does not create /data/ml, and does not write anything.

Exit 0: preflight PASS. Exit 1: input missing or schema incomplete (run must stop).
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark, load_config  # noqa: E402

logger = get_logger("preflight_b7_01")

REQUIRED = [
    "user_session",
    "event_time",
    "event_type",
    "event_type_rank",
    "event_sort_key",
    "product_id",
    "category_id",
    "brand",
    "price",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--curated-path", default="/data/curated/events")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    result = {
        "check": "E3_hdfs_curated_preflight",
        "curated_path": args.curated_path,
        "checked_utc": datetime.now(timezone.utc).isoformat(),
        "rows_scanned": False,
    }
    code = 1
    spark = None
    try:
        cfg = load_config(os.path.join(ROOT, "configs", "config.example.yaml"))
        spark = get_spark(app_name="P4B-B7-01-Preflight", cfg=cfg)
        spark.sparkContext.setLogLevel("ERROR")
        df = spark.read.parquet(args.curated_path)
        files = df.inputFiles()
        missing = [c for c in REQUIRED if c not in df.columns]
        result["parquet_files"] = len(files)
        result["schema_columns"] = df.columns
        result["missing_required_columns"] = missing
        result["spark_version"] = spark.version
        if not files:
            result.update(status="FAIL", reason="no parquet files under curated path")
        elif missing:
            result.update(status="FAIL", reason=f"schema missing columns: {missing}")
        else:
            result.update(status="PASS", reason="path readable, parquet files present, schema complete")
            code = 0
    except Exception as exc:  # noqa: BLE001 - recorded as evidence, then exit code 1
        result.update(status="FAIL", reason=f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}")
        logger.error("Preflight failed: %s", exc)
    finally:
        result["exit_code"] = code
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        if spark is not None:
            spark.stop()
    print(json.dumps({k: result.get(k) for k in ("status", "parquet_files", "exit_code")}, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())
