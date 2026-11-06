"""Write a SAMPLED B6 Session Features Parquet for the P4-B-B7-01 smoke test.

B6 (ml/session_features.py) had no writer. This script applies the existing B6 logic to
whole sessions chosen by a deterministic hash of user_session, then writes the result.

Safety:
- Sampling is per session (all events of a chosen session are kept), never per row.
- Default sample is 1% of sessions. Full-dataset runs are not blocked here, but must
  be requested explicitly by passing --sample-percent 100 and belong to B6-02 / P5.
- The output must not be an official path; the write uses mode 'errorifexists' so an
  existing directory is never overwritten.
- Curated input is read only. Nothing under the input path is modified.

Usage (run from repository root on VM B):
    python3 scripts/run/run_b6_session_features.py \
        --curated-path /data/curated/events --n 2 --sample-percent 1 \
        --output-path /data/ml/smoke/p4_b_b7_01/<run_id>/session_features \
        --stats-dir reports/P4_B_B7_01/<run_id>
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ml.session_features import build_session_features  # noqa: E402
from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark, load_config  # noqa: E402
from src.ml.training.lr_b6_smoke import (  # noqa: E402
    HASH_BUCKETS,
    assert_smoke_output_path,
    sample_whole_sessions,
)

logger = get_logger("run_b6_session_features")

# Columns B6 needs from Curated (data_contract curated-v1.0 + ranking keys).
REQUIRED_CURATED_COLUMNS = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write sampled B6 Session Features to Parquet.")
    parser.add_argument("--curated-path", default="/data/curated/events")
    parser.add_argument("--n", type=int, required=True, help="Prediction point N (positive integer).")
    parser.add_argument("--sample-percent", type=float, default=0.1, help="Percent of SESSIONS, 0 < p <= 100.")
    parser.add_argument(
        "--allow-large-sample",
        action="store_true",
        help="Required for --sample-percent > 1. Not set by default; full runs need explicit approval.",
    )
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--stats-dir", required=True, help="Local directory for the JSON stats file.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.n < 1:
        raise SystemExit("--n must be a positive integer")
    if not (0.0 < args.sample_percent <= 100.0):
        raise SystemExit("--sample-percent must be in (0, 100]")
    if args.sample_percent > 1.0 and not args.allow_large_sample:
        raise SystemExit("--sample-percent > 1 requires --allow-large-sample (not default)")
    assert_smoke_output_path(args.output_path)

    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="P4B-B7-01-B6Writer", cfg=cfg)
    spark.sparkContext.setLogLevel("ERROR")

    sampled = None
    try:
        raw = spark.read.parquet(args.curated_path)
        missing = [c for c in REQUIRED_CURATED_COLUMNS if c not in raw.columns]
        if missing:
            raise SystemExit(f"Curated input missing B6 columns: {missing}")
        input_files = len(raw.inputFiles())
        logger.info("Curated input: %d parquet files, columns OK", input_files)

        # Whole-session sample of the full Curated source. Materialised ONCE so that the
        # audit, features, counts and write below read the small sample, not the 109M-row source.
        sampled = sample_whole_sessions(raw, args.sample_percent).persist(StorageLevel.MEMORY_AND_DISK)
        sampled_events = int(sampled.count())
        sampled_sessions = int(sampled.select("user_session").distinct().count())
        logger.info("Sample materialised: events=%d sessions=%d", sampled_events, sampled_sessions)
        if sampled_sessions == 0:
            raise SystemExit("Sample is empty; increase --sample-percent or check the source.")

        dataset, audit, invalid_rows = build_session_features(sampled, args.n)
        dataset = dataset.withColumn("prediction_n", F.lit(int(args.n)))

        status_counts = {
            r["status"]: int(r["count"]) for r in audit.groupBy("status").count().collect()
        }
        label_counts = {
            str(r["label"]): int(r["count"])
            for r in dataset.groupBy("label").count().collect()
        }
        dataset_rows = int(dataset.count())
        invalid_count = int(invalid_rows.count())
        if dataset_rows == 0:
            raise SystemExit("B6 produced 0 eligible sessions; refusing to write an empty dataset.")

        logger.info("Writing sampled B6 features to %s (rows=%d)", args.output_path, dataset_rows)
        dataset.write.mode("errorifexists").parquet(args.output_path)

        stats = {
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "curated_path": args.curated_path,
            "curated_input_files": input_files,
            "output_path": args.output_path,
            "prediction_n": args.n,
            "sample_percent": args.sample_percent,
            "sample_scope": (
                "deterministic hash(user_session) mod 10000 < threshold over the WHOLE Curated source; "
                "all events of a selected session are kept; no date filter; sessions crossing date "
                "boundaries are kept whole; sessions truncated by the end of the dataset are not corrected (see F8)"
            ),
            "hash_threshold_of_10000": int(round(args.sample_percent * HASH_BUCKETS / 100.0)),
            "sampled_event_rows": sampled_events,
            "sampled_distinct_sessions": sampled_sessions,
            "spark_version": spark.version,
            "audit_status_counts": status_counts,
            "invalid_session_rows": invalid_count,
            "output_session_rows": dataset_rows,
            "output_label_counts": label_counts,
            "output_columns": dataset.columns,
        }
        os.makedirs(args.stats_dir, exist_ok=True)
        stats_path = os.path.join(args.stats_dir, "b6_writer_stats.json")
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info("Stats written to %s", stats_path)
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        return 0
    finally:
        if sampled is not None:
            sampled.unpersist()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
