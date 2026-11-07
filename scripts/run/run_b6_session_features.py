"""Write B6 Session Features Parquet.

Two input kinds:

1. sample (default, unchanged): writes a SAMPLED B6 Session Features Parquet for the P4-B-B7-01
   smoke test. Whole-session sampling by a deterministic hash of user_session, then the existing B6 logic.
   - Default sample is 1% of sessions. Full-dataset runs are not blocked here, but must be requested
     explicitly with --sample-percent 100 and belong to B6-02 / P5.
   - The write uses mode 'errorifexists' so an existing directory is never overwritten.

2. snapshot (P5-B-B6-01 stage 1): reads a snapshot (part=sample) or nested subset with its manifest.
   - No sampling is applied again (sample_whole_sessions is never called on this path).
   - Each session gets h11_split (train, validation, test) from the B6-01 rule (classify_sessions).
   - Only those sessions are ranked; the dataset holds eligible sessions only (one row per session).
   - Output: features root partitioned by h11_split (h11_split=train/, h11_split=validation/,
     h11_split=test/). The test partition carries labels for B9 only.
   - --is-valid-rule is required when the snapshot has an is_valid column (exit 5 = waiting decision).
   - --max-session-events-gate blocks the run (exit 2) when the longest session is above the gate.

Curated input is read only in sample mode. Nothing under the input path is modified.

Usage (sample, legacy):
    python3 scripts/run/run_b6_session_features.py --curated-path /data/curated/events --n 2 \
        --sample-percent 1 --output-path /data/ml/smoke/p4_b_b7_01/<run_id>/session_features \
        --stats-dir reports/P4_B_B7_01/<run_id>

Usage (snapshot, stage 1):
    python3 scripts/run/run_b6_session_features.py --input-kind snapshot --n 3 \
        --snapshot-dir WORK/ml_snapshot_455/subsets/pilot_57/ --snapshot-manifest WORK/ml_snapshot_455/subsets/pilot_57/_subset_manifest.json \
        --output-path WORK/ml_stage1/RID/b6_02_pilot/features --stats-dir reports/P5_B_B6_01/ml_stage1/RID/b6_02_pilot \
        --is-valid-rule ignore

Exit codes: 0 written; 1 error; 2 BLOCKED (arguments, manifest, gate); 5 WAITING_DECISION.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import yaml  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ml.session_features import build_session_features  # noqa: E402
from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark, load_config  # noqa: E402
from src.ml.dataset.ml_stage1 import (  # noqa: E402
    DecisionRequiredError,
    SessionLengthGateError,
    build_snapshot_features,
    check_input_manifest,
    file_uri,
    load_json,
    sha256_file,
    write_json,
)
from src.ml.dataset.n_selection import SplitBounds  # noqa: E402
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
DEFAULT_BOUNDS_CONFIG = os.path.join(ROOT, "configs", "n_candidate_analysis.example.yaml")
DEFAULT_STAGE1_CONFIG = os.path.join(ROOT, "configs", "ml_stage1.example.yaml")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write B6 Session Features to Parquet.")
    parser.add_argument("--input-kind", choices=["sample", "snapshot"], default="sample")
    parser.add_argument("--n", type=int, required=True, help="Prediction point N (positive integer).")
    parser.add_argument("--output-path", default=None, help="Output directory (must be new).")
    parser.add_argument("--stats-dir", required=True, help="Local directory for the JSON stats file.")
    # sample mode
    parser.add_argument("--curated-path", default="/data/curated/events")
    parser.add_argument("--sample-percent", type=float, default=0.1, help="Percent of SESSIONS, 0 < p <= 100.")
    parser.add_argument(
        "--allow-large-sample",
        action="store_true",
        help="Required for --sample-percent > 1. Not set by default; full runs need explicit approval.",
    )
    # snapshot mode
    parser.add_argument("--snapshot-dir", default=None, help="part=sample directory or a nested subset directory.")
    parser.add_argument("--snapshot-manifest", default=None, help="Manifest JSON of --snapshot-dir.")
    parser.add_argument("--bounds-config", default=DEFAULT_BOUNDS_CONFIG, help="YAML with windows_utc.")
    parser.add_argument("--is-valid-rule", choices=["ignore", "apply"], default=None,
                        help="Required when the snapshot has an is_valid column. ignore = as B6-01; apply = filter.")
    parser.add_argument("--max-session-events-gate", type=int, default=None,
                        help="Block when the longest session has more events (default: read from ml_stage1 config).")
    return parser.parse_args(argv)


def run_sample(args: argparse.Namespace) -> int:
    """Original sample path (unchanged behaviour)."""
    if args.output_path is None:
        raise SystemExit("--output-path is required in sample mode")
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
            "input_kind": "sample",
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


def _gate_from_config(args: argparse.Namespace) -> None:
    if args.max_session_events_gate is None and os.path.exists(DEFAULT_STAGE1_CONFIG):
        with open(DEFAULT_STAGE1_CONFIG, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        args.max_session_events_gate = (cfg.get("b6_02") or {}).get("max_session_events_gate")


def run_snapshot(args: argparse.Namespace) -> int:
    """Stage-1 snapshot path. Never samples again."""
    if args.output_path is None or not args.snapshot_dir or not args.snapshot_manifest:
        print("BLOCKED: snapshot mode needs --output-path, --snapshot-dir and --snapshot-manifest", file=sys.stderr)
        return 2
    if not os.path.exists(args.snapshot_manifest) or not os.path.isdir(args.snapshot_dir):
        print("BLOCKED: snapshot directory or manifest does not exist", file=sys.stderr)
        return 2
    manifest = load_json(args.snapshot_manifest)
    if manifest.get("kind") not in ("snapshot", "subset"):
        print("BLOCKED: manifest kind must be snapshot or subset", file=sys.stderr)
        return 2
    if manifest["kind"] == "snapshot" and os.path.basename(args.snapshot_dir.rstrip("/\\")) != "part=sample":
        print("BLOCKED: a snapshot must be read from its part=sample directory", file=sys.stderr)
        return 2
    assert_smoke_output_path(args.output_path)
    if os.path.exists(args.output_path):
        print(f"BLOCKED: output {args.output_path} already exists", file=sys.stderr)
        return 2
    _gate_from_config(args)
    with open(args.bounds_config, "r", encoding="utf-8") as f:
        bounds = SplitBounds.from_iso(yaml.safe_load(f)["windows_utc"])

    cfg = load_config("configs/config.example.yaml")
    spark = get_spark(app_name="P5ML-B6-02Snapshot", cfg=cfg)
    spark.sparkContext.setLogLevel("ERROR")
    dataset = None
    try:
        events = spark.read.parquet(file_uri(args.snapshot_dir))
        problems = check_input_manifest(events, manifest)
        missing = [c for c in REQUIRED_CURATED_COLUMNS if c not in events.columns]
        if missing:
            problems.append(f"missing B6 columns {missing}")
        if problems:
            print(f"BLOCKED: input does not match its manifest: {problems}", file=sys.stderr)
            return 2
        try:
            dataset, stats = build_snapshot_features(
                events,
                args.n,
                bounds,
                is_valid_rule=args.is_valid_rule,
                max_session_events_gate=args.max_session_events_gate,
            )
        except DecisionRequiredError as exc:
            print(f"WAITING_DECISION: {exc}", file=sys.stderr)
            return 5
        except SessionLengthGateError as exc:
            print(f"BLOCKED: {exc}", file=sys.stderr)
            return 2

        dataset.write.mode("errorifexists").partitionBy("h11_split").parquet(file_uri(args.output_path))
        stats.update({
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "input_kind": "snapshot_" + manifest["kind"],
            "input_name": manifest.get("name"),
            "input_dir": args.snapshot_dir,
            "input_manifest": args.snapshot_manifest,
            "input_manifest_sha256": sha256_file(args.snapshot_manifest),
            "source_scope": "SAMPLED_SESSIONS",
            "sample_threshold": int(manifest["threshold"]),
            "output_path": args.output_path,
            "spark_version": spark.version,
            "output_columns": dataset.columns,
            "bounds_utc": {
                "train": [bounds.train_start.isoformat(), bounds.train_end.isoformat()],
                "validation": [bounds.val_start.isoformat(), bounds.val_end.isoformat()],
                "test": [bounds.test_start.isoformat(), bounds.test_end.isoformat()],
            },
        })
        os.makedirs(args.stats_dir, exist_ok=True)
        write_json(os.path.join(args.stats_dir, "b6_02_snapshot_stats.json"), stats)
        print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
        return 0
    finally:
        if dataset is not None:
            dataset.unpersist()
        spark.stop()


def main() -> int:
    args = parse_args()
    if args.n < 1:
        raise SystemExit("--n must be a positive integer")
    if args.input_kind == "snapshot":
        return run_snapshot(args)
    return run_sample(args)


if __name__ == "__main__":
    sys.exit(main())
