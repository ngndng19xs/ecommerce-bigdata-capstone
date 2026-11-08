#!/usr/bin/env python3
"""P5 ML pipeline CLI: snapshot → B6 → B7 → B8 → B9 orchestration."""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset import b6_groups, snapshot_stream  # noqa: E402
from src.ml.training import ml_b7_b9  # noqa: E402


def cmd_snapshot_build(args):
    """Build incremental snapshot (S4 redesign)."""
    from scripts.run import p5_snapshot
    p5_snapshot.main_build(args)


def cmd_snapshot_verify(args):
    """Verify snapshot integrity."""
    from scripts.run import p5_snapshot
    p5_snapshot.main_verify(args)


def cmd_b6_analyze(args):
    """B6-01: Analyze hash groups and N candidates."""
    spark = get_spark("P5_ML_B6_Analyze")
    try:
        snapshot_dir = args.snapshot_dir
        if not os.path.exists(snapshot_dir):
            print(f"ERROR: snapshot_dir not found: {snapshot_dir}", file=sys.stderr)
            return 1

        # Read manifest
        manifest_path = os.path.join(snapshot_dir, "manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        n_groups = manifest.get("n_groups", 8)

        results = []
        for group_id in range(n_groups):
            df = b6_groups.read_snapshot_group(spark, snapshot_dir, part="sample", group=group_id)
            # TODO: call analyze_group_b601 with bounds and h11_split_table
            results.append({"group_id": group_id, "status": "TODO"})

        merged = b6_groups.merge_b601_results(results)
        print(json.dumps(merged, indent=2))
        return 0
    finally:
        spark.stop()


def cmd_b7_train(args):
    """B7: Train Logistic Regression."""
    spark = get_spark("P5_ML_B7_Train")
    try:
        # TODO: read train/val from B6-02 output, train model
        print("B7 training: TODO")
        return 0
    finally:
        spark.stop()


def cmd_b8_tune(args):
    """B8: Tune threshold and freeze model."""
    spark = get_spark("P5_ML_B8_Tune")
    try:
        # TODO: read val data, tune threshold, freeze
        print("B8 threshold tuning: TODO")
        return 0
    finally:
        spark.stop()


def cmd_b9_test(args):
    """B9: Final test on held-out test split."""
    spark = get_spark("P5_ML_B9_Test")
    try:
        # TODO: read test data, compute final metrics
        print("B9 final test: TODO")
        return 0
    finally:
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="P5 ML pipeline: snapshot → B6 → B7 → B8 → B9",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Pipeline stage")

    # snapshot subcommands
    snap_parser = subparsers.add_parser("snapshot-build", help="Build incremental snapshot")
    snap_parser.add_argument("--source", required=True, help="HDFS source path")
    snap_parser.add_argument("--output-dir", required=True, help="Local output snapshot dir")
    snap_parser.add_argument("--sample-threshold", type=int, default=455, help="Sample threshold hash")
    snap_parser.add_argument("--n-groups", type=int, default=8, help="Number of hash groups")
    snap_parser.set_defaults(func=cmd_snapshot_build)

    snap_verify = subparsers.add_parser("snapshot-verify", help="Verify snapshot integrity")
    snap_verify.add_argument("--snapshot-dir", required=True, help="Snapshot directory")
    snap_verify.set_defaults(func=cmd_snapshot_verify)

    # B6
    b6_parser = subparsers.add_parser("b6-analyze", help="B6-01 analyze hash groups")
    b6_parser.add_argument("--snapshot-dir", required=True, help="Snapshot directory")
    b6_parser.set_defaults(func=cmd_b6_analyze)

    # B7
    b7_parser = subparsers.add_parser("b7-train", help="B7 train Logistic Regression")
    b7_parser.add_argument("--features-dir", required=True, help="B6-02 features directory")
    b7_parser.add_argument("--output-dir", required=True, help="Output model directory")
    b7_parser.set_defaults(func=cmd_b7_train)

    # B8
    b8_parser = subparsers.add_parser("b8-tune", help="B8 tune threshold and freeze")
    b8_parser.add_argument("--model-dir", required=True, help="Trained model directory")
    b8_parser.add_argument("--val-features", required=True, help="Validation features")
    b8_parser.set_defaults(func=cmd_b8_tune)

    # B9
    b9_parser = subparsers.add_parser("b9-test", help="B9 final test")
    b9_parser.add_argument("--model-dir", required=True, help="Frozen model directory")
    b9_parser.add_argument("--test-features", required=True, help="Test features")
    b9_parser.set_defaults(func=cmd_b9_test)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
