"""Source manifest for P5-B-B6-01, reusing source_manifest.py WITHOUT modifying it.

Usage:
  python scripts/run/source_manifest_p5.py make
  python scripts/run/source_manifest_p5.py verify [--out <json>]

Manifest: reports/P5_B_B6_01/source_manifest.json
"""

import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

_spec = importlib.util.spec_from_file_location("source_manifest_base", os.path.join(HERE, "source_manifest.py"))
sm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sm)

P5_KEY_FILES = [
    # B6 code that the analysis reuses or uses as oracle
    "ml/features.py",
    "ml/session_features.py",
    # shared utilities used by the analysis CLI
    "src/common/logger.py",
    "src/common/spark_session.py",
    "src/ml/training/lr_b6_smoke.py",
    # P5-B-B6-01 deliverables
    "src/ml/dataset/n_selection.py",
    "scripts/run/run_p5_b6_01_n_analysis.py",
    "scripts/run/source_manifest_p5.py",
    "configs/n_candidate_analysis.example.yaml",
    "tests/unit/test_p5_b6_01_n_selection.py",
    "tests/unit/test_b6_02.py",
    "tests/unit/test_ml_features.py",
    "tests/unit/test_b6_01_edges.py",
]

sm.KEY_FILES = P5_KEY_FILES
sm.MANIFEST_REL = os.path.join("reports", "P5_B_B6_01", "source_manifest.json")
sm.ROOT = ROOT


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="P5-B-B6-01 source manifest (wrapper).")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("make")
    p_verify = sub.add_parser("verify")
    p_verify.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.cmd == "make":
        return sm.cmd_make(args)
    return sm.cmd_verify(args)


if __name__ == "__main__":
    sys.exit(main())
