"""Source manifest for P4-B-B7-02, reusing the B7-01 tool WITHOUT modifying it.

Why a wrapper: scripts/run/source_manifest.py is itself hashed in the committed B7-01 manifest.
Editing it would make the historical B7-01 TC16 fail on VM B. This wrapper imports the module
and overrides its KEY_FILES and MANIFEST_REL in memory, so the B7-01 manifest and its evidence
stay valid.

Usage:
  python scripts/run/source_manifest_b7_02.py make
  python scripts/run/source_manifest_b7_02.py verify [--out <json>]

Output manifest: reports/P4_B_B7_02/source_manifest.json
"""

import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

_spec = importlib.util.spec_from_file_location("source_manifest_base", os.path.join(HERE, "source_manifest.py"))
sm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sm)

# B7-01 key files (unchanged) plus the files that define B7-02.
B7_02_EXTRA = [
    "configs/b7_smoke_hyperparams.example.yaml",
    "tests/unit/test_b7_02_preprocessing_seed.py",
    "scripts/run/check_b7_02_evidence.py",
    "scripts/run/source_manifest_b7_02.py",
    "scripts/run/run_b7_02_vm_b.sh",
]
KEY_FILES_B7_02 = list(sm.KEY_FILES) + [f for f in B7_02_EXTRA if f not in sm.KEY_FILES]

sm.KEY_FILES = KEY_FILES_B7_02
sm.MANIFEST_REL = os.path.join("reports", "P4_B_B7_02", "source_manifest.json")
sm.ROOT = ROOT


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="P4-B-B7-02 source manifest (wrapper).")
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
