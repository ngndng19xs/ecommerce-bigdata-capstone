#!/usr/bin/env bash
# Stage 1 targeted and regression tests. One pytest process per file: Spark sessions are not shared
# between files (the Windows blockmgr issue seen earlier also applies to repeated sessions).
# Usage: PY=python3 bash scripts/run/p5_stage1_tests.sh   (run from anywhere; cd to the repo first)
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO" || exit 2
PY="${PY:-python3}"
FILES=(
  tests/unit/test_ml_stage1_snapshot.py
  tests/unit/test_ml_stage1_b6.py
  tests/unit/test_ml_stage1_compare.py
  tests/unit/test_ml_stage1_watchdog.py
  tests/unit/test_ml_stage1_ledger.py
  tests/unit/test_ml_stage1_reuse.py
  tests/unit/test_p5_b6_01_n_selection.py
  tests/unit/test_b6_02.py
  tests/unit/test_ml_features.py
  tests/unit/test_b6_01_edges.py
  tests/unit/test_b7_01_lr_smoke.py
  tests/unit/test_b7_01_tooling.py
  tests/unit/test_b7_02_preprocessing_seed.py
)
rc_all=0
for f in "${FILES[@]}"; do
  echo "=== $f"
  if ! "$PY" -m pytest -q "$f"; then
    echo "FAILED: $f"
    rc_all=1
  fi
done
exit $rc_all
