#!/usr/bin/env bash
# P4-B-B7-02 acceptance runner for Linux VM B (preprocessing + seed/repeatability).
#
# Usage (repository mounted at /media/sf_ecommerce-bigdata-capstone):
#   cd /media/sf_ecommerce-bigdata-capstone
#   bash scripts/run/run_b7_02_vm_b.sh
#   bash scripts/run/run_b7_02_vm_b.sh --help
#
# Optional env: RUN_ID, PYTHON (default python3), REPO (override; default derived from this script).
# This runner uses SYNTHETIC data only. It does not read HDFS, does not read the test range,
# does not retrain on full data and does not write to any B7-01 model or report.
#
# Step graph (a step runs only when its prerequisites PASS):
#   E1 environment (python, pyspark, java recorded to environment.json)
#   E2 source integrity (reports/P4_B_B7_02/source_manifest.json)        -> gates E4, E5
#   E4a tests/unit/test_b7_02_preprocessing_seed.py (JUnit XML)            needs E2
#   E4b regression: test_b7_01_tooling, test_b7_01_lr_smoke, test_b6_02,
#       test_split_metrics -- each in its own process                       needs E2
#   E5 evidence criteria (check_b7_02_evidence.py)                         needs E4a, E2
#
# Status: PASS, FAIL, BLOCKED, SKIPPED. Exit: 0 PASS, 1 FAIL, 2 BLOCKED.
# A run never overwrites an existing RUN_ID directory.
set -u

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  sed -n '1,25p' "$0"
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PY="${PYTHON:-python3}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

# Same resource profile as the accepted B7-01 runner (2 CPU, ~4.6 GB RAM).
export CAPSTONE_SPARK_MASTER="${CAPSTONE_SPARK_MASTER:-local[2]}"
export CAPSTONE_DRIVER_MEMORY="${CAPSTONE_DRIVER_MEMORY:-1g}"
export CAPSTONE_SHUFFLE_PARTITIONS="${CAPSTONE_SHUFFLE_PARTITIONS:-4}"
export PYSPARK_SUBMIT_ARGS="--driver-memory ${CAPSTONE_DRIVER_MEMORY} pyspark-shell"

if [[ "${REPO}" == *sf_CapstoneRepo* ]]; then
  echo "ERROR: /media/sf_CapstoneRepo is outdated. Use /media/sf_ecommerce-bigdata-capstone." >&2
  exit 2
fi
REQUIRED_FILES=(
  "src/ml/training/lr_b6_smoke.py"
  "scripts/run/source_manifest_b7_02.py"
  "scripts/run/check_b7_02_evidence.py"
  "tests/unit/test_b7_02_preprocessing_seed.py"
  "tests/unit/test_b7_01_tooling.py"
  "tests/unit/test_b7_01_lr_smoke.py"
  "configs/b7_smoke_hyperparams.example.yaml"
  "reports/P4_B_B7_02/source_manifest.json"
)
for f in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "${REPO}/${f}" ]; then
    echo "ERROR: required file missing: ${REPO}/${f}" >&2
    exit 2
  fi
done
PY_ABS="$(command -v "${PY}" || true)"
if [ -z "${PY_ABS}" ]; then echo "ERROR: interpreter not found: ${PY}" >&2; exit 2; fi
export PYSPARK_PYTHON="${PY_ABS}"
export PYSPARK_DRIVER_PYTHON="${PY_ABS}"

cd "${REPO}" || exit 2
REPORT_DIR="${REPO}/reports/P4_B_B7_02/${RUN_ID}"
# Python receives RELATIVE paths (cwd = repo). Git Bash /e/... paths are not valid for Windows Python.
REPORT_REL="reports/P4_B_B7_02/${RUN_ID}"
if ! mkdir "${REPORT_DIR}" 2>/dev/null; then
  echo "ERROR: ${REPORT_DIR} already exists. Choose a new RUN_ID; evidence is never overwritten." >&2
  exit 2
fi
LOG="${REPORT_DIR}/vm_b_run.log"
SUMMARY="${REPORT_DIR}/vm_b_summary.txt"
exec > >(tee -a "${LOG}") 2>&1

STEPS=()
FINALIZED=0
add_step() { STEPS+=("$1|$2|$3|$4"); }
status_of() {
  local e n s
  for e in "${STEPS[@]:-}"; do
    IFS='|' read -r n s _ _ <<< "$e"
    if [ "$n" = "$1" ]; then echo "$s"; return; fi
  done
  echo "NOT_RUN"
}
gate() {  # gate NAME PREREQ... -> 0 if all prerequisites PASS
  local name="$1"; shift
  local p
  for p in "$@"; do
    if [ "$(status_of "$p")" != "PASS" ]; then
      add_step "$name" "SKIPPED" "-" "prerequisite $p is $(status_of "$p")"
      echo "SKIPPED ${name}: prerequisite ${p} is $(status_of "$p")"
      return 1
    fi
  done
  return 0
}
run_step() {  # run_step NAME NOTE cmd...
  local name="$1" note="$2"; shift 2
  echo; echo "===== ${name}: ${note} ====="
  "$@"
  local rc=$?
  if [ $rc -eq 0 ]; then add_step "$name" "PASS" "$rc" "$note"; else add_step "$name" "FAIL" "$rc" "$note"; fi
  echo "${name} exit=${rc}"
  return $rc
}

finalize() {
  [ "${FINALIZED}" = "1" ] && return
  FINALIZED=1
  local overall="PASS" e s
  for e in "${STEPS[@]:-}"; do
    IFS='|' read -r _ s _ _ <<< "$e"
    if [ "$s" = "FAIL" ]; then overall="FAIL"; fi
  done
  if [ "${overall}" != "FAIL" ]; then
    for e in "${STEPS[@]:-}"; do
      IFS='|' read -r _ s _ _ <<< "$e"
      if [ "$s" = "BLOCKED" ] || [ "$s" = "SKIPPED" ] || [ "$s" = "NOT_RUN" ]; then overall="BLOCKED"; fi
    done
  fi
  {
    echo "TASK=P4-B-B7-02"
    echo "RUN_ID=${RUN_ID}"
    echo "REPO=${REPO}"
    echo "PYTHON=$(${PY_ABS} --version 2>&1)"
    echo "SPARK_MASTER=${CAPSTONE_SPARK_MASTER} DRIVER_MEMORY=${CAPSTONE_DRIVER_MEMORY} SHUFFLE_PARTITIONS=${CAPSTONE_SHUFFLE_PARTITIONS}"
    echo "DATA=synthetic only (no HDFS read, no test range read)"
    echo "REPORT_DIR=${REPORT_DIR}"
    echo "OVERALL=${overall}"
    echo "--- steps (NAME|STATUS|EXIT|NOTE) ---"
    printf '%s\n' "${STEPS[@]:-}"
  } > "${SUMMARY}"
  echo; echo "===== SUMMARY ====="; cat "${SUMMARY}"
  case "${overall}" in PASS) exit 0 ;; FAIL) exit 1 ;; *) exit 2 ;; esac
}
trap finalize EXIT

# --- E1 environment -----------------------------------------------------------
echo "===== E1 environment ====="
whoami; pwd; free -m; nproc
"${PY_ABS}" -c "
import json, sys, platform, subprocess
info = {'python': sys.version.split()[0], 'platform': platform.platform()}
try:
    import pyspark; info['pyspark'] = pyspark.__version__
except Exception as exc:
    info['pyspark'] = 'IMPORT_FAILED: %s' % exc
try:
    info['java'] = subprocess.run(['java', '-version'], capture_output=True, text=True).stderr.splitlines()[0]
except Exception as exc:
    info['java'] = 'UNKNOWN: %s' % exc
json.dump(info, open('${REPORT_REL}/environment.json', 'w'), indent=2)
print(json.dumps(info, indent=2))
"
E1_RC=$?
if [ $E1_RC -eq 0 ]; then add_step "E1_environment" "PASS" "0" "environment recorded"; else add_step "E1_environment" "BLOCKED" "$E1_RC" "environment probe failed"; fi

# --- E2 source integrity -------------------------------------------------------
"${PY_ABS}" scripts/run/source_manifest_b7_02.py verify --out "${REPORT_REL}/source_integrity.json" > /dev/null
E2_RC=$?
case $E2_RC in
  0) add_step "E2_source_integrity" "PASS" "0" "B7-02 manifest SHA-256 match" ;;
  1) add_step "E2_source_integrity" "FAIL" "1" "content differs from manifest: Spark steps not run" ;;
  *) add_step "E2_source_integrity" "BLOCKED" "$E2_RC" "manifest missing or unreadable" ;;
esac

# --- E4a B7-02 preprocessing and seed tests ----------------------------------
if gate "E4a_b7_02_tests" "E2_source_integrity"; then
  run_step "E4a_b7_02_tests" "tests/unit/test_b7_02_preprocessing_seed.py (JUnit XML)" \
    bash -c "'${PY_ABS}' -m pytest -q -p no:cacheprovider --junitxml='${REPORT_REL}/b7_02_junit.xml' tests/unit/test_b7_02_preprocessing_seed.py > '${REPORT_REL}/b7_02_pytest.log' 2>&1"
  tail -1 "${REPORT_DIR}/b7_02_pytest.log" 2>/dev/null
fi

# --- E4b regression: each file in its own process ----------------------------
if gate "E4b_regression_group" "E2_source_integrity"; then
  E4B_OK=1
  for t in test_b7_01_tooling test_b7_01_lr_smoke test_b6_02 test_split_metrics; do
    run_step "E4b_${t}" "tests/unit/${t}.py in its own process" \
      bash -c "'${PY_ABS}' -m pytest -q -p no:cacheprovider tests/unit/${t}.py > '${REPORT_DIR}/e4b_${t}.log' 2>&1" || E4B_OK=0
    tail -1 "${REPORT_DIR}/e4b_${t}.log" 2>/dev/null
  done
  if [ "${E4B_OK}" = "1" ]; then add_step "E4b_regression_group" "PASS" "0" "all regression files PASS"; else add_step "E4b_regression_group" "FAIL" "1" "at least one regression file failed"; fi
fi

# --- E5 evidence criteria -------------------------------------------------------
if gate "E5_evidence_criteria" "E2_source_integrity" "E4a_b7_02_tests"; then
  run_step "E5_evidence_criteria" "required TC list vs JUnit results" \
    "${PY_ABS}" scripts/run/check_b7_02_evidence.py \
      --junit "${REPORT_REL}/b7_02_junit.xml" \
      --source "${REPORT_REL}/source_integrity.json" \
      --env "${REPORT_REL}/environment.json" \
      --out "${REPORT_REL}/evidence_check.json"
fi

exit 0
