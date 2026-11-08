#!/usr/bin/env bash
# P4-B-B7-01 acceptance runner for Linux VM B.
#
# Usage (repository mounted at /media/sf_ecommerce-bigdata-capstone):
#   cd /media/sf_ecommerce-bigdata-capstone
#   bash scripts/run/run_b7_01_vm_b.sh
#
# Overrides (optional): N, SAMPLE_PCT, CURATED, RUN_ID, PYTHON, REPO, ALLOW_LARGE_SAMPLE=1
#
# Step graph (a step runs only when all its prerequisites PASS):
#   E1  environment (informational; pyspark import must work)
#   E2a source integrity: manifest verify (TC16)      -> gates E3, E4a, E4b, E5, E6
#   E4a unit tests B7-01 (incl. TC15 repeatability)   needs E2a
#   E4b regression tests ML/B6                        needs E2a
#   E3  HDFS Curated preflight (metadata only)        needs E1, E2a
#   E5  write sampled B6 Session Features             needs E3, E4a
#   E6  smoke LR on B6 features                       needs E5
#   E7  evidence criteria                             needs E6, E2a
#
# Status values: PASS, FAIL, BLOCKED (cannot run: missing evidence/env), SKIPPED (prerequisite not PASS).
# Exit code: 0 = OVERALL PASS, 1 = OVERALL FAIL, 2 = OVERALL BLOCKED.
# A run never overwrites an existing RUN_ID directory.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PY="${PYTHON:-python3}"
N="${N:-2}"
SAMPLE_PCT="${SAMPLE_PCT:-0.1}"
CURATED="${CURATED:-/data/curated/events}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ALLOW_LARGE_SAMPLE="${ALLOW_LARGE_SAMPLE:-0}"

# Spark resource profile for a 2-core / ~4.6 GB RAM VM with ~1.6 GB free at check time.
# 1g driver is chosen so that the JVM plus OS and Python workers stay inside the free RAM.
export CAPSTONE_SPARK_MASTER="${CAPSTONE_SPARK_MASTER:-local[2]}"
export CAPSTONE_DRIVER_MEMORY="${CAPSTONE_DRIVER_MEMORY:-1g}"
export CAPSTONE_SHUFFLE_PARTITIONS="${CAPSTONE_SHUFFLE_PARTITIONS:-4}"
export PYSPARK_SUBMIT_ARGS="--driver-memory ${CAPSTONE_DRIVER_MEMORY} pyspark-shell"
# Tests and scripts that build SparkSession directly need an absolute Python path for workers.
PY_ABS="$(command -v "${PY}" || true)"
if [ -z "${PY_ABS}" ]; then echo "ERROR: interpreter not found: ${PY}" >&2; exit 2; fi
export PYSPARK_PYTHON="${PY_ABS}"
export PYSPARK_DRIVER_PYTHON="${PY_ABS}"

# --- Repository checks (before anything is created) --------------------------
if [[ "${REPO}" == *sf_CapstoneRepo* ]]; then
  echo "ERROR: /media/sf_CapstoneRepo is the outdated mount. Use /media/sf_ecommerce-bigdata-capstone." >&2
  exit 2
fi
REQUIRED_FILES=(
  "src/ml/training/lr_b6_smoke.py"
  "scripts/run/run_b6_session_features.py"
  "scripts/run/run_b7_lr_smoke.py"
  "scripts/run/preflight_b7_01.py"
  "scripts/run/source_manifest.py"
  "scripts/run/check_b7_01_evidence.py"
  "ml/session_features.py"
  "ml/features.py"
  "tests/unit/test_b7_01_lr_smoke.py"
  "configs/config.example.yaml"
  "reports/P4_B_B7_01/source_manifest.json"
)
for f in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "${REPO}/${f}" ]; then
    echo "ERROR: required file missing: ${REPO}/${f}. Check the shared-folder mount and that the manifest was generated on Windows." >&2
    exit 2
  fi
done
if [ "${SAMPLE_PCT}" != "" ] && awk "BEGIN{exit !(${SAMPLE_PCT} > 1)}"; then
  if [ "${ALLOW_LARGE_SAMPLE}" != "1" ]; then
    echo "ERROR: SAMPLE_PCT=${SAMPLE_PCT} > 1 needs ALLOW_LARGE_SAMPLE=1 (not the default)." >&2
    exit 2
  fi
fi

cd "${REPO}" || exit 2
REPORT_ROOT="${REPO}/reports/P4_B_B7_01"
REPORT_DIR="${REPORT_ROOT}/${RUN_ID}"
SMOKE_ROOT="${SMOKE_ROOT:-/data/ml/smoke/p4_b_b7_01/${RUN_ID}}"  # smoke-only HDFS path; never an official path
if ! mkdir "${REPORT_DIR}" 2>/dev/null; then
  echo "ERROR: ${REPORT_DIR} already exists. Pick a new RUN_ID; existing evidence is never overwritten." >&2
  exit 2
fi
LOG="${REPORT_DIR}/vm_b_run.log"
SUMMARY="${REPORT_DIR}/vm_b_summary.txt"
exec > >(tee -a "${LOG}") 2>&1

STEPS=()   # entries: NAME|STATUS|RC|NOTE
FINALIZED=0

add_step() { STEPS+=("$1|$2|$3|$4"); }
status_of() {  # status_of NAME -> prints status or NOT_RUN
  local e
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
    echo "RUN_ID=${RUN_ID}"
    echo "REPO=${REPO}"
    echo "PYTHON=$(${PY} --version 2>&1)"
    echo "N=${N} SAMPLE_PCT=${SAMPLE_PCT} CURATED=${CURATED} ALLOW_LARGE_SAMPLE=${ALLOW_LARGE_SAMPLE}"
    echo "SPARK_MASTER=${CAPSTONE_SPARK_MASTER} DRIVER_MEMORY=${CAPSTONE_DRIVER_MEMORY} SHUFFLE_PARTITIONS=${CAPSTONE_SHUFFLE_PARTITIONS}"
    echo "SMOKE_ROOT=${SMOKE_ROOT}"
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
"${PY}" --version
"${PY}" -c "import sys, pyspark; print('pyspark', pyspark.__version__, 'python', sys.version.split()[0])"
E1_RC=$?
java -version 2>&1 | head -2
if [ $E1_RC -eq 0 ]; then add_step "E1_environment" "PASS" "0" "pyspark importable"; else add_step "E1_environment" "BLOCKED" "$E1_RC" "pyspark not importable"; fi

# --- E2a source integrity (TC16) ----------------------------------------------
"${PY}" scripts/run/source_manifest.py verify --out "${REPORT_DIR}/tc16_source.json"
E2_RC=$?
case $E2_RC in
  0) add_step "E2a_source_integrity_TC16" "PASS" "0" "manifest SHA-256 match" ;;
  1) add_step "E2a_source_integrity_TC16" "FAIL" "1" "content differs from manifest: Spark steps not run" ;;
  *) add_step "E2a_source_integrity_TC16" "BLOCKED" "$E2_RC" "manifest missing or unreadable: TC16 not verified" ;;
esac

# --- E4a / E4b unit and regression tests (no HDFS needed) ---------------------
if gate "E4a_unit_b7_01" "E2a_source_integrity_TC16"; then
  run_step "E4a_unit_b7_01" "tests/unit/test_b7_01_lr_smoke.py (includes TC15)" \
    bash -c "'${PY}' -m pytest -q -p no:cacheprovider tests/unit/test_b7_01_lr_smoke.py > '${REPORT_DIR}/e4a_pytest.log' 2>&1"
  tail -1 "${REPORT_DIR}/e4a_pytest.log" 2>/dev/null
fi
# Each regression file runs in its OWN pytest process. Several SparkSession create/stop cycles in
# one process corrupted the shuffle temp dir (seen on Windows; same design risk on VM B).
if gate "E4b_regression_ml_b6_group" "E2a_source_integrity_TC16"; then
  E4B_OK=1
  for t in test_b6_02 test_ml_features test_b6_01_edges test_session_dataset test_split_metrics; do
    run_step "E4b_${t}" "tests/unit/${t}.py in its own process" \
      bash -c "'${PY}' -m pytest -q -p no:cacheprovider tests/unit/${t}.py > '${REPORT_DIR}/e4b_${t}.log' 2>&1" || E4B_OK=0
    tail -1 "${REPORT_DIR}/e4b_${t}.log" 2>/dev/null
  done
  if [ "${E4B_OK}" = "1" ]; then add_step "E4b_regression_ml_b6_group" "PASS" "0" "all 5 files PASS"; else add_step "E4b_regression_ml_b6_group" "FAIL" "1" "at least one file failed"; fi
fi

# --- E3 HDFS preflight (metadata only; /data/ml is NOT required) --------------
if gate "E3_hdfs_preflight" "E1_environment" "E2a_source_integrity_TC16"; then
  run_step "E3_hdfs_preflight" "Curated path exists, parquet files present, schema complete (no row scan)" \
    "${PY}" scripts/run/preflight_b7_01.py --curated-path "${CURATED}" --out "${REPORT_DIR}/e3_preflight.json"
fi

# --- E5 sampled B6 Session Features ------------------------------------------
if gate "E5_b6_writer_sample" "E3_hdfs_preflight" "E4a_unit_b7_01"; then
  E5_ARGS=(--curated-path "${CURATED}" --n "${N}" --sample-percent "${SAMPLE_PCT}"
           --output-path "${SMOKE_ROOT}/session_features" --stats-dir "${REPORT_DIR}")
  if [ "${ALLOW_LARGE_SAMPLE}" = "1" ]; then E5_ARGS+=(--allow-large-sample); fi
  run_step "E5_b6_writer_sample" "whole-session hash sample ${SAMPLE_PCT}% of Curated, N=${N}" \
    "${PY}" scripts/run/run_b6_session_features.py "${E5_ARGS[@]}"
fi

# --- E6 smoke LR ---------------------------------------------------------------
if gate "E6_lr_smoke" "E5_b6_writer_sample"; then
  run_step "E6_lr_smoke" "LR on B6 features, train/validation only" \
    "${PY}" scripts/run/run_b7_lr_smoke.py \
      --input-path "${SMOKE_ROOT}/session_features" \
      --output-dir "${SMOKE_ROOT}/model" \
      --report-dir "${REPORT_DIR}"
fi

# --- E7 evidence criteria ------------------------------------------------------
if gate "E7_evidence_criteria" "E6_lr_smoke" "E2a_source_integrity_TC16"; then
  run_step "E7_evidence_criteria" "criteria from smoke_report, writer stats, TC16 file" \
    "${PY}" scripts/run/check_b7_01_evidence.py --report-dir "${REPORT_DIR}"
fi

exit 0
