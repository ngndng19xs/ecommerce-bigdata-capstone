#!/usr/bin/env bash
# P5-B-B6-01 acceptance runner for Linux VM B (N candidate analysis).
#
# Usage (repository mounted at /media/sf_ecommerce-bigdata-capstone):
#   cd /media/sf_ecommerce-bigdata-capstone
#   RUN_ID=p5_vm_1 bash scripts/run/run_p5_b6_01_vm_b.sh
#   bash scripts/run/run_p5_b6_01_vm_b.sh --help
#
# Steps (a step runs only when its prerequisites PASS):
#   E1  environment (python, pyspark, java -> environment.json)
#   E2  source integrity (reports/P5_B_B6_01/source_manifest.json)              -> gates E3-E6
#   E3  HDFS Curated preflight, metadata only (no row scan)                      needs E1, E2
#   E4a tests/unit/test_p5_b6_01_n_selection.py (JUnit)                          needs E2
#   E4b regression B6: test_b6_02, test_ml_features, test_b6_01_edges (one process each)  needs E2
#   E5  sample analysis: whole-session hash sample 0.1% of Curated, N=2,3,5      needs E3, E4a, E4b
#   E6  FULL analysis of all Curated: ONLY when FULL_RUN=1 AND CONFIRM_FULL=1    needs E5
#
# Output: reports/P5_B_B6_01/<RUN_ID>/  (never overwrites an existing RUN_ID)
# Status: PASS, FAIL, BLOCKED, SKIPPED.  Exit: 0 PASS, 1 FAIL, 2 BLOCKED.
set -u

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  sed -n '1,20p' "$0"
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
PY="${PYTHON:-python3}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
CURATED="${CURATED:-/data/curated/events}"
SAMPLE_PCT="${SAMPLE_PCT:-0.1}"
FULL_RUN="${FULL_RUN:-0}"
CONFIRM_FULL="${CONFIRM_FULL:-0}"

export CAPSTONE_SPARK_MASTER="${CAPSTONE_SPARK_MASTER:-local[2]}"
export CAPSTONE_DRIVER_MEMORY="${CAPSTONE_DRIVER_MEMORY:-1g}"
export CAPSTONE_SHUFFLE_PARTITIONS="${CAPSTONE_SHUFFLE_PARTITIONS:-4}"
export PYSPARK_SUBMIT_ARGS="--driver-memory ${CAPSTONE_DRIVER_MEMORY} pyspark-shell"

if [[ "${REPO}" == *sf_CapstoneRepo* ]]; then
  echo "ERROR: /media/sf_CapstoneRepo is outdated. Use /media/sf_ecommerce-bigdata-capstone." >&2
  exit 2
fi
REQUIRED_FILES=(
  "src/ml/dataset/n_selection.py"
  "scripts/run/run_p5_b6_01_n_analysis.py"
  "scripts/run/source_manifest_p5.py"
  "scripts/run/preflight_b7_01.py"
  "configs/n_candidate_analysis.example.yaml"
  "tests/unit/test_p5_b6_01_n_selection.py"
  "reports/P5_B_B6_01/source_manifest.json"
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
REPORT_REL="reports/P5_B_B6_01/${RUN_ID}"
REPORT_DIR="${REPO}/${REPORT_REL}"
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
gate() {
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
run_step() {
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
      IFS='|' read -r n s _ _ <<< "$e"
      # E6 (full) is deliberately SKIPPED by default and must not turn the overall result to BLOCKED.
      if [ "$n" = "E6_full_analysis" ]; then continue; fi
      if [ "$s" = "BLOCKED" ] || [ "$s" = "SKIPPED" ] || [ "$s" = "NOT_RUN" ]; then overall="BLOCKED"; fi
    done
  fi
  {
    echo "TASK=P5-B-B6-01"
    echo "RUN_ID=${RUN_ID}"
    echo "REPO=${REPO}"
    echo "PYTHON=$(${PY_ABS} --version 2>&1)"
    echo "CURATED=${CURATED} SAMPLE_PCT=${SAMPLE_PCT} FULL_RUN=${FULL_RUN} CONFIRM_FULL=${CONFIRM_FULL}"
    echo "SPARK_MASTER=${CAPSTONE_SPARK_MASTER} DRIVER_MEMORY=${CAPSTONE_DRIVER_MEMORY} SHUFFLE_PARTITIONS=${CAPSTONE_SHUFFLE_PARTITIONS}"
    echo "REPORT_DIR=${REPORT_DIR}"
    echo "OVERALL=${overall}"
    echo "NOTE=No N is selected by this run. Final N needs evidence review and A/B approval."
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
import os, time, datetime
info['python_local_utc_offset_hours'] = datetime.datetime.now().astimezone().utcoffset().total_seconds() / 3600
info['tz_env'] = os.environ.get('TZ')
info['tzname'] = list(time.tzname)
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

# --- E2 source integrity ------------------------------------------------------
"${PY_ABS}" scripts/run/source_manifest_p5.py verify --out "${REPORT_REL}/source_integrity.json" > /dev/null
E2_RC=$?
case $E2_RC in
  0) add_step "E2_source_integrity" "PASS" "0" "P5 manifest SHA-256 match" ;;
  1) add_step "E2_source_integrity" "FAIL" "1" "content differs from manifest: Spark steps not run" ;;
  *) add_step "E2_source_integrity" "BLOCKED" "$E2_RC" "manifest missing or unreadable" ;;
esac

# --- E3 HDFS preflight (metadata only) ---------------------------------------
if gate "E3_hdfs_preflight" "E1_environment" "E2_source_integrity"; then
  run_step "E3_hdfs_preflight" "Curated path, parquet files, schema (no row scan)" \
    "${PY_ABS}" scripts/run/preflight_b7_01.py --curated-path "${CURATED}" --out "${REPORT_REL}/e3_preflight.json"
fi

# --- E4a P5 unit tests --------------------------------------------------------
if gate "E4a_p5_unit_tests" "E2_source_integrity"; then
  run_step "E4a_p5_unit_tests" "tests/unit/test_p5_b6_01_n_selection.py (JUnit)" \
    bash -c "'${PY_ABS}' -m pytest -q -p no:cacheprovider --junitxml='${REPORT_REL}/p5_junit.xml' tests/unit/test_p5_b6_01_n_selection.py > '${REPORT_REL}/p5_pytest.log' 2>&1"
  tail -1 "${REPORT_REL}/p5_pytest.log" 2>/dev/null
fi

# --- E4b regression B6 (one process per file) ---------------------------------
if gate "E4b_regression_b6_group" "E2_source_integrity"; then
  E4B_OK=1
  for t in test_b6_02 test_ml_features test_b6_01_edges; do
    run_step "E4b_${t}" "tests/unit/${t}.py in its own process" \
      bash -c "'${PY_ABS}' -m pytest -q -p no:cacheprovider tests/unit/${t}.py > '${REPORT_REL}/e4b_${t}.log' 2>&1" || E4B_OK=0
    tail -1 "${REPORT_REL}/e4b_${t}.log" 2>/dev/null
  done
  if [ "${E4B_OK}" = "1" ]; then add_step "E4b_regression_b6_group" "PASS" "0" "all B6 regression files PASS"; else add_step "E4b_regression_b6_group" "FAIL" "1" "at least one regression file failed"; fi
fi

# --- E5 sample analysis -------------------------------------------------------
if gate "E5_sample_analysis" "E3_hdfs_preflight" "E4a_p5_unit_tests" "E4b_regression_b6_group"; then
  if [ "${SAMPLE_PCT}" != "" ] && awk "BEGIN{exit !(${SAMPLE_PCT} > 1)}"; then
    echo "ERROR: SAMPLE_PCT=${SAMPLE_PCT} > 1 is not allowed for the sample step."; add_step "E5_sample_analysis" "FAIL" "2" "SAMPLE_PCT > 1"
  else
    run_step "E5_sample_analysis" "acceptance: official selected_n only, whole-session sample ${SAMPLE_PCT}%" \
      "${PY_ABS}" scripts/run/run_p5_b6_01_n_analysis.py --input-path "${CURATED}" --mode sample \
        --analysis-scope acceptance --sample-percent "${SAMPLE_PCT}" \
        --output-dir "${REPORT_REL}/sample" --run-id "${RUN_ID}_sample"
    run_step "E5b_reference_table" "reference: N candidates 2,3,5 for comparison only (does not change N)" \
      "${PY_ABS}" scripts/run/run_p5_b6_01_n_analysis.py --input-path "${CURATED}" --mode sample \
        --analysis-scope reference --sample-percent "${SAMPLE_PCT}" \
        --output-dir "${REPORT_REL}/reference" --run-id "${RUN_ID}_reference"
  fi
fi

# --- E5c gap diagnostic on the same sample (no ranking, no labels) ------------
if gate "E5c_gap_diagnostic" "E3_hdfs_preflight" "E4a_p5_unit_tests"; then
  if [ "${SAMPLE_PCT}" != "" ] && awk "BEGIN{exit !(${SAMPLE_PCT} > 1)}"; then
    add_step "E5c_gap_diagnostic" "FAIL" "2" "SAMPLE_PCT > 1 not allowed for the diagnostic"
  else
    run_step "E5c_gap_diagnostic" "explain every session bucket on the same whole-session sample" \
      "${PY_ABS}" scripts/run/run_p5_b6_01_n_analysis.py --input-path "${CURATED}" --mode sample \
        --sample-percent "${SAMPLE_PCT}" --diagnose-only \
        --output-dir "${REPORT_REL}/diagnostic" --run-id "${RUN_ID}_diagnostic"
  fi
fi

# --- E6 full analysis (explicitly enabled only) -------------------------------
if [ "${FULL_RUN}" = "1" ] && [ "${CONFIRM_FULL}" = "1" ]; then
  if gate "E6_full_analysis" "E5_sample_analysis"; then
    run_step "E6_full_analysis" "FULL Curated analysis (explicitly confirmed)" \
      "${PY_ABS}" scripts/run/run_p5_b6_01_n_analysis.py --input-path "${CURATED}" --mode full \
        --confirm-full-run --output-dir "${REPORT_REL}/full" --run-id "${RUN_ID}_full"
  fi
else
  add_step "E6_full_analysis" "SKIPPED" "-" "full run not requested (needs FULL_RUN=1 and CONFIRM_FULL=1)"
  echo "SKIPPED E6_full_analysis: FULL_RUN and CONFIRM_FULL not both set"
fi

exit 0
