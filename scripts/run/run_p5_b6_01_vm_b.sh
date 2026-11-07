#!/usr/bin/env bash
# DEPRECATED for stage 1 (P5-B-B6-01 ML). Use scripts/run/run_p5_ml_stage1_vm_b.sh instead.
# This file is kept to reproduce the P5 evidence. Its fixes: an interrupt (INT, TERM, HUP) gives
# INCOMPLETE with exit 3, and OVERALL is never PASS when a mandatory step is missing.
#
# P5-B-B6-01 acceptance runner for Linux VM B (N candidate analysis, selected_n=3).
#
# Usage (repository mounted at /media/sf_ecommerce-bigdata-capstone):
#   cd /media/sf_ecommerce-bigdata-capstone
#   RUN_ID=p5_staged_cmp_1 bash scripts/run/run_p5_b6_01_vm_b.sh                 # 0.1% sample + comparisons
#   RUN_ID=p5_bench_1pct_1 SAMPLE_PCT=1 bash scripts/run/run_p5_b6_01_vm_b.sh    # 1% benchmark
#   FULL_RUN=1 CONFIRM_FULL=1 RUN_ID=p5_full_n3_1 bash scripts/run/run_p5_b6_01_vm_b.sh   # only after GO
#   bash scripts/run/run_p5_b6_01_vm_b.sh --help
#
# Steps (a step runs only when its prerequisites PASS):
#   E1   environment (python, pyspark, java, local UTC offset, TZ -> environment.json)
#   E2   source integrity (reports/P5_B_B6_01/source_manifest.json)                   -> gates the rest
#   E3   HDFS Curated preflight, metadata only (no row scan)
#   E4a  tests/unit/test_p5_b6_01_n_selection.py (JUnit)
#   E4b  regression B6: test_b6_02, test_ml_features, test_b6_01_edges (one process each)
#   E5   legacy engine, acceptance N=3, whole-session sample SAMPLE_PCT (oracle)
#   E5b  legacy engine, reference N=2/3/5 (comparison only, never changes N)
#   E5c  gap diagnostic on the same sample (no ranking, no labels)
#   E5s  staged engine, semi_join,   disk_persist   (acceptance)
#   E5t  staged engine, time_filter, disk_persist   (acceptance)
#   E5p  staged engine, semi_join,   staging Parquet(acceptance)
#   E5r  staged engine, semi_join,   reference N=2/3/5
#   E5k  staged engine, semi_join, run with TZ=Asia/Seoul (timezone independence)
#   E5cmp business comparison: baseline vs E5/E5s/E5t/E5p/E5k, and E5b vs E5r (must be identical)
#   E6p  Full resource preflight (RAM, swap, local disk, HDFS, other Spark jobs, new output dir)
#   E6   FULL analysis: ONLY with FULL_RUN=1 AND CONFIRM_FULL=1, and only if E1-E5cmp and E6p PASS
#
# Optional env: SAMPLE_PCT (default 0.1, max 1), CURATED, BASELINE_JSON, STAGED_CHECKS=0,
#   WORK_ROOT (default $HOME/p5_b6_01_work; spark.local.dir, event logs, staging live there, NOT in
#   the shared folder), MONITOR_INTERVAL (s, default 15), FULL_ENGINE (staged|legacy),
#   FULL_RANKING_INPUT (semi_join|time_filter), FULL_MATERIALIZE (disk_persist|staging),
#   FULL_MAX_WINDOW_GROUP (int, blocks Phase B when exceeded), FULL_AQE (keep|on|off),
#   PREFLIGHT_MIN_MEM_MB (1200), PREFLIGHT_MIN_LOCAL_GB (30), PREFLIGHT_MIN_HDFS_GB (5).
#
# Output: reports/P5_B_B6_01/<RUN_ID>/ (never overwrites an existing RUN_ID).
# Status: PASS, FAIL, BLOCKED, SKIPPED.  Exit: 0 PASS, 1 FAIL, 2 BLOCKED.
set -u

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  sed -n '1,38p' "$0"
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
STAGED_CHECKS="${STAGED_CHECKS:-1}"
BASELINE_JSON="${BASELINE_JSON:-reports/P5_B_B6_01/p5_n3_utc_v1/sample/n_candidate_result.json}"
WORK_ROOT="${WORK_ROOT:-${HOME}/p5_b6_01_work}"
MONITOR_INTERVAL="${MONITOR_INTERVAL:-15}"
FULL_ENGINE="${FULL_ENGINE:-staged}"
FULL_RANKING_INPUT="${FULL_RANKING_INPUT:-semi_join}"
FULL_MATERIALIZE="${FULL_MATERIALIZE:-disk_persist}"
FULL_MAX_WINDOW_GROUP="${FULL_MAX_WINDOW_GROUP:-}"
FULL_AQE="${FULL_AQE:-keep}"
FULL_REQUESTED=0
if [ "${FULL_RUN}" = "1" ] && [ "${CONFIRM_FULL}" = "1" ]; then FULL_REQUESTED=1; fi

if [[ "${REPO}" == *sf_CapstoneRepo* ]]; then
  echo "ERROR: /media/sf_CapstoneRepo is outdated. Use /media/sf_ecommerce-bigdata-capstone." >&2
  exit 2
fi
REQUIRED_FILES=(
  "src/ml/dataset/n_selection.py"
  "scripts/run/run_p5_b6_01_n_analysis.py"
  "scripts/run/source_manifest_p5.py"
  "scripts/run/preflight_b7_01.py"
  "scripts/run/compare_p5_results.py"
  "scripts/run/p5_resource_preflight.py"
  "scripts/run/p5_eventlog_metrics.py"
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
if awk "BEGIN{exit !(${SAMPLE_PCT} > 1)}"; then
  echo "ERROR: SAMPLE_PCT=${SAMPLE_PCT} > 1 is not allowed for sample steps." >&2
  exit 2
fi
PY_ABS="$(command -v "${PY}" || true)"
if [ -z "${PY_ABS}" ]; then echo "ERROR: interpreter not found: ${PY}" >&2; exit 2; fi

cd "${REPO}" || exit 2
REPORT_REL="reports/P5_B_B6_01/${RUN_ID}"
REPORT_DIR="${REPO}/${REPORT_REL}"
WORK_DIR="${WORK_ROOT}/${RUN_ID}"
if [ -e "${REPORT_DIR}" ] || [ -e "${WORK_DIR}" ]; then
  echo "ERROR: ${REPORT_DIR} or ${WORK_DIR} already exists. Choose a new RUN_ID; evidence is never overwritten." >&2
  exit 2
fi
mkdir -p "${REPORT_DIR}" "${WORK_DIR}/spark_local" "${WORK_DIR}/eventlog" "${WORK_DIR}/staging" || exit 2
LOG="${REPORT_DIR}/vm_b_run.log"
SUMMARY="${REPORT_DIR}/vm_b_summary.txt"
exec > >(tee -a "${LOG}") 2>&1

export CAPSTONE_SPARK_MASTER="${CAPSTONE_SPARK_MASTER:-local[2]}"
export CAPSTONE_DRIVER_MEMORY="${CAPSTONE_DRIVER_MEMORY:-1g}"
export CAPSTONE_SHUFFLE_PARTITIONS="${CAPSTONE_SHUFFLE_PARTITIONS:-4}"
# spark.local.dir and event logs on the VM local disk (not the VirtualBox shared folder).
export PYSPARK_SUBMIT_ARGS="--driver-memory ${CAPSTONE_DRIVER_MEMORY} --conf spark.local.dir=${WORK_DIR}/spark_local --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false --conf spark.eventLog.dir=file://${WORK_DIR}/eventlog pyspark-shell"
export PYSPARK_PYTHON="${PY_ABS}"
export PYSPARK_DRIVER_PYTHON="${PY_ABS}"

STEPS=()
FINALIZED=0
MONITOR_PID=""
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
run_step() {  # exit 0 -> PASS, 2 -> BLOCKED, anything else -> FAIL
  local name="$1" note="$2"; shift 2
  echo; echo "===== ${name}: ${note} ====="
  local t0; t0=$(date +%s)
  "$@"
  local rc=$?
  local dt=$(( $(date +%s) - t0 ))
  case $rc in
    0) add_step "$name" "PASS" "$rc" "${note} (${dt}s)" ;;
    2) add_step "$name" "BLOCKED" "$rc" "${note} (${dt}s)" ;;
    *) add_step "$name" "FAIL" "$rc" "${note} (${dt}s)" ;;
  esac
  echo "${name} exit=${rc} elapsed=${dt}s"
  return $rc
}
eventlog_metrics() {  # best effort: measured shuffle/spill/memory for the last Spark application
  "${PY_ABS}" scripts/run/p5_eventlog_metrics.py --log-dir "${WORK_DIR}/eventlog" --latest \
    --out "${REPORT_REL}/$1_eventlog_metrics.json" || echo "WARN: no event log metrics for $1"
}
start_monitor() {
  ( while true; do
      echo "--- $(date -u +%Y-%m-%dT%H:%M:%SZ)"
      free -m | sed -n '2,3p'
      df -h "${WORK_DIR}" | tail -1
      du -sh "${WORK_DIR}/spark_local" 2>/dev/null | cut -f1 | sed 's/^/spark_local=/'
      sleep "${MONITOR_INTERVAL}"
    done ) > "${REPORT_DIR}/resource_monitor.log" 2>&1 &
  MONITOR_PID=$!
}

finalize() {
  [ "${FINALIZED}" = "1" ] && return
  FINALIZED=1
  if [ -n "${MONITOR_PID}" ]; then kill "${MONITOR_PID}" 2>/dev/null; fi
  local overall="PASS" e n s
  for e in "${STEPS[@]:-}"; do
    IFS='|' read -r _ s _ _ <<< "$e"
    if [ "$s" = "FAIL" ]; then overall="FAIL"; fi
  done
  if [ "${overall}" != "FAIL" ]; then
    for e in "${STEPS[@]:-}"; do
      IFS='|' read -r n s _ _ <<< "$e"
      # Full steps only count when a Full run was explicitly requested.
      if [ "${FULL_REQUESTED}" != "1" ] && { [ "$n" = "E6_full_analysis" ] || [ "$n" = "E6p_full_resource_preflight" ]; }; then continue; fi
      if [ "$s" = "BLOCKED" ] || [ "$s" = "SKIPPED" ] || [ "$s" = "NOT_RUN" ]; then overall="BLOCKED"; fi
    done
  fi
  if [ "${INTERRUPTED}" = "1" ] && [ "${overall}" != "FAIL" ]; then overall="INCOMPLETE"; fi
  for n in ${MANDATORY_STEPS}; do
    found=0
    for e in "${STEPS[@]:-}"; do
      IFS='|' read -r en _ _ _ <<< "$e"
      if [ "$en" = "$n" ]; then found=1; fi
    done
    if [ "$found" = "0" ] && [ "${overall}" != "FAIL" ]; then overall="INCOMPLETE"; fi
  done
  {
    echo "TASK=P5-B-B6-01"
    echo "RUN_ID=${RUN_ID}"
    echo "REPO=${REPO}"
    echo "PYTHON=$(${PY_ABS} --version 2>&1)"
    echo "CURATED=${CURATED} SAMPLE_PCT=${SAMPLE_PCT} FULL_RUN=${FULL_RUN} CONFIRM_FULL=${CONFIRM_FULL}"
    echo "SPARK_MASTER=${CAPSTONE_SPARK_MASTER} DRIVER_MEMORY=${CAPSTONE_DRIVER_MEMORY} SHUFFLE_PARTITIONS=${CAPSTONE_SHUFFLE_PARTITIONS}"
    echo "WORK_DIR=${WORK_DIR}"
    echo "FULL_ENGINE=${FULL_ENGINE} FULL_RANKING_INPUT=${FULL_RANKING_INPUT} FULL_MATERIALIZE=${FULL_MATERIALIZE} FULL_MAX_WINDOW_GROUP=${FULL_MAX_WINDOW_GROUP:-record_only} FULL_AQE=${FULL_AQE}"
    echo "REPORT_DIR=${REPORT_DIR}"
    echo "OVERALL=${overall}"
    echo "NOTE=Official N comes from config selected_n=3 (design decision B). H11 remains PENDING_A_REVIEW."
    echo "--- steps (NAME|STATUS|EXIT|NOTE) ---"
    printf '%s\n' "${STEPS[@]:-}"
  } > "${SUMMARY}"
  echo; echo "===== SUMMARY ====="; cat "${SUMMARY}"
  case "${overall}" in PASS) exit 0 ;; FAIL) exit 1 ;; INCOMPLETE) exit 3 ;; *) exit 2 ;; esac
}
INTERRUPTED=0
trap 'INTERRUPTED=1; finalize' INT TERM HUP
trap finalize EXIT
# Mandatory steps for this configuration. A mandatory step that never reached STEPS is INCOMPLETE.
MANDATORY_STEPS="E1_environment E2_source_integrity E3_hdfs_preflight E4a_p5_unit_tests E4b_regression_b6_group E5_sample_analysis E5b_reference_table E5c_gap_diagnostic"
if [ "${STAGED_CHECKS:-1}" = "1" ]; then MANDATORY_STEPS="${MANDATORY_STEPS} E5s_staged_semi_join E5t_staged_time_filter E5p_staged_staging E5r_staged_reference E5k_staged_kst E5cmp_business_comparison"; fi

ANALYSIS=(scripts/run/run_p5_b6_01_n_analysis.py --input-path "${CURATED}")

# --- E1 environment -----------------------------------------------------------
echo "===== E1 environment ====="
whoami; pwd; free -m; nproc; df -h "${WORK_DIR}"
"${PY_ABS}" -c "
import json, sys, platform, subprocess
info = {'python': sys.version.split()[0], 'platform': platform.platform()}
import os, time, datetime
info['python_local_utc_offset_hours'] = datetime.datetime.now().astimezone().utcoffset().total_seconds() / 3600
info['tz_env'] = os.environ.get('TZ')
info['tzname'] = list(time.tzname)
info['pyspark_submit_args'] = os.environ.get('PYSPARK_SUBMIT_ARGS')
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
start_monitor

# --- E4a P5 unit tests --------------------------------------------------------
if gate "E4a_p5_unit_tests" "E2_source_integrity"; then
  run_step "E4a_p5_unit_tests" "tests/unit/test_p5_b6_01_n_selection.py (JUnit)" \
    bash -c "'${PY_ABS}' -m pytest -q -p no:cacheprovider -rs --junitxml='${REPORT_REL}/p5_junit.xml' tests/unit/test_p5_b6_01_n_selection.py > '${REPORT_REL}/p5_pytest.log' 2>&1"
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

SAMPLE_GATE=("E3_hdfs_preflight" "E4a_p5_unit_tests" "E4b_regression_b6_group")
SAMPLE_ARGS=(--mode sample --sample-percent "${SAMPLE_PCT}")

# --- E5 / E5b / E5c legacy engine (oracle) --------------------------------------
if gate "E5_sample_analysis" "${SAMPLE_GATE[@]}"; then
  run_step "E5_sample_analysis" "legacy, acceptance N=3, sample ${SAMPLE_PCT}%" \
    "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine legacy --analysis-scope acceptance \
      --output-dir "${REPORT_REL}/sample" --run-id "${RUN_ID}_sample"
  eventlog_metrics E5
fi
if gate "E5b_reference_table" "${SAMPLE_GATE[@]}"; then
  run_step "E5b_reference_table" "legacy, reference N=2/3/5 (comparison only)" \
    "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine legacy --analysis-scope reference \
      --output-dir "${REPORT_REL}/reference" --run-id "${RUN_ID}_reference"
fi
if gate "E5c_gap_diagnostic" "E3_hdfs_preflight" "E4a_p5_unit_tests"; then
  run_step "E5c_gap_diagnostic" "explain every session bucket on the same sample" \
    "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --diagnose-only \
      --output-dir "${REPORT_REL}/diagnostic" --run-id "${RUN_ID}_diagnostic"
fi

# --- E5s/E5t/E5p/E5r/E5k staged engine ----------------------------------------
if [ "${STAGED_CHECKS}" = "1" ]; then
  if gate "E5s_staged_semi_join" "${SAMPLE_GATE[@]}"; then
    run_step "E5s_staged_semi_join" "staged, semi_join, disk_persist, acceptance N=3" \
      "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine staged --ranking-input semi_join \
        --materialize disk_persist --analysis-scope acceptance \
        --output-dir "${REPORT_REL}/sample_staged_semi_join" --run-id "${RUN_ID}_staged_sj"
    eventlog_metrics E5s
  fi
  if gate "E5t_staged_time_filter" "${SAMPLE_GATE[@]}"; then
    run_step "E5t_staged_time_filter" "staged, time_filter, disk_persist, acceptance N=3" \
      "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine staged --ranking-input time_filter \
        --materialize disk_persist --analysis-scope acceptance \
        --output-dir "${REPORT_REL}/sample_staged_time_filter" --run-id "${RUN_ID}_staged_tf"
    eventlog_metrics E5t
  fi
  if gate "E5p_staged_staging" "${SAMPLE_GATE[@]}"; then
    run_step "E5p_staged_staging" "staged, semi_join, staging Parquet, acceptance N=3" \
      "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine staged --ranking-input semi_join \
        --materialize staging --staging-dir "file://${WORK_DIR}/staging/e5p" --analysis-scope acceptance \
        --output-dir "${REPORT_REL}/sample_staged_staging" --run-id "${RUN_ID}_staged_stage"
    eventlog_metrics E5p
    du -sh "${WORK_DIR}/staging/e5p" 2>/dev/null | sed 's/^/E5p staging size: /'
  fi
  if gate "E5r_staged_reference" "${SAMPLE_GATE[@]}"; then
    run_step "E5r_staged_reference" "staged, semi_join, reference N=2/3/5" \
      "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine staged --ranking-input semi_join \
        --materialize disk_persist --analysis-scope reference \
        --output-dir "${REPORT_REL}/reference_staged" --run-id "${RUN_ID}_staged_ref"
  fi
  if gate "E5k_staged_kst" "${SAMPLE_GATE[@]}"; then
    run_step "E5k_staged_kst" "staged, semi_join, process TZ=Asia/Seoul (results must not change)" \
      env TZ=Asia/Seoul "${PY_ABS}" "${ANALYSIS[@]}" "${SAMPLE_ARGS[@]}" --engine staged --ranking-input semi_join \
        --materialize disk_persist --analysis-scope acceptance \
        --output-dir "${REPORT_REL}/sample_staged_kst" --run-id "${RUN_ID}_staged_kst"
  fi
  # --- E5cmp business comparison --------------------------------------------------
  compare_all() {
    local base="${REPORT_REL}/sample/n_candidate_result.json"
    local cands=(--candidate "${REPORT_REL}/sample_staged_semi_join/n_candidate_result.json"
                 --candidate "${REPORT_REL}/sample_staged_time_filter/n_candidate_result.json"
                 --candidate "${REPORT_REL}/sample_staged_staging/n_candidate_result.json"
                 --candidate "${REPORT_REL}/sample_staged_kst/n_candidate_result.json")
    if [ "${SAMPLE_PCT}" = "0.1" ] && [ -f "${BASELINE_JSON}" ]; then
      echo "Baseline: accepted UTC result ${BASELINE_JSON} (legacy E5 of this run is also compared)"
      cands+=(--candidate "${base}")
      base="${BASELINE_JSON}"
    else
      echo "Baseline: legacy E5 result of this run (no accepted baseline for SAMPLE_PCT=${SAMPLE_PCT})"
    fi
    "${PY_ABS}" scripts/run/compare_p5_results.py --baseline "${base}" "${cands[@]}" \
      --out "${REPORT_REL}/compare_acceptance.json" || return 1
    "${PY_ABS}" scripts/run/compare_p5_results.py --baseline "${REPORT_REL}/reference/n_candidate_result.json" \
      --candidate "${REPORT_REL}/reference_staged/n_candidate_result.json" \
      --out "${REPORT_REL}/compare_reference.json" || return 1
  }
  if gate "E5cmp_business_comparison" "E5_sample_analysis" "E5b_reference_table" "E5s_staged_semi_join" \
       "E5t_staged_time_filter" "E5p_staged_staging" "E5r_staged_reference" "E5k_staged_kst"; then
    run_step "E5cmp_business_comparison" "acceptance results identical on business values; reference E5b == E5r" \
      compare_all
  fi
else
  add_step "E5cmp_business_comparison" "SKIPPED" "-" "STAGED_CHECKS=0"
fi

# --- E6p / E6 Full (explicitly requested only) --------------------------------
if [ "${FULL_REQUESTED}" = "1" ]; then
  if gate "E6p_full_resource_preflight" "E1_environment" "E2_source_integrity" "E3_hdfs_preflight"; then
    run_step "E6p_full_resource_preflight" "RAM, swap, local disk, HDFS, other Spark jobs, new output dir" \
      "${PY_ABS}" scripts/run/p5_resource_preflight.py --local-dir "${WORK_DIR}/spark_local" \
        --output-dir "${REPORT_REL}/full" --out "${REPORT_REL}/e6p_resource_preflight.json" \
        --min-mem-available-mb "${PREFLIGHT_MIN_MEM_MB:-1200}" --min-local-free-gb "${PREFLIGHT_MIN_LOCAL_GB:-30}" \
        --min-hdfs-free-gb "${PREFLIGHT_MIN_HDFS_GB:-5}"
  fi
  if gate "E6_full_analysis" "E1_environment" "E2_source_integrity" "E3_hdfs_preflight" "E4a_p5_unit_tests" \
       "E4b_regression_b6_group" "E5_sample_analysis" "E5c_gap_diagnostic" "E5s_staged_semi_join" \
       "E5k_staged_kst" "E5cmp_business_comparison" "E6p_full_resource_preflight"; then
    FULL_ARGS=(--mode full --confirm-full-run --engine "${FULL_ENGINE}" --analysis-scope acceptance --aqe "${FULL_AQE}")
    if [ "${FULL_ENGINE}" = "staged" ]; then
      FULL_ARGS+=(--ranking-input "${FULL_RANKING_INPUT}" --materialize "${FULL_MATERIALIZE}")
      if [ "${FULL_MATERIALIZE}" = "staging" ]; then FULL_ARGS+=(--staging-dir "file://${WORK_DIR}/staging/full"); fi
      if [ -n "${FULL_MAX_WINDOW_GROUP}" ]; then FULL_ARGS+=(--max-window-group "${FULL_MAX_WINDOW_GROUP}"); fi
    fi
    run_step "E6_full_analysis" "FULL Curated, N=3, engine ${FULL_ENGINE}" \
      "${PY_ABS}" "${ANALYSIS[@]}" "${FULL_ARGS[@]}" --output-dir "${REPORT_REL}/full" --run-id "${RUN_ID}_full"
    eventlog_metrics E6
  fi
else
  add_step "E6p_full_resource_preflight" "SKIPPED" "-" "full run not requested"
  add_step "E6_full_analysis" "SKIPPED" "-" "full run not requested (needs FULL_RUN=1 and CONFIRM_FULL=1)"
  echo "SKIPPED E6: FULL_RUN and CONFIRM_FULL not both set"
fi

exit 0
