#!/usr/bin/env bash
# P5-B-B6-01 / ML stage 1 runner for VM B (CentOS 7, Python 3.8.11, Spark 3.1.2, 2 vCPU, 4.6 GB RAM).
#
# Runs steps S0..S11 in order and stops at the first step that does not PASS, then writes the
# verdict (S12). A fully passing run ends as WAITING_GO (exit 4): the main 455 run is NOT started.
# Every heavy step runs in its own process group under the watchdog (scripts/run/p5_watchdog.py).
#
# Usage:
#   WORK_ROOT=/var/tmp/p5_b6_01_work_student RUN_ID=stage1_20261107T100000Z \
#     bash scripts/run/run_p5_ml_stage1_vm_b.sh
#   Resume after a stop (never automatic):
#   WORK_ROOT=/var/tmp/p5_b6_01_work_student RESUME_RUN_ID=stage1_... CONFIRM_RESUME=1 \
#     bash scripts/run/run_p5_ml_stage1_vm_b.sh
#   B6-02 pilot needs a decision when the snapshot has is_valid: IS_VALID_RULE=ignore or apply
#   Print the steps without running anything: DRY_RUN=1 bash scripts/run/run_p5_ml_stage1_vm_b.sh
#
# Exit: 0 PASS (only with GO, not used here), 1 FAIL, 2 BLOCKED, 3 INCOMPLETE, 4 WAITING_GO or WAITING_DECISION.
set -u

REQUIRED_WORK_ROOT="/var/tmp/p5_b6_01_work_student"
WORK_ROOT="${WORK_ROOT:-$REQUIRED_WORK_ROOT}"
if [ "$WORK_ROOT" != "$REQUIRED_WORK_ROOT" ]; then
  echo "BLOCKED: WORK_ROOT must be $REQUIRED_WORK_ROOT (got $WORK_ROOT). Snapshot and outputs never go to the repo or the Shared Folder."
  exit 2
fi
REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
PY="${PY:-python3}"
export PY
CURATED="${CURATED:-/data/curated/events}"
CONFIG="$REPO/configs/ml_stage1.example.yaml"
BOUNDS_CONFIG="$REPO/configs/n_candidate_analysis.example.yaml"
LEDGER="$REPO/scripts/run/p5_stage1_ledger.py"
SNAP_CLI="$REPO/scripts/run/p5_snapshot.py"
B601_CLI="$REPO/scripts/run/run_p5_b6_01_n_analysis.py"
B602_CLI="$REPO/scripts/run/run_b6_session_features.py"
COMPARE="$REPO/scripts/run/compare_p5_results.py"
CHECKS="$REPO/scripts/run/p5_stage1_checks.py"
EVENTMETRICS="$REPO/scripts/run/p5_eventlog_metrics.py"
PREFLIGHT="$REPO/scripts/run/p5_resource_preflight.py"
TESTS="$REPO/scripts/run/p5_stage1_tests.sh"
BASE_E5="$REPO/reports/P5_B_B6_01/p5_staged_cmp_1/sample/n_candidate_result.json"
BASE_E5B="$REPO/reports/P5_B_B6_01/p5_staged_cmp_1/reference/n_candidate_result.json"
THRESHOLD=455
IS_VALID_RULE="${IS_VALID_RULE:-}"
REUSE_SHARED="${REUSE_SHARED:-0}"   # 1: reuse the shared snapshot/subsets/read-once check of an earlier run
REUSE_FLAGS=""
DRY_RUN="${DRY_RUN:-0}"
export DRY_RUN

if [ -n "${RESUME_RUN_ID:-}" ]; then
  RUN_ID="$RESUME_RUN_ID"
  RESUME=1
  if [ "${CONFIRM_RESUME:-0}" != "1" ]; then
    echo "BLOCKED: resume needs CONFIRM_RESUME=1 (manual confirmation)."
    exit 2
  fi
else
  RUN_ID="${RUN_ID:-stage1_$(date -u +%Y%m%dT%H%M%SZ)}"
  RESUME=0
fi
case "$RUN_ID" in
  stage1_*) ;;
  *) echo "BLOCKED: RUN_ID must start with stage1_ (got $RUN_ID)"; exit 2 ;;
esac

RUN_WORK="$WORK_ROOT/ml_stage1/$RUN_ID"
RUN_REPORT="$REPO/reports/P5_B_B6_01/ml_stage1/$RUN_ID"
SNAP_ROOT="$WORK_ROOT/ml_snapshot_455"
SUBSET_DIR="$SNAP_ROOT/subsets"
EVENTLOG_DIR="$RUN_WORK/eventlog"
SPARK_LOCAL="$RUN_WORK/spark_local"
if [ "$RESUME" = "0" ]; then
  if [ -e "$RUN_WORK" ] || [ -e "$RUN_REPORT" ]; then
    echo "BLOCKED: RUN_ID $RUN_ID already exists. Choose a new RUN_ID (never overwrite a run)."
    exit 2
  fi
fi
if [ "$DRY_RUN" != "1" ]; then
  mkdir -p "$RUN_WORK/logs" "$EVENTLOG_DIR" "$SPARK_LOCAL" "$RUN_REPORT"
fi

# Spark settings for every job of this run (read by get_spark in src/common/spark_session.py).
export CAPSTONE_SPARK_MASTER="local[2]"
export CAPSTONE_DRIVER_MEMORY="1g"
export CAPSTONE_SHUFFLE_PARTITIONS="64"
export CAPSTONE_SPARK_CONF_JSON="{\"spark.eventLog.enabled\": \"true\", \"spark.eventLog.compress\": \"false\", \"spark.eventLog.dir\": \"file://${EVENTLOG_DIR}\", \"spark.local.dir\": \"${SPARK_LOCAL}\", \"spark.ui.enabled\": \"false\"}"

on_interrupt() {
  echo "INTERRUPTED: the running step is stopped by the ledger; the chain ends and the verdict is written." >&2
}
trap on_interrupt INT TERM HUP

t() {  # timeout in seconds from configs/ml_stage1.example.yaml
  "$PY" -c "import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))['timeouts_seconds'][sys.argv[2]])" "$CONFIG" "$1" 2>/dev/null || echo 1800
}
sha_of() {  # sha256 of a file, or "none" when it does not exist yet
  if [ -f "$1" ]; then sha256sum "$1" | cut -d' ' -f1; else echo none; fi
}

reuse_or_run() {
  # reuse_or_run NAME MANIFEST SHA_FILE PARENT_MANIFEST TIMEOUT INPUT_SHA OUTPUT MARKER WATCH RESUMABLE -- CMD...
  # With REUSE_SHARED=1 and an existing shared artefact (snapshot, subset, read-once check), the
  # artefact is verified and reused instead of rebuilt. Otherwise the step runs normally.
  local name="$1" manifest="$2" sha_file="$3" parent="$4"
  shift 4
  if [ "$REUSE_SHARED" = "1" ] && [ -f "$manifest" ]; then
    if [ "$DRY_RUN" = "1" ]; then echo "DRY-RUN reuse step=$name manifest=$manifest"; return 0; fi
    local extra=()
    [ -f "$sha_file" ] && extra+=(--sha-file "$sha_file")
    [ -n "$parent" ] && extra+=(--parent-manifest "$parent")
    [ -n "$REUSE_FLAGS" ] && extra+=($REUSE_FLAGS)
    "$PY" "$LEDGER" reuse-check --name "$name" --report-dir "$RUN_REPORT" --manifest "$manifest" ${extra[@]+"${extra[@]}"}
    local rc=$?
    echo "[$name] reused exit=$rc"
    return $rc
  fi
  run_step "$name" "$@"
}

run_step() {
  # run_step NAME TIMEOUT INPUT_SHA OUTPUT MARKER WATCH RESUMABLE -- CMD...
  local name="$1" timeout="$2" input_sha="$3" output="$4" marker="$5" watch="$6" resumable="$7"
  shift 7
  [ "${1:-}" = "--" ] && shift
  local extra=()
  [ "$watch" = "1" ] && extra+=(--watch)
  if [ "$resumable" = "1" ] && [ "$RESUME" = "1" ]; then extra+=(--resume); fi
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN step=$name timeout=$timeout output=${output:-none} marker=${marker:-none}"
    echo "        cmd: $*"
    return 0
  fi
  "$PY" "$LEDGER" run-step --name "$name" --report-dir "$RUN_REPORT" --log-dir "$RUN_WORK/logs" \
    --config "$CONFIG" --timeout "$timeout" --input-sha "$input_sha" \
    --output-path "$output" --marker "$marker" ${extra[@]+"${extra[@]}"} -- "$@"
  local rc=$?
  echo "[$name] exit=$rc"
  return $rc
}

chain() {
  run_step S0_env "$(t env)" none "" "$RUN_REPORT/env.json" 0 0 -- \
    "$PY" "$LEDGER" probe --report-dir "$RUN_REPORT" --source "$CURATED" --work-root "$WORK_ROOT" --repo "$REPO" || return $?
  local src_list_sha
  src_list_sha="$(sha_of "$RUN_REPORT/env/hdfs_ls_r.txt")"

  run_step S1_preflight "$(t env)" none "" "$RUN_REPORT/preflight.json" 0 0 -- \
    "$PY" "$PREFLIGHT" --local-dir "$WORK_ROOT" --output-dir "$RUN_WORK/preflight_check" \
    --out "$RUN_REPORT/preflight.json" --min-mem-available-mb 1200 --min-local-free-gb 20 --min-hdfs-free-gb 5 || return $?

  run_step S2_tests "$(t tests)" none "" "" 0 0 -- bash "$TESTS" || return $?

  run_step S3_watchdog_selftest "$(t watchdog_selftest)" none "" "$RUN_REPORT/watchdog_selftest.json" 0 0 -- \
    "$PY" "$LEDGER" watchdog-selftest --report-dir "$RUN_REPORT" || return $?

  # Recheck memory after S2 tests before starting the large HDFS scan.
  if [ "$DRY_RUN" != "1" ]; then
    echo "[S4 SAFETY] Waiting for MemAvailable >= 1200 MB"
    ready=0
    for i in $(seq 1 18); do
      mem_mb=$(awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo)
      if [ "$mem_mb" -ge 1200 ]; then
        ready=1
        break
      fi
      sleep 5
    done
    if [ "$ready" != "1" ]; then
      echo "BLOCKED: insufficient MemAvailable before S4" >&2
      return 2
    fi
    echo "[S4 SAFETY] PASS: MemAvailable=${mem_mb} MB"
  fi

  reuse_or_run S4_snapshot_build "$SNAP_ROOT/snapshot_manifest.json" "$SNAP_ROOT/snapshot_manifest.sha256" "" \
    "$(t snapshot_build)" "$src_list_sha" "$SNAP_ROOT" \
    "$SNAP_ROOT/snapshot_manifest.json" 1 1 -- \
    "$PY" "$SNAP_CLI" --config "$BOUNDS_CONFIG" build --source "$CURATED" --work-root "$WORK_ROOT" \
    --run-id "$RUN_ID" --threshold "$THRESHOLD" --coalesce 16 --source-listing "$RUN_REPORT/env/hdfs_ls_r.txt" || return $?
  local manifest_sha
  manifest_sha="$(sha_of "$SNAP_ROOT/snapshot_manifest.json")"

  # S4b reads the event log of the snapshot job only (it is the latest finished application).
  if [ "$DRY_RUN" != "1" ]; then
    "$PY" "$EVENTMETRICS" --log-dir "$EVENTLOG_DIR" --latest --out "$RUN_REPORT/S4_eventlog_metrics.json" >/dev/null 2>&1 || true
  fi
  local src_bytes
  src_bytes="$("$PY" -c "import json,sys; print(json.load(open(sys.argv[1]))['hdfs_curated_bytes'])" "$RUN_REPORT/env.json" 2>/dev/null || echo 0)"
  REUSE_FLAGS="--require-pass"
  reuse_or_run S4b_read_once_check "$SNAP_ROOT/read_once_check.json" "" "" 120 "$manifest_sha" "" \
    "$RUN_REPORT/read_once_check.json" 0 0 -- \
    "$PY" "$LEDGER" read-once-check --eventlog-metrics "$RUN_REPORT/S4_eventlog_metrics.json" \
    --source-bytes "$src_bytes" --out "$RUN_REPORT/read_once_check.json" || return $?
  REUSE_FLAGS=""
  if [ -f "$RUN_REPORT/read_once_check.json" ] && [ ! -f "$SNAP_ROOT/read_once_check.json" ]; then
    cp "$RUN_REPORT/read_once_check.json" "$SNAP_ROOT/read_once_check.json"  # kept with the snapshot for later reuse
  fi

  run_step S5_snapshot_verify "$(t snapshot_verify)" "$manifest_sha" "" "$RUN_REPORT/snapshot_verify.json" 0 1 -- \
    "$PY" "$SNAP_CLI" --config "$BOUNDS_CONFIG" verify --snapshot-root "$SNAP_ROOT" \
    --out "$RUN_REPORT/snapshot_verify.json" || return $?

  reuse_or_run S6a_subset_pilot_57 "$SUBSET_DIR/pilot_57/_subset_manifest.json" "" "$SNAP_ROOT/snapshot_manifest.json" \
    "$(t subsets)" "$manifest_sha" "$SUBSET_DIR/pilot_57" \
    "$SUBSET_DIR/pilot_57/_subset_manifest.json" 1 1 -- \
    "$PY" "$SNAP_CLI" --config "$BOUNDS_CONFIG" subset --snapshot-root "$SNAP_ROOT" --threshold 57 --name pilot_57 || return $?
  local pilot_sha
  pilot_sha="$(sha_of "$SUBSET_DIR/pilot_57/_subset_manifest.json")"

  reuse_or_run S6b_subset_check_10 "$SUBSET_DIR/check_10/_subset_manifest.json" "" "$SNAP_ROOT/snapshot_manifest.json" \
    "$(t subsets)" "$manifest_sha" "$SUBSET_DIR/check_10" \
    "$SUBSET_DIR/check_10/_subset_manifest.json" 1 1 -- \
    "$PY" "$SNAP_CLI" --config "$BOUNDS_CONFIG" subset --snapshot-root "$SNAP_ROOT" --threshold 10 --name check_10 \
    --expect-rows 107965 --expect-sessions 22923 || return $?
  local check_sha
  check_sha="$(sha_of "$SUBSET_DIR/check_10/_subset_manifest.json")"

  run_step S7a_check10_acceptance "$(t check10)" "$check_sha" "$RUN_WORK/check10/acceptance" \
    "$RUN_WORK/check10/acceptance/n_candidate_result.json" 1 1 -- \
    "$PY" "$B601_CLI" --mode snapshot --input-path "$SUBSET_DIR/check_10" \
    --input-manifest "$SUBSET_DIR/check_10/_subset_manifest.json" --analysis-scope acceptance \
    --engine staged --output-dir "$RUN_WORK/check10/acceptance" --run-id "$RUN_ID" || return $?

  run_step S7b_check10_reference "$(t check10)" "$check_sha" "$RUN_WORK/check10/reference" \
    "$RUN_WORK/check10/reference/n_candidate_result.json" 1 1 -- \
    "$PY" "$B601_CLI" --mode snapshot --input-path "$SUBSET_DIR/check_10" \
    --input-manifest "$SUBSET_DIR/check_10/_subset_manifest.json" --analysis-scope reference \
    --engine staged --output-dir "$RUN_WORK/check10/reference" --run-id "$RUN_ID" || return $?

  run_step S7c_compare_acceptance 300 "$check_sha" "" "$RUN_REPORT/compare_check10_acceptance.json" 0 0 -- \
    "$PY" "$COMPARE" --baseline "$BASE_E5" --candidate "$RUN_WORK/check10/acceptance/n_candidate_result.json" \
    --require-n 3 --out "$RUN_REPORT/compare_check10_acceptance.json" || return $?

  run_step S7d_compare_reference 300 "$check_sha" "" "$RUN_REPORT/compare_check10_reference.json" 0 0 -- \
    "$PY" "$COMPARE" --baseline "$BASE_E5B" --candidate "$RUN_WORK/check10/reference/n_candidate_result.json" \
    --require-n 2,3,5 --out "$RUN_REPORT/compare_check10_reference.json" || return $?

  run_step S8_pilot_b6_01 "$(t pilot_b6_01)" "$pilot_sha" "$RUN_REPORT/pilot_b6_01" \
    "$RUN_REPORT/pilot_b6_01/n_candidate_result.json" 1 1 -- \
    "$PY" "$B601_CLI" --mode snapshot --input-path "$SUBSET_DIR/pilot_57" \
    --input-manifest "$SUBSET_DIR/pilot_57/_subset_manifest.json" --analysis-scope acceptance \
    --engine staged --output-dir "$RUN_REPORT/pilot_b6_01" --run-id "$RUN_ID" || return $?

  local b602_args=()
  if [ -n "$IS_VALID_RULE" ]; then b602_args+=(--is-valid-rule "$IS_VALID_RULE"); fi
  run_step S9_pilot_b6_02 "$(t pilot_b6_02)" "$pilot_sha" "$RUN_WORK/pilot/b6_02/features" \
    "$RUN_WORK/pilot/b6_02/features/_SUCCESS" 1 1 -- \
    "$PY" "$B602_CLI" --input-kind snapshot --n 3 --snapshot-dir "$SUBSET_DIR/pilot_57" \
    --snapshot-manifest "$SUBSET_DIR/pilot_57/_subset_manifest.json" \
    --output-path "$RUN_WORK/pilot/b6_02/features" --stats-dir "$RUN_REPORT/pilot_b6_02" \
    ${b602_args[@]+"${b602_args[@]}"} || return $?

  run_step S10_cross_checks "$(t checks)" none "" "$RUN_REPORT/pilot_checks.json" 0 0 -- \
    "$PY" "$CHECKS" --subset-dir "$SUBSET_DIR/pilot_57" --features-root "$RUN_WORK/pilot/b6_02/features" \
    --pilot-b6-01 "$RUN_REPORT/pilot_b6_01/n_candidate_result.json" --n 3 --out "$RUN_REPORT/pilot_checks.json" || return $?

  run_step S11_report "$(t report)" none "" "$RUN_REPORT/forecast.json" 0 0 -- \
    "$PY" "$LEDGER" report --report-dir "$RUN_REPORT" --steps "S8_pilot_b6_01 S9_pilot_b6_02" || return $?
  return 0
}

MANDATORY="S0_env S1_preflight S2_tests S3_watchdog_selftest S4_snapshot_build S4b_read_once_check S5_snapshot_verify S6a_subset_pilot_57 S6b_subset_check_10 S7a_check10_acceptance S7b_check10_reference S7c_compare_acceptance S7d_compare_reference S8_pilot_b6_01 S9_pilot_b6_02 S10_cross_checks S11_report"

echo "RUN_ID=$RUN_ID  RESUME=$RESUME  WORK_RUN=$RUN_WORK  REPORT=$RUN_REPORT  DRY_RUN=$DRY_RUN"
chain
CHAIN_RC=$?
echo "chain ended with exit $CHAIN_RC"
if [ "$DRY_RUN" = "1" ]; then
  exit 0
fi
"$PY" "$LEDGER" summarize --report-dir "$RUN_REPORT" --run-id "$RUN_ID" --mandatory "$MANDATORY"
exit $?
