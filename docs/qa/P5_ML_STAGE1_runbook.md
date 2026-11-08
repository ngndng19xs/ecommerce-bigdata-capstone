# P5-B-B6-01 / ML stage 1: runbook (snapshot 455, pilot, watchdog, resume)

Status: sampled-session pipeline on VM B. Not a Full run. H11 windows remain PENDING_A_REVIEW.
Every output built from the snapshot is `source_scope=SAMPLED_SESSIONS`. Nothing here approves N,
the H11 windows or a threshold.

## 1. Purpose and limits

- Read the Curated source (HDFS, 109,950,743 events, 774 files) ONCE to build a snapshot of
  whole sessions with hash < 455 of 10000 (4.55%, about 5 million events).
- Every later step reads that snapshot on local disk: no more HDFS scans.
- Stage 1 stops at `WAITING_GO`. The main 455 run (B6-01 enforce, full B6-02) and any Full run
  are NOT part of stage 1 and need a human GO file.
- A watchdog stops the Spark job when RAM, swap or disk is in danger. It never restarts anything.
  It cannot guarantee a time, and it cannot free RAM held by other services.

## 2. Where things live

| What | Where | In Shared Folder / repo? |
|---|---|---|
| Curated (source) | HDFS `/data/curated/events` | no (read only) |
| Snapshot 455, subsets, features, Spark scratch, event logs | `/var/tmp/p5_b6_01_work_student/...` | never |
| Small reports (JSON, TXT, forecast, watchdog status) | `reports/P5_B_B6_01/ml_stage1/<RUN_ID>/` | yes, not committed |
| Code, tests, config, this runbook | repo | committed |

`WORK_ROOT` is fixed to `/var/tmp/p5_b6_01_work_student`. The runner refuses any other value.

## 3. Hash, validity and nested samples

- Session bucket: `pmod(xxhash64(user_session), 10000)` through `F.expr` (PySpark 3.1.2 has no `F.pmod`).
- Valid session: `user_session IS NOT NULL AND trim(user_session) <> ''` (the engine rule; Spark
  `trim` removes spaces only, a tab is kept).
- Nested samples: check_10 (< 10) inside pilot_57 (< 57) inside snapshot (< 455).
  check_10 must contain 107,965 events and 22,923 sessions (the P5 0.1% evidence).
- Invalid keys (NULL, empty, spaces only) go to `part=invalid_key`; they are never sessions.

## 4. Steps (runner `scripts/run/run_p5_ml_stage1_vm_b.sh`)

| Step | What | Reads | PASS condition |
|---|---|---|---|
| S0 env | environment, `free`, `swapon`, `df`, other Spark processes, `hdfs dfs -du -s` and `-ls -R` (metadata only) | HDFS metadata | HDFS listing has files, size known |
| S1 preflight | MemAvailable >= 1200 MB, `/var/tmp` >= 20 GB, no other Spark job, new output dir | local | exit 0 |
| S2 tests | targeted and regression tests, one process per file | repo fixtures | all pass |
| S3 watchdog self-test | dummy process group, forced threshold: group stopped, status written | none | status KILLED_LOW_MEMORY, group gone |
| S4 snapshot build | ONE Spark write: `part=sample` and `part=invalid_key`, coalesce 16, manifest | HDFS once | build exit 0, manifest written |
| S4b read once | bytes read by the S4 job / Curated bytes (`hdfs dfs -du -s`) | event log | ratio in [0.8, 1.25] |
| S5 snapshot verify | recompute counts from disk, compare with manifest | local | no problem listed |
| S6a / S6b subsets | pilot_57 and check_10, nested, expected counts for check_10 | local | counts as expected, hash check 0 |
| S7a / S7b check_10 | B6-01 `--mode snapshot`: acceptance N=3, reference N=2,3,5 | local | run exit 0 |
| S7c / S7d compare | comparator with `--require-n 3` and `--require-n 2,3,5` against E5 and E5b | evidence JSON | EQUAL |
| S8 pilot B6-01 | B6-01 on pilot_57, acceptance N=3, `record_only` gate | local | QA pass, no gap, no leakage |
| S9 pilot B6-02 | B6-02 `--input-kind snapshot` on pilot_57, H11 split directories | local | written; exit 5 = WAITING_DECISION |
| S10 cross checks | (c) H11 counts equal B6-01 buckets, (d) eligible and label_1 equal B6-01 | local | PASS |
| S11 report | resource summary and forecast for the main run | ledger | forecast written |
| S12 verdict | OVERALL from the step files | ledger | see section 6 |

The chain stops at the first step that does not PASS. Later steps are never run on a partial input.

## 5. Decisions the runner will not take for you

- `is_valid`: if the snapshot has a column `is_valid`, B6-02 needs `IS_VALID_RULE=ignore` or
  `IS_VALID_RULE=apply`. Without it, S9 ends `WAITING_DECISION`. B6-01 never filters `is_valid`,
  so `ignore` is the only choice that keeps B6-01 and B6-02 on the same events. `apply` needs member A's approval.
- B6-02 gate: `max_session_events_gate` in `configs/ml_stage1.example.yaml` is `null` (record only).
  Set it after the pilot; the agent never sets it by guess.
- GO: a main run needs `WAITING_GO/GO_<RUN_ID>.txt` written by member B by hand (name, date UTC,
  reason). The check exists; the main run is not wired into the runner in stage 1.

## 6. Verdicts and exit codes

| OVERALL | Exit | Meaning |
|---|---|---|
| PASS | 0 | every mandatory step PASS and GO given (not reachable in stage 1) |
| WAITING_GO | 4 | every stage 1 check passed; main run not authorised |
| WAITING_DECISION | 4 | only the B6-02 pilot waits for `IS_VALID_RULE` |
| INCOMPLETE | 3 | a step was KILLED_*, TIMEOUT or INTERRUPTED, or a mandatory step never ran |
| BLOCKED | 2 | preflight or a guard refused (wrong WORK_ROOT, existing output, resume without confirmation) |
| FAIL | 1 | a check failed (comparator not EQUAL, verify problems, B6 cross check) |

A stopped or interrupted run is never PASS. A missing mandatory step is never PASS.

## 7. Watchdog (`scripts/run/p5_watchdog.py`)

Samples every 5 s: MemAvailable (`/proc/meminfo`), swap pages in + out (`/proc/vmstat`, difference
between two samples), free space on `/var/tmp`.

| Condition | Action |
|---|---|
| MemAvailable < 800 MB | warning in the log (once a minute) |
| MemAvailable < 300 MB for 5 s | stop the job group: `KILLED_LOW_MEMORY_EMERGENCY` |
| MemAvailable < 500 MB for 30 s | stop: `KILLED_LOW_MEMORY` |
| swap in + out > 1024 KiB/s over 30 s | stop: `KILLED_SWAP` |
| `/var/tmp` free < 10 GB in 2 samples | stop: `KILLED_DISK` |

Stop = SIGTERM to the job's process group, 20 s grace, then SIGKILL of that group only. Nothing else
is signalled (no `killall`, no `pkill`, no HDFS daemon). The job runs with `setsid`, the watchdog
runs outside that group. Thresholds are starting values; the pilot records the real distribution
(`resource_summary.txt`, step watch files) before anyone changes them.

## 8. Resume

- Each step that PASSES writes a checkpoint: input SHA, code SHA of the stage 1 files, output path, marker.
- A later run may reuse a step only when checkpoint, marker (`_SUCCESS` or manifest), input SHA and code SHA all match.
- A killed, timed-out or interrupted step is run again from the start. Its partial output is
  renamed `<name>.INCOMPLETE-<n>` and never deleted or read.
- Resume is manual: `RESUME_RUN_ID=<RUN_ID> CONFIRM_RESUME=1 WORK_ROOT=... bash scripts/run/run_p5_ml_stage1_vm_b.sh`.
  The cheap steps (S0–S3) always run again.
- A new RUN_ID that needs the shared snapshot or subsets: `REUSE_SHARED=1`. The shared artefact is
  reused only when its manifest SHA (or parent link) matches. S5 verify still runs.

## 9. Commands for member B (VM B)

Preflight (read only):

    free -m; swapon --show; df -hT /var/tmp /media/sf_ecommerce-bigdata-capstone
    ps -ef | grep -E "[s]park"
    hdfs dfs -du -s /data/curated/events

Stage 1 run (new RUN_ID, nothing else running):

    cd /media/sf_ecommerce-bigdata-capstone
    mkdir -p /var/tmp/p5_b6_01_work_student
    WORK_ROOT=/var/tmp/p5_b6_01_work_student RUN_ID=stage1_$(date -u +%Y%m%dT%H%M%SZ) \
      nohup bash scripts/run/run_p5_ml_stage1_vm_b.sh > /var/tmp/p5_b6_01_work_student/stage1_console.log 2>&1 &
    tail -f /var/tmp/p5_b6_01_work_student/stage1_console.log

Follow the run:

    cat reports/P5_B_B6_01/ml_stage1/<RUN_ID>/run_status.json
    tail -3 reports/P5_B_B6_01/ml_stage1/<RUN_ID>/watch/*.json
    df -h /var/tmp; free -m

Do not start a second run while one is active. Do not run Spark jobs by hand in parallel.

## 10. Files to send back for acceptance

`run_status.json`, `steps/*.json`, `snapshot_manifest.json`, `snapshot_verify.json`,
`read_once_check.json`, `compare_check10_*.json`, `pilot_b6_01/n_candidate_result.json`,
`pilot_b6_02/b6_02_snapshot_stats.json`, `pilot_checks.json`, `watchdog_selftest.json`,
`resource_summary.txt`, `forecast.json`. No Parquet, no event logs.

## 11. Acceptance criteria for stage 1 (PASS = WAITING_GO)

A. Targeted and regression tests PASS (Windows and VM).
B. Watchdog self-test PASS on the VM.
C. Snapshot manifest and verify PASS; manifest SHA recorded.
D. Read once: ratio in [0.8, 1.25].
E. check_10: acceptance N=3 EQUAL E5; reference N=2,3,5 EQUAL E5b (`--require-n` respected).
F. pilot_57 B6-01: QA pass, gap 0, leakage 0, no missing N.
G. pilot_57 B6-02: H11 counts equal B6-01 buckets; eligible and label_1 equal B6-01 for train and validation.
H. Resource summary and forecast present.
I. Local commit made, git status shows only expected files.

Stage 1 never claims a time for the main run. The forecast is `pilot seconds x (455 / 57) x 2` per step,
a range to be read with care (sorting and window functions are not linear).

## 12. Not in stage 1

B7, B8 and B9 official pipelines (stage 2); the main 455 run; any Full run over 110 million events;
splitting into 8 session groups (only if the pilot shows it is needed); changes to H11, S2, N=3,
labels, exclusions or SplitBounds; anything that writes to HDFS.
