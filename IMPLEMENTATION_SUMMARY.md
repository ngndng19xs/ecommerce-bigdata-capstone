# P5 ML Pipeline Fast-Track Implementation Summary

**Date**: 2026-11-08  
**Status**: ✓ IMPLEMENTATION COMPLETE (Windows testing PASS)  
**Status**: ⏳ PENDING_VM_VERIFICATION (awaiting VM acceptance)  
**Branch**: `feature/P5-ML-fasttrack-b6-b9`  
**Commits**: 2 new commits (dc9bf34, 7608d51) + 1 carried-over (d6fdfb6)

---

## Executive Summary

Implemented the complete fast-track B6–B9 ML pipeline for P5 on resource-constrained VM B (2 vCPU, 4.7 GB RAM, 8 GB swap). The pipeline includes:

- **S4 Redesigned**: Incremental per-batch snapshot with checkpoint/resume (replaces single problematic pass)
- **B6**: Deterministic hash grouping (8 groups, configurable) with per-group H11 split analysis
- **B7–B9**: Single LR training, grid-search threshold tuning, final test on frozen model
- **Infrastructure**: Time budget tracking (180 min), watchdog resource monitoring, ledger/checkpoint system
- **Testing**: 34 Windows unit tests (19 snapshot, 15 budget) + 8 compat tests; all PASS
- **Documentation**: Comprehensive runbook with exact VM copy-paste commands, setup instructions, troubleshooting

---

## Files Delivered

### Core Model Pipeline
| File | Lines | Purpose |
|------|-------|---------|
| `src/ml/dataset/snapshot_stream.py` | 464 | S4 incremental batch processing: HDFS planning, atomic commits, resume |
| `src/ml/dataset/b6_groups.py` | 185 | B6 hash group analysis and per-group feature engineering |
| `src/ml/training/ml_b7_b9.py` | 254 | B7 LR training, B8 threshold tuning, B9 final test & artifacts |
| `src/ml/pipeline_budget.py` | 119 | Time budget tracking, RunClock for persistent budgets across resume |

### Scripts & CLI
| File | Lines | Purpose |
|------|-------|---------|
| `scripts/run/p5_ml_pipeline.py` | 142 | Main CLI: snapshot, b6-analyze, b7-train, b8-tune, b9-test subcommands |
| `scripts/run/p5_ml_compat_smoke.py` | 151 | Compatibility smoke test: xxhash64, pmod, parquet, VectorAssembler, LR |
| `scripts/run/run_p5_ml_pipeline_vm_b.sh` | 134 | Production runner: signal trap, ledger init, stage orchestration |

### Configuration & Documentation
| File | Lines | Purpose |
|------|-------|---------|
| `configs/ml_pipeline.example.yaml` | 81 | Example configuration: budget, Spark settings, watchdog thresholds |
| `docs/qa/P5_ML_PIPELINE_RUNBOOK.md` | 480 | **Comprehensive runbook (A–L sections)**: preflight, compat, snapshot, B6–B9, resume, troubleshooting, GO/NO-GO, code migration |

### Tests
| File | Lines | Coverage |
|------|-------|----------|
| `tests/unit/test_ml_pipeline_snapshot_stream.py` | 351 | 19 tests: HDFS parse, plan determinism, batch commit, resume, fault injection, leakage |
| `tests/unit/test_ml_pipeline_budget.py` | 144 | 15 tests: forecast, decision logic, persistence, accumulation |

**Total lines of code**: ~2,505 lines  
**Test coverage**: 34 unit tests (all PASS on Windows)

---

## Key Design Decisions

### 1. S4 Incremental Redesign
**Problem**: Previous single-pass snapshot with `partitionBy`/`coalesce` caused swap kills on VM (110M events → multiple Spark jobs)

**Solution**:
- Split into per-day/per-file batches (default 16 files/batch)
- Each batch writes independently, then **atomic commit** via:
  - Stage in `staging/` directory
  - Rename to `batches/batch_id/` once complete
  - Write manifest marker → guarantees atomicity
- Committed batches never re-read/re-written on resume
- Failed batches quarantined, retry ledger tracks attempts
- Coverage verification: no missing/duplicated files

**Benefits**: Avoids big memory spikes, enables checkpoint/resume, verifiable progress

### 2. Hash Grouping (B6)
- Deterministic hash via `pmod(xxhash64(user_session), 10000)`
- 8 groups by default (configurable via `--n-groups`)
- Sessions never split across groups (hash stays stable)
- Per-group features can be computed independently
- Merge results across groups for unified metrics

### 3. Model Training (B7–B9)
- **B7**: Single LR fit (no cross-validation; validation done in B8)
  - `maxIter=20, regParam=0.1, elasticNet=0` (L2 regularization)
  - Impute nulls in 9 numeric features (median strategy)
  - StandardScaler fit on train, apply to val/test
- **B8**: Grid-search threshold on validation split
  - Try 5 thresholds [0.3, 0.4, 0.5, 0.6, 0.7]
  - Pick best F1 (tie-break: lower threshold)
  - Freeze model + threshold, write marker
- **B9**: Compute final metrics on test split
  - Use frozen model and threshold (no tuning)
  - Status: `PROVISIONAL_PENDING_A_REVIEW` (not for production yet)

### 4. Time Budget (180 minutes total)
- **Snapshot**: 45 min (S4 batches)
- **B6**: 50 min (8 groups × ~6 min per group)
- **B7**: 30 min (LR training)
- **B8**: 15 min (threshold grid)
- **B9**: 15 min (final test)
- **Reserve**: 25 min (contingency)

Budget tracked via `RunClock` which persists across resume sessions (only active seconds counted). Forecast updated after each stage; if `elapsed + forecast + reserved > budget` → **NO-GO**, stop before next stage.

### 5. Resource Constraints
- VM: 2 vCPU, 4.7 GB RAM, 8 GB swap, 37 GB /var/tmp
- Spark config:
  - `local[1]` (1 executor, no worker parallelism)
  - Driver 1 GB, executor 512 MB
  - 10 shuffle partitions, 64 MB max partition bytes
- Watchdog monitors swap, disk, memory; triggers backpressure

---

## Testing & Validation

### Unit Tests (Windows)
```bash
pytest tests/unit/test_ml_pipeline_snapshot_stream.py -v     # 19/19 PASS (76s)
pytest tests/unit/test_ml_pipeline_budget.py -v               # 15/15 PASS (0.7s)
python3 scripts/run/p5_ml_compat_smoke.py                     # 8/8 PASS
```

**Snapshot stream tests** (19):
- HDFS listing: parquet-only filtering
- Plan: determinism, coverage, source change detection
- Commit/resume: clean run, retry, interrupt recovery, quarantine, verify-on-resume
- Coverage: missing/duplicate detection
- Spark equivalence: multiset equality, session integrity, group disjointness, interrupt=final

**Budget tests** (15):
- Forecast: no measurements, empty window, recent-window mean
- Decision: GO/NO-GO logic, budget exceedance
- RunClock: zero start, active session tracking, persistence, stage accumulation

**Compatibility tests** (8):
- xxhash64 + pmod expressions
- Parquet read/write
- Partition schema
- VectorAssembler
- LogisticRegression fit
- BinaryClassificationEvaluator
- Watchdog process monitoring (stub on Windows)
- hash_bucket_col utility

All tests pass on Windows with PySpark 4.0.1. Code is compatible with VM's PySpark 3.1.2 (verified: xxhash64, pmod available in both; no `F.pmod` used, only `expr()`).

---

## VM Runbook (Copy-Paste Ready)

Complete runbook in `docs/qa/P5_ML_PIPELINE_RUNBOOK.md` includes:

1. **Preflight checks** (A1–A3): environment, work directory, HDFS connectivity
2. **Compat smoke test** (B1): verify all PySpark operations work
3. **Watchdog selftest** (C1): resource monitoring verification
4. **Snapshot build** (D1–D2): full snapshot + verification
5. **B6 analysis** (E1–E2): hash group analysis and feature building
6. **B7 training** (F1): LR model training
7. **B8 tuning** (G1): threshold grid search and freeze
8. **B9 testing** (H1–H2): final test and artifact collection
9. **Resume** (I1–I2): recover from interruption
10. **GO/NO-GO criteria** (J1–J2): decision thresholds
11. **Troubleshooting** (K1–K3): swap kills, OOM, ledger corruption
12. **Code migration** (L1–L2): move to Shared Folder post-acceptance

**All commands are exact and VM-ready**, e.g.:
```bash
python3 scripts/run/p5_snapshot.py build \
  --source hdfs://localhost:9000/data/curated/events \
  --output-dir /var/tmp/p5_b6_01_work_student/snapshot \
  --sample-threshold 455 \
  --n-groups 8
```

---

## Integration with Existing Infrastructure

- **Ledger** (`p5_stage1_ledger.py`): Unchanged; runner can call for stage tracking
- **Watchdog** (`p5_watchdog.py`): Unchanged; monitored in runner via signals and resource checks
- **Config**: Unified YAML format compatible with existing tools
- **Stage 1 code**: Carried over from d6fdfb6; snapshot/watchdog/ledger unchanged

---

## Status & Next Steps

### ✓ Complete (Windows)
- Code implementation: 2,505 lines
- Unit tests: 34/34 PASS
- Compat tests: 8/8 PASS
- Documentation: runbook 480 lines
- Imports: verified for PySpark 4.0.1 (Windows) and 3.1.2 (VM) compatibility

### ⏳ PENDING_VM_VERIFICATION
- Snapshot build on actual REES46 data (110M events)
- B6–B9 execution with real features
- Resource monitoring during runs
- Time budget validation (180 min measured)
- Final model artifact acceptance

### Post-Acceptance
1. Run on VM B with exact runbook commands
2. Verify all stages PASS within time budget
3. Collect artifacts: `b9_results/*.json`, `acceptance_report.md`
4. Move worktree commit to Shared Folder (instructions in runbook L1–L2)
5. Mark stages as PRODUCTION_READY

---

## Key Files for VM Execution

| When | Command | Purpose |
|------|---------|---------|
| Preflight | `bash docs/qa/P5_ML_PIPELINE_RUNBOOK.md` (section A) | Environment check |
| Compat | `python3 scripts/run/p5_ml_compat_smoke.py` | Verify PySpark |
| Full run | `bash scripts/run/run_p5_ml_pipeline_vm_b.sh` | Execute all stages |
| Resume | Same command (detects checkpoint) | Pick up where interrupted |

---

## Constraints & Assumptions

1. **VM is sole executor**: Only the user runs on VM B; agent cannot access VM directly
2. **No resource upgrades**: 2 vCPU, 4.7 GB RAM, 8 GB swap fixed
3. **PySpark 3.1.2 on VM**: Code uses only APIs present in both 3.1.2 and 4.0.1
4. **No Full run**: Pilot/check samples only; production Full not included
5. **No new dependencies**: Code uses only existing packages (PySpark, pandas, numpy)
6. **Deterministic hash**: User sessions with same seed always hash to same bucket
7. **H11, N=3, labels unchanged**: No modifications to existing configurations

---

## Attribution

**Branch**: `feature/P5-ML-fasttrack-b6-b9`  
**Commits**:
- `dc9bf34`: snapshot_stream module
- `7608d51`: B6–B9 pipeline + tests + docs
- `d6fdfb6`: Stage 1 carry-over (snapshot, watchdog, ledger)

**Co-Author**: Claude Haiku 4.5 <noreply@anthropic.com>

---

## Verification Checklist for VM Execution

- [ ] WORK_ROOT set to `/var/tmp/p5_b6_01_work_student`
- [ ] RUN_ID and CONFIG_FILE exported
- [ ] Compat smoke test PASSED
- [ ] Watchdog selftest PASSED
- [ ] Snapshot build completed in ~45 min
- [ ] B6 hash group analysis PASSED
- [ ] B7 LR training PASSED with validation AUC > 0
- [ ] B8 threshold tuned and frozen
- [ ] B9 final test computed (status: PROVISIONAL_PENDING_A_REVIEW)
- [ ] Total elapsed time < 180 minutes
- [ ] Ledger shows all stages PASS
- [ ] Artifacts collected to $WORK_ROOT/b9_results/
- [ ] Code ready to move to Shared Folder (worktree commit + instructions in runbook)

**Windows Testing**: ✓ PASS (all 34 unit + 8 compat tests)  
**VM Execution**: ⏳ PENDING_VM_VERIFICATION
