# P5 ML Pipeline Runbook (Fast-Track B6–B9)

**Status**: PENDING_VM_VERIFICATION  
**Target VM**: CentOS 7, Python 3.8.11, PySpark 3.1.2, 2 vCPU, 4697 MiB RAM, 8 GB swap, 37 GB local disk  
**Budget**: 180 minutes (snapshot 45m, B6 50m, B7 30m, B8 15m, B9 15m, reserve 25m)

---

## A. Preflight Checks

### A1. Environment Verification

```bash
# SSH to VM B
ssh user@vm-b

# Check OS and resources
cat /etc/centos-release
free -h
df -h /var/tmp
nproc
java -version
python3 --version

# Expected output:
# - CentOS 7
# - 4697 MiB RAM
# - 37 GB /var/tmp
# - 2 CPU cores
# - Java 8
# - Python 3.8.11
```

### A2. Create Work Directory

```bash
# Initialize work directory (MANDATORY)
export WORK_ROOT="/var/tmp/p5_b6_01_work_student"
mkdir -p "$WORK_ROOT/logs"
mkdir -p "$WORK_ROOT/snapshots"
mkdir -p "$WORK_ROOT/models"
chmod 700 "$WORK_ROOT"

# Verify HDFS connectivity
hadoop fs -ls hdfs://localhost:9000/data/curated/events | head -5

# Expected: list of parquet files with dates like event_date=2019-10-01/
```

### A3. Code Setup

```bash
# Clone or sync the worktree to VM (if not already present)
# Option 1: Clone from GitHub (if available)
git clone https://github.com/your-org/ecommerce-bigdata-capstone.git ~/capstone
cd ~/capstone

# Option 2: Copy from Shared Folder (if on shared mount)
cp -r /mnt/shared/capstone ~/capstone

# Verify structure
ls -la ~/capstone/scripts/run/p5_*.py
ls -la ~/capstone/src/ml/dataset/
```

---

## B. Compatibility Smoke Test

### B1. Run PySpark Compatibility Test

```bash
cd ~/capstone
export PYSPARK_PYTHON=$(which python3)
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON

python3 scripts/run/p5_ml_compat_smoke.py

# Expected output:
# ✓ xxhash64 and pmod work
# ✓ parquet read/write work
# ✓ partition schema handling works
# ✓ VectorAssembler works
# ✓ LogisticRegression fit works
# ✓ BinaryClassificationEvaluator works (AUC=...)
# ✓ All compatibility tests PASSED
```

If this fails, the VM environment is not ready. Do not proceed.

---

## C. Watchdog Self-Test

### C1. Start Watchdog Monitor

```bash
cd ~/capstone

# Run watchdog self-test (checks process monitoring, swap tracking)
python3 scripts/run/p5_watchdog.py selftest \
  --work-dir "$WORK_ROOT" \
  --timeout-seconds 60

# Expected output:
# ✓ Process monitoring OK
# ✓ Swap delta tracking OK
# ✓ Disk usage check OK
# Watchdog self-test PASSED
```

---

## D. Snapshot Build (S4 Redesign)

### D1. Full Snapshot Build

```bash
export RUN_ID="p5_ml_snapshot_$(date +%Y%m%d_%H%M%S)"
export CONFIG_FILE="~/capstone/configs/ml_pipeline.example.yaml"

# Build snapshot with checkpoint/resume support
python3 scripts/run/p5_snapshot.py build \
  --source "hdfs://localhost:9000/data/curated/events" \
  --output-dir "$WORK_ROOT/snapshot" \
  --sample-threshold 455 \
  --n-groups 8 \
  --run-id "$RUN_ID" \
  --checkpoint-dir "$WORK_ROOT/snapshots/$RUN_ID" \
  --batch-size-files 16

# Monitor progress
watch -n 10 "ls -lh $WORK_ROOT/snapshot/batches/ | wc -l"

# Expected duration: ~45 minutes
# If interrupted: resume with same RUN_ID (skips committed batches)
```

### D2. Verify Snapshot

```bash
python3 scripts/run/p5_snapshot.py verify \
  --snapshot-dir "$WORK_ROOT/snapshot" \
  --sample-threshold 455

# Expected output:
# ✓ Manifest valid
# ✓ All batches committed
# ✓ Sample count: ~2,749,877 (455/10000 of 60M events)
# ✓ Invalid-key count: ~X
```

---

## E. B6: Hash Group Analysis

### E1. Analyze Groups (B6-01)

```bash
python3 scripts/run/p5_ml_pipeline.py b6-analyze \
  --snapshot-dir "$WORK_ROOT/snapshot"

# Expected output:
# Groups analyzed: 8
# Total sample rows: 2,749,877
# Per-n candidates:
#   n=1: X eligible sessions
#   n=2: Y eligible sessions
#   n=3: Z eligible sessions (lowest chosen)
```

### E2. Build Features per Group (B6-02)

```bash
# This is internal to the pipeline; features written to:
# $WORK_ROOT/b6_features/group_0/features.parquet
# $WORK_ROOT/b6_features/group_1/features.parquet
# ... etc

# Expected duration: ~50 minutes (8 groups)
```

---

## F. B7: Train Logistic Regression

### F1. Train Model

```bash
python3 scripts/run/p5_ml_pipeline.py b7-train \
  --features-dir "$WORK_ROOT/b6_features" \
  --output-dir "$WORK_ROOT/b7_model"

# Expected output:
# Training on N rows with label distribution: {0: M, 1: K}
# Validation AUC: 0.XX
# Model saved to: $WORK_ROOT/b7_model/model

# Expected duration: ~30 minutes
```

---

## G. B8: Threshold Tuning & Freeze

### G1. Tune Threshold

```bash
python3 scripts/run/p5_ml_pipeline.py b8-tune \
  --model-dir "$WORK_ROOT/b7_model" \
  --val-features "$WORK_ROOT/b6_features"

# Expected output:
# Threshold grid: [0.3, 0.4, 0.5, 0.6, 0.7]
# Best threshold: 0.5
# Best F1: 0.XX
# Model and threshold FROZEN at: $WORK_ROOT/b8_frozen/

# Expected duration: ~15 minutes
```

---

## H. B9: Final Test (PROVISIONAL)

### H1. Run Final Test

```bash
python3 scripts/run/p5_ml_pipeline.py b9-test \
  --model-dir "$WORK_ROOT/b8_frozen" \
  --test-features "$WORK_ROOT/b6_features"

# Expected output:
# Test metrics on held-out split:
#   Precision: 0.XX
#   Recall: 0.XX
#   F1: 0.XX
#   Accuracy: 0.XX
# Status: PROVISIONAL_PENDING_A_REVIEW

# Expected duration: ~15 minutes
```

### H2. Collect Artifacts

```bash
# Results directory
ls -lh $WORK_ROOT/b9_results/

# Key files:
# - model_metadata.json
# - validation_metrics.json
# - final_test_metrics.json
# - leakage_check.json
# - run_manifest.json
# - acceptance_report.md
```

---

## I. Resume After Interruption

### I1. Check Run Status

```bash
# List available RUN_IDs
ls -d $WORK_ROOT/logs/p5_*

# Check ledger for latest run
cat $WORK_ROOT/logs/LATEST_RUN_ID/ledger.json
```

### I2. Resume Snapshot (if interrupted)

```bash
export RUN_ID="p5_ml_snapshot_20261107_110530"  # your RUN_ID

python3 scripts/run/p5_snapshot.py build \
  --source "hdfs://localhost:9000/data/curated/events" \
  --output-dir "$WORK_ROOT/snapshot" \
  --sample-threshold 455 \
  --n-groups 8 \
  --run-id "$RUN_ID" \
  --checkpoint-dir "$WORK_ROOT/snapshots/$RUN_ID"

# Committed batches are skipped; only pending batches are reprocessed.
```

---

## J. GO/NO-GO Decision Criteria

### J1. Pre-Pipeline GO

✓ **GO** if:
- [ ] Compat smoke test PASSED
- [ ] Watchdog selftest PASSED
- [ ] HDFS connectivity verified
- [ ] Snapshot build target ~2.75M rows (455/10000 of ~60M)

✗ **NO-GO** if:
- [ ] Any incompatibility detected (xxhash64, pmod, parquet, LR)
- [ ] Swap < 6 GB available
- [ ] Free disk < 5 GB
- [ ] HDFS unreachable

### J2. Time Budget NO-GO

```bash
# Check elapsed time and forecast
cat $WORK_ROOT/logs/LATEST_RUN_ID/clock.json

# If elapsed + forecast + reserve > 180 min → **NO-GO**, stop before next stage
# Example:
#   elapsed: 120 min
#   forecast_remaining: 45 min (based on recent batches)
#   reserved: 25 min
#   total needed: 190 min > 180 min budget → **NO-GO**
```

---

## K. Troubleshooting

### K1. Snapshot Fails with SWAP kill

```
# Symptom: "Killed" message, swap swap usage > 7GB

# Mitigation:
# 1. Reduce batch size: --batch-size-files 8
# 2. Re-run with same RUN_ID (resumes from last committed batch)
# 3. If still fails, reduce N groups or sample threshold
```

### K2. B7/B8 Model Training OOM

```
# Symptom: "OutOfMemory" or "Error in collecting results"

# Mitigation:
# 1. Reduce spark.executor.memory in config
# 2. Reduce spark.sql.shuffle.partitions
# 3. Split features into smaller chunks
```

### K3. Ledger or Checkpoint Corruption

```bash
# If ledger.json is corrupted:
# 1. Delete corrupt file: rm $WORK_ROOT/logs/$RUN_ID/ledger.json
# 2. Re-run with new RUN_ID
# 3. Check quarantine dir for failed batches:
#    ls $WORK_ROOT/snapshots/$RUN_ID/quarantine/
```

---

## L. Moving Code to Shared Folder (Post-Acceptance)

### L1. Prepare Worktree Commit

Once the pipeline passes manual acceptance on VM, commit the code:

```bash
cd ~/capstone

# Verify current state
git status

# Stage only code files (not reports, models, data)
git add src/ml/dataset/b6_groups.py
git add src/ml/training/ml_b7_b9.py
git add src/ml/pipeline_budget.py
git add src/ml/dataset/snapshot_stream.py
git add scripts/run/p5_ml_pipeline.py
git add scripts/run/p5_ml_compat_smoke.py
git add scripts/run/run_p5_ml_pipeline_vm_b.sh
git add configs/ml_pipeline.example.yaml
git add docs/qa/P5_ML_PIPELINE_RUNBOOK.md

# Commit
git commit -m "feat(P5-B-B6-01): implement fast-track B6–B9 pipeline with incremental snapshot

- S4 redesign: per-day/per-file batches with checkpoint/resume
- B6: deterministic hash grouping (8 groups by default, configurable)
- B7: single LR fit (maxIter=20, regParam=0.1, elasticNet=0)
- B8: threshold grid search and model freeze
- B9: final test and artifact collection
- Watchdog integration for resource monitoring
- Time budget tracking (180-minute total)
- Windows compatibility tests + VM runbook

Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>"
```

### L2. Copy to Shared Folder

```bash
# After acceptance, copy the worktree commit to the Shared Folder repo:

cd ~/capstone
git format-patch -1 HEAD --stdout > /tmp/p5_ml_pipeline.patch

# On a machine with access to the Shared Folder:
cd /mnt/shared/capstone
git apply /tmp/p5_ml_pipeline.patch
# or manually copy files

# Then:
git add src/ml/dataset/b6_groups.py src/ml/training/ml_b7_b9.py ...
git commit -m "feat: P5 ML pipeline B6–B9 (via fast-track worktree)"
git push origin main
```

---

## VM Runbook: Quick Reference

**All steps must run on VM B. Windows is for testing only.**

```bash
# 1. Preflight
export WORK_ROOT=/var/tmp/p5_b6_01_work_student
mkdir -p $WORK_ROOT/{logs,snapshots,models}

# 2. Compat smoke
python3 scripts/run/p5_ml_compat_smoke.py

# 3. Watchdog
python3 scripts/run/p5_watchdog.py selftest

# 4. Snapshot (45 min)
export RUN_ID="p5_ml_snapshot_$(date +%s)"
python3 scripts/run/p5_snapshot.py build \
  --source hdfs://localhost:9000/data/curated/events \
  --output-dir $WORK_ROOT/snapshot \
  --run-id $RUN_ID

# 5. B6 (50 min) — internal
python3 scripts/run/p5_ml_pipeline.py b6-analyze --snapshot-dir $WORK_ROOT/snapshot

# 6. B7 (30 min) — LR training
python3 scripts/run/p5_ml_pipeline.py b7-train \
  --features-dir $WORK_ROOT/b6_features \
  --output-dir $WORK_ROOT/b7_model

# 7. B8 (15 min) — threshold
python3 scripts/run/p5_ml_pipeline.py b8-tune \
  --model-dir $WORK_ROOT/b7_model \
  --val-features $WORK_ROOT/b6_features

# 8. B9 (15 min) — final test
python3 scripts/run/p5_ml_pipeline.py b9-test \
  --model-dir $WORK_ROOT/b8_frozen \
  --test-features $WORK_ROOT/b6_features

# 9. Collect results
ls -lh $WORK_ROOT/b9_results/
cat $WORK_ROOT/b9_results/final_test_metrics.json
```

---

## Final Checklist (PENDING_VM_VERIFICATION)

- [ ] Compat smoke test PASSED on VM
- [ ] Watchdog selftest PASSED
- [ ] Snapshot built successfully (S4 redesign)
- [ ] B6 hash groups analyzed
- [ ] B7 LR training completed with validation AUC
- [ ] B8 threshold tuned and model frozen
- [ ] B9 final test metrics computed (status: PROVISIONAL_PENDING_A_REVIEW)
- [ ] All artifacts collected to $WORK_ROOT/b9_results/
- [ ] Total time < 180 minutes
- [ ] Ledger shows PASS for all stages
- [ ] Code committed to worktree with Sonnet 5.5 trailer
- [ ] Ready to move to Shared Folder post-acceptance

**VM Testing Status**: PENDING_VM_VERIFICATION (all code written; awaiting actual VM execution)
