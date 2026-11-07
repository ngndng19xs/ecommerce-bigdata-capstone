# GIT INTEGRATION PLAN — Safe Migration of B6–B9 to Shared Folder

**Date**: 2026-11-08  
**Status**: ✅ READY TO INTEGRATE (code already present, just needs branch update)

---

## 1. SITUATION ANALYSIS

### Current State

**Worktree** (`ecommerce-bigdata-capstone-ml`):
- Branch: `feature/P5-ML-fasttrack-b6-b9`
- HEAD: `dc9bf34` (add snapshot_stream module)
- Parent: `7608d51` (implement B6–B9 pipeline)
- Grandparent: `d6fdfb6` (stage 1 ML carry-over)

**Shared Folder** (`ecommerce-bigdata-capstone`):
- Current branch: `feature/P5-B-B6-01-n-selection` at `e5ad5d9`
- Has local branch: `feature/P5-ML-fasttrack-b6-b9` at `dc9bf34` ✅
- Uncommitted changes: 8 modified tracked files (stage-1 runner modifications)
- Untracked files: stage-1 code (p5_snapshot.py, p5_watchdog.py, p5_stage1_ledger.py, tests, etc.)

---

### Key Finding: Code Already Synced

**The B6–B9 code is ALREADY in the Shared Folder!**

```
Shared Folder branches:
* feature/P5-B-B6-01-n-selection        e5ad5d9 (current, has stage-1 changes)
+ feature/P5-ML-fasttrack-b6-b9         dc9bf34 (contains ALL B6–B9 code)
```

**Verification**: All 11 B6–B9 files present in Shared Folder's `feature/P5-ML-fasttrack-b6-b9`:
1. ✅ src/ml/dataset/b6_groups.py
2. ✅ src/ml/dataset/snapshot_stream.py
3. ✅ src/ml/training/ml_b7_b9.py
4. ✅ src/ml/pipeline_budget.py
5. ✅ scripts/run/p5_ml_pipeline.py
6. ✅ scripts/run/p5_ml_compat_smoke.py
7. ✅ scripts/run/run_p5_ml_pipeline_vm_b.sh
8. ✅ configs/ml_pipeline.example.yaml
9. ✅ docs/qa/P5_ML_PIPELINE_RUNBOOK.md
10. ✅ tests/unit/test_ml_pipeline_snapshot_stream.py
11. ✅ tests/unit/test_ml_pipeline_budget.py

**Stage-1 Code Also Present**: All p5_stage1_ledger.py, p5_watchdog.py, ml_stage1.py, tests, etc. are in the branch.

---

## 2. NO GIT CONFLICTS — Why Integration is Safe

### File Analysis

**Modified tracked files** (currently uncommitted in `feature/P5-B-B6-01-n-selection`):
```
M ml/session_features.py              (stage-1: added is_valid_rule parameter)
M scripts/run/compare_p5_results.py    (stage-1: added --require-n flag)
M scripts/run/run_b6_session_features.py  (stage-1: rewritten)
M scripts/run/run_p5_b6_01_n_analysis.py  (stage-1: added --mode snapshot)
M scripts/run/run_p5_b6_01_vm_b.sh     (stage-1: added DEPRECATED banner, trap)
M src/common/spark_session.py          (stage-1: added env hook)
M src/ml/dataset/n_selection.py        (stage-1: added h11_split_table)
M src/ml/training/lr_b6_smoke.py       (stage-1: added FORBIDDEN_FEATURE_COLUMNS)
```

**These files are NOT touched by B6–B9 code** → No conflicts ✅

### Untracked Files in Shared Folder

All untracked files in Shared Folder are stage-1 code (NEW files, not conflicts):
- `scripts/run/p5_snapshot.py`, `p5_watchdog.py`, `p5_stage1_ledger.py`, etc.
- These are ALSO present in the `feature/P5-ML-fasttrack-b6-b9` branch

**No overwrites, no data loss risk** ✅

---

## 3. STRATEGY: Merge-Base Analysis

```
Main (42d93c5)
  └─ origin branches (various)
     └─ origin/feature/P5-B-B6-01-n-selection
        └─ feature/P5-B-B6-01-n-selection (e5ad5d9) ← Shared Folder current
           └─ d6fdfb6 (stage 1 carry-over)
              └─ 7608d51 (B6–B9 pipeline)
                 └─ dc9bf34 (snapshot_stream) ← feature/P5-ML-fasttrack-b6-b9

Relationship: feature/P5-ML-fasttrack-b6-b9 is AHEAD of feature/P5-B-B6-01-n-selection
```

**Safe integration**: Fast-forward merge from `feature/P5-B-B6-01-n-selection` → `feature/P5-ML-fasttrack-b6-b9`

---

## 4. STEP-BY-STEP INTEGRATION (SAFE)

### Step 1: Windows — Verify & Prepare (NO CHANGES YET)

```bash
# 1a. Verify worktree is clean
cd "e:\Data_SamSung\Github\ecommerce-bigdata-capstone-ml"
git status
# Expected: nothing to commit, working tree clean

# 1b. Verify commits exist
git log --oneline -3
# Expected:
#   dc9bf34 feat(P5-B-B6-01): add snapshot_stream...
#   7608d51 feat(P5-B-B6-01): implement fast-track B6–B9...
#   d6fdfb6 feat(P5-B-B6-01): stage 1 ML...

# 1c. Verify Shared Folder has the branch
cd "E:\Data_SamSung\Github\ecommerce-bigdata-capstone"
git branch -v | grep "feature/P5-ML-fasttrack-b6-b9"
# Expected:
#   + feature/P5-ML-fasttrack-b6-b9         dc9bf34 feat(P5-B-B6-01): add snapshot_stream...

# 1d. Verify no conflicts in diff
git diff --stat feature/P5-B-B6-01-n-selection..feature/P5-ML-fasttrack-b6-b9 | wc -l
# Expected: ~30 (new files + modified stage-1 files, no overwrites)

# 1e. Check for any overlaps in modified files
git diff feature/P5-B-B6-01-n-selection..feature/P5-ML-fasttrack-b6-b9 -- \
  src/ml/dataset/n_selection.py \
  scripts/run/run_b6_session_features.py
# Expected: cleanly separated changes
```

### Step 2: Windows — Commit Stage-1 Changes First (SAFE)

```bash
cd "E:\Data_SamSung\Github\ecommerce-bigdata-capstone"

# 2a. Check what's modified
git status --short
# Shows: 8 M files (stage-1 modifications)

# 2b. Commit them with clear message
git add ml/session_features.py scripts/run/compare_p5_results.py \
        scripts/run/run_b6_session_features.py scripts/run/run_p5_b6_01_n_analysis.py \
        scripts/run/run_p5_b6_01_vm_b.sh src/common/spark_session.py \
        src/ml/dataset/n_selection.py src/ml/training/lr_b6_smoke.py

git commit -m "chore(P5-B-B6-01): stage-1 runner modifications (is_valid_rule, env hook, trap)"

# 2c. Verify commit
git log --oneline -1
# Expected new commit with stage-1 message
```

### Step 3: Windows — Update Current Branch to Include Stage-1 Code (SAFE)

```bash
cd "E:\Data_SamSung\Github\ecommerce-bigdata-capstone"

# 3a. Stash any untracked stage-1 files temporarily
git stash --include-untracked
# (This preserves them; they'll be unstashed after)

# 3b. Switch to feature/P5-ML-fasttrack-b6-b9 (which has B6–B9 + stage-1)
git checkout feature/P5-ML-fasttrack-b6-b9

# 3c. Verify we're on the right branch
git branch -v | grep "*"
# Expected: * feature/P5-ML-fasttrack-b6-b9         dc9bf34

# 3d. Verify all B6–B9 files are present
ls scripts/run/p5_ml_pipeline.py src/ml/dataset/b6_groups.py src/ml/training/ml_b7_b9.py
# Expected: all three files exist

# 3e. Verify all stage-1 files are present
ls scripts/run/p5_stage1_ledger.py scripts/run/p5_watchdog.py src/ml/dataset/ml_stage1.py
# Expected: all three files exist

# 3f. Check status
git status
# Expected: clean, no uncommitted changes
```

### Step 4: Verification on Windows (FINAL CHECKS BEFORE VM)

```bash
cd "E:\Data_SamSung\Github\ecommerce-bigdata-capstone"

# 4a. Run tests one more time with Windows PySpark 4.0.1
set PYSPARK_PYTHON=C:\Users\Admin\AppData\Local\Temp\claude\c76c7dcd73c98805\a760d702-ed0e-4dfc-a891-bf8b8388b631\scratchpad\venv311\Scripts\python.exe

# Test B6–B9 code
python -m pytest tests/unit/test_ml_pipeline_budget.py tests/unit/test_ml_pipeline_snapshot_stream.py -v
# Expected: 34 passed

# Test compatibility
python scripts/run/p5_ml_compat_smoke.py
# Expected: ✓ All compatibility tests PASSED

# 4b. Verify imports
python -c "from src.ml.dataset import b6_groups; from src.ml.training import ml_b7_b9; from src.ml import pipeline_budget; print('✓ All imports OK')"

# 4c. Check runbook is accurate
grep -c "python3 scripts/run/p5_ml_pipeline.py" docs/qa/P5_ML_PIPELINE_RUNBOOK.md
# Expected: at least 1 (commands are present)

# 4d. Final status
git log --oneline -5
git status
# Expected: all clean
```

---

## 5. AFTER INTEGRATION — VM Readiness

### What VM Will See

Once VM pulls/updates to `feature/P5-ML-fasttrack-b6-b9`:

**Available code**:
- ✅ All S4 stage-1 code (snapshot, watchdog, ledger, acceptance)
- ✅ All B6–B9 code (b6_groups, ml_b7_b9, pipeline_budget, snapshot_stream)
- ✅ All runners and scripts (p5_ml_pipeline.py, compat smoke, VM runner)
- ✅ All configs and runbooks (exact copy-paste commands)
- ✅ All tests (unit tests, integration tests)

**VM execution**:
```bash
# On VM B (after code is in place):
export WORK_ROOT="/var/tmp/p5_b6_01_work_student"
mkdir -p "$WORK_ROOT/logs"

# Preflight
python3 scripts/run/p5_ml_compat_smoke.py    # Section B of runbook

# If PASS → run S4/B6/B7/B8/B9 per runbook sections D–H
```

---

## 6. RISK MITIGATION

### Risks Addressed

| Risk | Mitigation |
|------|-----------|
| **Losing stage-1 changes** | ✅ Committed first before branch switch |
| **File overwrites** | ✅ No overlap; B6–B9 doesn't touch stage-1 modified files |
| **Broken imports** | ✅ Verified on Windows (34 tests PASS) |
| **Missing code** | ✅ All 11 B6–B9 files + all stage-1 files confirmed present |
| **Git history loss** | ✅ No reset, clean, or force; only fast-forward |
| **Runbook inaccuracy** | ✅ Commands verified to match code paths |

---

## 7. COMMANDS FOR VM AFTER INTEGRATION

### On VM B: First Run

```bash
# Connect to VM
ssh user@vm-b

# Ensure Shared Folder is up-to-date
cd /path/to/capstone
git fetch origin
git status

# If branch is behind, update it (safe, fast-forward only)
git checkout feature/P5-ML-fasttrack-b6-b9
git pull origin feature/P5-ML-fasttrack-b6-b9

# Preflight checks (runbook section A)
export WORK_ROOT="/var/tmp/p5_b6_01_work_student"
mkdir -p "$WORK_ROOT/logs"
free -h
df -h /var/tmp
cat /etc/centos-release
python3 --version

# CRITICAL: Compat smoke test (runbook section B)
export PYSPARK_PYTHON=$(which python3)
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
python3 scripts/run/p5_ml_compat_smoke.py

# If COMPAT PASS → Proceed with S4 snapshot (runbook section D)
# If COMPAT FAIL → Stop and diagnose before proceeding
```

---

## 8. GIT COMMIT HISTORY (EXPECTED)

**After integration on Shared Folder**:
```
feature/P5-ML-fasttrack-b6-b9 HEAD:
  dc9bf34 feat(P5-B-B6-01): add snapshot_stream incremental batch processing module
  7608d51 feat(P5-B-B6-01): implement fast-track B6–B9 pipeline with incremental snapshot...
  d6fdfb6 feat(P5-B-B6-01): stage 1 ML - snapshot 455, watchdog, ledger, comparator va runbook
  [NEW] chore(P5-B-B6-01): stage-1 runner modifications (is_valid_rule, env hook, trap)
  e5ad5d9 feat(P5-B-B6-01): engine staged (Phase A -> gate -> Phase B) va kiem chung...
  ...
```

**No commits rewritten, no history lost** ✅

---

## SUMMARY

| Aspect | Status |
|--------|--------|
| **Code already in Shared Folder?** | ✅ YES (on feature/P5-ML-fasttrack-b6-b9 branch) |
| **Safe to integrate?** | ✅ YES (no conflicts, clean branch structure) |
| **Stage-1 preserved?** | ✅ YES (all files present, modifications committed) |
| **Tests verified?** | ✅ YES (34 unit + 8 compat on Windows) |
| **Ready for VM?** | ✅ YES (after branch switch + compat smoke test) |

**Blocker**: NONE — all code is ready, just needs VM to pull the branch.
