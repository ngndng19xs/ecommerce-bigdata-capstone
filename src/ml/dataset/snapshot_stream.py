"""P5 ML: incremental snapshot (S4) built batch by batch, with atomic commit and safe resume.

Why: one Spark write over all 774 Curated files was killed three times on VM B (swap). Here the
source files are split into small batches. Each batch is one short Spark job:
read its files -> tag rows -> keep only sampled sessions and invalid keys -> write to a STAGING
directory -> verify -> rename to its final name -> write its manifest.

Design rules
- Selection depends only on the session key: pmod(xxhash64(user_session), 10000) < threshold.
  A session spread over many files or days is kept whole, because every one of its events passes
  the same test, whichever batch reads it. Days are never used to build or cut a session.
- Invalid keys (NULL, empty, blanks) are stored with the tag 'invalid_key'. They are not sessions.
- Each kept row carries two technical columns, stored as ordinary columns (no partition columns,
  no sort, no dynamic partitions): __snapshot_part ('sample' or 'invalid_key') and
  __ml_group (hash bucket % groups for sample rows, -1 for invalid rows). Readers drop both.
- A batch is committed by rename (same filesystem) and a manifest written afterwards. A batch
  directory without a valid manifest is quarantined (renamed, never deleted) and redone.
- Committed batches are never read from HDFS or written again on resume.

The orchestration (run_batches, BatchStore) takes callables, so it is tested without Spark.
"""

import hashlib
import json
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ml.dataset.ml_stage1 import (
    HASH_BUCKETS,
    PART_INVALID,
    PART_SAMPLE,
    check_threshold,
    file_uri,
    hash_bucket_col,
    invalid_key_cond,
    list_files,
    load_json,
    sha256_file,
    write_json,
)

SNAP_PART_COL = "__snapshot_part"
GROUP_COL = "__ml_group"
TAG_COLUMNS = (SNAP_PART_COL, GROUP_COL)
DEFAULT_GROUPS = 8
DEFAULT_FILES_PER_BATCH = 16
PLAN_KIND = "snapshot_stream_plan"
MANIFEST_KIND = "snapshot_stream"

_LS_RE = re.compile(r"^-\S+\s+\S+\s+\S+\s+\S+\s+(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})\s+(.+)$")


class SourceChangedError(RuntimeError):
    """The source file listing differs from the one the plan was built on."""


class DuplicateBatchError(RuntimeError):
    """A batch (or a source file) would be committed twice."""


class BatchFailedError(RuntimeError):
    """A batch failed on every attempt."""


# --- Source listing and plan (pure) -----------------------------------------------------------


def parse_hdfs_ls(text: str) -> List[Dict[str, Any]]:
    """Parquet files from `hdfs dfs -ls -R` output: path, size, mtime. Directories and markers are skipped."""
    files = []
    for line in text.splitlines():
        m = _LS_RE.match(line.strip())
        if not m:
            continue
        size, day, hhmm, path = int(m.group(1)), m.group(2), m.group(3), m.group(4).strip()
        if os.path.basename(path).startswith(("_", ".")) or not path.endswith(".parquet"):
            continue
        files.append({"path": path, "size": size, "mtime": f"{day} {hhmm}"})
    return sorted(files, key=lambda f: f["path"])


def listing_fingerprint(files: Sequence[Dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for f in sorted(files, key=lambda x: x["path"]):
        digest.update(f"{f['path']}|{f['size']}|{f['mtime']}\n".encode("utf-8"))
    return digest.hexdigest()


def plan_batches(files: Sequence[Dict[str, Any]], files_per_batch: int = DEFAULT_FILES_PER_BATCH) -> List[Dict[str, Any]]:
    """Deterministic batches of consecutive files (sorted by path)."""
    if files_per_batch < 1:
        raise ValueError("files_per_batch must be >= 1")
    ordered = sorted(files, key=lambda f: f["path"])
    batches = []
    for i in range(0, len(ordered), files_per_batch):
        chunk = ordered[i:i + files_per_batch]
        batches.append({
            "batch_id": f"b{len(batches) + 1:04d}",
            "files": chunk,
            "bytes": sum(f["size"] for f in chunk),
            "files_sha": listing_fingerprint(chunk),
        })
    return batches


def build_plan(files: Sequence[Dict[str, Any]], source_root: str, schema_json: Dict[str, Any], threshold: int,
               groups: int, files_per_batch: int) -> Dict[str, Any]:
    check_threshold(threshold)
    if groups < 1:
        raise ValueError("groups must be >= 1")
    plan = {
        "kind": PLAN_KIND,
        "source_root": source_root,
        "source_fingerprint": listing_fingerprint(files),
        "source_files": len(files),
        "source_bytes": sum(f["size"] for f in files),
        "schema": schema_json,
        "threshold": int(threshold),
        "groups": int(groups),
        "hash_expression": f"pmod(xxhash64(user_session), {HASH_BUCKETS})",
        "files_per_batch": int(files_per_batch),
        "batches": plan_batches(files, files_per_batch),
    }
    plan["plan_sha256"] = hashlib.sha256(
        json.dumps({k: v for k, v in plan.items() if k != "schema"}, sort_keys=True).encode("utf-8")).hexdigest()
    return plan


def check_source_identity(plan: Dict[str, Any], current_files: Sequence[Dict[str, Any]]) -> None:
    now = listing_fingerprint(current_files)
    if now != plan["source_fingerprint"]:
        raise SourceChangedError(
            f"source listing changed since the plan (plan {plan['source_fingerprint'][:12]}, now {now[:12]}); STOP")


def coverage_problems(plan: Dict[str, Any], manifests: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    """Files missing from, or duplicated across, the committed batches."""
    seen: Dict[str, int] = {}
    for bid, m in manifests.items():
        for f in m.get("source_files", []):
            seen[f["path"]] = seen.get(f["path"], 0) + 1
    planned = [f["path"] for b in plan["batches"] for f in b["files"]]
    return {
        "missing": sorted(p for p in planned if p not in seen),
        "duplicated": sorted(p for p, n in seen.items() if n > 1),
        "unknown": sorted(p for p in seen if p not in set(planned)),
    }


# --- Batch store: staging, atomic commit, quarantine (filesystem only) ----------------------------


class BatchStore:
    def __init__(self, root: str):
        self.root = root
        self.batches = os.path.join(root, "batches")
        self.staging = os.path.join(root, "staging")
        self.quarantine_dir = os.path.join(root, "quarantine")
        for d in (self.batches, self.staging, self.quarantine_dir):
            os.makedirs(d, exist_ok=True)

    def batch_dir(self, bid: str) -> str:
        return os.path.join(self.batches, bid)

    def manifest_path(self, bid: str) -> str:
        return os.path.join(self.batches, f"{bid}.json")

    def staging_path(self, bid: str, attempt: int) -> str:
        return os.path.join(self.staging, f"{bid}.attempt{attempt}")

    def is_committed(self, bid: str) -> bool:
        return os.path.isdir(self.batch_dir(bid)) and os.path.isfile(self.manifest_path(bid))

    def manifest(self, bid: str) -> Dict[str, Any]:
        return load_json(self.manifest_path(bid))

    def quarantine(self, path: str) -> Optional[str]:
        """Move a path under quarantine/ with a unique name. Never deletes."""
        if not os.path.exists(path):
            return None
        base = os.path.basename(path.rstrip("/\\"))
        n = 1
        while os.path.exists(os.path.join(self.quarantine_dir, f"{base}.{n}")):
            n += 1
        target = os.path.join(self.quarantine_dir, f"{base}.{n}")
        os.rename(path, target)
        return target

    def commit(self, bid: str, staging_path: str, manifest: Dict[str, Any]) -> None:
        final = self.batch_dir(bid)
        if os.path.exists(final) or os.path.exists(self.manifest_path(bid)):
            raise DuplicateBatchError(f"batch {bid} is already committed; refusing to commit it twice")
        os.rename(staging_path, final)  # atomic on one filesystem
        write_json(self.manifest_path(bid), manifest)  # the manifest is the commit marker

    def reconcile(self, plan: Dict[str, Any], verify: Optional[Callable[[str, Dict[str, Any]], bool]] = None
                  ) -> Dict[str, Any]:
        """Bring the store to a state where every batch is either committed-and-valid or pending."""
        planned = [b["batch_id"] for b in plan["batches"]]
        quarantined: List[str] = []
        for name in sorted(os.listdir(self.staging)):  # leftovers of an interrupted run
            moved = self.quarantine(os.path.join(self.staging, name))
            quarantined.append(moved)
        known = set(planned)
        for name in sorted(os.listdir(self.batches)):
            bid = name[:-5] if name.endswith(".json") else name
            if bid not in known:
                raise SourceChangedError(f"batch {bid} exists in the store but not in the plan; STOP")
        committed: List[str] = []
        pending: List[str] = []
        for bid in planned:
            has_dir = os.path.isdir(self.batch_dir(bid))
            has_man = os.path.isfile(self.manifest_path(bid))
            if has_dir and has_man:
                ok = True
                if verify is not None:
                    try:
                        ok = bool(verify(bid, self.manifest(bid)))
                    except Exception:  # noqa: BLE001 - an unreadable batch is not committed
                        ok = False
                if ok:
                    committed.append(bid)
                    continue
                quarantined.append(self.quarantine(self.batch_dir(bid)))
                quarantined.append(self.quarantine(self.manifest_path(bid)))
            elif has_dir:
                quarantined.append(self.quarantine(self.batch_dir(bid)))
            elif has_man:
                quarantined.append(self.quarantine(self.manifest_path(bid)))
            pending.append(bid)
        return {"committed": committed, "pending": pending, "quarantined": [q for q in quarantined if q]}


# --- Orchestration with retries (pure: callables do the work) -------------------------------------


def run_batches(plan: Dict[str, Any], store: BatchStore,
                process_fn: Callable[[Dict[str, Any], str], None],
                stats_fn: Callable[[str], Dict[str, Any]],
                max_attempts: int = 2,
                should_continue: Optional[Callable[[List[float], int], bool]] = None,
                log: Callable[[str], None] = print,
                clock: Callable[[], float] = time.time) -> Dict[str, Any]:
    """Process pending batches in plan order.

    process_fn(batch, staging_path) must write the batch output into staging_path.
    stats_fn(staging_path) returns the stats recorded in the manifest.
    should_continue(done_seconds, remaining_batches) is asked before each batch; False stops the loop
    (budget). Every retry is logged in the returned attempts list; none can duplicate data because
    each attempt writes to its own staging directory.
    """
    durations: List[float] = []
    attempts_log: List[Dict[str, Any]] = []
    done_now: List[str] = []
    pending = [b for b in plan["batches"] if not store.is_committed(b["batch_id"])]
    for i, batch in enumerate(pending):
        bid = batch["batch_id"]
        if should_continue is not None and not should_continue(durations, len(pending) - i):
            return {"status": "STOPPED_BY_BUDGET", "committed_now": done_now, "attempts": attempts_log,
                    "durations": durations}
        last_error = None
        for attempt in range(1, max_attempts + 1):
            staging = store.staging_path(bid, attempt)
            t0 = clock()
            try:
                process_fn(batch, staging)
                stats = stats_fn(staging)
                manifest = {
                    "kind": "snapshot_batch",
                    "batch_id": bid,
                    "plan_sha256": plan["plan_sha256"],
                    "source_files": batch["files"],
                    "files_sha": batch["files_sha"],
                    "stats": stats,
                    "files": list_files(staging),
                    "attempt": attempt,
                    "elapsed_seconds": round(clock() - t0, 2),
                }
                store.commit(bid, staging, manifest)
                durations.append(clock() - t0)
                attempts_log.append({"batch_id": bid, "attempt": attempt, "status": "PASS"})
                done_now.append(bid)
                log(f"[snapshot] {bid} committed (attempt {attempt}, {durations[-1]:.1f}s)")
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001 - recorded, partial output quarantined, then retried
                last_error = f"{type(exc).__name__}: {exc}"
                attempts_log.append({"batch_id": bid, "attempt": attempt, "status": "FAILED", "error": last_error})
                log(f"[snapshot] {bid} attempt {attempt} failed: {last_error}")
                store.quarantine(staging)
        if last_error is not None:
            raise BatchFailedError(f"batch {bid} failed on all {max_attempts} attempts: {last_error}")
    return {"status": "COMPLETE", "committed_now": done_now, "attempts": attempts_log, "durations": durations}


# --- Spark side -----------------------------------------------------------------------------------


def tag_frame(df: DataFrame, threshold: int, groups: int) -> DataFrame:
    """Keep only sample sessions and invalid keys, add the two technical columns. No action."""
    clash = [c for c in TAG_COLUMNS if c in df.columns]
    if clash:
        raise ValueError(f"source already has technical columns {clash}")
    check_threshold(threshold)
    part = (F.when(invalid_key_cond(), F.lit(PART_INVALID))
            .when(hash_bucket_col() < F.lit(int(threshold)), F.lit(PART_SAMPLE)))
    out = df.withColumn(SNAP_PART_COL, part).filter(F.col(SNAP_PART_COL).isNotNull())
    return out.withColumn(
        GROUP_COL,
        F.when(F.col(SNAP_PART_COL) == PART_SAMPLE, (hash_bucket_col() % F.lit(int(groups))).cast("int"))
        .otherwise(F.lit(-1)),
    )


def write_batch(spark, batch: Dict[str, Any], source_root: str, schema, out_dir: str, threshold: int,
                groups: int, coalesce_n: int = 2) -> None:
    """One short job: read the batch files, tag, write to the staging directory (local)."""
    paths = [f["path"] for f in batch["files"]]
    df = spark.read.option("basePath", source_root).schema(schema).parquet(*paths)
    tag_frame(df, threshold, groups).coalesce(int(coalesce_n)).write.mode("errorifexists").parquet(file_uri(out_dir))


def batch_stats(spark, batch_path: str) -> Dict[str, Any]:
    """Row counts and a content checksum per (part, group), computed from the local files only."""
    df = spark.read.parquet(file_uri(batch_path))
    rows = (df.groupBy(SNAP_PART_COL, GROUP_COL)
            .agg(F.count(F.lit(1)).alias("rows"),
                 F.sum(F.xxhash64("user_session", "event_time", "event_sort_key")).alias("checksum"))
            .collect())
    by_key = {f"{r[SNAP_PART_COL]}|{r[GROUP_COL]}": {"rows": int(r["rows"]), "checksum": int(r["checksum"] or 0)}
              for r in rows}
    return {
        "rows_total": sum(v["rows"] for v in by_key.values()),
        "rows_sample": sum(v["rows"] for k, v in by_key.items() if k.startswith(PART_SAMPLE + "|")),
        "rows_invalid": sum(v["rows"] for k, v in by_key.items() if k.startswith(PART_INVALID + "|")),
        "by_part_group": by_key,
        "columns": sorted(df.columns),
    }


def verify_committed_batch(spark, store: BatchStore, bid: str, manifest: Dict[str, Any]) -> bool:
    """Resume check on local files: file list, sizes and content checksum equal the manifest."""
    if list_files(store.batch_dir(bid)) != manifest.get("files"):
        return False
    return batch_stats(spark, store.batch_dir(bid)) == manifest.get("stats")


def committed_batch_dirs(store: BatchStore, plan: Dict[str, Any]) -> List[str]:
    return [store.batch_dir(b["batch_id"]) for b in plan["batches"] if store.is_committed(b["batch_id"])]


def read_snapshot(spark, store: BatchStore, plan: Dict[str, Any], part: str = PART_SAMPLE,
                  group: Optional[int] = None, keep_tags: bool = False) -> DataFrame:
    """Local snapshot view. The filter uses the STORED tags: no hashing and no sampling happens here."""
    dirs = committed_batch_dirs(store, plan)
    if len(dirs) != len(plan["batches"]):
        raise RuntimeError("snapshot is not complete: not every planned batch is committed")
    df = spark.read.parquet(*[file_uri(d) for d in dirs]).filter(F.col(SNAP_PART_COL) == part)
    if group is not None:
        df = df.filter(F.col(GROUP_COL) == int(group))
    return df if keep_tags else df.drop(*TAG_COLUMNS)


# --- Whole-snapshot verification and final manifest -------------------------------------------------


def verify_snapshot(spark, store: BatchStore, plan: Dict[str, Any], bounds) -> Dict[str, Any]:
    """Independent check from the local files. Never touches HDFS."""
    from src.ml.dataset.n_selection import classify_sessions, valid_session_events

    problems: List[str] = []
    manifests = {b["batch_id"]: store.manifest(b["batch_id"]) for b in plan["batches"] if store.is_committed(b["batch_id"])}
    if len(manifests) != len(plan["batches"]):
        problems.append(f"{len(plan['batches']) - len(manifests)} batches are not committed")
    cov = coverage_problems(plan, manifests)
    for key in ("missing", "duplicated", "unknown"):
        if cov[key]:
            problems.append(f"source files {key}: {len(cov[key])}")
    if problems:
        return {"status": "FAIL", "problems": problems, "coverage": {k: len(v) for k, v in cov.items()}}

    all_df = spark.read.parquet(*[file_uri(store.batch_dir(b)) for b in manifests])
    sample = all_df.filter(F.col(SNAP_PART_COL) == PART_SAMPLE)
    invalid = all_df.filter(F.col(SNAP_PART_COL) == PART_INVALID)
    threshold, groups = plan["threshold"], plan["groups"]

    totals = {
        "rows_sample": sum(m["stats"]["rows_sample"] for m in manifests.values()),
        "rows_invalid": sum(m["stats"]["rows_invalid"] for m in manifests.values()),
    }
    checks = {
        "sample_rows": int(sample.count()),
        "invalid_rows": int(invalid.count()),
        "sample_with_invalid_key": int(sample.filter(invalid_key_cond()).count()),
        "invalid_part_with_valid_key": int(invalid.filter(~invalid_key_cond()).count()),
        "hash_at_or_above_threshold": int(sample.filter(hash_bucket_col() >= F.lit(int(threshold))).count()),
        "group_not_matching_hash": int(sample.filter(
            F.col(GROUP_COL) != (hash_bucket_col() % F.lit(int(groups))).cast("int")).count()),
        "sessions": int(sample.select("user_session").distinct().count()),
    }
    # a session must live in exactly one group
    multi = (sample.groupBy("user_session").agg(F.countDistinct(GROUP_COL).alias("g")).filter(F.col("g") > 1).count())
    checks["sessions_in_more_than_one_group"] = int(multi)
    per_group = {str(r[GROUP_COL]): int(r["n"]) for r in
                 sample.groupBy(GROUP_COL).agg(F.countDistinct("user_session").alias("n")).collect()}
    checks["sessions_by_group"] = per_group
    checks["sessions_sum_of_groups"] = sum(per_group.values())

    if checks["sample_rows"] != totals["rows_sample"]:
        problems.append("sample rows differ from the batch manifests")
    if checks["invalid_rows"] != totals["rows_invalid"]:
        problems.append("invalid rows differ from the batch manifests")
    for key in ("sample_with_invalid_key", "invalid_part_with_valid_key", "hash_at_or_above_threshold",
                "group_not_matching_hash", "sessions_in_more_than_one_group"):
        if checks[key] != 0:
            problems.append(f"{key} = {checks[key]} (must be 0)")
    if checks["sessions"] != checks["sessions_sum_of_groups"]:
        problems.append("sum of sessions per group differs from distinct sessions (a session is in two groups)")

    data_cols = [c for c in all_df.columns if c not in TAG_COLUMNS]
    checks["data_columns"] = sorted(data_cols)
    buckets = classify_sessions(valid_session_events(sample.drop(*TAG_COLUMNS)), bounds)
    checks["h11_bucket_sessions"] = {str(r["bucket"]): int(r["count"]) for r in buckets.groupBy("bucket").count().collect()}
    checks["month_rows"] = {str(r["m"]): int(r["count"]) for r in
                            sample.groupBy(F.date_format("event_time", "yyyy-MM").alias("m")).count().collect()}
    return {"status": "PASS" if not problems else "FAIL", "problems": problems, "coverage": {k: len(v) for k, v in cov.items()},
            "totals": totals, "checks": checks}


def finalize_manifest(plan: Dict[str, Any], store: BatchStore, verification: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    """Write snapshot_manifest.json (+ .sha256) only when every batch is committed and verified."""
    if verification.get("status") != "PASS":
        raise RuntimeError("refusing to finalize: verification is not PASS")
    manifests = {b["batch_id"]: store.manifest(b["batch_id"]) for b in plan["batches"]}
    manifest = {
        "kind": MANIFEST_KIND,
        "sampling_scope": "SAMPLED_SESSIONS",
        "sample_threshold": plan["threshold"],
        "groups": plan["groups"],
        "hash_expression": plan["hash_expression"],
        "source_root": plan["source_root"],
        "source_fingerprint": plan["source_fingerprint"],
        "source_files": plan["source_files"],
        "source_bytes": plan["source_bytes"],
        "plan_sha256": plan["plan_sha256"],
        "schema": plan["schema"],
        "batches": [{"batch_id": bid, "rows": m["stats"]["rows_total"], "files_sha": m["files_sha"],
                     "attempt": m["attempt"], "elapsed_seconds": m["elapsed_seconds"]} for bid, m in manifests.items()],
        "totals": verification["totals"],
        "checks": verification["checks"],
        "technical_columns": list(TAG_COLUMNS),
    }
    manifest.update(extra)
    path = os.path.join(store.root, "snapshot_manifest.json")
    write_json(path, manifest)
    sha = sha256_file(path)
    with open(os.path.join(store.root, "snapshot_manifest.sha256"), "w", encoding="utf-8") as f:
        f.write(f"{sha}  snapshot_manifest.json\n")
    return manifest
