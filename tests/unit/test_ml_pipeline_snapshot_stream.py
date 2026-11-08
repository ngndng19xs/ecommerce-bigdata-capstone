"""P5 ML pipeline: incremental snapshot. Fault injection on commit/resume (no Spark) and an
equivalence test against a single-pass reference (Spark, small synthetic data, platform independent paths)."""

import os
import random
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.ml.dataset import snapshot_stream as ss  # noqa: E402


def _files(n):
    return [{"path": f"/data/curated/events/p{i:03d}.parquet", "size": 100 + i, "mtime": "2026-11-01 10:00"}
            for i in range(n)]


def _plan(n_files=6, per_batch=2):
    return ss.build_plan(_files(n_files), "/data/curated/events", {"fields": []}, 455, 8, per_batch)


def _store(tmp_path):
    return ss.BatchStore(str(tmp_path / "snap"))


def _process_ok(batch, staging):
    os.makedirs(staging)
    with open(os.path.join(staging, "part-0.parquet"), "w", encoding="utf-8") as f:
        f.write(batch["batch_id"])


def _stats(staging):
    return {"rows_total": 1, "rows_sample": 1, "rows_invalid": 0, "by_part_group": {}, "columns": []}


# --- listing and plan ------------------------------------------------------------------------


def test_parse_hdfs_ls_keeps_parquet_files_only():
    text = "\n".join([
        "drwxr-xr-x   - hdp supergroup          0 2026-11-01 10:00 /data/curated/events/event_date=2019-10-01",
        "-rw-r--r--   1 hdp supergroup   45123456 2026-11-01 10:01 /data/curated/events/event_date=2019-10-01/part-0.snappy.parquet",
        "-rw-r--r--   1 hdp supergroup          0 2026-11-01 10:01 /data/curated/events/_SUCCESS",
        "-rw-r--r--   1 hdp supergroup       1234 2026-11-01 10:01 /data/curated/events/.hidden.parquet",
    ])
    files = ss.parse_hdfs_ls(text)
    assert [f["path"] for f in files] == ["/data/curated/events/event_date=2019-10-01/part-0.snappy.parquet"]
    assert files[0]["size"] == 45123456


def test_plan_is_deterministic_and_covers_every_file_once():
    p1, p2 = _plan(7, 3), _plan(7, 3)
    assert p1["plan_sha256"] == p2["plan_sha256"]
    ids = [f["path"] for b in p1["batches"] for f in b["files"]]
    assert sorted(ids) == sorted(f["path"] for f in _files(7)) and len(ids) == len(set(ids))
    assert [b["batch_id"] for b in p1["batches"]] == ["b0001", "b0002", "b0003"]


def test_source_change_is_detected():
    plan = _plan()
    ss.check_source_identity(plan, _files(6))
    changed = _files(6)
    changed[2]["size"] += 1
    with pytest.raises(ss.SourceChangedError):
        ss.check_source_identity(plan, changed)


# --- commit, retry and resume -----------------------------------------------------------------


def test_clean_run_commits_every_batch_once_with_full_coverage(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    result = ss.run_batches(plan, store, _process_ok, _stats, log=lambda m: None)
    assert result["status"] == "COMPLETE" and result["committed_now"] == ["b0001", "b0002", "b0003"]
    manifests = {b["batch_id"]: store.manifest(b["batch_id"]) for b in plan["batches"]}
    cov = ss.coverage_problems(plan, manifests)
    assert cov == {"missing": [], "duplicated": [], "unknown": []}


def test_retry_after_a_failed_attempt_does_not_duplicate(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    calls = {"n": 0}

    def flaky(batch, staging):
        calls["n"] += 1
        _process_ok(batch, staging)
        if calls["n"] == 1:
            raise RuntimeError("Spark job died")

    result = ss.run_batches(plan, store, flaky, _stats, max_attempts=2, log=lambda m: None)
    statuses = [(a["batch_id"], a["attempt"], a["status"]) for a in result["attempts"]]
    assert statuses[:2] == [("b0001", 1, "FAILED"), ("b0001", 2, "PASS")]
    assert os.listdir(store.quarantine_dir)  # the failed attempt was kept, not deleted
    assert store.is_committed("b0001") and store.is_committed("b0003")
    assert os.listdir(store.batch_dir("b0001")) == ["part-0.parquet"]


def test_all_attempts_failing_stops_and_keeps_earlier_batches(tmp_path):
    plan, store = _plan(), _store(tmp_path)

    def fail_on_second(batch, staging):
        if batch["batch_id"] == "b0002":
            raise RuntimeError("boom")
        _process_ok(batch, staging)

    with pytest.raises(ss.BatchFailedError):
        ss.run_batches(plan, store, fail_on_second, _stats, max_attempts=2, log=lambda m: None)
    assert store.is_committed("b0001") and not store.is_committed("b0002")


def test_interrupt_midway_then_resume_commits_each_batch_exactly_once(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    started = []

    def interrupted(batch, staging):
        started.append(batch["batch_id"])
        _process_ok(batch, staging)
        if batch["batch_id"] == "b0002":
            raise KeyboardInterrupt()  # SIGINT: not an Exception, nothing catches it

    with pytest.raises(KeyboardInterrupt):
        ss.run_batches(plan, store, interrupted, _stats, log=lambda m: None)
    assert store.is_committed("b0001") and not store.is_committed("b0002")
    assert os.listdir(store.staging)  # the interrupted attempt is still in staging

    store2 = ss.BatchStore(store.root)  # new process
    rec = store2.reconcile(plan)
    assert rec["committed"] == ["b0001"] and rec["pending"] == ["b0002", "b0003"] and rec["quarantined"]
    assert os.listdir(store2.staging) == []
    reprocessed = []

    def second(batch, staging):
        reprocessed.append(batch["batch_id"])
        _process_ok(batch, staging)

    ss.run_batches(plan, store2, second, _stats, log=lambda m: None)
    assert reprocessed == ["b0002", "b0003"], "a committed batch must not be read or written again"
    manifests = {b["batch_id"]: store2.manifest(b["batch_id"]) for b in plan["batches"]}
    assert ss.coverage_problems(plan, manifests) == {"missing": [], "duplicated": [], "unknown": []}


def test_crash_between_rename_and_manifest_is_quarantined_and_redone(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    os.makedirs(store.batch_dir("b0001"))  # directory renamed, manifest never written
    rec = store.reconcile(plan)
    assert rec["pending"][0] == "b0001" and "b0001" not in rec["committed"]
    assert any(q.endswith("b0001.1") for q in rec["quarantined"])


def test_manifest_without_directory_is_quarantined(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    ss.write_json(store.manifest_path("b0001"), {"kind": "snapshot_batch"})
    rec = store.reconcile(plan)
    assert "b0001" in rec["pending"] and rec["quarantined"]


def test_batch_failing_verification_on_resume_is_redone(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    ss.run_batches(plan, store, _process_ok, _stats, log=lambda m: None)
    rec = ss.BatchStore(store.root).reconcile(plan, verify=lambda bid, man: bid != "b0002")
    assert rec["committed"] == ["b0001", "b0003"] and rec["pending"] == ["b0002"]


def test_committing_a_batch_twice_is_refused(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    ss.run_batches(plan, store, _process_ok, _stats, log=lambda m: None)
    staging = store.staging_path("b0001", 9)
    _process_ok(plan["batches"][0], staging)
    with pytest.raises(ss.DuplicateBatchError):
        store.commit("b0001", staging, {})


def test_a_batch_unknown_to_the_plan_stops_the_run(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    os.makedirs(os.path.join(store.batches, "b9999"))
    with pytest.raises(ss.SourceChangedError):
        store.reconcile(plan)


def test_budget_stop_leaves_committed_batches_valid(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    seen = []

    def stop_after_one(durations, remaining):
        seen.append((len(durations), remaining))
        return len(durations) < 1

    result = ss.run_batches(plan, store, _process_ok, _stats, should_continue=stop_after_one, log=lambda m: None)
    assert result["status"] == "STOPPED_BY_BUDGET" and result["committed_now"] == ["b0001"]
    assert seen == [(0, 3), (1, 2)]


def test_coverage_detects_a_missing_and_a_duplicated_file(tmp_path):
    plan, store = _plan(), _store(tmp_path)
    ss.run_batches(plan, store, _process_ok, _stats, log=lambda m: None)
    manifests = {b["batch_id"]: store.manifest(b["batch_id"]) for b in plan["batches"]}
    manifests["b0003"] = dict(manifests["b0003"], source_files=manifests["b0002"]["source_files"])
    cov = ss.coverage_problems(plan, manifests)
    assert cov["duplicated"] and cov["missing"]


# --- Spark equivalence --------------------------------------------------------------------------

from datetime import datetime as _dt  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType  # noqa: E402

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.ml_stage1 import file_uri, hash_bucket_col, invalid_key_cond  # noqa: E402
from src.ml.dataset.n_selection import SplitBounds  # noqa: E402

BOUNDS = SplitBounds(_dt(2019, 10, 1), _dt(2019, 11, 13), _dt(2019, 11, 13), _dt(2019, 11, 22),
                     _dt(2019, 11, 22), _dt(2019, 12, 1))
SCHEMA = StructType([
    StructField("user_session", StringType(), True), StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True), StructField("event_type_rank", IntegerType(), True),
    StructField("event_sort_key", StringType(), True), StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True), StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
])
THRESHOLD, GROUPS = 3000, 8


@pytest.fixture(scope="module")
def spark():
    s = get_spark("TestMLPipelineStream")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def source(spark, tmp_path_factory):
    """Six source files; sessions are spread over several files and over several days."""
    root = tmp_path_factory.mktemp("curated")
    rnd = random.Random(11)
    base = datetime(2019, 10, 1)
    per_file = [[] for _ in range(6)]
    for i in range(500):
        key = f"s-{i:04d}"
        start = base + timedelta(seconds=rnd.randint(0, 60 * 24 * 3600))
        for j in range(rnd.randint(1, 4)):
            when = (start + timedelta(hours=20 * j)).strftime("%Y-%m-%d %H:%M:%S")  # crosses day boundaries
            row = (key, when, rnd.choice(["view", "cart", "purchase"]), 1, f"{key}-{j}", "P", "C", "b", 5.0)
            per_file[rnd.randrange(6)].append(row)
    per_file[0] += [("", "2019-10-03 10:00:00", "view", 1, "bad-0", "P", "C", "b", 1.0),
                    ("   ", "2019-10-03 10:00:00", "view", 1, "bad-1", "P", "C", "b", 1.0),
                    (None, "2019-10-03 10:00:00", "view", 1, "bad-2", "P", "C", "b", 1.0)]
    per_file[1] += [per_file[1][0]] if per_file[1] else []  # an exact duplicate event: multiset must keep both
    paths = []
    for idx, rows in enumerate(per_file):
        d = root / f"f{idx}"
        df = spark.createDataFrame(rows, SCHEMA).withColumn(
            "event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        df.coalesce(1).write.mode("overwrite").parquet(file_uri(str(d)))
        paths.append(str(next(d.glob("part-*.parquet"))))
    files = [{"path": file_uri(p), "size": os.path.getsize(p), "mtime": "t"} for p in paths]
    full = spark.read.parquet(*[f["path"] for f in files])
    return {"root": root, "files": files, "df": full}


def _multiset(df, cols):
    return Counter(tuple(r) for r in df.select(*cols).collect())


def _run_stream(spark, source, tmp_path, per_batch=2, interrupt_after=None):
    schema = source["df"].schema
    plan = ss.build_plan(source["files"], file_uri(str(source["root"])), schema.jsonValue(), THRESHOLD, GROUPS, per_batch)
    store = ss.BatchStore(str(tmp_path / "snap"))

    def process(batch, staging):
        ss.write_batch(spark, batch, plan["source_root"], schema, staging, THRESHOLD, GROUPS, coalesce_n=1)
        if interrupt_after and batch["batch_id"] == interrupt_after:
            raise KeyboardInterrupt()

    ss.run_batches(plan, store, process, lambda p: ss.batch_stats(spark, p), log=lambda m: None)
    return plan, store


def test_incremental_snapshot_equals_single_pass_reference(spark, source, tmp_path):
    plan, store = _run_stream(spark, source, tmp_path)
    cols = [c for c in source["df"].columns]
    raw = source["df"]
    ref_sample = raw.filter(~invalid_key_cond() & (hash_bucket_col() < THRESHOLD))
    ref_invalid = raw.filter(invalid_key_cond())
    got_sample = ss.read_snapshot(spark, store, plan, "sample")
    got_invalid = ss.read_snapshot(spark, store, plan, "invalid_key")
    assert _multiset(got_sample, cols) == _multiset(ref_sample, cols)
    assert _multiset(got_invalid, cols) == _multiset(ref_invalid, cols)
    assert sum(_multiset(got_sample, cols).values()) > 100  # the test is not vacuous


def test_whole_sessions_survive_being_split_over_files_and_days(spark, source, tmp_path):
    plan, store = _run_stream(spark, source, tmp_path)
    raw = source["df"]
    sel = {r[0] for r in raw.filter(~invalid_key_cond() & (hash_bucket_col() < THRESHOLD))
           .select("user_session").distinct().collect()}
    per_session_ref = Counter(r[0] for r in raw.filter(F.col("user_session").isin(list(sel))).select("user_session").collect())
    per_session_got = Counter(r[0] for r in ss.read_snapshot(spark, store, plan).select("user_session").collect())
    assert per_session_got == per_session_ref
    assert any(n > 1 for n in per_session_ref.values())


def test_groups_follow_the_hash_and_never_split_a_session(spark, source, tmp_path):
    plan, store = _run_stream(spark, source, tmp_path)
    verification = ss.verify_snapshot(spark, store, plan, BOUNDS)
    assert verification["status"] == "PASS", verification["problems"]
    checks = verification["checks"]
    assert checks["sessions_in_more_than_one_group"] == 0
    assert checks["sessions"] == checks["sessions_sum_of_groups"]
    union = sum(
        ss.read_snapshot(spark, store, plan, group=g).count() for g in range(GROUPS))
    assert union == ss.read_snapshot(spark, store, plan).count()


def test_interrupt_and_resume_gives_the_same_snapshot(spark, source, tmp_path):
    cols = list(source["df"].columns)
    clean_plan, clean_store = _run_stream(spark, source, tmp_path / "clean")
    expected = _multiset(ss.read_snapshot(spark, clean_store, clean_plan), cols)

    schema = source["df"].schema
    plan = ss.build_plan(source["files"], file_uri(str(source["root"])), schema.jsonValue(), THRESHOLD, GROUPS, 2)
    store = ss.BatchStore(str(tmp_path / "resume" / "snap"))

    def process_interrupt(batch, staging):
        ss.write_batch(spark, batch, plan["source_root"], schema, staging, THRESHOLD, GROUPS, coalesce_n=1)
        if batch["batch_id"] == "b0002":
            raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        ss.run_batches(plan, store, process_interrupt, lambda p: ss.batch_stats(spark, p), log=lambda m: None)

    store2 = ss.BatchStore(store.root)
    store2.reconcile(plan, verify=lambda bid, man: ss.verify_committed_batch(spark, store2, bid, man))
    ss.run_batches(plan, store2, lambda b, s: ss.write_batch(spark, b, plan["source_root"], schema, s, THRESHOLD,
                                                             GROUPS, coalesce_n=1),
                   lambda p: ss.batch_stats(spark, p), log=lambda m: None)
    assert _multiset(ss.read_snapshot(spark, store2, plan), cols) == expected


def test_tag_columns_are_refused_when_the_source_already_has_them(spark):
    df = spark.createDataFrame([("a", 1)], "user_session string, __ml_group int")
    with pytest.raises(ValueError):
        ss.tag_frame(df, 455, 8)
