"""P5-B-B6-01 / ML Giai doan 1: snapshot 455 (build), kiem tra (verify) va ban sao long (subset).

Usage (tu thu muc goc repo):
  python scripts/run/p5_snapshot.py build  --source /data/curated/events --work-root WORK --run-id RID \
         [--source-listing FILE] [--threshold 455] [--coalesce 16]
  python scripts/run/p5_snapshot.py verify --snapshot-root WORK/ml_snapshot_455 --out FILE
  python scripts/run/p5_snapshot.py subset --snapshot-root WORK/ml_snapshot_455 --threshold 10 --name check_10 \
         --expect-rows 107965 --expect-sessions 22923

Exit: 0 PASS; 1 FAIL (kiem tra khong dat); 2 BLOCKED (tham so, duong dan, thieu cot).
build chi doc nguon mot lan (mot lenh ghi). verify va subset chi doc snapshot local.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import yaml  # noqa: E402

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.ml_stage1 import (  # noqa: E402
    HASH_EXPR,
    HASH_BUCKETS,
    PART_INVALID,
    PART_SAMPLE,
    SOURCE_ROWS_REFERENCE,
    check_source_columns,
    check_threshold,
    compare_stats,
    compute_invalid_stats,
    compute_part_stats,
    file_uri,
    list_files,
    load_json,
    sha256_file,
    write_json,
    write_snapshot,
    write_subset,
)
from src.ml.dataset.n_selection import SplitBounds  # noqa: E402

DEFAULT_CONFIG = os.path.join(ROOT, "configs", "n_candidate_analysis.example.yaml")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_incomplete(path: str) -> str:
    """Rename a partial output so no later step reads it. Never deletes anything."""
    attempt = 1
    while os.path.exists(f"{path}.INCOMPLETE-{attempt}"):
        attempt += 1
    target = f"{path}.INCOMPLETE-{attempt}"
    os.rename(path, target)
    return target


def load_bounds(config_path: str) -> SplitBounds:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return SplitBounds.from_iso(cfg["windows_utc"])


def cmd_build(args) -> int:
    threshold = check_threshold(args.threshold)
    bounds = load_bounds(args.config)  # fail fast before any read of the source
    work = Path(args.work_root)
    final = work / args.name
    tmp = work / f"{args.name}.tmp-{args.run_id}"
    if final.exists():
        print(f"BLOCKED: {final} already exists (never overwrite a snapshot)", file=sys.stderr)
        return 2
    if tmp.exists():  # leftover of an interrupted attempt: keep it under a new name, never read it
        print(f"previous partial attempt renamed to {mark_incomplete(str(tmp))}", file=sys.stderr)
    tmp.mkdir(parents=True)

    spark = get_spark(app_name="P5ML-SnapshotBuild")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        raw = spark.read.parquet(args.source)  # Curated lives in HDFS: no file:// here
        missing = check_source_columns(raw.columns)
        if missing:
            print(f"BLOCKED: source is missing columns {missing}", file=sys.stderr)
            return 2
        schema_json = raw.schema.jsonValue()
        source_files = len(raw.inputFiles())
        snap_dir = tmp / "snapshot"
        part_col = write_snapshot(raw, file_uri(str(snap_dir)), threshold, args.coalesce)

        sample_stats = compute_part_stats(spark, str(snap_dir / f"{part_col}={PART_SAMPLE}"), bounds, threshold)
        invalid_stats = compute_invalid_stats(spark, str(snap_dir / f"{part_col}={PART_INVALID}"))
        manifest = {
            "kind": "snapshot",
            "name": args.name,
            "run_id": args.run_id,
            "created_utc": now_utc(),
            "source_path": args.source,
            "source_input_files": source_files,
            "source_listing_sha256": sha256_file(args.source_listing) if args.source_listing else None,
            "source_rows_reference": SOURCE_ROWS_REFERENCE,
            "threshold": threshold,
            "hash_expression": HASH_EXPR.format(buckets=HASH_BUCKETS),
            "sampling_scope": "SAMPLED_SESSIONS",
            "invalid_rule": "user_session IS NULL OR trim(user_session) = ''",
            "part_column": part_col,
            "coalesce": int(args.coalesce),
            "schema": schema_json,
            "schema_names": sorted(raw.columns),
            "rows": sample_stats["rows"],
            "distinct_sessions": sample_stats["distinct_sessions"],
            "sample": sample_stats,
            "invalid": invalid_stats,
            "files": list_files(str(snap_dir)),
            "spark_version": spark.version,
            "note": "Subsets check_10 (threshold 10) and pilot_57 (threshold 57) are nested in this sample.",
        }
        write_json(str(tmp / "snapshot_manifest.json"), manifest)
        sha = sha256_file(str(tmp / "snapshot_manifest.json"))
        (tmp / "snapshot_manifest.sha256").write_text(f"{sha}  snapshot_manifest.json\n", encoding="utf-8")
        os.rename(str(tmp), str(final))
        print(json.dumps({"status": "PASS", "snapshot_root": str(final), "rows": manifest["rows"],
                          "distinct_sessions": manifest["distinct_sessions"], "invalid": invalid_stats,
                          "manifest_sha256": sha}, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


def cmd_verify(args) -> int:
    root = Path(args.snapshot_root)
    manifest = load_json(str(root / "snapshot_manifest.json"))
    problems = []
    expected_sha = (root / "snapshot_manifest.sha256").read_text(encoding="utf-8").split()[0]
    if sha256_file(str(root / "snapshot_manifest.json")) != expected_sha:
        problems.append("snapshot_manifest.json sha256 differs from snapshot_manifest.sha256")
    listing = list_files(str(root / "snapshot"))
    if listing != manifest["files"]:
        problems.append("file list or file sizes differ from manifest")

    bounds = load_bounds(args.config)
    spark = get_spark(app_name="P5ML-SnapshotVerify")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        part = manifest["part_column"]
        sample_stats = compute_part_stats(spark, str(root / "snapshot" / f"{part}={PART_SAMPLE}"), bounds,
                                          manifest["threshold"])
        invalid_stats = compute_invalid_stats(spark, str(root / "snapshot" / f"{part}={PART_INVALID}"))
        problems += compare_stats(manifest["sample"], sample_stats)
        problems += compare_stats(manifest["invalid"], invalid_stats)
        if invalid_stats["rows_not_invalid"] != 0:
            problems.append("invalid_key directory holds rows that are not invalid")
        if sample_stats["invalid_key_rows"] != 0:
            problems.append("sample directory holds invalid session keys")
        if sample_stats["hash_violations"] != 0:
            problems.append("sample directory holds sessions with hash >= threshold")
        report = {
            "status": "PASS" if not problems else "FAIL",
            "checked_utc": now_utc(),
            "snapshot_root": str(root),
            "manifest_sha256": expected_sha,
            "rows": sample_stats["rows"],
            "distinct_sessions": sample_stats["distinct_sessions"],
            "invalid": invalid_stats,
            "h11_bucket_sessions": sample_stats["h11_bucket_sessions"],
            "month_counts": sample_stats["month_counts"],
            "problems": problems,
        }
    finally:
        spark.stop()
    write_json(args.out, report)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if not problems else 1


def cmd_subset(args) -> int:
    root = Path(args.snapshot_root)
    manifest = load_json(str(root / "snapshot_manifest.json"))
    threshold = check_threshold(args.threshold)
    if threshold > int(manifest["threshold"]):
        print("BLOCKED: subset threshold must not exceed the snapshot threshold", file=sys.stderr)
        return 2
    bounds = load_bounds(args.config)
    out = root / "subsets" / args.name
    tmp = root / "subsets" / f"{args.name}.tmp"
    if out.exists():
        print(f"BLOCKED: {out} already exists", file=sys.stderr)
        return 2
    if tmp.exists():  # leftover of an interrupted attempt: keep it under a new name, never read it
        print(f"previous partial attempt renamed to {mark_incomplete(str(tmp))}", file=sys.stderr)
    # Spark's write creates tmp itself; it must not exist before the write (errorifexists).
    (root / "subsets").mkdir(parents=True, exist_ok=True)

    spark = get_spark(app_name="P5ML-Subset")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        sample_dir = root / "snapshot" / f"{manifest['part_column']}={PART_SAMPLE}"
        sample = spark.read.parquet(file_uri(str(sample_dir)))
        write_subset(sample, file_uri(str(tmp)), threshold, coalesce_n=4)
        stats = compute_part_stats(spark, str(tmp), bounds, threshold)
        problems = []
        if stats["hash_violations"] != 0:
            problems.append("subset holds sessions with hash >= subset threshold")
        if args.expect_rows is not None and stats["rows"] != args.expect_rows:
            problems.append(f"rows={stats['rows']} expected {args.expect_rows}")
        if args.expect_sessions is not None and stats["distinct_sessions"] != args.expect_sessions:
            problems.append(f"sessions={stats['distinct_sessions']} expected {args.expect_sessions}")
        sub_manifest = {
            "kind": "subset",
            "name": args.name,
            "created_utc": now_utc(),
            "threshold": threshold,
            "parent_threshold": int(manifest["threshold"]),
            "parent_manifest_sha256": sha256_file(str(root / "snapshot_manifest.json")),
            "rows": stats["rows"],
            "distinct_sessions": stats["distinct_sessions"],
            "expected_rows": args.expect_rows,
            "expected_sessions": args.expect_sessions,
            "schema_names": stats["schema_names"],
            "stats": stats,
            "files": list_files(str(tmp)),
            "problems": problems,
        }
        write_json(str(tmp / "_subset_manifest.json"), sub_manifest)
        os.rename(str(tmp), str(out))
        summary = {"status": "PASS" if not problems else "FAIL", "name": args.name, "rows": stats["rows"],
                   "distinct_sessions": stats["distinct_sessions"], "problems": problems,
                   "subset_root": str(out)}
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0 if not problems else 1
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="P5-B-B6-01 ML stage 1 snapshot tools.")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="YAML with windows_utc (H11 windows).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="Read the Curated source once and write the snapshot.")
    b.add_argument("--source", required=True)
    b.add_argument("--work-root", required=True)
    b.add_argument("--name", default="ml_snapshot_455")
    b.add_argument("--run-id", required=True)
    b.add_argument("--threshold", type=int, default=455)
    b.add_argument("--coalesce", type=int, default=16)
    b.add_argument("--source-listing", default=None, help="Text file with hdfs dfs -ls -R output (metadata).")
    b.set_defaults(func=cmd_build)

    v = sub.add_parser("verify", help="Re-check the snapshot on local disk against its manifest.")
    v.add_argument("--snapshot-root", required=True)
    v.add_argument("--out", required=True)
    v.set_defaults(func=cmd_verify)

    s = sub.add_parser("subset", help="Nested subset of the snapshot (no source read).")
    s.add_argument("--snapshot-root", required=True)
    s.add_argument("--threshold", type=int, required=True)
    s.add_argument("--name", required=True)
    s.add_argument("--expect-rows", type=int, default=None)
    s.add_argument("--expect-sessions", type=int, default=None)
    s.set_defaults(func=cmd_subset)
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
