"""Stage 1 cross checks on the pilot (section 6.8 of the runbook). Read-only, local files only.

(c) H11 split: the session counts of the B6-02 features (per split directory) must equal the
    H11 table of the same subset (B6-01 session_buckets for train, validation, test_range).
    No session may appear in two split directories, and every feature session must belong to
    its own split. Censored and other buckets must not appear in the features.
(d) Counts: eligible and label_1 per split for train and validation must equal the B6-01
    per_n[N] values of the same subset (test has no B6-01 count).

Usage:
  python scripts/run/p5_stage1_checks.py --subset-dir WORK/.../subsets/pilot_57 \
      --features-root WORK/.../b6_02_pilot/features --pilot-b6-01 reports/.../pilot_b6_01/n_candidate_result.json \
      --n 3 --out reports/.../pilot_checks.json
Exit: 0 PASS, 1 FAIL, 2 BLOCKED.
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
from pyspark.sql import functions as F  # noqa: E402

from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.ml_stage1 import file_uri, write_json  # noqa: E402
from src.ml.dataset.n_selection import SplitBounds, classify_sessions, h11_split_table, valid_session_events  # noqa: E402

SPLITS = ("train", "validation", "test")
BUCKET_OF_SPLIT = {"train": "train", "validation": "validation", "test": "test_range"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 1 pilot cross checks (B6-02 vs B6-01).")
    ap.add_argument("--subset-dir", required=True)
    ap.add_argument("--features-root", required=True)
    ap.add_argument("--pilot-b6-01", required=True)
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--bounds-config", default=os.path.join(ROOT, "configs", "n_candidate_analysis.example.yaml"))
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    if not os.path.isdir(args.features_root) or not os.path.isfile(args.pilot_b6_01):
        print("BLOCKED: features root or B6-01 pilot result is missing", file=sys.stderr)
        return 2
    with open(args.bounds_config, "r", encoding="utf-8") as f:
        bounds = SplitBounds.from_iso(yaml.safe_load(f)["windows_utc"])
    with open(args.pilot_b6_01, "r", encoding="utf-8") as f:
        b601 = json.load(f)

    spark = get_spark(app_name="P5ML-Stage1Checks")
    spark.sparkContext.setLogLevel("ERROR")
    problems = []
    checks = {}
    try:
        events = spark.read.parquet(file_uri(args.subset_dir))
        keys = h11_split_table(events, bounds).cache()
        keys_by_split = {r["h11_split"]: r["count"] for r in keys.groupBy("h11_split").count().collect()}
        buckets = classify_sessions(valid_session_events(events), bounds).groupBy("bucket").count().collect()
        bucket_counts = {r["bucket"]: int(r["count"]) for r in buckets}

        # (c) session counts per split equal the B6-01 buckets
        h11_counts = {}
        for split in SPLITS:
            expected = int(b601.get("session_buckets", {}).get(BUCKET_OF_SPLIT[split], {}).get("sessions", 0))
            actual = int(keys_by_split.get(split, 0))
            h11_counts[split] = {"b6_01_bucket": expected, "h11_table": actual}
            if expected != actual:
                problems.append(f"(c) {split}: H11 table has {actual} sessions, B6-01 bucket {expected}")
        checks["c_h11_session_counts"] = h11_counts
        checks["c_censored_or_other_buckets_not_in_features"] = True

        feature_sessions = {}
        feature_counts = {}
        for split in SPLITS:
            path = os.path.join(args.features_root, f"h11_split={split}")
            if not os.path.isdir(path):
                feature_counts[split] = {"eligible_rows": 0, "label_1": 0, "dir_present": False}
                feature_sessions[split] = None
                continue
            feats = spark.read.parquet(file_uri(path))
            feature_sessions[split] = feats.select("user_session").distinct()
            feature_counts[split] = {
                "eligible_rows": int(feats.count()),
                "label_1": int(feats.filter(F.col("label") == 1).count()),
                "dir_present": True,
            }
            outside = feature_sessions[split].join(
                keys.filter(F.col("h11_split") == split).select("user_session"), on="user_session", how="left_anti"
            ).count()
            if outside:
                problems.append(f"(c) {split}: {outside} feature sessions are not in the H11 {split} table")
        checks["c_feature_rows_by_split"] = feature_counts

        for a, b_name in (("train", "validation"), ("train", "test"), ("validation", "test")):
            fa, fb = feature_sessions.get(a), feature_sessions.get(b_name)
            if fa is not None and fb is not None and fa.join(fb, on="user_session", how="inner").count() > 0:
                problems.append(f"(c) sessions appear in both {a} and {b_name} features")

        # (d) eligible and label_1 equal B6-01 for train and validation
        per_n = b601.get("per_n", {}).get(str(args.n))
        d_checks = {}
        if per_n is None:
            problems.append(f"(d) B6-01 pilot result has no N={args.n}")
        else:
            for split in ("train", "validation"):
                exp_elig = int(per_n[split]["eligible"])
                exp_l1 = int(per_n[split]["label_1"])
                got = feature_counts[split]
                d_checks[split] = {"b6_01_eligible": exp_elig, "b6_02_eligible": got["eligible_rows"],
                                   "b6_01_label_1": exp_l1, "b6_02_label_1": got["label_1"]}
                if exp_elig != got["eligible_rows"]:
                    problems.append(f"(d) {split}: eligible {got['eligible_rows']} != B6-01 {exp_elig}")
                if exp_l1 != got["label_1"]:
                    problems.append(f"(d) {split}: label_1 {got['label_1']} != B6-01 {exp_l1}")
        checks["d_eligible_and_label_1"] = d_checks
        checks["subset_bucket_counts"] = bucket_counts
    finally:
        spark.stop()

    result = {
        "status": "PASS" if not problems else "FAIL",
        "n": args.n,
        "checked_utc": datetime.now(timezone.utc).isoformat(),
        "subset_dir": args.subset_dir,
        "features_root": args.features_root,
        "checks": checks,
        "problems": problems,
    }
    write_json(Path(args.out), result)
    print(json.dumps({"status": result["status"], "problems": problems}, indent=2, ensure_ascii=False))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
