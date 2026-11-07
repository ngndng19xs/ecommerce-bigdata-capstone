"""P5-B-B6-01 N candidate analysis CLI.

Modes:
  sample (default): whole-session hash sample (default 0.1% of sessions). Development and QA only.
  full            : all of Curated. Requires --confirm-full-run. Never started by default.

Writes ONLY to a NEW --output-dir (refuses to overwrite). Never writes to HDFS and never
touches Curated, ML features, models or B7 outputs.

Exit codes: 0 analysis completed and QA invariants PASS; 1 analysis error or QA FAIL;
2 blocked (bad arguments, preflight failure, output directory exists).
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import yaml  # noqa: E402

from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.n_selection import (  # noqa: E402
    SplitBounds,
    analyze_n_candidates,
    parse_n_candidates,
)
from src.ml.training.lr_b6_smoke import sample_whole_sessions  # noqa: E402

logger = get_logger("run_p5_b6_01")

REQUIRED_COLUMNS = ["user_session", "event_time", "event_type", "event_type_rank", "event_sort_key"]


def _table_md(result: dict) -> str:
    ns = result["n_candidates"]
    header = "| Metric | " + " | ".join(f"N={n}" for n in ns) + " |"
    sep = "|---|" + "---:|" * len(ns)
    rows = []
    for split in ("train", "validation", "train_plus_validation"):
        rows.append(f"| **{split}** |" + " |" * len(ns))
        for key in ("in_scope_sessions", "too_short", "early_purchase", "eligible", "label_0", "label_1",
                    "coverage_ratio", "positive_rate"):
            vals = [result["per_n"][str(n)][split].get(key) for n in ns]
            rows.append(f"| {key} | " + " | ".join("-" if v is None else str(v) for v in vals) + " |")
    return "\n".join([header, sep] + rows) + "\n"


def resolve_analysis_ns(cfg: dict, scope: str) -> list:
    """Which N values a run analyses.

    acceptance : only cfg['selected_n'] (official prediction point). Never a data-driven choice.
    reference  : cfg['n_candidates'] for the comparison table. Does not change the official N.
    """
    if scope not in ("acceptance", "reference"):
        raise ValueError(f"unknown analysis scope: {scope}")
    candidates = parse_n_candidates(cfg["n_candidates"])
    if scope == "reference":
        return candidates
    pp = cfg.get("prediction_point") or {}
    selected = int(cfg["selected_n"])
    if selected not in candidates:
        raise ValueError(f"selected_n={selected} is not in n_candidates={candidates}")
    if pp.get("validation_or_test_used_for_selection") is not False or pp.get("test_labels_used_for_selection") is not False:
        raise ValueError("selected_n must be a design decision: validation/test must not be used for selection")
    if int(pp.get("selected_n", selected)) != selected:
        raise ValueError("prediction_point.selected_n and selected_n disagree")
    return [selected]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="P5-B-B6-01 N analysis (no full run by default).")
    ap.add_argument("--config", default=os.path.join(ROOT, "configs", "n_candidate_analysis.example.yaml"))
    ap.add_argument("--input-path", default="/data/curated/events")
    ap.add_argument("--mode", choices=["sample", "full"], default="sample")
    ap.add_argument("--analysis-scope", choices=["acceptance", "reference"], default="acceptance",
                    help="acceptance: official selected_n only (default). reference: N candidates table.")
    ap.add_argument("--sample-percent", type=float, default=None)
    ap.add_argument("--confirm-full-run", action="store_true")
    ap.add_argument("--output-dir", required=True, help="NEW directory; must not exist.")
    ap.add_argument("--run-id", required=True)
    return ap.parse_args(argv)


def _qa_pass(result: dict) -> tuple:
    problems = []
    for n, block in result["per_n"].items():
        for split in ("train", "validation"):
            if not (block[split]["checks"]["partition_ok"] and block[split]["checks"]["label_sum_ok"]):
                problems.append(f"N={n} {split}: partition or label-sum invariant failed")
            if block[split]["in_scope_sessions"] == 0:
                problems.append(f"N={n} {split}: empty split, no evidence for this N")
        if block["train"]["label_1"] == 0 and block["train"]["eligible"] > 0:
            problems.append(f"N={n} train: no positive label, cannot fit a classifier")
    if result["leakage_qa"]["candidate_events_at_or_after_own_split_end"] != 0:
        problems.append("candidate events at or after own split end")
    if result["leakage_qa"]["ranked_sessions_outside_train_validation"] != 0:
        problems.append("sessions outside train/validation were ranked")
    return (not problems), problems


def main(argv=None) -> int:
    args = parse_args(argv)

    if args.mode == "full" and not args.confirm_full_run:
        print("BLOCKED: --mode full requires --confirm-full-run (not the default).", file=sys.stderr)
        return 2
    if os.path.exists(args.output_dir):
        print(f"BLOCKED: output directory already exists: {args.output_dir}", file=sys.stderr)
        return 2

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    try:
        ns = resolve_analysis_ns(cfg, args.analysis_scope)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    bounds = SplitBounds.from_iso(cfg["windows_utc"])
    percent = args.sample_percent
    if percent is None:
        percent = float(cfg["sampling"]["test_mode_default_percent"]) if args.mode == "sample" else 100.0
    if args.mode == "sample" and percent > 1.0 and os.environ.get("ALLOW_LARGE_SAMPLE") != "1":
        print("BLOCKED: sample percent > 1 needs ALLOW_LARGE_SAMPLE=1.", file=sys.stderr)
        return 2

    spark = get_spark(app_name="P5B6N-Analysis", cfg=cfg_spark(cfg))
    spark.sparkContext.setLogLevel("ERROR")
    started = datetime.now(timezone.utc).isoformat()
    try:
        raw = spark.read.parquet(args.input_path)
        missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
        if missing:
            print(f"BLOCKED: input missing columns {missing}", file=sys.stderr)
            return 2
        input_files = len(raw.inputFiles())
        events = raw if args.mode == "full" else sample_whole_sessions(raw, percent)
        logger.info("Mode=%s percent=%s input_files=%d N=%s", args.mode, percent, input_files, ns)

        result = analyze_n_candidates(events, ns, bounds)
        ok, problems = _qa_pass(result)

        os.makedirs(args.output_dir)
        with open(os.path.join(args.output_dir, "n_candidate_result.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        with open(os.path.join(args.output_dir, "n_candidate_table.md"), "w", encoding="utf-8") as f:
            f.write(_table_md(result))
        manifest = {
            "task": "P5-B-B6-01",
            "run_id": args.run_id,
            "started_utc": started,
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "sample_percent": None if args.mode == "full" else percent,
            "input_path": args.input_path,
            "input_files": input_files,
            "analysis_scope": args.analysis_scope,
            "selected_n": int(cfg["selected_n"]),
            "n_analysed": ns,
            "windows_utc": cfg["windows_utc"],
            "config_snapshot": cfg,
            "spark_version": spark.version,
            "python_version": sys.version.split()[0],
            "qa_pass": ok,
            "qa_problems": problems,
            "decision_note": (
                "Official N comes from config selected_n (design decision by member B). The script never "
                "selects N from data. H11 windows remain PENDING_A_REVIEW; member A has not approved."
            ),
        }
        with open(os.path.join(args.output_dir, "run_manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        print(json.dumps({"qa_pass": ok, "problems": problems, "output_dir": args.output_dir}, indent=2,
                         ensure_ascii=False))
        return 0 if ok else 1
    except Exception as exc:  # noqa: BLE001 - recorded in exit code and stderr
        logger.error("Analysis failed: %s", exc)
        print(f"FAILED: {type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}", file=sys.stderr)
        return 1
    finally:
        spark.stop()


def cfg_spark(cfg: dict) -> dict:
    s = cfg.get("spark", {})
    return {"ml": {"spark_master": s.get("master", "local[2]"), "driver_memory": s.get("driver_memory", "1g"),
                   "shuffle_partitions": s.get("shuffle_partitions", 4)}}


if __name__ == "__main__":
    sys.exit(main())
