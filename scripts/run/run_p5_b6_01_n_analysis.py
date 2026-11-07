"""P5-B-B6-01 N candidate analysis CLI.

Modes:
  sample (default): whole-session hash sample (default 0.1% of sessions). Development and QA only.
  full            : all of Curated. Requires --confirm-full-run. Never started by default.
  snapshot        : reads a stage-1 snapshot or nested subset (part=sample or subsets/<name>) with its
                    manifest. No sampling is applied again. Manifest must match schema and row count.

Writes ONLY to a NEW --output-dir (refuses to overwrite). Never writes to HDFS and never
touches Curated, ML features, models or B7 outputs.

Exit codes: 0 analysis completed and QA invariants PASS; 1 analysis error or QA FAIL;
2 blocked (bad arguments, preflight failure, output directory exists).
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import yaml  # noqa: E402

from src.common.logger import get_logger  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.n_selection import (  # noqa: E402
    SplitBounds,
    WindowGroupGateError,
    analyze_n_candidates,
    analyze_n_candidates_staged,
    assert_session_utc,
    diagnose_session_buckets,
    parse_n_candidates,
)
from src.ml.training.lr_b6_smoke import sample_whole_sessions  # noqa: E402
from src.ml.dataset.ml_stage1 import check_input_manifest, file_uri, load_json, sha256_file  # noqa: E402

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
    ap.add_argument("--mode", choices=["sample", "full", "snapshot"], default="sample")
    ap.add_argument("--input-manifest", default=None,
                    help="snapshot mode only: manifest JSON of --input-path (snapshot_manifest.json or _subset_manifest.json).")
    ap.add_argument("--analysis-scope", choices=["acceptance", "reference"], default="acceptance",
                    help="acceptance: official selected_n only (default). reference: N candidates table.")
    ap.add_argument("--sample-percent", type=float, default=None)
    ap.add_argument("--confirm-full-run", action="store_true")
    ap.add_argument("--diagnose-only", action="store_true",
                    help="Explain session buckets only (no ranking, no labels). Writes gap_diagnostic.json.")
    ap.add_argument("--engine", choices=["legacy", "staged"], default="legacy",
                    help="legacy: accepted implementation (oracle). staged: Phase A -> gate -> Phase B.")
    ap.add_argument("--ranking-input", choices=["semi_join", "time_filter"], default="semi_join",
                    help="staged engine only: events entering the S2 window.")
    ap.add_argument("--materialize", choices=["disk_persist", "staging"], default="disk_persist",
                    help="staged engine only: how the Phase A session summary is kept between actions.")
    ap.add_argument("--staging-dir", default=None,
                    help="staged + staging only: NEW local directory URI, e.g. file:///home/student/p5_staging/RUN")
    ap.add_argument("--max-window-group", type=int, default=None,
                    help="staged engine only: block Phase B if the largest window group exceeds this "
                         "(default: record only).")
    ap.add_argument("--aqe", choices=["keep", "on", "off"], default="keep",
                    help="spark.sql.adaptive.enabled: keep the session value (Spark 3.1 default false), or set it.")
    ap.add_argument("--output-dir", required=True, help="NEW directory; must not exist.")
    ap.add_argument("--run-id", required=True)
    return ap.parse_args(argv)


def _local_path_of(uri: str):
    """Local filesystem path for file:// URIs (None for HDFS or other schemes)."""
    if uri.startswith("file://"):
        from urllib.parse import urlparse
        from urllib.request import url2pathname
        return url2pathname(urlparse(uri).path)
    return None


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
    lq = result["leakage_qa"]
    # Legacy engine keys
    if lq.get("candidate_events_at_or_after_own_split_end", 0) != 0:
        problems.append("candidate events at or after own split end")
    if lq.get("ranked_sessions_outside_train_validation", 0) != 0:
        problems.append("sessions outside train/validation were ranked")
    # Staged engine keys
    if lq.get("candidate_sessions_ending_at_or_after_own_split_end", 0) != 0:
        problems.append("candidate sessions end at or after own split end")
    if lq.get("ranked_test_range_sessions", 0) != 0:
        problems.append("test-range sessions reached the ranking phase (test must stay sealed)")
    if lq.get("length_mismatch_between_summary_and_ranking", 0) != 0:
        problems.append("ranked session length differs from the Phase A session length")
    gap = result.get("session_buckets", {}).get("gap", {}).get("sessions", 0)
    if gap:
        problems.append(f"{gap} sessions unexplained by any bucket rule (gap): investigate, never dropped silently")
    return (not problems), problems


def _qa_warnings(result: dict) -> list:
    """Explained exclusions that must be visible in the report but do not fail QA."""
    b = result.get("session_buckets", {})
    warnings = []
    null_sessions = b.get("excluded_null_event_time", {}).get("sessions", 0)
    if null_sessions:
        warnings.append(f"{null_sessions} sessions excluded: at least one event with NULL event_time")
    after = b.get("after_test_end", {}).get("sessions", 0)
    if after:
        warnings.append(f"{after} sessions start at or after test_end and are outside the analysis")
    null_events = result.get("events", {}).get("null_event_time_events_in_valid_sessions", 0)
    if null_events:
        warnings.append(f"{null_events} events with NULL event_time inside valid sessions")
    return warnings


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
    if args.engine == "staged" and args.materialize == "staging":
        if not args.staging_dir:
            print("BLOCKED: --materialize staging requires --staging-dir.", file=sys.stderr)
            return 2
        local = _local_path_of(args.staging_dir)
        if local is not None and os.path.exists(local):
            print(f"BLOCKED: staging directory already exists: {args.staging_dir}", file=sys.stderr)
            return 2

    if args.mode == "snapshot":
        if args.sample_percent is not None:
            print("BLOCKED: --mode snapshot does not sample; do not pass --sample-percent.", file=sys.stderr)
            return 2
        if not args.input_manifest or not os.path.exists(args.input_manifest):
            print("BLOCKED: --mode snapshot needs --input-manifest pointing to an existing manifest.", file=sys.stderr)
            return 2
        snap_manifest = load_json(args.input_manifest)
        if snap_manifest.get("kind") not in ("snapshot", "subset"):
            print("BLOCKED: manifest kind must be snapshot or subset.", file=sys.stderr)
            return 2
        if snap_manifest["kind"] == "snapshot" and os.path.basename(args.input_path.rstrip("/\\")) != "part=sample":
            print("BLOCKED: a snapshot must be read from its part=sample directory, never the root.", file=sys.stderr)
            return 2
        snap_manifest_sha = sha256_file(args.input_manifest)

    spark = get_spark(app_name="P5B6N-Analysis", cfg=cfg_spark(cfg))
    if args.aqe != "keep":
        spark.conf.set("spark.sql.adaptive.enabled", "true" if args.aqe == "on" else "false")
    spark.sparkContext.setLogLevel("ERROR")
    started = datetime.now(timezone.utc).isoformat()
    try:
        session_tz = assert_session_utc(spark)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        spark.stop()
        return 2
    python_offset_hours = datetime.now().astimezone().utcoffset().total_seconds() / 3600
    try:
        read_path = args.input_path
        if args.mode == "snapshot" and "://" not in read_path:
            read_path = file_uri(read_path)  # local snapshot: never resolved against HDFS
        raw = spark.read.parquet(read_path)
        missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
        if missing:
            print(f"BLOCKED: input missing columns {missing}", file=sys.stderr)
            return 2
        input_files = len(raw.inputFiles())
        if args.mode == "snapshot":
            problems_in = check_input_manifest(raw, snap_manifest)
            if problems_in:
                print(f"BLOCKED: input does not match its manifest: {problems_in}", file=sys.stderr)
                return 2
            events = raw
        else:
            events = raw if args.mode == "full" else sample_whole_sessions(raw, percent)
        logger.info("Mode=%s percent=%s input_files=%d N=%s", args.mode, percent, input_files, ns)

        if args.diagnose_only:
            diag = diagnose_session_buckets(events, bounds)
            os.makedirs(args.output_dir)
            with open(os.path.join(args.output_dir, "gap_diagnostic.json"), "w", encoding="utf-8") as f:
                json.dump({"mode": args.mode, "sample_percent": None if args.mode == "full" else percent,
                           "input_path": args.input_path, "input_files": input_files,
                           "bounds_utc": cfg["windows_utc"], "diagnostic": diag}, f, indent=2,
                          ensure_ascii=False)
            print(json.dumps({"diagnosis": diag["diagnosis"], "unexplained_gap_sessions": diag["unexplained_gap_sessions"]},
                             indent=2, ensure_ascii=False))
            return 0

        t0 = time.time()
        if args.engine == "staged":
            try:
                result = analyze_n_candidates_staged(
                    events, ns, bounds,
                    ranking_input=args.ranking_input,
                    materialize=args.materialize,
                    staging_dir=args.staging_dir,
                    max_window_group=args.max_window_group,
                )
            except WindowGroupGateError as gate_exc:
                os.makedirs(args.output_dir)
                with open(os.path.join(args.output_dir, "gate_report.json"), "w", encoding="utf-8") as f:
                    json.dump({"status": "BLOCKED", "reason": str(gate_exc), "gate": gate_exc.gate,
                               "run_id": args.run_id, "mode": args.mode}, f, indent=2, ensure_ascii=False)
                print(f"BLOCKED: {gate_exc}", file=sys.stderr)
                return 2
        else:
            result = analyze_n_candidates(events, ns, bounds)
        elapsed = round(time.time() - t0, 1)
        ok, problems = _qa_pass(result)
        warnings = _qa_warnings(result)

        os.makedirs(args.output_dir)
        plan_files = {}
        for phase, text in ((result.get("execution") or {}).pop("plans", {}) or {}).items():
            name = f"plan_{phase}.txt"
            with open(os.path.join(args.output_dir, name), "w", encoding="utf-8") as f:
                f.write(text)
            plan_files[phase] = name
        if "execution" in result:
            result["execution"]["plan_files"] = plan_files
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
            "sample_percent": None if args.mode in ("full", "snapshot") else percent,
            "source_scope": "FULL" if args.mode == "full" else "SAMPLED_SESSIONS",
            "sample_threshold": (int(snap_manifest["threshold"]) if args.mode == "snapshot"
                                 else (None if args.mode == "full" else int(round(percent * 100)))),
            "input_kind": ("snapshot_" + snap_manifest["kind"]) if args.mode == "snapshot" else args.mode,
            "input_name": snap_manifest.get("name") if args.mode == "snapshot" else None,
            "input_manifest": args.input_manifest,
            "input_manifest_sha256": snap_manifest_sha if args.mode == "snapshot" else None,
            "shuffle_partitions_config": cfg_spark(cfg)["ml"]["shuffle_partitions"],
            "input_path": args.input_path,
            "input_files": input_files,
            "analysis_scope": args.analysis_scope,
            "engine": args.engine,
            "ranking_input": args.ranking_input if args.engine == "staged" else None,
            "materialize": args.materialize if args.engine == "staged" else None,
            "analysis_elapsed_seconds": elapsed,
            "selected_n": int(cfg["selected_n"]),
            "n_analysed": ns,
            "windows_utc": cfg["windows_utc"],
            "config_snapshot": cfg,
            "spark_version": spark.version,
            "spark_session_timezone": session_tz,
            "python_local_utc_offset_hours": python_offset_hours,
            "tz_env": os.environ.get("TZ"),
            "python_version": sys.version.split()[0],
            "qa_pass": ok,
            "qa_problems": problems,
            "qa_warnings": warnings,
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
