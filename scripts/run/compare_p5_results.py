"""Compare P5-B-B6-01 results on BUSINESS values only (never byte-for-byte JSON).

Compared exactly:
- events: total, invalid_session_events, null_event_time_events_in_valid_sessions
- session_buckets: number of sessions per bucket (gap and after_test_end included)
- per_n: for every N present in BOTH files, for train / validation / train_plus_validation:
  in_scope_sessions, too_short, early_purchase, eligible, label_0, label_1, len_equals_n
- leakage QA: all leakage counters are zero (legacy and staged key names both understood)

Ignored on purpose: run_id, timings, plan text, config snapshot, field order, engine metadata.

Usage:
  python scripts/run/compare_p5_results.py --baseline A/n_candidate_result.json \
      --candidate B/n_candidate_result.json [--candidate C/...] --out compare.json
Exit 0: every candidate equals the baseline. Exit 1: a difference. Exit 2: missing/unreadable file.
"""

import argparse
import json
import os
import sys

SPLITS = ("train", "validation", "train_plus_validation")
METRICS = ("in_scope_sessions", "too_short", "early_purchase", "eligible", "label_0", "label_1", "len_equals_n")
EVENT_KEYS = ("total", "invalid_session_events", "null_event_time_events_in_valid_sessions")
LEAKAGE_KEYS = (
    "candidate_events_at_or_after_own_split_end",          # legacy
    "ranked_sessions_outside_train_validation",            # legacy
    "candidate_sessions_ending_at_or_after_own_split_end",  # staged
    "ranked_test_range_sessions",                          # staged
    "length_mismatch_between_summary_and_ranking",         # staged
)


def leakage_zero(result: dict) -> bool:
    lq = result.get("leakage_qa", {})
    return all(int(lq.get(k, 0) or 0) == 0 for k in LEAKAGE_KEYS)


def extract(result: dict) -> dict:
    return {
        "events": {k: result.get("events", {}).get(k) for k in EVENT_KEYS},
        "session_buckets": {k: v.get("sessions") for k, v in result.get("session_buckets", {}).items()},
        "per_n": {
            n: {s: {m: block[s][m] for m in METRICS} for s in SPLITS}
            for n, block in result.get("per_n", {}).items()
        },
        "leakage_zero": leakage_zero(result),
    }


def compare(baseline: dict, candidate: dict, require_n=None) -> dict:
    """Compare business values. require_n: N values (strings) that BOTH files must contain.

    A candidate missing a required N is a difference (never EQUAL). A baseline missing a required N
    raises ValueError: the comparison cannot be made.
    """
    a, b = extract(baseline), extract(candidate)
    diffs = []
    if require_n:
        missing_in_base = [n for n in require_n if n not in a["per_n"]]
        if missing_in_base:
            raise ValueError(f"baseline has no N {missing_in_base}; cannot compare")
        missing_in_cand = [n for n in require_n if n not in b["per_n"]]
        if missing_in_cand:
            diffs.append(f"missing N in candidate: {missing_in_cand} (required {list(require_n)})")
    for k in EVENT_KEYS:
        if a["events"][k] != b["events"][k]:
            diffs.append(f"events.{k}: baseline={a['events'][k]} candidate={b['events'][k]}")
    for k in sorted(set(a["session_buckets"]) | set(b["session_buckets"])):
        va, vb = a["session_buckets"].get(k, 0), b["session_buckets"].get(k, 0)
        if va != vb:
            diffs.append(f"session_buckets.{k}: baseline={va} candidate={vb}")
    common = sorted(set(a["per_n"]) & set(b["per_n"]), key=int)
    if not common:
        diffs.append("per_n: no N value in common")
    for n in common:
        for s in SPLITS:
            for m in METRICS:
                va, vb = a["per_n"][n][s][m], b["per_n"][n][s][m]
                if va != vb:
                    diffs.append(f"per_n.{n}.{s}.{m}: baseline={va} candidate={vb}")
    if not (a["leakage_zero"] and b["leakage_zero"]):
        diffs.append(f"leakage: baseline_zero={a['leakage_zero']} candidate_zero={b['leakage_zero']}")
    return {
        "equal": not diffs,
        "required_n": list(require_n) if require_n else None,
        "n_compared": common,
        "n_only_in_baseline": sorted(set(a["per_n"]) - set(b["per_n"]), key=int),
        "n_only_in_candidate": sorted(set(b["per_n"]) - set(a["per_n"]), key=int),
        "differences": diffs,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Compare P5-B-B6-01 results on business values.")
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", action="append", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--require-n", default=None,
                    help="Comma-separated N that every candidate must contain, e.g. 3 or 2,3,5 (missing = mismatch).")
    args = ap.parse_args(argv)

    try:
        with open(args.baseline, "r", encoding="utf-8") as f:
            base = json.load(f)
        cands = []
        for p in args.candidate:
            with open(p, "r", encoding="utf-8") as f:
                cands.append((p, json.load(f)))
    except (OSError, ValueError) as exc:
        print(f"BLOCKED: cannot read result file: {exc}", file=sys.stderr)
        return 2

    require_n = None
    if args.require_n:
        try:
            require_n = [str(int(x)) for x in args.require_n.split(",") if x.strip()]
        except ValueError:
            print(f"BLOCKED: --require-n must be integers, got {args.require_n!r}", file=sys.stderr)
            return 2

    report = {"baseline": args.baseline, "comparisons": []}
    for path, cand in cands:
        try:
            r = compare(base, cand, require_n)
        except ValueError as exc:
            print(f"BLOCKED: {exc}", file=sys.stderr)
            return 2
        r["candidate"] = path
        report["comparisons"].append(r)
    report["all_equal"] = all(r["equal"] for r in report["comparisons"])
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    for r in report["comparisons"]:
        status = "EQUAL" if r["equal"] else f"DIFFERENT ({len(r['differences'])} fields)"
        print(f"{status}: {r['candidate']} (N compared: {r['n_compared']})")
        for d in r["differences"][:20]:
            print(f"  - {d}")
    return 0 if report["all_equal"] else 1


if __name__ == "__main__":
    sys.exit(main())
