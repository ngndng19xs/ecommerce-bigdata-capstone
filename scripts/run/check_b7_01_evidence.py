"""Evaluate P4-B-B7-01 acceptance criteria from the evidence files. Never hardcodes a PASS.

Inputs (all in one report directory):
  smoke_report.json       written by run_b7_lr_smoke.py
  b6_writer_stats.json    written by run_b6_session_features.py
  tc16_source.json        written by source_manifest.py verify

Output: evidence_check.json with one entry per criterion (PASS / FAIL / BLOCKED) and OVERALL.
Exit 0: all criteria PASS. Exit 1: any criterion FAIL. Exit 2: an evidence file is missing (BLOCKED).

dates_not_read is NOT compared with a fixed number. It is checked against the identity
    distinct_session_dates_total == len(train_dates) + len(val_dates) + dates_not_read
and the split must be chronological (train dates all before validation dates).
"""

import argparse
import json
import os
import sys


def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def evaluate(smoke, writer, tc16):
    crit = []

    def add(name, ok, detail):
        crit.append({"criterion": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    add("E6 smoke_status PASS and exit_code 0",
        smoke.get("smoke_status") == "PASS" and smoke.get("exit_code") == 0,
        f"smoke_status={smoke.get('smoke_status')} exit_code={smoke.get('exit_code')}")

    add("TC16 source integrity PASS (manifest SHA-256 match)",
        tc16.get("status") == "PASS",
        f"status={tc16.get('status')} files_matched={tc16.get('files_matched')}/{tc16.get('files_total')}")

    split = smoke.get("split", {})
    train_d = split.get("train_dates", [])
    val_d = split.get("val_dates", [])
    not_read = split.get("dates_not_read")
    total = split.get("distinct_session_dates_total")
    identity_ok = (
        isinstance(total, int) and isinstance(not_read, int)
        and total == len(train_d) + len(val_d) + not_read and not_read >= 0
    )
    chrono_ok = bool(train_d) and bool(val_d) and max(train_d) < min(val_d)
    add("TC14 test range not read: date identity holds and split is chronological",
        identity_ok and chrono_ok,
        f"distinct_total={total} train={len(train_d)} val={len(val_d)} not_read={not_read} chronological={chrono_ok}")

    train = smoke.get("train", {})
    add("TC05 train has both classes",
        train.get("pos", 0) > 0 and train.get("neg", 0) > 0,
        f"train pos={train.get('pos')} neg={train.get('neg')}")

    val = smoke.get("validation", {})
    add("TC13 validation transform row count preserved and outputs finite",
        val.get("validation_rows_in") == val.get("validation_rows_out")
        and val.get("bad_prob_size") == 0 and val.get("bad_prob_value") == 0
        and val.get("bad_features") == 0 and val.get("null_prediction") == 0,
        f"in={val.get('validation_rows_in')} out={val.get('validation_rows_out')}")

    add("B6 writer produced a non-empty, two-class dataset",
        writer.get("output_session_rows", 0) > 0 and len(writer.get("output_label_counts", {})) == 2,
        f"rows={writer.get('output_session_rows')} labels={writer.get('output_label_counts')}")

    add("B6 writer sample is consistent (eligible <= sampled sessions, same N)",
        writer.get("output_session_rows", 0) <= writer.get("sampled_distinct_sessions", 0)
        and writer.get("prediction_n") == smoke.get("prediction_n"),
        f"eligible={writer.get('output_session_rows')} sampled={writer.get('sampled_distinct_sessions')} "
        f"N writer={writer.get('prediction_n')} smoke={smoke.get('prediction_n')}")

    overall = "PASS" if all(c["status"] == "PASS" for c in crit) else "FAIL"
    return crit, overall


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir", required=True)
    args = parser.parse_args()

    paths = {
        "smoke": os.path.join(args.report_dir, "smoke_report.json"),
        "writer": os.path.join(args.report_dir, "b6_writer_stats.json"),
        "tc16": os.path.join(args.report_dir, "tc16_source.json"),
    }
    missing = [name for name, p in paths.items() if not os.path.isfile(p)]
    result = {"report_dir": args.report_dir, "evidence_missing": missing}
    if missing:
        result.update(OVERALL="BLOCKED", criteria=[], reason=f"missing evidence: {missing}")
        code = 2
    else:
        crit, overall = evaluate(_load(paths["smoke"]), _load(paths["writer"]), _load(paths["tc16"]))
        result.update(OVERALL=overall, criteria=crit)
        code = 0 if overall == "PASS" else 1
    out = os.path.join(args.report_dir, "evidence_check.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(json.dumps({"OVERALL": result["OVERALL"], "evidence_check": out}, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())
