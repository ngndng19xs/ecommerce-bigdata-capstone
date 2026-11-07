"""Evaluate P4-B-B7-02 acceptance from pytest JUnit XML. Never writes PASS by itself.

Input : --junit <path>   JUnit XML produced by pytest --junitxml for tests/unit/test_b7_02_preprocessing_seed.py
        --source <json>  optional source_integrity.json from source_manifest_b7_02.py verify
        --env <json>     optional environment.json (python/pyspark/java versions)
Output: evidence_check.json next to --out (default: alongside the junit file)

Each required TC must appear in the XML AND have passed. Missing -> NOT_RUN (counts as not passed).
Exit 0: every required TC PASS and source PASS. Exit 1: any TC FAIL/NOT_RUN or source FAIL.
Exit 2: junit file missing or unreadable (BLOCKED).
"""

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

REQUIRED = {
    "TC01_indexer_fit_train_only": "test_tc01_indexer_labels_learned_from_train_only",
    "TC02_unknown_category": "test_tc02_unknown_category_keeps_rows_and_is_not_mapped_to_known",
    "TC03_null_category": "test_tc03_null_category_keeps_rows",
    "TC04_stable_mapping": "test_tc04_known_category_mapping_stable_across_transforms",
    "TC05_vector_dimension": "test_tc05_vector_dimension_consistent_and_fixed_after_fit",
    "TC06_encoder_config_and_collision": "test_tc06_encoder_config_and_unknown_vector_distinct_from_known",
    "TC07_encoder_not_refit": "test_tc07_encoder_metadata_not_refit_on_validation",
    "TC08_assembler_inputs": "test_tc08_assembler_inputs_are_exact_pipeline_features",
    "TC09_feature_order": "test_tc09_feature_order_is_positional",
    "TC10_no_forbidden_features": "test_tc10_no_forbidden_column_enters_features",
    "TC11_null_numeric_rejected": "test_tc11_null_numeric_is_rejected_not_passed_to_model",
    "TC12_scaler_train_only": "test_tc12_scaler_statistics_are_train_only",
    "TC13_save_load": "test_tc13_save_load_consistency",
    "TC14_randomness_audit": "test_tc14_randomness_audit_on_b7_path",
    "TC15_split_deterministic": "test_tc15_chronological_split_deterministic_and_order_independent",
    "TC15b_membership_partitioning": "test_tc15_membership_identical_across_partitionings",
    "TC16_no_hidden_python_random": "test_tc16_no_hidden_python_randomness_in_fit",
    "TC17_repeatability_partitioning": "test_tc17_repeatability_across_partitionings",
    "TC18_hyperparam_config": "test_tc18_hyperparameter_config_matches_code_defaults",
}


def _read_junit(path):
    root = ET.parse(path).getroot()
    results = {}
    for case in root.iter("testcase"):
        name = case.get("name")
        status = "PASS"
        for child in case:
            if child.tag in ("failure", "error"):
                status = "FAIL"
            elif child.tag == "skipped":
                status = "SKIPPED"
        results[name] = status
    return results


def evaluate(junit_results, source=None):
    rows = []
    for tc, test_name in REQUIRED.items():
        status = junit_results.get(test_name, "NOT_RUN")
        rows.append({"tc": tc, "pytest_name": test_name, "status": status})
    source_status = (source or {}).get("status", "NOT_RUN")
    overall_ok = all(r["status"] == "PASS" for r in rows) and source_status == "PASS"
    return rows, source_status, ("PASS" if overall_ok else "FAIL")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit", required=True)
    parser.add_argument("--source", default=None)
    parser.add_argument("--env", default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    result = {
        "task": "P4-B-B7-02",
        "checked_utc": datetime.now(timezone.utc).isoformat(),
        "junit": args.junit,
    }
    try:
        junit_results = _read_junit(args.junit)
    except (OSError, ET.ParseError) as exc:
        result.update(OVERALL="BLOCKED", reason=f"junit unreadable: {exc}", criteria=[])
        code = 2
    else:
        source = None
        if args.source and os.path.isfile(args.source):
            with open(args.source, "r", encoding="utf-8") as f:
                source = json.load(f)
        env = None
        if args.env and os.path.isfile(args.env):
            with open(args.env, "r", encoding="utf-8") as f:
                env = json.load(f)
        rows, source_status, overall = evaluate(junit_results, source)
        result.update(
            OVERALL=overall,
            source_status=source_status,
            tests_in_junit=len(junit_results),
            criteria=rows,
            environment=env,
        )
        code = 0 if overall == "PASS" else 1
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(json.dumps({"OVERALL": result["OVERALL"], "out": args.out}, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())
