"""P5 ML stage 1: comparator must never call a candidate EQUAL when a required N is missing."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "run"))

import compare_p5_results as cmp  # noqa: E402


def _block(seed):
    base = {"in_scope_sessions": 10 + seed, "too_short": 2, "early_purchase": 1, "eligible": 7 + seed,
            "label_0": 5, "label_1": 2 + seed, "len_equals_n": 1}
    return {"train": dict(base), "validation": dict(base), "train_plus_validation": dict(base)}


def _result(ns):
    return {
        "events": {"total": 100, "invalid_session_events": 0, "null_event_time_events_in_valid_sessions": 0},
        "session_buckets": {"train": {"sessions": 10}, "validation": {"sessions": 4}},
        "per_n": {str(n): _block(n) for n in ns},
        "leakage_qa": {"candidate_events_at_or_after_own_split_end": 0},
    }


def test_identical_results_are_equal_with_required_n():
    report = cmp.compare(_result([2, 3, 5]), _result([2, 3, 5]), require_n=["2", "3", "5"])
    assert report["equal"] is True


def test_candidate_missing_a_required_n_is_never_equal():
    report = cmp.compare(_result([2, 3, 5]), _result([3]), require_n=["2", "3", "5"])
    assert report["equal"] is False
    assert any("missing N in candidate" in d for d in report["differences"])


def test_without_require_n_only_common_values_are_compared():
    # the historical behaviour is kept for callers that do not pass --require-n
    report = cmp.compare(_result([2, 3, 5]), _result([3]))
    assert report["equal"] is True
    assert report["n_only_in_baseline"] == ["2", "5"]


def test_baseline_missing_a_required_n_cannot_be_compared():
    with pytest.raises(ValueError):
        cmp.compare(_result([3]), _result([3]), require_n=["2", "3"])


def test_a_real_difference_is_still_reported():
    cand = _result([3])
    cand["per_n"]["3"]["train"]["eligible"] = 999
    report = cmp.compare(_result([3]), cand, require_n=["3"])
    assert report["equal"] is False
    assert any("per_n.3.train.eligible" in d for d in report["differences"])


def test_main_returns_blocked_for_bad_require_n(tmp_path):
    import json

    base = tmp_path / "base.json"
    cand = tmp_path / "cand.json"
    base.write_text(json.dumps(_result([3])), encoding="utf-8")
    cand.write_text(json.dumps(_result([3])), encoding="utf-8")
    code = cmp.main(["--baseline", str(base), "--candidate", str(cand), "--out", str(tmp_path / "o.json"),
                     "--require-n", "three"])
    assert code == 2
    code = cmp.main(["--baseline", str(base), "--candidate", str(cand), "--out", str(tmp_path / "o.json"),
                     "--require-n", "3"])
    assert code == 0
