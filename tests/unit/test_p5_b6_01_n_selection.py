"""P5-B-B6-01: N candidate analysis tests.

Expected numbers are derived by hand from the synthetic sessions below (see the table in
the docstring of build_fixture), not by running the code under test.

Windows (chronological, UTC, end exclusive):
  train      2019-10-01T00:00Z .. 2019-10-05T00:00Z
  validation 2019-10-05T00:00Z .. 2019-10-08T00:00Z
  test       2019-10-08T00:00Z .. 2019-10-10T00:00Z
"""

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ml.session_features import build_session_features  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.ml.dataset.n_selection import (  # noqa: E402
    SplitBounds,
    analyze_n_candidates,
    parse_n_candidates,
    valid_session_events,
    classify_sessions,
)

RANK = {"view": 1, "cart": 2, "remove_from_cart": 3, "purchase": 4}
CFG = {
    "train_start": "2019-10-01T00:00:00Z",
    "train_end_exclusive": "2019-10-05T00:00:00Z",
    "validation_start": "2019-10-05T00:00:00Z",
    "validation_end_exclusive": "2019-10-08T00:00:00Z",
    "test_start": "2019-10-08T00:00:00Z",
    "test_end_exclusive": "2019-10-10T00:00:00Z",
}
BOUNDS = SplitBounds.from_iso(CFG)
T = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")  # noqa: E731

SCHEMA = StructType([
    StructField("user_session", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_type_rank", IntegerType(), True),
    StructField("event_sort_key", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
])


@pytest.fixture(scope="module")
def spark():
    session = get_spark("TestP5B6N")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _session_rows(sid, start, kinds, step=60, counter=None):
    rows = []
    for k, kind in enumerate(kinds):
        counter[0] += 1
        rows.append((sid, start + timedelta(seconds=step * k), kind, RANK[kind],
                     "{:064x}".format(counter[0]), f"P{k % 3}", f"C{k % 2}", "A", 10.0 + k))
    return rows


def build_fixture(overrides=None):
    """Sessions (kinds listed in event order).

    Train (N=2 / N=3 / N=5 expectations in the tests):
      T_short1  start=train_start      [view]                         too short for all N
      T_eq2     [view, cart]           length == 2 -> eligible label 0 for N=2
      T_early   [view, purchase, cart] purchase at event 2 -> early purchase for N=2,3
      T_p1      [view, cart, view, purchase]  purchase at event 4 -> label 1 for N=2,3
      T_p0      six views              label 0 for N=2,3,5
      T_cross   start 10-04T23:00, ends 10-05T01:00 -> censored_train (purchase inside)
    Validation:
      V_exact   start=validation_start [view, cart, view, purchase] -> label 1 for N=2,3
      V_p0      start 10-06, five views -> label 0 for N=2,3,5 (len == 5 for N=5)
      V_cross   start 10-07T23:00, ends 10-08T01:00 -> censored_validation (purchase inside)
    Test range (must never change train/validation results):
      X_a       start 10-08T01:00 [view, cart, purchase]
      X_b       start 10-09T12:00 [view, view]
    Invalid:  two events with NULL user_session and one with blank session.
    """
    counter = [0]
    sessions = [
        ("T_short1", T("2019-10-01T00:00:00"), ["view"]),
        ("T_eq2", T("2019-10-02T10:00:00"), ["view", "cart"]),
        ("T_early", T("2019-10-02T11:00:00"), ["view", "purchase", "cart"]),
        ("T_p1", T("2019-10-02T12:00:00"), ["view", "cart", "view", "purchase"]),
        ("T_p0", T("2019-10-03T09:00:00"), ["view"] * 6),
        ("T_cross", T("2019-10-04T23:00:00"), ["view", "cart", "purchase"]),
        ("V_exact", T("2019-10-05T00:00:00"), ["view", "cart", "view", "purchase"]),
        ("V_p0", T("2019-10-06T09:00:00"), ["view"] * 5),
        ("V_cross", T("2019-10-07T23:00:00"), ["view", "cart", "purchase"]),
        ("X_a", T("2019-10-08T01:00:00"), ["view", "cart", "purchase"]),
        ("X_b", T("2019-10-09T12:00:00"), ["view", "view"]),
    ]
    rows = []
    for sid, start, kinds in sessions:
        k = list(kinds)
        if overrides and sid in overrides:
            k = overrides[sid](k)
        if sid == "T_cross":
            rows += _session_rows(sid, start, k, step=3600, counter=counter)  # ends 10-05T01:00
        elif sid == "V_cross":
            rows += _session_rows(sid, start, k, step=3600, counter=counter)  # ends 10-08T01:00
        else:
            rows += _session_rows(sid, start, k, step=60, counter=counter)
    # Invalid session IDs: NULL and blank must not count as sessions.
    for i, sid in enumerate([None, None, "   "]):
        counter[0] += 1
        rows.append((sid, T("2019-10-02T08:00:00") + timedelta(seconds=i), "view", 1,
                     "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    return rows


STRING_TIME_SCHEMA = StructType(
    [StructField(f.name, StringType() if f.name == "event_time" else f.dataType, True) for f in SCHEMA.fields]
)


def make_df(spark, rows):
    """Build events with event_time given as UTC wall-clock values.

    Rows hold naive datetimes meaning UTC. They are sent to Spark as strings and parsed in the
    session time zone (UTC). Passing datetime objects to createDataFrame would convert them with
    the local time zone of the Python process, which hid the timezone bug in earlier tests.
    """
    as_text = [
        (r[0], None if r[1] is None else r[1].strftime("%Y-%m-%d %H:%M:%S")) + tuple(r[2:]) for r in rows
    ]
    df = spark.createDataFrame(as_text, STRING_TIME_SCHEMA)
    return df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))


@pytest.fixture(scope="module")
def base_result(spark):
    return analyze_n_candidates(make_df(spark, build_fixture()), [2, 3, 5], BOUNDS)


# --- Parsing and windows ------------------------------------------------------


def test_t01_n_candidates_sorted_unique_and_validated():
    assert parse_n_candidates([5, 2, 3, 3]) == [2, 3, 5]
    with pytest.raises(ValueError):
        parse_n_candidates([0, 2])
    with pytest.raises(ValueError):
        parse_n_candidates([])


def test_t02_windows_must_be_ordered_and_non_overlapping():
    bad = dict(CFG, train_end_exclusive="2019-10-06T00:00:00Z")  # overlaps validation
    with pytest.raises(ValueError):
        SplitBounds.from_iso(bad)


# --- Exact expectations (N=2, N=3, N=5) ---------------------------------------


def test_t03_train_counts_match_hand_derived_expectations(base_result):
    tr2 = base_result["per_n"]["2"]["train"]
    assert (tr2["in_scope_sessions"], tr2["too_short"], tr2["early_purchase"], tr2["eligible"]) == (5, 1, 1, 3)
    assert (tr2["label_0"], tr2["label_1"], tr2["len_equals_n"]) == (2, 1, 1)
    tr3 = base_result["per_n"]["3"]["train"]
    assert (tr3["too_short"], tr3["early_purchase"], tr3["eligible"]) == (2, 1, 2)
    assert (tr3["label_0"], tr3["label_1"]) == (1, 1)
    tr5 = base_result["per_n"]["5"]["train"]
    assert (tr5["too_short"], tr5["early_purchase"], tr5["eligible"]) == (4, 0, 1)
    assert (tr5["label_0"], tr5["label_1"]) == (1, 0)


def test_t04_validation_counts_match_hand_derived_expectations(base_result):
    va2 = base_result["per_n"]["2"]["validation"]
    assert (va2["in_scope_sessions"], va2["eligible"], va2["label_0"], va2["label_1"]) == (2, 2, 1, 1)
    va5 = base_result["per_n"]["5"]["validation"]
    assert (va5["too_short"], va5["eligible"], va5["label_0"], va5["len_equals_n"]) == (1, 1, 1, 1)


def test_t05_partition_and_label_sum_invariants(base_result):
    for n, block in base_result["per_n"].items():
        for split in ("train", "validation", "train_plus_validation"):
            r = block[split]
            assert r["in_scope_sessions"] == r["too_short"] + r["early_purchase"] + r["eligible"], (n, split)
            assert r["eligible"] == r["label_0"] + r["label_1"], (n, split)
        for split in ("train", "validation"):
            assert block[split]["checks"]["partition_ok"] and block[split]["checks"]["label_sum_ok"]


def test_t06_coverage_and_positive_rate_definitions(base_result):
    r = base_result["per_n"]["2"]["train"]
    assert r["coverage_ratio"] == pytest.approx(3 / 5)
    assert r["positive_rate"] == pytest.approx(1 / 3)


def test_t07_invalid_sessions_counted_as_events_not_sessions(base_result):
    assert base_result["events"]["invalid_session_events"] == 3
    total_sessions = sum(v["sessions"] for k, v in base_result["session_buckets"].items())
    assert total_sessions == 11  # 11 valid sessions; NULL/blank are not sessions


def test_t08_boundary_sessions_censored_and_excluded(base_result):
    assert base_result["boundary_qa"]["train_censored_sessions"] == 1
    assert base_result["boundary_qa"]["validation_censored_sessions"] == 1
    # T_cross carries a purchase; if it leaked into train it would add a label_1 for N=2.
    assert base_result["per_n"]["2"]["train"]["label_1"] == 1


def test_t09_test_range_sessions_counted_but_not_ranked(base_result):
    assert base_result["boundary_qa"]["test_range_sessions_not_labelled"] == 2
    assert base_result["leakage_qa"]["ranked_sessions_outside_train_validation"] == 0
    assert set(base_result["leakage_qa"]["sessions_ranked_by_bucket"]) <= {"train", "validation"}


def test_t10_no_candidate_event_at_or_after_own_split_end(base_result):
    assert base_result["leakage_qa"]["candidate_events_at_or_after_own_split_end"] == 0


def test_t11_each_session_in_exactly_one_bucket(spark):
    sessions = classify_sessions(valid_session_events(make_df(spark, build_fixture())), BOUNDS)
    ids = [r[0] for r in sessions.select("user_session").collect()]
    assert len(ids) == len(set(ids)) == 11


# --- Adversarial: test-range labels must not influence N selection ------------


def test_t12_changing_test_range_purchases_does_not_change_train_or_validation(spark, base_result):
    def flip(kinds):
        return [k if k != "purchase" else "view" for k in kinds] + ["purchase"]

    poisoned = analyze_n_candidates(
        make_df(spark, build_fixture(overrides={"X_a": flip, "X_b": lambda k: k + ["purchase", "purchase"]})),
        [2, 3, 5],
        BOUNDS,
    )
    for n in ("2", "3", "5"):
        for split in ("train", "validation", "train_plus_validation"):
            assert poisoned["per_n"][n][split] == base_result["per_n"][n][split], (n, split)


def test_t13_changing_suffix_after_n_keeps_prefix_features_b6(spark):
    """B6 prefix features for T_p1 must not depend on events after N=2 (label may change)."""
    rows_base = build_fixture()
    rows_alt = build_fixture(overrides={"T_p1": lambda k: k[:2] + ["view", "view", "purchase"]})
    feats = {}
    for name, rows in (("base", rows_base), ("alt", rows_alt)):
        ds, _, _ = build_session_features(make_df(spark, rows), 2)
        feats[name] = ds.filter("user_session = 'T_p1'").select(
            "n_view", "n_cart", "n_remove_from_cart", "n_unique_products", "avg_price", "max_price",
            "prefix_duration_seconds", "last_event_type").collect()[0].asDict()
    assert feats["base"] == feats["alt"]


# --- Consistency with accepted B6 logic ----------------------------------------


@pytest.mark.parametrize("n", [2, 3, 5])
def test_t14_matches_b6_audit_on_train_contained_sessions(spark, base_result, n):
    """Oracle: B6 build_session_features applied to the train-contained sessions only."""
    rows = [r for r in build_fixture() if r[0] in {"T_short1", "T_eq2", "T_early", "T_p1", "T_p0"}]
    _, audit, _ = build_session_features(make_df(spark, rows), n)
    counts = {r["status"]: int(r["count"]) for r in audit.groupBy("status").count().collect()}
    tr = base_result["per_n"][str(n)]["train"]
    assert counts.get("too_short", 0) == tr["too_short"]
    assert counts.get("purchased_by_n", 0) == tr["early_purchase"]
    assert counts.get("eligible", 0) == tr["eligible"]


# --- Determinism --------------------------------------------------------------


def test_t15_repeatable_on_same_input(spark, base_result):
    again = analyze_n_candidates(make_df(spark, build_fixture()), [2, 3, 5], BOUNDS)
    assert again["per_n"] == base_result["per_n"]
    assert again["boundary_qa"] == base_result["boundary_qa"]


def test_t16_out_of_order_rows_and_repartitioning_do_not_change_result(spark, base_result):
    rows = build_fixture()
    random.Random(7).shuffle(rows)
    df = make_df(spark, rows).repartition(7)
    shuffled = analyze_n_candidates(df, [2, 3, 5], BOUNDS)
    assert shuffled["per_n"] == base_result["per_n"]


def test_t17_timestamp_ties_ranked_by_type_rank_then_sort_key(spark):
    """Two events at the same second: view (rank 1) must precede purchase (rank 4) regardless of row order."""
    counter = [100]
    start = T("2019-10-02T13:00:00")
    rows_a = _session_rows("S_tie", start, ["view"], counter=counter)
    counter[0] += 1
    rows_a.append(("S_tie", start, "purchase", 4, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    rows_b = list(reversed(rows_a))
    res_a = analyze_n_candidates(make_df(spark, rows_a), [1], BOUNDS)
    res_b = analyze_n_candidates(make_df(spark, rows_b), [1], BOUNDS)
    assert res_a["per_n"] == res_b["per_n"]
    # view then purchase: purchase is at event 2 > N=1, so the session is label 1.
    assert res_a["per_n"]["1"]["train"]["label_1"] == 1


def test_t18_session_starting_exactly_on_boundaries(base_result):
    """train_start is included in train; validation_start belongs to validation (end exclusive)."""
    assert base_result["per_n"]["2"]["train"]["in_scope_sessions"] == 5
    assert base_result["per_n"]["2"]["validation"]["in_scope_sessions"] == 2


def test_t19_example_config_windows_parse_and_are_ordered():
    import yaml
    cfg = yaml.safe_load((ROOT / "configs" / "n_candidate_analysis.example.yaml").read_text(encoding="utf-8"))
    b = SplitBounds.from_iso(cfg["windows_utc"])  # validates ordering and non-overlap
    assert b.train_end == b.val_start and b.val_end == b.test_start
    assert parse_n_candidates(cfg["n_candidates"]) == [2, 3, 5]


def _load_cli():
    import importlib.util
    spec = importlib.util.spec_from_file_location("p5cli_qa", ROOT / "scripts" / "run" / "run_p5_b6_01_n_analysis.py")
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    return cli


def test_t21_qa_flags_empty_validation_and_no_positive_train(base_result):
    cli = _load_cli()
    # Fixture expectation: the only QA problem is N=5 train having no positive label
    # (the single eligible train session for N=5 is label 0). QA must report it, not hide it.
    ok, problems = cli._qa_pass(base_result)
    assert not ok
    assert problems == ["N=5 train: no positive label, cannot fit a classifier"], problems

    empty_val = {**base_result, "per_n": {k: {**v, "validation": {**v["validation"], "in_scope_sessions": 0}}
                                          for k, v in base_result["per_n"].items()}}
    ok, problems = cli._qa_pass(empty_val)
    assert not ok and any("empty split" in p for p in problems)

    no_pos = {**base_result, "per_n": {k: {**v, "train": {**v["train"], "label_1": 0}}
                                       for k, v in base_result["per_n"].items()}}
    ok, problems = cli._qa_pass(no_pos)
    assert not ok and any("no positive label" in p for p in problems)


def test_t20_cli_refuses_full_run_without_flag_and_existing_output(tmp_path, monkeypatch):
    """Both refusals happen before Spark starts, so they cost nothing and cannot start a full job."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("p5cli", ROOT / "scripts" / "run" / "run_p5_b6_01_n_analysis.py")
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)

    monkeypatch.setattr(sys, "argv", ["x", "--mode", "full", "--output-dir", str(tmp_path / "o1"), "--run-id", "t"])
    assert cli.main() == 2

    existing = tmp_path / "exists"
    existing.mkdir()
    monkeypatch.setattr(sys, "argv", ["x", "--output-dir", str(existing), "--run-id", "t"])
    assert cli.main() == 2


def _example_cfg():
    import yaml
    return yaml.safe_load((ROOT / "configs" / "n_candidate_analysis.example.yaml").read_text(encoding="utf-8"))


def test_t22_official_prediction_point_is_n3_and_not_data_driven():
    cfg = _example_cfg()
    assert cfg["selected_n"] == 3
    assert cfg["selected_n"] in cfg["n_candidates"]
    pp = cfg["prediction_point"]
    assert pp["selected_n"] == 3
    assert pp["validation_or_test_used_for_selection"] is False
    assert pp["test_labels_used_for_selection"] is False
    assert "PENDING_A_REVIEW" in pp["approval_status"]
    assert pp["features_scope"] == "first selected_n events only"


def test_t23_n3_result_identical_whether_analysed_alone_or_with_candidates(spark, base_result):
    alone = analyze_n_candidates(make_df(spark, build_fixture()), [3], BOUNDS)
    assert alone["per_n"]["3"] == base_result["per_n"]["3"]
    assert list(alone["per_n"].keys()) == ["3"]


def test_t24_acceptance_scope_uses_only_selected_n_and_rejects_invalid_configs():
    cli = _load_cli()
    cfg = _example_cfg()
    assert cli.resolve_analysis_ns(cfg, "acceptance") == [3]
    assert cli.resolve_analysis_ns(cfg, "reference") == [2, 3, 5]

    not_candidate = {**cfg, "selected_n": 4}
    with pytest.raises(ValueError):
        cli.resolve_analysis_ns(not_candidate, "acceptance")

    data_driven = {**cfg, "prediction_point": {**cfg["prediction_point"],
                                               "validation_or_test_used_for_selection": True}}
    with pytest.raises(ValueError):
        cli.resolve_analysis_ns(data_driven, "acceptance")


def test_t25_cli_has_no_n_override_and_defaults_to_acceptance():
    cli = _load_cli()
    args = cli.parse_args(["--output-dir", "x", "--run-id", "r"])
    assert args.analysis_scope == "acceptance"
    with pytest.raises(SystemExit):
        cli.parse_args(["--output-dir", "x", "--run-id", "r", "--n", "2"])


def _extra_rows():
    """Edge sessions added on top of the base fixture (see test_t26..t30)."""
    counter = [500]
    rows = []
    # Mixed: one event with NULL event_time inside an otherwise valid session.
    counter[0] += 1
    rows.append(("Z_null_mixed", T("2019-10-02T15:00:00"), "view", 1, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    counter[0] += 1
    rows.append(("Z_null_mixed", None, "cart", 2, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    # All events NULL event_time.
    for k in range(2):
        counter[0] += 1
        rows.append(("Z_null_all", None, "view", 1, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    # Starts exactly at test_end (2019-10-10T00:00): outside the analysis.
    counter[0] += 1
    rows.append(("Z_after_exact", T("2019-10-10T00:00:00"), "view", 1, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    # Starts one second before test_end: still inside test_range.
    counter[0] += 1
    rows.append(("Z_test_last", T("2019-10-09T23:59:59"), "view", 1, "{:064x}".format(counter[0]), "P0", "C0", "A", 10.0))
    return rows


def test_t26_null_event_time_sessions_excluded_explicitly_not_ranked(spark, base_result):
    res = analyze_n_candidates(make_df(spark, build_fixture() + _extra_rows()), [2, 3, 5], BOUNDS)
    assert res["session_buckets"]["excluded_null_event_time"]["sessions"] == 2
    assert res["events"]["null_event_time_events_in_valid_sessions"] == 3
    # Excluded sessions never enter the analysis, so the N=3 table is unchanged.
    assert res["per_n"]["3"] == base_result["per_n"]["3"]
    cli = _load_cli()
    warnings = cli._qa_warnings(res)
    assert any("NULL event_time" in w for w in warnings)


def test_t27_session_start_at_or_after_test_end_is_out_of_scope(spark):
    res = analyze_n_candidates(make_df(spark, build_fixture() + _extra_rows()), [3], BOUNDS)
    b = res["session_buckets"]
    assert b["after_test_end"]["sessions"] == 1          # starts exactly at test_end
    assert b["test_range"]["sessions"] == 3              # X_a, X_b, and Z_test_last (1s before end)
    assert res["boundary_qa"]["after_test_end_sessions"] == 1


def test_t28_unexplained_gap_is_reported_and_fails_qa(spark):
    """Windows with a hole (train_end < validation_start) create real gap sessions; QA must fail."""
    holed = SplitBounds.from_iso({
        "train_start": "2019-10-01T00:00:00Z",
        "train_end_exclusive": "2019-10-04T00:00:00Z",
        "validation_start": "2019-10-05T00:00:00Z",
        "validation_end_exclusive": "2019-10-08T00:00:00Z",
        "test_start": "2019-10-08T00:00:00Z",
        "test_end_exclusive": "2019-10-10T00:00:00Z",
    })
    res = analyze_n_candidates(make_df(spark, build_fixture()), [3], holed)
    assert res["session_buckets"]["gap"]["sessions"] > 0
    cli = _load_cli()
    ok, problems = cli._qa_pass(res)
    assert not ok and any("unexplained" in p for p in problems)


def test_t29_diagnostic_partitions_every_valid_session(spark):
    from src.ml.dataset.n_selection import diagnose_session_buckets
    diag = diagnose_session_buckets(make_df(spark, build_fixture() + _extra_rows()), BOUNDS)
    total = sum(v["sessions"] for v in diag["session_buckets"].values())
    assert total == 15  # 11 base + 4 edge sessions; NULL/blank invalid rows are not sessions
    assert diag["unexplained_gap_sessions"] == 0
    assert diag["null_event_time_events_in_valid_sessions"] == 3
    # Z_null_all has every event_time NULL, so its session_start is NULL; it is excluded as
    # excluded_null_event_time before any window comparison runs.
    assert diag["sessions_with_null_start"] == 1
    assert diag["after_test_end_events"] == 1


def test_t30_all_null_time_session_has_no_start_and_is_excluded(spark):
    from src.ml.dataset.n_selection import classify_sessions
    sessions = classify_sessions(valid_session_events(make_df(spark, _extra_rows())), BOUNDS)
    row = sessions.filter("user_session = 'Z_null_all'").collect()[0]
    assert row["session_start"] is None and row["bucket"] == "excluded_null_event_time"


# --- Timezone independence (UTC boundaries regardless of OS / Python / JVM zone) ----------------

# (session, UTC event times, expected bucket) around the official H11 boundaries.
BOUNDARY_CASES = [
    ("B_train_1112_2000", ["2019-11-12 20:00:00"], "train"),
    ("B_train_last_second", ["2019-11-12 23:59:59"], "train"),
    ("B_cross_train_val", ["2019-11-12 23:59:58", "2019-11-13 00:00:01"], "censored_train"),
    ("B_val_first_second", ["2019-11-13 00:00:00"], "validation"),
    ("B_val_last_second", ["2019-11-21 23:59:59"], "validation"),
    ("B_cross_val_test", ["2019-11-21 23:59:58", "2019-11-22 00:00:05"], "censored_validation"),
    ("B_test_first_second", ["2019-11-22 00:00:00"], "test_range"),
    ("B_test_1130_2000", ["2019-11-30 20:00:00"], "test_range"),
    ("B_test_last_second", ["2019-11-30 23:59:59"], "test_range"),
    ("B_after_test_end", ["2019-12-01 00:00:00"], "after_test_end"),
]


def _h11_bounds():
    return SplitBounds.from_iso(_example_cfg()["windows_utc"])


def _boundary_rows():
    rows, k = [], 900
    for sid, times, _ in BOUNDARY_CASES:
        for t in times:
            k += 1
            rows.append((sid, T(t.replace(" ", "T")), "view", 1, "{:064x}".format(k), "P0", "C0", "A", 10.0))
    return rows


def test_t31_utc_literal_is_independent_of_python_local_zone(spark):
    from datetime import timezone as tz, timedelta as td
    from src.ml.dataset.n_selection import utc_ts_literal
    naive = spark.range(1).select(utc_ts_literal(datetime(2019, 12, 1)).cast("long")).first()[0]
    aware_kst = spark.range(1).select(
        utc_ts_literal(datetime(2019, 12, 1, 9, 0, 0, tzinfo=tz(td(hours=9)))).cast("long")).first()[0]
    assert naive == 1575158400      # 2019-12-01T00:00:00Z
    assert aware_kst == 1575158400  # 09:00 KST is the same instant


def test_t32_h11_boundaries_assign_utc_buckets(spark):
    sessions = classify_sessions(valid_session_events(make_df(spark, _boundary_rows())), _h11_bounds())
    got = {r[0]: r[1] for r in sessions.select("user_session", "bucket").collect()}
    expected = {sid: bucket for sid, _, bucket in BOUNDARY_CASES}
    assert got == expected


def test_t33_non_utc_session_time_zone_is_rejected(spark):
    old = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
        with pytest.raises(ValueError, match="UTC"):
            analyze_n_candidates(make_df(spark, build_fixture()), [3], BOUNDS)
    finally:
        spark.conf.set("spark.sql.session.timeZone", old)


@pytest.mark.skipif(sys.platform.startswith("win"),
                    reason="TZ environment variable does not change the Python local zone on Windows; "
                           "this test must run on Linux (VM B)")
def test_t34_same_buckets_when_os_zone_is_kst(tmp_path):
    """Run the classification in a child process with TZ=Asia/Seoul; buckets must not change."""
    import json
    import os
    import subprocess
    script = tmp_path / "kst_probe.py"
    script.write_text(
        "import json, sys, datetime\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from pyspark.sql import functions as F\n"
        "from src.common.spark_session import get_spark\n"
        "from src.ml.dataset.n_selection import SplitBounds, classify_sessions, valid_session_events\n"
        "offset = datetime.datetime.now().astimezone().utcoffset().total_seconds() / 3600\n"
        "spark = get_spark('KSTProbe')\n"
        "rows = json.loads(sys.argv[1]); bounds = SplitBounds.from_iso(json.loads(sys.argv[2]))\n"
        "df = spark.createDataFrame(rows, 'user_session string, ts string').select(\n"
        "    'user_session', F.to_timestamp('ts', 'yyyy-MM-dd HH:mm:ss').alias('event_time'))\n"
        "got = {r[0]: r[1] for r in classify_sessions(valid_session_events(df), bounds)"
        ".select('user_session', 'bucket').collect()}\n"
        "print('RESULT=' + json.dumps({'offset_hours': offset, 'buckets': got}))\n"
        "spark.stop()\n",
        encoding="utf-8",
    )
    rows = [[sid, t] for sid, times, _ in BOUNDARY_CASES for t in times]
    env = dict(os.environ, TZ="Asia/Seoul")
    proc = subprocess.run(
        [sys.executable, str(script), json.dumps(rows), json.dumps(_example_cfg()["windows_utc"])],
        env=env, capture_output=True, text=True, timeout=600,
    )
    line = [ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT=")]
    assert proc.returncode == 0 and line, proc.stderr[-2000:]
    out = json.loads(line[-1][len("RESULT="):])
    if out["offset_hours"] != 9:
        pytest.skip(f"Asia/Seoul zone data not available in this OS (offset={out['offset_hours']})")
    assert out["buckets"] == {sid: bucket for sid, _, bucket in BOUNDARY_CASES}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
