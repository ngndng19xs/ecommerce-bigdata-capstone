"""P5 ML pipeline: time budget and forecast (pure Python, no Spark)."""

import json
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.ml import pipeline_budget as pb  # noqa: E402


def test_forecast_with_no_measurements_is_none():
    assert pb.forecast_remaining_seconds([], 5) is None


def test_forecast_with_empty_measurements_is_zero():
    assert pb.forecast_remaining_seconds([1, 2, 3], 0) == 0.0


def test_forecast_uses_recent_window():
    durations = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    remaining = 3
    forecast = pb.forecast_remaining_seconds(durations, remaining, recent=3)
    mean_of_last_3 = (40.0 + 50.0 + 60.0) / 3
    assert forecast == pytest.approx(mean_of_last_3 * 3)


def test_reserved_after_snapshot():
    reserved = pb.reserved_after("snapshot")
    assert reserved == sum([
        pb.STAGE_BUDGET_SECONDS["b6"],
        pb.STAGE_BUDGET_SECONDS["b7"],
        pb.STAGE_BUDGET_SECONDS["b8"],
        pb.STAGE_BUDGET_SECONDS["b9"],
        pb.STAGE_BUDGET_SECONDS["reserve"],
    ])


def test_reserved_after_last_stage_is_just_reserve():
    reserved = pb.reserved_after("b9")
    assert reserved == pb.STAGE_BUDGET_SECONDS["reserve"]


def test_budget_decision_no_forecast_is_go_but_flagged():
    decision = pb.budget_decision(100, None, 200, 10800)
    assert decision["decision"] == "GO"
    assert decision["forecast_available"] is False


def test_budget_decision_fits_is_go():
    decision = pb.budget_decision(100, 200, 300, 10800)
    assert decision["decision"] == "GO"
    assert decision["forecast_available"] is True
    assert decision["needed_total"] == 600


def test_budget_decision_exceeds_budget_is_no_go():
    decision = pb.budget_decision(5000, 4000, 2000, 10800)
    assert decision["decision"] == "TIME_BUDGET_NO_GO"
    assert decision["needed_total"] == 11000


def test_run_clock_starts_with_zero_elapsed():
    with tempfile.TemporaryDirectory() as tmp:
        clock = pb.RunClock(os.path.join(tmp, "clock.json"), now=lambda: 100)
        assert clock.elapsed() == 0.0
        assert clock.remaining() == 10800.0


def test_run_clock_tracks_active_session():
    now_val = [100]

    def mock_now():
        return now_val[0]

    with tempfile.TemporaryDirectory() as tmp:
        clock = pb.RunClock(os.path.join(tmp, "clock.json"), now=mock_now)
        clock.started_active()
        now_val[0] = 150
        assert clock.elapsed() == pytest.approx(50.0)
        clock.stop_active()
        assert clock.elapsed() == pytest.approx(50.0)
        now_val[0] = 200
        assert clock.elapsed() == pytest.approx(50.0)  # time while stopped is not counted


def test_run_clock_persists_and_resumes():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "clock.json")
        clock1 = pb.RunClock(path, now=lambda: 0)
        clock1.started_active()
        clock1.state["active_seconds"] = 100
        clock1.save()

        clock2 = pb.RunClock(path, now=lambda: 0)
        assert clock2.elapsed() == 100.0


def test_run_clock_saves_stage_seconds():
    with tempfile.TemporaryDirectory() as tmp:
        clock = pb.RunClock(os.path.join(tmp, "clock.json"))
        clock.add_stage_seconds("snapshot", 45 * 60)
        assert clock.state["stage_seconds"]["snapshot"] == 45 * 60
        with open(os.path.join(tmp, "clock.json"), "r", encoding="utf-8") as f:
            saved = json.load(f)
        assert saved["stage_seconds"]["snapshot"] == 45 * 60


def test_run_clock_accumulates_stage_seconds():
    with tempfile.TemporaryDirectory() as tmp:
        clock = pb.RunClock(os.path.join(tmp, "clock.json"))
        clock.add_stage_seconds("b6", 100)
        clock.add_stage_seconds("b6", 50)
        assert clock.state["stage_seconds"]["b6"] == 150


def test_run_clock_budget_is_override_on_creation():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "clock.json")
        clock1 = pb.RunClock(path, budget=5000)
        clock1.save()

        clock2 = pb.RunClock(path, budget=3000)
        assert clock2.budget == 3000
        assert clock2.remaining() <= 3000


def test_budget_decision_table_for_typical_run():
    """E2E-like: 4 batches of snapshot, none committed yet."""
    stages = [("snapshot", 4, [100, 120, 110, 130])]  # recent mean ~120, 4 batches done, 0 remaining
    elapsed = sum([120, 110, 130]) / 3 * 4  # forecast for 4 more... oops, let me recalculate
    elapsed = 100 + 120 + 110 + 130  # all 4 done
    remaining = 0  # no more batches
    forecast = pb.forecast_remaining_seconds([100, 120, 110, 130], 0)
    reserved = pb.reserved_after("snapshot")
    decision = pb.budget_decision(elapsed, forecast, reserved, 10800)
    assert decision["decision"] == "GO"
    assert decision["needed_total"] <= 10800
