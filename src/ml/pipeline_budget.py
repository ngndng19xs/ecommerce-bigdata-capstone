"""Time budget and forecast for the P5 ML pipeline (pure Python, no Spark).

The pipeline has a global budget (default 180 minutes). After a few batches or groups the
forecast says whether the remaining work still fits. If not, the answer is TIME_BUDGET_NO_GO and
the caller stops BEFORE starting more heavy work; checkpoints stay valid.

Nothing here promises a time. It only compares a measured throughput with the remaining budget.
"""

import json
import os
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

DEFAULT_BUDGET_SECONDS = 10800  # 180 minutes
# Planning shares of the global budget (seconds). They are targets, not guarantees.
STAGE_BUDGET_SECONDS = {
    "snapshot": 45 * 60,
    "b6": 50 * 60,
    "b7": 30 * 60,
    "b8": 15 * 60,
    "b9": 15 * 60,
    "reserve": 25 * 60,
}
STAGE_ORDER = ("snapshot", "b6", "b7", "b8", "b9")


def forecast_remaining_seconds(done_seconds: Sequence[float], remaining_units: int,
                               recent: int = 5) -> Optional[float]:
    """Forecast = mean of the most recent unit durations x remaining units.

    Uses the last `recent` durations, so a slow start-up batch does not dominate. Returns None
    when nothing has been measured yet (no forecast is better than a guess).
    """
    if remaining_units <= 0:
        return 0.0
    if not done_seconds:
        return None
    window = list(done_seconds)[-recent:]
    return (sum(window) / len(window)) * remaining_units


def reserved_after(stage: str, stage_budgets: Optional[Dict[str, int]] = None) -> float:
    """Seconds reserved for the stages that come after `stage`, plus the safety reserve."""
    budgets = stage_budgets or STAGE_BUDGET_SECONDS
    idx = STAGE_ORDER.index(stage)
    later = sum(budgets[s] for s in STAGE_ORDER[idx + 1:])
    return float(later + budgets["reserve"])


def budget_decision(elapsed: float, forecast_remaining: Optional[float], reserved: float,
                    budget: float = DEFAULT_BUDGET_SECONDS) -> Dict[str, Any]:
    """GO while elapsed + forecast + reserved fits in the budget; NO_GO otherwise.

    With no forecast yet the decision is GO but flagged, so the first units can be measured.
    """
    if forecast_remaining is None:
        return {"decision": "GO", "forecast_available": False, "reason": "no unit measured yet",
                "elapsed": elapsed, "budget": budget}
    needed = elapsed + forecast_remaining + reserved
    ok = needed <= budget
    return {
        "decision": "GO" if ok else "TIME_BUDGET_NO_GO",
        "forecast_available": True,
        "elapsed": round(elapsed, 1),
        "forecast_remaining": round(forecast_remaining, 1),
        "reserved": round(reserved, 1),
        "needed_total": round(needed, 1),
        "budget": budget,
        "reason": ("fits the budget" if ok else
                   f"needs {needed:.0f}s but the budget is {budget:.0f}s"),
    }


class RunClock:
    """Persistent wall clock of one RUN_ID, so a resume keeps counting the same budget.

    Time that passes while the run is stopped (waiting for a human) is NOT counted: only the
    seconds of active sessions are added. `started_active()` and `stop_active()` bracket a session.
    """

    def __init__(self, path: str, budget: float = DEFAULT_BUDGET_SECONDS,
                 now: Callable[[], float] = time.time):
        self.path = path
        self.budget = budget
        self._now = now
        self._session_start: Optional[float] = None
        self.state = {"budget_seconds": budget, "active_seconds": 0.0, "stage_seconds": {}}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                self.state.update(json.load(f))
            self.state["budget_seconds"] = budget

    def started_active(self) -> None:
        self._session_start = self._now()

    def stop_active(self) -> None:
        if self._session_start is not None:
            self.state["active_seconds"] = self.elapsed()
            self._session_start = None
            self.save()

    def elapsed(self) -> float:
        extra = (self._now() - self._session_start) if self._session_start is not None else 0.0
        return float(self.state["active_seconds"]) + extra

    def add_stage_seconds(self, stage: str, seconds: float) -> None:
        self.state["stage_seconds"][stage] = float(self.state["stage_seconds"].get(stage, 0.0)) + seconds
        self.save()

    def remaining(self) -> float:
        return self.budget - self.elapsed()

    def save(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)
