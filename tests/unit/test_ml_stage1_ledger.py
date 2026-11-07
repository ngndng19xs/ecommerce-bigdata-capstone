"""P5 ML stage 1 ledger: verdict priorities, checkpoints, resume, timeouts and GO. Runs on Windows too."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "run"))

import p5_stage1_ledger as L  # noqa: E402

CONFIG = str(ROOT / "configs" / "ml_stage1.example.yaml")


# --- verdict ------------------------------------------------------------------------------------


@pytest.mark.parametrize("statuses, mandatory, go, expected", [
    ({"A": "PASS", "B": "PASS"}, ["A", "B"], False, "WAITING_GO"),
    ({"A": "PASS", "B": "PASS"}, ["A", "B"], True, "PASS"),
    ({"A": "PASS", "B": "FAIL"}, ["A", "B"], False, "FAIL"),
    ({"A": "PASS", "B": "BLOCKED"}, ["A", "B", "C"], False, "BLOCKED"),
    ({"A": "KILLED_LOW_MEMORY", "B": "NOT_RUN"}, ["A", "B"], False, "INCOMPLETE"),
    ({"A": "PASS"}, ["A", "B"], False, "INCOMPLETE"),
    ({"A": "PASS", "B": "WAITING_DECISION", "C": "NOT_RUN"}, ["A", "B", "C"], False, "WAITING_DECISION"),
    ({"A": "TIMEOUT"}, ["A"], False, "INCOMPLETE"),
    ({"A": "INTERRUPTED"}, ["A"], False, "INCOMPLETE"),
    ({"A": "PASS", "B": "SKIPPED"}, ["A", "B"], False, "INCOMPLETE"),
    ({"A": "KILLED_SWAP", "B": "FAIL"}, ["A", "B"], False, "FAIL"),
    ({"A": "WAITING_DECISION", "B": "KILLED_DISK"}, ["A", "B"], False, "INCOMPLETE"),
])
def test_overall_priorities(statuses, mandatory, go, expected):
    assert L.overall(statuses, mandatory, go_granted=go) == expected


def test_exit_codes_and_rc_mapping():
    assert L.EXIT_BY_OVERALL == {"PASS": 0, "FAIL": 1, "BLOCKED": 2, "INCOMPLETE": 3,
                                 "WAITING_DECISION": 4, "WAITING_GO": 4}
    assert L.status_from_rc(0) == "PASS"
    assert L.status_from_rc(2) == "BLOCKED"
    assert L.status_from_rc(5) == "WAITING_DECISION"
    assert L.status_from_rc(1) == "FAIL"
    assert L.status_from_rc(9) == "FAIL"


# --- checkpoints and resume ---------------------------------------------------------------------


def _ckpt(**over):
    base = {"status": "PASS", "input_sha": "in1", "code_sha": "code1"}
    base.update(over)
    return base


def test_resume_skips_only_when_everything_matches():
    skip, why = L.should_skip(_ckpt(), True, "in1", "code1")
    assert skip is True and "unchanged" in why
    assert L.should_skip(None, True, "in1", "code1")[0] is False
    assert L.should_skip(_ckpt(status="KILLED_SWAP"), True, "in1", "code1")[0] is False
    assert L.should_skip(_ckpt(), False, "in1", "code1")[0] is False  # marker missing
    assert L.should_skip(_ckpt(), True, "in2", "code1")[0] is False  # input changed
    assert L.should_skip(_ckpt(), True, "in1", "code2")[0] is False  # code changed


def test_mark_incomplete_renames_and_never_deletes(tmp_path):
    out = tmp_path / "features"
    out.mkdir()
    (out / "part.parquet").write_text("partial", encoding="utf-8")
    first = L.mark_incomplete(str(out))
    assert first.endswith("features.INCOMPLETE-1") and not out.exists()
    assert (Path(first) / "part.parquet").read_text(encoding="utf-8") == "partial"
    out.mkdir()
    second = L.mark_incomplete(str(out))
    assert second.endswith("features.INCOMPLETE-2")
    assert L.mark_incomplete(str(tmp_path / "missing")) is None


def test_code_sha_follows_file_content(tmp_path):
    (tmp_path / "a.py").write_text("x = 1\n", encoding="utf-8")
    before = L.code_sha(["a.py", "b.py"], root=str(tmp_path))
    assert L.code_sha(["a.py", "b.py"], root=str(tmp_path)) == before
    (tmp_path / "a.py").write_text("x = 2\n", encoding="utf-8")
    assert L.code_sha(["a.py", "b.py"], root=str(tmp_path)) != before


def test_go_check_needs_three_lines(tmp_path):
    run_dir = tmp_path / "run"
    assert L.go_check(str(run_dir), "stage1_x")[0] is False
    (run_dir / "WAITING_GO").mkdir(parents=True)
    go = run_dir / "WAITING_GO" / "GO_stage1_x.txt"
    go.write_text("member B\n2026-11-08T09:00:00Z\n", encoding="utf-8")
    assert L.go_check(str(run_dir), "stage1_x")[0] is False
    go.write_text("member B\n2026-11-08T09:00:00Z\nreviewed pilot numbers\n", encoding="utf-8")
    assert L.go_check(str(run_dir), "stage1_x")[0] is True


# --- run-step on any OS (no process groups needed) ----------------------------------------------


def _run(tmp_path, name, cmd, extra=()):
    rep = tmp_path / "report"
    logs = tmp_path / "logs"
    argv = ["run-step", "--name", name, "--report-dir", str(rep), "--log-dir", str(logs),
            "--config", CONFIG, "--timeout", "60", "--input-sha", "in1", *extra, "--"] + cmd
    return L.main(argv), rep


def _py(code):
    return [sys.executable, "-c", code]


def test_run_step_pass_writes_step_and_checkpoint(tmp_path):
    marker = tmp_path / "out.txt"
    code, rep = _run(tmp_path, "T_pass", _py(f"open(r'{marker}', 'w').write('ok')"),
                     ["--marker", str(marker)])
    assert code == 0
    step = json.loads((rep / "steps" / "T_pass.json").read_text(encoding="utf-8"))
    assert step["status"] == "PASS"
    assert (rep / "checkpoints" / "T_pass.json").exists()


def test_run_step_reuses_a_valid_checkpoint_on_resume(tmp_path):
    marker = tmp_path / "out.txt"
    cmd = _py(f"open(r'{marker}', 'w').write('ok')")
    _run(tmp_path, "T_resume", cmd, ["--marker", str(marker)])
    code, rep = _run(tmp_path, "T_resume", cmd, ["--marker", str(marker), "--resume"])
    assert code == 0
    step = json.loads((rep / "steps" / "T_resume.json").read_text(encoding="utf-8"))
    assert step["status"] == "PASS" and step["reused"] is True


def test_run_step_reruns_when_the_input_changed(tmp_path):
    marker = tmp_path / "out.txt"
    cmd = _py(f"open(r'{marker}', 'w').write('ok')")
    _run(tmp_path, "T_changed", cmd, ["--marker", str(marker)])
    rep = tmp_path / "report"
    argv = ["run-step", "--name", "T_changed", "--report-dir", str(rep), "--log-dir", str(tmp_path / "logs"),
            "--config", CONFIG, "--timeout", "60", "--input-sha", "in2", "--marker", str(marker), "--resume", "--"] + cmd
    assert L.main(argv) == 0
    step = json.loads((rep / "steps" / "T_changed.json").read_text(encoding="utf-8"))
    assert step["status"] == "PASS" and not step.get("reused")


@pytest.mark.parametrize("code_value, expected_rc, expected_status", [
    (1, 1, "FAIL"),
    (2, 2, "BLOCKED"),
    (5, 5, "WAITING_DECISION"),
])
def test_run_step_maps_exit_codes(tmp_path, code_value, expected_rc, expected_status):
    rc, rep = _run(tmp_path, "T_rc", _py(f"import sys; sys.exit({code_value})"))
    assert rc == expected_rc
    assert json.loads((rep / "steps" / "T_rc.json").read_text(encoding="utf-8"))["status"] == expected_status
    assert not (rep / "checkpoints" / "T_rc.json").exists()


def test_run_step_timeout_is_incomplete(tmp_path):
    rc, rep = _run(tmp_path, "T_timeout", _py("import time; time.sleep(60)"), ["--timeout", "1"])
    assert rc == 3
    assert json.loads((rep / "steps" / "T_timeout.json").read_text(encoding="utf-8"))["status"] == "TIMEOUT"


def test_run_step_refuses_an_existing_output(tmp_path):
    out = tmp_path / "already"
    out.mkdir()
    rc, _ = _run(tmp_path, "T_exists", _py("pass"), ["--output-path", str(out)])
    assert rc == 2


def test_run_step_needs_a_command(tmp_path):
    rep = tmp_path / "report"
    argv = ["run-step", "--name", "T_empty", "--report-dir", str(rep), "--log-dir", str(tmp_path / "logs"),
            "--config", CONFIG, "--"]
    assert L.main(argv) == 2


# --- summarize ----------------------------------------------------------------------------------


def test_summarize_is_waiting_go_when_all_mandatory_pass(tmp_path):
    rep = tmp_path / "report"
    (rep / "steps").mkdir(parents=True)
    for name in ("A", "B"):
        (rep / "steps" / f"{name}.json").write_text(json.dumps({"name": name, "status": "PASS"}), encoding="utf-8")
    code = L.main(["summarize", "--report-dir", str(rep), "--run-id", "stage1_t", "--mandatory", "A B"])
    assert code == 4
    status = json.loads((rep / "run_status.json").read_text(encoding="utf-8"))
    assert status["overall"] == "WAITING_GO" and status["mandatory_status"] == {"A": "PASS", "B": "PASS"}


def test_summarize_reports_a_missing_mandatory_step_as_incomplete(tmp_path):
    rep = tmp_path / "report"
    (rep / "steps").mkdir(parents=True)
    (rep / "steps" / "A.json").write_text(json.dumps({"name": "A", "status": "PASS"}), encoding="utf-8")
    code = L.main(["summarize", "--report-dir", str(rep), "--run-id", "stage1_t", "--mandatory", "A B"])
    assert code == 3


def test_summarize_is_pass_only_with_go(tmp_path):
    rep = tmp_path / "report"
    (rep / "steps").mkdir(parents=True)
    (rep / "steps" / "A.json").write_text(json.dumps({"name": "A", "status": "PASS"}), encoding="utf-8")
    go_dir = tmp_path / "gorun"
    (go_dir / "WAITING_GO").mkdir(parents=True)
    (go_dir / "WAITING_GO" / "GO_stage1_t.txt").write_text("B\n2026-11-08\nreason\n", encoding="utf-8")
    code = L.main(["summarize", "--report-dir", str(rep), "--run-id", "stage1_t", "--mandatory", "A",
                   "--go-check", "--run-dir-go", str(go_dir)])
    assert code == 0


def test_read_once_check_accepts_one_pass(tmp_path):
    metrics = tmp_path / "m.json"
    metrics.write_text(json.dumps({"input_mb_read": 1000.0}), encoding="utf-8")
    out = tmp_path / "o.json"
    assert L.main(["read-once-check", "--eventlog-metrics", str(metrics), "--source-bytes",
                   str(1000 * 1024 * 1024), "--out", str(out)]) == 0
    assert L.main(["read-once-check", "--eventlog-metrics", str(metrics), "--source-bytes",
                   str(300 * 1024 * 1024), "--out", str(out)]) == 1  # ratio 3.3: read more than one pass
