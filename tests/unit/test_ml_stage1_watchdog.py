"""P5 ML stage 1 watchdog: decision rules on synthetic samples, /proc parsing and the stop path.

Process-group signalling is POSIX only. It is covered on the VM by the watchdog self-test (step S3).
"""

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "run"))

import p5_watchdog as w  # noqa: E402

TH = w.Thresholds()


def S(t, mem=1500.0, pages=0, disk=100.0):
    return w.Sample(t=float(t), mem_mb=float(mem), swap_pages=int(pages), disk_gb=float(disk))


def test_emergency_needs_five_seconds_below_300():
    d = w.decide([S(0, 250), S(5, 240)], TH)
    assert d.kind == "STOP" and d.status == "KILLED_LOW_MEMORY_EMERGENCY"


def test_one_low_sample_is_not_a_stop():
    d = w.decide([S(0, 900), S(5, 250)], TH)
    assert d is not None and d.kind == "WARN"  # warning only: the window is not fully below the limit


def test_stop_after_thirty_seconds_below_500():
    samples = [S(t, mem=450) for t in range(0, 35, 5)]  # 0..30 s
    d = w.decide(samples, TH)
    assert d.kind == "STOP" and d.status == "KILLED_LOW_MEMORY"


def test_no_stop_when_the_low_period_is_shorter_than_the_window():
    samples = [S(t, mem=450) for t in range(0, 25, 5)]  # 0..20 s, the window needs 30 s of coverage
    d = w.decide(samples, TH)
    assert d is None or d.kind == "WARN"


def test_swap_rate_above_limit_stops_the_job():
    # 15000 pages in 30 s with 4 KiB pages = 2000 KiB/s, above 1024 KiB/s
    samples = [S(t, mem=1500, pages=int(t / 30 * 15000)) for t in range(0, 35, 5)]
    d = w.decide(samples, TH)
    assert d.kind == "STOP" and d.status == "KILLED_SWAP"


def test_swap_rate_below_limit_does_not_stop():
    samples = [S(t, mem=1500, pages=int(t / 30 * 3000)) for t in range(0, 35, 5)]  # 400 KiB/s
    assert w.decide(samples, TH) is None


def test_swap_rate_uses_the_difference_not_the_boot_average():
    # a large counter value from before the run must not trigger a stop by itself
    samples = [S(t, mem=1500, pages=10_000_000) for t in range(0, 35, 5)]
    assert w.decide(samples, TH) is None


def test_disk_stop_after_two_samples_below_limit():
    d = w.decide([S(0, disk=50), S(5, disk=9), S(10, disk=9)], TH)
    assert d.kind == "STOP" and d.status == "KILLED_DISK"


def test_warning_between_800_and_the_stop_levels():
    d = w.decide([S(0, mem=700)], TH)
    assert d.kind == "WARN"


def test_parse_meminfo_available():
    text = "MemTotal:        4808624 kB\nMemFree:          649000 kB\nMemAvailable:    1048576 kB\n"
    assert w.parse_meminfo_available_mb(text) == 1024.0


def test_parse_vmstat_swap_pages_sums_in_and_out():
    assert w.parse_vmstat_swap_pages("nr_free 1\npswpin 10\npswpout 20\nother 5\n") == 30


def test_parse_vmstat_missing_counters_fails():
    with pytest.raises(ValueError):
        w.parse_vmstat_swap_pages("nr_free 1\n")


def test_read_sample_from_fake_proc_files(tmp_path):
    meminfo = tmp_path / "meminfo"
    vmstat = tmp_path / "vmstat"
    meminfo.write_text("MemAvailable: 2048 kB\n", encoding="utf-8")
    vmstat.write_text("pswpin 3\npswpout 4\n", encoding="utf-8")
    th = w.Thresholds(disk_path=str(tmp_path))  # /var/tmp does not exist on Windows
    s = w.read_sample(th, meminfo=str(meminfo), vmstat=str(vmstat), clock=lambda: 42.0)
    assert (s.t, s.swap_pages) == (42.0, 7)
    assert s.mem_mb == 2.0  # 2048 kB = 2 MB
    assert s.disk_gb > 0


def test_stop_group_refuses_the_watchdogs_own_group():
    if os.name == "nt":
        pytest.skip("process groups are POSIX only; covered by the VM self-test")
    with pytest.raises(RuntimeError):
        w.stop_group(os.getpgrp(), grace=0)


def test_watch_writes_a_stop_status_and_stops_the_group(tmp_path, monkeypatch):
    calls = []

    def fake_stop(pgid, grace, alive=None, sleep=None):  # noqa: ARG001 - same call shape as the real one
        calls.append(pgid)
        return True

    monkeypatch.setattr(w, "stop_group", fake_stop)
    clock = {"t": 0.0}

    def sampler():
        s = S(clock["t"], mem=450)
        clock["t"] += 5.0
        return s

    status_file = tmp_path / "watch.json"
    result = w.watch(12345, str(status_file), TH, page_kib=4.0, sampler=sampler,
                     alive=lambda pgid: True, sleep=lambda s: None, clock=lambda: clock["t"],
                     log=lambda msg: None)
    assert result["status"] == "KILLED_LOW_MEMORY"
    assert calls == [12345]
    saved = json.loads(status_file.read_text(encoding="utf-8"))
    assert saved["status"] == "KILLED_LOW_MEMORY" and saved["group_ended"] is True


def test_watch_reports_job_ended_without_stopping(tmp_path):
    status_file = tmp_path / "watch.json"
    result = w.watch(999, str(status_file), TH, sampler=lambda: S(0), alive=lambda pgid: False,
                     sleep=lambda s: None, clock=lambda: 0.0, log=lambda msg: None)
    assert result["status"] == "JOB_ENDED"
    assert json.loads(status_file.read_text(encoding="utf-8"))["status"] == "JOB_ENDED"


def test_thresholds_match_the_runbook_defaults():
    assert (TH.warn_mem_mb, TH.emergency_mem_mb, TH.emergency_seconds) == (800.0, 300.0, 5.0)
    assert (TH.stop_mem_mb, TH.stop_mem_seconds, TH.swap_kib_s, TH.swap_seconds, TH.stop_disk_gb) == \
        (500.0, 30.0, 1024.0, 30.0, 10.0)
