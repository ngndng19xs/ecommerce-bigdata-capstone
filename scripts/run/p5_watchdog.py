"""P5 ML stage 1 watchdog: stops ONE process group (the Spark job) when the VM is in danger.

It never restarts anything and never touches other processes (no killall, no pkill, no HDFS daemons).
It runs outside the process group it watches (the job is started with start_new_session=True).

Samples every `interval` seconds:
  - MemAvailable (MB) from /proc/meminfo
  - swap pages swapped in + out from /proc/vmstat (pswpin, pswpout). Rate is computed from the
    difference between two samples, never from the average-since-boot line of vmstat.
  - free space of the disk holding /var/tmp (GB)

Rules (defaults = starting values from the stage 1 runbook, see configs/ml_stage1.example.yaml):
  WARN                        MemAvailable < warn_mem_mb (log only, at most once a minute)
  KILLED_LOW_MEMORY_EMERGENCY MemAvailable < emergency_mem_mb for emergency_seconds
  KILLED_LOW_MEMORY           MemAvailable < stop_mem_mb for stop_mem_seconds
  KILLED_SWAP                 swap-in + swap-out average > swap_kib_s over swap_seconds
  KILLED_DISK                 free disk < stop_disk_gb in disk_consecutive samples in a row

"Sustained T seconds": the window of the last T seconds is fully covered by samples (the oldest
sample of the window is at most one interval after its start) and every sample of the window
satisfies the condition.

On STOP: SIGTERM to the process group, wait kill_grace seconds, SIGKILL if the group still exists.
Status is written to --status-file as JSON, then the watchdog exits with 0.

Usage:
  python scripts/run/p5_watchdog.py --pgid 12345 --status-file WORK/.../job_watch.json
"""

import argparse
import json
import os
import shutil
import signal
import sys
import time
from dataclasses import asdict, dataclass
from typing import Callable, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Thresholds:
    warn_mem_mb: float = 800.0
    emergency_mem_mb: float = 300.0
    emergency_seconds: float = 5.0
    stop_mem_mb: float = 500.0
    stop_mem_seconds: float = 30.0
    swap_kib_s: float = 1024.0
    swap_seconds: float = 30.0
    stop_disk_gb: float = 10.0
    disk_consecutive: int = 2
    interval: float = 5.0
    kill_grace: float = 20.0
    disk_path: str = "/var/tmp"


@dataclass(frozen=True)
class Sample:
    t: float
    mem_mb: float
    swap_pages: int
    disk_gb: float


@dataclass(frozen=True)
class Decision:
    kind: str  # "STOP" or "WARN"
    status: str
    reason: str


# --- Pure decision logic (testable on any OS) -----------------------------------------------


def _window(samples: List[Sample], seconds: float) -> List[Sample]:
    t_last = samples[-1].t
    return [s for s in samples if s.t >= t_last - seconds]


def sustained(samples: List[Sample], predicate: Callable[[Sample], bool], seconds: float, interval: float) -> bool:
    if len(samples) < 2:
        return False
    t_last = samples[-1].t
    win = _window(samples, seconds)
    if len(win) < 2:
        return False
    if win[0].t > (t_last - seconds) + interval + 1e-6:
        return False  # window not covered: missing samples
    return all(predicate(s) for s in win)


def swap_rate_kib_s(samples: List[Sample], seconds: float, interval: float, page_kib: float = 4.0) -> Optional[float]:
    if len(samples) < 2:
        return None
    t_last = samples[-1].t
    earlier = [s for s in samples if s.t <= t_last - seconds]
    first = earlier[-1] if earlier else samples[0]
    dt = t_last - first.t
    if dt < seconds - interval:
        return None
    return (samples[-1].swap_pages - first.swap_pages) * page_kib / dt


def decide(samples: List[Sample], th: Thresholds, page_kib: float = 4.0) -> Optional[Decision]:
    if not samples:
        return None
    last = samples[-1]
    if sustained(samples, lambda s: s.mem_mb < th.emergency_mem_mb, th.emergency_seconds, th.interval):
        return Decision("STOP", "KILLED_LOW_MEMORY_EMERGENCY",
                        f"MemAvailable < {th.emergency_mem_mb} MB for {th.emergency_seconds} s")
    if sustained(samples, lambda s: s.mem_mb < th.stop_mem_mb, th.stop_mem_seconds, th.interval):
        return Decision("STOP", "KILLED_LOW_MEMORY",
                        f"MemAvailable < {th.stop_mem_mb} MB for {th.stop_mem_seconds} s")
    rate = swap_rate_kib_s(samples, th.swap_seconds, th.interval, page_kib)
    if rate is not None and rate > th.swap_kib_s:
        return Decision("STOP", "KILLED_SWAP",
                        f"swap-in+out {rate:.0f} KiB/s over {th.swap_seconds} s (limit {th.swap_kib_s:.0f})")
    k = th.disk_consecutive
    if len(samples) >= k and all(s.disk_gb < th.stop_disk_gb for s in samples[-k:]):
        return Decision("STOP", "KILLED_DISK",
                        f"free space on {th.disk_path} < {th.stop_disk_gb} GB in {k} samples")
    if last.mem_mb < th.warn_mem_mb:
        return Decision("WARN", "WARNING_LOW_MEMORY", f"MemAvailable {last.mem_mb:.0f} MB < {th.warn_mem_mb} MB")
    return None


def parse_meminfo_available_mb(text: str) -> float:
    for line in text.splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024.0
    raise ValueError("MemAvailable not found")


def parse_vmstat_swap_pages(text: str) -> int:
    values = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in ("pswpin", "pswpout"):
            values[parts[0]] = int(parts[1])
    if set(values) != {"pswpin", "pswpout"}:
        raise ValueError("pswpin/pswpout not found")
    return values["pswpin"] + values["pswpout"]


def read_sample(th: Thresholds, meminfo: str = "/proc/meminfo", vmstat: str = "/proc/vmstat",
                clock: Callable[[], float] = time.time) -> Sample:
    with open(meminfo, "r", encoding="utf-8") as f:
        mem = parse_meminfo_available_mb(f.read())
    with open(vmstat, "r", encoding="utf-8") as f:
        pages = parse_vmstat_swap_pages(f.read())
    disk_gb = shutil.disk_usage(th.disk_path).free / float(1024 ** 3)
    return Sample(t=clock(), mem_mb=mem, swap_pages=pages, disk_gb=disk_gb)


# --- Process group handling (POSIX only) ----------------------------------------------------


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def stop_group(pgid: int, grace: float, alive: Callable[[int], bool] = group_alive,
               sleep: Callable[[float], None] = time.sleep) -> bool:
    """SIGTERM the group, wait, SIGKILL if needed. Returns True when the group is gone."""
    if pgid <= 1:
        raise ValueError("refusing to signal pgid <= 1")
    if pgid == os.getpgrp():
        raise RuntimeError("refusing to signal the watchdog's own process group")
    os.killpg(pgid, signal.SIGTERM)
    waited = 0.0
    while alive(pgid) and waited < grace:
        sleep(0.5)
        waited += 0.5
    if alive(pgid):
        os.killpg(pgid, signal.SIGKILL)
        sleep(1.0)
    return not alive(pgid)


# --- Watch loop -----------------------------------------------------------------------------


def watch(pgid: int, status_file: str, th: Thresholds, page_kib: float = 4.0,
          sampler: Optional[Callable[[], Sample]] = None, alive: Callable[[int], bool] = group_alive,
          sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time,
          log: Optional[Callable[[str], None]] = None, max_seconds: Optional[float] = None) -> dict:
    sampler = sampler or (lambda: read_sample(th, clock=clock))
    log = log or (lambda msg: print(msg, file=sys.stderr, flush=True))
    keep = max(th.emergency_seconds, th.stop_mem_seconds, th.swap_seconds) * 2 + 2 * th.interval
    samples: List[Sample] = []
    summary = {"min_mem_mb": None, "max_swap_kib_s": None, "min_disk_gb": None, "samples": 0}
    started = clock()
    last_warn = -1e9
    result = {"status": "JOB_ENDED", "reason": "process group ended before any limit was reached"}
    while alive(pgid):
        if max_seconds is not None and clock() - started > max_seconds:
            result = {"status": "WATCH_TIMEOUT", "reason": f"watchdog max_seconds={max_seconds} reached"}
            break
        s = sampler()
        samples.append(s)
        samples = [x for x in samples if x.t >= s.t - keep]
        summary["samples"] += 1
        summary["min_mem_mb"] = s.mem_mb if summary["min_mem_mb"] is None else min(summary["min_mem_mb"], s.mem_mb)
        summary["min_disk_gb"] = s.disk_gb if summary["min_disk_gb"] is None else min(summary["min_disk_gb"], s.disk_gb)
        rate = swap_rate_kib_s(samples, th.swap_seconds, th.interval, page_kib)
        if rate is not None:
            summary["max_swap_kib_s"] = rate if summary["max_swap_kib_s"] is None else max(summary["max_swap_kib_s"], rate)
        d = decide(samples, th, page_kib)
        if d is not None and d.kind == "WARN":
            if clock() - last_warn >= 60:
                log(f"[watchdog] {d.status}: {d.reason}")
                last_warn = clock()
        elif d is not None and d.kind == "STOP":
            ended = stop_group(pgid, th.kill_grace, alive=alive, sleep=sleep)
            result = {"status": d.status, "reason": d.reason, "group_ended": ended}
            break
        sleep(th.interval)
    result.update({"pgid": pgid, "finished_utc_epoch": clock(), "thresholds": asdict(th), "summary": summary,
                   "last_sample": asdict(samples[-1]) if samples else None})
    tmp = status_file + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(status_file)), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    os.replace(tmp, status_file)
    return result


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Stop one process group when RAM, swap or disk is in danger.")
    ap.add_argument("--pgid", type=int, required=True)
    ap.add_argument("--status-file", required=True)
    ap.add_argument("--disk-path", default="/var/tmp")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--warn-mem-mb", type=float, default=800.0)
    ap.add_argument("--emergency-mem-mb", type=float, default=300.0)
    ap.add_argument("--emergency-seconds", type=float, default=5.0)
    ap.add_argument("--stop-mem-mb", type=float, default=500.0)
    ap.add_argument("--stop-mem-seconds", type=float, default=30.0)
    ap.add_argument("--swap-kib-s", type=float, default=1024.0)
    ap.add_argument("--swap-seconds", type=float, default=30.0)
    ap.add_argument("--stop-disk-gb", type=float, default=10.0)
    ap.add_argument("--kill-grace", type=float, default=20.0)
    ap.add_argument("--max-seconds", type=float, default=None, help="Stop watching after this many seconds (no kill).")
    return ap


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    th = Thresholds(
        warn_mem_mb=args.warn_mem_mb,
        emergency_mem_mb=args.emergency_mem_mb,
        emergency_seconds=args.emergency_seconds,
        stop_mem_mb=args.stop_mem_mb,
        stop_mem_seconds=args.stop_mem_seconds,
        swap_kib_s=args.swap_kib_s,
        swap_seconds=args.swap_seconds,
        stop_disk_gb=args.stop_disk_gb,
        interval=args.interval,
        kill_grace=args.kill_grace,
        disk_path=args.disk_path,
    )
    if not hasattr(os, "killpg"):
        print("BLOCKED: the watchdog needs POSIX process groups (run it on the VM).", file=sys.stderr)
        return 2
    page_kib = (os.sysconf("SC_PAGE_SIZE") / 1024.0) if hasattr(os, "sysconf") else 4.0
    result = watch(args.pgid, args.status_file, th, page_kib=page_kib, max_seconds=args.max_seconds)
    print(json.dumps({"status": result["status"], "reason": result.get("reason")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
