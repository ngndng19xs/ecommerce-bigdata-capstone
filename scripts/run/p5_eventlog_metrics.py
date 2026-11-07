"""Summarise a Spark event log (JSON lines, uncompressed) into measured resource metrics.

Reads SparkListenerTaskEnd task metrics, which Spark 3.1 writes when spark.eventLog.enabled=true:
shuffle bytes written / read, memory and disk spill, peak execution memory, input bytes.
These are MEASURED values for the benchmark gate; nothing is estimated here.

Usage:
  python scripts/run/p5_eventlog_metrics.py --log-dir /path/eventlog --latest --out metrics.json
  python scripts/run/p5_eventlog_metrics.py --log-file /path/eventlog/app-123 --out metrics.json
"""

import argparse
import json
import os
import sys


def summarise(lines) -> dict:
    m = {
        "tasks": 0, "failed_tasks": 0, "stages_completed": 0,
        "shuffle_bytes_written": 0, "shuffle_bytes_read": 0,
        "memory_bytes_spilled": 0, "disk_bytes_spilled": 0,
        "input_bytes_read": 0, "peak_execution_memory_max": 0,
        "largest_task_shuffle_read_bytes": 0,
        "app_start_ms": None, "app_end_ms": None,
    }
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            ev = json.loads(raw)
        except ValueError:
            continue
        kind = ev.get("Event")
        if kind == "SparkListenerApplicationStart":
            m["app_start_ms"] = ev.get("Timestamp")
        elif kind == "SparkListenerApplicationEnd":
            m["app_end_ms"] = ev.get("Timestamp")
        elif kind == "SparkListenerStageCompleted":
            m["stages_completed"] += 1
        elif kind == "SparkListenerTaskEnd":
            m["tasks"] += 1
            reason = (ev.get("Task End Reason") or {}).get("Reason")
            if reason and reason != "Success":
                m["failed_tasks"] += 1
            tm = ev.get("Task Metrics") or {}
            sw = tm.get("Shuffle Write Metrics") or {}
            sr = tm.get("Shuffle Read Metrics") or {}
            read = int(sr.get("Remote Bytes Read", 0) or 0) + int(sr.get("Local Bytes Read", 0) or 0)
            m["shuffle_bytes_written"] += int(sw.get("Shuffle Bytes Written", 0) or 0)
            m["shuffle_bytes_read"] += read
            m["largest_task_shuffle_read_bytes"] = max(m["largest_task_shuffle_read_bytes"], read)
            m["memory_bytes_spilled"] += int(tm.get("Memory Bytes Spilled", 0) or 0)
            m["disk_bytes_spilled"] += int(tm.get("Disk Bytes Spilled", 0) or 0)
            m["input_bytes_read"] += int((tm.get("Input Metrics") or {}).get("Bytes Read", 0) or 0)
            m["peak_execution_memory_max"] = max(m["peak_execution_memory_max"],
                                                 int(tm.get("Peak Execution Memory", 0) or 0))
    if m["app_start_ms"] and m["app_end_ms"]:
        m["app_duration_seconds"] = round((m["app_end_ms"] - m["app_start_ms"]) / 1000.0, 1)
    mb_names = {
        "shuffle_bytes_written": "shuffle_mb_written",
        "shuffle_bytes_read": "shuffle_mb_read",
        "memory_bytes_spilled": "memory_mb_spilled",
        "disk_bytes_spilled": "disk_mb_spilled",
        "input_bytes_read": "input_mb_read",
        "peak_execution_memory_max": "peak_execution_memory_max_mb",
        "largest_task_shuffle_read_bytes": "largest_task_shuffle_read_mb",
    }
    for src, dst in mb_names.items():
        m[dst] = round(m[src] / (1024 * 1024), 1)
    return m


def latest_log(log_dir: str) -> str:
    files = [os.path.join(log_dir, f) for f in os.listdir(log_dir)
             if not f.endswith(".inprogress") and os.path.isfile(os.path.join(log_dir, f))]
    if not files:
        raise FileNotFoundError(f"no finished event log in {log_dir}")
    return max(files, key=os.path.getmtime)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--log-file")
    src.add_argument("--log-dir")
    ap.add_argument("--latest", action="store_true", help="with --log-dir: newest finished log")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    try:
        path = args.log_file or latest_log(args.log_dir)
        if path.endswith((".lz4", ".snappy", ".zstd", ".lzf")):
            raise ValueError(f"compressed event log not supported: {path}")
        with open(path, "r", encoding="utf-8") as f:
            metrics = summarise(f)
    except (OSError, ValueError) as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    metrics["log_file"] = path
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(json.dumps({k: metrics[k] for k in ("tasks", "failed_tasks", "shuffle_mb_written", "disk_mb_spilled",
                                               "peak_execution_memory_max_mb") if k in metrics}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
