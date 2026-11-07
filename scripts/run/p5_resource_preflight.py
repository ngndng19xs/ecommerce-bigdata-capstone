"""Resource preflight for a P5-B-B6-01 Full run on VM B. Never changes the system.

Checks (thresholds are arguments; defaults explained below):
- MemAvailable >= --min-mem-available-mb (default 1200 MB): local[2] driver heap 1g + JVM overhead
  + Python process is ~1.5 GB; below 1.2 GB available the OS starts swapping on a 2-CPU VM.
- Swap used <= --max-swap-used-mb (default 1024 MB); --swap-policy warn|block (default warn):
  swap already in use at start means memory pressure, but idle daemons often keep some swapped pages.
- Free space on --local-dir >= --min-local-free-gb (default 30 GB): spark.local.dir holds shuffle and
  spill files for the whole SparkContext. On VM B /, /tmp (and possibly HDFS blocks) share one 50 GB
  filesystem, so this is checked again during the run by the runner's resource monitor.
- HDFS free >= --min-hdfs-free-gb (default 5 GB): the job writes only small JSON reports.
- No other SparkSubmit process running (another Spark job would compete for the same RAM).
- --output-dir must not exist (evidence is never overwritten).
- Records (does not block on) the Python local UTC offset: results no longer depend on it.

Exit 0: PASS. Exit 2: BLOCKED (do not start the Full job).
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone

GB = 1024 ** 3


def parse_meminfo(text: str) -> dict:
    """/proc/meminfo -> {field: kB}."""
    out = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        parts = rest.split()
        if parts and parts[0].isdigit():
            out[key.strip()] = int(parts[0])
    return out


def parse_hdfs_df(text: str):
    """Output of `hdfs dfs -df /` (bytes) -> available bytes, or None if not parseable."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for ln in lines[1:]:
        cols = ln.split()
        if len(cols) >= 4 and cols[3].isdigit():
            return int(cols[3])
    return None


def count_spark_submit(proc_root: str = "/proc") -> int:
    n = 0
    try:
        pids = [p for p in os.listdir(proc_root) if p.isdigit()]
    except OSError:
        return 0
    for pid in pids:
        try:
            with open(os.path.join(proc_root, pid, "cmdline"), "rb") as f:
                if b"org.apache.spark.deploy.SparkSubmit" in f.read():
                    n += 1
        except OSError:
            continue
    return n


def evaluate(values: dict, thresholds: dict) -> dict:
    """Pure decision function. values/thresholds in MB/GB as named. Returns status and checks."""
    checks = []

    def add(name, ok, detail, blocking=True):
        checks.append({"check": name, "status": "PASS" if ok else ("FAIL" if blocking else "WARN"),
                       "blocking": blocking, "detail": detail})

    mem = values.get("mem_available_mb")
    add("ram_available", mem is not None and mem >= thresholds["min_mem_available_mb"],
        f"MemAvailable={mem} MB, need >= {thresholds['min_mem_available_mb']} MB")
    swap = values.get("swap_used_mb")
    add("swap_used", swap is not None and swap <= thresholds["max_swap_used_mb"],
        f"swap used={swap} MB, limit {thresholds['max_swap_used_mb']} MB",
        blocking=thresholds["swap_policy"] == "block")
    disk = values.get("local_free_gb")
    add("local_disk_free", disk is not None and disk >= thresholds["min_local_free_gb"],
        f"free on {values.get('local_dir')}={disk} GB, need >= {thresholds['min_local_free_gb']} GB")
    hdfs = values.get("hdfs_free_gb")
    add("hdfs_free", hdfs is not None and hdfs >= thresholds["min_hdfs_free_gb"],
        f"HDFS free={hdfs} GB, need >= {thresholds['min_hdfs_free_gb']} GB")
    others = values.get("other_spark_submit")
    add("no_other_spark_job", others == 0, f"SparkSubmit processes running={others}")
    add("output_dir_new", values.get("output_dir_exists") is False,
        f"output dir {values.get('output_dir')} exists={values.get('output_dir_exists')}")
    blocked = any(c["status"] == "FAIL" and c["blocking"] for c in checks)
    return {"status": "BLOCKED" if blocked else "PASS", "checks": checks}


def collect_values(local_dir: str, output_dir: str) -> dict:
    v = {"local_dir": local_dir, "output_dir": output_dir, "output_dir_exists": os.path.exists(output_dir)}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            mi = parse_meminfo(f.read())
        v["mem_available_mb"] = round(mi["MemAvailable"] / 1024)
        v["swap_used_mb"] = round((mi["SwapTotal"] - mi["SwapFree"]) / 1024)
    except (OSError, KeyError):
        v["mem_available_mb"] = None
        v["swap_used_mb"] = None
    try:
        v["local_free_gb"] = round(shutil.disk_usage(local_dir).free / GB, 1)
    except OSError:
        v["local_free_gb"] = None
    try:
        out = subprocess.run(["hdfs", "dfs", "-df", "/"], capture_output=True, text=True, timeout=120)
        avail = parse_hdfs_df(out.stdout) if out.returncode == 0 else None
        v["hdfs_free_gb"] = None if avail is None else round(avail / GB, 1)
    except (OSError, subprocess.SubprocessError):
        v["hdfs_free_gb"] = None
    v["other_spark_submit"] = count_spark_submit()
    v["python_local_utc_offset_hours"] = datetime.now().astimezone().utcoffset().total_seconds() / 3600
    v["spark_env"] = {k: os.environ.get(k) for k in (
        "CAPSTONE_SPARK_MASTER", "CAPSTONE_DRIVER_MEMORY", "CAPSTONE_SHUFFLE_PARTITIONS", "PYSPARK_SUBMIT_ARGS")}
    return v


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="P5-B-B6-01 Full run resource preflight (read-only).")
    ap.add_argument("--local-dir", required=True, help="Directory used as spark.local.dir.")
    ap.add_argument("--output-dir", required=True, help="Full-run output dir; must not exist.")
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-mem-available-mb", type=int, default=1200)
    ap.add_argument("--max-swap-used-mb", type=int, default=1024)
    ap.add_argument("--swap-policy", choices=["warn", "block"], default="warn")
    ap.add_argument("--min-local-free-gb", type=float, default=30.0)
    ap.add_argument("--min-hdfs-free-gb", type=float, default=5.0)
    args = ap.parse_args(argv)

    thresholds = {
        "min_mem_available_mb": args.min_mem_available_mb,
        "max_swap_used_mb": args.max_swap_used_mb,
        "swap_policy": args.swap_policy,
        "min_local_free_gb": args.min_local_free_gb,
        "min_hdfs_free_gb": args.min_hdfs_free_gb,
    }
    values = collect_values(args.local_dir, args.output_dir)
    decision = evaluate(values, thresholds)
    report = {"checked_utc": datetime.now(timezone.utc).isoformat(), "thresholds": thresholds,
              "values": values, **decision}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    for c in decision["checks"]:
        print(f"{c['status']:5s} {c['check']}: {c['detail']}")
    print(f"PREFLIGHT={decision['status']}")
    return 0 if decision["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
