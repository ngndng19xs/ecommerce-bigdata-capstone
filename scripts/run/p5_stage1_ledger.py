"""P5-B-B6-01 / ML stage 1: step ledger, checkpoints, resume, GO check and overall verdict.

All status logic lives here (pure functions first) so it can be tested on Windows.
The runner (run_p5_ml_stage1_vm_b.sh) calls these subcommands in order.

Subcommands:
  probe                 S0: environment and HDFS metadata (hdfs dfs -du -s, -ls -R). Metadata only.
  run-step              run one step in its own process group, with timeout and watchdog.
  read-once-check       S4b: bytes read by the snapshot job must be about one pass over Curated.
  watchdog-selftest     S3: forced threshold on a dummy process group; the group must be stopped.
  go-check              the human GO file exists and is filled in.
  summarize             OVERALL verdict from the step files (exit code 0..4).
  report                S11: resource summary and forecast for the main run (x455/57 x2).

Exit codes of run-step: 0 PASS; 1 FAIL; 2 BLOCKED; 3 INCOMPLETE (killed, timeout, interrupted);
5 WAITING_DECISION. summarize: 0 PASS; 1 FAIL; 2 BLOCKED; 3 INCOMPLETE; 4 WAITING_GO or WAITING_DECISION.
"""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import p5_watchdog  # noqa: E402

WATCHDOG = os.path.join(HERE, "p5_watchdog.py")
DEFAULT_CONFIG = os.path.join(ROOT, "configs", "ml_stage1.example.yaml")
KILLED_STATES = {"KILLED_LOW_MEMORY", "KILLED_LOW_MEMORY_EMERGENCY", "KILLED_SWAP", "KILLED_DISK"}
INCOMPLETE_STATES = KILLED_STATES | {"TIMEOUT", "INTERRUPTED"}
EXIT_BY_OVERALL = {"PASS": 0, "FAIL": 1, "BLOCKED": 2, "INCOMPLETE": 3, "WAITING_DECISION": 4, "WAITING_GO": 4}
EXIT_BY_STEP = {"PASS": 0, "FAIL": 1, "BLOCKED": 2, "WAITING_DECISION": 5}
STAGE1_CODE_FILES = [
    "src/ml/dataset/ml_stage1.py",
    "src/ml/dataset/n_selection.py",
    "ml/session_features.py",
    "ml/features.py",
    "src/common/spark_session.py",
    "scripts/run/p5_snapshot.py",
    "scripts/run/run_p5_b6_01_n_analysis.py",
    "scripts/run/run_b6_session_features.py",
    "scripts/run/compare_p5_results.py",
    "scripts/run/p5_watchdog.py",
    "scripts/run/p5_stage1_ledger.py",
    "scripts/run/p5_stage1_checks.py",
    "configs/n_candidate_analysis.example.yaml",
    "configs/ml_stage1.example.yaml",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def status_from_rc(rc: int) -> str:
    return {0: "PASS", 2: "BLOCKED", 5: "WAITING_DECISION"}.get(rc, "FAIL")


def overall(statuses: Dict[str, str], mandatory: Sequence[str], go_granted: bool = False) -> str:
    """Priority: FAIL > BLOCKED > INCOMPLETE > WAITING_DECISION > WAITING_GO (or PASS with GO).

    A missing mandatory step (NOT_RUN) or a skipped one counts as INCOMPLETE, unless a
    WAITING_DECISION explains it. Nothing is PASS without GO in stage 1.
    """
    values = [statuses.get(name, "NOT_RUN") for name in mandatory]
    if "FAIL" in values:
        return "FAIL"
    if "BLOCKED" in values:
        return "BLOCKED"
    if any(v in INCOMPLETE_STATES for v in values):
        return "INCOMPLETE"
    if "WAITING_DECISION" in values:
        return "WAITING_DECISION"
    if any(v in ("NOT_RUN", "SKIPPED") for v in values):
        return "INCOMPLETE"
    return "PASS" if go_granted else "WAITING_GO"


def code_sha(paths: Iterable[str], root: str = ROOT) -> str:
    digest = hashlib.sha256()
    for rel in sorted(paths):
        full = os.path.join(root, rel)
        digest.update(rel.encode("utf-8") + b"\0")
        if os.path.exists(full):
            with open(full, "rb") as f:
                digest.update(hashlib.sha256(f.read()).digest())
        else:
            digest.update(b"MISSING")
    return digest.hexdigest()


def should_skip(checkpoint: Optional[Dict[str, Any]], marker_exists: bool, input_sha: str,
                code_sha_now: str) -> Tuple[bool, str]:
    """Reuse a step only when its PASS checkpoint, output marker, input and code all still match."""
    if not checkpoint:
        return False, "no checkpoint"
    if checkpoint.get("status") != "PASS":
        return False, "checkpoint is not PASS"
    if not marker_exists:
        return False, "output marker missing (_SUCCESS or manifest)"
    if checkpoint.get("input_sha") != input_sha:
        return False, "input manifest changed"
    if checkpoint.get("code_sha") != code_sha_now:
        return False, "code changed since the checkpoint"
    return True, "checkpoint PASS, marker present, input and code unchanged"


def incomplete_name(path: str, attempt: int) -> str:
    return f"{path}.INCOMPLETE-{attempt}"


def mark_incomplete(path: str) -> Optional[str]:
    """Rename a partial output so no later step can read it. Never deletes anything."""
    if not os.path.exists(path):
        return None
    attempt = 1
    while os.path.exists(incomplete_name(path, attempt)):
        attempt += 1
    target = incomplete_name(path, attempt)
    os.rename(path, target)
    return target


def go_check(run_dir: str, run_id: str) -> Tuple[bool, str]:
    path = Path(run_dir) / "WAITING_GO" / f"GO_{run_id}.txt"
    if not path.exists():
        return False, f"missing {path}"
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if len(lines) < 3:
        return False, "GO file must hold three lines: name, date (UTC), reason"
    return True, f"GO by {lines[0]} at {lines[1]}"


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
    os.replace(str(tmp), str(path))


def load_config(path: str) -> Dict[str, Any]:
    import yaml

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def watchdog_args(cfg: Dict[str, Any]) -> List[str]:
    wd = cfg.get("watchdog", {})
    mapping = [
        ("interval", "--interval"),
        ("warn_mem_mb", "--warn-mem-mb"),
        ("emergency_mem_mb", "--emergency-mem-mb"),
        ("emergency_seconds", "--emergency-seconds"),
        ("stop_mem_mb", "--stop-mem-mb"),
        ("stop_mem_seconds", "--stop-mem-seconds"),
        ("swap_kib_s", "--swap-kib-s"),
        ("swap_seconds", "--swap-seconds"),
        ("stop_disk_gb", "--stop-disk-gb"),
        ("kill_grace", "--kill-grace"),
        ("disk_path", "--disk-path"),
    ]
    out: List[str] = []
    for key, flag in mapping:
        if key in wd and wd[key] is not None:
            out += [flag, str(wd[key])]
    return out


# --- Signals and process groups (POSIX) ----------------------------------------------------

_INTERRUPTED = {"flag": False, "signal": None}


def _on_signal(signum, frame):  # noqa: ARG001 - signature required by signal module
    _INTERRUPTED["flag"] = True
    _INTERRUPTED["signal"] = signum


def _install_signal_handlers() -> Dict[int, Any]:
    previous = {}
    for name in ("SIGINT", "SIGTERM", "SIGHUP"):
        if hasattr(signal, name):
            sig = getattr(signal, name)
            previous[sig] = signal.signal(sig, _on_signal)
    return previous


def _restore_signal_handlers(previous: Dict[int, Any]) -> None:
    for sig, handler in previous.items():
        signal.signal(sig, handler)


def _kill_group(proc: subprocess.Popen, pgid: Optional[int], grace: float) -> None:
    if pgid is None:  # Windows: no process groups
        proc.kill()
        proc.wait(timeout=30)
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    waited = 0.0
    while (proc.poll() is None or p5_watchdog.group_alive(pgid)) and waited < grace:
        time.sleep(0.5)
        waited += 0.5
    if p5_watchdog.group_alive(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    proc.poll()


# --- Subcommands ------------------------------------------------------------------------------


def cmd_run_step(args) -> int:
    report = Path(args.report_dir)
    logs = Path(args.log_dir)
    cfg = load_config(args.config)
    cmd = list(args.cmd)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("BLOCKED: no command after --", file=sys.stderr)
        return 2
    code_now = code_sha(STAGE1_CODE_FILES)
    checkpoint = read_json(report / "checkpoints" / f"{args.name}.json")
    marker_exists = bool(args.marker) and os.path.exists(args.marker)

    if args.resume:
        skip, why = should_skip(checkpoint, marker_exists, args.input_sha, code_now)
        if skip:
            write_json(report / "steps" / f"{args.name}.json", {
                "name": args.name, "status": "PASS", "reused": True, "reason": why, "finished_utc": now_utc(),
            })
            print(f"[{args.name}] reused from checkpoint: {why}")
            return 0
        if args.output_path and os.path.exists(args.output_path):
            renamed = mark_incomplete(args.output_path)
            print(f"[{args.name}] previous partial output renamed to {renamed}")
    elif args.output_path and os.path.exists(args.output_path):
        print(f"BLOCKED: output {args.output_path} already exists (new RUN_ID required)", file=sys.stderr)
        return 2

    logs.mkdir(parents=True, exist_ok=True)
    attempt = len(list(logs.glob(f"{args.name}.attempt*.log"))) + 1
    log_path = logs / f"{args.name}.attempt{attempt}.log"
    popen_kwargs: Dict[str, Any] = {}
    if os.name != "nt":
        popen_kwargs["start_new_session"] = True
    watch_status = report / "watch" / f"{args.name}.json"
    watch_status.parent.mkdir(parents=True, exist_ok=True)
    if watch_status.exists():
        watch_status.unlink()

    status: Optional[str] = None
    rc: Optional[int] = None
    pgid: Optional[int] = None
    watch_proc: Optional[subprocess.Popen] = None
    previous = _install_signal_handlers()
    _INTERRUPTED["flag"] = False
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as logf:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT, **popen_kwargs)
        try:
            if os.name != "nt":
                pgid = os.getpgid(proc.pid)
            if args.watch and pgid is not None:
                wd_cmd = [sys.executable, WATCHDOG, "--pgid", str(pgid), "--status-file", str(watch_status)]
                wd_cmd += watchdog_args(cfg)
                watch_proc = subprocess.Popen(wd_cmd, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
            while True:
                rc = proc.poll()
                if rc is not None:
                    break
                if _INTERRUPTED["flag"]:
                    _kill_group(proc, pgid, float(cfg.get("watchdog", {}).get("kill_grace", 20)))
                    status = "INTERRUPTED"
                    rc = proc.poll()
                    break
                if args.timeout and (time.time() - t0) > args.timeout:
                    _kill_group(proc, pgid, float(cfg.get("watchdog", {}).get("kill_grace", 20)))
                    status = "TIMEOUT"
                    rc = proc.poll()
                    break
                time.sleep(1.0)
        finally:
            if watch_proc is not None and watch_proc.poll() is None:
                # Let watchdog finish writing its status after stopping the job.
                # Otherwise a SIGTERM can be recorded as generic FAIL.
                wait_seconds = (
                    float(cfg.get("watchdog", {}).get("kill_grace", 20)) + 10.0
                    if rc is not None and rc < 0 else 8.0
                )
                try:
                    watch_proc.wait(timeout=wait_seconds)
                except subprocess.TimeoutExpired:
                    watch_proc.terminate()
                    try:
                        watch_proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        watch_proc.kill()
                        watch_proc.wait(timeout=5)
            _restore_signal_handlers(previous)

    watch_info = read_json(watch_status)
    if watch_info and watch_info.get("status") in KILLED_STATES:
        status = watch_info["status"]
    if status is None:
        status = status_from_rc(rc if rc is not None else 1)
    elapsed = round(time.time() - t0, 1)
    result = {
        "name": args.name,
        "status": status,
        "exit_code": rc,
        "elapsed_seconds": elapsed,
        "pgid": pgid,
        "attempt": attempt,
        "log": str(log_path),
        "watch": watch_info,
        "input_sha": args.input_sha,
        "code_sha": code_now,
        "output_path": args.output_path,
        "marker": args.marker,
        "finished_utc": now_utc(),
    }
    if status == "PASS" and args.marker and not os.path.exists(args.marker):
        result["status"] = status = "FAIL"
        result["reason"] = "step exited 0 but its output marker is missing"
    write_json(report / "steps" / f"{args.name}.json", result)
    if status == "PASS":
        write_json(report / "checkpoints" / f"{args.name}.json", {
            "name": args.name, "status": "PASS", "input_sha": args.input_sha, "code_sha": code_now,
            "output_path": args.output_path, "marker": args.marker, "written_utc": now_utc(),
        })
    elif status in INCOMPLETE_STATES:
        ckpt = report / "checkpoints" / f"{args.name}.json"
        if ckpt.exists():
            ckpt.unlink()
        if args.output_path and os.path.exists(args.output_path):
            renamed = mark_incomplete(args.output_path)
            print(f"[{args.name}] partial output renamed to {renamed}")
    print(json.dumps({"name": args.name, "status": status, "elapsed_seconds": elapsed}, ensure_ascii=False))
    return {"PASS": 0, "FAIL": 1, "BLOCKED": 2, "WAITING_DECISION": 5}.get(status, 3)


def cmd_probe(args) -> int:
    report = Path(args.report_dir)
    env_dir = report / "env"
    env_dir.mkdir(parents=True, exist_ok=True)
    commands = {
        "free_m": ["free", "-m"],
        "swapon": ["swapon", "--show"],
        "df_work_root": ["df", "-hT", args.work_root],
        "df_repo": ["df", "-hT", args.repo],
        "python": [sys.executable, "--version"],
        "java": ["java", "-version"],
        "hdfs_du": ["hdfs", "dfs", "-du", "-s", args.source],
        "hdfs_ls_r": ["hdfs", "dfs", "-ls", "-R", args.source],
    }
    results: Dict[str, Any] = {}
    for key, argv in commands.items():
        try:
            proc = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180,
                                  universal_newlines=True)
            text, rc = proc.stdout, proc.returncode
        except (OSError, subprocess.TimeoutExpired) as exc:
            text, rc = f"ERROR: {exc}", -1
        (env_dir / f"{key}.txt").write_text(text, encoding="utf-8")
        results[key] = {"rc": rc}
    ps = subprocess.run("ps -ef | grep -E '[s]park' || true", shell=True, stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, universal_newlines=True)
    (env_dir / "ps_spark.txt").write_text(ps.stdout, encoding="utf-8")
    spark_procs = len([ln for ln in ps.stdout.splitlines() if ln.strip()])

    du_text = (env_dir / "hdfs_du.txt").read_text(encoding="utf-8")
    du_bytes = None
    for tok in du_text.split():
        if tok.isdigit():
            du_bytes = int(tok)
            break
    ls_text = (env_dir / "hdfs_ls_r.txt").read_text(encoding="utf-8")
    files = sum(1 for ln in ls_text.splitlines() if ln.startswith("-"))
    info = {
        "checked_utc": now_utc(),
        "commands": results,
        "hdfs_curated_bytes": du_bytes,
        "hdfs_curated_files": files,
        "other_spark_processes": spark_procs,
        "python_version_text": (env_dir / "python.txt").read_text(encoding="utf-8").strip(),
    }
    write_json(report / "env.json", info)
    ok = results["hdfs_du"]["rc"] == 0 and results["hdfs_ls_r"]["rc"] == 0 and du_bytes and files > 0
    print(json.dumps(info, indent=2, ensure_ascii=False))
    return 0 if ok else 1


def cmd_reuse_check(args) -> int:
    """Reuse a SHARED artefact (snapshot, subset, read-once check) written by an earlier run.

    Only the manifest is trusted, and only when its SHA matches the sha file (snapshot) and its
    parent link matches (subset). The step verify (S5) still runs on every run.
    """
    report = Path(args.report_dir)
    problems: List[str] = []
    manifest = read_json(Path(args.manifest))
    if manifest is None:
        problems.append(f"manifest missing: {args.manifest}")
    if manifest is not None and args.require_pass and manifest.get("status") != "PASS":
        problems.append(f"status is {manifest.get('status')!r}, not PASS")
    if args.sha_file and os.path.exists(args.sha_file) and manifest is not None:
        expected = Path(args.sha_file).read_text(encoding="utf-8").split()[0]
        if sha256_path(args.manifest) != expected:
            problems.append("manifest sha256 differs from the sha file")
    if args.parent_manifest and manifest is not None:
        if manifest.get("parent_manifest_sha256") != sha256_path(args.parent_manifest):
            problems.append("subset was built from another snapshot manifest")
    status = "PASS" if not problems else "FAIL"
    write_json(report / "steps" / f"{args.name}.json", {
        "name": args.name, "status": status, "reused": True, "reuse_source": args.manifest,
        "reason": "; ".join(problems) if problems else "shared artefact verified and reused",
        "finished_utc": now_utc(),
    })
    print(json.dumps({"name": args.name, "status": status, "problems": problems}, ensure_ascii=False))
    return 0 if status == "PASS" else 1


def sha256_path(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def cmd_read_once_check(args) -> int:
    metrics = read_json(Path(args.eventlog_metrics))
    if metrics is None:
        print(f"BLOCKED: no event log metrics at {args.eventlog_metrics}", file=sys.stderr)
        return 2
    read_bytes = float(metrics.get("input_mb_read", 0.0)) * 1024 * 1024
    source_bytes = float(args.source_bytes)
    ratio = read_bytes / source_bytes if source_bytes else float("inf")
    ok = 0.8 <= ratio <= 1.25
    out = {"status": "PASS" if ok else "FAIL", "input_bytes_read": int(read_bytes),
           "source_bytes": int(source_bytes), "ratio": round(ratio, 4), "accepted_range": [0.8, 1.25]}
    write_json(Path(args.out), out)
    print(json.dumps(out, indent=2))
    return 0 if ok else 1


def cmd_watchdog_selftest(args) -> int:
    report = Path(args.report_dir)
    if os.name == "nt":
        print("BLOCKED: the watchdog self-test needs POSIX process groups; run it on the VM.", file=sys.stderr)
        return 2
    t0 = time.time()
    job = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)
    pgid = os.getpgid(job.pid)
    status_file = report / "watch" / "selftest.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)
    wd_cmd = [sys.executable, WATCHDOG, "--pgid", str(pgid), "--status-file", str(status_file),
              "--interval", "1", "--stop-mem-mb", "100000", "--stop-mem-seconds", "3",
              "--kill-grace", "5", "--max-seconds", "60"]
    wd_proc = subprocess.Popen(wd_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               universal_newlines=True)
    deadline = time.time() + 90
    while wd_proc.poll() is None and time.time() < deadline:
        job.poll()  # reap the job as soon as it dies: a zombie leader would keep its group "alive"
        time.sleep(0.5)
    if wd_proc.poll() is None:
        wd_proc.kill()
    wd_out, _ = wd_proc.communicate(timeout=10)

    class _WD:  # same shape as subprocess.CompletedProcess for the checks below
        returncode = wd_proc.returncode
        stdout = wd_out or ""

    wd = _WD()
    group_still_alive = p5_watchdog.group_alive(pgid)
    if group_still_alive:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        job.wait(timeout=10)
    except subprocess.TimeoutExpired:
        job.kill()
    st = read_json(status_file) or {}
    checks = {
        "watchdog_exit_0": wd.returncode == 0,
        "status_is_killed_low_memory": st.get("status") == "KILLED_LOW_MEMORY",
        "job_group_stopped": not group_still_alive,
        "elapsed_under_60s": (time.time() - t0) < 60,
        "watchdog_outside_job_group": pgid != os.getpgrp(),
    }
    out = {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
           "watchdog_status": st.get("status"), "watchdog_output": wd.stdout[-500:], "checked_utc": now_utc()}
    write_json(report / "watchdog_selftest.json", out)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0 if out["status"] == "PASS" else 1


def cmd_go_check(args) -> int:
    ok, why = go_check(args.run_dir, args.run_id)
    print(json.dumps({"go": ok, "reason": why}, ensure_ascii=False))
    return 0 if ok else 2


def cmd_summarize(args) -> int:
    report = Path(args.report_dir)
    mandatory = [s for s in args.mandatory.split() if s]
    statuses: Dict[str, str] = {}
    details: Dict[str, Any] = {}
    for path in sorted((report / "steps").glob("*.json")):
        data = read_json(path) or {}
        statuses[data.get("name", path.stem)] = data.get("status", "FAIL")
        details[data.get("name", path.stem)] = {k: data.get(k) for k in ("status", "exit_code", "elapsed_seconds",
                                                                          "reason", "reused")}
    go_ok, go_why = (go_check(args.run_dir_go, args.run_id) if args.go_check else (False, "GO not requested"))
    verdict = overall(statuses, mandatory, go_granted=go_ok)
    out = {
        "run_id": args.run_id,
        "overall": verdict,
        "exit_code": EXIT_BY_OVERALL[verdict],
        "mandatory": mandatory,
        "mandatory_status": {name: statuses.get(name, "NOT_RUN") for name in mandatory},
        "go": {"granted": go_ok, "reason": go_why},
        "steps": details,
        "summarized_utc": now_utc(),
        "note": "Stage 1 never reports PASS without GO. WAITING_GO means every stage 1 check passed.",
    }
    write_json(report / "run_status.json", out)
    print(json.dumps({"overall": verdict, "exit_code": out["exit_code"],
                      "mandatory_status": out["mandatory_status"]}, indent=2, ensure_ascii=False))
    return out["exit_code"]


def cmd_report(args) -> int:
    report = Path(args.report_dir)
    scale = float(args.main_threshold) / float(args.pilot_threshold)
    rows = {}
    for name in args.steps.split():
        data = read_json(report / "steps" / f"{name}.json") or {}
        watch = read_json(report / "watch" / f"{name}.json") or {}
        summary = watch.get("summary", {})
        elapsed = data.get("elapsed_seconds")
        rows[name] = {
            "status": data.get("status"),
            "pilot_seconds": elapsed,
            "forecast_main_seconds_estimate": round(elapsed * scale * args.safety_factor, 1) if elapsed else None,
            "min_mem_available_mb": summary.get("min_mem_mb"),
            "max_swap_kib_s": summary.get("max_swap_kib_s"),
            "min_var_tmp_free_gb": summary.get("min_disk_gb"),
            "watch_status": watch.get("status"),
        }
    total = sum(r["forecast_main_seconds_estimate"] or 0 for r in rows.values())
    out = {
        "method": "forecast = pilot seconds x (main threshold / pilot threshold) x safety factor, per step",
        "scale": round(scale, 3),
        "safety_factor": args.safety_factor,
        "steps": rows,
        "total_forecast_seconds_estimate": round(total, 1),
        "total_forecast_hours_estimate": round(total / 3600.0, 2),
        "caveat": "Estimate from a 1/8 pilot. Sorting and window functions are not linear; treat as a range.",
        "generated_utc": now_utc(),
    }
    write_json(report / "forecast.json", out)
    lines = ["RESOURCE SUMMARY (stage 1 pilot)"]
    for name, r in rows.items():
        lines.append(f"{name}: status={r['status']} pilot_s={r['pilot_seconds']} "
                     f"min_mem_mb={r['min_mem_available_mb']} max_swap_kib_s={r['max_swap_kib_s']} "
                     f"min_var_tmp_gb={r['min_var_tmp_free_gb']} watch={r['watch_status']}")
    (report / "resource_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="P5 ML stage 1 step ledger.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("probe")
    p.add_argument("--report-dir", required=True)
    p.add_argument("--source", default="/data/curated/events")
    p.add_argument("--work-root", required=True)
    p.add_argument("--repo", default=ROOT)
    p.set_defaults(func=cmd_probe)

    r = sub.add_parser("run-step")
    r.add_argument("--name", required=True)
    r.add_argument("--report-dir", required=True)
    r.add_argument("--log-dir", required=True)
    r.add_argument("--config", default=DEFAULT_CONFIG)
    r.add_argument("--timeout", type=float, default=1800.0)
    r.add_argument("--watch", action="store_true", help="run the watchdog on this step's process group")
    r.add_argument("--resume", action="store_true", help="reuse a valid PASS checkpoint instead of running")
    r.add_argument("--input-sha", default="none")
    r.add_argument("--output-path", default=None)
    r.add_argument("--marker", default=None, help="file that proves the output is complete")
    r.add_argument("cmd", nargs=argparse.REMAINDER)
    r.set_defaults(func=cmd_run_step)

    u = sub.add_parser("reuse-check")
    u.add_argument("--name", required=True)
    u.add_argument("--report-dir", required=True)
    u.add_argument("--manifest", required=True)
    u.add_argument("--sha-file", default=None)
    u.add_argument("--parent-manifest", default=None)
    u.add_argument("--require-pass", action="store_true")
    u.set_defaults(func=cmd_reuse_check)

    k = sub.add_parser("read-once-check")
    k.add_argument("--eventlog-metrics", required=True)
    k.add_argument("--source-bytes", required=True, type=float)
    k.add_argument("--out", required=True)
    k.set_defaults(func=cmd_read_once_check)

    w = sub.add_parser("watchdog-selftest")
    w.add_argument("--report-dir", required=True)
    w.set_defaults(func=cmd_watchdog_selftest)

    g = sub.add_parser("go-check")
    g.add_argument("--run-dir", required=True)
    g.add_argument("--run-id", required=True)
    g.set_defaults(func=cmd_go_check)

    s = sub.add_parser("summarize")
    s.add_argument("--report-dir", required=True)
    s.add_argument("--run-id", required=True)
    s.add_argument("--mandatory", required=True, help="space-separated step names that must be PASS")
    s.add_argument("--go-check", action="store_true")
    s.add_argument("--run-dir-go", default=None, help="directory that holds WAITING_GO/ (defaults to report dir)")
    s.set_defaults(func=cmd_summarize)

    rp = sub.add_parser("report")
    rp.add_argument("--report-dir", required=True)
    rp.add_argument("--steps", required=True, help="pilot step names, e.g. 'pilot_b6_01 pilot_b6_02'")
    rp.add_argument("--pilot-threshold", type=int, default=57)
    rp.add_argument("--main-threshold", type=int, default=455)
    rp.add_argument("--safety-factor", type=float, default=2.0)
    rp.set_defaults(func=cmd_report)
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if args.cmd == "summarize" and not args.run_dir_go:
        args.run_dir_go = args.report_dir
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
