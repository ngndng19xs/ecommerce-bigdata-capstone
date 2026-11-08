"""Source integrity manifest for the P4-B-B7-01 acceptance run.

Two subcommands, stdlib only (runs on Windows and on VM B without installing Git):

  make    Run where Git exists (Windows). Records HEAD, branch, uncommitted entries and the
          SHA-256 of every file in KEY_FILES. Writes reports/P4_B_B7_01/source_manifest.json.
          Contains only repo-relative paths, hashes and git metadata. No env vars, no emails,
          no tokens.

  verify  Run on VM B. Recomputes SHA-256 of the same files and compares with the manifest.
          Exit 0: every file matches. Exit 1: mismatch or missing file. Exit 2: manifest missing
          or unreadable (TC16 must then be reported BLOCKED/UNVERIFIED, never PASS).

Usage:
  python scripts/run/source_manifest.py make
  python scripts/run/source_manifest.py verify [--out <json path>]
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MANIFEST_REL = os.path.join("reports", "P4_B_B7_01", "source_manifest.json")

# Files that define the B6 -> B7 acceptance behaviour. Changing any of them changes the evidence.
KEY_FILES = [
    "ml/features.py",
    "ml/session_features.py",
    "src/common/logger.py",
    "src/common/spark_session.py",
    "src/ml/training/lr_b6_smoke.py",
    "src/ml/training/train_lr.py",
    "src/ml/features/feature_pipeline.py",
    "src/ml/dataset/split.py",
    "src/ml/evaluation/metrics.py",
    "scripts/run/run_b6_session_features.py",
    "scripts/run/run_b7_lr_smoke.py",
    "scripts/run/run_b7_01_vm_b.sh",
    "scripts/run/source_manifest.py",
    "scripts/run/check_b7_01_evidence.py",
    "scripts/run/preflight_b7_01.py",
    "tests/unit/test_b7_01_lr_smoke.py",
    "tests/unit/test_b7_01_tooling.py",
    "tests/unit/test_b7_01_tooling.py",
    "tests/unit/test_b6_02.py",
    "tests/unit/test_ml_features.py",
    "tests/unit/test_b6_01_edges.py",
    "tests/unit/test_session_dataset.py",
    "tests/unit/test_split_metrics.py",
    "configs/config.example.yaml",
]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _git(args):
    try:
        out = subprocess.run(
            ["git"] + args, cwd=ROOT, capture_output=True, text=True, check=True
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cmd_make(_args) -> int:
    head = _git(["rev-parse", "HEAD"])
    branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    porcelain = _git(["status", "--porcelain"])
    entries = [line for line in (porcelain or "").splitlines() if line.strip()]

    files = []
    for rel in KEY_FILES:
        full = os.path.join(ROOT, rel)
        if not os.path.isfile(full):
            print(f"ERROR: key file missing on this machine: {rel}", file=sys.stderr)
            return 1
        files.append({"path": rel.replace("\\", "/"), "sha256": sha256_file(full), "bytes": os.path.getsize(full)})

    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "generated_on": "git-host",
        "git": {
            "available": head is not None,
            "head": head,
            "branch": branch,
            "uncommitted_entries": entries,
            "working_tree_snapshot": bool(entries) or head is None,
        },
        "files": files,
    }
    out_path = os.path.join(ROOT, MANIFEST_REL)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"MANIFEST_WRITTEN={out_path}")
    print(f"HEAD={head} BRANCH={branch} UNCOMMITTED_ENTRIES={len(entries)} FILES={len(files)}")
    return 0


def cmd_verify(args) -> int:
    manifest_path = os.path.join(ROOT, MANIFEST_REL)
    result = {
        "check": "TC16_source_integrity",
        "repo_root": ROOT,
        "manifest_path": MANIFEST_REL,
        "verified_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        expected = manifest["files"]
    except (OSError, ValueError, KeyError) as exc:
        result.update(status="BLOCKED", reason=f"manifest missing or unreadable: {exc}")
        return _finish(result, args, 2)

    mismatches, missing, matched = [], [], []
    for entry in expected:
        full = os.path.join(ROOT, entry["path"])
        if not os.path.isfile(full):
            missing.append(entry["path"])
            continue
        actual = sha256_file(full)
        (matched if actual == entry["sha256"] else mismatches).append(entry["path"])

    git_info = manifest.get("git", {})
    head_here = _git(["rev-parse", "HEAD"])  # None on VM B (git not installed)
    result["manifest_git"] = git_info
    result["vm_git_available"] = head_here is not None
    result["files_total"] = len(expected)
    result["files_matched"] = len(matched)
    result["files_mismatched"] = mismatches
    result["files_missing"] = missing
    result["working_tree_snapshot"] = bool(git_info.get("working_tree_snapshot", True))

    if mismatches or missing:
        result.update(
            status="FAIL",
            reason="content differs from manifest; do not run Spark steps on unknown source",
        )
        return _finish(result, args, 1)

    result.update(
        status="PASS",
        reason=(
            "all key files match the manifest SHA-256"
            + ("; commit not independently verified (git not available on this host)" if head_here is None else "")
            + ("; manifest describes a WORKING TREE SNAPSHOT, not a clean commit" if result["working_tree_snapshot"] else "")
        ),
    )
    return _finish(result, args, 0)


def _finish(result, args, code) -> int:
    text = json.dumps(result, indent=2, ensure_ascii=False)
    print(text)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description="P4-B-B7-01 source manifest tool.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("make")
    p_verify = sub.add_parser("verify")
    p_verify.add_argument("--out", default=None, help="Also write the verification JSON to this path.")
    args = parser.parse_args()
    if args.cmd == "make":
        return cmd_make(args)
    return cmd_verify(args)


if __name__ == "__main__":
    sys.exit(main())
