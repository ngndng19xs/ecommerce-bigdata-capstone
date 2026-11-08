"""P5 ML stage 1: a later run may reuse a SHARED artefact only when its manifest and links still match."""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "run"))

import p5_stage1_ledger as L  # noqa: E402


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot(tmp_path):
    snap = tmp_path / "ml_snapshot_455"
    snap.mkdir()
    manifest = snap / "snapshot_manifest.json"
    manifest.write_text(json.dumps({"kind": "snapshot", "threshold": 455}), encoding="utf-8")
    shafile = snap / "snapshot_manifest.sha256"
    shafile.write_text(f"{_sha(manifest)}  snapshot_manifest.json\n", encoding="utf-8")
    return manifest, shafile


def _reuse(tmp_path, manifest, *extra):
    return L.main(["reuse-check", "--name", "S4_snapshot_build", "--report-dir", str(tmp_path / "report"),
                   "--manifest", str(manifest), *extra])


def test_snapshot_reuse_needs_a_matching_sha(tmp_path):
    manifest, shafile = _snapshot(tmp_path)
    assert _reuse(tmp_path, manifest, "--sha-file", str(shafile)) == 0
    step = json.loads((tmp_path / "report" / "steps" / "S4_snapshot_build.json").read_text(encoding="utf-8"))
    assert step["status"] == "PASS" and step["reused"] is True
    manifest.write_text(json.dumps({"kind": "snapshot", "threshold": 10}), encoding="utf-8")  # tampered
    assert _reuse(tmp_path, manifest, "--sha-file", str(shafile)) == 1


def test_subset_reuse_needs_the_same_parent_snapshot(tmp_path):
    manifest, _ = _snapshot(tmp_path)
    subset = tmp_path / "_subset_manifest.json"
    subset.write_text(json.dumps({"kind": "subset", "parent_manifest_sha256": _sha(manifest)}), encoding="utf-8")
    assert _reuse(tmp_path, subset, "--parent-manifest", str(manifest)) == 0
    subset.write_text(json.dumps({"kind": "subset", "parent_manifest_sha256": "0" * 64}), encoding="utf-8")
    assert _reuse(tmp_path, subset, "--parent-manifest", str(manifest)) == 1


def test_read_once_reuse_requires_pass(tmp_path):
    ok = tmp_path / "read_once_check.json"
    ok.write_text(json.dumps({"status": "PASS", "ratio": 1.0}), encoding="utf-8")
    assert _reuse(tmp_path, ok, "--require-pass") == 0
    ok.write_text(json.dumps({"status": "FAIL", "ratio": 3.3}), encoding="utf-8")
    assert _reuse(tmp_path, ok, "--require-pass") == 1


def test_missing_manifest_is_never_reused(tmp_path):
    assert _reuse(tmp_path, tmp_path / "absent.json") == 1
