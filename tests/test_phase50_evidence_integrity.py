from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile

from app.research.microstructure_signal_research import PHASE42H_REPORT_MEMBER, verify_phase42h_evidence
from phase50_test_utils import ROOT, load_json


def test_phase50_evidence_integrity_uses_passed_phase42h_bundle() -> None:
    report = load_json("data/debug/phase_5_0_evidence_integrity_report.json")
    assert report["status"] == "pass"
    assert report["bundle_filename"] == "phase_4_2h_hotpath_environment_latency_bundle.zip"
    assert report["bundle_sha256_valid"] is True
    assert report["bundle_extractable"] is True
    assert report["runtime_status"] == "pass"
    assert report["primary_failure"] is None
    assert report["phase41_status"] == "pass"
    assert report["clock_sync_status"] == "pass"
    assert report["clock_offset_drift_valid"] is True
    assert report["clock_offset_sample_quality_valid"] is True
    assert report["snapshot_copy_budget_met"] is True
    assert report["strict_100ms_observability_ready"] is True
    assert report["low_latency_ready"] is True
    assert report["phase5_ready"] is False
    assert report["phase5_ready_false_interpretation"] == "acceptable_before_phase5_implementation"


def test_phase50_evidence_integrity_fails_on_corrupted_sha256(tmp_path: Path) -> None:
    sha_path = tmp_path / "bad_sha256.txt"
    sha_path.write_text("sha256: " + ("0" * 64) + "\n", encoding="utf-8")
    report, _ = verify_phase42h_evidence(ROOT, ROOT / "phase_4_2h_hotpath_environment_latency_bundle.zip", sha_path)
    assert report["status"] == "fail"
    assert report["bundle_sha256_valid"] is False


def test_phase50_evidence_integrity_fails_when_bundle_missing(tmp_path: Path) -> None:
    sha_path = tmp_path / "sha256.txt"
    sha_path.write_text("sha256: " + ("0" * 64) + "\n", encoding="utf-8")
    report, _ = verify_phase42h_evidence(ROOT, tmp_path / "missing.zip", sha_path)
    assert report["status"] == "fail"
    assert report["bundle_extractable"] is False


def test_phase50_evidence_integrity_fails_when_runtime_report_missing(tmp_path: Path) -> None:
    bundle = _bundle_without_member(tmp_path, PHASE42H_REPORT_MEMBER)
    sha_path = _sha_file_for_bundle(tmp_path, bundle)
    report, _ = verify_phase42h_evidence(ROOT, bundle, sha_path)
    assert report["status"] == "fail"
    assert report["runtime_status"] is None


def test_phase50_evidence_integrity_fails_when_strict_100ms_ready_false(tmp_path: Path) -> None:
    bundle = _mutated_runtime_bundle(tmp_path, {"strict_100ms_observability_ready": False})
    sha_path = _sha_file_for_bundle(tmp_path, bundle)
    report, _ = verify_phase42h_evidence(ROOT, bundle, sha_path)
    assert report["status"] == "fail"
    assert report["strict_100ms_observability_ready"] is False


def test_phase50_evidence_integrity_fails_when_low_latency_ready_false(tmp_path: Path) -> None:
    bundle = _mutated_runtime_bundle(tmp_path, {"low_latency_ready": False})
    sha_path = _sha_file_for_bundle(tmp_path, bundle)
    report, _ = verify_phase42h_evidence(ROOT, bundle, sha_path)
    assert report["status"] == "fail"
    assert report["low_latency_ready"] is False


def test_phase50_evidence_integrity_accepts_phase5_ready_false_explicitly() -> None:
    report = load_json("data/debug/phase_5_0_evidence_integrity_report.json")
    assert report["phase5_ready"] is False
    assert report["phase5_ready_false_interpretation"] == "acceptable_before_phase5_implementation"


def _mutated_runtime_bundle(tmp_path: Path, updates: dict) -> Path:
    source = ROOT / "phase_4_2h_hotpath_environment_latency_bundle.zip"
    target = tmp_path / "mutated_phase42h_bundle.zip"
    with zipfile.ZipFile(source) as incoming, zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as outgoing:
        for name in incoming.namelist():
            data = incoming.read(name)
            if name == PHASE42H_REPORT_MEMBER:
                payload = json.loads(data)
                payload.update(updates)
                data = json.dumps(payload).encode("utf-8")
            outgoing.writestr(name, data)
    return target


def _bundle_without_member(tmp_path: Path, omitted_member: str) -> Path:
    source = ROOT / "phase_4_2h_hotpath_environment_latency_bundle.zip"
    target = tmp_path / "missing_runtime_report.zip"
    with zipfile.ZipFile(source) as incoming, zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as outgoing:
        for name in incoming.namelist():
            if name != omitted_member:
                outgoing.writestr(name, incoming.read(name))
    return target


def _sha_file_for_bundle(tmp_path: Path, bundle: Path) -> Path:
    digest = hashlib.sha256(bundle.read_bytes()).hexdigest()
    sha_path = tmp_path / f"{bundle.stem}_sha256.txt"
    sha_path.write_text(f"sha256: {digest}\n", encoding="utf-8")
    return sha_path
