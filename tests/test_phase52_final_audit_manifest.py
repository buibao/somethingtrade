from __future__ import annotations

import hashlib
import json
from pathlib import Path

import scripts.build_phase52_final_audit_manifest as final_audit


def test_raw_failed_session_remains_failed_even_when_repaired_eval_pass_exists(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_raw_session(sessions_root, "session_001_sanity_30m", status="pass")
    _write_raw_session(sessions_root, "session_005_medium_2h", status="fail", primary_failure="HOT_PATH_DECOUPLING_INCOMPLETE")
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, expected = _write_evidence(tmp_path)

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256=expected,
    )

    failed_ids = {session["session_id"] for session in manifest["failed_raw_sessions"]}
    assert "session_005_medium_2h" in failed_ids
    assert manifest["session_counts"]["raw_fail"] == 1
    assert all(session["session_id"] != "session_005_medium_2h" for session in manifest["runtime_pass_sessions"])


def test_repaired_eval_pass_counts_as_audit_only_not_raw_runtime_pass(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, expected = _write_evidence(tmp_path)

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256=expected,
    )

    assert manifest["session_counts"]["raw_pass"] == 0
    assert manifest["session_counts"]["repaired_eval_pass"] == 1
    assert manifest["research_usable_sessions"][0]["audit_only"] is True
    assert manifest["research_usable_sessions"][0]["usability_mode"] == "audit_only_repaired_eval"


def test_final_status_partial_pass_with_four_raw_passes_and_repaired_eval(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    for index in range(1, 5):
        _write_raw_session(sessions_root, f"session_{index:03d}_sanity_30m", status="pass")
    _write_raw_session(sessions_root, "session_005_medium_2h", status="fail", primary_failure="LATENCY_PROFILE_MISSING")
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, expected = _write_evidence(tmp_path)

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256=expected,
    )

    assert manifest["status"] == "partial_pass_with_repaired_eval_evidence"
    assert manifest["readiness_decision"]["usable_for_runtime_pipeline_audit"] is True
    assert manifest["readiness_decision"]["sufficient_for_long_run_stability_claim"] is False


def test_phase52f_never_claims_model_training_or_phase5_ready(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    for index in range(1, 5):
        _write_raw_session(sessions_root, f"session_{index:03d}_sanity_30m", status="pass")
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, expected = _write_evidence(tmp_path)

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256=expected,
    )

    assert manifest["readiness_decision"]["sufficient_for_model_training"] is False
    assert manifest["readiness_decision"]["sufficient_for_strategy_backtest"] is False
    assert manifest["readiness_decision"]["sufficient_for_execution_or_pnl"] is False
    assert manifest["phase_boundary"]["phase5_ready"] is False
    assert manifest["phase_boundary"]["model_strategy_execution_pnl_scope"] is False


def test_evidence_zip_sha256_mismatch_warns_and_marks_invalid(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    for index in range(1, 5):
        _write_raw_session(sessions_root, f"session_{index:03d}_sanity_30m", status="pass")
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, _expected = _write_evidence(tmp_path)

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256="0" * 64,
    )

    assert manifest["status"] == "partial_collection_incomplete"
    assert manifest["readiness_decision"]["usable_for_runtime_pipeline_audit"] is False
    assert manifest["evidence"]["evidence_zip_valid"] is False
    assert "session_005 repaired eval evidence zip sha256 mismatch or missing" in manifest["warnings"]


def test_missing_evidence_zip_blocks_positive_final_status(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    for index in range(1, 5):
        _write_raw_session(sessions_root, f"session_{index:03d}_sanity_30m", status="pass")
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    missing_evidence = tmp_path / "phase_5_2_session_005_repaired_eval_final_evidence.zip"

    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=missing_evidence,
        expected_evidence_sha256="0" * 64,
    )

    assert manifest["status"] == "partial_collection_incomplete"
    assert manifest["readiness_decision"]["usable_for_runtime_pipeline_audit"] is False
    assert manifest["evidence"]["evidence_zip_valid"] is False
    assert manifest["evidence"]["missing_expected_evidence_files"]


def test_large_jsonl_sha256_is_skipped_without_full_scan(tmp_path: Path) -> None:
    large = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m/data/dataset/large.jsonl"
    large.parent.mkdir(parents=True, exist_ok=True)
    with large.open("wb") as handle:
        handle.truncate(final_audit.LARGE_FILE_THRESHOLD_BYTES + 1)

    metadata = final_audit.artifact_metadata(large, role="session_artifact")

    assert metadata["size_bytes"] == final_audit.LARGE_FILE_THRESHOLD_BYTES + 1
    assert metadata["sha256"] is None
    assert metadata["sha256_status"] == "skipped_large_file"
    assert metadata["first_nonempty_line_status"] == "skipped_large_file"


def test_script_does_not_modify_existing_session_report_mtimes(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    session_dir = _write_raw_session(sessions_root, "session_001_sanity_30m", status="pass")
    report_path = session_dir / final_audit.PHASE42H_REPORT
    before = report_path.stat().st_mtime_ns
    evidence, expected = _write_evidence(tmp_path)

    exit_code = final_audit.main(
        [
            "--sessions-root",
            str(sessions_root),
            "--evidence-zip",
            str(evidence),
            "--expected-evidence-sha256",
            expected,
            "--output-json",
            str(tmp_path / "data/reports/phase_5_2_final_audit_manifest.json"),
            "--output-md",
            str(tmp_path / "data/reports/phase_5_2_final_audit_manifest.md"),
            "--inventory-json",
            str(tmp_path / "data/debug/phase_5_2_final_audit_artifact_inventory.json"),
        ]
    )

    assert exit_code == 0
    assert report_path.stat().st_mtime_ns == before


def test_script_does_not_create_cache_tmp_sqlite_files(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_raw_session(sessions_root, "session_001_sanity_30m", status="pass")
    evidence, expected = _write_evidence(tmp_path)

    final_audit.main(
        [
            "--sessions-root",
            str(sessions_root),
            "--evidence-zip",
            str(evidence),
            "--expected-evidence-sha256",
            expected,
            "--output-json",
            str(tmp_path / "data/reports/phase_5_2_final_audit_manifest.json"),
            "--output-md",
            str(tmp_path / "data/reports/phase_5_2_final_audit_manifest.md"),
            "--inventory-json",
            str(tmp_path / "data/debug/phase_5_2_final_audit_artifact_inventory.json"),
        ]
    )

    cache_root = tmp_path / "data/cache"
    assert list(cache_root.glob("tmp*/phase42h_*.sqlite")) == []


def test_markdown_contains_do_not_claim_section(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_repaired_session(sessions_root, "session_005_medium_2h_repaired_eval")
    evidence, expected = _write_evidence(tmp_path)
    manifest, _inventory = final_audit.build_phase52_final_audit_manifest(
        sessions_root=sessions_root,
        evidence_zip=evidence,
        expected_evidence_sha256=expected,
    )

    markdown = final_audit.render_phase52_final_audit_markdown(manifest)

    assert "## Do Not Claim" in markdown
    assert "Do not claim all Phase 5.2 sessions passed." in markdown
    assert "Do not claim trading edge." in markdown


def _write_raw_session(
    sessions_root: Path,
    session_id: str,
    *,
    status: str,
    primary_failure: str | None = None,
) -> Path:
    return _write_session(
        sessions_root,
        session_id,
        status=status,
        primary_failure=primary_failure,
        evaluation_mode="fresh_capture",
        derived_artifact_mode=None,
        rebuild_derived_artifacts=None,
        fresh_capture_required=True,
        fresh_capture_performed=status == "pass",
        skip_capture=False,
        fixture_mode=False,
    )


def _write_repaired_session(sessions_root: Path, session_id: str) -> Path:
    return _write_session(
        sessions_root,
        session_id,
        status="pass",
        primary_failure=None,
        evaluation_mode="existing_artifacts",
        derived_artifact_mode="reuse_existing",
        rebuild_derived_artifacts=False,
        fresh_capture_required=False,
        fresh_capture_performed=False,
        skip_capture=True,
        fixture_mode=False,
    )


def _write_session(
    sessions_root: Path,
    session_id: str,
    *,
    status: str,
    primary_failure: str | None,
    evaluation_mode: str,
    derived_artifact_mode: str | None,
    rebuild_derived_artifacts: bool | None,
    fresh_capture_required: bool,
    fresh_capture_performed: bool,
    skip_capture: bool,
    fixture_mode: bool,
) -> Path:
    session_dir = sessions_root / session_id
    report_path = session_dir / final_audit.PHASE42H_REPORT
    self_check_path = session_dir / final_audit.PHASE42H_SELF_CHECK
    stage_profile_path = session_dir / final_audit.PHASE42H_STAGE_PROFILE
    report_path.parent.mkdir(parents=True, exist_ok=True)
    self_check_path.parent.mkdir(parents=True, exist_ok=True)
    stage_profile_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "phase": "4.2H",
        "status": status,
        "primary_failure": primary_failure,
        "hard_fail_reasons": [primary_failure] if primary_failure else [],
        "failure_classifications": [primary_failure] if primary_failure else [],
        "evaluation_mode": evaluation_mode,
        "derived_artifact_mode": derived_artifact_mode,
        "rebuild_derived_artifacts": rebuild_derived_artifacts,
        "fresh_capture_required": fresh_capture_required,
        "fresh_capture_performed": fresh_capture_performed,
        "skip_capture": skip_capture,
        "fixture_mode": fixture_mode,
        "duration_sec": 1800.0,
        "latency_profile_status": "pass" if status == "pass" else "fail",
        "hot_path_decoupling_status": "pass" if status == "pass" else "fail",
        "implementation_status": "pass" if status == "pass" else "fail",
        "strict_100ms_observability_ready": status == "pass",
        "low_latency_ready": status == "pass",
        "clock_sync_status": "pass" if status == "pass" else "fail",
        "phase41_runtime_report_status": "pass" if status == "pass" else "fail",
        "phase41_runtime_ready": status == "pass",
        "phase5_ready": False,
        "max_future_gap_ms": 100,
        "warning_reasons": [],
        "latency_stage_profile_artifact": {"valid": status == "pass"},
        "streaming_finalization": {"skipped": evaluation_mode == "existing_artifacts"},
        "queue_backpressure_artifact_normalization": {"performed": evaluation_mode == "existing_artifacts"},
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    self_check_path.write_text(json.dumps({"passed": status == "pass"}), encoding="utf-8")
    stage_profile_path.write_text(json.dumps({"performed": status == "pass"}), encoding="utf-8")
    return session_dir


def _write_evidence(tmp_path: Path) -> tuple[Path, str]:
    evidence = tmp_path / "phase_5_2_session_005_repaired_eval_final_evidence.zip"
    evidence.write_bytes(b"phase52 repaired eval evidence")
    digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
    Path(str(evidence) + ".sha256").write_text(f"{digest}  {evidence.name}\n", encoding="utf-8")
    return evidence, digest
