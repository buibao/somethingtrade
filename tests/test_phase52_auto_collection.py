from __future__ import annotations

import json
from pathlib import Path
import zipfile
from types import SimpleNamespace
import time
from typing import Any

import pytest

import app.research.hotpath_environment_latency as hotpath
import app.research.phase52_auto_collection as phase52
from app.research.phase52_auto_collection import (
    ALL_SESSIONS_BUNDLE,
    ALL_SESSIONS_SHA256,
    AUDIT_BUNDLE,
    FILE_SIZE_MANIFEST,
    FULL_DATASET_BUNDLE,
    MANIFEST_PATH,
    REPORT_JSON_PATH,
    STATUS_PATH,
    build_auto_collection_manifest,
    default_session_plan,
    evaluate_session_quality,
    parse_sha256_file,
    plan_totals,
    run_auto_collection,
    run_controlled_capture,
    synthetic_phase42h_runtime_report,
)


def test_phase52_auto_default_plan_has_multiple_sessions() -> None:
    plan = default_session_plan()
    assert len(plan) == 10
    assert plan[0]["session_id"] == "session_001_sanity_30m"
    assert plan[-1]["session_id"] == "session_010_final_3h"


def test_phase52_real_capture_command_includes_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    session_dir = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m"

    _mock_phase42h_subprocess(monkeypatch, commands=commands, report_status="pass")
    phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=session_dir, requested_duration_sec=1800)

    command = commands[0]
    assert "--clean" in command
    assert "--root" in command
    assert command[command.index("--root") + 1] == str(session_dir)
    assert "--duration-sec" in command
    assert command[command.index("--duration-sec") + 1] == "1800"
    assert "--skip-pytest" in command


def test_phase52_real_capture_command_still_includes_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    test_phase52_real_capture_command_includes_clean(tmp_path, monkeypatch)


def test_phase52_all_real_sessions_use_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    _mock_phase42h_subprocess(monkeypatch, commands=commands, report_status="pass")
    for item in default_session_plan():
        session_dir = tmp_path / "data/phase_5_2/sessions" / str(item["session_id"])
        phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=session_dir, requested_duration_sec=float(item["requested_duration_sec"]))
    assert len(commands) == len(default_session_plan())
    assert all("--clean" in command for command in commands)


def test_phase52_real_capture_does_not_use_capture_output_true(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        observed.update(kwargs)
        session_dir = Path(list(command)[list(command).index("--root") + 1])
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(synthetic_phase42h_runtime_report(requested_duration_sec=1)), encoding="utf-8")
        stdout_handle = kwargs.get("stdout")
        if stdout_handle is not None:
            stdout_handle.write("streamed stdout\n")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=tmp_path / "data/phase_5_2/sessions/s1", requested_duration_sec=1)
    assert observed.get("capture_output") is not True
    assert observed.get("stderr") is phase52.subprocess.STDOUT


def test_phase52_real_capture_streams_child_output_to_console_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        session_dir = Path(list(command)[list(command).index("--root") + 1])
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(synthetic_phase42h_runtime_report(requested_duration_sec=1)), encoding="utf-8")
        kwargs["stdout"].write("child stdout line\n")
        kwargs["stdout"].write("child stderr line redirected\n")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    session_dir = tmp_path / "data/phase_5_2/sessions/s1"
    phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=session_dir, requested_duration_sec=1)
    console = (session_dir / "phase_5_2_s1_console.log").read_text(encoding="utf-8")
    assert "child stdout line" in console
    assert "child stderr line redirected" in console
    assert "child_exit_code=0" in console


def test_phase52_long_child_output_does_not_accumulate_in_memory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        session_dir = Path(list(command)[list(command).index("--root") + 1])
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(synthetic_phase42h_runtime_report(requested_duration_sec=1)), encoding="utf-8")
        assert kwargs.get("capture_output") is not True
        assert kwargs.get("stdout") is not None
        kwargs["stdout"].write("x" * 1_000_000)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    exit_code, _report, output = phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=tmp_path / "data/phase_5_2/sessions/s1", requested_duration_sec=1)
    assert exit_code == 0
    assert len(output) < 300


def test_phase52_exit_code_preserved_when_streaming_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        session_dir = Path(list(command)[list(command).index("--root") + 1])
        report = synthetic_phase42h_runtime_report(requested_duration_sec=1)
        report["status"] = "fail"
        report["primary_failure"] = "PROCESS_FAILED"
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report), encoding="utf-8")
        kwargs["stdout"].write("failed child output\n")
        return SimpleNamespace(returncode=7, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    exit_code, report, _output = phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=tmp_path / "data/phase_5_2/sessions/s1", requested_duration_sec=1)
    assert exit_code == 7
    assert report["exit_code"] == 7


def test_phase52_oom_classification_still_works_after_streaming_output_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        kwargs["stdout"].write("killed child output\n")
        return SimpleNamespace(returncode=-9, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    monkeypatch.setattr(
        phase52,
        "_collect_kernel_log_text",
        lambda **_kwargs: "Out of memory: Killed process 22259 (python)\n",
    )
    exit_code, report, _output = phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=tmp_path / "data/phase_5_2/sessions/s1", requested_duration_sec=1)
    assert exit_code == -9
    assert report["primary_failure"] == "OOM_KILLED"


def test_phase52_resume_retry_still_uses_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    _seed_manifest(tmp_path, retry_count=0, research_eligible=False, status="fail")
    _mock_phase42h_subprocess(monkeypatch, commands=commands, report_status="pass")

    run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=1,
        strict_100ms=True,
        resume=True,
        max_session_retries=1,
        cooldown_sec=0,
    )

    assert commands
    assert all("--clean" in command for command in commands)


def test_phase52_auto_plan_fits_24h_budget() -> None:
    totals = plan_totals(default_session_plan())
    assert totals["total_wall_clock_hours"] <= 24.0
    assert totals["total_requested_capture_sec"] == 77400


def test_phase52_auto_manifest_contains_required_fields(tmp_path: Path) -> None:
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True)
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    for field in (
        "plan_name",
        "started_at_utc",
        "ended_at_utc",
        "total_budget_hours",
        "total_requested_capture_sec",
        "total_actual_capture_sec",
        "session_count",
        "passed_session_count",
        "failed_session_count",
        "research_eligible_session_count",
        "sessions",
        "stopped_early",
        "stop_reason",
        "next_phase_recommendation",
    ):
        assert field in manifest


def test_phase52_auto_status_contains_current_session(tmp_path: Path) -> None:
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True)
    status = _read_json(tmp_path / STATUS_PATH)
    assert "current_session" in status
    assert status["completed_session_count"] == 1


def test_phase52_auto_resume_skips_completed_sessions(tmp_path: Path) -> None:
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True)
    bundle = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m/phase_5_2_session_001_sanity_30m_capture_bundle.zip"
    first_mtime = bundle.stat().st_mtime
    time.sleep(0.01)
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True, resume=True)
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    assert manifest["sessions"][0]["resume_action"] == "skipped_completed_research_eligible"
    assert bundle.stat().st_mtime == first_mtime


def test_phase52_auto_resume_retries_failed_sessions_with_limit(tmp_path: Path) -> None:
    _seed_manifest(tmp_path, retry_count=0, research_eligible=False, status="fail")
    failed_bundle = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m/phase_5_2_session_001_sanity_30m_capture_bundle.zip"
    failed_bundle.parent.mkdir(parents=True, exist_ok=True)
    failed_bundle.write_bytes(b"failed attempt artifact")
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True, resume=True, max_session_retries=1)
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    assert manifest["sessions"][0]["retry_count"] == 1
    assert manifest["sessions"][0]["research_eligible"] is True
    preserved = tmp_path / "data/phase_5_2/failed_audit/session_001_sanity_30m/attempt_001/phase_5_2_session_001_sanity_30m_capture_bundle.zip"
    assert preserved.read_bytes() == b"failed attempt artifact"

    _seed_manifest(tmp_path, retry_count=1, research_eligible=False, status="fail")
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True, resume=True, max_session_retries=1)
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    assert manifest["sessions"][0]["resume_action"] == "retry_limit_reached"


def test_phase52_auto_does_not_overwrite_completed_bundles(tmp_path: Path) -> None:
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True)
    bundle = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m/phase_5_2_session_001_sanity_30m_capture_bundle.zip"
    original = bundle.read_bytes()
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True, resume=True)
    assert bundle.read_bytes() == original


def test_phase52_stop_after_current_session_file_stops_gracefully(tmp_path: Path) -> None:
    stop_file = tmp_path / "stop_after_current"
    stop_file.write_text("stop", encoding="utf-8")
    run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        dry_run=True,
        create_bundles=True,
        test_max_sessions=3,
        strict_100ms=True,
        stop_after_current_session_file=stop_file,
    )
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    assert manifest["stopped_early"] is True
    assert manifest["session_count"] == 1
    assert "stop-after-current-session" in manifest["stop_reason"]


def test_phase52_report_status_pass_when_graceful_stop_after_current_session_with_no_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stop_file = tmp_path / "stop_after_current"
    original_capture = phase52.run_controlled_capture
    calls = {"count": 0}

    def stop_after_third_session(**kwargs: Any) -> dict[str, Any]:
        result = original_capture(**kwargs)
        calls["count"] += 1
        if calls["count"] == 3:
            stop_file.write_text("stop", encoding="utf-8")
        return result

    monkeypatch.setattr(phase52, "run_controlled_capture", stop_after_third_session)
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        dry_run=True,
        create_bundles=True,
        test_max_sessions=5,
        strict_100ms=True,
        stop_after_current_session_file=stop_file,
    )
    manifest = report["manifest"]
    assert manifest["session_count"] == 3
    assert manifest["passed_session_count"] == 3
    assert manifest["failed_session_count"] == 0
    assert manifest["research_eligible_session_count"] == 3
    assert manifest["stopped_early"] is True
    assert "stop-after-current-session file observed" in manifest["stop_reason"]
    assert report["status"] == "pass"
    assert report["aggregate_status"] == "stopped_early"
    assert report["collection_status"] == "stopped_early"
    assert report["last_failure"] is None


def test_phase52_report_status_fail_when_stopped_early_due_to_failed_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["pass", "oom"])
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=3,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
        cooldown_sec=0,
    )
    status = _read_json(tmp_path / STATUS_PATH)
    assert report["status"] == "fail"
    assert report["last_failure"] == "OOM_KILLED"
    assert report["manifest"]["failed_session_count"] == 1
    assert report["manifest"]["stopped_early"] is True
    assert report["collection_status"] == "partial_fail_stopped_early"
    assert status["last_failure"] == "OOM_KILLED"


def test_phase52_status_manifest_report_consistent_for_graceful_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stop_file = tmp_path / "stop_after_current"
    original_capture = phase52.run_controlled_capture
    calls = {"count": 0}

    def stop_after_third_session(**kwargs: Any) -> dict[str, Any]:
        result = original_capture(**kwargs)
        calls["count"] += 1
        if calls["count"] == 3:
            stop_file.write_text("stop", encoding="utf-8")
        return result

    monkeypatch.setattr(phase52, "run_controlled_capture", stop_after_third_session)
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        dry_run=True,
        create_bundles=True,
        test_max_sessions=5,
        strict_100ms=True,
        stop_after_current_session_file=stop_file,
    )
    manifest = _read_json(tmp_path / MANIFEST_PATH)
    status = _read_json(tmp_path / STATUS_PATH)
    report_json = _read_json(tmp_path / REPORT_JSON_PATH)

    assert report["status"] == report_json["status"] == "pass"
    assert report_json["collection_status"] == "stopped_early"
    assert manifest["stopped_early"] is True
    assert status["stopped_early"] is True
    assert status["stop_reason"] == manifest["stop_reason"]
    assert "stop-after-current-session file observed" in manifest["stop_reason"]
    for field in ("completed_session_count", "passed_session_count", "failed_session_count", "research_eligible_session_count"):
        manifest_field = "session_count" if field == "completed_session_count" else field
        assert status[field] == manifest[manifest_field]
    assert status["last_failure"] is None
    assert report_json["last_failure"] is None


def test_phase52_session_quality_gate_accepts_valid_session() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1), bundle_sha_valid=True)
    assert quality["research_eligible"] is True


def test_phase52_quality_requires_hotpath_status_pass() -> None:
    report = synthetic_phase42h_runtime_report(requested_duration_sec=1)
    report["status"] = "fail"
    quality = evaluate_session_quality(report, bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "status_pass" in quality["failure_reasons"]


def test_phase52_quality_requires_primary_failure_none() -> None:
    report = synthetic_phase42h_runtime_report(requested_duration_sec=1)
    report["primary_failure"] = "ARTIFACT_CLEANUP_FAILURE"
    quality = evaluate_session_quality(report, bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "primary_failure_none" in quality["failure_reasons"]


def test_phase52_session_quality_gate_rejects_strict_100ms_false() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1, simulate_failure="strict_100ms_false"), bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "strict_100ms_observability_ready" in quality["failure_reasons"]


def test_phase52_session_quality_gate_rejects_low_latency_false() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1, simulate_failure="low_latency_false"), bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "low_latency_ready" in quality["failure_reasons"]


def test_phase52_session_quality_gate_rejects_clock_sync_fail() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1, simulate_failure="clock_sync_fail"), bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "clock_sync_status_pass" in quality["failure_reasons"]


def test_phase52_quality_requires_phase41_pass() -> None:
    report = synthetic_phase42h_runtime_report(requested_duration_sec=1)
    report["phase41_runtime_report"]["phase_4_1_status"] = "fail"
    quality = evaluate_session_quality(report, bundle_sha_valid=True)
    assert quality["research_eligible"] is False
    assert "phase41_status_pass" in quality["failure_reasons"]


def test_phase52_quality_requires_bundle_sha_valid() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1), bundle_sha_valid=False)
    assert quality["research_eligible"] is False
    assert "bundle_sha256_valid" in quality["failure_reasons"]


def test_phase52_quality_passes_only_when_all_gates_pass() -> None:
    quality = evaluate_session_quality(synthetic_phase42h_runtime_report(requested_duration_sec=1), bundle_sha_valid=True)
    assert quality["status"] == "pass"
    assert quality["research_eligible"] is True
    assert quality["failure_reasons"] == []


def test_phase52_failed_session_not_research_eligible(tmp_path: Path) -> None:
    result = run_controlled_capture(
        root=tmp_path,
        session_id="failed_session",
        plan_name="test_plan",
        requested_duration_sec=1,
        dry_run=True,
        create_bundle=True,
        simulate_failure="primary_failure",
    )
    assert result["status"] == "fail"
    assert result["research_eligible"] is False


def test_phase52_bundle_sha256_matches(tmp_path: Path) -> None:
    result = run_controlled_capture(root=tmp_path, session_id="s1", plan_name="test_plan", requested_duration_sec=1, dry_run=True, create_bundle=True)
    bundle = tmp_path / result["artifact_paths"]["bundle"]
    sha_path = tmp_path / result["artifact_paths"]["sha256"]
    assert parse_sha256_file(sha_path) == _sha256(bundle)


def test_phase52_all_sessions_bundle_created(tmp_path: Path) -> None:
    run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=2, strict_100ms=True)
    assert (tmp_path / ALL_SESSIONS_BUNDLE).exists()
    assert (tmp_path / ALL_SESSIONS_SHA256).exists()
    assert parse_sha256_file(tmp_path / ALL_SESSIONS_SHA256) == _sha256(tmp_path / ALL_SESSIONS_BUNDLE)
    assert (tmp_path / AUDIT_BUNDLE).exists()


def test_phase52_default_audit_bundle_excludes_jsonl(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    dataset = collection_root / "sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert not any(name.endswith(".jsonl") for name in archive.namelist())


def test_phase52_default_audit_bundle_excludes_nested_zip(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    nested = collection_root / "sessions/session_001_sanity_30m/phase_4_2h_latency_profile_datasets.zip"
    nested.write_bytes(b"nested zip placeholder")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert not any(name.endswith(".zip") for name in archive.namelist())


def test_phase52_default_audit_bundle_includes_reports_logs_metadata(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        names = set(archive.namelist())
    assert "file_size_manifest.json" in names
    assert any(name.endswith("_metadata.json") for name in names)
    assert any(name.endswith("_quality_report.json") for name in names)
    assert any(name.endswith("_console.log") for name in names)
    assert any(name.endswith("phase_4_2h_hotpath_environment_latency_report.json") for name in names)
    assert "data/debug/phase_5_2_auto_collection_status.json" in names


def test_phase52_full_dataset_bundle_requires_explicit_flag(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    dataset = collection_root / "sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")

    phase52.create_all_sessions_bundle(tmp_path, collection_root, include_large_datasets=False)
    assert not (tmp_path / FULL_DATASET_BUNDLE).exists()

    phase52.create_all_sessions_bundle(tmp_path, collection_root, include_large_datasets=True)
    with zipfile.ZipFile(tmp_path / FULL_DATASET_BUNDLE) as archive:
        assert "data/phase_5_2/sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl" in archive.namelist()


def test_phase52_bundle_file_size_manifest_present(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    phase52.create_all_sessions_bundle(tmp_path, collection_root)
    assert (tmp_path / FILE_SIZE_MANIFEST).exists()
    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert "file_size_manifest.json" in archive.namelist()


def test_phase52_file_size_manifest_marks_large_files_excluded_from_audit_bundle(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    dataset = collection_root / "sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    manifest = _read_json(tmp_path / FILE_SIZE_MANIFEST)
    item = next(file for file in manifest["files"] if file["path"].endswith("large_capture.jsonl"))
    assert item["artifact_type"] == "large_dataset"
    assert item["included_in_audit_bundle"] is False
    assert item["included_in_full_bundle"] is False


def test_phase52_no_zip_inside_zip_by_default(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    (collection_root / "sessions/session_001_sanity_30m/phase_4_2h_hotpath_environment_latency_bundle.zip").write_bytes(b"nested")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert not any(name.endswith(".zip") for name in archive.namelist())


def test_phase52_bundle_creation_streaming_no_read_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)

    def blocked_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"read_bytes should not be used while bundling {self}")

    monkeypatch.setattr(Path, "read_bytes", blocked_read_bytes)
    phase52.create_all_sessions_bundle(tmp_path, collection_root)
    assert (tmp_path / AUDIT_BUNDLE).exists()


def test_phase52_all_sessions_bundle_does_not_duplicate_session_dataset_files_by_default(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    dataset = collection_root / "sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert "data/phase_5_2/sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl" not in archive.namelist()


def test_phase42h_latency_profile_datasets_zip_not_included_in_default_audit_bundle(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    nested = collection_root / "sessions/session_001_sanity_30m/data/dataset/phase_4_2h_latency_profile_datasets.zip"
    nested.parent.mkdir(parents=True, exist_ok=True)
    nested.write_bytes(b"nested")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    with zipfile.ZipFile(tmp_path / AUDIT_BUNDLE) as archive:
        assert nested.relative_to(tmp_path).as_posix() not in archive.namelist()


def test_phase52_bundle_manifest_lists_omitted_large_files(tmp_path: Path) -> None:
    collection_root = _seed_phase52_bundle_inputs(tmp_path)
    dataset = collection_root / "sessions/session_001_sanity_30m/data/dataset/large_capture.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")

    phase52.create_all_sessions_bundle(tmp_path, collection_root)

    manifest = _read_json(tmp_path / FILE_SIZE_MANIFEST)
    assert any(file["path"].endswith("large_capture.jsonl") and file["included_in_audit_bundle"] is False for file in manifest["files"])


def test_phase52_no_live_trading_execution_wallet_logic_flags(tmp_path: Path) -> None:
    report = run_auto_collection(root=tmp_path, plan_name="test_plan", total_budget_hours=24, dry_run=True, create_bundles=True, test_max_sessions=1, strict_100ms=True)
    assert report["no_live_trading"] is True
    assert report["no_execution"] is True
    assert report["no_wallet_logic"] is True
    assert report["no_order_placement"] is True


def test_phase52_simulated_session_001_passes_after_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    _mock_phase42h_subprocess(monkeypatch, commands=commands, report_status="pass")
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_001_sanity_30m",
        plan_name="test_plan",
        requested_duration_sec=1800,
        dry_run=False,
        create_bundle=True,
    )
    assert "--clean" in commands[0]
    assert result["status"] == "pass"
    assert result["research_eligible"] is True


def test_phase52_simulated_session_001_uses_source_gitignore_not_session_gitignore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".gitignore").write_text(
        "\n".join(
            [
                "*.jsonl",
                "data/dataset/",
                "data/debug/",
                "data/cache/",
                "data/logs/",
                "data/reports/",
                "logs/",
                "reports/",
                "debug/",
                "cache/",
                "*.zip",
                "*.log",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_run(command: list[str], **kwargs) -> SimpleNamespace:
        command = list(command)
        if command[:2] == ["uname", "-p"]:
            return SimpleNamespace(returncode=0, stdout="x86_64\n", stderr="")
        if command and command[0] == "git":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        session_dir = Path(command[command.index("--root") + 1])
        assert not (session_dir / ".gitignore").exists()
        preflight = hotpath.run_phase42h_vps_preflight(session_dir, source_root=tmp_path, required_imports=("json",), check_network=False)
        report = synthetic_phase42h_runtime_report(requested_duration_sec=1800)
        report["preflight_report"] = preflight
        report["gitignore_validation"] = preflight["checks"]["gitignore_status"]["validation"]
        if preflight["checks"]["gitignore_status"]["passed"] is not True:
            report["status"] = "fail"
            report["primary_failure"] = "GITIGNORE_POLICY_FAILURE"
            report["failure_classifications"] = ["GITIGNORE_POLICY_FAILURE"]
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report), encoding="utf-8")
        (session_dir / "phase_4_2h_hotpath_environment_latency_bundle.zip").write_bytes(b"phase42h bundle")
        stdout_handle = kwargs.get("stdout")
        if stdout_handle is not None:
            stdout_handle.write("mock phase42h\n")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_001_sanity_30m",
        plan_name="test_plan",
        requested_duration_sec=1800,
        dry_run=False,
        create_bundle=True,
    )

    assert result["research_eligible"] is True
    assert "GITIGNORE_POLICY_FAILURE" not in result["metadata"]["failure_reasons"]


def test_phase52_simulated_session_001_fails_if_cleanup_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase42h_subprocess(monkeypatch, commands=[], report_status="cleanup_missing")
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_001_sanity_30m",
        plan_name="test_plan",
        requested_duration_sec=1800,
        dry_run=False,
        create_bundle=True,
    )
    assert result["status"] == "fail"
    assert result["research_eligible"] is False
    assert "status_pass" in result["quality_report"]["failure_reasons"]


def test_phase52_failed_session_does_not_continue_silently_when_fail_session_on_quality_gate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase42h_subprocess(monkeypatch, commands=[], report_status="cleanup_missing")
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=2,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
    )
    manifest = report["manifest"]
    assert manifest["session_count"] == 1
    assert manifest["stopped_early"] is True
    assert "quality gate failed" in manifest["stop_reason"]
    assert report["status"] == "fail"
    assert report["last_failure"] == "ARTIFACT_CLEANUP_FAILURE"


def test_exit_code_minus_9_with_oom_dmesg_maps_to_oom_killed() -> None:
    evidence = "[Thu May 28 01:10:00 2026] Out of memory: Killed process 22259 (python) total-vm:1 anon-rss:3714540kB\n"
    classification = phase52.classify_child_process_failure(
        exit_code=-9,
        kernel_log_text=evidence,
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        report_missing=True,
    )
    assert classification["primary_failure"] == "OOM_KILLED"
    assert "OOM_KILLED" in classification["failure_classifications"]
    assert classification["oom_evidence"]["killed_pid"] == 22259


def test_exit_code_minus_9_without_oom_maps_to_process_sigkill() -> None:
    classification = phase52.classify_child_process_failure(
        exit_code=-9,
        kernel_log_text="[Thu May 28 01:10:00 2026] python exited\n",
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        report_missing=True,
    )
    assert classification["primary_failure"] == "PROCESS_SIGKILL"


def test_missing_hotpath_report_after_oom_is_not_synthetic_failure() -> None:
    classification = phase52.classify_child_process_failure(
        exit_code=-9,
        kernel_log_text="[Thu May 28 01:10:00 2026] Out of memory: Killed process 22259 (python)\n",
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        report_missing=True,
    )
    report = phase52.phase42h_process_failure_report(
        requested_duration_sec=3600,
        exit_code=-9,
        classification=classification,
        started_at_utc="2026-05-28T01:00:00Z",
        ended_at_utc="2026-05-28T01:59:00Z",
    )
    assert report["primary_failure"] == "OOM_KILLED"
    assert report["primary_failure"] != "SYNTHETIC_FAILURE"


def test_missing_hotpath_report_without_process_kill_remains_report_missing() -> None:
    classification = phase52.classify_child_process_failure(
        exit_code=1,
        kernel_log_text="",
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        report_missing=True,
    )
    assert classification["primary_failure"] == "REPORT_MISSING"


def test_oom_evidence_window_uses_session_start_end_times() -> None:
    logs = "\n".join(
        [
            "[Thu May 28 00:00:00 2026] Out of memory: Killed process 1 (python)",
            "[Thu May 28 01:10:00 2026] Out of memory: Killed process 22259 (python)",
        ]
    )
    evidence = phase52.detect_oom_evidence(
        logs,
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        window_sec=60,
    )
    assert evidence["oom_detected"] is True
    assert evidence["matched_line_count"] == 1
    assert evidence["killed_pid"] == 22259


def test_oom_evidence_included_in_metadata(tmp_path: Path) -> None:
    classification = phase52.classify_child_process_failure(
        exit_code=-9,
        kernel_log_text="[Thu May 28 01:10:00 2026] Out of memory: Killed process 22259 (python)\n",
        session_started_at_utc="2026-05-28T01:00:00Z",
        session_ended_at_utc="2026-05-28T01:59:00Z",
        report_missing=True,
    )
    runtime_report = phase52.phase42h_process_failure_report(
        requested_duration_sec=3600,
        exit_code=-9,
        classification=classification,
        started_at_utc="2026-05-28T01:00:00Z",
        ended_at_utc="2026-05-28T01:59:00Z",
    )
    quality = evaluate_session_quality(runtime_report, bundle_sha_valid=False)
    metadata = phase52.build_session_metadata(
        root_path=tmp_path,
        session_id="session_002_short_1h",
        plan_name="test_plan",
        requested_duration_sec=3600,
        actual_duration_sec=60,
        started_at_utc="2026-05-28T01:00:00Z",
        ended_at_utc="2026-05-28T01:59:00Z",
        runtime_report=runtime_report,
        quality_report=quality,
        notes="",
    )
    assert metadata["exit_code"] == -9
    assert metadata["oom_detected"] is True
    assert metadata["oom_killed_pid"] == 22259
    assert "Out of memory" in metadata["oom_log_excerpt"]


def test_oom_evidence_included_in_collection_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["oom"])
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=1,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
    )
    session = report["manifest"]["sessions"][0]
    assert session["primary_failure"] == "OOM_KILLED"
    assert report["collection_status"] == "partial_fail_stopped_early"


def test_fail_session_on_quality_gate_stops_after_oom(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["pass", "oom"])
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=3,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
        cooldown_sec=0,
    )
    manifest = report["manifest"]
    assert manifest["session_count"] == 2
    assert manifest["sessions"][0]["research_eligible"] is True
    assert manifest["sessions"][1]["primary_failure"] == "OOM_KILLED"
    assert manifest["stopped_early"] is True


def test_phase52_prior_passed_session_remains_research_eligible_after_later_oom(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["pass", "oom"])
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=2,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
        cooldown_sec=0,
    )
    assert report["manifest"]["sessions"][0]["session_id"] == "session_001_sanity_30m"
    assert report["manifest"]["sessions"][0]["research_eligible"] is True
    assert report["manifest"]["sessions"][1]["primary_failure"] == "OOM_KILLED"


def test_phase52_collection_report_status_distinguishes_partial_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["pass", "oom"])
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        create_bundles=True,
        test_max_sessions=2,
        strict_100ms=True,
        fail_session_on_quality_gate=True,
        cooldown_sec=0,
    )
    assert report["status"] == "fail"
    assert report["collection_status"] == "partial_fail_stopped_early"


def test_phase52_failed_session_bundle_contains_console_metadata_preflight_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["oom"])
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_002_short_1h",
        plan_name="test_plan",
        requested_duration_sec=3600,
        dry_run=False,
        create_bundle=True,
    )
    for key in ("bundle", "console_log", "metadata", "quality_report"):
        assert (tmp_path / result["artifact_paths"][key]).exists()
    assert result["metadata"]["primary_failure"] == "OOM_KILLED"


def test_phase52_synthetic_failure_not_used_for_known_oom(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["oom"])
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_002_short_1h",
        plan_name="test_plan",
        requested_duration_sec=3600,
        dry_run=False,
        create_bundle=True,
    )
    assert result["metadata"]["primary_failure"] == "OOM_KILLED"
    assert result["metadata"]["primary_failure"] != "SYNTHETIC_FAILURE"


def test_real_run_sigkill_fallback_report_does_not_say_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["oom"])
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_002_short_1h",
        plan_name="test_plan",
        requested_duration_sec=3600,
        dry_run=False,
        create_bundle=True,
    )
    with zipfile.ZipFile(tmp_path / result["artifact_paths"]["bundle"]) as archive:
        markdown = archive.read("data/reports/phase_4_2h_hotpath_environment_latency_report.md").decode("utf-8")
        payload = json.loads(archive.read("data/reports/phase_4_2h_hotpath_environment_latency_report.json"))
    assert "Fallback Phase 4.2H report" in markdown
    assert "dry-run" not in markdown.lower()
    assert payload["dry_run"] is False


def test_real_run_missing_hotpath_fallback_contains_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_phase52_real_capture_sequence(monkeypatch, ["oom"])
    result = run_controlled_capture(
        root=tmp_path,
        session_id="session_002_short_1h",
        plan_name="test_plan",
        requested_duration_sec=3600,
        dry_run=False,
        create_bundle=True,
    )
    with zipfile.ZipFile(tmp_path / result["artifact_paths"]["bundle"]) as archive:
        payload = json.loads(archive.read("data/reports/phase_4_2h_hotpath_environment_latency_report.json"))
    assert payload["child_exit_code"] == -9
    assert payload["fallback_reason"] == "child_exited_before_hotpath_report"


def test_dry_run_fallback_report_only_for_dry_run(tmp_path: Path) -> None:
    result = run_controlled_capture(
        root=tmp_path,
        session_id="dry_run_session",
        plan_name="test_plan",
        requested_duration_sec=1,
        dry_run=True,
        create_bundle=True,
    )
    with zipfile.ZipFile(tmp_path / result["artifact_paths"]["bundle"]) as archive:
        markdown = archive.read("data/reports/phase_4_2h_hotpath_environment_latency_report.md").decode("utf-8")
    assert "dry-run" in markdown.lower()


def test_source_repo_dirty_ignores_runtime_artifacts_in_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    before_state = {
        "repo_commit": "abc123",
        "source_repo_dirty": False,
        "runtime_artifacts_present": False,
        "ignored_runtime_artifacts_present": False,
    }

    monkeypatch.setattr(
        phase52,
        "_git_source_state",
        lambda root_path: {
            "repo_commit": "abc123",
            "source_repo_dirty": False,
            "raw_status_lines": ["?? data/phase_5_2/sessions/session_002/artifact.zip"],
            "source_status_lines": [],
            "runtime_status_lines": ["?? data/phase_5_2/sessions/session_002/artifact.zip"],
            "runtime_artifacts_present": True,
            "ignored_runtime_artifacts_present": True,
        },
    )
    runtime_report = synthetic_phase42h_runtime_report(requested_duration_sec=1)
    quality = evaluate_session_quality(runtime_report, bundle_sha_valid=True)
    metadata = phase52.build_session_metadata(
        root_path=tmp_path,
        session_id="session_001_sanity_30m",
        plan_name="test_plan",
        requested_duration_sec=1,
        actual_duration_sec=1,
        started_at_utc="2026-05-28T00:00:00Z",
        ended_at_utc="2026-05-28T00:00:01Z",
        runtime_report=runtime_report,
        quality_report=quality,
        notes="",
        source_repo_state_before=before_state,
    )
    assert metadata["source_repo_dirty"] is False
    assert metadata["source_repo_dirty_before_session"] is False
    assert metadata["source_repo_dirty_after_session"] is False
    assert metadata["runtime_artifacts_present"] is True
    assert metadata["ignored_runtime_artifacts_present"] is True


def test_runtime_artifact_status_lines_do_not_mark_source_dirty() -> None:
    assert phase52._is_runtime_artifact_status_line("?? data/phase_5_2/sessions/session_002/bundle.zip") is True
    assert phase52._is_runtime_artifact_status_line(" M bot/app/research/phase52_auto_collection.py") is False


def test_phase52_simulated_child_sigkill_oom_failure_classification() -> None:
    test_exit_code_minus_9_with_oom_dmesg_maps_to_oom_killed()


def test_phase52_simulated_child_sigkill_no_oom_failure_classification() -> None:
    test_exit_code_minus_9_without_oom_maps_to_process_sigkill()


def test_phase52_simulated_session_002_oom_preserves_session_001_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    test_phase52_prior_passed_session_remains_research_eligible_after_later_oom(tmp_path, monkeypatch)


def test_phase52_stop_after_current_session_file_honored(tmp_path: Path) -> None:
    stop_file = tmp_path / "stop_after_current"
    stop_file.write_text("stop", encoding="utf-8")
    report = run_auto_collection(
        root=tmp_path,
        plan_name="test_plan",
        total_budget_hours=24,
        dry_run=True,
        create_bundles=True,
        test_max_sessions=2,
        strict_100ms=True,
        stop_after_current_session_file=stop_file,
    )
    assert report["manifest"]["session_count"] == 1
    assert report["manifest"]["stopped_early"] is True


def _seed_manifest(tmp_path: Path, *, retry_count: int, research_eligible: bool, status: str) -> None:
    manifest = build_auto_collection_manifest(
        plan_name="test_plan",
        started_at_utc="2026-01-01T00:00:00Z",
        ended_at_utc="2026-01-01T00:00:01Z",
        total_budget_hours=24,
        plan=default_session_plan("test_plan")[:1],
        sessions=[
            {
                "session_id": "session_001_sanity_30m",
                "status": status,
                "research_eligible": research_eligible,
                "retry_count": retry_count,
                "artifact_paths": {"bundle": "old.zip"},
            }
        ],
        stopped_early=False,
        stop_reason="",
    )
    (tmp_path / MANIFEST_PATH).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / MANIFEST_PATH).write_text(json.dumps(manifest), encoding="utf-8")


def _seed_phase52_bundle_inputs(tmp_path: Path) -> Path:
    collection_root = tmp_path / "data/phase_5_2"
    session_dir = collection_root / "sessions/session_001_sanity_30m"
    (session_dir / "data/reports").mkdir(parents=True, exist_ok=True)
    (session_dir / "data/debug").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data/debug").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data/reports").mkdir(parents=True, exist_ok=True)
    (session_dir / "phase_5_2_session_001_sanity_30m_metadata.json").write_text("{}", encoding="utf-8")
    (session_dir / "phase_5_2_session_001_sanity_30m_quality_report.json").write_text("{}", encoding="utf-8")
    (session_dir / "phase_5_2_session_001_sanity_30m_console.log").write_text("console\n", encoding="utf-8")
    (session_dir / "phase_5_2_session_001_sanity_30m_sha256.txt").write_text("sha256: abc\n", encoding="utf-8")
    (session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").write_text("{}", encoding="utf-8")
    (session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.md").write_text("# report\n", encoding="utf-8")
    (session_dir / "data/debug/phase_4_2h_artifact_cleanup.json").write_text("{}", encoding="utf-8")
    (tmp_path / "data/debug/phase_5_2_auto_collection_status.json").write_text("{}", encoding="utf-8")
    (tmp_path / "data/reports/phase_5_2_auto_collection_report.json").write_text("{}", encoding="utf-8")
    return collection_root


def _mock_phase42h_subprocess(monkeypatch: pytest.MonkeyPatch, *, commands: list[list[str]], report_status: str) -> None:
    def fake_run(command: list[str], **kwargs) -> SimpleNamespace:
        command = list(command)
        if command[:2] == ["uname", "-p"]:
            return SimpleNamespace(returncode=0, stdout="x86_64\n", stderr="")
        if command and command[0] == "git":
            if command[1:3] == ["rev-parse", "HEAD"]:
                return SimpleNamespace(returncode=0, stdout="test-commit\n", stderr="")
            if command[1:2] == ["status"]:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
        if not any("run_phase42h_hotpath_environment_latency.py" in str(part) for part in command):
            raise AssertionError(f"Unexpected subprocess command in test mock: {command}")
        if "--root" not in command:
            raise AssertionError(f"Phase 4.2H test command missing --root: {command}")
        commands.append(command)
        session_dir = Path(command[command.index("--root") + 1])
        report = synthetic_phase42h_runtime_report(requested_duration_sec=float(command[command.index("--duration-sec") + 1]))
        if report_status == "cleanup_missing":
            report["status"] = "fail"
            report["primary_failure"] = "ARTIFACT_CLEANUP_FAILURE"
            report["failure_classifications"] = ["ARTIFACT_CLEANUP_FAILURE"]
            report["hard_fail_reasons"] = ["artifact cleanup was not performed"]
            report["cleanup_report"] = {"cleanup_performed": False, "errors": []}
        else:
            report["cleanup_report"] = {"cleanup_performed": True, "errors": []}
        report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report), encoding="utf-8")
        bundle = session_dir / "phase_4_2h_hotpath_environment_latency_bundle.zip"
        bundle.write_bytes(b"phase42h bundle")
        stdout_handle = kwargs.get("stdout")
        if stdout_handle is not None:
            stdout_handle.write("mock phase42h\n")
        return SimpleNamespace(returncode=0 if report["status"] == "pass" else 1, stdout="", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)


def _mock_phase52_real_capture_sequence(monkeypatch: pytest.MonkeyPatch, outcomes: list[str]) -> None:
    calls = {"index": 0}

    def fake_capture(*, root_path: Path, session_dir: Path, requested_duration_sec: float) -> tuple[int, dict[str, Any], str]:
        outcome = outcomes[min(calls["index"], len(outcomes) - 1)]
        calls["index"] += 1
        if outcome == "pass":
            report = synthetic_phase42h_runtime_report(requested_duration_sec=requested_duration_sec)
            report["cleanup_report"] = {"cleanup_performed": True, "errors": []}
            return 0, report, "mock phase42h pass\n"
        classification = phase52.classify_child_process_failure(
            exit_code=-9,
            kernel_log_text="[Thu May 28 01:10:00 2026] Out of memory: Killed process 22259 (python) anon-rss:3714540kB\n",
            session_started_at_utc="2026-05-28T01:00:00Z",
            session_ended_at_utc="2026-05-28T01:59:00Z",
            report_missing=True,
        )
        report = phase52.phase42h_process_failure_report(
            requested_duration_sec=requested_duration_sec,
            exit_code=-9,
            classification=classification,
            started_at_utc="2026-05-28T01:00:00Z",
            ended_at_utc="2026-05-28T01:59:00Z",
        )
        return -9, report, "mock phase42h killed\n"

    monkeypatch.setattr(phase52, "_run_real_phase42h_capture", fake_capture)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
