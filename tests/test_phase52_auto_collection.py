from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

import app.research.hotpath_environment_latency as hotpath
import app.research.phase52_auto_collection as phase52
from app.research.phase52_auto_collection import (
    ALL_SESSIONS_BUNDLE,
    ALL_SESSIONS_SHA256,
    MANIFEST_PATH,
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


def test_phase52_all_real_sessions_use_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    _mock_phase42h_subprocess(monkeypatch, commands=commands, report_status="pass")
    for item in default_session_plan():
        session_dir = tmp_path / "data/phase_5_2/sessions" / str(item["session_id"])
        phase52._run_real_phase42h_capture(root_path=tmp_path, session_dir=session_dir, requested_duration_sec=float(item["requested_duration_sec"]))
    assert len(commands) == len(default_session_plan())
    assert all("--clean" in command for command in commands)


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
        return SimpleNamespace(returncode=0, stdout="mock phase42h\n", stderr="")

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


def _mock_phase42h_subprocess(monkeypatch: pytest.MonkeyPatch, *, commands: list[list[str]], report_status: str) -> None:
    def fake_run(command: list[str], **kwargs) -> SimpleNamespace:
        if command and command[0] == "git":
            if command[1:3] == ["rev-parse", "HEAD"]:
                return SimpleNamespace(returncode=0, stdout="test-commit\n", stderr="")
            if command[1:2] == ["status"]:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
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
        return SimpleNamespace(returncode=0 if report["status"] == "pass" else 1, stdout="mock phase42h\n", stderr="")

    monkeypatch.setattr("app.research.phase52_auto_collection.subprocess.run", fake_run)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
