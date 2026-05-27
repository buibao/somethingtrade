from __future__ import annotations

import json
from pathlib import Path
import time

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


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
