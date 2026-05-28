from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
from typing import Any
import zipfile


PHASE = "5.2"
PRIMARY_HORIZON_MS = 100
DEFAULT_COLLECTION_ROOT = Path("data/phase_5_2")
MANIFEST_PATH = Path("data/debug/phase_5_2_auto_collection_manifest.json")
STATUS_PATH = Path("data/debug/phase_5_2_auto_collection_status.json")
REPORT_JSON_PATH = Path("data/reports/phase_5_2_auto_collection_report.json")
REPORT_MD_PATH = Path("data/reports/phase_5_2_auto_collection_report.md")
AUDIT_BUNDLE = Path("phase_5_2_audit_bundle.zip")
AUDIT_SHA256 = Path("phase_5_2_audit_bundle_sha256.txt")
FULL_DATASET_BUNDLE = Path("phase_5_2_full_dataset_bundle.zip")
FULL_DATASET_SHA256 = Path("phase_5_2_full_dataset_bundle_sha256.txt")
FILE_SIZE_MANIFEST = Path("data/debug/phase_5_2_file_size_manifest.json")
ALL_SESSIONS_BUNDLE = AUDIT_BUNDLE
ALL_SESSIONS_SHA256 = AUDIT_SHA256
LEGACY_ALL_SESSIONS_BUNDLE = Path("phase_5_2_auto_collection_all_sessions_bundle.zip")
LEGACY_ALL_SESSIONS_SHA256 = Path("phase_5_2_auto_collection_all_sessions_sha256.txt")
STOP_FILE_DEFAULT = Path("data/debug/phase_5_2_stop_after_current_session")

REQUIRED_SESSION_METADATA_FIELDS = (
    "phase",
    "session_id",
    "plan_name",
    "repo_commit",
    "source_repo_dirty",
    "source_repo_dirty_before_session",
    "source_repo_dirty_after_session",
    "runtime_artifacts_present",
    "ignored_runtime_artifacts_present",
    "started_at_utc",
    "ended_at_utc",
    "requested_duration_sec",
    "actual_duration_sec",
    "host_info",
    "os_info",
    "python_version",
    "vps_provider",
    "vps_region",
    "runtime_status",
    "primary_failure",
    "failure_reasons",
    "clock_sync_status",
    "accepted_clock_sample_count",
    "discarded_clock_sample_count",
    "snapshot_copy_p99_us",
    "end_to_end_hot_path_p99_ms",
    "strict_100ms_observability_ready",
    "low_latency_ready",
    "research_eligible",
    "notes",
)

OOM_CLASSIFICATION = "OOM_KILLED"
PROCESS_SIGKILL_CLASSIFICATION = "PROCESS_SIGKILL"
REPORT_MISSING_CLASSIFICATION = "REPORT_MISSING"
KERNEL_OOM_PATTERNS = (
    "Out of memory: Killed process",
    "Killed process",
    "oom-kill",
    "Memory cgroup out of memory",
)


def default_session_plan(plan_name: str = "phase52_24h_default") -> list[dict[str, Any]]:
    sessions = [
        ("session_001_sanity_30m", 1800, 300),
        ("session_002_short_1h", 3600, 600),
        ("session_003_short_1h", 3600, 600),
        ("session_004_medium_2h", 7200, 900),
        ("session_005_medium_2h", 7200, 900),
        ("session_006_medium_2h", 7200, 900),
        ("session_007_long_3h", 10800, 1200),
        ("session_008_long_3h", 10800, 1200),
        ("session_009_long_4h", 14400, 1200),
        ("session_010_final_3h", 10800, 0),
    ]
    return [
        {
            "session_id": session_id,
            "plan_name": plan_name,
            "requested_duration_sec": duration,
            "cooldown_after_sec": cooldown,
        }
        for session_id, duration, cooldown in sessions
    ]


def plan_totals(plan: list[dict[str, Any]]) -> dict[str, float]:
    capture_sec = sum(float(item.get("requested_duration_sec", 0.0)) for item in plan)
    cooldown_sec = sum(float(item.get("cooldown_after_sec", 0.0)) for item in plan)
    return {
        "total_requested_capture_sec": capture_sec,
        "total_cooldown_sec": cooldown_sec,
        "total_wall_clock_sec": capture_sec + cooldown_sec,
        "total_wall_clock_hours": (capture_sec + cooldown_sec) / 3600.0,
    }


def run_controlled_capture(
    *,
    root: str | Path,
    session_id: str,
    plan_name: str,
    requested_duration_sec: float,
    output_dir: str | Path = DEFAULT_COLLECTION_ROOT,
    strict_100ms: bool = True,
    create_bundle: bool = True,
    fail_session_on_quality_gate: bool = False,
    dry_run: bool = False,
    simulate_failure: str | None = None,
    notes: str = "",
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    collection_root = _resolve(root_path, output_dir)
    session_dir = collection_root / "sessions" / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    console_path = session_dir / f"phase_5_2_{session_id}_console.log"
    metadata_path = session_dir / f"phase_5_2_{session_id}_metadata.json"
    quality_path = session_dir / f"phase_5_2_{session_id}_quality_report.json"
    bundle_path = session_dir / f"phase_5_2_{session_id}_capture_bundle.zip"
    sha_path = session_dir / f"phase_5_2_{session_id}_sha256.txt"

    started = _utc_now()
    start_mono = time.monotonic()
    source_repo_state_before = _git_source_state(root_path)
    runtime_report: dict[str, Any]
    console_lines = [
        f"Phase 5.2 controlled capture started: {started}",
        f"session_id={session_id}",
        f"requested_duration_sec={requested_duration_sec}",
        f"strict_100ms={strict_100ms}",
        f"dry_run={dry_run}",
    ]
    if dry_run:
        runtime_report = synthetic_phase42h_runtime_report(requested_duration_sec=requested_duration_sec, simulate_failure=simulate_failure)
        if create_bundle:
            _write_synthetic_phase42h_bundle(bundle_path, runtime_report, dry_run=True)
        console_lines.append("dry-run synthetic capture completed")
        exit_code = 0 if runtime_report.get("status") == "pass" else 1
    else:
        _write_text(console_path, "\n".join(console_lines) + "\n")
        exit_code, runtime_report, subprocess_output = _run_real_phase42h_capture(
            root_path=root_path,
            session_dir=session_dir,
            requested_duration_sec=requested_duration_sec,
        )
        if subprocess_output:
            _append_text(console_path, subprocess_output.rstrip() + "\n")
        source_bundle = _find_phase42h_bundle(session_dir, runtime_report)
        if create_bundle and source_bundle is not None and source_bundle.exists():
            shutil.copy2(source_bundle, bundle_path)
    ended = _utc_now()
    actual_duration = max(0.0, time.monotonic() - start_mono)
    if dry_run:
        actual_duration = min(float(requested_duration_sec), actual_duration)
    console_lines.append(f"Phase 5.2 controlled capture ended: {ended}")
    console_lines.append(f"exit_code={exit_code}")
    if dry_run:
        _write_text(console_path, "\n".join(console_lines) + "\n")
    else:
        _append_text(console_path, f"Phase 5.2 controlled capture ended: {ended}\nexit_code={exit_code}\n")

    if create_bundle and not bundle_path.exists():
        _write_synthetic_phase42h_bundle(bundle_path, runtime_report, dry_run=dry_run)
    bundle_sha = _sha256_file(bundle_path) if bundle_path.exists() else ""
    _write_text(
        sha_path,
        "\n".join(
            [
                f"filename: {bundle_path.name}",
                f"sha256: {bundle_sha}",
                f"file_size_bytes: {bundle_path.stat().st_size if bundle_path.exists() else 0}",
                f"utc_timestamp: {_utc_now()}",
            ]
        )
        + "\n",
    )
    bundle_sha_valid = bundle_path.exists() and parse_sha256_file(sha_path) == bundle_sha
    quality = evaluate_session_quality(runtime_report, bundle_sha_valid=bundle_sha_valid)
    metadata = build_session_metadata(
        root_path=root_path,
        session_id=session_id,
        plan_name=plan_name,
        requested_duration_sec=requested_duration_sec,
        actual_duration_sec=actual_duration,
        started_at_utc=started,
        ended_at_utc=ended,
        runtime_report=runtime_report,
        quality_report=quality,
        notes=notes,
        source_repo_state_before=source_repo_state_before,
    )
    _write_json(quality_path, quality)
    _write_json(metadata_path, metadata)
    return {
        "session_id": session_id,
        "status": runtime_report.get("status", "fail"),
        "research_eligible": quality["research_eligible"],
        "exit_code": 1 if fail_session_on_quality_gate and not quality["research_eligible"] else exit_code,
        "artifact_paths": {
            "bundle": _relative(root_path, bundle_path),
            "sha256": _relative(root_path, sha_path),
            "console_log": _relative(root_path, console_path),
            "metadata": _relative(root_path, metadata_path),
            "quality_report": _relative(root_path, quality_path),
        },
        "quality_report": quality,
        "metadata": metadata,
    }


def run_auto_collection(
    *,
    root: str | Path,
    plan_name: str,
    total_budget_hours: float,
    output_dir: str | Path = DEFAULT_COLLECTION_ROOT,
    strict_100ms: bool = True,
    create_bundles: bool = True,
    fail_session_on_quality_gate: bool = False,
    resume: bool = False,
    max_session_retries: int = 1,
    cooldown_sec: int | None = None,
    session_plan_json: str | Path | None = None,
    stop_after_current_session_file: str | Path | None = None,
    dry_run: bool = False,
    test_max_sessions: int | None = None,
    include_large_datasets: bool = False,
    allow_nested_zip: bool = False,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    collection_root = _resolve(root_path, output_dir)
    _ensure_dirs(root_path, collection_root)
    stop_file = _resolve(root_path, stop_after_current_session_file or STOP_FILE_DEFAULT)
    plan = load_session_plan(plan_name=plan_name, session_plan_json=_resolve(root_path, session_plan_json) if session_plan_json else None)
    if test_max_sessions is not None:
        plan = plan[:test_max_sessions]
    totals = plan_totals(plan)
    started = _existing_started_at(root_path) if resume else None
    started = started or _utc_now()
    existing_manifest = _read_json(root_path / MANIFEST_PATH) if resume and (root_path / MANIFEST_PATH).exists() else {}
    existing_by_id = {session["session_id"]: session for session in existing_manifest.get("sessions", []) if isinstance(session, dict)}
    sessions: list[dict[str, Any]] = []
    stopped_early = False
    stop_reason = ""
    current_session = None

    for index, item in enumerate(plan, start=1):
        session_id = str(item["session_id"])
        current_session = session_id
        existing = existing_by_id.get(session_id, {})
        if resume and _completed_research_eligible(existing):
            sessions.append({**existing, "resume_action": "skipped_completed_research_eligible"})
            continue
        retry_count = int(existing.get("retry_count", 0) or 0)
        if resume and existing and retry_count >= max_session_retries and not _completed_research_eligible(existing):
            sessions.append({**existing, "resume_action": "retry_limit_reached"})
            continue
        if resume and existing and not _completed_research_eligible(existing):
            _preserve_failed_attempt(collection_root, session_id=session_id, retry_count=retry_count)

        _write_status(
            root_path,
            build_status(
                plan_name=plan_name,
                current_session=session_id,
                sessions=sessions,
                running=True,
                stopped_early=False,
                stop_reason="",
            ),
        )
        result = run_controlled_capture(
            root=root_path,
            session_id=session_id,
            plan_name=plan_name,
            requested_duration_sec=float(item["requested_duration_sec"]),
            output_dir=collection_root,
            strict_100ms=strict_100ms,
            create_bundle=create_bundles,
            fail_session_on_quality_gate=fail_session_on_quality_gate,
            dry_run=dry_run,
            notes="auto collection session",
        )
        session_entry = {
            "session_id": session_id,
            "index": index,
            "requested_duration_sec": float(item["requested_duration_sec"]),
            "cooldown_after_sec": int(cooldown_sec if cooldown_sec is not None else item.get("cooldown_after_sec", 0)),
            "status": result["status"],
            "research_eligible": result["research_eligible"],
            "runtime_status": result["metadata"]["runtime_status"],
            "primary_failure": result["metadata"]["primary_failure"],
            "actual_duration_sec": result["metadata"]["actual_duration_sec"],
            "retry_count": retry_count + 1,
            "artifact_paths": result["artifact_paths"],
        }
        sessions.append(session_entry)
        if fail_session_on_quality_gate and session_entry["research_eligible"] is not True:
            stopped_early = True
            stop_reason = f"quality gate failed for {session_id}; fail-session-on-quality-gate stopped auto collection"
            break
        if stop_file.exists():
            stopped_early = True
            stop_reason = f"stop-after-current-session file observed after {session_id}"
            break
        delay = session_entry["cooldown_after_sec"]
        if delay > 0 and not dry_run:
            time.sleep(delay)

    manifest = build_auto_collection_manifest(
        plan_name=plan_name,
        started_at_utc=started,
        ended_at_utc=_utc_now(),
        total_budget_hours=total_budget_hours,
        plan=plan,
        sessions=sessions,
        stopped_early=stopped_early,
        stop_reason=stop_reason,
    )
    report = build_auto_collection_report(
        manifest=manifest,
        strict_100ms=strict_100ms,
        include_large_datasets=include_large_datasets,
        allow_nested_zip=allow_nested_zip,
    )
    _write_json(root_path / MANIFEST_PATH, manifest)
    _write_json(root_path / STATUS_PATH, build_status(plan_name=plan_name, current_session=current_session, sessions=sessions, running=False, stopped_early=stopped_early, stop_reason=stop_reason))
    _write_json(root_path / REPORT_JSON_PATH, report)
    _write_text(root_path / REPORT_MD_PATH, render_auto_collection_markdown(report))
    if create_bundles:
        create_all_sessions_bundle(
            root_path,
            collection_root,
            include_large_datasets=include_large_datasets,
            allow_nested_zip=allow_nested_zip,
        )
    return report


def load_session_plan(*, plan_name: str, session_plan_json: Path | None = None) -> list[dict[str, Any]]:
    if session_plan_json is not None and session_plan_json.exists():
        payload = _read_json(session_plan_json)
        sessions = payload.get("sessions", payload)
        return [
            {
                "session_id": str(item["session_id"]),
                "plan_name": plan_name,
                "requested_duration_sec": float(item["requested_duration_sec"]),
                "cooldown_after_sec": int(item.get("cooldown_after_sec", 0)),
            }
            for item in sessions
            if isinstance(item, dict)
        ]
    return default_session_plan(plan_name)


def evaluate_session_quality(runtime_report: dict[str, Any], *, bundle_sha_valid: bool) -> dict[str, Any]:
    phase41 = _dict(runtime_report.get("phase41_runtime_report"))
    clock = _dict(runtime_report.get("clock_offset_summary"))
    phase41_status = phase41.get("phase_4_1_status") or runtime_report.get("phase41_runtime_report_status")
    checks = {
        "status_pass": runtime_report.get("status") == "pass",
        "primary_failure_none": runtime_report.get("primary_failure") is None,
        "phase41_status_pass": phase41_status == "pass",
        "clock_sync_status_pass": runtime_report.get("clock_sync_status") == "pass",
        "clock_offset_drift_valid": clock.get("clock_offset_drift_valid") is True,
        "clock_offset_sample_quality_valid": clock.get("clock_offset_sample_quality_valid") is True,
        "strict_100ms_observability_ready": runtime_report.get("strict_100ms_observability_ready") is True,
        "low_latency_ready": runtime_report.get("low_latency_ready") is True,
        "bundle_sha256_valid": bundle_sha_valid,
    }
    failure_reasons = [name for name, passed in checks.items() if not passed]
    return {
        "phase": PHASE,
        "status": "pass" if not failure_reasons else "fail",
        "research_eligible": not failure_reasons,
        "checks": checks,
        "failure_reasons": failure_reasons,
        "bundle_sha256_valid": bundle_sha_valid,
        "strict_100ms_hard_requirement": True,
    }


def build_session_metadata(
    *,
    root_path: Path,
    session_id: str,
    plan_name: str,
    requested_duration_sec: float,
    actual_duration_sec: float,
    started_at_utc: str,
    ended_at_utc: str,
    runtime_report: dict[str, Any],
    quality_report: dict[str, Any],
    notes: str,
    source_repo_state_before: dict[str, Any] | None = None,
) -> dict[str, Any]:
    phase41 = _dict(runtime_report.get("phase41_runtime_report"))
    clock = _dict(runtime_report.get("clock_offset_summary"))
    hot_path = _dict(runtime_report.get("hot_path_latency_summary"))
    latency_metrics = _dict(hot_path.get("metrics"))
    end_to_end = _dict(latency_metrics.get("end_to_end_local_hot_path_ms"))
    before_state = source_repo_state_before or _git_source_state(root_path)
    after_state = _git_source_state(root_path)
    metadata = {
        "phase": PHASE,
        "session_id": session_id,
        "plan_name": plan_name,
        "repo_commit": before_state.get("repo_commit", "unknown"),
        "source_repo_dirty": before_state.get("source_repo_dirty", False),
        "source_repo_dirty_before_session": before_state.get("source_repo_dirty", False),
        "source_repo_dirty_after_session": after_state.get("source_repo_dirty", False),
        "source_repo_status_before_session": before_state,
        "source_repo_status_after_session": after_state,
        "runtime_artifacts_present": after_state.get("runtime_artifacts_present", False),
        "ignored_runtime_artifacts_present": after_state.get("ignored_runtime_artifacts_present", False),
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "requested_duration_sec": requested_duration_sec,
        "actual_duration_sec": actual_duration_sec,
        "host_info": {"hostname": platform.node(), "machine": platform.machine(), "processor": platform.processor()},
        "os_info": {"platform": platform.platform(), "system": platform.system(), "release": platform.release()},
        "python_version": sys.version,
        "vps_provider": os.environ.get("PHASE52_VPS_PROVIDER", ""),
        "vps_region": os.environ.get("PHASE52_VPS_REGION", ""),
        "runtime_status": runtime_report.get("status"),
        "primary_failure": runtime_report.get("primary_failure"),
        "failure_classifications": runtime_report.get("failure_classifications", []),
        "exit_code": runtime_report.get("exit_code"),
        "oom_detected": _dict(runtime_report.get("oom_evidence")).get("oom_detected", False),
        "oom_log_excerpt": _dict(runtime_report.get("oom_evidence")).get("oom_log_excerpt", ""),
        "oom_killed_pid": _dict(runtime_report.get("oom_evidence")).get("killed_pid"),
        "process_failure_evidence": runtime_report.get("process_failure_evidence", {}),
        "memory_telemetry": runtime_report.get("memory_telemetry", {}),
        "failure_reasons": quality_report.get("failure_reasons", []),
        "clock_sync_status": runtime_report.get("clock_sync_status"),
        "accepted_clock_sample_count": clock.get("accepted_clock_sample_count", 0),
        "discarded_clock_sample_count": clock.get("discarded_clock_sample_count", 0),
        "snapshot_copy_p99_us": phase41.get("snapshot_copy_p99_us"),
        "end_to_end_hot_path_p99_ms": end_to_end.get("p99"),
        "strict_100ms_observability_ready": runtime_report.get("strict_100ms_observability_ready") is True,
        "low_latency_ready": runtime_report.get("low_latency_ready") is True,
        "research_eligible": quality_report["research_eligible"],
        "notes": notes,
        "no_live_trading": True,
        "no_execution": True,
        "no_wallet_logic": True,
        "no_order_placement": True,
    }
    missing = [field for field in REQUIRED_SESSION_METADATA_FIELDS if field not in metadata]
    metadata["metadata_missing_required_fields"] = missing
    return metadata


def build_auto_collection_manifest(
    *,
    plan_name: str,
    started_at_utc: str,
    ended_at_utc: str,
    total_budget_hours: float,
    plan: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    stopped_early: bool,
    stop_reason: str,
) -> dict[str, Any]:
    actual_capture = sum(float(session.get("actual_duration_sec", 0.0) or 0.0) for session in sessions)
    passed = sum(1 for session in sessions if session.get("status") == "pass")
    eligible = sum(1 for session in sessions if session.get("research_eligible") is True)
    failed = sum(1 for session in sessions if session.get("status") != "pass")
    return {
        "phase": PHASE,
        "schema_version": "phase_5_2_auto_collection_manifest_v1",
        "plan_name": plan_name,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "total_budget_hours": total_budget_hours,
        "total_requested_capture_sec": sum(float(item.get("requested_duration_sec", 0.0)) for item in plan),
        "total_planned_wall_clock_sec": plan_totals(plan)["total_wall_clock_sec"],
        "total_actual_capture_sec": actual_capture,
        "session_count": len(sessions),
        "planned_session_count": len(plan),
        "passed_session_count": passed,
        "failed_session_count": failed,
        "research_eligible_session_count": eligible,
        "sessions": sessions,
        "stopped_early": stopped_early,
        "stop_reason": stop_reason,
        "next_phase_recommendation": "download artifacts and run Phase 5.1 multi_bundle analysis when enough research_eligible sessions exist",
        "no_live_trading": True,
        "no_execution": True,
        "no_wallet_logic": True,
    }


def build_status(
    *,
    plan_name: str,
    current_session: str | None,
    sessions: list[dict[str, Any]],
    running: bool,
    stopped_early: bool,
    stop_reason: str,
) -> dict[str, Any]:
    last_failure = _last_failure(sessions, stopped_early=stopped_early, stop_reason=stop_reason)
    return {
        "phase": PHASE,
        "plan_name": plan_name,
        "updated_at_utc": _utc_now(),
        "running": running,
        "current_session": current_session,
        "completed_session_count": len(sessions),
        "passed_session_count": sum(1 for session in sessions if session.get("status") == "pass"),
        "failed_session_count": sum(1 for session in sessions if session.get("status") != "pass"),
        "research_eligible_session_count": sum(1 for session in sessions if session.get("research_eligible") is True),
        "last_failure": last_failure,
        "stopped_early": stopped_early,
        "stop_reason": stop_reason,
    }


def build_auto_collection_report(
    *,
    manifest: dict[str, Any],
    strict_100ms: bool,
    include_large_datasets: bool = False,
    allow_nested_zip: bool = False,
) -> dict[str, Any]:
    bundle_mode = "full_dataset_explicit" if include_large_datasets else "audit_light"
    collection_status = _collection_status(manifest)
    return {
        "phase": PHASE,
        "schema_version": "phase_5_2_auto_collection_report_v1",
        "status": _report_status(manifest),
        "collection_status": collection_status,
        "aggregate_status": collection_status,
        "last_failure": _last_failure(
            list(_manifest_sessions(manifest)),
            stopped_early=manifest.get("stopped_early") is True,
            stop_reason=str(manifest.get("stop_reason") or ""),
        ),
        "strict_100ms_hard_requirement": strict_100ms,
        "binance_websocket_research_only": True,
        "manifest": manifest,
        "artifact_manifest_path": MANIFEST_PATH.as_posix(),
        "status_path": STATUS_PATH.as_posix(),
        "artifact_bundle_mode": bundle_mode,
        "audit_bundle_path": AUDIT_BUNDLE.as_posix(),
        "audit_sha256_path": AUDIT_SHA256.as_posix(),
        "full_dataset_bundle_path": FULL_DATASET_BUNDLE.as_posix(),
        "full_dataset_sha256_path": FULL_DATASET_SHA256.as_posix(),
        "file_size_manifest_path": FILE_SIZE_MANIFEST.as_posix(),
        "include_large_datasets": include_large_datasets,
        "allow_nested_zip": allow_nested_zip,
        "no_nested_zip_by_default": allow_nested_zip is False,
        "all_sessions_bundle_path": ALL_SESSIONS_BUNDLE.as_posix(),
        "all_sessions_sha256_path": ALL_SESSIONS_SHA256.as_posix(),
        "no_live_trading": True,
        "no_execution": True,
        "no_wallet_logic": True,
        "no_order_placement": True,
        "no_production_strategy": True,
    }


def create_all_sessions_bundle(
    root_path: Path,
    collection_root: Path,
    *,
    include_large_datasets: bool = False,
    allow_nested_zip: bool = False,
) -> dict[str, Any]:
    audit_path = root_path / AUDIT_BUNDLE
    audit_sha_path = root_path / AUDIT_SHA256
    full_path = root_path / FULL_DATASET_BUNDLE
    full_sha_path = root_path / FULL_DATASET_SHA256
    for stale in (audit_path, audit_sha_path):
        if stale.exists():
            stale.unlink()
    if not include_large_datasets:
        for stale in (full_path, full_sha_path):
            if stale.exists():
                stale.unlink()

    files = _phase52_artifact_file_records(
        root_path,
        collection_root,
        include_large_datasets=include_large_datasets,
        allow_nested_zip=allow_nested_zip,
    )
    manifest_payload = {
        "schema_version": "phase_5_2_file_size_manifest_v1",
        "created_at_utc": _utc_now(),
        "bundle_policy": {
            "default_bundle": "audit_light",
            "include_large_datasets": include_large_datasets,
            "allow_nested_zip": allow_nested_zip,
            "large_jsonl_excluded_from_audit_bundle": True,
            "zip_inside_zip_excluded_by_default": allow_nested_zip is False,
        },
        "audit_bundle_path": AUDIT_BUNDLE.as_posix(),
        "full_dataset_bundle_path": FULL_DATASET_BUNDLE.as_posix() if include_large_datasets else None,
        "files": files,
    }
    _write_json(root_path / FILE_SIZE_MANIFEST, manifest_payload)
    manifest_text = json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n"

    audit_files = [item for item in files if item["included_in_audit_bundle"] is True]
    with zipfile.ZipFile(audit_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in audit_files:
            archive.write(root_path / item["path"], item["path"])
        archive.write(root_path / FILE_SIZE_MANIFEST, FILE_SIZE_MANIFEST.as_posix())
        archive.writestr("file_size_manifest.json", manifest_text)
    audit_sha = _sha256_file(audit_path)
    _write_bundle_sha(audit_sha_path, bundle_path=audit_path, sha256=audit_sha)

    result = {
        "path": AUDIT_BUNDLE.as_posix(),
        "sha256": audit_sha,
        "size_bytes": audit_path.stat().st_size,
        "file_size_manifest_path": FILE_SIZE_MANIFEST.as_posix(),
        "bundle_mode": "audit_light",
        "include_large_datasets": include_large_datasets,
        "allow_nested_zip": allow_nested_zip,
    }
    if include_large_datasets:
        for stale in (full_path, full_sha_path):
            if stale.exists():
                stale.unlink()
        full_files = [item for item in files if item["included_in_full_bundle"] is True]
        with zipfile.ZipFile(full_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in full_files:
                archive.write(root_path / item["path"], item["path"])
            archive.write(root_path / FILE_SIZE_MANIFEST, FILE_SIZE_MANIFEST.as_posix())
            archive.writestr("file_size_manifest.json", manifest_text)
        full_sha = _sha256_file(full_path)
        _write_bundle_sha(full_sha_path, bundle_path=full_path, sha256=full_sha)
        result.update(
            {
                "full_dataset_bundle_path": FULL_DATASET_BUNDLE.as_posix(),
                "full_dataset_sha256": full_sha,
                "full_dataset_size_bytes": full_path.stat().st_size,
                "bundle_mode": "audit_light_plus_full_dataset",
            }
        )
    return result


def _phase52_artifact_file_records(
    root_path: Path,
    collection_root: Path,
    *,
    include_large_datasets: bool,
    allow_nested_zip: bool,
) -> list[dict[str, Any]]:
    output_names = {
        AUDIT_BUNDLE.as_posix(),
        AUDIT_SHA256.as_posix(),
        FULL_DATASET_BUNDLE.as_posix(),
        FULL_DATASET_SHA256.as_posix(),
        LEGACY_ALL_SESSIONS_BUNDLE.as_posix(),
        LEGACY_ALL_SESSIONS_SHA256.as_posix(),
    }
    bases = (collection_root, root_path / "data/debug", root_path / "data/reports")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for base in bases:
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            relative = _relative(root_path, path)
            if relative in seen or relative in output_names or relative == FILE_SIZE_MANIFEST.as_posix():
                continue
            if not _is_phase52_bundle_candidate(root_path=root_path, collection_root=collection_root, path=path, relative=relative):
                continue
            suffix = path.suffix.lower()
            is_large_dataset = suffix == ".jsonl"
            is_zip = suffix == ".zip"
            included_in_audit = not is_large_dataset and (not is_zip or allow_nested_zip)
            included_in_full = include_large_datasets and (is_large_dataset or (is_zip and allow_nested_zip))
            records.append(
                {
                    "path": relative,
                    "size_bytes": path.stat().st_size,
                    "artifact_type": _phase52_artifact_type(relative),
                    "included_in_audit_bundle": included_in_audit,
                    "included_in_full_bundle": included_in_full,
                }
            )
            seen.add(relative)
    return sorted(records, key=lambda item: item["path"])


def _is_phase52_bundle_candidate(*, root_path: Path, collection_root: Path, path: Path, relative: str) -> bool:
    try:
        path.resolve().relative_to(collection_root.resolve())
        return True
    except ValueError:
        pass
    normalized = relative.replace("\\", "/")
    return "phase_5_2" in normalized or "phase52" in normalized


def _phase52_artifact_type(relative: str) -> str:
    normalized = relative.replace("\\", "/")
    suffix = Path(normalized).suffix.lower()
    name = Path(normalized).name.lower()
    if suffix == ".jsonl":
        return "large_dataset"
    if suffix == ".zip":
        return "archive"
    if suffix == ".log":
        return "console_log" if "console" in name else "log"
    if "metadata" in name:
        return "metadata"
    if "quality" in name:
        return "quality_report"
    if "sha256" in name:
        return "sha256"
    if "/reports/" in normalized or normalized.startswith("data/reports/"):
        return "report"
    if "/debug/" in normalized or normalized.startswith("data/debug/"):
        return "debug"
    return "artifact"


def _write_bundle_sha(path: Path, *, bundle_path: Path, sha256: str) -> None:
    _write_text(
        path,
        "\n".join(
            [
                f"filename: {bundle_path.name}",
                f"sha256: {sha256}",
                f"file_size_bytes: {bundle_path.stat().st_size}",
                f"utc_timestamp: {_utc_now()}",
            ]
        )
        + "\n",
    )


def render_auto_collection_markdown(report: dict[str, Any]) -> str:
    manifest = report["manifest"]
    lines = [
        "# Phase 5.2 Auto Collection Report",
        "",
        f"Plan: {manifest.get('plan_name')}",
        f"Strict 100ms: {report.get('strict_100ms_hard_requirement')}",
        f"Sessions completed: {manifest.get('session_count')}",
        f"Research eligible sessions: {manifest.get('research_eligible_session_count')}",
        f"Stopped early: {manifest.get('stopped_early')}",
        "",
        "## Scope",
        "Data collection only. No live trading, no execution, no wallet/private-key logic, no order placement, and no production strategy.",
        "",
        "## Next Phase Recommendation",
        str(manifest.get("next_phase_recommendation")),
        "",
    ]
    return "\n".join(lines)


def synthetic_phase42h_runtime_report(*, requested_duration_sec: float, simulate_failure: str | None = None) -> dict[str, Any]:
    report = {
        "phase": "4.2H",
        "status": "pass",
        "primary_failure": None,
        "clock_sync_status": "pass",
        "strict_100ms_observability_ready": True,
        "low_latency_ready": True,
        "duration_sec": requested_duration_sec,
        "clock_offset_summary": {
            "clock_offset_drift_valid": True,
            "clock_offset_sample_quality_valid": True,
            "accepted_clock_sample_count": 8,
            "discarded_clock_sample_count": 0,
        },
        "phase41_runtime_report_status": "pass",
        "phase41_runtime_report": {
            "phase_4_1_status": "pass",
            "phase_4_1_pass": True,
            "snapshot_copy_budget_met": True,
            "snapshot_copy_p99_us": 75.0,
        },
        "hot_path_latency_summary": {
            "metrics": {
                "end_to_end_local_hot_path_ms": {
                    "p99": 5.0,
                }
            }
        },
        "hard_fail_reasons": [],
        "warning_reasons": [],
    }
    if simulate_failure == "strict_100ms_false":
        report["strict_100ms_observability_ready"] = False
        report["status"] = "fail"
        report["primary_failure"] = "STRICT_100MS_OBSERVABILITY_FAILURE"
    elif simulate_failure == "low_latency_false":
        report["low_latency_ready"] = False
        report["status"] = "fail"
        report["primary_failure"] = "LOW_LATENCY_FAILURE"
    elif simulate_failure == "clock_sync_fail":
        report["clock_sync_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = "CLOCK_SYNC_FAILURE"
    elif simulate_failure == "primary_failure":
        report["status"] = "fail"
        report["primary_failure"] = "SYNTHETIC_FAILURE"
    if report["primary_failure"]:
        report["hard_fail_reasons"] = [str(report["primary_failure"])]
    return report


def parse_sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.strip().startswith("sha256:"):
            return line.split(":", 1)[1].strip()
    return ""


def _run_real_phase42h_capture(*, root_path: Path, session_dir: Path, requested_duration_sec: float) -> tuple[int, dict[str, Any], str]:
    session_dir.mkdir(parents=True, exist_ok=True)
    console_path = session_dir / f"phase_5_2_{session_dir.name}_console.log"
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(root_path / "scripts/run_phase42h_hotpath_environment_latency.py"),
        "--root",
        str(session_dir),
        "--duration-sec",
        str(requested_duration_sec),
        "--environment-name",
        os.environ.get("PHASE52_VPS_PROVIDER", "phase52_vps"),
        "--environment-region",
        os.environ.get("PHASE52_VPS_REGION", "unknown"),
        "--run-mode",
        "vps_final",
        "--skip-pytest",
        "--clean",
    ]
    started_at_utc = _utc_now()
    _append_text(
        console_path,
        "\n".join(
            [
                f"Phase 4.2H child command started: {started_at_utc}",
                "command=" + " ".join(str(part) for part in command),
                "--- child output ---",
            ]
        )
        + "\n",
    )
    with console_path.open("a", encoding="utf-8", errors="replace") as console_handle:
        process = subprocess.run(
            command,
            cwd=root_path,
            text=True,
            stdout=console_handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    ended_at_utc = _utc_now()
    _append_text(
        console_path,
        "\n".join(
            [
                "--- child output ended ---",
                f"Phase 4.2H child command ended: {ended_at_utc}",
                f"child_exit_code={process.returncode}",
            ]
        )
        + "\n",
    )
    report_path = session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
    if report_path.exists():
        report = _read_json(report_path)
        report.setdefault("exit_code", process.returncode)
    else:
        kernel_log_text = _collect_kernel_log_text(started_at_utc=started_at_utc, ended_at_utc=ended_at_utc)
        classification = classify_child_process_failure(
            exit_code=process.returncode,
            kernel_log_text=kernel_log_text,
            session_started_at_utc=started_at_utc,
            session_ended_at_utc=ended_at_utc,
            report_missing=True,
        )
        report = phase42h_process_failure_report(
            requested_duration_sec=requested_duration_sec,
            exit_code=process.returncode,
            classification=classification,
            started_at_utc=started_at_utc,
            ended_at_utc=ended_at_utc,
        )
    duration_sec = max(0.0, (_parse_utc(ended_at_utc) - _parse_utc(started_at_utc)).total_seconds())
    summary = f"Phase 4.2H child output streamed to {_relative(root_path, console_path)}; duration_sec={duration_sec:.3f}; exit_code={process.returncode}"
    return process.returncode, report, summary


def phase42h_process_failure_report(
    *,
    requested_duration_sec: float,
    exit_code: int,
    classification: dict[str, Any],
    started_at_utc: str,
    ended_at_utc: str,
) -> dict[str, Any]:
    primary = str(classification.get("primary_failure") or REPORT_MISSING_CLASSIFICATION)
    report = synthetic_phase42h_runtime_report(requested_duration_sec=requested_duration_sec)
    report["status"] = "fail"
    report["primary_failure"] = primary
    report["failure_classifications"] = list(classification.get("failure_classifications", [primary]))
    report["hard_fail_reasons"] = [str(classification.get("reason") or primary)]
    report["exit_code"] = exit_code
    report["child_exit_code"] = exit_code
    report["child_signal"] = "SIGKILL" if exit_code == -9 else None
    report["dry_run"] = False
    report["fallback_reason"] = "child_exited_before_hotpath_report"
    report["started_at_utc"] = started_at_utc
    report["ended_at_utc"] = ended_at_utc
    report["oom_evidence"] = classification.get("oom_evidence", {})
    report["process_failure_evidence"] = {
        "exit_code": exit_code,
        "child_exit_code": exit_code,
        "child_signal": "SIGKILL" if exit_code == -9 else None,
        "report_missing": True,
        "primary_failure": primary,
        "fallback_reason": "child_exited_before_hotpath_report",
        "session_started_at_utc": started_at_utc,
        "session_ended_at_utc": ended_at_utc,
    }
    report["memory_telemetry"] = classification.get("memory_telemetry", {})
    return report


def classify_child_process_failure(
    *,
    exit_code: int,
    kernel_log_text: str,
    session_started_at_utc: str,
    session_ended_at_utc: str,
    report_missing: bool,
) -> dict[str, Any]:
    oom_evidence = detect_oom_evidence(
        kernel_log_text,
        session_started_at_utc=session_started_at_utc,
        session_ended_at_utc=session_ended_at_utc,
    )
    if exit_code == -9 and oom_evidence.get("oom_detected") is True:
        return {
            "primary_failure": OOM_CLASSIFICATION,
            "failure_classifications": [OOM_CLASSIFICATION],
            "reason": "Phase 4.2H child exited -9 and kernel logs show OOM kill in the session window",
            "oom_evidence": oom_evidence,
        }
    if exit_code == -9:
        return {
            "primary_failure": PROCESS_SIGKILL_CLASSIFICATION,
            "failure_classifications": [PROCESS_SIGKILL_CLASSIFICATION],
            "reason": "Phase 4.2H child exited -9 without kernel OOM evidence",
            "oom_evidence": oom_evidence,
        }
    if report_missing:
        return {
            "primary_failure": REPORT_MISSING_CLASSIFICATION,
            "failure_classifications": [REPORT_MISSING_CLASSIFICATION],
            "reason": "Phase 4.2H report missing after child process returned",
            "oom_evidence": oom_evidence,
        }
    return {
        "primary_failure": "PROCESS_FAILED",
        "failure_classifications": ["PROCESS_FAILED"],
        "reason": f"Phase 4.2H child exited {exit_code}",
        "oom_evidence": oom_evidence,
    }


def detect_oom_evidence(
    kernel_log_text: str,
    *,
    session_started_at_utc: str,
    session_ended_at_utc: str,
    window_sec: int = 600,
) -> dict[str, Any]:
    started = _parse_utc(session_started_at_utc)
    ended = _parse_utc(session_ended_at_utc)
    window_start = started - timedelta(seconds=window_sec)
    window_end = ended + timedelta(seconds=window_sec)
    matches: list[str] = []
    killed_pid: int | None = None
    for raw_line in kernel_log_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not any(pattern in line for pattern in KERNEL_OOM_PATTERNS):
            continue
        line_time = _parse_kernel_log_time(line, default_year=started.year)
        if line_time is not None and not (window_start <= line_time <= window_end):
            continue
        matches.append(line)
        pid_match = re.search(r"Killed process\s+(\d+)", line)
        if pid_match:
            killed_pid = int(pid_match.group(1))
    excerpt = "\n".join(matches[:5])
    return {
        "oom_detected": bool(matches),
        "oom_evidence_source": "kernel_log",
        "oom_log_excerpt": excerpt,
        "matched_line_count": len(matches),
        "killed_pid": killed_pid,
        "session_window_start_utc": window_start.isoformat().replace("+00:00", "Z"),
        "session_window_end_utc": window_end.isoformat().replace("+00:00", "Z"),
    }


def _collect_kernel_log_text(*, started_at_utc: str, ended_at_utc: str) -> str:
    commands = [
        ["journalctl", "-k", "--since", started_at_utc, "--until", ended_at_utc, "--no-pager"],
        ["dmesg", "--ctime", "--color=never"],
        ["dmesg", "--color=never"],
    ]
    outputs: list[str] = []
    for command in commands:
        try:
            process = subprocess.run(command, text=True, capture_output=True, check=False, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            continue
        if process.stdout:
            outputs.append(process.stdout)
    return "\n".join(outputs)


def _find_phase42h_bundle(session_dir: Path, runtime_report: dict[str, Any]) -> Path | None:
    pass_bundle = session_dir / "phase_4_2h_hotpath_environment_latency_bundle.zip"
    fail_bundle = session_dir / "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip"
    if runtime_report.get("status") == "pass" and pass_bundle.exists():
        return pass_bundle
    if fail_bundle.exists():
        return fail_bundle
    return pass_bundle if pass_bundle.exists() else None


def _write_synthetic_phase42h_bundle(path: Path, runtime_report: dict[str, Any], *, dry_run: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        md = "# Synthetic Phase 4.2H report for Phase 5.2 dry-run\n"
        runtime_report.setdefault("dry_run", True)
    else:
        md = "# Fallback Phase 4.2H report generated because child process exited before producing hotpath report.\n"
        runtime_report["dry_run"] = False
        runtime_report.setdefault("fallback_reason", "child_exited_before_hotpath_report")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data/reports/phase_4_2h_hotpath_environment_latency_report.json", json.dumps(runtime_report, indent=2, sort_keys=True))
        archive.writestr("data/reports/phase_4_2h_hotpath_environment_latency_report.md", md)


def _completed_research_eligible(session: dict[str, Any]) -> bool:
    bundle_path = _dict(session.get("artifact_paths")).get("bundle")
    return session.get("status") == "pass" and session.get("research_eligible") is True and bool(bundle_path)


def _preserve_failed_attempt(collection_root: Path, *, session_id: str, retry_count: int) -> None:
    session_dir = collection_root / "sessions" / session_id
    if not session_dir.exists() or not any(session_dir.iterdir()):
        return
    audit_root = collection_root / "failed_audit" / session_id
    target = audit_root / f"attempt_{retry_count + 1:03d}"
    suffix = 2
    while target.exists():
        target = audit_root / f"attempt_{retry_count + 1:03d}_{suffix}"
        suffix += 1
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(session_dir, target)


def _existing_started_at(root_path: Path) -> str | None:
    path = root_path / MANIFEST_PATH
    if not path.exists():
        return None
    return _read_json(path).get("started_at_utc")


def _write_status(root_path: Path, status: dict[str, Any]) -> None:
    _write_json(root_path / STATUS_PATH, status)


def _ensure_dirs(root_path: Path, collection_root: Path) -> None:
    collection_root.mkdir(parents=True, exist_ok=True)
    (collection_root / "sessions").mkdir(parents=True, exist_ok=True)
    (root_path / "data/debug").mkdir(parents=True, exist_ok=True)
    (root_path / "data/reports").mkdir(parents=True, exist_ok=True)


def _git_identity(root_path: Path) -> tuple[str, bool]:
    state = _git_source_state(root_path)
    return str(state.get("repo_commit", "unknown")), bool(state.get("source_repo_dirty", False))


def _git_source_state(root_path: Path) -> dict[str, Any]:
    try:
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root_path, text=True, capture_output=True, check=False)
        status = subprocess.run(["git", "status", "--short", "--untracked-files=all"], cwd=root_path, text=True, capture_output=True, check=False)
    except OSError:
        return {
            "repo_commit": "unknown",
            "source_repo_dirty": False,
            "git_available": False,
            "raw_status_lines": [],
            "source_status_lines": [],
            "runtime_status_lines": [],
            "runtime_artifacts_present": _runtime_artifacts_present(root_path),
            "ignored_runtime_artifacts_present": _ignored_runtime_artifacts_present(root_path),
        }
    raw_lines = status.stdout.splitlines() if status.returncode == 0 else []
    runtime_lines = [line for line in raw_lines if _is_runtime_artifact_status_line(line)]
    source_lines = [line for line in raw_lines if line not in runtime_lines]
    return {
        "repo_commit": commit.stdout.strip() if commit.returncode == 0 else "unknown",
        "source_repo_dirty": bool(source_lines),
        "git_available": True,
        "raw_status_lines": raw_lines[:50],
        "source_status_lines": source_lines[:50],
        "runtime_status_lines": runtime_lines[:50],
        "runtime_artifacts_present": _runtime_artifacts_present(root_path),
        "ignored_runtime_artifacts_present": _ignored_runtime_artifacts_present(root_path),
    }


def _is_runtime_artifact_status_line(line: str) -> bool:
    path = line[3:].strip() if len(line) > 3 else line.strip()
    return _is_runtime_artifact_path(path)


def _is_runtime_artifact_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return (
        normalized.startswith("data/phase_5_2/")
        or normalized.startswith("data/dataset/")
        or normalized.startswith("data/debug/")
        or normalized.startswith("data/cache/")
        or normalized.startswith("data/logs/")
        or normalized.startswith("data/reports/")
        or normalized.endswith(".jsonl")
        or normalized.endswith(".zip")
        or normalized.endswith(".log")
        or normalized in {
            AUDIT_BUNDLE.as_posix(),
            AUDIT_SHA256.as_posix(),
            FULL_DATASET_BUNDLE.as_posix(),
            FULL_DATASET_SHA256.as_posix(),
            LEGACY_ALL_SESSIONS_BUNDLE.as_posix(),
            LEGACY_ALL_SESSIONS_SHA256.as_posix(),
        }
    )


def _runtime_artifacts_present(root_path: Path) -> bool:
    candidates = (
        root_path / "data/phase_5_2",
        root_path / "data/debug/phase_5_2_auto_collection_status.json",
        root_path / "data/reports/phase_5_2_auto_collection_report.json",
        root_path / AUDIT_BUNDLE,
        root_path / AUDIT_SHA256,
        root_path / FULL_DATASET_BUNDLE,
        root_path / FULL_DATASET_SHA256,
        root_path / LEGACY_ALL_SESSIONS_BUNDLE,
        root_path / LEGACY_ALL_SESSIONS_SHA256,
    )
    return any(path.exists() for path in candidates)


def _ignored_runtime_artifacts_present(root_path: Path) -> bool:
    return _runtime_artifacts_present(root_path)


def _resolve(root_path: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root_path / candidate


def _relative(root_path: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root_path.resolve()).as_posix()
    except ValueError:
        return str(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", errors="replace") as handle:
        handle.write(text)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _manifest_sessions(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [session for session in manifest.get("sessions", []) if isinstance(session, dict)]


def _stop_after_current_session_observed(stop_reason: str) -> bool:
    return stop_reason.startswith("stop-after-current-session file observed")


def _all_completed_sessions_passed(sessions: list[dict[str, Any]]) -> bool:
    return bool(sessions) and all(session.get("status") == "pass" for session in sessions)


def _report_status(manifest: dict[str, Any]) -> str:
    failed = int(manifest.get("failed_session_count", 0) or 0)
    stopped_early = manifest.get("stopped_early") is True
    stop_reason = str(manifest.get("stop_reason") or "")
    sessions = _manifest_sessions(manifest)
    if failed > 0:
        return "fail"
    if stopped_early:
        if _stop_after_current_session_observed(stop_reason) and _all_completed_sessions_passed(sessions):
            return "pass"
        return "fail"
    return "pass"


def _last_failure(
    sessions: list[dict[str, Any]],
    *,
    stopped_early: bool = False,
    stop_reason: str = "",
) -> str | None:
    failed_sessions = [session for session in sessions if session.get("status") != "pass"]
    if failed_sessions:
        failed = failed_sessions[-1]
        primary = failed.get("primary_failure")
        if primary:
            return str(primary)
        runtime_status = failed.get("runtime_status")
        if runtime_status and runtime_status != "pass":
            return str(runtime_status)
        return "SESSION_FAILED"
    if stopped_early and stop_reason.startswith("quality gate failed"):
        return "QUALITY_GATE_FAILED"
    return None


def _collection_status(manifest: dict[str, Any]) -> str:
    if int(manifest.get("failed_session_count", 0) or 0) > 0 and manifest.get("stopped_early") is True:
        return "partial_fail_stopped_early"
    if int(manifest.get("failed_session_count", 0) or 0) > 0:
        return "partial_fail"
    if manifest.get("stopped_early") is True:
        return "stopped_early"
    return "complete_pass"


def _parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_kernel_log_time(line: str, *, default_year: int) -> datetime | None:
    bracket = re.match(r"^\[(?P<stamp>[A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d+\s+\d+:\d+:\d+(?:\s+\d{4})?)\]", line)
    if bracket:
        stamp = bracket.group("stamp")
        for fmt in ("%a %b %d %H:%M:%S %Y", "%a %b %d %H:%M:%S"):
            try:
                parsed = datetime.strptime(stamp, fmt)
                if parsed.year == 1900:
                    parsed = parsed.replace(year=default_year)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    syslog = re.match(r"^(?P<stamp>[A-Z][a-z]{2}\s+\d+\s+\d+:\d+:\d+)", line)
    if syslog:
        try:
            parsed = datetime.strptime(f"{default_year} {syslog.group('stamp')}", "%Y %b %d %H:%M:%S")
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    iso = re.match(r"^(?P<stamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)", line)
    if iso:
        try:
            return _parse_utc(iso.group("stamp"))
        except ValueError:
            return None
    return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
