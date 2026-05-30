from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any


PHASE = "5.2F"
DEFAULT_SESSIONS_ROOT = Path("data/phase_5_2/sessions")
DEFAULT_OUTPUT_JSON = Path("data/reports/phase_5_2_final_audit_manifest.json")
DEFAULT_OUTPUT_MD = Path("data/reports/phase_5_2_final_audit_manifest.md")
DEFAULT_INVENTORY_JSON = Path("data/debug/phase_5_2_final_audit_artifact_inventory.json")
DEFAULT_EVIDENCE_ZIP = Path("phase_5_2_session_005_repaired_eval_final_evidence.zip")
DEFAULT_AUDIT_NOTE = Path("phase_5_2_session_005_repaired_eval_audit_note.md")
PHASE42H_REPORT = Path("data/reports/phase_4_2h_hotpath_environment_latency_report.json")
PHASE42H_SELF_CHECK = Path("data/reports/phase42h_self_check.json")
PHASE42H_STAGE_PROFILE = Path("data/debug/phase_4_2h_latency_stage_profile.json")
LARGE_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024
EXPECTED_EVIDENCE_SHA256 = "e5cfc7754cfbd9bc39b144ee9eed9ce7830e2bc0f98d58c7475dcdc3c0c006cc"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Phase 5.2F final audit manifest.")
    parser.add_argument("--sessions-root", default=str(DEFAULT_SESSIONS_ROOT))
    parser.add_argument("--evidence-zip", default=str(DEFAULT_EVIDENCE_ZIP))
    parser.add_argument("--expected-evidence-sha256", default=EXPECTED_EVIDENCE_SHA256)
    parser.add_argument("--audit-note", default=str(DEFAULT_AUDIT_NOTE))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--output-md", default=str(DEFAULT_OUTPUT_MD))
    parser.add_argument("--inventory-json", default=str(DEFAULT_INVENTORY_JSON))
    parser.add_argument("--sha256-large-jsonl", action="store_true")
    args = parser.parse_args(argv)

    manifest, inventory = build_phase52_final_audit_manifest(
        sessions_root=args.sessions_root,
        evidence_zip=args.evidence_zip,
        expected_evidence_sha256=args.expected_evidence_sha256,
        audit_note=args.audit_note,
        sha256_large_jsonl=args.sha256_large_jsonl,
    )
    _write_json(args.output_json, manifest)
    _write_text(args.output_md, render_phase52_final_audit_markdown(manifest))
    _write_json(args.inventory_json, inventory)
    print(f"Phase 5.2F final audit manifest: {args.output_json}")
    print(f"Phase 5.2F artifact inventory: {args.inventory_json}")
    return 0


def build_phase52_final_audit_manifest(
    *,
    sessions_root: str | Path = DEFAULT_SESSIONS_ROOT,
    evidence_zip: str | Path = DEFAULT_EVIDENCE_ZIP,
    expected_evidence_sha256: str = EXPECTED_EVIDENCE_SHA256,
    audit_note: str | Path | None = DEFAULT_AUDIT_NOTE,
    sha256_large_jsonl: bool = False,
    generated_at_utc: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = Path(sessions_root)
    evidence_path = Path(evidence_zip)
    expected_hash = str(expected_evidence_sha256).strip().lower()
    sessions = [_summarize_session(session_dir) for session_dir in _session_dirs(root)]
    raw_sessions = [session for session in sessions if session["session_kind"] == "raw"]
    repaired_sessions = [session for session in sessions if session["session_kind"] == "repaired_eval"]
    runtime_pass_sessions = [session for session in raw_sessions if session["status"] == "pass"]
    failed_raw_sessions = [session for session in raw_sessions if session["status"] == "fail"]
    repaired_audit_pass_sessions = [session for session in repaired_sessions if _is_repaired_audit_pass(session)]
    research_usable_sessions = [
        _usable_session_entry(session, usability_mode="raw_runtime_pass")
        for session in runtime_pass_sessions
    ] + [
        _usable_session_entry(session, usability_mode="audit_only_repaired_eval")
        for session in repaired_audit_pass_sessions
    ]

    evidence = _build_evidence_report(evidence_path, expected_hash, audit_note=Path(audit_note) if audit_note else None)
    warnings = _manifest_warnings(
        sessions=sessions,
        evidence=evidence,
        raw_pass_count=len(runtime_pass_sessions),
        repaired_audit_pass_count=len(repaired_audit_pass_sessions),
    )
    status = _final_status(
        raw_pass_count=len(runtime_pass_sessions),
        repaired_audit_pass_count=len(repaired_audit_pass_sessions),
        evidence=evidence,
    )
    readiness = _readiness_decision(
        raw_pass_sessions=runtime_pass_sessions,
        repaired_audit_pass_sessions=repaired_audit_pass_sessions,
        evidence=evidence,
    )
    audit_notes = _audit_notes(raw_sessions=raw_sessions, repaired_sessions=repaired_sessions)
    generated_at = generated_at_utc or _utc_now()
    manifest = {
        "phase": PHASE,
        "status": status,
        "generated_at_utc": generated_at,
        "scan_root": _display_path(root),
        "session_counts": {
            "raw_total": len(raw_sessions),
            "raw_pass": len(runtime_pass_sessions),
            "raw_fail": len(failed_raw_sessions),
            "repaired_eval_total": len(repaired_sessions),
            "repaired_eval_pass": len(repaired_audit_pass_sessions),
            "repaired_eval_fail": len([session for session in repaired_sessions if session["status"] == "fail"]),
        },
        "sessions": sessions,
        "raw_sessions": raw_sessions,
        "repaired_eval_sessions": repaired_sessions,
        "failed_raw_sessions": failed_raw_sessions,
        "runtime_pass_sessions": runtime_pass_sessions,
        "repaired_audit_pass_sessions": repaired_audit_pass_sessions,
        "research_usable_sessions": research_usable_sessions,
        "evidence": evidence,
        "readiness_decision": readiness,
        "phase_boundary": {
            "phase5_ready": False,
            "model_strategy_execution_pnl_scope": False,
            "strict_100ms_policy_relaxed": any(_reported_strict_policy_relaxed(session) for session in sessions),
        },
        "hard_constraints": {
            "no_existing_session_reports_modified": True,
            "no_derived_artifacts_rebuilt": True,
            "no_large_jsonl_full_scan": True,
            "no_sqlite_cache_created": True,
        },
        "warnings": warnings,
        "audit_notes": audit_notes,
    }
    inventory = build_phase52_final_audit_inventory(
        sessions_root=root,
        evidence_zip=evidence_path,
        audit_note=Path(audit_note) if audit_note else None,
        generated_at_utc=generated_at,
        sha256_large_jsonl=sha256_large_jsonl,
    )
    return manifest, inventory


def build_phase52_final_audit_inventory(
    *,
    sessions_root: str | Path = DEFAULT_SESSIONS_ROOT,
    evidence_zip: str | Path = DEFAULT_EVIDENCE_ZIP,
    audit_note: str | Path | None = DEFAULT_AUDIT_NOTE,
    generated_at_utc: str | None = None,
    sha256_large_jsonl: bool = False,
    large_file_threshold_bytes: int = LARGE_FILE_THRESHOLD_BYTES,
) -> dict[str, Any]:
    root = Path(sessions_root)
    evidence_path = Path(evidence_zip)
    note_path = Path(audit_note) if audit_note else None
    files: list[dict[str, Any]] = []
    if root.exists():
        for path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: _display_path(item)):
            files.append(
                artifact_metadata(
                    path,
                    role="session_artifact",
                    large_file_threshold_bytes=large_file_threshold_bytes,
                    sha256_large_jsonl=sha256_large_jsonl,
                )
            )
    for path, role, force_sha in (
        (evidence_path, "session_005_repaired_eval_final_evidence_zip", True),
        (Path(str(evidence_path) + ".sha256"), "session_005_repaired_eval_final_evidence_sha256", False),
        (note_path, "session_005_repaired_eval_audit_note", False),
    ):
        if path is not None and path.exists():
            files.append(
                artifact_metadata(
                    path,
                    role=role,
                    large_file_threshold_bytes=large_file_threshold_bytes,
                    sha256_large_jsonl=sha256_large_jsonl,
                    force_sha256=force_sha,
                )
            )
    return {
        "phase": PHASE,
        "generated_at_utc": generated_at_utc or _utc_now(),
        "scan_root": _display_path(root),
        "large_file_threshold_bytes": large_file_threshold_bytes,
        "sha256_large_jsonl": sha256_large_jsonl,
        "file_count": len(files),
        "large_jsonl_sha256_skipped_count": sum(1 for item in files if item.get("sha256_status") == "skipped_large_file"),
        "files": files,
    }


def artifact_metadata(
    path: str | Path,
    *,
    role: str,
    large_file_threshold_bytes: int = LARGE_FILE_THRESHOLD_BYTES,
    sha256_large_jsonl: bool = False,
    force_sha256: bool = False,
) -> dict[str, Any]:
    target = Path(path)
    exists = target.exists()
    is_file = target.is_file() if exists else False
    size_bytes = target.stat().st_size if exists and is_file else 0
    suffix = target.suffix.lower()
    sha256: str | None = None
    sha256_status = "missing"
    if exists and is_file:
        if suffix == ".jsonl" and size_bytes > large_file_threshold_bytes and not sha256_large_jsonl and not force_sha256:
            sha256_status = "skipped_large_file"
        elif size_bytes > large_file_threshold_bytes and not force_sha256 and suffix not in {".json", ".md", ".txt", ".sha256"}:
            sha256_status = "skipped_large_file"
        else:
            sha256 = _sha256_file(target)
            sha256_status = "computed"
    first_line_status: str | None = None
    if suffix == ".jsonl":
        if not exists or not is_file:
            first_line_status = "missing"
        elif size_bytes > large_file_threshold_bytes:
            first_line_status = "skipped_large_file"
        else:
            first_line_status = _first_nonempty_jsonl_status(target)
    return {
        "path": _display_path(target),
        "role": role,
        "exists": exists,
        "is_file": is_file,
        "size_bytes": size_bytes,
        "mtime_utc": _mtime_utc(target) if exists else None,
        "sha256": sha256,
        "sha256_status": sha256_status,
        "first_nonempty_line_status": first_line_status,
    }


def render_phase52_final_audit_markdown(manifest: dict[str, Any]) -> str:
    counts = _dict(manifest.get("session_counts"))
    evidence = _dict(manifest.get("evidence"))
    readiness = _dict(manifest.get("readiness_decision"))
    lines = [
        f"# Phase 5.2F Final Audit Manifest",
        "",
        f"Phase 5.2F status: {manifest.get('status')}",
        "",
        "Raw `session_005_medium_2h` remains fail if its raw report says fail.",
        "Repaired `session_005_medium_2h_repaired_eval` is an audit-only pass when present and passing.",
        "This is not full long-session validation.",
        "This is not sufficient for model, strategy, execution, or PnL work.",
        "",
        "## Session Counts",
        "",
        f"- Raw pass/fail/total: {counts.get('raw_pass')} / {counts.get('raw_fail')} / {counts.get('raw_total')}",
        f"- Repaired eval pass/fail/total: {counts.get('repaired_eval_pass')} / {counts.get('repaired_eval_fail')} / {counts.get('repaired_eval_total')}",
        "",
        "## Sessions",
        "",
        "| Session | Kind | Duration | Status | Primary Failure | Strict 100ms | Low Latency | Phase5 Ready | Usability |",
        "|---|---|---:|---|---|---:|---:|---:|---|",
    ]
    for session in manifest.get("sessions", []):
        usability = _session_usability_label(_dict(session), manifest)
        lines.append(
            "| "
            + " | ".join(
                [
                    str(session.get("session_id")),
                    str(session.get("session_kind")),
                    str(session.get("duration_sec")),
                    str(session.get("status")),
                    str(session.get("primary_failure")),
                    str(session.get("strict_100ms_observability_ready")),
                    str(session.get("low_latency_ready")),
                    str(session.get("phase5_ready")),
                    usability,
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Evidence",
            "",
            f"- Evidence zip: `{evidence.get('session_005_repaired_eval_zip')}`",
            f"- Expected sha256: `{evidence.get('expected_sha256')}`",
            f"- Actual sha256: `{evidence.get('actual_sha256')}`",
            f"- Evidence zip valid: `{evidence.get('evidence_zip_valid')}`",
            "",
            "## Readiness Decision",
            "",
        ]
    )
    for key, value in readiness.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Next Recommended Phase",
            "",
            "Phase 5.3 Dataset Integrity & Research Readiness",
            "",
            "## Do Not Claim",
            "",
            "- Do not claim all Phase 5.2 sessions passed.",
            "- Do not claim production low-latency readiness.",
            "- Do not claim model training readiness.",
            "- Do not claim trading edge.",
            "",
            "## Warnings",
            "",
        ]
    )
    warnings = list(manifest.get("warnings") or [])
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- none")
    return "\n".join(lines) + "\n"


def _summarize_session(session_dir: Path) -> dict[str, Any]:
    session_id = session_dir.name
    report_path = session_dir / PHASE42H_REPORT
    self_check_path = session_dir / PHASE42H_SELF_CHECK
    stage_profile_path = session_dir / PHASE42H_STAGE_PROFILE
    report = _read_json(report_path)
    metadata_path = session_dir / f"phase_5_2_{session_id}_metadata.json"
    quality_path = session_dir / f"phase_5_2_{session_id}_quality_report.json"
    session_kind = _session_kind(session_id=session_id, report=report, report_exists=report_path.exists())
    status = str(report.get("status") or ("missing" if not report_path.exists() else "unknown"))
    latency_stage_artifact = _dict(report.get("latency_stage_profile_artifact"))
    streaming = _dict(report.get("streaming_finalization"))
    return {
        "session_id": session_id,
        "session_path": _display_path(session_dir),
        "session_kind": session_kind,
        "intended_duration_class": _intended_duration_class(session_id),
        "duration_sec": _float_or_none(report.get("duration_sec"), _dict(report.get("capture")).get("duration_sec")),
        "status": status,
        "primary_failure": report.get("primary_failure"),
        "hard_fail_reasons": _list(report.get("hard_fail_reasons")),
        "failure_classifications": _list(report.get("failure_classifications")),
        "evaluation_mode": report.get("evaluation_mode"),
        "derived_artifact_mode": report.get("derived_artifact_mode"),
        "rebuild_derived_artifacts": report.get("rebuild_derived_artifacts"),
        "fresh_capture_required": report.get("fresh_capture_required"),
        "fresh_capture_performed": report.get("fresh_capture_performed"),
        "skip_capture": report.get("skip_capture"),
        "fixture_mode": report.get("fixture_mode"),
        "latency_profile_status": report.get("latency_profile_status"),
        "hot_path_decoupling_status": report.get("hot_path_decoupling_status"),
        "implementation_status": report.get("implementation_status"),
        "strict_100ms_observability_ready": report.get("strict_100ms_observability_ready"),
        "low_latency_ready": report.get("low_latency_ready"),
        "clock_sync_status": report.get("clock_sync_status"),
        "phase41_runtime_report_status": report.get("phase41_runtime_report_status"),
        "phase41_runtime_ready": report.get("phase41_runtime_ready"),
        "phase5_ready": report.get("phase5_ready"),
        "max_future_gap_ms": report.get("max_future_gap_ms"),
        "warning_reasons": _list(report.get("warning_reasons")),
        "latency_stage_profile_artifact_valid": latency_stage_artifact.get("valid") if latency_stage_artifact else None,
        "streaming_finalization_skipped": streaming.get("skipped") if streaming else None,
        "queue_backpressure_artifact_normalization": report.get("queue_backpressure_artifact_normalization"),
        "report_json_exists": report_path.exists(),
        "self_check_exists": self_check_path.exists(),
        "stage_profile_exists": stage_profile_path.exists(),
        "evidence_files_present": {
            "phase42h_report_json": report_path.exists(),
            "phase42h_self_check_json": self_check_path.exists(),
            "phase42h_latency_stage_profile_json": stage_profile_path.exists(),
            "phase42h_pass_bundle_zip": (session_dir / "phase_4_2h_hotpath_environment_latency_bundle.zip").exists(),
            "phase42h_fail_audit_bundle_zip": (session_dir / "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip").exists(),
            "phase52_metadata_json": metadata_path.exists(),
            "phase52_quality_report_json": quality_path.exists(),
            "phase52_capture_bundle_zip": (session_dir / f"phase_5_2_{session_id}_capture_bundle.zip").exists(),
            "phase52_sha256_txt": (session_dir / f"phase_5_2_{session_id}_sha256.txt").exists(),
        },
    }


def _is_repaired_audit_pass(session: dict[str, Any]) -> bool:
    return (
        session.get("session_kind") == "repaired_eval"
        and session.get("status") == "pass"
        and session.get("evaluation_mode") == "existing_artifacts"
        and session.get("derived_artifact_mode") == "reuse_existing"
        and session.get("rebuild_derived_artifacts") is False
        and session.get("fresh_capture_performed") is False
        and session.get("strict_100ms_observability_ready") is True
        and session.get("low_latency_ready") is True
        and session.get("phase5_ready") is False
    )


def _usable_session_entry(session: dict[str, Any], *, usability_mode: str) -> dict[str, Any]:
    return {
        "session_id": session.get("session_id"),
        "session_path": session.get("session_path"),
        "session_kind": session.get("session_kind"),
        "intended_duration_class": session.get("intended_duration_class"),
        "status": session.get("status"),
        "usability_mode": usability_mode,
        "audit_only": usability_mode == "audit_only_repaired_eval",
    }


def _build_evidence_report(evidence_zip: Path, expected_sha256: str, *, audit_note: Path | None) -> dict[str, Any]:
    actual = _sha256_file(evidence_zip) if evidence_zip.exists() and evidence_zip.is_file() else None
    sha_path = Path(str(evidence_zip) + ".sha256")
    recorded_sha = _read_recorded_sha256(sha_path) if sha_path.exists() else None
    missing = []
    if not evidence_zip.exists():
        missing.append(_display_path(evidence_zip))
    if not sha_path.exists():
        missing.append(_display_path(sha_path))
    expected = expected_sha256.strip().lower()
    return {
        "session_005_repaired_eval_zip": _display_path(evidence_zip),
        "expected_sha256": expected,
        "actual_sha256": actual,
        "sha256_file": _display_path(sha_path),
        "sha256_file_exists": sha_path.exists(),
        "sha256_file_value": recorded_sha,
        "audit_note": _display_path(audit_note) if audit_note and audit_note.exists() else None,
        "audit_note_exists": bool(audit_note and audit_note.exists()),
        "evidence_zip_valid": bool(actual and actual == expected),
        "missing_expected_evidence_files": missing,
    }


def _manifest_warnings(
    *,
    sessions: list[dict[str, Any]],
    evidence: dict[str, Any],
    raw_pass_count: int,
    repaired_audit_pass_count: int,
) -> list[str]:
    warnings: list[str] = []
    if evidence.get("evidence_zip_valid") is not True:
        warnings.append("session_005 repaired eval evidence zip sha256 mismatch or missing")
    if raw_pass_count < 4:
        warnings.append("fewer than 4 raw runtime sessions passed")
    if repaired_audit_pass_count < 1:
        warnings.append("no repaired eval audit-only pass session found")
    for session in sessions:
        if session.get("strict_100ms_observability_ready") is not None and session.get("low_latency_ready") is not None:
            if session.get("strict_100ms_observability_ready") is not session.get("low_latency_ready"):
                warnings.append(f"{session.get('session_id')}: low_latency_ready differs from strict_100ms_observability_ready")
        if session.get("phase5_ready") is True:
            warnings.append(f"{session.get('session_id')}: phase5_ready=true ignored by Phase 5.2F boundary")
        if _reported_strict_policy_relaxed(session):
            warnings.append(f"{session.get('session_id')}: max_future_gap_ms was not 100")
    return _unique(warnings)


def _readiness_decision(
    *,
    raw_pass_sessions: list[dict[str, Any]],
    repaired_audit_pass_sessions: list[dict[str, Any]],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    usable_for_runtime = (
        len(raw_pass_sessions) >= 4
        and bool(repaired_audit_pass_sessions)
        and evidence.get("evidence_zip_valid") is True
    )
    usable_for_latency = any(
        session.get("latency_stage_profile_artifact_valid") is True
        and session.get("strict_100ms_observability_ready") is True
        for session in repaired_audit_pass_sessions
    )
    reason = (
        "4+ raw runtime passes plus repaired eval audit-only evidence support partial pipeline audit"
        if usable_for_runtime
        else "partial collection evidence is insufficient for full Phase 5.2 validation"
    )
    return {
        "usable_for_runtime_pipeline_audit": usable_for_runtime,
        "usable_for_latency_profile_research": usable_for_latency,
        "sufficient_for_long_run_stability_claim": False,
        "sufficient_for_model_training": False,
        "sufficient_for_strategy_backtest": False,
        "sufficient_for_execution_or_pnl": False,
        "decision_reason": reason,
    }


def _final_status(*, raw_pass_count: int, repaired_audit_pass_count: int, evidence: dict[str, Any]) -> str:
    if raw_pass_count >= 4 and repaired_audit_pass_count >= 1 and evidence.get("evidence_zip_valid") is True:
        return "partial_pass_with_repaired_eval_evidence"
    return "partial_collection_incomplete"


def _audit_notes(*, raw_sessions: list[dict[str, Any]], repaired_sessions: list[dict[str, Any]]) -> list[str]:
    notes = [
        "Phase 5.2F closes the current collection as partial collection, not full long-session validation.",
        "Repaired eval sessions are audit-only and do not count as raw runtime passes.",
        "No model, strategy, execution, order placement, PnL, or trading-signal logic is in scope.",
    ]
    if any(session.get("session_id") == "session_005_medium_2h" and session.get("status") == "fail" for session in raw_sessions):
        notes.append("raw session_005_medium_2h remains failed.")
    if any(session.get("status") == "pass" for session in repaired_sessions):
        notes.append("session_005 repaired eval evidence may support artifact usability only.")
    return notes


def _session_kind(*, session_id: str, report: dict[str, Any], report_exists: bool) -> str:
    lowered = session_id.lower()
    if "repaired_eval" in lowered or report.get("evaluation_mode") == "existing_artifacts":
        return "repaired_eval"
    if report_exists:
        return "raw"
    return "unknown"


def _intended_duration_class(session_id: str) -> str:
    lowered = session_id.lower()
    for label in ("sanity_30m", "short_1h", "medium_2h", "long_3h", "long_4h"):
        if label in lowered:
            return label
    return "unknown"


def _session_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted([path for path in root.iterdir() if path.is_dir()], key=lambda item: item.name)


def _reported_strict_policy_relaxed(session: dict[str, Any]) -> bool:
    value = session.get("max_future_gap_ms")
    return value is not None and _num(value) != 100.0


def _session_usability_label(session: dict[str, Any], manifest: dict[str, Any]) -> str:
    for item in manifest.get("research_usable_sessions", []):
        if item.get("session_id") == session.get("session_id"):
            return str(item.get("usability_mode"))
    return "not_usable"


def _first_nonempty_jsonl_status(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    json.loads(stripped)
                except json.JSONDecodeError:
                    return "invalid_json"
                return "valid_json"
    except OSError:
        return "read_error"
    return "no_nonempty_lines"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_recorded_sha256(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    match = re.search(r"\b[a-fA-F0-9]{64}\b", text)
    return match.group(0).lower() if match else None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mtime_utc(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _float_or_none(*values: Any) -> float | None:
    for value in values:
        try:
            result = float(value)
        except (TypeError, ValueError):
            continue
        return result
    return None


def _num(value: Any) -> float:
    number = _float_or_none(value)
    return number if number is not None else 0.0


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys(str(item) for item in items if str(item)))


if __name__ == "__main__":
    raise SystemExit(main())
