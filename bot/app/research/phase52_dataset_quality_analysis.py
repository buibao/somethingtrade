from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median
from typing import Any


SCHEMA_VERSION = "phase_5_2_dataset_quality_analysis_v1"
DEFAULT_SESSIONS_ROOT = Path("data/phase_5_2/sessions")
DEFAULT_OUTPUT_JSON = Path("data/reports/phase_5_2_dataset_quality_analysis.json")
DEFAULT_OUTPUT_MD = Path("data/reports/phase_5_2_dataset_quality_analysis.md")
PHASE42H_REPORT = Path("data/reports/phase_4_2h_hotpath_environment_latency_report.json")
MEMORY_FINALIZATION_DELTA_LIMIT_BYTES = 250 * 1024 * 1024
CAPTURE_DURATION_GRACE_SEC = 120.0
MIN_ELIGIBLE_SESSIONS = 4
MIN_PHASE5_CAPTURE_DURATION_SEC = 14_400.0
TOP_GENERATED_FILE_COUNT = 10


def build_phase52_dataset_quality_analysis(sessions_root: str | Path = DEFAULT_SESSIONS_ROOT) -> dict[str, Any]:
    root = Path(sessions_root)
    eligible_sessions: list[dict[str, Any]] = []
    excluded_sessions: list[dict[str, Any]] = []
    failed_session_count = 0

    for session_dir in _session_dirs(root):
        session_id = session_dir.name
        quality_path = session_dir / f"phase_5_2_{session_id}_quality_report.json"
        metadata_path = session_dir / f"phase_5_2_{session_id}_metadata.json"
        hotpath_path = session_dir / PHASE42H_REPORT

        quality = _read_json(quality_path) if quality_path.exists() else {}
        metadata = _read_json(metadata_path) if metadata_path.exists() else {}
        hotpath = _read_json(hotpath_path) if hotpath_path.exists() else {}

        reasons = _eligibility_failure_reasons(
            quality_path=quality_path,
            metadata_path=metadata_path,
            hotpath_path=hotpath_path,
            quality=quality,
            metadata=metadata,
            hotpath=hotpath,
        )
        if reasons:
            excluded_sessions.append({"session_id": session_id, "reason": ", ".join(reasons), "reasons": reasons})
            if _session_failed(quality, metadata, hotpath):
                failed_session_count += 1
            continue

        eligible_sessions.append(_summarize_session(session_id=session_id, quality=quality, metadata=metadata, hotpath=hotpath))

    aggregate = _build_aggregate(eligible_sessions, excluded_sessions=excluded_sessions)
    total_requested = float(aggregate["total_requested_duration_sec"])
    total_capture = float(aggregate["total_capture_duration_sec"])
    total_labeled = int(aggregate["total_labeled_sample_count"])
    total_clean = int(aggregate["total_clean_sample_count"])

    warnings = _build_warnings(eligible_sessions, excluded_sessions)
    phase5_blockers = _phase5_analysis_blockers(eligible_sessions, aggregate, failed_session_count)
    long_blockers = _long_collection_blockers(eligible_sessions, aggregate, failed_session_count)
    blockers = _unique([*phase5_blockers, *long_blockers])
    ready_for_phase_5_analysis = not phase5_blockers
    ready_for_long_collection = not long_blockers

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "status": "pass" if ready_for_phase_5_analysis else "fail",
        "sessions_root": _display_path(root),
        "eligible_session_count": len(eligible_sessions),
        "excluded_session_count": len(excluded_sessions),
        "failed_session_count": failed_session_count,
        "total_requested_duration_sec": total_requested,
        "total_capture_duration_sec": total_capture,
        "total_labeled_sample_count": total_labeled,
        "total_clean_sample_count": total_clean,
        "sessions": eligible_sessions,
        "excluded_sessions": excluded_sessions,
        "aggregate": aggregate,
        "warnings": warnings,
        "blockers": blockers,
        "ready_for_phase_5_analysis": ready_for_phase_5_analysis,
        "ready_for_long_collection": ready_for_long_collection,
        "no_model_logic": True,
        "no_strategy_logic": True,
        "no_execution_logic": True,
        "no_pnl_logic": True,
        "no_live_trading": True,
    }


def run_phase52_dataset_quality_analysis(
    *,
    sessions_root: str | Path = DEFAULT_SESSIONS_ROOT,
    output_json: str | Path = DEFAULT_OUTPUT_JSON,
    output_md: str | Path = DEFAULT_OUTPUT_MD,
) -> dict[str, Any]:
    report = build_phase52_dataset_quality_analysis(sessions_root)
    _write_json(output_json, report)
    _write_text(output_md, render_phase52_dataset_quality_markdown(report))
    return report


def render_phase52_dataset_quality_markdown(report: dict[str, Any]) -> str:
    aggregate = _dict(report.get("aggregate"))
    blockers = list(report.get("blockers") or [])
    warnings = list(report.get("warnings") or [])
    lines = [
        "# Phase 5.2 Dataset Quality Analysis",
        "",
        f"Status: {report.get('status')}",
        f"Ready for Phase 5 analysis: {report.get('ready_for_phase_5_analysis')}",
        f"Ready for long collection: {report.get('ready_for_long_collection')}",
        f"Eligible sessions: {report.get('eligible_session_count')}",
        f"Excluded sessions: {report.get('excluded_session_count')}",
        f"Failed sessions: {report.get('failed_session_count')}",
        f"Total capture duration sec: {report.get('total_capture_duration_sec')}",
        f"Total labeled samples: {report.get('total_labeled_sample_count')}",
        f"Total clean samples: {report.get('total_clean_sample_count')}",
        "",
        "## Blockers",
    ]
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.extend(["", "## Warnings"])
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    lines.extend(
        [
            "",
            "## Aggregate",
            f"- Capture duration sec min/median/max: {_triple_text(aggregate.get('capture_duration_sec'))}",
            f"- Finalization duration sec min/median/max: {_triple_text(aggregate.get('finalization_duration_sec'))}",
            f"- Bundle duration sec min/median/max: {_triple_text(aggregate.get('bundle_duration_sec'))}",
            f"- Hot path p95 min/median/max: {_triple_text(aggregate.get('end_to_end_local_hot_path_ms_p95'))}",
            f"- Hot path p99 min/median/max: {_triple_text(aggregate.get('end_to_end_local_hot_path_ms_p99'))}",
            f"- Duration anomalies: {', '.join(aggregate.get('sessions_with_duration_anomalies') or []) or 'none'}",
            f"- Memory anomalies: {', '.join(aggregate.get('sessions_with_memory_anomalies') or []) or 'none'}",
            f"- Queue/writer anomalies: {', '.join(aggregate.get('sessions_with_queue_or_writer_anomalies') or []) or 'none'}",
            "",
            "## Eligible Sessions",
        ]
    )
    sessions = list(report.get("sessions") or [])
    if sessions:
        for session in sessions:
            lines.append(
                f"- {session.get('session_id')}: capture={session.get('capture_duration_sec')}, "
                f"labeled={session.get('labeled_sample_count')}, clean={session.get('clean_sample_count')}"
            )
    else:
        lines.append("- none")
    excluded = list(report.get("excluded_sessions") or [])
    lines.extend(["", "## Excluded Sessions"])
    if excluded:
        for session in excluded:
            lines.append(f"- {session.get('session_id')}: {session.get('reason')}")
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _eligibility_failure_reasons(
    *,
    quality_path: Path,
    metadata_path: Path,
    hotpath_path: Path,
    quality: dict[str, Any],
    metadata: dict[str, Any],
    hotpath: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not quality_path.exists():
        reasons.append("missing_quality_report")
    if not metadata_path.exists():
        reasons.append("missing_metadata")
    if not hotpath_path.exists():
        reasons.append("missing_hotpath_report")
    if quality_path.exists() and not quality:
        reasons.append("invalid_quality_report")
    if metadata_path.exists() and not metadata:
        reasons.append("invalid_metadata")
    if hotpath_path.exists() and not hotpath:
        reasons.append("invalid_hotpath_report")
    if not quality or not metadata or not hotpath:
        return reasons
    if quality.get("status") != "pass":
        reasons.append("quality_status_not_pass")
    if quality.get("research_eligible") is not True:
        reasons.append("quality_research_eligible_not_true")
    if quality.get("bundle_sha256_valid") is not True:
        reasons.append("quality_bundle_sha256_valid_not_true")
    if metadata.get("runtime_status") != "pass":
        reasons.append("metadata_runtime_status_not_pass")
    if metadata.get("research_eligible") is not True:
        reasons.append("metadata_research_eligible_not_true")
    if hotpath.get("status") != "pass":
        reasons.append("hotpath_status_not_pass")
    if hotpath.get("strict_100ms_observability_ready") is not True:
        reasons.append("hotpath_strict_100ms_observability_ready_not_true")
    if hotpath.get("low_latency_ready") is not True:
        reasons.append("hotpath_low_latency_ready_not_true")
    return reasons


def _session_failed(quality: dict[str, Any], metadata: dict[str, Any], hotpath: dict[str, Any]) -> bool:
    return (
        (quality and quality.get("status") != "pass")
        or (metadata and metadata.get("runtime_status") != "pass")
        or (hotpath and hotpath.get("status") != "pass")
        or bool(metadata.get("primary_failure"))
        or bool(hotpath.get("primary_failure"))
    )


def _summarize_session(*, session_id: str, quality: dict[str, Any], metadata: dict[str, Any], hotpath: dict[str, Any]) -> dict[str, Any]:
    latency = _dict(_dict(hotpath.get("hot_path_latency_summary")).get("metrics"))
    end_to_end = _dict(latency.get("end_to_end_local_hot_path_ms"))
    queue = _dict(hotpath.get("queue_backpressure_summary"))
    writer = _dict(hotpath.get("writer_batch_report"))
    phase41 = _dict(hotpath.get("phase41_runtime_report"))
    memory = _memory_telemetry(metadata, hotpath)
    warning_reasons = _unique([*(quality.get("warning_reasons") or []), *(hotpath.get("warning_reasons") or [])])
    return {
        "session_id": session_id,
        "requested_duration_sec": _float_or_none(metadata.get("requested_duration_sec"), hotpath.get("duration_sec")),
        "actual_duration_sec": _float_or_none(metadata.get("actual_duration_sec")),
        "capture_duration_sec": _float_or_none(metadata.get("capture_duration_sec"), hotpath.get("capture_duration_sec"), _dict(hotpath.get("capture")).get("capture_duration_sec")),
        "finalization_duration_sec": _float_or_none(metadata.get("finalization_duration_sec"), hotpath.get("finalization_duration_sec")),
        "bundle_duration_sec": _float_or_none(metadata.get("bundle_duration_sec"), hotpath.get("bundle_duration_sec")),
        "total_child_duration_sec": _float_or_none(metadata.get("total_child_duration_sec"), hotpath.get("total_child_duration_sec")),
        "status": hotpath.get("status"),
        "primary_failure": hotpath.get("primary_failure") or metadata.get("primary_failure"),
        "warning_reasons": warning_reasons,
        "strict_100ms_observability_ready": hotpath.get("strict_100ms_observability_ready") is True,
        "low_latency_ready": hotpath.get("low_latency_ready") is True,
        "clock_sync_status": hotpath.get("clock_sync_status"),
        "labeled_sample_count": int(_num(hotpath.get("labeled_sample_count"))),
        "clean_sample_count": int(_num(hotpath.get("clean_sample_count"))),
        "top_generated_files_by_size": _top_generated_files(memory),
        "corrected_hybrid_100ms_valid_rate_by_source": _corrected_hybrid_rates(hotpath),
        "end_to_end_local_hot_path_ms_p50": _float_or_none(end_to_end.get("p50")),
        "end_to_end_local_hot_path_ms_p95": _float_or_none(end_to_end.get("p95")),
        "end_to_end_local_hot_path_ms_p99": _float_or_none(end_to_end.get("p99")),
        "queue_dropped_messages": int(_num(queue.get("queue_dropped_messages"))),
        "writer_dropped_records": int(_num(writer.get("writer_dropped_records"))),
        "writer_error_count": int(_num(writer.get("writer_error_count"))),
        "sequence_gap_count": int(_num(phase41.get("sequence_gap_count"))),
        "memory_finalization_delta_bytes": _float_or_none(memory.get("finalization_memory_delta_bytes")),
        "memory_peak_rss_bytes": int(_num(memory.get("peak_rss_bytes"))) if memory else None,
        "memory_telemetry_available": bool(memory) and memory.get("available") is not False,
    }


def _build_aggregate(sessions: list[dict[str, Any]], *, excluded_sessions: list[dict[str, Any]]) -> dict[str, Any]:
    duration_anomalies = [session["session_id"] for session in sessions if _has_duration_anomaly(session)]
    memory_anomalies = [session["session_id"] for session in sessions if _has_memory_anomaly(session)]
    queue_writer_anomalies = [session["session_id"] for session in sessions if _has_queue_writer_anomaly(session)]
    rate_by_source: dict[str, list[float]] = {}
    for session in sessions:
        for source, rate in _dict(session.get("corrected_hybrid_100ms_valid_rate_by_source")).items():
            value = _float_or_none(rate)
            if value is not None:
                rate_by_source.setdefault(source, []).append(value)
    return {
        "eligible_session_count": len(sessions),
        "excluded_session_count": len(excluded_sessions),
        "total_requested_duration_sec": _sum_field(sessions, "requested_duration_sec"),
        "total_capture_duration_sec": _sum_field(sessions, "capture_duration_sec"),
        "total_labeled_sample_count": int(_sum_field(sessions, "labeled_sample_count")),
        "total_clean_sample_count": int(_sum_field(sessions, "clean_sample_count")),
        "capture_duration_sec": _triple(_values(sessions, "capture_duration_sec")),
        "finalization_duration_sec": _triple(_values(sessions, "finalization_duration_sec")),
        "bundle_duration_sec": _triple(_values(sessions, "bundle_duration_sec")),
        "end_to_end_local_hot_path_ms_p95": _triple(_values(sessions, "end_to_end_local_hot_path_ms_p95")),
        "end_to_end_local_hot_path_ms_p99": _triple(_values(sessions, "end_to_end_local_hot_path_ms_p99")),
        "corrected_hybrid_100ms_valid_rate_by_source": {source: _triple(values) for source, values in sorted(rate_by_source.items())},
        "sessions_with_warnings": [session["session_id"] for session in sessions if session.get("warning_reasons")],
        "sessions_with_duration_anomalies": duration_anomalies,
        "sessions_with_memory_anomalies": memory_anomalies,
        "sessions_with_queue_or_writer_anomalies": queue_writer_anomalies,
    }


def _phase5_analysis_blockers(sessions: list[dict[str, Any]], aggregate: dict[str, Any], failed_session_count: int) -> list[str]:
    blockers = _shared_readiness_blockers(sessions, aggregate, failed_session_count, prefix="phase_5_analysis")
    if float(aggregate.get("total_capture_duration_sec") or 0.0) < MIN_PHASE5_CAPTURE_DURATION_SEC:
        blockers.append("phase_5_analysis_total_capture_duration_sec_lt_14400")
    if any(session.get("strict_100ms_observability_ready") is not True for session in sessions):
        blockers.append("phase_5_analysis_strict_100ms_observability_not_true_for_every_session")
    if any(session.get("low_latency_ready") is not True for session in sessions):
        blockers.append("phase_5_analysis_low_latency_not_true_for_every_session")
    if any(session.get("clock_sync_status") != "pass" for session in sessions):
        blockers.append("phase_5_analysis_clock_sync_status_not_pass_for_every_session")
    return _unique(blockers)


def _long_collection_blockers(sessions: list[dict[str, Any]], aggregate: dict[str, Any], failed_session_count: int) -> list[str]:
    return _shared_readiness_blockers(sessions, aggregate, failed_session_count, prefix="long_collection")


def _shared_readiness_blockers(sessions: list[dict[str, Any]], aggregate: dict[str, Any], failed_session_count: int, *, prefix: str) -> list[str]:
    blockers: list[str] = []
    if len(sessions) < MIN_ELIGIBLE_SESSIONS:
        blockers.append(f"{prefix}_eligible_session_count_lt_4")
    if failed_session_count != 0:
        blockers.append(f"{prefix}_failed_session_count_nonzero")
    if aggregate.get("sessions_with_duration_anomalies"):
        blockers.append(f"{prefix}_duration_anomalies_present")
    if aggregate.get("sessions_with_memory_anomalies"):
        blockers.append(f"{prefix}_memory_anomalies_present")
    if aggregate.get("sessions_with_queue_or_writer_anomalies"):
        blockers.append(f"{prefix}_queue_or_writer_anomalies_present")
    return blockers


def _build_warnings(sessions: list[dict[str, Any]], excluded_sessions: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    if excluded_sessions:
        warnings.append(f"excluded_session_count={len(excluded_sessions)}")
    for session in sessions:
        for reason in session.get("warning_reasons") or []:
            warnings.append(f"{session['session_id']}:{reason}")
    return warnings


def _has_duration_anomaly(session: dict[str, Any]) -> bool:
    requested = _float_or_none(session.get("requested_duration_sec"))
    capture = _float_or_none(session.get("capture_duration_sec"))
    return requested is None or capture is None or capture > requested + CAPTURE_DURATION_GRACE_SEC


def _has_memory_anomaly(session: dict[str, Any]) -> bool:
    delta = _float_or_none(session.get("memory_finalization_delta_bytes"))
    return session.get("memory_telemetry_available") is not True or delta is None or delta > MEMORY_FINALIZATION_DELTA_LIMIT_BYTES


def _has_queue_writer_anomaly(session: dict[str, Any]) -> bool:
    return (
        int(_num(session.get("queue_dropped_messages"))) > 0
        or int(_num(session.get("writer_dropped_records"))) > 0
        or int(_num(session.get("writer_error_count"))) > 0
        or int(_num(session.get("sequence_gap_count"))) > 0
    )


def _corrected_hybrid_rates(hotpath: dict[str, Any]) -> dict[str, float]:
    rates: dict[str, float] = {}
    for source, report in _dict(hotpath.get("sources")).items():
        metrics = _dict(_dict(report).get("corrected_hybrid"))
        h100 = _dict(metrics.get("corrected_hybrid_100ms"))
        rate = _float_or_none(h100.get("valid_rate_eligible_rows"))
        if rate is not None:
            rates[str(source)] = rate
    return rates


def _top_generated_files(memory: dict[str, Any]) -> list[dict[str, Any]]:
    sizes = _dict(memory.get("generated_file_sizes_bytes"))
    items = []
    for path, size in sizes.items():
        size_value = _float_or_none(size)
        if size_value is not None:
            items.append({"path": str(path), "size_bytes": int(size_value)})
    items.sort(key=lambda item: (-int(item["size_bytes"]), str(item["path"])))
    return items[:TOP_GENERATED_FILE_COUNT]


def _memory_telemetry(metadata: dict[str, Any], hotpath: dict[str, Any]) -> dict[str, Any]:
    memory = hotpath.get("memory_telemetry")
    if isinstance(memory, dict) and memory:
        return memory
    memory = metadata.get("memory_telemetry")
    return memory if isinstance(memory, dict) else {}


def _session_dirs(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted((path for path in root.iterdir() if path.is_dir()), key=lambda path: path.name)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        handle.write(text)


def _values(sessions: list[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for session in sessions:
        value = _float_or_none(session.get(field))
        if value is not None:
            values.append(value)
    return values


def _sum_field(sessions: list[dict[str, Any]], field: str) -> float:
    return sum(_values(sessions, field))


def _triple(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "median": None, "max": None}
    ordered = sorted(values)
    return {"min": ordered[0], "median": float(median(ordered)), "max": ordered[-1]}


def _triple_text(value: Any) -> str:
    triple = _dict(value)
    return f"{triple.get('min')}/{triple.get('median')}/{triple.get('max')}"


def _float_or_none(*values: Any) -> float | None:
    for value in values:
        try:
            result = float(value)
        except (TypeError, ValueError):
            continue
        if result == result and result not in {float("inf"), float("-inf")}:
            return result
    return None


def _num(value: Any) -> float:
    number = _float_or_none(value)
    return number if number is not None else 0.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _unique(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
