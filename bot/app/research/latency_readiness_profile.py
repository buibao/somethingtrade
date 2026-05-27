from __future__ import annotations

from collections import Counter
from pathlib import Path
import json
import math
import shutil
from typing import Any
import zipfile

from app.research.clock_sync_receive_lag import (
    ARTIFACT_DIRECTORIES,
    MAX_CLOCK_OFFSET_DRIFT_MS,
    MAX_NEGATIVE_CORRECTED_LAG_RATE,
    OLD_BUNDLE_PATTERNS,
    SERVER_TIME_RTT_HARD_FAIL_MS,
    SERVER_TIME_RTT_WARNING_MS,
    build_clock_sanity_report,
    build_corrected_hybrid_summary,
    build_receive_lag_raw_vs_corrected,
    compute_clock_offset_summary,
    compute_phase42e_source_report,
    generate_corrected_time_protocol_rows,
)
from app.research.orderbook_labeled_dataset import validate_clean_samples, write_jsonl
from app.research.reference_feed_benchmark import (
    AGGTRADE_REFERENCE_EVENTS,
    BENCHMARK_LABELS,
    BOOKTICKER_REFERENCE_QUOTES,
    REFERENCE_SOURCES,
    REQUIRED_100MS_VALID_RATE,
    TRADE_REFERENCE_EVENTS,
    ReferenceValidationResult,
    generate_benchmark_rows,
    validate_depth_reference_events,
    validate_reference_events,
)
from app.research.time_protocol_benchmark import (
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    TIME_PROTOCOL_LABELS,
    build_protocol_summary,
    generate_time_protocol_rows,
    run_phase42d_leakage_check,
    validate_gitignore_rules,
    validate_timestamp_schema,
)


PHASE = "4.2F+G"
STRICT_LAG_P95_MS = 100.0
STRICT_LAG_P99_MS = 200.0
QUEUE_DEPTH_NEAR_CAPACITY_RATIO = 0.80
QUEUE_PUT_BLOCK_WARNING_MS = 5.0

LATENCY_PROFILE_SAMPLES = Path("data/dataset/phase_4_2fg_latency_profile_samples.jsonl")
CORRECTED_TIME_PROTOCOL_LABELS = Path("data/dataset/phase_4_2fg_corrected_time_protocol_labels.jsonl")
LATENCY_PROFILE_DATASETS_ZIP = Path("data/dataset/phase_4_2fg_latency_profile_datasets.zip")

PHASE42FG_REPORT_JSON = Path("data/reports/phase_4_2fg_latency_readiness_profile_report.json")
PHASE42FG_REPORT_MD = Path("data/reports/phase_4_2fg_latency_readiness_profile_report.md")
PHASE42FG_SELF_CHECK_JSON = Path("data/reports/phase42fg_self_check.json")
PHASE42FG_CLEANUP_REPORT = Path("data/debug/phase_4_2fg_artifact_cleanup.json")
PHASE42FG_CLOCK_OFFSET_SAMPLES = Path("data/debug/phase_4_2fg_clock_offset_samples.json")
PHASE42FG_RECEIVE_LAG_RAW_VS_CORRECTED = Path("data/debug/phase_4_2fg_receive_lag_raw_vs_corrected.json")
PHASE42FG_CORRECTED_HYBRID_SUMMARY = Path("data/debug/phase_4_2fg_corrected_hybrid_summary.json")
PHASE42FG_LATENCY_STAGE_PROFILE = Path("data/debug/phase_4_2fg_latency_stage_profile.json")
PHASE42FG_QUEUE_BACKPRESSURE_REPORT = Path("data/debug/phase_4_2fg_queue_backpressure_report.json")
PHASE42FG_CLOCK_SANITY_REPORT = Path("data/debug/phase_4_2fg_clock_sanity_report.json")
PHASE42FG_LEAKAGE_CHECK = Path("data/debug/phase_4_2fg_leakage_check.json")
PHASE42FG_CAPTURE_DIAGNOSTICS = Path("data/debug/phase_4_2fg_multifeed_capture_diagnostics.json")
PHASE42FG_TYPECHECK_REPORT = Path("data/debug/phase_4_2fg_typecheck_report.txt")
PHASE42FG_PYTEST_OUTPUT = Path("data/debug/phase_4_2fg_pytest_output.txt")
PHASE42FG_INVESTIGATION = Path("data/debug/phase42fg_failure_investigation.md")
PHASE42FG_PASS_BUNDLE = Path("phase_4_2fg_latency_readiness_profile_bundle.zip")
PHASE42FG_FAIL_AUDIT_BUNDLE = Path("phase_4_2fg_latency_readiness_profile_fail_audit_bundle.zip")

REQUIRED_STAGE_NAMES = (
    "socket_recv_monotonic_ns",
    "ws_message_received_monotonic_ns",
    "parse_start_monotonic_ns",
    "parse_end_monotonic_ns",
    "book_apply_start_monotonic_ns",
    "book_apply_end_monotonic_ns",
    "sample_build_start_monotonic_ns",
    "sample_emit_monotonic_ns",
    "queue_put_monotonic_ns",
    "writer_enqueue_monotonic_ns",
    "file_write_start_monotonic_ns",
    "file_write_end_monotonic_ns",
)

LATENCY_METRIC_NAMES = (
    "socket_to_parse_start_ms",
    "parse_duration_ms",
    "parse_to_apply_start_ms",
    "book_apply_duration_ms",
    "apply_to_sample_emit_ms",
    "input_queue_put_to_sample_emit_ms",
    "queue_wait_ms",
    "writer_wait_ms",
    "file_write_duration_ms",
    "end_to_end_local_hot_path_ms",
)

PHASE42FG_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "implementation_status",
        "fresh_capture_status",
        "clock_sync_status",
        "readiness_semantics_status",
        "latency_profile_status",
        "strict_100ms_observability_status",
        "protocol_decision_status",
        "primary_failure",
        "failure_classifications",
        "market_time_label_ready",
        "strict_100ms_observability_ready",
        "relaxed_250ms_observability_candidate",
        "low_latency_ready",
        "phase5_ready",
        "symbol",
        "duration_sec",
        "max_future_gap_ms",
        "clock_offset_summary",
        "receive_lag_summary",
        "hot_path_latency_summary",
        "queue_backpressure_summary",
        "sources",
        "selected_protocol_candidate",
        "selected_operational_budget_ms",
        "readiness_decision_reason",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

PHASE42FG_REQUIRED_BUNDLE_FILES = (
    "data/reports/phase_4_2fg_latency_readiness_profile_report.json",
    "data/reports/phase_4_2fg_latency_readiness_profile_report.md",
    "data/reports/phase42fg_self_check.json",
    "data/debug/phase_4_2fg_artifact_cleanup.json",
    "data/debug/phase_4_2fg_clock_offset_samples.json",
    "data/debug/phase_4_2fg_receive_lag_raw_vs_corrected.json",
    "data/debug/phase_4_2fg_corrected_hybrid_summary.json",
    "data/debug/phase_4_2fg_latency_stage_profile.json",
    "data/debug/phase_4_2fg_queue_backpressure_report.json",
    "data/debug/phase_4_2fg_clock_sanity_report.json",
    "data/debug/phase_4_2fg_leakage_check.json",
    "data/debug/phase_4_2fg_multifeed_capture_diagnostics.json",
    "data/debug/phase_4_2fg_typecheck_report.txt",
    "data/debug/phase_4_2fg_pytest_output.txt",
)


def cleanup_phase42fg_artifacts(root: str | Path) -> dict[str, Any]:
    root_path = Path(root).resolve()
    deleted_files: list[str] = []
    missing_files_skipped: list[str] = []
    errors: list[str] = []

    for relative in ARTIFACT_DIRECTORIES:
        directory = root_path / relative
        if not directory.exists():
            missing_files_skipped.append(relative)
            continue
        if not _is_within(directory.resolve(), root_path):
            errors.append(f"refusing to clean outside root: {relative}")
            continue
        for child in directory.iterdir():
            display = _relative_display(root_path, child)
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
                deleted_files.append(display)
            except OSError as exc:
                errors.append(f"{display}: {exc}")

    for pattern in OLD_BUNDLE_PATTERNS:
        for path in root_path.glob(pattern):
            if not path.is_file():
                continue
            if not _is_within(path.resolve(), root_path):
                errors.append(f"refusing to delete outside root: {path}")
                continue
            display = _relative_display(root_path, path)
            try:
                path.unlink()
                deleted_files.append(display)
            except OSError as exc:
                errors.append(f"{display}: {exc}")

    report = {
        "cleanup_performed": True,
        "deleted_files": sorted(set(deleted_files)),
        "missing_files_skipped": sorted(set(missing_files_skipped)),
        "errors": errors,
    }
    _write_json(root_path / PHASE42FG_CLEANUP_REPORT, report)
    return report


def run_phase42fg_analysis(
    *,
    root: str | Path,
    symbol: str,
    clock_offset_samples: list[dict[str, Any]],
    clean_samples_path: str | Path = "data/dataset/orderbook_clean_samples.jsonl",
    bookticker_path: str | Path = BOOKTICKER_REFERENCE_QUOTES,
    trade_path: str | Path = TRADE_REFERENCE_EVENTS,
    aggtrade_path: str | Path = AGGTRADE_REFERENCE_EVENTS,
    latency_profile_samples_path: str | Path = LATENCY_PROFILE_SAMPLES,
    receive_labels_path: str | Path = BENCHMARK_LABELS,
    time_protocol_labels_path: str | Path = TIME_PROTOCOL_LABELS,
    corrected_labels_path: str | Path = CORRECTED_TIME_PROTOCOL_LABELS,
    capture: dict[str, Any],
    cleanup_report: dict[str, Any] | None,
    gitignore_validation: dict[str, Any],
    pytest_passed: bool = True,
    typecheck_passed: bool = True,
    typecheck_summary: str = "",
    fresh_capture_required: bool = True,
) -> dict[str, Any]:
    root_path = Path(root)
    clean_path = _resolve(root_path, clean_samples_path)
    clean_validation = validate_clean_samples(clean_path)
    clean_samples = clean_validation.samples if clean_validation.valid else []
    validations: dict[str, ReferenceValidationResult] = {
        "depth_mid": validate_depth_reference_events(clean_samples),
        "bookTicker_mid": validate_reference_events(_resolve(root_path, bookticker_path), reference_source="bookTicker_mid"),
        "trade_price": validate_reference_events(_resolve(root_path, trade_path), reference_source="trade_price"),
        "aggTrade_price": validate_reference_events(_resolve(root_path, aggtrade_path), reference_source="aggTrade_price"),
    }
    references_by_source = {source: validation.valid_events for source, validation in validations.items()}
    timestamp_schema = validate_timestamp_schema(clean_samples, references_by_source)
    receive_rows = generate_benchmark_rows(clean_samples, references_by_source) if clean_samples else []
    write_jsonl(_resolve(root_path, receive_labels_path), receive_rows)
    time_rows = generate_time_protocol_rows(clean_samples, references_by_source, timestamp_schema) if clean_samples else []
    write_jsonl(_resolve(root_path, time_protocol_labels_path), time_rows)
    clock_offset_summary = compute_clock_offset_summary(clock_offset_samples)
    corrected_rows = generate_corrected_time_protocol_rows(
        time_rows,
        estimated_clock_offset_ms=_float_or_none(clock_offset_summary.get("estimated_clock_offset_ms")),
        clock_offset_drift_valid=clock_offset_summary.get("clock_offset_drift_valid") is True,
    )
    for row in corrected_rows:
        row["schema_version"] = "phase_4_2fg_corrected_time_protocol_v1"
    write_jsonl(_resolve(root_path, corrected_labels_path), corrected_rows)
    leakage = run_phase42d_leakage_check(time_rows, output_path=root_path / PHASE42FG_LEAKAGE_CHECK)
    sources = {
        source: compute_phase42e_source_report(
            source=source,
            validation=validations[source],
            time_rows=time_rows,
            corrected_rows=corrected_rows,
            timestamp_schema=timestamp_schema,
            leakage_result=leakage,
        )
        for source in REFERENCE_SOURCES
    }
    clock_sanity = build_clock_sanity_report(clock_offset_summary=clock_offset_summary, sources=sources)
    latency_profile = build_latency_stage_profile(_resolve(root_path, latency_profile_samples_path))
    phase41_report = _read_json(root_path / "data/reports/phase_4_1_orderbook_quality_report.json")
    queue_report = build_queue_backpressure_report(
        phase41_report=phase41_report,
        latency_profile=latency_profile,
    )
    report = build_phase42fg_report(
        symbol=symbol,
        clean_samples=clean_samples,
        sources=sources,
        timestamp_schema=timestamp_schema,
        leakage_result=leakage,
        clock_offset_samples=clock_offset_samples,
        clock_offset_summary=clock_offset_summary,
        clock_sanity=clock_sanity,
        latency_profile=latency_profile,
        queue_report=queue_report,
        phase41_report=phase41_report,
        capture=capture,
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        fresh_capture_required=fresh_capture_required,
        labeled_sample_count=len(corrected_rows),
    )
    if clean_validation.failure_classification:
        report["hard_fail_reasons"].append(f"clean sample validation failed: {clean_validation.failure_classification}")
        report["primary_failure"] = report.get("primary_failure") or "INPUT_DATASET_FAILURE"
        report = evaluate_phase42fg_report(report)
    return report


def build_phase42fg_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    sources: dict[str, dict[str, Any]],
    timestamp_schema: dict[str, Any],
    leakage_result: dict[str, Any],
    clock_offset_samples: list[dict[str, Any]],
    clock_offset_summary: dict[str, Any],
    clock_sanity: dict[str, Any],
    latency_profile: dict[str, Any],
    queue_report: dict[str, Any],
    phase41_report: dict[str, Any],
    capture: dict[str, Any],
    cleanup_report: dict[str, Any] | None,
    gitignore_validation: dict[str, Any],
    pytest_passed: bool,
    typecheck_passed: bool,
    typecheck_summary: str,
    fresh_capture_required: bool,
    labeled_sample_count: int,
) -> dict[str, Any]:
    protocol_summary = build_protocol_summary(
        {
            source: {
                "exchange_time_supported": report.get("exchange_time_supported"),
                "receive_time": report.get("receive_time"),
                "exchange_time": report.get("exchange_time"),
                "hybrid": report.get("corrected_hybrid"),
            }
            for source, report in sources.items()
        }
    )
    corrected_summary = build_corrected_hybrid_summary(sources)
    semantics = compute_readiness_semantics(
        sources=sources,
        leakage_result=leakage_result,
        clock_sanity_report=clock_sanity,
        queue_report=queue_report,
        phase41_report=phase41_report,
        clock_offset_summary=clock_offset_summary,
    )
    warnings = set(_readiness_warnings(semantics, clock_offset_summary, latency_profile, queue_report, sources))
    report = {
        "phase": PHASE,
        "status": "pass",
        "implementation_status": "pass",
        "fresh_capture_status": "pass",
        "clock_sync_status": "pass" if clock_sanity.get("clock_sanity_valid") is True else "fail",
        "readiness_semantics_status": "pass",
        "latency_profile_status": "pass" if int(_num(latency_profile.get("sample_count"))) > 0 else "fail",
        "strict_100ms_observability_status": "pass" if semantics["strict_100ms_observability_ready"] else "fail",
        "protocol_decision_status": "pass",
        "receive_time_coverage_status": protocol_summary.get("receive_time_coverage_status"),
        "exchange_time_coverage_status": protocol_summary.get("exchange_time_coverage_status"),
        "corrected_hybrid_status": "pass" if _any_corrected_hybrid_passes(sources) else "fail",
        "primary_failure": None,
        "failure_classifications": [],
        **semantics,
        "symbol": symbol.upper(),
        "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
        "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
        "fixture_mode": bool(capture.get("fixture_mode", False)),
        "skip_capture": bool(capture.get("skip_capture", False)),
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "horizon_ms": 100,
        "websocket_time_unit": "MILLISECOND",
        "future_receive_lag_hard_gate_used": False,
        "fresh_capture_required": fresh_capture_required,
        "capture": capture,
        "cleanup_report": cleanup_report or {},
        "gitignore_validation": gitignore_validation,
        "pytest_passed": pytest_passed,
        "typecheck_passed": typecheck_passed,
        "typecheck_summary": typecheck_summary,
        "timestamp_schema": timestamp_schema,
        "clock_offset_samples": clock_offset_samples,
        "clock_offset_summary": clock_offset_summary,
        "clock_sanity_report": clock_sanity,
        "leakage_check": leakage_result,
        "protocol_summary": protocol_summary,
        "corrected_hybrid_summary": corrected_summary,
        "receive_lag_summary": build_receive_lag_raw_vs_corrected(sources),
        "hot_path_latency_summary": latency_profile,
        "queue_backpressure_summary": queue_report,
        "phase41_runtime_report": phase41_report,
        "dataset_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker_reference_quotes": _display_path(BOOKTICKER_REFERENCE_QUOTES),
            "trade_reference_events": _display_path(TRADE_REFERENCE_EVENTS),
            "aggtrade_reference_events": _display_path(AGGTRADE_REFERENCE_EVENTS),
            "receive_time_reference_labels": _display_path(BENCHMARK_LABELS),
            "time_protocol_labels": _display_path(TIME_PROTOCOL_LABELS),
            "corrected_time_protocol_labels": _display_path(CORRECTED_TIME_PROTOCOL_LABELS),
            "latency_profile_samples": _display_path(LATENCY_PROFILE_SAMPLES),
            "latency_profile_datasets_zip": _display_path(LATENCY_PROFILE_DATASETS_ZIP),
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": labeled_sample_count,
        "sources": sources,
        "hard_fail_reasons": [],
        "warning_reasons": sorted(warnings),
    }
    return evaluate_phase42fg_report(report)


def compute_readiness_semantics(
    *,
    sources: dict[str, dict[str, Any]],
    leakage_result: dict[str, Any],
    clock_sanity_report: dict[str, Any],
    queue_report: dict[str, Any],
    phase41_report: dict[str, Any],
    clock_offset_summary: dict[str, Any],
) -> dict[str, Any]:
    no_leakage = _num(leakage_result.get("feature_leakage_violations")) == 0 and _num(leakage_result.get("label_leakage_violations")) == 0
    clock_ok = clock_sanity_report.get("clock_sanity_valid") is True
    queue_ok = _num(queue_report.get("queue_dropped_messages")) == 0
    sequence_ok = _num(phase41_report.get("sequence_gap_count")) == 0
    exchange_candidates = [
        (source, report)
        for source, report in sources.items()
        if report.get("exchange_time_supported") is True
        and _num(_dict(report.get("exchange_time")).get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE
    ]
    market_time_label_ready = bool(exchange_candidates) and no_leakage
    strict_candidate = _select_strict_100ms_candidate(
        exchange_candidates,
        no_leakage=no_leakage,
        clock_ok=clock_ok,
        queue_ok=queue_ok,
        sequence_ok=sequence_ok,
        clock_offset_summary=clock_offset_summary,
    )
    strict_ready = strict_candidate is not None
    relaxed_250 = (
        not strict_ready
        and any(
            _num(
                _dict(_dict(report.get("corrected_hybrid")).get("corrected_hybrid_250ms")).get(
                    "valid_rate_eligible_rows"
                )
            )
            >= REQUIRED_100MS_VALID_RATE
            for _, report in exchange_candidates
        )
    )
    if strict_ready:
        reason = "strict_100ms_corrected_hybrid_observability_passed"
    elif relaxed_250:
        reason = "corrected_hybrid_250ms_passed_but_strict_100ms_observability_failed"
    elif market_time_label_ready:
        reason = "market_time_label_ready_but_strict_100ms_observability_failed"
    else:
        reason = "market_time_label_not_ready"
    return {
        "market_time_label_ready": market_time_label_ready,
        "strict_100ms_observability_ready": strict_ready,
        "relaxed_250ms_observability_candidate": relaxed_250,
        "low_latency_ready": strict_ready,
        "phase5_ready": False,
        "selected_protocol_candidate": strict_candidate,
        "selected_operational_budget_ms": 100 if strict_candidate is not None else None,
        "readiness_decision_reason": reason,
    }


def build_latency_stage_profile(samples_path: str | Path) -> dict[str, Any]:
    rows = _read_jsonl(samples_path)
    stage_counts: dict[str, dict[str, Any]] = {}
    metric_values: dict[str, list[float]] = {name: [] for name in LATENCY_METRIC_NAMES}
    queue_depth_values: list[float] = []
    disk_hot_path = False
    debug_hot_path = False
    batch_writer_enabled = False
    for row in rows:
        stages = _dict(row.get("stages"))
        metrics = _dict(row.get("metrics"))
        for stage in REQUIRED_STAGE_NAMES:
            value = stages.get(stage)
            stats = stage_counts.setdefault(stage, {"available_count": 0, "stage_not_available_count": 0})
            if isinstance(value, bool) or not isinstance(value, int):
                stats["stage_not_available_count"] += 1
            else:
                stats["available_count"] += 1
        for metric in LATENCY_METRIC_NAMES:
            value = _float_or_none(metrics.get(metric))
            if value is not None:
                metric_values[metric].append(value)
        queue_depth = _float_or_none(row.get("queue_size_at_enqueue"))
        if queue_depth is not None:
            queue_depth_values.append(queue_depth)
        disk_hot_path = disk_hot_path or row.get("disk_write_on_hot_path") is True
        debug_hot_path = debug_hot_path or row.get("debug_logging_on_hot_path") is True
        batch_writer_enabled = batch_writer_enabled or row.get("batch_writer_enabled") is True
    metric_summary = {metric: _series_summary(values) for metric, values in metric_values.items()}
    unavailable = {
        stage: "stage_not_available"
        for stage, stats in stage_counts.items()
        if int(stats["available_count"]) == 0
    }
    return {
        "performed": bool(rows),
        "sample_count": len(rows),
        "stage_availability": stage_counts,
        "unavailable_stages": unavailable,
        "metrics": metric_summary,
        "queue_depth_from_latency_samples": _series_summary(queue_depth_values),
        "disk_write_on_hot_path": disk_hot_path,
        "debug_logging_on_hot_path": debug_hot_path,
        "batch_writer_enabled": batch_writer_enabled,
        "queue_backpressure_detected": False,
        "stage_profile_path": _display_path(samples_path),
    }


def build_queue_backpressure_report(
    *,
    phase41_report: dict[str, Any],
    latency_profile: dict[str, Any],
) -> dict[str, Any]:
    queue = _dict(phase41_report.get("queue"))
    capacity = _num(queue.get("queue_capacity"))
    depth_p95 = _num(queue.get("queue_depth_p95"))
    queue_backpressure_detected = _num(queue.get("queue_backpressure_events")) > 0
    queue_depth_near_capacity = capacity > 0 and depth_p95 >= capacity * QUEUE_DEPTH_NEAR_CAPACITY_RATIO
    queue_put_block_p95 = _num(queue.get("queue_put_block_p95_ms"))
    warnings: list[str] = []
    if queue_depth_near_capacity:
        warnings.append("queue_depth_near_capacity")
    if queue_put_block_p95 > QUEUE_PUT_BLOCK_WARNING_MS:
        warnings.append("queue_put_block_p95_gt_5ms")
    if _num(queue.get("queue_dropped_messages")) > 0:
        warnings.append("queue_dropped_messages_gt_0")
    if latency_profile.get("disk_write_on_hot_path") is True:
        warnings.append("disk_write_on_hot_path_detected")
    if latency_profile.get("debug_logging_on_hot_path") is True:
        warnings.append("debug_logging_on_hot_path_detected")
    return {
        "performed": bool(queue),
        "queue_max_size": int(_num(queue.get("queue_max_size"))),
        "queue_capacity": int(capacity) if capacity else None,
        "queue_depth_p50": queue.get("queue_depth_p50"),
        "queue_depth_p95": queue.get("queue_depth_p95"),
        "queue_depth_p99": queue.get("queue_depth_p99"),
        "queue_dropped_messages": int(_num(queue.get("queue_dropped_messages"))),
        "queue_backpressure_events": int(_num(queue.get("queue_backpressure_events"))),
        "queue_backpressure_detected": queue_backpressure_detected,
        "queue_depth_near_capacity": queue_depth_near_capacity,
        "queue_put_block_count": int(_num(queue.get("queue_put_block_count"))),
        "queue_put_block_p95_ms": queue.get("queue_put_block_p95_ms"),
        "writer_flush_count": 0,
        "writer_flush_p95_ms": 0.0,
        "disk_write_on_hot_path": latency_profile.get("disk_write_on_hot_path") is True,
        "debug_logging_on_hot_path": latency_profile.get("debug_logging_on_hot_path") is True,
        "batch_writer_enabled": latency_profile.get("batch_writer_enabled") is True,
        "warnings": warnings,
    }


def evaluate_phase42fg_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard: list[str] = [str(reason) for reason in evaluated.get("hard_fail_reasons", [])]
    classifications: list[str] = [str(item) for item in evaluated.get("failure_classifications", []) if item]
    warnings: list[str] = [str(reason) for reason in evaluated.get("warning_reasons", []) if reason]
    implementation_status = str(evaluated.get("implementation_status", "pass"))
    fresh_capture_status = str(evaluated.get("fresh_capture_status", "pass"))
    clock_sync_status = str(evaluated.get("clock_sync_status", "pass"))
    readiness_semantics_status = str(evaluated.get("readiness_semantics_status", "pass"))
    latency_profile_status = str(evaluated.get("latency_profile_status", "pass"))
    protocol_decision_status = str(evaluated.get("protocol_decision_status", "pass"))
    primary: str | None = evaluated.get("primary_failure")

    def add(reason: str, classification: str, *, implementation: bool = False) -> None:
        nonlocal implementation_status, fresh_capture_status, clock_sync_status, readiness_semantics_status
        nonlocal latency_profile_status, protocol_decision_status, primary
        hard.append(reason)
        if classification not in classifications:
            classifications.append(classification)
        primary = primary or classification
        if implementation:
            implementation_status = "fail"
        if classification in {"FRESH_CAPTURE_NOT_PERFORMED", "FRESH_CAPTURE_DURATION_FAILURE"}:
            fresh_capture_status = "fail"
        if classification in {"CLOCK_SYNC_FAILURE", "CLOCK_OFFSET_DRIFT_FAILURE", "SERVER_TIME_RTT_FAILURE"}:
            clock_sync_status = "fail"
        if classification in {"READINESS_SEMANTICS_FAILURE", "PHASE5_READY_FORBIDDEN"}:
            readiness_semantics_status = "fail"
            protocol_decision_status = "fail"
        if classification in {"LATENCY_PROFILE_MISSING", "QUEUE_BACKPRESSURE_REPORT_MISSING"}:
            latency_profile_status = "fail"

    for error in validate_phase42fg_report_schema(evaluated):
        add(f"report schema invalid: {error}", "REPORT_SCHEMA_FAILURE", implementation=True)
    if evaluated.get("pytest_passed") is not True:
        add("pytest failed", "TEST_FAILURE", implementation=True)
    if evaluated.get("typecheck_passed") is not True:
        add("typecheck/compileall failed", "TYPECHECK_FAILURE", implementation=True)
    if _dict(evaluated.get("gitignore_validation")).get("passed") is not True:
        add("generated artifact .gitignore rules missing", "GITIGNORE_POLICY_FAILURE", implementation=True)
    cleanup = _dict(evaluated.get("cleanup_report"))
    if evaluated.get("fresh_capture_required") is True and cleanup.get("cleanup_performed") is not True:
        add("artifact cleanup was not performed", "ARTIFACT_CLEANUP_FAILURE", implementation=True)
    if cleanup.get("errors"):
        add("artifact cleanup failed", "ARTIFACT_CLEANUP_FAILURE", implementation=True)
    if evaluated.get("fresh_capture_required") is True:
        if evaluated.get("fresh_capture_performed") is not True or evaluated.get("fixture_mode") is True or evaluated.get("skip_capture") is True:
            add("fresh 30-minute capture was not performed", "FRESH_CAPTURE_NOT_PERFORMED")
        if _num(evaluated.get("duration_sec")) < 1800:
            add("fresh capture duration_sec < 1800", "FRESH_CAPTURE_DURATION_FAILURE")
    if evaluated.get("max_future_gap_ms") != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        add("max_future_gap_ms was relaxed", "HORIZON_100MS_POLICY_RELAXED", implementation=True)
    if evaluated.get("future_receive_lag_hard_gate_used") is not False:
        add("future_receive_lag used as hard validity gate", "FUTURE_RECEIVE_LAG_GATE_FAILURE", implementation=True)
    clock_summary = _dict(evaluated.get("clock_offset_summary"))
    if clock_summary.get("estimated_clock_offset_ms") is None:
        add("clock offset was not computed", "CLOCK_SYNC_FAILURE")
    if clock_summary.get("offset_drift_ms") is None:
        add("clock offset drift was not computed", "CLOCK_SYNC_FAILURE")
    if clock_summary.get("clock_offset_drift_valid") is not True:
        add("clock offset drift exceeded threshold", "CLOCK_OFFSET_DRIFT_FAILURE")
    if _num(clock_summary.get("server_time_rtt_p95_ms")) > SERVER_TIME_RTT_HARD_FAIL_MS:
        add("server-time RTT p95 exceeded hard threshold", "SERVER_TIME_RTT_FAILURE")
    if _num(clock_summary.get("server_time_rtt_p95_ms")) > SERVER_TIME_RTT_WARNING_MS:
        warnings.append("server_time_rtt_p95_elevated")
    if _num(clock_summary.get("offset_abs_drift_ms")) > 0:
        warnings.append("clock_offset_measured")

    latency = _dict(evaluated.get("hot_path_latency_summary"))
    if latency.get("performed") is not True:
        add("latency stage profile missing", "LATENCY_PROFILE_MISSING", implementation=True)
    queue = _dict(evaluated.get("queue_backpressure_summary"))
    if queue.get("performed") is not True:
        add("queue backpressure report missing", "QUEUE_BACKPRESSURE_REPORT_MISSING", implementation=True)
    if _num(queue.get("queue_dropped_messages")) > 0:
        add("queue_dropped_messages > 0", "QUEUE_DROPPED_MESSAGES_FAILURE")
    if queue.get("disk_write_on_hot_path") is True:
        warnings.append("disk_write_on_hot_path_detected")
    if queue.get("debug_logging_on_hot_path") is True:
        warnings.append("debug_logging_on_hot_path_detected")
    if _num(queue.get("queue_put_block_p95_ms")) > QUEUE_PUT_BLOCK_WARNING_MS:
        warnings.append("queue_put_block_p95_gt_5ms")
    if queue.get("queue_depth_near_capacity") is True:
        warnings.append("queue_depth_near_capacity")

    leakage = _dict(evaluated.get("leakage_check"))
    if leakage.get("performed") is not True:
        add("leakage check missing", "LEAKAGE_FAILURE", implementation=True)
    if _num(leakage.get("feature_leakage_violations")) > 0:
        add("feature leakage violations detected", "FEATURE_LEAKAGE_FAILURE")
    if _num(leakage.get("label_leakage_violations")) > 0:
        add("label leakage violations detected", "LABEL_LEAKAGE_FAILURE")

    sources = _dict(evaluated.get("sources"))
    for source in REFERENCE_SOURCES:
        source_report = _dict(sources.get(source))
        if not source_report:
            add(f"{source} protocol report missing", "PROTOCOL_REPORT_MISSING", implementation=True)
            continue
        if not _dict(source_report.get("receive_time")):
            add(f"{source} receive-time protocol missing", "RECEIVE_TIME_PROTOCOL_MISSING", implementation=True)
        if "exchange_time" not in source_report:
            add(f"{source} exchange-time protocol missing", "EXCHANGE_TIME_PROTOCOL_MISSING", implementation=True)
        if not _dict(source_report.get("corrected_hybrid")):
            add(f"{source} corrected hybrid missing", "CORRECTED_HYBRID_MISSING", implementation=True)
        if source_report.get("exchange_time_supported") is True:
            exchange = _dict(source_report.get("exchange_time"))
            if exchange.get("selection_time_basis") == "local_recv_monotonic_ns":
                add(f"{source} exchange-time protocol uses local receive timestamp", "EXCHANGE_TIME_FAKE_TIMESTAMP", implementation=True)
        if source == "bookTicker_mid" and source_report.get("exchange_time_supported") is True and source_report.get("exchange_timestamp_field_used") not in {"E", "T"}:
            add("bookTicker exchange-time support lacks real E/T timestamp", "BOOKTICKER_FAKE_EXCHANGE_TIMESTAMP", implementation=True)
        for budget_ms in (25, 50, 100, 250):
            metrics = _dict(_dict(source_report.get("corrected_hybrid")).get(f"corrected_hybrid_{budget_ms}ms"))
            if metrics.get("future_receive_lag_hard_gate_used") is not False:
                add(f"{source} corrected_hybrid_{budget_ms}ms used future lag as gate", "FUTURE_RECEIVE_LAG_GATE_FAILURE", implementation=True)
            if int(metrics.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
                add(f"{source} corrected_hybrid_{budget_ms}ms max_future_gap_ms != 100", "HORIZON_100MS_POLICY_RELAXED", implementation=True)

    if evaluated.get("low_latency_ready") is not evaluated.get("strict_100ms_observability_ready"):
        add("low_latency_ready does not equal strict_100ms_observability_ready", "READINESS_SEMANTICS_FAILURE", implementation=True)
    if evaluated.get("phase5_ready") is not False:
        add("phase5_ready must be false in Phase 4.2F+G", "PHASE5_READY_FORBIDDEN", implementation=True)
    if evaluated.get("low_latency_ready") is True and evaluated.get("strict_100ms_observability_ready") is not True:
        add("low_latency_ready=true while strict_100ms_observability_ready=false", "READINESS_SEMANTICS_FAILURE", implementation=True)
    if not str(evaluated.get("readiness_decision_reason") or "").strip():
        add("readiness_decision_reason missing", "READINESS_SEMANTICS_FAILURE", implementation=True)

    strict_ready = evaluated.get("strict_100ms_observability_ready") is True
    evaluated["implementation_status"] = implementation_status
    evaluated["fresh_capture_status"] = fresh_capture_status
    evaluated["clock_sync_status"] = clock_sync_status
    evaluated["readiness_semantics_status"] = readiness_semantics_status
    evaluated["latency_profile_status"] = latency_profile_status
    evaluated["strict_100ms_observability_status"] = "pass" if strict_ready else "fail"
    evaluated["protocol_decision_status"] = protocol_decision_status
    evaluated["status"] = "fail" if hard else "pass"
    evaluated["primary_failure"] = primary if hard else None
    evaluated["failure_classifications"] = sorted(set(classifications)) if hard else []
    evaluated["hard_fail_reasons"] = list(dict.fromkeys(hard))
    evaluated["warning_reasons"] = sorted(set(warnings))
    return evaluated


def validate_phase42fg_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42FG_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in (
        "implementation_status",
        "fresh_capture_status",
        "clock_sync_status",
        "readiness_semantics_status",
        "latency_profile_status",
        "strict_100ms_observability_status",
        "protocol_decision_status",
    ):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    if report.get("phase") != PHASE:
        errors.append("phase must be 4.2F+G")
    if report.get("max_future_gap_ms") != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        errors.append("max_future_gap_ms must be 100")
    if report.get("low_latency_ready") is not report.get("strict_100ms_observability_ready"):
        errors.append("low_latency_ready must equal strict_100ms_observability_ready")
    if report.get("phase5_ready") is not False:
        errors.append("phase5_ready must be false")
    sources = report.get("sources")
    if not isinstance(sources, dict):
        errors.append("sources must be an object")
    else:
        for source in REFERENCE_SOURCES:
            if not isinstance(sources.get(source), dict):
                errors.append(f"missing source: {source}")
    return errors


def write_phase42fg_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
    bundle_path: str | Path | None = None,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42fg_report(report)
    _write_json(root_path / PHASE42FG_REPORT_JSON, report)
    _write_text(root_path / PHASE42FG_REPORT_MD, render_phase42fg_markdown(report))
    _write_json(root_path / PHASE42FG_CLEANUP_REPORT, report.get("cleanup_report", {}))
    _write_json(root_path / PHASE42FG_CLOCK_OFFSET_SAMPLES, {"samples": report.get("clock_offset_samples", []), "summary": report.get("clock_offset_summary", {})})
    _write_json(root_path / PHASE42FG_RECEIVE_LAG_RAW_VS_CORRECTED, report.get("receive_lag_summary", {}))
    _write_json(root_path / PHASE42FG_CORRECTED_HYBRID_SUMMARY, report.get("corrected_hybrid_summary", {}))
    _write_json(root_path / PHASE42FG_LATENCY_STAGE_PROFILE, report.get("hot_path_latency_summary", {}))
    _write_json(root_path / PHASE42FG_QUEUE_BACKPRESSURE_REPORT, report.get("queue_backpressure_summary", {}))
    _write_json(root_path / PHASE42FG_CLOCK_SANITY_REPORT, report.get("clock_sanity_report", {}))
    _write_json(root_path / PHASE42FG_LEAKAGE_CHECK, report.get("leakage_check", {}))
    _write_json(root_path / PHASE42FG_CAPTURE_DIAGNOSTICS, _dict(_dict(report.get("capture")).get("capture_diagnostics")))
    _write_text(root_path / PHASE42FG_PYTEST_OUTPUT, pytest_output)
    typecheck_path = root_path / PHASE42FG_TYPECHECK_REPORT
    if not typecheck_path.exists():
        _write_text(typecheck_path, str(report.get("typecheck_summary") or "typecheck not run\n"))
    classification = None if report.get("status") == "pass" else classify_phase42fg_failure(report)
    self_check = {
        "phase": PHASE,
        "passed": report.get("status") == "pass",
        "status": report.get("status"),
        "implementation_status": report.get("implementation_status"),
        "fresh_capture_status": report.get("fresh_capture_status"),
        "clock_sync_status": report.get("clock_sync_status"),
        "readiness_semantics_status": report.get("readiness_semantics_status"),
        "latency_profile_status": report.get("latency_profile_status"),
        "strict_100ms_observability_ready": report.get("strict_100ms_observability_ready"),
        "low_latency_ready": report.get("low_latency_ready"),
        "phase5_ready": report.get("phase5_ready"),
        "relaxed_250ms_observability_candidate": report.get("relaxed_250ms_observability_candidate"),
        "selected_protocol_candidate": report.get("selected_protocol_candidate"),
        "readiness_decision_reason": report.get("readiness_decision_reason"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42FG_REPORT_JSON),
        "report_md_path": _display_path(PHASE42FG_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42FG_PYTEST_OUTPUT),
        "typecheck_report_path": _display_path(PHASE42FG_TYPECHECK_REPORT),
        "bundle_path": _display_path(bundle_path or (PHASE42FG_PASS_BUNDLE if report.get("status") == "pass" else PHASE42FG_FAIL_AUDIT_BUNDLE)),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42FG_SELF_CHECK_JSON, self_check)
    if report.get("status") != "pass":
        write_phase42fg_failure_investigation(root=root_path, report=report, classification=classification)


def render_phase42fg_markdown(report: dict[str, Any]) -> str:
    latency = _dict(report.get("hot_path_latency_summary"))
    queue = _dict(report.get("queue_backpressure_summary"))
    lines = [
        "# Phase 4.2F+G Latency Readiness Profile Report",
        "",
        f"Status: **{report.get('status')}**",
        "",
        "## Readiness",
        "",
        f"- Market-time label ready: `{report.get('market_time_label_ready')}`",
        f"- Strict 100ms observability ready: `{report.get('strict_100ms_observability_ready')}`",
        f"- Relaxed 250ms candidate: `{report.get('relaxed_250ms_observability_candidate')}`",
        f"- Low latency ready: `{report.get('low_latency_ready')}`",
        f"- Phase 5 ready: `{report.get('phase5_ready')}`",
        f"- Decision reason: `{report.get('readiness_decision_reason')}`",
        "",
        "## Clock Offset",
        "",
        f"`{json.dumps(report.get('clock_offset_summary', {}), sort_keys=True)}`",
        "",
        "## Latency Profile",
        "",
        f"- Samples: `{latency.get('sample_count')}`",
        f"- Metrics: `{json.dumps(latency.get('metrics', {}), sort_keys=True)}`",
        "",
        "## Queue",
        "",
        f"`{json.dumps(queue, sort_keys=True)}`",
        "",
        "## Sources",
        "",
    ]
    for source in REFERENCE_SOURCES:
        source_report = _dict(_dict(report.get("sources")).get(source))
        exchange = _dict(source_report.get("exchange_time"))
        corrected = _dict(source_report.get("corrected_hybrid"))
        rates = {key: _dict(value).get("valid_rate_eligible_rows") for key, value in corrected.items()}
        lines.append(
            f"- `{source}` exchange_supported=`{source_report.get('exchange_time_supported')}` "
            f"field=`{source_report.get('exchange_timestamp_field_used')}` "
            f"exchange_rate=`{exchange.get('valid_rate_eligible_rows')}` corrected_hybrid=`{json.dumps(rates, sort_keys=True)}`"
        )
    lines.extend(["", "## Hard Fail Reasons", ""])
    reasons = report.get("hard_fail_reasons", [])
    lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- None")
    lines.extend(["", "## Warnings", ""])
    warnings = report.get("warning_reasons", [])
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- None")
    lines.extend(["", "## Phase Boundary", "", "100ms remains the hard readiness requirement. Phase 5, model, strategy, execution, and PnL work are out of scope.", ""])
    return "\n".join(lines)


def create_phase42fg_dataset_zip(root: str | Path) -> Path:
    root_path = Path(root)
    target = root_path / LATENCY_PROFILE_DATASETS_ZIP
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    dataset_paths = (
        "data/dataset/orderbook_clean_samples.jsonl",
        "data/dataset/bookticker_reference_quotes.jsonl",
        "data/dataset/trade_reference_events.jsonl",
        "data/dataset/aggtrade_reference_events.jsonl",
        "data/dataset/orderbook_reference_benchmark_labels.jsonl",
        "data/dataset/orderbook_time_protocol_benchmark_labels.jsonl",
        "data/dataset/phase_4_2fg_corrected_time_protocol_labels.jsonl",
        "data/dataset/phase_4_2fg_latency_profile_samples.jsonl",
    )
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in dataset_paths:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
    return target


def create_phase42fg_bundle(
    *,
    root: str | Path,
    pass_bundle: bool,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    target = Path(bundle_path) if bundle_path is not None else root_path / (PHASE42FG_PASS_BUNDLE if pass_bundle else PHASE42FG_FAIL_AUDIT_BUNDLE)
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in PHASE42FG_REQUIRED_BUNDLE_FILES:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / PHASE42FG_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42FG_INVESTIGATION))
        dataset_zip = root_path / LATENCY_PROFILE_DATASETS_ZIP
        if dataset_zip.exists() and dataset_zip.is_file():
            archive.write(dataset_zip, _display_path(LATENCY_PROFILE_DATASETS_ZIP))
    missing = phase42fg_bundle_missing_files(target, pass_bundle=pass_bundle)
    if missing:
        raise RuntimeError(f"Phase 4.2F+G bundle missing required files: {missing}")
    return target


def phase42fg_bundle_missing_files(bundle_path: str | Path, *, pass_bundle: bool = True) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    required: list[str] = list(PHASE42FG_REQUIRED_BUNDLE_FILES)
    if not pass_bundle:
        required.append(_display_path(PHASE42FG_INVESTIGATION))
    return [name for name in required if name not in names]


def write_phase42fg_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2F+G Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Status: `{report.get('status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Fresh capture: `{report.get('fresh_capture_status')}`",
        f"- Clock sync: `{report.get('clock_sync_status')}`",
        f"- Latency profile: `{report.get('latency_profile_status')}`",
        f"- Readiness reason: `{report.get('readiness_decision_reason')}`",
        f"- Report path: `{_display_path(PHASE42FG_REPORT_JSON)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Phase Boundary",
        "",
        "No 100ms threshold relaxation was applied. No Phase 5/model/strategy/execution/PnL work was added.",
        "",
    ]
    _write_text(Path(root) / PHASE42FG_INVESTIGATION, "\n".join(lines))


def classify_phase42fg_failure(report: dict[str, Any]) -> str:
    primary = str(report.get("primary_failure") or "")
    classifications = [str(item) for item in report.get("failure_classifications", []) if item]
    known = (
        "ARTIFACT_CLEANUP_FAILURE",
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "GITIGNORE_POLICY_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "FRESH_CAPTURE_DURATION_FAILURE",
        "CLOCK_SYNC_FAILURE",
        "CLOCK_OFFSET_DRIFT_FAILURE",
        "SERVER_TIME_RTT_FAILURE",
        "LATENCY_PROFILE_MISSING",
        "QUEUE_BACKPRESSURE_REPORT_MISSING",
        "QUEUE_DROPPED_MESSAGES_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "READINESS_SEMANTICS_FAILURE",
        "PHASE5_READY_FORBIDDEN",
    )
    for classification in known:
        if classification in primary:
            return classification
    for classification in known:
        if classification in classifications:
            return classification
    return "UNKNOWN_PHASE42FG_FAILURE"


def _select_strict_100ms_candidate(
    exchange_candidates: list[tuple[str, dict[str, Any]]],
    *,
    no_leakage: bool,
    clock_ok: bool,
    queue_ok: bool,
    sequence_ok: bool,
    clock_offset_summary: dict[str, Any],
) -> dict[str, Any] | None:
    if not (no_leakage and clock_ok and queue_ok and sequence_ok):
        return None
    candidates: list[dict[str, Any]] = []
    for source, report in exchange_candidates:
        exchange_rate = _num(_dict(report.get("exchange_time")).get("valid_rate_eligible_rows"))
        metrics = _dict(_dict(report.get("corrected_hybrid")).get("corrected_hybrid_100ms"))
        hybrid_rate = _num(metrics.get("valid_rate_eligible_rows"))
        p95 = _float_or_none(metrics.get("corrected_feature_receive_lag_p95_ms"))
        p99 = _float_or_none(metrics.get("corrected_feature_receive_lag_p99_ms"))
        if (
            hybrid_rate >= REQUIRED_100MS_VALID_RATE
            and p95 is not None
            and p95 <= STRICT_LAG_P95_MS
            and p99 is not None
            and p99 <= STRICT_LAG_P99_MS
        ):
            candidates.append(
                {
                    "source": source,
                    "protocol": "corrected_hybrid_low_latency_protocol",
                    "budget_ms": 100,
                    "exchange_time_valid_rate": exchange_rate,
                    "corrected_hybrid_valid_rate": hybrid_rate,
                    "corrected_feature_receive_lag_p95_ms": p95,
                    "corrected_feature_receive_lag_p99_ms": p99,
                    "clock_offset_summary": clock_offset_summary,
                }
            )
    candidates.sort(key=lambda item: (-float(item["corrected_hybrid_valid_rate"]), str(item["source"])))
    return candidates[0] if candidates else None


def _readiness_warnings(
    semantics: dict[str, Any],
    clock_offset_summary: dict[str, Any],
    latency_profile: dict[str, Any],
    queue_report: dict[str, Any],
    sources: dict[str, dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    if semantics.get("relaxed_250ms_observability_candidate") is True:
        warnings.append("corrected_hybrid_250ms_passes_but_100ms_fails")
    if _num(clock_offset_summary.get("server_time_rtt_p95_ms")) > SERVER_TIME_RTT_WARNING_MS:
        warnings.append("server_time_rtt_p95_elevated")
    if latency_profile.get("disk_write_on_hot_path") is True:
        warnings.append("disk_write_on_hot_path_detected")
    if latency_profile.get("debug_logging_on_hot_path") is True:
        warnings.append("debug_logging_on_hot_path_detected")
    warnings.extend(str(item) for item in queue_report.get("warnings", []) if item)
    for source, report in sources.items():
        metrics = _dict(_dict(report.get("corrected_hybrid")).get("corrected_hybrid_100ms"))
        p95 = _float_or_none(metrics.get("corrected_feature_receive_lag_p95_ms"))
        p99 = _float_or_none(metrics.get("corrected_feature_receive_lag_p99_ms"))
        if p95 is not None and p95 > STRICT_LAG_P95_MS:
            warnings.append(f"{source}_corrected_feature_receive_lag_p95_gt_100ms")
        if p99 is not None and p99 > STRICT_LAG_P99_MS:
            warnings.append(f"{source}_corrected_feature_receive_lag_p99_gt_200ms")
    if _dict(sources.get("bookTicker_mid")).get("exchange_time_supported") is not True:
        warnings.append("bookTicker_mid_missing_exchange_timestamp")
    return warnings


def _any_corrected_hybrid_passes(sources: dict[str, dict[str, Any]]) -> bool:
    return any(
        _num(_dict(metrics).get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE
        for report in sources.values()
        for metrics in _dict(report.get("corrected_hybrid")).values()
    )


def _series_summary(values: list[float]) -> dict[str, Any]:
    clean = sorted(value for value in values if math.isfinite(value))
    return {
        "count": len(clean),
        "p50": _percentile(clean, 0.50),
        "p95": _percentile(clean, 0.95),
        "p99": _percentile(clean, 0.99),
        "max": max(clean) if clean else None,
    }


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("status") == "pass":
        return "Phase 4.2F+G benchmark execution passed; strict 100ms readiness remains separate from 250ms diagnostics."
    return f"Phase 4.2F+G failed with classification {classification}. A fail audit bundle was created."


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _relative_display(root: Path, path: Path) -> str:
    try:
        return _display_path(path.relative_to(root))
    except ValueError:
        return _display_path(path)


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _read_json(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def _finite_values(values: Any) -> list[float]:
    result: list[float] = []
    for value in values:
        number = _float_or_none(value)
        if number is not None:
            result.append(number)
    return result


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _num(value: Any) -> float:
    number = _float_or_none(value)
    return number if number is not None else 0.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _percentile(values: list[float], percentile: float) -> float | None:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = int(round((len(clean) - 1) * percentile))
    return clean[min(max(index, 0), len(clean) - 1)]


def _write_json(path: str | Path | None, payload: Any) -> None:
    if path is None:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
