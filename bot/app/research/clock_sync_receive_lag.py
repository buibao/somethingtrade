from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import math
import shutil
from typing import Any
import zipfile

from app.research.orderbook_labeled_dataset import validate_clean_samples, write_jsonl
from app.research.reference_feed_benchmark import (
    AGGTRADE_REFERENCE_EVENTS,
    BENCHMARK_LABELS,
    BOOKTICKER_REFERENCE_QUOTES,
    REFERENCE_SOURCES,
    REQUIRED_100MS_VALID_RATE,
    SEMANTIC_DESCRIPTIONS,
    SEMANTIC_TYPES,
    TRADE_REFERENCE_EVENTS,
    ReferenceValidationResult,
    generate_benchmark_rows,
    validate_depth_reference_events,
    validate_reference_events,
)
from app.research.time_protocol_benchmark import (
    ALLOWED_CLOCK_SKEW_MS,
    HYBRID_BUDGETS_MS,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    TIME_PROTOCOL_LABELS,
    build_protocol_summary,
    compute_source_protocol_report,
    feature_exchange_ts_ms,
    generate_time_protocol_rows,
    receive_lag_ms,
    run_phase42d_leakage_check,
    source_exchange_ts_ms,
    validate_gitignore_rules,
    validate_timestamp_schema,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE = "4.2E"
MAX_CLOCK_OFFSET_DRIFT_MS = 50.0
SERVER_TIME_RTT_WARNING_MS = 250.0
SERVER_TIME_RTT_HARD_FAIL_MS = 1000.0
MAX_NEGATIVE_CORRECTED_LAG_RATE = 0.001
LOW_RTT_CLOCK_SAMPLE_MAX_MS = 150.0
LOW_RTT_CLOCK_SAMPLE_MIN_COUNT = 2
LOW_RTT_DYNAMIC_MARGIN_MS = 25.0
LOW_RTT_DYNAMIC_MAD_MULTIPLIER = 3.0

CORRECTED_TIME_PROTOCOL_LABELS = Path("data/dataset/phase_4_2e_corrected_time_protocol_labels.jsonl")
CORRECTED_TIME_PROTOCOL_DATASETS_ZIP = Path("data/dataset/phase_4_2e_corrected_time_protocol_datasets.zip")

PHASE42E_REPORT_JSON = Path("data/reports/phase_4_2e_clock_sync_receive_lag_report.json")
PHASE42E_REPORT_MD = Path("data/reports/phase_4_2e_clock_sync_receive_lag_report.md")
PHASE42E_SELF_CHECK_JSON = Path("data/reports/phase42e_self_check.json")
PHASE42E_CLEANUP_REPORT = Path("data/debug/phase_4_2e_artifact_cleanup.json")
PHASE42E_CLOCK_OFFSET_SAMPLES = Path("data/debug/phase_4_2e_clock_offset_samples.json")
PHASE42E_RECEIVE_LAG_RAW_VS_CORRECTED = Path("data/debug/phase_4_2e_receive_lag_raw_vs_corrected.json")
PHASE42E_CORRECTED_HYBRID_SUMMARY = Path("data/debug/phase_4_2e_corrected_hybrid_summary.json")
PHASE42E_CLOCK_SANITY_REPORT = Path("data/debug/phase_4_2e_clock_sanity_report.json")
PHASE42E_LEAKAGE_CHECK = Path("data/debug/phase_4_2e_leakage_check.json")
PHASE42E_CAPTURE_DIAGNOSTICS = Path("data/debug/phase_4_2e_multifeed_capture_diagnostics.json")
PHASE42E_TYPECHECK_REPORT = Path("data/debug/phase_4_2e_typecheck_report.txt")
PHASE42E_PYTEST_OUTPUT = Path("data/debug/phase_4_2e_pytest_output.txt")
PHASE42E_INVESTIGATION = Path("data/debug/phase42e_failure_investigation.md")
PHASE42E_PASS_BUNDLE = Path("phase_4_2e_clock_sync_receive_lag_bundle.zip")
PHASE42E_FAIL_AUDIT_BUNDLE = Path("phase_4_2e_clock_sync_receive_lag_fail_audit_bundle.zip")

ARTIFACT_DIRECTORIES = (
    "data/dataset",
    "data/reports",
    "data/debug",
    "data/cache",
    "data/logs",
    "logs",
    "reports",
    "debug",
    "cache",
)

OLD_BUNDLE_PATTERNS = (
    "phase_4_1_*.zip",
    "phase_4_2*.zip",
    "phase42*.zip",
    "*_runtime_pass_bundle.zip",
    "*_benchmark_bundle.zip",
    "*_dataset_quality_bundle.zip",
    "*_fail_audit_bundle.zip",
)

PHASE42E_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "implementation_status",
        "fresh_capture_status",
        "clock_sync_status",
        "exchange_time_coverage_status",
        "corrected_hybrid_status",
        "protocol_decision_status",
        "low_latency_ready",
        "primary_failure",
        "failure_classifications",
        "symbol",
        "duration_sec",
        "max_future_gap_ms",
        "clock_offset_summary",
        "sources",
        "selected_protocol_candidate",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

PHASE42E_REQUIRED_BUNDLE_FILES = (
    "data/reports/phase_4_2e_clock_sync_receive_lag_report.json",
    "data/reports/phase_4_2e_clock_sync_receive_lag_report.md",
    "data/reports/phase42e_self_check.json",
    "data/debug/phase_4_2e_artifact_cleanup.json",
    "data/debug/phase_4_2e_clock_offset_samples.json",
    "data/debug/phase_4_2e_receive_lag_raw_vs_corrected.json",
    "data/debug/phase_4_2e_corrected_hybrid_summary.json",
    "data/debug/phase_4_2e_clock_sanity_report.json",
    "data/debug/phase_4_2e_leakage_check.json",
    "data/debug/phase_4_2e_multifeed_capture_diagnostics.json",
    "data/debug/phase_4_2e_typecheck_report.txt",
    "data/debug/phase_4_2e_pytest_output.txt",
)


def cleanup_phase42e_artifacts(root: str | Path) -> dict[str, Any]:
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
    _write_json(root_path / PHASE42E_CLEANUP_REPORT, report)
    return report


def build_server_time_sample(
    *,
    sample_id: int,
    phase: str,
    local_wall_before_request_ms: float,
    local_wall_after_response_ms: float,
    binance_server_time_ms: float,
) -> dict[str, Any]:
    midpoint = (local_wall_before_request_ms + local_wall_after_response_ms) / 2.0
    rtt_ms = local_wall_after_response_ms - local_wall_before_request_ms
    return {
        "sample_id": sample_id,
        "phase": phase,
        "local_wall_before_request_ms": local_wall_before_request_ms,
        "local_wall_after_response_ms": local_wall_after_response_ms,
        "local_wall_midpoint_ms": midpoint,
        "binance_server_time_ms": binance_server_time_ms,
        "round_trip_ms": rtt_ms,
        "server_time_rtt_ms": rtt_ms,
        "estimated_clock_offset_ms": midpoint - binance_server_time_ms,
        "accepted_for_clock_offset": None,
        "rejection_reason": None,
    }


def compute_clock_offset_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    before = [sample for sample in samples if sample.get("phase") == "before_capture"]
    after = [sample for sample in samples if sample.get("phase") == "after_capture"]
    offsets = _finite_values(sample.get("estimated_clock_offset_ms") for sample in samples)
    rtts = _finite_values(_sample_server_time_rtt_ms(sample) for sample in samples)
    offset_before = _float_or_none(before[0].get("estimated_clock_offset_ms")) if before else None
    offset_after = _float_or_none(after[-1].get("estimated_clock_offset_ms")) if after else None
    raw_drift = (max(offsets) - min(offsets)) if offsets else None
    raw_before_after_drift = offset_after - offset_before if offset_before is not None and offset_after is not None else None
    rtt_threshold = _low_rtt_acceptance_threshold(rtts)
    accepted_samples: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    discarded_reason_counts: Counter[str] = Counter()
    for sample in samples:
        rtt = _sample_server_time_rtt_ms(sample)
        if rtt is not None:
            sample["server_time_rtt_ms"] = rtt
        offset = _float_or_none(sample.get("estimated_clock_offset_ms"))
        rejection_reason: str | None = None
        if rtt is None:
            rejection_reason = "missing_server_time_rtt"
        elif offset is None:
            rejection_reason = "missing_estimated_clock_offset"
        elif rtt_threshold is None:
            rejection_reason = "clock_sample_threshold_unavailable"
        elif rtt > rtt_threshold:
            rejection_reason = "high_rtt_outlier"

        sample["accepted_for_clock_offset"] = rejection_reason is None
        sample["rejection_reason"] = rejection_reason
        if rejection_reason is None:
            accepted_samples.append(sample)
        else:
            discarded_reason_counts[rejection_reason] += 1
            discarded.append(
                {
                    "sample_id": sample.get("sample_id"),
                    "phase": sample.get("phase"),
                    "server_time_rtt_ms": rtt,
                    "estimated_clock_offset_ms": offset,
                    "reason": rejection_reason,
                }
            )

    accepted_offsets = _finite_values(sample.get("estimated_clock_offset_ms") for sample in accepted_samples)
    trimmed_offsets = _trim_offsets_for_median(accepted_offsets)
    robust_offset = _percentile(trimmed_offsets, 0.50)
    robust_drift = (max(accepted_offsets) - min(accepted_offsets)) if len(accepted_offsets) >= LOW_RTT_CLOCK_SAMPLE_MIN_COUNT else None
    sample_quality_valid = len(accepted_offsets) >= LOW_RTT_CLOCK_SAMPLE_MIN_COUNT
    robust_drift_valid = sample_quality_valid and robust_drift is not None and robust_drift <= MAX_CLOCK_OFFSET_DRIFT_MS
    rtt_p50 = _percentile(rtts, 0.50)
    rtt_p95 = _percentile(rtts, 0.95)
    return {
        "sample_count": len(samples),
        "before_sample_count": len(before),
        "after_sample_count": len(after),
        "offset_before_ms": offset_before,
        "offset_after_ms": offset_after,
        "offset_median_ms": robust_offset,
        "offset_min_ms": min(offsets) if offsets else None,
        "offset_max_ms": max(offsets) if offsets else None,
        "offset_drift_ms": robust_drift,
        "offset_abs_drift_ms": robust_drift,
        "server_time_rtt_p50_ms": rtt_p50,
        "server_time_rtt_p95_ms": rtt_p95,
        "estimated_clock_offset_ms": robust_offset,
        "estimator": "low_rtt_trimmed_median",
        "clock_offset_estimator_strategy": "low_rtt_trimmed_median",
        "raw_clock_offset_drift_ms": raw_drift,
        "raw_before_after_offset_drift_ms": raw_before_after_drift,
        "raw_estimated_clock_offset_ms_values": offsets,
        "raw_server_time_rtt_ms_values": rtts,
        "robust_estimated_clock_offset_ms": robust_offset,
        "robust_offset_drift_ms": robust_drift,
        "robust_clock_offset_drift_valid": robust_drift_valid,
        "accepted_clock_sample_count": len(accepted_offsets),
        "discarded_clock_sample_count": len(discarded),
        "discarded_clock_sample_reasons": discarded,
        "discarded_clock_sample_reason_counts": dict(sorted(discarded_reason_counts.items())),
        "low_rtt_acceptance_threshold_ms": rtt_threshold,
        "clock_offset_sample_quality_valid": sample_quality_valid,
        "min_accepted_clock_sample_count": LOW_RTT_CLOCK_SAMPLE_MIN_COUNT,
        "max_clock_offset_drift_ms": MAX_CLOCK_OFFSET_DRIFT_MS,
        "server_time_rtt_warning_ms": SERVER_TIME_RTT_WARNING_MS,
        "server_time_rtt_hard_fail_ms": SERVER_TIME_RTT_HARD_FAIL_MS,
        "before_after_samples_present": bool(before and after),
        "clock_offset_drift_valid": robust_drift_valid,
        "server_time_rtt_valid": rtt_p95 is not None and float(rtt_p95 or 0.0) <= SERVER_TIME_RTT_HARD_FAIL_MS,
    }


def raw_receive_lag_ms(*, local_recv_wall_ts: Any, exchange_ts_ms: float | None) -> float | None:
    return receive_lag_ms(local_recv_wall_ts=local_recv_wall_ts, exchange_ts_ms=exchange_ts_ms)


def corrected_receive_lag_ms(raw_lag_ms: float | None, estimated_clock_offset_ms: float | None) -> float | None:
    if raw_lag_ms is None or estimated_clock_offset_ms is None:
        return None
    return raw_lag_ms - estimated_clock_offset_ms


def build_corrected_hybrid_label(
    *,
    exchange_label: dict[str, Any],
    corrected_feature_receive_lag_ms: float | None,
    corrected_future_receive_lag_ms: float | None,
    feature_lag_budget_ms: int,
    clock_offset_drift_valid: bool,
    allowed_clock_skew_ms: float = ALLOWED_CLOCK_SKEW_MS,
) -> dict[str, Any]:
    feature_recv_ns = exchange_label.get("feature_local_recv_monotonic_ns")
    future_recv_ns = exchange_label.get("future_reference_local_recv_monotonic_ns")
    base = {
        "protocol": "corrected_hybrid_low_latency_protocol",
        "reference_source": exchange_label.get("reference_source"),
        "horizon_ms": 100,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "feature_lag_budget_ms": feature_lag_budget_ms,
        "allowed_clock_skew_ms": allowed_clock_skew_ms,
        "exchange_time_valid": exchange_label.get("valid") is True,
        "raw_feature_receive_lag_ms": exchange_label.get("feature_receive_lag_ms"),
        "corrected_feature_receive_lag_ms": corrected_feature_receive_lag_ms,
        "raw_future_receive_lag_ms": exchange_label.get("future_receive_lag_ms"),
        "corrected_future_receive_lag_ms": corrected_future_receive_lag_ms,
        "future_receive_lag_is_telemetry_only": True,
        "future_receive_lag_hard_gate_used": False,
        "clock_offset_drift_valid": clock_offset_drift_valid,
        "no_cross_stream_receive_reorder": None,
        "eligible": bool(exchange_label.get("eligible", False)),
        "valid": False,
        "invalid_reason": None,
    }
    if exchange_label.get("eligible") is not True:
        return {**base, "invalid_reason": str(exchange_label.get("invalid_reason") or "EXCHANGE_TIME_NOT_ELIGIBLE")}
    if exchange_label.get("valid") is not True:
        return {**base, "invalid_reason": str(exchange_label.get("invalid_reason") or "EXCHANGE_TIME_INVALID")}
    if not clock_offset_drift_valid:
        return {**base, "invalid_reason": "CLOCK_OFFSET_DRIFT_INVALID"}
    if corrected_feature_receive_lag_ms is None:
        return {**base, "invalid_reason": "CORRECTED_FEATURE_RECEIVE_LAG_MISSING"}
    if corrected_feature_receive_lag_ms < -allowed_clock_skew_ms:
        return {**base, "invalid_reason": "CORRECTED_LAG_CLOCK_SANITY_FAILURE"}
    if corrected_feature_receive_lag_ms > feature_lag_budget_ms:
        return {**base, "invalid_reason": "CORRECTED_FEATURE_RECEIVE_LAG_TOO_HIGH"}
    if not isinstance(feature_recv_ns, int) or not isinstance(future_recv_ns, int):
        return {**base, "no_cross_stream_receive_reorder": False, "invalid_reason": "CROSS_STREAM_RECEIVE_REORDER"}
    no_reorder = future_recv_ns > feature_recv_ns
    base["no_cross_stream_receive_reorder"] = no_reorder
    if not no_reorder:
        return {**base, "invalid_reason": "CROSS_STREAM_RECEIVE_REORDER"}
    return {**base, "valid": True, "invalid_reason": None}


def generate_corrected_time_protocol_rows(
    time_protocol_rows: list[dict[str, Any]],
    *,
    estimated_clock_offset_ms: float | None,
    clock_offset_drift_valid: bool,
    allowed_clock_skew_ms: float = ALLOWED_CLOCK_SKEW_MS,
) -> list[dict[str, Any]]:
    corrected_rows: list[dict[str, Any]] = []
    for row in time_protocol_rows:
        corrected_labels_by_source: dict[str, dict[str, Any]] = {}
        for source in REFERENCE_SOURCES:
            labels = _dict(_dict(row.get("protocol_labels")).get(source))
            receive_label = _dict(labels.get("receive_time"))
            exchange_label = _dict(labels.get("exchange_time"))
            raw_feature = _float_or_none(exchange_label.get("feature_receive_lag_ms"))
            raw_future = _float_or_none(exchange_label.get("future_receive_lag_ms"))
            corrected_feature = corrected_receive_lag_ms(raw_feature, estimated_clock_offset_ms)
            corrected_future = corrected_receive_lag_ms(raw_future, estimated_clock_offset_ms)
            corrected_source = {
                "receive_time": receive_label,
                "exchange_time": exchange_label,
                "raw_feature_receive_lag_ms": raw_feature,
                "corrected_feature_receive_lag_ms": corrected_feature,
                "raw_future_receive_lag_ms": raw_future,
                "corrected_future_receive_lag_ms": corrected_future,
            }
            for budget_ms in HYBRID_BUDGETS_MS:
                corrected_source[f"corrected_hybrid_{budget_ms}ms"] = build_corrected_hybrid_label(
                    exchange_label=exchange_label,
                    corrected_feature_receive_lag_ms=corrected_feature,
                    corrected_future_receive_lag_ms=corrected_future,
                    feature_lag_budget_ms=budget_ms,
                    clock_offset_drift_valid=clock_offset_drift_valid,
                    allowed_clock_skew_ms=allowed_clock_skew_ms,
                )
            corrected_labels_by_source[source] = corrected_source
        corrected_rows.append(
            {
                **row,
                "schema_version": "phase_4_2e_corrected_time_protocol_v1",
                "corrected_protocol_labels": corrected_labels_by_source,
                "clock_offset_estimator": "low_rtt_trimmed_median",
                "estimated_clock_offset_ms": estimated_clock_offset_ms,
                "future_receive_lag_hard_gate_used": False,
            }
        )
    return corrected_rows


def compute_phase42e_source_report(
    *,
    source: str,
    validation: ReferenceValidationResult,
    time_rows: list[dict[str, Any]],
    corrected_rows: list[dict[str, Any]],
    timestamp_schema: dict[str, Any],
    leakage_result: dict[str, Any],
) -> dict[str, Any]:
    base = compute_source_protocol_report(
        source=source,
        validation=validation,
        rows=time_rows,
        timestamp_schema=timestamp_schema,
        leakage_result=leakage_result,
    )
    corrected_labels = [
        _dict(_dict(_dict(row.get("corrected_protocol_labels")).get(source)))
        for row in corrected_rows
        if isinstance(row.get("corrected_protocol_labels"), dict)
    ]
    corrected_hybrid = {
        f"corrected_hybrid_{budget_ms}ms": _corrected_hybrid_metrics(
            [
                _dict(label.get(f"corrected_hybrid_{budget_ms}ms"))
                for label in corrected_labels
                if isinstance(label.get(f"corrected_hybrid_{budget_ms}ms"), dict)
            ],
            budget_ms=budget_ms,
        )
        for budget_ms in HYBRID_BUDGETS_MS
    }
    raw_feature = _finite_values(label.get("raw_feature_receive_lag_ms") for label in corrected_labels)
    raw_future = _finite_values(label.get("raw_future_receive_lag_ms") for label in corrected_labels)
    corrected_feature = _finite_values(label.get("corrected_feature_receive_lag_ms") for label in corrected_labels)
    corrected_future = _finite_values(label.get("corrected_future_receive_lag_ms") for label in corrected_labels)
    return {
        "source": source,
        "semantic_type": SEMANTIC_TYPES[source],
        "semantic_description": SEMANTIC_DESCRIPTIONS[source],
        "exchange_time_supported": base.get("exchange_time_supported"),
        "exchange_timestamp_field_used": base.get("exchange_timestamp_field_used"),
        "unsupported_reason": base.get("unsupported_reason"),
        "receive_time": base.get("receive_time", {}),
        "exchange_time": base.get("exchange_time", {}),
        "raw_receive_lag": _lag_summary(raw_feature, raw_future, prefix="raw"),
        "corrected_receive_lag": _lag_summary(corrected_feature, corrected_future, prefix="corrected"),
        "clock_offset_explains_raw_lag": _clock_offset_explains(raw_feature, corrected_feature),
        "corrected_hybrid": corrected_hybrid,
    }


def build_receive_lag_raw_vs_corrected(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        source: {
            "raw_receive_lag": _dict(report.get("raw_receive_lag")),
            "corrected_receive_lag": _dict(report.get("corrected_receive_lag")),
            "clock_offset_explains_raw_lag": report.get("clock_offset_explains_raw_lag"),
        }
        for source, report in sources.items()
    }


def build_corrected_hybrid_summary(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "corrected_hybrid_valid_rates": {
            source: {
                budget_key: _num(_dict(metrics).get("valid_rate_eligible_rows"))
                for budget_key, metrics in _dict(report.get("corrected_hybrid")).items()
            }
            for source, report in sources.items()
            if report.get("exchange_time_supported") is True
        },
        "supported_exchange_time_sources": sorted(
            source for source, report in sources.items() if report.get("exchange_time_supported") is True
        ),
        "unsupported_exchange_time_sources": sorted(
            source for source, report in sources.items() if report.get("exchange_time_supported") is not True
        ),
    }


def build_clock_sanity_report(
    *,
    clock_offset_summary: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    allowed_clock_skew_ms: float = ALLOWED_CLOCK_SKEW_MS,
    max_negative_corrected_lag_rate: float = MAX_NEGATIVE_CORRECTED_LAG_RATE,
) -> dict[str, Any]:
    source_reports: dict[str, dict[str, Any]] = {}
    corrected_lag_sanity_valid = True
    for source, report in sources.items():
        lag = _dict(report.get("corrected_receive_lag"))
        feature_count = int(_num(lag.get("feature_corrected_receive_lag_count")))
        negative_count = int(_num(lag.get("feature_corrected_receive_lag_below_negative_skew_count")))
        negative_rate = negative_count / feature_count if feature_count else 0.0
        valid = negative_rate <= max_negative_corrected_lag_rate
        corrected_lag_sanity_valid = corrected_lag_sanity_valid and valid
        source_reports[source] = {
            "feature_corrected_receive_lag_count": feature_count,
            "negative_corrected_lag_beyond_skew_count": negative_count,
            "negative_corrected_lag_beyond_skew_rate": negative_rate,
            "corrected_lag_sanity_valid": valid,
        }
    rtt_p95 = _float_or_none(clock_offset_summary.get("server_time_rtt_p95_ms"))
    drift_valid = clock_offset_summary.get("clock_offset_drift_valid") is True
    sample_quality_valid = clock_offset_summary.get("clock_offset_sample_quality_valid") is True
    rtt_valid = rtt_p95 is not None and rtt_p95 <= SERVER_TIME_RTT_HARD_FAIL_MS
    return {
        "performed": True,
        "allowed_clock_skew_ms": allowed_clock_skew_ms,
        "max_negative_corrected_lag_rate": max_negative_corrected_lag_rate,
        "clock_offset_drift_valid": drift_valid,
        "clock_offset_sample_quality_valid": sample_quality_valid,
        "server_time_rtt_valid": rtt_valid,
        "server_time_rtt_warning": rtt_p95 is not None and rtt_p95 > SERVER_TIME_RTT_WARNING_MS,
        "corrected_lag_sanity_valid": corrected_lag_sanity_valid,
        "clock_sanity_valid": sample_quality_valid and drift_valid and rtt_valid and corrected_lag_sanity_valid,
        "sources": source_reports,
    }


def build_phase42e_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    validations: dict[str, ReferenceValidationResult],
    time_rows: list[dict[str, Any]],
    corrected_rows: list[dict[str, Any]],
    timestamp_schema: dict[str, Any],
    leakage_result: dict[str, Any],
    clock_offset_samples: list[dict[str, Any]],
    clock_offset_summary: dict[str, Any],
    capture: dict[str, Any],
    cleanup_report: dict[str, Any] | None,
    gitignore_validation: dict[str, Any],
    pytest_passed: bool = True,
    typecheck_passed: bool = True,
    typecheck_summary: str = "",
    fresh_capture_required: bool = True,
) -> dict[str, Any]:
    sources = {
        source: compute_phase42e_source_report(
            source=source,
            validation=validations[source],
            time_rows=time_rows,
            corrected_rows=corrected_rows,
            timestamp_schema=timestamp_schema,
            leakage_result=leakage_result,
        )
        for source in REFERENCE_SOURCES
    }
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
    clock_sanity = build_clock_sanity_report(clock_offset_summary=clock_offset_summary, sources=sources)
    selected = select_corrected_protocol_candidate(
        sources=sources,
        leakage_result=leakage_result,
        clock_sanity_report=clock_sanity,
        clock_offset_summary=clock_offset_summary,
    )
    low_latency_ready = selected is not None
    corrected_hybrid_status = "pass" if _any_corrected_hybrid_passes(sources) else "fail"
    warnings: list[str] = []
    if clock_offset_summary.get("server_time_rtt_p95_ms") is not None and _num(clock_offset_summary.get("server_time_rtt_p95_ms")) > SERVER_TIME_RTT_WARNING_MS:
        warnings.append("server_time_rtt_p95_elevated")
    if _num(clock_offset_summary.get("offset_abs_drift_ms")) > 0:
        warnings.append("clock_offset_measured")
    if any(_num(_dict(report.get("exchange_time")).get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE for report in sources.values()) and not low_latency_ready:
        warnings.append("exchange_time_market_coverage_passed_but_corrected_hybrid_live_observability_failed")
    if _dict(sources.get("bookTicker_mid")).get("exchange_time_supported") is not True:
        warnings.append("bookTicker_mid_missing_exchange_timestamp")
    if selected is not None and int(selected.get("budget_ms", 0)) == 250:
        strict_rates = [
            _num(_dict(_dict(sources[selected["source"]].get("corrected_hybrid")).get(f"corrected_hybrid_{budget}ms")).get("valid_rate_eligible_rows"))
            for budget in (25, 50, 100)
        ]
        if all(rate < REQUIRED_100MS_VALID_RATE for rate in strict_rates):
            warnings.append("corrected_hybrid_250ms_only_passed_not_strict_100ms_operational")

    report = {
        "phase": PHASE,
        "status": "pass",
        "implementation_status": "pass",
        "fresh_capture_status": "pass",
        "clock_sync_status": "pass" if clock_sanity.get("clock_sanity_valid") is True else "fail",
        "receive_time_coverage_status": protocol_summary.get("receive_time_coverage_status"),
        "exchange_time_coverage_status": protocol_summary.get("exchange_time_coverage_status"),
        "corrected_hybrid_status": corrected_hybrid_status,
        "protocol_decision_status": "pass" if low_latency_ready else "fail",
        "low_latency_ready": low_latency_ready,
        "primary_failure": None,
        "failure_classifications": [],
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
        "dataset_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker_reference_quotes": _display_path(BOOKTICKER_REFERENCE_QUOTES),
            "trade_reference_events": _display_path(TRADE_REFERENCE_EVENTS),
            "aggtrade_reference_events": _display_path(AGGTRADE_REFERENCE_EVENTS),
            "receive_time_reference_labels": _display_path(BENCHMARK_LABELS),
            "time_protocol_labels": _display_path(TIME_PROTOCOL_LABELS),
            "corrected_time_protocol_labels": _display_path(CORRECTED_TIME_PROTOCOL_LABELS),
            "corrected_time_protocol_datasets_zip": _display_path(CORRECTED_TIME_PROTOCOL_DATASETS_ZIP),
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": len(corrected_rows),
        "sources": sources,
        "selected_protocol_candidate": selected,
        "hard_fail_reasons": [],
        "warning_reasons": sorted(set(warnings)),
    }
    return evaluate_phase42e_report(report)


def run_phase42e_analysis(
    *,
    root: str | Path,
    symbol: str,
    clock_offset_samples: list[dict[str, Any]],
    clean_samples_path: str | Path = "data/dataset/orderbook_clean_samples.jsonl",
    bookticker_path: str | Path = BOOKTICKER_REFERENCE_QUOTES,
    trade_path: str | Path = TRADE_REFERENCE_EVENTS,
    aggtrade_path: str | Path = AGGTRADE_REFERENCE_EVENTS,
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
    write_jsonl(_resolve(root_path, corrected_labels_path), corrected_rows)
    leakage = run_phase42d_leakage_check(time_rows, output_path=root_path / PHASE42E_LEAKAGE_CHECK)
    report = build_phase42e_report(
        symbol=symbol,
        clean_samples=clean_samples,
        validations=validations,
        time_rows=time_rows,
        corrected_rows=corrected_rows,
        timestamp_schema=timestamp_schema,
        leakage_result=leakage,
        clock_offset_samples=clock_offset_samples,
        clock_offset_summary=clock_offset_summary,
        capture=capture,
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        fresh_capture_required=fresh_capture_required,
    )
    if clean_validation.failure_classification:
        report["hard_fail_reasons"].append(f"clean sample validation failed: {clean_validation.failure_classification}")
        report["primary_failure"] = report.get("primary_failure") or "INPUT_DATASET_FAILURE"
        report = evaluate_phase42e_report(report)
    return report


def evaluate_phase42e_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard: list[str] = [str(reason) for reason in evaluated.get("hard_fail_reasons", [])]
    classifications: list[str] = [str(item) for item in evaluated.get("failure_classifications", []) if item]
    warnings: list[str] = [str(reason) for reason in evaluated.get("warning_reasons", []) if reason]
    implementation_status = "pass"
    fresh_capture_status = str(evaluated.get("fresh_capture_status", "fail"))
    clock_sync_status = str(evaluated.get("clock_sync_status", "fail"))
    exchange_status = str(evaluated.get("exchange_time_coverage_status", "fail"))
    corrected_hybrid_status = str(evaluated.get("corrected_hybrid_status", "fail"))
    decision_status = str(evaluated.get("protocol_decision_status", "fail"))
    primary: str | None = evaluated.get("primary_failure")

    def add(reason: str, classification: str, *, implementation: bool = False) -> None:
        nonlocal implementation_status, fresh_capture_status, clock_sync_status, corrected_hybrid_status, decision_status, primary
        hard.append(reason)
        if classification not in classifications:
            classifications.append(classification)
        primary = primary or classification
        if implementation:
            implementation_status = "fail"
        if classification in {"FRESH_CAPTURE_NOT_PERFORMED", "FRESH_CAPTURE_DURATION_FAILURE"}:
            fresh_capture_status = "fail"
        if classification in {"CLOCK_SYNC_FAILURE", "CLOCK_OFFSET_SAMPLE_QUALITY_FAILURE", "CLOCK_OFFSET_DRIFT_FAILURE", "SERVER_TIME_RTT_FAILURE", "CORRECTED_LAG_CLOCK_SANITY_FAILURE"}:
            clock_sync_status = "fail"
        if classification == "CORRECTED_HYBRID_FAILURE":
            corrected_hybrid_status = "fail"
        if classification in {"PROTOCOL_DECISION_FAILURE", "CORRECTED_HYBRID_FAILURE"}:
            decision_status = "fail"

    for error in validate_phase42e_report_schema(evaluated):
        add(f"report schema invalid: {error}", "REPORT_SCHEMA_FAILURE", implementation=True)
    if evaluated.get("pytest_passed") is not True:
        add("pytest failed", "TEST_FAILURE", implementation=True)
    if evaluated.get("typecheck_passed") is not True:
        add("typecheck/compileall failed", "TYPECHECK_FAILURE", implementation=True)
    if _dict(evaluated.get("gitignore_validation")).get("passed") is not True:
        add("generated JSONL/heavy artifact .gitignore rules missing", "GITIGNORE_POLICY_FAILURE", implementation=True)
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
    if _num(evaluated.get("clean_sample_count")) <= 0:
        add("input clean sample dataset missing or empty", "INPUT_DATASET_FAILURE")
    if _num(evaluated.get("labeled_sample_count")) <= 0:
        add("corrected time protocol labels were not generated", "INPUT_DATASET_FAILURE")
    if evaluated.get("max_future_gap_ms") != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        add("max_future_gap_ms was relaxed", "HORIZON_100MS_POLICY_RELAXED", implementation=True)
    if evaluated.get("future_receive_lag_hard_gate_used") is not False:
        add("future_receive_lag_ms used as a hard gate", "CORRECTED_HYBRID_FAILURE", implementation=True)

    clock_summary = _dict(evaluated.get("clock_offset_summary"))
    if _num(clock_summary.get("before_sample_count")) <= 0 or _num(clock_summary.get("after_sample_count")) <= 0:
        add("Binance server time before/after samples missing", "CLOCK_SYNC_FAILURE")
    sample_quality_valid = clock_summary.get("clock_offset_sample_quality_valid") is True
    if not sample_quality_valid:
        accepted = clock_summary.get("accepted_clock_sample_count")
        minimum = clock_summary.get("min_accepted_clock_sample_count")
        add(f"too few low-RTT Binance server-time samples accepted for clock offset: {accepted} < {minimum}", "CLOCK_OFFSET_SAMPLE_QUALITY_FAILURE")
    else:
        if clock_summary.get("estimated_clock_offset_ms") is None:
            add("clock offset was not computed", "CLOCK_SYNC_FAILURE")
        if clock_summary.get("offset_drift_ms") is None:
            add("clock offset drift was not computed", "CLOCK_SYNC_FAILURE")
        elif clock_summary.get("clock_offset_drift_valid") is not True:
            add("clock offset drift exceeded threshold", "CLOCK_OFFSET_DRIFT_FAILURE")
    if clock_summary.get("server_time_rtt_valid") is not True:
        add("server-time RTT p95 exceeded hard threshold", "SERVER_TIME_RTT_FAILURE")

    clock_sanity = _dict(evaluated.get("clock_sanity_report"))
    if clock_sanity.get("performed") is not True:
        add("clock sanity report missing", "CLOCK_SYNC_FAILURE", implementation=True)
    if clock_sanity.get("corrected_lag_sanity_valid") is not True:
        add("corrected feature receive lag has too many negative values beyond skew", "CORRECTED_LAG_CLOCK_SANITY_FAILURE")
    if clock_sanity.get("server_time_rtt_warning") is True:
        warnings.append("server_time_rtt_p95_elevated")

    leakage = _dict(evaluated.get("leakage_check"))
    if leakage.get("performed") is not True:
        add("leakage check missing", "LEAKAGE_FAILURE", implementation=True)
    if _num(leakage.get("feature_leakage_violations")) > 0:
        add("feature leakage violations detected", "FEATURE_LEAKAGE_FAILURE")
    if _num(leakage.get("label_leakage_violations")) > 0:
        add("label leakage violations detected", "LABEL_LEAKAGE_FAILURE")

    sources = _dict(evaluated.get("sources"))
    if exchange_status != "pass":
        add("no exchange-time source achieved valid_rate_eligible_rows >= 0.95", "EXCHANGE_TIME_COVERAGE_FAILURE")
    for source in REFERENCE_SOURCES:
        source_report = _dict(sources.get(source))
        if source_report.get("exchange_time_supported") is True:
            exchange = _dict(source_report.get("exchange_time"))
            if exchange.get("selection_time_basis") == "local_recv_monotonic_ns":
                add(f"{source} exchange-time protocol uses local receive timestamp", "EXCHANGE_TIME_FAKE_TIMESTAMP", implementation=True)
        if source == "bookTicker_mid" and source_report.get("exchange_time_supported") is True and source_report.get("exchange_timestamp_field_used") not in {"E", "T"}:
            add("bookTicker exchange-time support lacks real E/T timestamp", "BOOKTICKER_FAKE_EXCHANGE_TIMESTAMP", implementation=True)
        corrected_hybrid = _dict(source_report.get("corrected_hybrid"))
        for budget_ms in HYBRID_BUDGETS_MS:
            key = f"corrected_hybrid_{budget_ms}ms"
            metrics = _dict(corrected_hybrid.get(key))
            if not metrics:
                add(f"{source} {key} missing", "CORRECTED_HYBRID_MISSING", implementation=True)
            if metrics.get("future_receive_lag_hard_gate_used") is not False:
                add(f"{source} {key} used future_receive_lag_ms as hard gate", "CORRECTED_HYBRID_FAILURE", implementation=True)
            if int(metrics.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
                add(f"{source} {key} max_future_gap_ms != 100", "HORIZON_100MS_POLICY_RELAXED", implementation=True)
        if not _dict(source_report.get("raw_receive_lag")):
            add(f"{source} raw receive lag missing", "RECEIVE_LAG_REPORT_FAILURE", implementation=True)
        if not _dict(source_report.get("corrected_receive_lag")):
            add(f"{source} corrected receive lag missing", "RECEIVE_LAG_REPORT_FAILURE", implementation=True)

    if corrected_hybrid_status != "pass":
        if exchange_status == "pass":
            warnings.append("exchange_time_market_coverage_passed_but_corrected_hybrid_live_observability_failed")
        add("no corrected hybrid budget achieved valid_rate_eligible_rows >= 0.95", "CORRECTED_HYBRID_FAILURE")
    if evaluated.get("low_latency_ready") is not True:
        add("no low-latency-ready corrected protocol candidate selected", "PROTOCOL_DECISION_FAILURE")

    hard = list(dict.fromkeys(hard))
    evaluated["implementation_status"] = implementation_status
    evaluated["fresh_capture_status"] = fresh_capture_status if "FRESH_CAPTURE" in " ".join(classifications) else ("fail" if any(c in classifications for c in ("FRESH_CAPTURE_NOT_PERFORMED", "FRESH_CAPTURE_DURATION_FAILURE")) else "pass")
    evaluated["clock_sync_status"] = clock_sync_status
    evaluated["corrected_hybrid_status"] = corrected_hybrid_status
    evaluated["protocol_decision_status"] = decision_status
    evaluated["status"] = "fail" if hard else "pass"
    evaluated["primary_failure"] = primary if hard else None
    evaluated["failure_classifications"] = sorted(set(classifications)) if hard else []
    evaluated["hard_fail_reasons"] = hard
    evaluated["warning_reasons"] = sorted(set(warnings))
    return evaluated


def validate_phase42e_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42E_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in (
        "implementation_status",
        "fresh_capture_status",
        "clock_sync_status",
        "exchange_time_coverage_status",
        "corrected_hybrid_status",
        "protocol_decision_status",
    ):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    if report.get("max_future_gap_ms") != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        errors.append("max_future_gap_ms must be 100")
    sources = report.get("sources")
    if not isinstance(sources, dict):
        errors.append("sources must be an object")
    else:
        for source in REFERENCE_SOURCES:
            source_report = sources.get(source)
            if not isinstance(source_report, dict):
                errors.append(f"missing source: {source}")
                continue
            for key in ("receive_time", "exchange_time", "raw_receive_lag", "corrected_receive_lag", "corrected_hybrid"):
                if key not in source_report:
                    errors.append(f"missing {source} field: {key}")
    if "phase5_ready" in report or "phase_5_ready" in report:
        errors.append("Phase 5 readiness flag is forbidden in Phase 4.2E")
    return errors


def write_phase42e_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
    bundle_path: str | Path | None = None,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42e_report(report)
    _write_json(root_path / PHASE42E_REPORT_JSON, report)
    _write_text(root_path / PHASE42E_REPORT_MD, render_phase42e_markdown(report))
    _write_json(root_path / PHASE42E_CLOCK_OFFSET_SAMPLES, {"samples": report.get("clock_offset_samples", []), "summary": report.get("clock_offset_summary", {})})
    _write_json(root_path / PHASE42E_RECEIVE_LAG_RAW_VS_CORRECTED, build_receive_lag_raw_vs_corrected(_dict(report.get("sources"))))
    _write_json(root_path / PHASE42E_CORRECTED_HYBRID_SUMMARY, report.get("corrected_hybrid_summary", {}))
    _write_json(root_path / PHASE42E_CLOCK_SANITY_REPORT, report.get("clock_sanity_report", {}))
    _write_json(root_path / PHASE42E_LEAKAGE_CHECK, report.get("leakage_check", {}))
    _write_text(root_path / PHASE42E_PYTEST_OUTPUT, pytest_output)
    classification = None if report.get("status") == "pass" else classify_phase42e_failure(report)
    self_check = {
        "phase": PHASE,
        "passed": report.get("status") == "pass",
        "status": report.get("status"),
        "implementation_status": report.get("implementation_status"),
        "fresh_capture_status": report.get("fresh_capture_status"),
        "clock_sync_status": report.get("clock_sync_status"),
        "exchange_time_coverage_status": report.get("exchange_time_coverage_status"),
        "corrected_hybrid_status": report.get("corrected_hybrid_status"),
        "protocol_decision_status": report.get("protocol_decision_status"),
        "low_latency_ready": report.get("low_latency_ready"),
        "selected_protocol_candidate": report.get("selected_protocol_candidate"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42E_REPORT_JSON),
        "report_md_path": _display_path(PHASE42E_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42E_PYTEST_OUTPUT),
        "typecheck_report_path": _display_path(PHASE42E_TYPECHECK_REPORT),
        "bundle_path": _display_path(bundle_path or (PHASE42E_PASS_BUNDLE if report.get("status") == "pass" else PHASE42E_FAIL_AUDIT_BUNDLE)),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42E_SELF_CHECK_JSON, self_check)
    if report.get("status") != "pass":
        write_phase42e_failure_investigation(root=root_path, report=report, classification=classification)


def render_phase42e_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phase 4.2E Clock Sync Receive-Lag Report",
        "",
        f"Status: **{report.get('status')}**",
        "",
        "## Status",
        "",
        f"- Implementation: `{report.get('implementation_status')}`",
        f"- Fresh capture: `{report.get('fresh_capture_status')}`",
        f"- Clock sync: `{report.get('clock_sync_status')}`",
        f"- Exchange-time coverage: `{report.get('exchange_time_coverage_status')}`",
        f"- Corrected hybrid: `{report.get('corrected_hybrid_status')}`",
        f"- Low latency ready: `{report.get('low_latency_ready')}`",
        f"- Selected candidate: `{report.get('selected_protocol_candidate')}`",
        "",
        "## Clock Offset",
        "",
        f"`{json.dumps(report.get('clock_offset_summary', {}), sort_keys=True)}`",
        "",
        "## Sources",
        "",
    ]
    for source in REFERENCE_SOURCES:
        source_report = _dict(_dict(report.get("sources")).get(source))
        exchange = _dict(source_report.get("exchange_time"))
        corrected = _dict(source_report.get("corrected_hybrid"))
        lines.append(
            "- `{source}` exchange_supported=`{supported}` field=`{field}` exchange_rate=`{exchange_rate}` corrected_hybrid=`{rates}`".format(
                source=source,
                supported=source_report.get("exchange_time_supported"),
                field=source_report.get("exchange_timestamp_field_used"),
                exchange_rate=exchange.get("valid_rate_eligible_rows"),
                rates=json.dumps(
                    {key: _dict(value).get("valid_rate_eligible_rows") for key, value in corrected.items()},
                    sort_keys=True,
                ),
            )
        )
    lines.extend(["", "## Hard Fail Reasons", ""])
    reasons = report.get("hard_fail_reasons", [])
    lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- None")
    lines.extend(["", "## Warnings", ""])
    warnings = report.get("warning_reasons", [])
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- None")
    lines.extend(["", "## Phase Boundary", "", "100ms remained a hard requirement. No strategy/model/execution/PnL work is part of Phase 4.2E.", ""])
    return "\n".join(lines)


def create_phase42e_bundle(
    *,
    root: str | Path,
    pass_bundle: bool,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    target = Path(bundle_path) if bundle_path is not None else root_path / (PHASE42E_PASS_BUNDLE if pass_bundle else PHASE42E_FAIL_AUDIT_BUNDLE)
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in PHASE42E_REQUIRED_BUNDLE_FILES:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / PHASE42E_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42E_INVESTIGATION))
        dataset_zip = root_path / CORRECTED_TIME_PROTOCOL_DATASETS_ZIP
        if dataset_zip.exists() and dataset_zip.is_file():
            archive.write(dataset_zip, _display_path(CORRECTED_TIME_PROTOCOL_DATASETS_ZIP))
    missing = phase42e_bundle_missing_files(target, pass_bundle=pass_bundle)
    if missing:
        raise RuntimeError(f"Phase 4.2E bundle missing required files: {missing}")
    return target


def create_phase42e_dataset_zip(root: str | Path) -> Path:
    root_path = Path(root)
    target = root_path / CORRECTED_TIME_PROTOCOL_DATASETS_ZIP
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
        "data/dataset/phase_4_2e_corrected_time_protocol_labels.jsonl",
    )
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in dataset_paths:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
    return target


def phase42e_bundle_missing_files(bundle_path: str | Path, *, pass_bundle: bool = True) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    required: list[str] = list(PHASE42E_REQUIRED_BUNDLE_FILES)
    if not pass_bundle:
        required.append(_display_path(PHASE42E_INVESTIGATION))
    return [name for name in required if name not in names]


def write_phase42e_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2E Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Status: `{report.get('status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Fresh capture: `{report.get('fresh_capture_status')}`",
        f"- Clock sync: `{report.get('clock_sync_status')}`",
        f"- Exchange-time coverage: `{report.get('exchange_time_coverage_status')}`",
        f"- Corrected hybrid: `{report.get('corrected_hybrid_status')}`",
        f"- Report path: `{_display_path(PHASE42E_REPORT_JSON)}`",
        "",
        "## Clock Offset Summary",
        "",
        f"`{json.dumps(report.get('clock_offset_summary', {}), sort_keys=True)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Phase Boundary",
        "",
        "No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.",
        "",
    ]
    _write_text(Path(root) / PHASE42E_INVESTIGATION, "\n".join(lines))


def classify_phase42e_failure(report: dict[str, Any]) -> str:
    primary = str(report.get("primary_failure") or "")
    classifications = [str(item) for item in report.get("failure_classifications", []) if item]
    for classification in (
        "ARTIFACT_CLEANUP_FAILURE",
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "GITIGNORE_POLICY_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "FRESH_CAPTURE_DURATION_FAILURE",
        "CLOCK_SYNC_FAILURE",
        "CLOCK_OFFSET_SAMPLE_QUALITY_FAILURE",
        "CLOCK_OFFSET_DRIFT_FAILURE",
        "SERVER_TIME_RTT_FAILURE",
        "CORRECTED_LAG_CLOCK_SANITY_FAILURE",
        "INPUT_DATASET_FAILURE",
        "EXCHANGE_TIME_COVERAGE_FAILURE",
        "CORRECTED_HYBRID_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "PROTOCOL_DECISION_FAILURE",
    ):
        if classification in primary:
            return classification
    for classification in (
        "ARTIFACT_CLEANUP_FAILURE",
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "GITIGNORE_POLICY_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "FRESH_CAPTURE_DURATION_FAILURE",
        "CLOCK_SYNC_FAILURE",
        "CLOCK_OFFSET_SAMPLE_QUALITY_FAILURE",
        "CLOCK_OFFSET_DRIFT_FAILURE",
        "SERVER_TIME_RTT_FAILURE",
        "CORRECTED_LAG_CLOCK_SANITY_FAILURE",
        "INPUT_DATASET_FAILURE",
        "EXCHANGE_TIME_COVERAGE_FAILURE",
        "CORRECTED_HYBRID_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "PROTOCOL_DECISION_FAILURE",
    ):
        if classification in classifications:
            return classification
    return "UNKNOWN_PHASE42E_FAILURE"


def select_corrected_protocol_candidate(
    *,
    sources: dict[str, dict[str, Any]],
    leakage_result: dict[str, Any],
    clock_sanity_report: dict[str, Any],
    clock_offset_summary: dict[str, Any],
) -> dict[str, Any] | None:
    if _num(leakage_result.get("feature_leakage_violations")) > 0 or _num(leakage_result.get("label_leakage_violations")) > 0:
        return None
    if clock_sanity_report.get("clock_sanity_valid") is not True:
        return None
    candidates: list[dict[str, Any]] = []
    for source, report in sources.items():
        if report.get("exchange_time_supported") is not True:
            continue
        exchange_rate = _num(_dict(report.get("exchange_time")).get("valid_rate_eligible_rows"))
        if exchange_rate < REQUIRED_100MS_VALID_RATE:
            continue
        for budget_key, metrics_value in _dict(report.get("corrected_hybrid")).items():
            metrics = _dict(metrics_value)
            hybrid_rate = _num(metrics.get("valid_rate_eligible_rows"))
            if hybrid_rate >= REQUIRED_100MS_VALID_RATE:
                candidates.append(
                    {
                        "source": source,
                        "protocol": "corrected_hybrid_low_latency_protocol",
                        "budget_ms": int(str(budget_key).removeprefix("corrected_hybrid_").removesuffix("ms")),
                        "exchange_time_valid_rate": exchange_rate,
                        "corrected_hybrid_valid_rate": hybrid_rate,
                        "clock_offset_summary": clock_offset_summary,
                    }
                )
    candidates.sort(key=lambda item: (-float(item["corrected_hybrid_valid_rate"]), int(item["budget_ms"]), str(item["source"])))
    return candidates[0] if candidates else None


def _any_corrected_hybrid_passes(sources: dict[str, dict[str, Any]]) -> bool:
    return any(
        _num(_dict(metrics).get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE
        for report in sources.values()
        for metrics in _dict(report.get("corrected_hybrid")).values()
    )


def _corrected_hybrid_metrics(labels: list[dict[str, Any]], *, budget_ms: int) -> dict[str, Any]:
    eligible = [label for label in labels if label.get("eligible") is True]
    valid = [label for label in eligible if label.get("valid") is True]
    reasons = Counter(
        str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        for label in labels
        if label.get("valid") is not True
    )
    reasons.pop("None", None)
    corrected_feature = _finite_values(label.get("corrected_feature_receive_lag_ms") for label in labels)
    corrected_future = _finite_values(label.get("corrected_future_receive_lag_ms") for label in labels)
    return {
        "horizon_ms": 100,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "feature_lag_budget_ms": budget_ms,
        "future_receive_lag_hard_gate_used": False,
        "future_receive_lag_is_telemetry_only": True,
        "eligible_count": len(eligible),
        "valid_count": len(valid),
        "invalid_count": len(labels) - len(valid),
        "valid_rate_all_rows": len(valid) / len(labels) if labels else 0.0,
        "valid_rate_eligible_rows": len(valid) / len(eligible) if eligible else 0.0,
        "invalid_reason_counts": dict(sorted(reasons.items())),
        "corrected_feature_receive_lag_p50_ms": _percentile(corrected_feature, 0.50),
        "corrected_feature_receive_lag_p95_ms": _percentile(corrected_feature, 0.95),
        "corrected_feature_receive_lag_p99_ms": _percentile(corrected_feature, 0.99),
        "corrected_future_receive_lag_p50_ms": _percentile(corrected_future, 0.50),
        "corrected_future_receive_lag_p95_ms": _percentile(corrected_future, 0.95),
        "corrected_future_receive_lag_p99_ms": _percentile(corrected_future, 0.99),
        "cross_stream_receive_reorder_count": reasons.get("CROSS_STREAM_RECEIVE_REORDER", 0),
        "clock_sanity_violation_count": reasons.get("CORRECTED_LAG_CLOCK_SANITY_FAILURE", 0),
    }


def _lag_summary(feature_values: list[float], future_values: list[float], *, prefix: str) -> dict[str, Any]:
    below_skew = sum(1 for value in feature_values if value < -ALLOWED_CLOCK_SKEW_MS)
    return {
        f"feature_{prefix}_receive_lag_count": len(feature_values),
        f"feature_{prefix}_receive_lag_p50_ms": _percentile(feature_values, 0.50),
        f"feature_{prefix}_receive_lag_p95_ms": _percentile(feature_values, 0.95),
        f"feature_{prefix}_receive_lag_p99_ms": _percentile(feature_values, 0.99),
        f"future_{prefix}_receive_lag_count": len(future_values),
        f"future_{prefix}_receive_lag_p50_ms": _percentile(future_values, 0.50),
        f"future_{prefix}_receive_lag_p95_ms": _percentile(future_values, 0.95),
        f"future_{prefix}_receive_lag_p99_ms": _percentile(future_values, 0.99),
        f"feature_{prefix}_receive_lag_below_negative_skew_count": below_skew,
    }


def _clock_offset_explains(raw_feature: list[float], corrected_feature: list[float]) -> bool | None:
    raw_p50 = _percentile(raw_feature, 0.50)
    corrected_p50 = _percentile(corrected_feature, 0.50)
    if raw_p50 is None or corrected_p50 is None:
        return None
    if raw_p50 <= 1000:
        return False
    return abs(corrected_p50) <= 250


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("status") == "pass":
        return "Phase 4.2E Definition of Done passed; pass audit bundle was created."
    return f"Phase 4.2E failed with classification {classification}. A fail audit bundle was created."


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


def _wall_ts_ms(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _epoch_ms(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp() * 1000.0


def _epoch_ms(value: Any) -> float | None:
    number = _float_or_none(value)
    if number is None:
        return None
    absolute = abs(number)
    if absolute >= 1_000_000_000_000_000:
        return number / 1_000_000.0
    if absolute >= 1_000_000_000_000:
        return number
    if absolute >= 1_000_000_000:
        return number * 1000.0
    return number


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


def _sample_server_time_rtt_ms(sample: dict[str, Any]) -> float | None:
    return _float_or_none(sample.get("server_time_rtt_ms", sample.get("round_trip_ms")))


def _low_rtt_acceptance_threshold(rtts: list[float]) -> float | None:
    median = _percentile(rtts, 0.50)
    if median is None:
        return None
    deviations = [abs(value - median) for value in rtts if math.isfinite(value)]
    mad = _percentile(deviations, 0.50) or 0.0
    dynamic_threshold = median + max(LOW_RTT_DYNAMIC_MARGIN_MS, LOW_RTT_DYNAMIC_MAD_MULTIPLIER * mad)
    return min(LOW_RTT_CLOCK_SAMPLE_MAX_MS, dynamic_threshold)


def _trim_offsets_for_median(offsets: list[float]) -> list[float]:
    clean = sorted(value for value in offsets if math.isfinite(value))
    if len(clean) < 5:
        return clean
    return clean[1:-1]


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
