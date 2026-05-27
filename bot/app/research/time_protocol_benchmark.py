from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import math
import shutil
from typing import Any
import zipfile

from app.research.orderbook_labeled_dataset import (
    NS_PER_MS,
    compute_return_bps,
    direction_label,
    validate_clean_samples,
    write_jsonl,
)
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
    depth_reference_event,
    generate_benchmark_rows,
    reference_event_id,
    reference_price,
    required_streams,
    validate_depth_reference_events,
    validate_reference_events,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE = "4.2D"
HORIZON_NAME = "horizon_100ms"
HORIZON_MS = 100
REQUIRED_100MS_MAX_FUTURE_GAP_MS = 100
HYBRID_BUDGETS_MS = (25, 50, 100, 250)
ALLOWED_CLOCK_SKEW_MS = 5.0

TIME_PROTOCOL_LABELS = Path("data/dataset/orderbook_time_protocol_benchmark_labels.jsonl")
TIME_PROTOCOL_DATASETS_ZIP = Path("data/dataset/phase_4_2d_time_protocol_datasets.zip")
PHASE42D_REPORT_JSON = Path("data/reports/phase_4_2d_time_protocol_benchmark_report.json")
PHASE42D_REPORT_MD = Path("data/reports/phase_4_2d_time_protocol_benchmark_report.md")
PHASE42D_SELF_CHECK_JSON = Path("data/reports/phase42d_self_check.json")
PHASE42D_PROTOCOL_SUMMARY = Path("data/debug/phase_4_2d_protocol_summary.json")
PHASE42D_EXCHANGE_INVALID_CASES = Path("data/debug/phase_4_2d_exchange_time_invalid_cases.jsonl")
PHASE42D_HYBRID_INVALID_CASES = Path("data/debug/phase_4_2d_hybrid_invalid_cases.jsonl")
PHASE42D_RECEIVE_LAG_DISTRIBUTION = Path("data/debug/phase_4_2d_receive_lag_distribution.json")
PHASE42D_CLOCK_SANITY_REPORT = Path("data/debug/phase_4_2d_clock_sanity_report.json")
PHASE42D_LEAKAGE_CHECK = Path("data/debug/phase_4_2d_leakage_check.json")
PHASE42D_TYPECHECK_REPORT = Path("data/debug/phase_4_2d_typecheck_report.txt")
PHASE42D_PYTEST_OUTPUT = Path("data/debug/phase_4_2d_pytest_output.txt")
PHASE42D_INVESTIGATION = Path("data/debug/phase42d_failure_investigation.md")
PHASE42D_CLEANUP_REPORT = Path("data/debug/phase_4_2d_artifact_cleanup.json")
PHASE42D_CAPTURE_DIAGNOSTICS = Path("data/debug/phase_4_2d_multifeed_capture_diagnostics.json")
PHASE42D_BUNDLE = Path("phase_4_2d_time_protocol_benchmark_bundle.zip")

REQUIRED_GITIGNORE_PATTERNS = (
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
)

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
)

PHASE42D_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "implementation_status",
        "input_timestamp_schema_status",
        "receive_time_coverage_status",
        "exchange_time_coverage_status",
        "hybrid_low_latency_status",
        "protocol_decision_status",
        "primary_failure",
        "failure_classifications",
        "symbol",
        "horizon_ms",
        "max_future_gap_ms",
        "sources",
        "protocol_summary",
        "selected_protocol_candidate",
        "low_latency_ready",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

PHASE42D_REQUIRED_BUNDLE_FILES = (
    "app/",
    "tests/",
    "scripts/",
    "data/reports/phase_4_2d_time_protocol_benchmark_report.json",
    "data/reports/phase_4_2d_time_protocol_benchmark_report.md",
    "data/reports/phase42d_self_check.json",
    "data/debug/phase_4_2d_protocol_summary.json",
    "data/debug/phase_4_2d_exchange_time_invalid_cases.jsonl",
    "data/debug/phase_4_2d_hybrid_invalid_cases.jsonl",
    "data/debug/phase_4_2d_receive_lag_distribution.json",
    "data/debug/phase_4_2d_clock_sanity_report.json",
    "data/debug/phase_4_2d_leakage_check.json",
    "data/debug/phase_4_2d_typecheck_report.txt",
    "data/debug/phase_4_2d_pytest_output.txt",
)


def cleanup_phase42d_artifacts(root: str | Path) -> dict[str, Any]:
    root_path = Path(root).resolve()
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    errors: list[str] = []

    for relative in ARTIFACT_DIRECTORIES:
        directory = root_path / relative
        if not directory.exists():
            missing_paths.append(relative)
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
                deleted_paths.append(display)
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
                deleted_paths.append(display)
            except OSError as exc:
                errors.append(f"{display}: {exc}")

    report = {
        "cleanup_performed": True,
        "deleted_paths": sorted(set(deleted_paths)),
        "missing_paths_skipped": sorted(set(missing_paths)),
        "errors": errors,
    }
    _write_json(root_path / PHASE42D_CLEANUP_REPORT, report)
    return report


def validate_gitignore_rules(root: str | Path) -> dict[str, Any]:
    path = Path(root) / ".gitignore"
    patterns: set[str] = set()
    if path.exists():
        patterns = {
            line.strip()
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
    missing = [pattern for pattern in REQUIRED_GITIGNORE_PATTERNS if pattern not in patterns]
    return {
        "path": ".gitignore",
        "passed": not missing,
        "missing_patterns": missing,
        "required_patterns": list(REQUIRED_GITIGNORE_PATTERNS),
    }


def validate_timestamp_schema(
    clean_samples: list[dict[str, Any]],
    references_by_source: dict[str, list[dict[str, Any]]],
    *,
    allow_event_time_fallback: bool = True,
) -> dict[str, Any]:
    feature_rows_with_exchange_ts = sum(1 for sample in clean_samples if feature_exchange_ts_ms(sample) is not None)
    feature_exchange_time_supported = feature_rows_with_exchange_ts > 0
    sources: dict[str, dict[str, Any]] = {}
    for source in REFERENCE_SOURCES:
        rows = references_by_source.get(source, [])
        field_counts: Counter[str] = Counter()
        rows_with_exchange_ts = 0
        for row in rows:
            selected = source_exchange_ts_ms(
                row,
                source,
                allow_event_time_fallback=allow_event_time_fallback,
            )
            if selected is None:
                continue
            field_name, _timestamp_ms = selected
            rows_with_exchange_ts += 1
            field_counts[field_name] += 1
        field_used = _preferred_field_used(field_counts, source)
        supported = feature_exchange_time_supported and rows_with_exchange_ts > 0
        unsupported_reason = None
        if not feature_exchange_time_supported:
            unsupported_reason = "missing_feature_exchange_timestamp"
        elif rows_with_exchange_ts <= 0:
            unsupported_reason = "missing_exchange_timestamp"
        sources[source] = {
            "source": source,
            "exchange_time_supported": supported,
            "exchange_timestamp_field_used": field_used,
            "reference_rows_with_exchange_ts": rows_with_exchange_ts,
            "valid_reference_event_count": len(rows),
            "unsupported_reason": unsupported_reason,
            "exchange_timestamp_fallback_policy": (
                "T_preferred_E_fallback_allowed"
                if source in {"trade_price", "aggTrade_price"} and allow_event_time_fallback
                else "no_fallback"
            ),
        }
    supported_sources = [source for source, item in sources.items() if item["exchange_time_supported"] is True]
    status = "pass" if supported_sources else "fail"
    return {
        "performed": True,
        "status": status,
        "feature_exchange_time_supported": feature_exchange_time_supported,
        "feature_rows_with_exchange_ts": feature_rows_with_exchange_ts,
        "feature_row_count": len(clean_samples),
        "sources": sources,
        "supported_sources": supported_sources,
    }


def feature_exchange_ts_ms(sample: dict[str, Any]) -> float | None:
    return _epoch_ms(sample.get("exchange_ts", sample.get("exchange_event_ts")))


def source_exchange_ts_ms(
    row: dict[str, Any],
    source: str,
    *,
    allow_event_time_fallback: bool = True,
) -> tuple[str, float] | None:
    if source == "depth_mid":
        timestamp = _epoch_ms(row.get("exchange_ts", row.get("exchange_event_ts")))
        return ("E", timestamp) if timestamp is not None else None
    if source in {"trade_price", "aggTrade_price"}:
        trade_time = _epoch_ms(row.get("trade_time"))
        if trade_time is not None:
            return ("T", trade_time)
        if allow_event_time_fallback:
            event_time = _epoch_ms(row.get("exchange_ts", row.get("exchange_event_ts")))
            if event_time is not None:
                return ("E", event_time)
        return None
    if source == "bookTicker_mid":
        trade_time = _epoch_ms(row.get("trade_time"))
        if trade_time is not None:
            return ("T", trade_time)
        event_time = _epoch_ms(row.get("exchange_ts", row.get("exchange_event_ts")))
        if event_time is not None:
            return ("E", event_time)
    return None


def generate_time_protocol_rows(
    clean_samples: list[dict[str, Any]],
    references_by_source: dict[str, list[dict[str, Any]]],
    timestamp_schema: dict[str, Any],
    *,
    allow_event_time_fallback: bool = True,
    allowed_clock_skew_ms: float = ALLOWED_CLOCK_SKEW_MS,
) -> list[dict[str, Any]]:
    receive_sorted = {
        source: sorted(
            references_by_source.get(source, []),
            key=lambda row: int(row["local_recv_monotonic_ns"]),
        )
        for source in REFERENCE_SOURCES
    }
    receive_timestamps = {
        source: [int(row["local_recv_monotonic_ns"]) for row in rows]
        for source, rows in receive_sorted.items()
    }
    exchange_sorted: dict[str, list[dict[str, Any]]] = {}
    exchange_timestamps: dict[str, list[float]] = {}
    schema_sources = _dict(timestamp_schema.get("sources"))
    for source in REFERENCE_SOURCES:
        rows_with_ts: list[tuple[float, dict[str, Any]]] = []
        for row in references_by_source.get(source, []):
            selected = source_exchange_ts_ms(row, source, allow_event_time_fallback=allow_event_time_fallback)
            if selected is None:
                continue
            _field, timestamp_ms = selected
            rows_with_ts.append((timestamp_ms, row))
        rows_with_ts.sort(key=lambda item: item[0])
        exchange_sorted[source] = [row for _timestamp_ms, row in rows_with_ts]
        exchange_timestamps[source] = [timestamp_ms for timestamp_ms, _row in rows_with_ts]

    rows: list[dict[str, Any]] = []
    for index, sample in enumerate(clean_samples):
        feature_mid = _sample_mid_price(sample)
        feature_spread = _sample_spread_bps(sample)
        labels_by_source: dict[str, dict[str, Any]] = {}
        for source in REFERENCE_SOURCES:
            receive_label = build_receive_time_label(
                reference_source=source,
                feature_sample=sample,
                feature_mid_price=feature_mid,
                references=receive_sorted[source],
                reference_timestamps_ns=receive_timestamps[source],
            )
            source_schema = _dict(schema_sources.get(source))
            exchange_supported = source_schema.get("exchange_time_supported") is True
            exchange_label = build_exchange_time_label(
                reference_source=source,
                feature_sample=sample,
                feature_mid_price=feature_mid,
                references=exchange_sorted[source],
                reference_exchange_timestamps_ms=exchange_timestamps[source],
                exchange_time_supported=exchange_supported,
                unsupported_reason=str(source_schema.get("unsupported_reason") or "missing_exchange_timestamp"),
                allow_event_time_fallback=allow_event_time_fallback,
            )
            labels = {
                "receive_time": receive_label,
                "exchange_time": exchange_label,
            }
            for budget_ms in HYBRID_BUDGETS_MS:
                labels[f"hybrid_{budget_ms}ms"] = build_hybrid_label(
                    exchange_label=exchange_label,
                    feature_lag_budget_ms=budget_ms,
                    allowed_clock_skew_ms=allowed_clock_skew_ms,
                )
            labels_by_source[source] = labels
        rows.append(
            {
                "schema_version": "orderbook_time_protocol_benchmark_v1",
                "symbol": sample.get("symbol"),
                "source": sample.get("source"),
                "generation_id": sample.get("generation_id"),
                "state_version": sample.get("state_version"),
                "snapshot_version": sample.get("snapshot_version"),
                "last_update_id": sample.get("last_update_id"),
                "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
                "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
                "exchange_event_ts": sample.get("exchange_event_ts"),
                "feature_exchange_ts_ms": feature_exchange_ts_ms(sample),
                "feature_best_bid": _float_or_none(sample.get("best_bid")),
                "feature_best_ask": _float_or_none(sample.get("best_ask")),
                "feature_mid_price": feature_mid,
                "feature_spread_bps": feature_spread,
                "protocol_labels": labels_by_source,
                "quality": {
                    "input_clean_sample_valid": True,
                    "feature_source_indices": {},
                    "current_index": index,
                    "future_label_policy": "first_reference_event_at_or_after_target_time",
                    "exchange_time_selection_basis": "exchange_ts",
                    "receive_time_selection_basis": "local_recv_monotonic_ns",
                    "future_receive_lag_hard_gate_used": False,
                    "max_future_gap_policy_ms": {HORIZON_NAME: REQUIRED_100MS_MAX_FUTURE_GAP_MS},
                    "hybrid_feature_lag_budgets_ms": list(HYBRID_BUDGETS_MS),
                },
            }
        )
    return rows


def build_receive_time_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    references: list[dict[str, Any]],
    reference_timestamps_ns: list[int],
) -> dict[str, Any]:
    feature_ts = feature_sample.get("local_recv_monotonic_ns")
    target_ts = feature_ts + HORIZON_MS * NS_PER_MS if isinstance(feature_ts, int) else None
    first_after_feature = bisect_right(reference_timestamps_ns, feature_ts) if isinstance(feature_ts, int) else None
    base = {
        "protocol": "receive_time_label_protocol",
        "reference_source": reference_source,
        "horizon_ms": HORIZON_MS,
        "target_local_recv_monotonic_ns": target_ts,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "first_reference_index_after_feature": first_after_feature,
        "future_reference_index": None,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_event_id": None,
        "future_reference_price": None,
        "future_gap_ms": None,
        "receive_future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "eligible": False,
        "valid": False,
        "invalid_reason": None,
    }
    if not isinstance(feature_ts, int) or target_ts is None:
        return {**base, "invalid_reason": "FEATURE_TIMESTAMP_INVALID"}
    feature_mid_price_value = _float_or_none(feature_mid_price)
    if feature_mid_price_value is None or feature_mid_price_value <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    if not reference_timestamps_ns or target_ts > reference_timestamps_ns[-1]:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_index = bisect_left(reference_timestamps_ns, target_ts)
    if future_index >= len(references):
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_reference = references[future_index]
    future_ts = int(future_reference["local_recv_monotonic_ns"])
    future_gap_ms = (future_ts - target_ts) / NS_PER_MS
    future_price = reference_price(future_reference, reference_source)
    base.update(
        {
            "future_reference_index": future_index,
            "future_reference_local_recv_monotonic_ns": future_ts,
            "future_reference_event_id": reference_event_id(future_reference, reference_source),
            "future_reference_price": future_price,
            "future_gap_ms": future_gap_ms,
            "receive_future_gap_ms": future_gap_ms,
            "eligible": True,
        }
    )
    if future_gap_ms < 0:
        return {**base, "invalid_reason": "LABEL_LEAKAGE_FUTURE_BEFORE_TARGET"}
    if future_gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    future_price_value = _float_or_none(future_price)
    if future_price_value is None or future_price_value <= 0:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_PRICE_INVALID"}
    return_bps = compute_return_bps(feature_mid_price_value, future_price_value)
    return {
        **base,
        "return_bps": return_bps,
        "direction": direction_label(return_bps),
        "valid": True,
        "invalid_reason": None,
    }


def build_exchange_time_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    references: list[dict[str, Any]],
    reference_exchange_timestamps_ms: list[float],
    exchange_time_supported: bool,
    unsupported_reason: str,
    allow_event_time_fallback: bool = True,
) -> dict[str, Any]:
    feature_exchange_ms = feature_exchange_ts_ms(feature_sample)
    feature_recv_ns = feature_sample.get("local_recv_monotonic_ns")
    target_exchange_ms = feature_exchange_ms + HORIZON_MS if feature_exchange_ms is not None else None
    feature_receive_lag_ms = receive_lag_ms(
        local_recv_wall_ts=feature_sample.get("local_recv_wall_ts"),
        exchange_ts_ms=feature_exchange_ms,
    )
    base = {
        "protocol": "exchange_time_label_protocol",
        "reference_source": reference_source,
        "horizon_ms": HORIZON_MS,
        "target_exchange_ts_ms": target_exchange_ms,
        "feature_exchange_ts_ms": feature_exchange_ms,
        "feature_local_recv_monotonic_ns": feature_recv_ns,
        "feature_receive_lag_ms": feature_receive_lag_ms,
        "selection_time_basis": "exchange_ts",
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "future_reference_index": None,
        "future_reference_exchange_ts_ms": None,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_event_id": None,
        "future_reference_price": None,
        "future_receive_lag_ms": None,
        "exchange_future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "eligible": False,
        "valid": False,
        "invalid_reason": None,
    }
    if not exchange_time_supported:
        return {**base, "invalid_reason": unsupported_reason}
    if feature_exchange_ms is None or target_exchange_ms is None:
        return {**base, "invalid_reason": "FEATURE_EXCHANGE_TIMESTAMP_MISSING"}
    feature_mid_price_value = _float_or_none(feature_mid_price)
    if feature_mid_price_value is None or feature_mid_price_value <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    if not reference_exchange_timestamps_ms or target_exchange_ms > reference_exchange_timestamps_ms[-1]:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_index = bisect_left(reference_exchange_timestamps_ms, target_exchange_ms)
    if future_index >= len(references):
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_reference = references[future_index]
    selected = source_exchange_ts_ms(
        future_reference,
        reference_source,
        allow_event_time_fallback=allow_event_time_fallback,
    )
    if selected is None:
        return {**base, "invalid_reason": "REFERENCE_EXCHANGE_TIMESTAMP_MISSING"}
    _field, future_exchange_ms = selected
    future_gap_ms = future_exchange_ms - target_exchange_ms
    future_price = reference_price(future_reference, reference_source)
    future_recv_ns = future_reference.get("local_recv_monotonic_ns")
    base.update(
        {
            "future_reference_index": future_index,
            "future_reference_exchange_ts_ms": future_exchange_ms,
            "future_reference_local_recv_monotonic_ns": future_recv_ns,
            "future_reference_event_id": reference_event_id(future_reference, reference_source),
            "future_reference_price": future_price,
            "future_receive_lag_ms": receive_lag_ms(
                local_recv_wall_ts=future_reference.get("local_recv_wall_ts"),
                exchange_ts_ms=future_exchange_ms,
            ),
            "exchange_future_gap_ms": future_gap_ms,
            "eligible": True,
        }
    )
    if future_gap_ms < 0:
        return {**base, "invalid_reason": "LABEL_LEAKAGE_FUTURE_BEFORE_TARGET"}
    if future_gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    future_price_value = _float_or_none(future_price)
    if future_price_value is None or future_price_value <= 0:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_PRICE_INVALID"}
    return_bps = compute_return_bps(feature_mid_price_value, future_price_value)
    return {
        **base,
        "return_bps": return_bps,
        "direction": direction_label(return_bps),
        "valid": True,
        "invalid_reason": None,
    }


def build_hybrid_label(
    *,
    exchange_label: dict[str, Any],
    feature_lag_budget_ms: int,
    allowed_clock_skew_ms: float = ALLOWED_CLOCK_SKEW_MS,
) -> dict[str, Any]:
    feature_lag = _float_or_none(exchange_label.get("feature_receive_lag_ms"))
    future_lag = _float_or_none(exchange_label.get("future_receive_lag_ms"))
    feature_recv_ns = exchange_label.get("feature_local_recv_monotonic_ns")
    future_recv_ns = exchange_label.get("future_reference_local_recv_monotonic_ns")
    base = {
        "protocol": "hybrid_low_latency_protocol",
        "reference_source": exchange_label.get("reference_source"),
        "horizon_ms": HORIZON_MS,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "feature_lag_budget_ms": feature_lag_budget_ms,
        "allowed_clock_skew_ms": allowed_clock_skew_ms,
        "exchange_time_valid": exchange_label.get("valid") is True,
        "feature_receive_lag_ms": feature_lag,
        "future_receive_lag_ms": future_lag,
        "future_receive_lag_is_telemetry_only": True,
        "future_receive_lag_hard_gate_used": False,
        "no_cross_stream_receive_reorder": None,
        "clock_sanity_valid": None,
        "eligible": bool(exchange_label.get("eligible", False)),
        "valid": False,
        "invalid_reason": None,
    }
    if exchange_label.get("eligible") is not True:
        return {**base, "invalid_reason": str(exchange_label.get("invalid_reason") or "EXCHANGE_TIME_NOT_ELIGIBLE")}
    if exchange_label.get("valid") is not True:
        return {**base, "invalid_reason": str(exchange_label.get("invalid_reason") or "EXCHANGE_TIME_INVALID")}
    if feature_lag is None:
        return {**base, "clock_sanity_valid": False, "invalid_reason": "CLOCK_SANITY_VIOLATION"}
    clock_sanity_valid = feature_lag >= -allowed_clock_skew_ms
    base["clock_sanity_valid"] = clock_sanity_valid
    if not clock_sanity_valid:
        return {**base, "invalid_reason": "CLOCK_SANITY_VIOLATION"}
    if feature_lag > feature_lag_budget_ms:
        return {**base, "invalid_reason": "FEATURE_RECEIVE_LAG_TOO_HIGH"}
    if not isinstance(feature_recv_ns, int) or not isinstance(future_recv_ns, int):
        return {**base, "no_cross_stream_receive_reorder": False, "invalid_reason": "CROSS_STREAM_RECEIVE_REORDER"}
    no_reorder = future_recv_ns > feature_recv_ns
    base["no_cross_stream_receive_reorder"] = no_reorder
    if not no_reorder:
        return {**base, "invalid_reason": "CROSS_STREAM_RECEIVE_REORDER"}
    return {**base, "valid": True, "invalid_reason": None}


def compute_source_protocol_report(
    *,
    source: str,
    validation: ReferenceValidationResult,
    rows: list[dict[str, Any]],
    timestamp_schema: dict[str, Any],
    leakage_result: dict[str, Any],
) -> dict[str, Any]:
    labels = [
        _dict(_dict(row.get("protocol_labels")).get(source))
        for row in rows
        if isinstance(row.get("protocol_labels"), dict)
    ]
    source_schema = _dict(_dict(timestamp_schema.get("sources")).get(source))
    receive_labels = [_dict(label.get("receive_time")) for label in labels if isinstance(label.get("receive_time"), dict)]
    exchange_labels = [_dict(label.get("exchange_time")) for label in labels if isinstance(label.get("exchange_time"), dict)]
    receive_metrics = _protocol_metrics(receive_labels, gap_field="receive_future_gap_ms")
    exchange_metrics = _protocol_metrics(exchange_labels, gap_field="exchange_future_gap_ms")
    exchange_metrics["exchange_future_gap_p95_ms"] = exchange_metrics.get("future_gap_p95_ms")
    exchange_metrics["label_leakage_violations"] = int(
        _dict(leakage_result.get("label_leakage_violations_by_source")).get(source, 0) or 0
    )

    hybrid: dict[str, dict[str, Any]] = {}
    for budget_ms in HYBRID_BUDGETS_MS:
        hybrid_labels = [_dict(label.get(f"hybrid_{budget_ms}ms")) for label in labels if isinstance(label.get(f"hybrid_{budget_ms}ms"), dict)]
        hybrid[f"hybrid_{budget_ms}ms"] = _hybrid_metrics(hybrid_labels, budget_ms=budget_ms)

    telemetry = _receive_lag_telemetry(exchange_labels)
    result = {
        "source": source,
        "semantic_type": SEMANTIC_TYPES[source],
        "semantic_description": SEMANTIC_DESCRIPTIONS[source],
        "reference_event_count": validation.reference_event_count,
        "valid_reference_event_count": validation.valid_reference_event_count,
        "exchange_time_supported": bool(source_schema.get("exchange_time_supported", False)),
        "exchange_timestamp_field_used": source_schema.get("exchange_timestamp_field_used"),
        "unsupported_reason": source_schema.get("unsupported_reason"),
        "receive_time": receive_metrics,
        "exchange_time": exchange_metrics,
        "hybrid": hybrid,
        "receive_lag_telemetry": telemetry,
    }
    if source == "trade_price":
        result["semantic_warning"] = "trade_price labels are transaction-price labels, not quote-mid labels"
    if source == "aggTrade_price":
        result["semantic_warning"] = "aggTrade_price labels are aggregate transaction-price labels, not quote-mid labels"
    return result


def build_phase42d_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    validations: dict[str, ReferenceValidationResult],
    rows: list[dict[str, Any]],
    timestamp_schema: dict[str, Any],
    leakage_result: dict[str, Any],
    clock_sanity_report: dict[str, Any],
    capture: dict[str, Any],
    cleanup_report: dict[str, Any] | None,
    gitignore_validation: dict[str, Any],
    pytest_passed: bool = True,
    typecheck_passed: bool = True,
    typecheck_summary: str = "",
    output_labels_path: str | Path = TIME_PROTOCOL_LABELS,
    fresh_capture_required: bool = True,
) -> dict[str, Any]:
    sources = {
        source: compute_source_protocol_report(
            source=source,
            validation=validations[source],
            rows=rows,
            timestamp_schema=timestamp_schema,
            leakage_result=leakage_result,
        )
        for source in REFERENCE_SOURCES
    }
    protocol_summary = build_protocol_summary(sources)
    selected = select_protocol_candidate(
        sources=sources,
        leakage_result=leakage_result,
        clock_sanity_report=clock_sanity_report,
    )
    low_latency_ready = selected is not None
    warnings: list[str] = []
    if any(
        _num(_dict(source_report.get("exchange_time")).get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE
        for source_report in sources.values()
    ) and not low_latency_ready:
        warnings.append("exchange_time_market_coverage_passed_but_hybrid_live_observability_failed")
    for source in ("trade_price", "aggTrade_price"):
        if _dict(sources.get(source)).get("valid_reference_event_count"):
            warnings.append(f"{source}_transaction_semantics")
    if _dict(sources.get("bookTicker_mid")).get("exchange_time_supported") is not True:
        warnings.append("bookTicker_mid_missing_exchange_timestamp")

    report = {
        "phase": PHASE,
        "status": "pass",
        "implementation_status": "pass",
        "input_timestamp_schema_status": str(timestamp_schema.get("status", "fail")),
        "receive_time_coverage_status": protocol_summary["receive_time_coverage_status"],
        "exchange_time_coverage_status": protocol_summary["exchange_time_coverage_status"],
        "hybrid_low_latency_status": protocol_summary["hybrid_low_latency_status"],
        "protocol_decision_status": "pass" if low_latency_ready else "fail",
        "primary_failure": None,
        "failure_classifications": [],
        "symbol": symbol.upper(),
        "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
        "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
        "fixture_mode": bool(capture.get("fixture_mode", False)),
        "skip_capture": bool(capture.get("skip_capture", False)),
        "cleanup_performed": bool(_dict(cleanup_report).get("cleanup_performed", False)),
        "horizon_ms": HORIZON_MS,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "hybrid_feature_lag_budgets_ms": list(HYBRID_BUDGETS_MS),
        "allowed_clock_skew_ms": ALLOWED_CLOCK_SKEW_MS,
        "future_receive_lag_hard_gate_used": False,
        "fresh_capture_required": fresh_capture_required,
        "capture": capture,
        "cleanup_report": cleanup_report or {},
        "gitignore_validation": gitignore_validation,
        "typecheck_summary": typecheck_summary,
        "timestamp_schema": timestamp_schema,
        "dataset_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker_reference_quotes": _display_path(BOOKTICKER_REFERENCE_QUOTES),
            "trade_reference_events": _display_path(TRADE_REFERENCE_EVENTS),
            "aggtrade_reference_events": _display_path(AGGTRADE_REFERENCE_EVENTS),
            "receive_time_reference_labels": _display_path(BENCHMARK_LABELS),
            "time_protocol_labels": _display_path(output_labels_path),
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": len(rows),
        "sources": sources,
        "protocol_summary": protocol_summary,
        "selected_protocol_candidate": selected,
        "low_latency_ready": low_latency_ready,
        "leakage_check": leakage_result,
        "clock_sanity_report": clock_sanity_report,
        "hard_fail_reasons": [],
        "warning_reasons": sorted(set(warnings)),
        "pytest_passed": pytest_passed,
        "typecheck_passed": typecheck_passed,
        "no_phase5_ready_flag": True,
    }
    return evaluate_phase42d_report(report)


def build_protocol_summary(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    receive_rates = {
        source: _num(_dict(report.get("receive_time")).get("valid_rate_eligible_rows"))
        for source, report in sources.items()
    }
    exchange_rates = {
        source: _num(_dict(report.get("exchange_time")).get("valid_rate_eligible_rows"))
        for source, report in sources.items()
        if report.get("exchange_time_supported") is True
    }
    hybrid_rates: dict[str, dict[str, float]] = {}
    for source, report in sources.items():
        if report.get("exchange_time_supported") is not True:
            continue
        hybrid_rates[source] = {
            budget_key: _num(_dict(metrics).get("valid_rate_eligible_rows"))
            for budget_key, metrics in _dict(report.get("hybrid")).items()
        }
    return {
        "receive_time_coverage_status": "pass" if any(rate >= REQUIRED_100MS_VALID_RATE for rate in receive_rates.values()) else "fail",
        "exchange_time_coverage_status": "pass" if any(rate >= REQUIRED_100MS_VALID_RATE for rate in exchange_rates.values()) else "fail",
        "hybrid_low_latency_status": "pass" if any(
            rate >= REQUIRED_100MS_VALID_RATE
            for rates in hybrid_rates.values()
            for rate in rates.values()
        ) else "fail",
        "receive_time_valid_rates": receive_rates,
        "exchange_time_valid_rates": exchange_rates,
        "hybrid_valid_rates": hybrid_rates,
        "supported_exchange_time_sources": sorted(exchange_rates),
        "unsupported_exchange_time_sources": sorted(
            source for source, report in sources.items() if report.get("exchange_time_supported") is not True
        ),
    }


def select_protocol_candidate(
    *,
    sources: dict[str, dict[str, Any]],
    leakage_result: dict[str, Any],
    clock_sanity_report: dict[str, Any],
) -> dict[str, Any] | None:
    if _num(leakage_result.get("feature_leakage_violations")) > 0 or _num(leakage_result.get("label_leakage_violations")) > 0:
        return None
    clock_blockers = _dict(clock_sanity_report.get("clock_sanity_blocker_by_source"))
    candidates: list[dict[str, Any]] = []
    for source, report in sources.items():
        if report.get("exchange_time_supported") is not True:
            continue
        exchange = _dict(report.get("exchange_time"))
        exchange_rate = _num(exchange.get("valid_rate_eligible_rows"))
        if exchange_rate < REQUIRED_100MS_VALID_RATE:
            continue
        if clock_blockers.get(source) is True:
            continue
        for budget_key, metrics_value in _dict(report.get("hybrid")).items():
            metrics = _dict(metrics_value)
            hybrid_rate = _num(metrics.get("valid_rate_eligible_rows"))
            if hybrid_rate >= REQUIRED_100MS_VALID_RATE and _num(metrics.get("clock_sanity_violation_count")) == 0:
                candidates.append(
                    {
                        "source": source,
                        "protocol": "hybrid_low_latency_protocol",
                        "budget_ms": int(str(budget_key).removeprefix("hybrid_").removesuffix("ms")),
                        "valid_rate": hybrid_rate,
                        "exchange_time_valid_rate": exchange_rate,
                    }
                )
    candidates.sort(key=lambda item: (-float(item["valid_rate"]), int(item["budget_ms"]), str(item["source"])))
    return candidates[0] if candidates else None


def evaluate_phase42d_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard: list[str] = [str(reason) for reason in evaluated.get("hard_fail_reasons", [])]
    classifications: list[str] = [str(item) for item in evaluated.get("failure_classifications", []) if item]
    warnings: list[str] = [str(reason) for reason in evaluated.get("warning_reasons", []) if reason]
    implementation_status = "pass"
    schema_status = str(evaluated.get("input_timestamp_schema_status", "fail"))
    receive_status = str(evaluated.get("receive_time_coverage_status", "fail"))
    exchange_status = str(evaluated.get("exchange_time_coverage_status", "fail"))
    hybrid_status = str(evaluated.get("hybrid_low_latency_status", "fail"))
    decision_status = str(evaluated.get("protocol_decision_status", "fail"))
    primary: str | None = evaluated.get("primary_failure")

    def add(reason: str, classification: str, *, implementation: bool = False) -> None:
        nonlocal implementation_status, schema_status, exchange_status, hybrid_status, decision_status, primary
        hard.append(reason)
        if classification not in classifications:
            classifications.append(classification)
        primary = primary or classification
        if implementation:
            implementation_status = "fail"
        if classification == "INPUT_TIMESTAMP_SCHEMA_FAILURE":
            schema_status = "fail"
        if classification == "EXCHANGE_TIME_COVERAGE_FAILURE":
            exchange_status = "fail"
        if classification == "HYBRID_OBSERVABILITY_FAILURE":
            hybrid_status = "fail"
        if classification in {"PROTOCOL_DECISION_FAILURE", "HYBRID_OBSERVABILITY_FAILURE", "EXCHANGE_TIME_COVERAGE_FAILURE"}:
            decision_status = "fail"

    for error in validate_phase42d_report_schema(evaluated):
        add(f"report schema invalid: {error}", "REPORT_SCHEMA_FAILURE", implementation=True)
    if evaluated.get("pytest_passed") is not True:
        add("pytest failed", "TEST_FAILURE", implementation=True)
    if evaluated.get("typecheck_passed") is not True:
        add("typecheck/compileall failed", "TYPECHECK_FAILURE", implementation=True)
    if _dict(evaluated.get("gitignore_validation")).get("passed") is not True:
        add("generated JSONL/heavy artifact .gitignore rules missing", "GITIGNORE_POLICY_FAILURE", implementation=True)
    if bool(evaluated.get("fresh_capture_required", True)):
        if evaluated.get("cleanup_performed") is not True:
            add("old generated artifacts were not cleaned before final run", "ARTIFACT_CLEANUP_FAILURE", implementation=True)
        if evaluated.get("fresh_capture_performed") is not True or evaluated.get("fixture_mode") is True or evaluated.get("skip_capture") is True:
            add("fresh 30-minute capture was not performed", "FRESH_CAPTURE_NOT_PERFORMED")
        if _num(evaluated.get("duration_sec")) < 1800:
            add("fresh capture duration_sec < 1800", "FRESH_CAPTURE_DURATION_FAILURE")
    if _num(evaluated.get("clean_sample_count")) <= 0:
        add("input clean sample dataset missing or empty", "INPUT_DATASET_FAILURE")
    if _num(evaluated.get("labeled_sample_count")) <= 0:
        add("time protocol benchmark labels were not generated", "INPUT_DATASET_FAILURE")

    if evaluated.get("horizon_ms") != HORIZON_MS or evaluated.get("max_future_gap_ms") != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        add("100ms horizon/max_future_gap_ms policy was relaxed", "HORIZON_100MS_POLICY_RELAXED", implementation=True)
    if evaluated.get("future_receive_lag_hard_gate_used") is not False:
        add("future_receive_lag_ms was used as a hard validity gate", "HYBRID_PROTOCOL_FAILURE", implementation=True)

    timestamp_schema = _dict(evaluated.get("timestamp_schema"))
    if timestamp_schema.get("performed") is not True or timestamp_schema.get("status") != "pass":
        add("timestamp schema validation failed or found no exchange-time-capable source", "INPUT_TIMESTAMP_SCHEMA_FAILURE")

    sources = _dict(evaluated.get("sources"))
    if not sources:
        add("sources missing from report", "REPORT_SCHEMA_FAILURE", implementation=True)
    for source in REFERENCE_SOURCES:
        source_report = _dict(sources.get(source))
        receive = _dict(source_report.get("receive_time"))
        if not receive:
            add(f"{source} receive-time protocol was not computed", "RECEIVE_TIME_PROTOCOL_MISSING", implementation=True)
        if int(receive.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
            add(f"{source} receive-time max_future_gap_ms != 100", "HORIZON_100MS_POLICY_RELAXED", implementation=True)
        exchange = _dict(source_report.get("exchange_time"))
        if source_report.get("exchange_time_supported") is True:
            if not exchange:
                add(f"{source} exchange-time protocol missing", "EXCHANGE_TIME_PROTOCOL_MISSING", implementation=True)
            if exchange.get("selection_time_basis") == "local_recv_monotonic_ns":
                add(f"{source} exchange-time protocol uses local receive timestamp", "EXCHANGE_TIME_FAKE_TIMESTAMP", implementation=True)
        if source == "bookTicker_mid" and source_report.get("exchange_time_supported") is True:
            field = source_report.get("exchange_timestamp_field_used")
            if field not in {"E", "T"}:
                add("bookTicker exchange-time support lacks real E/T timestamp", "BOOKTICKER_FAKE_EXCHANGE_TIMESTAMP", implementation=True)
        hybrid = _dict(source_report.get("hybrid"))
        for budget_ms in HYBRID_BUDGETS_MS:
            budget_key = f"hybrid_{budget_ms}ms"
            metrics = _dict(hybrid.get(budget_key))
            if not metrics:
                add(f"{source} {budget_key} hybrid protocol missing", "HYBRID_PROTOCOL_MISSING", implementation=True)
            if metrics.get("future_receive_lag_hard_gate_used") is not False:
                add(f"{source} {budget_key} used future_receive_lag_ms as hard gate", "HYBRID_PROTOCOL_FAILURE", implementation=True)
            if "feature_receive_lag_p95_ms" not in metrics:
                add(f"{source} {budget_key} missing feature_receive_lag telemetry", "HYBRID_PROTOCOL_FAILURE", implementation=True)
            if "cross_stream_receive_reorder_count" not in metrics:
                add(f"{source} {budget_key} missing cross-stream receive reorder count", "HYBRID_PROTOCOL_FAILURE", implementation=True)

    leakage = _dict(evaluated.get("leakage_check"))
    if leakage.get("performed") is not True:
        add("leakage check missing", "LEAKAGE_FAILURE", implementation=True)
    if _num(leakage.get("feature_leakage_violations")) > 0:
        add("feature leakage violations detected", "FEATURE_LEAKAGE_FAILURE")
    if _num(leakage.get("label_leakage_violations")) > 0:
        add("label leakage violations detected", "LABEL_LEAKAGE_FAILURE")
    clock = _dict(evaluated.get("clock_sanity_report"))
    if clock.get("performed") is not True:
        add("clock sanity report missing", "CLOCK_SANITY_FAILURE", implementation=True)

    if exchange_status != "pass":
        add("no exchange-time source achieved valid_rate_eligible_rows >= 0.95", "EXCHANGE_TIME_COVERAGE_FAILURE")
    if exchange_status == "pass" and hybrid_status != "pass":
        if "exchange_time_market_coverage_passed_but_hybrid_live_observability_failed" not in warnings:
            warnings.append("exchange_time_market_coverage_passed_but_hybrid_live_observability_failed")
        add("exchange-time market coverage passed but hybrid live observability failed", "HYBRID_OBSERVABILITY_FAILURE")
    elif hybrid_status != "pass":
        add("no hybrid budget achieved valid_rate_eligible_rows >= 0.95", "HYBRID_OBSERVABILITY_FAILURE")
    if evaluated.get("low_latency_ready") is not True:
        add("no low-latency-ready protocol candidate selected", "PROTOCOL_DECISION_FAILURE")

    hard = list(dict.fromkeys(hard))
    evaluated["implementation_status"] = implementation_status
    evaluated["input_timestamp_schema_status"] = schema_status
    evaluated["receive_time_coverage_status"] = receive_status
    evaluated["exchange_time_coverage_status"] = exchange_status
    evaluated["hybrid_low_latency_status"] = hybrid_status
    evaluated["protocol_decision_status"] = decision_status
    evaluated["status"] = "fail" if hard else "pass"
    evaluated["primary_failure"] = primary if hard else None
    evaluated["failure_classifications"] = sorted(set(classifications)) if hard else []
    evaluated["hard_fail_reasons"] = hard
    evaluated["warning_reasons"] = sorted(set(warnings))
    return evaluated


def validate_phase42d_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42D_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in (
        "implementation_status",
        "input_timestamp_schema_status",
        "receive_time_coverage_status",
        "exchange_time_coverage_status",
        "hybrid_low_latency_status",
        "protocol_decision_status",
    ):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    if report.get("horizon_ms") != HORIZON_MS:
        errors.append("horizon_ms must be 100")
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
            for key in ("receive_time", "exchange_time", "hybrid", "receive_lag_telemetry"):
                if key not in source_report:
                    errors.append(f"missing {source} field: {key}")
    if "phase5_ready" in report or "phase_5_ready" in report:
        errors.append("Phase 5 readiness flag is forbidden in Phase 4.2D")
    return errors


def run_phase42d_analysis(
    *,
    root: str | Path,
    symbol: str,
    clean_samples_path: str | Path = "data/dataset/orderbook_clean_samples.jsonl",
    bookticker_path: str | Path = BOOKTICKER_REFERENCE_QUOTES,
    trade_path: str | Path = TRADE_REFERENCE_EVENTS,
    aggtrade_path: str | Path = AGGTRADE_REFERENCE_EVENTS,
    receive_labels_path: str | Path = BENCHMARK_LABELS,
    time_protocol_labels_path: str | Path = TIME_PROTOCOL_LABELS,
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
    rows = generate_time_protocol_rows(clean_samples, references_by_source, timestamp_schema) if clean_samples else []
    labels_path = _resolve(root_path, time_protocol_labels_path)
    write_jsonl(labels_path, rows)

    leakage = run_phase42d_leakage_check(rows, output_path=root_path / PHASE42D_LEAKAGE_CHECK)
    clock_sanity = build_clock_sanity_report(rows)
    report = build_phase42d_report(
        symbol=symbol,
        clean_samples=clean_samples,
        validations=validations,
        rows=rows,
        timestamp_schema=timestamp_schema,
        leakage_result=leakage,
        clock_sanity_report=clock_sanity,
        capture=capture,
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        output_labels_path=time_protocol_labels_path,
        fresh_capture_required=fresh_capture_required,
    )
    if clean_validation.failure_classification:
        report["hard_fail_reasons"].append(f"clean sample validation failed: {clean_validation.failure_classification}")
        report["primary_failure"] = report.get("primary_failure") or "INPUT_DATASET_FAILURE"
        report = evaluate_phase42d_report(report)
    return report


def run_phase42d_leakage_check(
    rows: list[dict[str, Any]],
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    by_source = {source: 0 for source in REFERENCE_SOURCES}
    for sample_index, row in enumerate(rows):
        quality = _dict(row.get("quality"))
        feature_sources = _dict(quality.get("feature_source_indices"))
        for feature_name, source_index in feature_sources.items():
            if isinstance(source_index, int) and source_index > sample_index:
                violations.append(
                    {
                        "type": "feature",
                        "sample_index": sample_index,
                        "feature": feature_name,
                        "source_index": source_index,
                        "reason": "past_feature_uses_future_sample",
                    }
                )
        labels_by_source = _dict(row.get("protocol_labels"))
        for source in REFERENCE_SOURCES:
            labels = _dict(labels_by_source.get(source))
            for protocol_name in ("receive_time", "exchange_time"):
                label = _dict(labels.get(protocol_name))
                if label.get("valid") is not True:
                    continue
                reason = _label_leakage_reason(label, protocol_name=protocol_name)
                if reason is None:
                    continue
                by_source[source] += 1
                violations.append(
                    {
                        "type": "label",
                        "reference_source": source,
                        "protocol": protocol_name,
                        "sample_index": sample_index,
                        "reason": reason,
                    }
                )
    feature_count = sum(1 for violation in violations if violation["type"] == "feature")
    label_count = sum(1 for violation in violations if violation["type"] == "label")
    result = {
        "performed": True,
        "passed": feature_count == 0 and label_count == 0,
        "feature_leakage_violations": feature_count,
        "label_leakage_violations": label_count,
        "label_leakage_violations_by_source": by_source,
        "checked_samples": len(rows),
        "checked_sources": list(REFERENCE_SOURCES),
        "checked_protocols": ["receive_time", "exchange_time"],
        "violations": violations,
    }
    _write_json(output_path, result)
    return result


def build_clock_sanity_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = {}
    blockers: dict[str, bool] = {}
    for source in REFERENCE_SOURCES:
        labels = [
            _dict(_dict(_dict(row.get("protocol_labels")).get(source)).get("exchange_time"))
            for row in rows
            if isinstance(row.get("protocol_labels"), dict)
        ]
        feature_lags = _finite_values(label.get("feature_receive_lag_ms") for label in labels)
        future_lags = _finite_values(label.get("future_receive_lag_ms") for label in labels)
        violation_count = sum(1 for value in feature_lags if value < -ALLOWED_CLOCK_SKEW_MS)
        blockers[source] = violation_count > 0
        by_source[source] = {
            "feature_receive_lag_count": len(feature_lags),
            "feature_receive_lag_min_ms": min(feature_lags) if feature_lags else None,
            "feature_receive_lag_p50_ms": _percentile(feature_lags, 0.50),
            "feature_receive_lag_p95_ms": _percentile(feature_lags, 0.95),
            "feature_receive_lag_p99_ms": _percentile(feature_lags, 0.99),
            "future_receive_lag_p50_ms": _percentile(future_lags, 0.50),
            "future_receive_lag_p95_ms": _percentile(future_lags, 0.95),
            "future_receive_lag_p99_ms": _percentile(future_lags, 0.99),
            "negative_lag_beyond_allowed_skew_count": violation_count,
            "allowed_clock_skew_ms": ALLOWED_CLOCK_SKEW_MS,
        }
    return {
        "performed": True,
        "allowed_clock_skew_ms": ALLOWED_CLOCK_SKEW_MS,
        "clock_sanity_blocker": any(blockers.values()),
        "clock_sanity_blocker_by_source": blockers,
        "sources": by_source,
    }


def write_phase42d_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42d_report(report)
    _write_json(root_path / PHASE42D_REPORT_JSON, report)
    _write_text(root_path / PHASE42D_REPORT_MD, render_phase42d_markdown(report))
    _write_json(root_path / PHASE42D_PROTOCOL_SUMMARY, report.get("protocol_summary", {}))
    _write_json(root_path / PHASE42D_RECEIVE_LAG_DISTRIBUTION, receive_lag_distribution_from_report(report))
    _write_json(root_path / PHASE42D_CLOCK_SANITY_REPORT, report.get("clock_sanity_report", {}))
    _write_json(root_path / PHASE42D_LEAKAGE_CHECK, report.get("leakage_check", {}))
    _write_text(root_path / PHASE42D_PYTEST_OUTPUT, pytest_output)
    write_phase42d_invalid_cases(root=root_path, labels_path=root_path / TIME_PROTOCOL_LABELS)
    classification = None if report.get("status") == "pass" else classify_phase42d_failure(report)
    self_check = {
        "phase": PHASE,
        "passed": report.get("status") == "pass",
        "status": report.get("status"),
        "implementation_status": report.get("implementation_status"),
        "input_timestamp_schema_status": report.get("input_timestamp_schema_status"),
        "receive_time_coverage_status": report.get("receive_time_coverage_status"),
        "exchange_time_coverage_status": report.get("exchange_time_coverage_status"),
        "hybrid_low_latency_status": report.get("hybrid_low_latency_status"),
        "protocol_decision_status": report.get("protocol_decision_status"),
        "low_latency_ready": report.get("low_latency_ready"),
        "selected_protocol_candidate": report.get("selected_protocol_candidate"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42D_REPORT_JSON),
        "report_md_path": _display_path(PHASE42D_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42D_PYTEST_OUTPUT),
        "typecheck_report_path": _display_path(PHASE42D_TYPECHECK_REPORT),
        "bundle_path": _display_path(PHASE42D_BUNDLE),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42D_SELF_CHECK_JSON, self_check)
    if report.get("status") != "pass":
        write_phase42d_failure_investigation(root=root_path, report=report, classification=classification)


def write_phase42d_invalid_cases(*, root: Path, labels_path: Path) -> None:
    exchange_cases: list[dict[str, Any]] = []
    hybrid_cases: list[dict[str, Any]] = []
    if labels_path.exists():
        with labels_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    row = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                for source, labels_value in _dict(row.get("protocol_labels")).items():
                    labels = _dict(labels_value)
                    exchange = _dict(labels.get("exchange_time"))
                    if exchange and exchange.get("valid") is not True:
                        exchange_cases.append(_invalid_case(row, source, "exchange_time", exchange, line_number))
                    for budget_ms in HYBRID_BUDGETS_MS:
                        key = f"hybrid_{budget_ms}ms"
                        hybrid = _dict(labels.get(key))
                        if hybrid and hybrid.get("valid") is not True:
                            hybrid_cases.append(_invalid_case(row, source, key, hybrid, line_number))
    write_jsonl(root / PHASE42D_EXCHANGE_INVALID_CASES, exchange_cases)
    write_jsonl(root / PHASE42D_HYBRID_INVALID_CASES, hybrid_cases)


def render_phase42d_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phase 4.2D Time Protocol Benchmark Report",
        "",
        f"Status: **{report.get('status')}**",
        "",
        "## Status Separation",
        "",
        f"- Implementation: `{report.get('implementation_status')}`",
        f"- Timestamp schema: `{report.get('input_timestamp_schema_status')}`",
        f"- Receive-time coverage: `{report.get('receive_time_coverage_status')}`",
        f"- Exchange-time coverage: `{report.get('exchange_time_coverage_status')}`",
        f"- Hybrid low-latency: `{report.get('hybrid_low_latency_status')}`",
        f"- Protocol decision: `{report.get('protocol_decision_status')}`",
        f"- Low latency ready: `{report.get('low_latency_ready')}`",
        f"- Selected protocol candidate: `{report.get('selected_protocol_candidate')}`",
        "",
        "## Sources",
        "",
    ]
    for source in REFERENCE_SOURCES:
        source_report = _dict(_dict(report.get("sources")).get(source))
        receive = _dict(source_report.get("receive_time"))
        exchange = _dict(source_report.get("exchange_time"))
        hybrid = _dict(source_report.get("hybrid"))
        lines.append(
            "- `{source}` exchange_supported=`{supported}` field=`{field}` "
            "receive_rate=`{receive_rate}` exchange_rate=`{exchange_rate}` hybrid=`{hybrid_rates}`".format(
                source=source,
                supported=source_report.get("exchange_time_supported"),
                field=source_report.get("exchange_timestamp_field_used"),
                receive_rate=receive.get("valid_rate_eligible_rows"),
                exchange_rate=exchange.get("valid_rate_eligible_rows"),
                hybrid_rates=json.dumps(
                    {
                        key: _dict(value).get("valid_rate_eligible_rows")
                        for key, value in hybrid.items()
                    },
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
    lines.extend(
        [
            "",
            "## Phase Boundary",
            "",
            "100ms remained a hard requirement. No strategy/model/execution/PnL work is part of Phase 4.2D.",
            "",
        ]
    )
    return "\n".join(lines)


def create_phase42d_bundle(
    *,
    root: str | Path,
    source_root: str | Path = REPO_ROOT,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    source_path = Path(source_root)
    target = Path(bundle_path) if bundle_path is not None else root_path / PHASE42D_BUNDLE
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for directory_name in ("app", "tests", "scripts"):
            archive.writestr(f"{directory_name}/", "")
        _write_directory_to_archive(archive, source_path / "bot/app", "app")
        _write_directory_to_archive(archive, source_path / "tests", "tests")
        _write_directory_to_archive(archive, source_path / "scripts", "scripts")
        for relative in PHASE42D_REQUIRED_BUNDLE_FILES:
            if relative.endswith("/"):
                continue
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        labels = root_path / TIME_PROTOCOL_LABELS
        if labels.exists() and labels.is_file():
            archive.write(labels, _display_path(TIME_PROTOCOL_LABELS))
        dataset_zip = root_path / TIME_PROTOCOL_DATASETS_ZIP
        if dataset_zip.exists() and dataset_zip.is_file():
            archive.write(dataset_zip, _display_path(TIME_PROTOCOL_DATASETS_ZIP))
        investigation = root_path / PHASE42D_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42D_INVESTIGATION))
    missing = phase42d_bundle_missing_files(target)
    if missing:
        raise RuntimeError(f"Phase 4.2D bundle missing required files: {missing}")
    return target


def phase42d_bundle_missing_files(bundle_path: str | Path) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    return [name for name in PHASE42D_REQUIRED_BUNDLE_FILES if name not in names]


def write_phase42d_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2D Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Status: `{report.get('status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Timestamp schema: `{report.get('input_timestamp_schema_status')}`",
        f"- Exchange-time coverage: `{report.get('exchange_time_coverage_status')}`",
        f"- Hybrid observability: `{report.get('hybrid_low_latency_status')}`",
        f"- Clock sanity blocker: `{_dict(report.get('clock_sanity_report')).get('clock_sanity_blocker')}`",
        f"- Report path: `{_display_path(PHASE42D_REPORT_JSON)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Classification Guide",
        "",
        f"- timestamp schema limitation: `{classification == 'INPUT_TIMESTAMP_SCHEMA_FAILURE'}`",
        f"- exchange-time coverage failure: `{classification == 'EXCHANGE_TIME_COVERAGE_FAILURE'}`",
        f"- hybrid observability failure: `{classification == 'HYBRID_OBSERVABILITY_FAILURE'}`",
        f"- clock sanity failure: `{'CLOCK_SANITY_FAILURE' in report.get('failure_classifications', [])}`",
        f"- leakage failure: `{classification in {'FEATURE_LEAKAGE_FAILURE', 'LABEL_LEAKAGE_FAILURE', 'LEAKAGE_FAILURE'}}`",
        f"- typecheck/test failure: `{classification in {'TYPECHECK_FAILURE', 'TEST_FAILURE'}}`",
        "",
        "## Phase Boundary",
        "",
        "No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.",
        "",
    ]
    _write_text(Path(root) / PHASE42D_INVESTIGATION, "\n".join(lines))


def classify_phase42d_failure(report: dict[str, Any]) -> str:
    classifications = [str(item) for item in report.get("failure_classifications", []) if item]
    primary = str(report.get("primary_failure") or "")
    for classification in (
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "GITIGNORE_POLICY_FAILURE",
        "ARTIFACT_CLEANUP_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "FRESH_CAPTURE_DURATION_FAILURE",
        "INPUT_DATASET_FAILURE",
        "INPUT_TIMESTAMP_SCHEMA_FAILURE",
        "EXCHANGE_TIME_COVERAGE_FAILURE",
        "HYBRID_OBSERVABILITY_FAILURE",
        "CLOCK_SANITY_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "HORIZON_100MS_POLICY_RELAXED",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "PROTOCOL_DECISION_FAILURE",
    ):
        if classification in primary:
            return classification
    for classification in (
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "GITIGNORE_POLICY_FAILURE",
        "ARTIFACT_CLEANUP_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "FRESH_CAPTURE_DURATION_FAILURE",
        "INPUT_DATASET_FAILURE",
        "INPUT_TIMESTAMP_SCHEMA_FAILURE",
        "EXCHANGE_TIME_COVERAGE_FAILURE",
        "HYBRID_OBSERVABILITY_FAILURE",
        "CLOCK_SANITY_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "HORIZON_100MS_POLICY_RELAXED",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "PROTOCOL_DECISION_FAILURE",
    ):
        if classification in classifications:
            return classification
    return "UNKNOWN_PHASE42D_FAILURE"


def receive_lag_ms(*, local_recv_wall_ts: Any, exchange_ts_ms: float | None) -> float | None:
    wall_ms = _wall_ts_ms(local_recv_wall_ts)
    if wall_ms is None or exchange_ts_ms is None:
        return None
    return wall_ms - exchange_ts_ms


def receive_lag_distribution_from_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        source: _dict(source_report).get("receive_lag_telemetry", {})
        for source, source_report in _dict(report.get("sources")).items()
    }


def _protocol_metrics(labels: list[dict[str, Any]], *, gap_field: str) -> dict[str, Any]:
    eligible = [label for label in labels if label.get("eligible") is True]
    valid = [label for label in eligible if label.get("valid") is True]
    reasons = Counter(
        str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        for label in labels
        if label.get("valid") is not True
    )
    reasons.pop("None", None)
    gaps = _finite_values(label.get(gap_field) for label in labels)
    return {
        "horizon_ms": HORIZON_MS,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "selection_time_basis": "exchange_ts" if gap_field == "exchange_future_gap_ms" else "local_recv_monotonic_ns",
        "eligible_count": len(eligible),
        "valid_count": len(valid),
        "invalid_count": len(labels) - len(valid),
        "valid_rate_all_rows": len(valid) / len(labels) if labels else 0.0,
        "valid_rate_eligible_rows": len(valid) / len(eligible) if eligible else 0.0,
        "invalid_reason_counts": dict(sorted(reasons.items())),
        "future_gap_p50_ms": _percentile(gaps, 0.50),
        "future_gap_p90_ms": _percentile(gaps, 0.90),
        "future_gap_p95_ms": _percentile(gaps, 0.95),
        "future_gap_p99_ms": _percentile(gaps, 0.99),
        "future_gap_max_ms": max(gaps) if gaps else None,
    }


def _hybrid_metrics(labels: list[dict[str, Any]], *, budget_ms: int) -> dict[str, Any]:
    eligible = [label for label in labels if label.get("eligible") is True]
    valid = [label for label in eligible if label.get("valid") is True]
    reasons = Counter(
        str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        for label in labels
        if label.get("valid") is not True
    )
    reasons.pop("None", None)
    feature_lags = _finite_values(label.get("feature_receive_lag_ms") for label in labels)
    future_lags = _finite_values(label.get("future_receive_lag_ms") for label in labels)
    return {
        "horizon_ms": HORIZON_MS,
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
        "feature_receive_lag_p50_ms": _percentile(feature_lags, 0.50),
        "feature_receive_lag_p95_ms": _percentile(feature_lags, 0.95),
        "feature_receive_lag_p99_ms": _percentile(feature_lags, 0.99),
        "future_receive_lag_p50_ms": _percentile(future_lags, 0.50),
        "future_receive_lag_p95_ms": _percentile(future_lags, 0.95),
        "future_receive_lag_p99_ms": _percentile(future_lags, 0.99),
        "future_receive_lag_over_budget_count": sum(1 for value in future_lags if value > budget_ms),
        "cross_stream_receive_reorder_count": reasons.get("CROSS_STREAM_RECEIVE_REORDER", 0),
        "clock_sanity_violation_count": reasons.get("CLOCK_SANITY_VIOLATION", 0),
    }


def _receive_lag_telemetry(exchange_labels: list[dict[str, Any]]) -> dict[str, Any]:
    feature_lags = _finite_values(label.get("feature_receive_lag_ms") for label in exchange_labels)
    future_lags = _finite_values(label.get("future_receive_lag_ms") for label in exchange_labels)
    return {
        "feature_receive_lag_p50_ms": _percentile(feature_lags, 0.50),
        "feature_receive_lag_p95_ms": _percentile(feature_lags, 0.95),
        "feature_receive_lag_p99_ms": _percentile(feature_lags, 0.99),
        "future_receive_lag_p50_ms": _percentile(future_lags, 0.50),
        "future_receive_lag_p95_ms": _percentile(future_lags, 0.95),
        "future_receive_lag_p99_ms": _percentile(future_lags, 0.99),
    }


def _label_leakage_reason(label: dict[str, Any], *, protocol_name: str) -> str | None:
    if protocol_name == "receive_time":
        target = label.get("target_local_recv_monotonic_ns")
        future = label.get("future_reference_local_recv_monotonic_ns")
        if isinstance(target, int) and isinstance(future, int) and future < target:
            return "future_reference_timestamp_before_target"
        return None
    target_exchange = _float_or_none(label.get("target_exchange_ts_ms"))
    future_exchange = _float_or_none(label.get("future_reference_exchange_ts_ms"))
    if target_exchange is not None and future_exchange is not None and future_exchange < target_exchange:
        return "future_reference_exchange_timestamp_before_target"
    return None


def _invalid_case(
    row: dict[str, Any],
    source: str,
    protocol: str,
    label: dict[str, Any],
    line_number: int,
) -> dict[str, Any]:
    return {
        "line": line_number,
        "symbol": row.get("symbol"),
        "generation_id": row.get("generation_id"),
        "last_update_id": row.get("last_update_id"),
        "reference_source": source,
        "protocol": protocol,
        "invalid_reason": label.get("invalid_reason"),
        "feature_local_recv_monotonic_ns": label.get("feature_local_recv_monotonic_ns", row.get("local_recv_monotonic_ns")),
        "future_reference_local_recv_monotonic_ns": label.get("future_reference_local_recv_monotonic_ns"),
        "feature_receive_lag_ms": label.get("feature_receive_lag_ms"),
        "future_receive_lag_ms": label.get("future_receive_lag_ms"),
    }


def _preferred_field_used(field_counts: Counter[str], source: str) -> str | None:
    if source in {"trade_price", "aggTrade_price"} and field_counts.get("T", 0) > 0:
        return "T"
    if field_counts.get("E", 0) > 0:
        return "E"
    if field_counts.get("T", 0) > 0:
        return "T"
    return None


def _sample_mid_price(sample: dict[str, Any]) -> float | None:
    value = _float_or_none(sample.get("mid_price"))
    if value is not None and value > 0:
        return value
    bid = _float_or_none(sample.get("best_bid"))
    ask = _float_or_none(sample.get("best_ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return (bid + ask) / 2.0


def _sample_spread_bps(sample: dict[str, Any]) -> float | None:
    value = _float_or_none(sample.get("spread_bps"))
    if value is not None and value >= 0:
        return value
    bid = _float_or_none(sample.get("best_bid"))
    ask = _float_or_none(sample.get("best_ask"))
    mid = _sample_mid_price(sample)
    if bid is None or ask is None or mid is None or mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def _valid_price(value: Any) -> bool:
    number = _float_or_none(value)
    return number is not None and number > 0


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


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("status") == "pass":
        return "Phase 4.2D Definition of Done passed; low-latency-ready protocol candidate selected."
    return f"Phase 4.2D failed with classification {classification}. No pass bundle was created."


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


def _write_directory_to_archive(
    archive: zipfile.ZipFile,
    directory: Path,
    archive_prefix: str,
) -> None:
    if not directory.exists():
        return
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        archive.write(path, str(Path(archive_prefix) / path.relative_to(directory)))
