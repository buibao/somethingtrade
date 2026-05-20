from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any
import zipfile

from app.research.orderbook_labeled_dataset import (
    NS_PER_MS,
    compute_return_bps,
    direction_label,
    validate_clean_samples,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE = "4.2C"
HORIZON_NAME = "horizon_100ms"
HORIZON_MS = 100
REQUIRED_100MS_MAX_FUTURE_GAP_MS = 100
REQUIRED_100MS_VALID_RATE = 0.95

REFERENCE_SOURCES = (
    "depth_mid",
    "bookTicker_mid",
    "trade_price",
    "aggTrade_price",
)
EXTERNAL_REFERENCE_SOURCES = (
    "bookTicker_mid",
    "trade_price",
    "aggTrade_price",
)
SEMANTIC_TYPES = {
    "depth_mid": "quote_mid",
    "bookTicker_mid": "quote_mid",
    "trade_price": "transaction_price",
    "aggTrade_price": "transaction_price",
}
SEMANTIC_DESCRIPTIONS = {
    "depth_mid": "future orderbook mid label",
    "bookTicker_mid": "future best bid/ask mid label",
    "trade_price": "future transaction price label",
    "aggTrade_price": "future aggregate transaction price label",
}
SEMANTIC_TIE_BREAKER = {
    "depth_mid": 0,
    "bookTicker_mid": 1,
    "trade_price": 2,
    "aggTrade_price": 3,
}

BOOKTICKER_REFERENCE_QUOTES = Path("data/dataset/bookticker_reference_quotes.jsonl")
TRADE_REFERENCE_EVENTS = Path("data/dataset/trade_reference_events.jsonl")
AGGTRADE_REFERENCE_EVENTS = Path("data/dataset/aggtrade_reference_events.jsonl")
BENCHMARK_LABELS = Path("data/dataset/orderbook_reference_benchmark_labels.jsonl")

PHASE42C_REPORT_JSON = Path("data/reports/phase_4_2c_reference_feed_benchmark_report.json")
PHASE42C_REPORT_MD = Path("data/reports/phase_4_2c_reference_feed_benchmark_report.md")
PHASE42C_SELF_CHECK_JSON = Path("data/reports/phase42c_self_check.json")
PHASE42C_REFERENCE_SUMMARY = Path("data/debug/phase_4_2c_reference_feed_summary.json")
PHASE42C_GAP_DISTRIBUTION = Path("data/debug/phase_4_2c_reference_gap_distribution.json")
PHASE42C_INVALID_100MS = Path("data/debug/phase_4_2c_100ms_invalid_cases.jsonl")
PHASE42C_LEAKAGE_CHECK = Path("data/debug/phase_4_2c_leakage_check.json")
PHASE42C_PYTEST_OUTPUT = Path("data/debug/phase_4_2c_pytest_output.txt")
PHASE42C_INVESTIGATION = Path("data/debug/phase42c_failure_investigation.md")
PHASE42C_CLEANUP_REPORT = Path("data/debug/phase_4_2c_artifact_cleanup.json")
PHASE42C_CAPTURE_DIAGNOSTICS = Path("data/debug/phase_4_2c_multifeed_capture_diagnostics.json")
PHASE42C_TYPECHECK_REPORT = Path("data/debug/phase_4_2c_typecheck_report.txt")
PHASE42C_BUNDLE = Path("phase_4_2c_reference_feed_benchmark_bundle.zip")

SOURCE_TO_STREAM_SUFFIX = {
    "depth_mid": "depth@100ms",
    "bookTicker_mid": "bookTicker",
    "trade_price": "trade",
    "aggTrade_price": "aggTrade",
}
SOURCE_TO_DATASET_KEY = {
    "depth_mid": "clean_samples",
    "bookTicker_mid": "bookticker_reference_quotes",
    "trade_price": "trade_reference_events",
    "aggTrade_price": "aggtrade_reference_events",
}

DEPTH_RUNTIME_ZERO_FIELDS = (
    "sample_before_ready_count",
    "feed_receive_stale_count",
    "queue_dropped_messages",
    "sequence_gap_count",
    "invalid_delta_count",
    "crossed_book_count",
    "book_empty_count",
    "one_side_missing_count",
    "clean_sample_schema_violation_count",
)

PHASE42C_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "implementation_status",
        "runtime_status",
        "benchmark_status",
        "definition_of_done_status",
        "primary_failure",
        "failure_classifications",
        "symbol",
        "duration_sec",
        "fresh_capture_performed",
        "fixture_mode",
        "skip_capture",
        "cleanup_performed",
        "capture",
        "dataset_paths",
        "clean_sample_count",
        "reference_sources",
        "ranking",
        "selected_reference_source",
        "selected_reference_source_status",
        "semantic_warning",
        "label_semantics",
        "depth_runtime_quality",
        "capture_diagnostics_path",
        "typecheck_report_path",
        "leakage_check",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

REQUIRED_SOURCE_METRIC_FIELDS = frozenset(
    {
        "reference_event_count",
        "valid_reference_event_count",
        "invalid_reference_event_count",
        "reference_sample_rate_per_sec",
        "gap_p50_ms",
        "gap_p90_ms",
        "gap_p95_ms",
        "gap_p99_ms",
        "gap_max_ms",
        "gap_over_100ms_count",
        "gap_over_100ms_total_duration_ms",
        "bad_time_coverage_ratio_100ms",
        "eligible_count",
        "valid_count",
        "invalid_count",
        "valid_rate_all_rows",
        "valid_rate_eligible_rows",
        "invalid_reason_counts",
        "future_gap_p50_ms",
        "future_gap_p90_ms",
        "future_gap_p95_ms",
        "future_gap_p99_ms",
        "future_gap_max_ms",
        "label_leakage_violations",
        "max_future_gap_ms",
        "passes_100ms_gate",
        "source_status",
    }
)

PHASE42C_REQUIRED_BUNDLE_FILES = (
    "app/",
    "tests/",
    "scripts/",
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/bookticker_reference_quotes.jsonl",
    "data/dataset/trade_reference_events.jsonl",
    "data/dataset/aggtrade_reference_events.jsonl",
    "data/dataset/orderbook_reference_benchmark_labels.jsonl",
    "data/reports/phase_4_2c_reference_feed_benchmark_report.json",
    "data/reports/phase_4_2c_reference_feed_benchmark_report.md",
    "data/reports/phase42c_self_check.json",
    "data/debug/phase_4_2c_reference_feed_summary.json",
    "data/debug/phase_4_2c_reference_gap_distribution.json",
    "data/debug/phase_4_2c_100ms_invalid_cases.jsonl",
    "data/debug/phase_4_2c_leakage_check.json",
    "data/debug/phase_4_2c_multifeed_capture_diagnostics.json",
    "data/debug/phase_4_2c_artifact_cleanup.json",
    "data/debug/phase_4_2c_typecheck_report.txt",
    "data/debug/phase_4_2c_pytest_output.txt",
)

CLEANUP_EXPLICIT_FILES = (
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/bookticker_reference_quotes.jsonl",
    "data/dataset/trade_reference_events.jsonl",
    "data/dataset/aggtrade_reference_events.jsonl",
    "data/dataset/orderbook_reference_benchmark_labels.jsonl",
    "data/reports/phase_4_1_orderbook_quality_report.json",
    "data/reports/phase_4_1_orderbook_quality_report.md",
    "data/reports/phase_4_2c_reference_feed_benchmark_report.json",
    "data/reports/phase_4_2c_reference_feed_benchmark_report.md",
    "data/reports/phase42c_self_check.json",
    "data/debug/phase_4_2c_reference_feed_summary.json",
    "data/debug/phase_4_2c_reference_gap_distribution.json",
    "data/debug/phase_4_2c_100ms_invalid_cases.jsonl",
    "data/debug/phase_4_2c_leakage_check.json",
    "data/debug/phase_4_2c_pytest_output.txt",
    "data/debug/phase42c_failure_investigation.md",
    "data/debug/phase_4_2c_multifeed_capture_diagnostics.json",
    "data/debug/phase_4_2c_artifact_cleanup.json",
    "data/debug/phase_4_2c_typecheck_report.txt",
    "data/debug/phase_4_2c_runtime_stdout.log",
    "data/debug/phase_4_2c_runtime_stderr.log",
    "data/debug/phase_4_2c_recapture_stdout.log",
    "data/debug/phase_4_2c_recapture_stderr.log",
    "data/debug/sequence_recovery_trace.jsonl",
    "phase_4_2c_reference_feed_benchmark_bundle.zip",
)


@dataclass(frozen=True)
class ReferenceValidationResult:
    reference_source: str
    file_exists: bool
    reference_event_count: int
    valid_reference_event_count: int
    invalid_reference_event_count: int
    valid_events: list[dict[str, Any]]
    invalid_events: list[dict[str, Any]]
    timestamp_monotonic_violations: int
    quality: dict[str, Any]


def cleanup_phase42c_artifacts(root: str | Path) -> dict[str, Any]:
    root_path = Path(root)
    deleted_files: list[str] = []
    missing_files_skipped: list[str] = []
    errors: list[str] = []
    candidates: list[Path] = [root_path / relative for relative in CLEANUP_EXPLICIT_FILES]
    for directory in ("data/dataset", "data/reports", "data/debug"):
        base = root_path / directory
        if not base.exists():
            continue
        for path in base.iterdir():
            if path.is_file() and path.name.startswith(("phase_4_2c_", "phase42c_")):
                candidates.append(path)

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        display = _relative_display(root_path, path)
        if not path.exists():
            missing_files_skipped.append(display)
            continue
        if path.is_dir():
            errors.append(f"refusing to delete directory: {display}")
            continue
        try:
            path.unlink()
            deleted_files.append(display)
        except OSError as exc:
            errors.append(f"{display}: {exc}")

    report = {
        "cleanup_performed": True,
        "deleted_files": sorted(deleted_files),
        "missing_files_skipped": sorted(missing_files_skipped),
        "errors": errors,
    }
    _write_json(root_path / PHASE42C_CLEANUP_REPORT, report)
    return report


def required_streams(symbol: str) -> list[str]:
    symbol_lower = symbol.lower()
    return [
        f"{symbol_lower}@depth@100ms",
        f"{symbol_lower}@bookTicker",
        f"{symbol_lower}@trade",
        f"{symbol_lower}@aggTrade",
    ]


def stream_name_for_source(*, symbol: str, reference_source: str) -> str:
    suffix = SOURCE_TO_STREAM_SUFFIX[reference_source]
    return f"{symbol.lower()}@{suffix}"


def validate_capture_diagnostics(diagnostics: dict[str, Any], *, symbol: str) -> list[str]:
    errors: list[str] = []
    requested = _list_of_str(diagnostics.get("requested_streams"))
    for stream in required_streams(symbol):
        if stream not in requested:
            errors.append(f"missing requested stream: {stream}")
    for field in (
        "message_count_by_stream",
        "parsed_count_by_source",
        "parse_error_count_by_source",
        "output_file_paths",
        "output_file_sizes_bytes",
    ):
        if not isinstance(diagnostics.get(field), dict):
            errors.append(f"missing diagnostics object: {field}")
    return errors


def validate_reference_event_schema(row: dict[str, Any], reference_source: str) -> list[str]:
    errors: list[str] = []
    for field in ("schema_version", "symbol", "source", "local_recv_monotonic_ns", "local_recv_wall_ts"):
        if field not in row or row.get(field) is None:
            errors.append(f"MISSING_{field.upper()}")
    if not isinstance(row.get("local_recv_monotonic_ns"), int) or isinstance(
        row.get("local_recv_monotonic_ns"), bool
    ):
        errors.append("MISSING_LOCAL_RECV_MONOTONIC_NS")
    if not row.get("local_recv_wall_ts"):
        errors.append("MISSING_LOCAL_RECV_WALL_TS")

    event_id_field = {
        "bookTicker_mid": "update_id",
        "trade_price": "trade_id",
        "aggTrade_price": "aggregate_trade_id",
        "depth_mid": "last_update_id",
    }.get(reference_source)
    if event_id_field and row.get(event_id_field) is None and row.get("event_id") is None:
        errors.append(f"MISSING_{event_id_field.upper()}")

    price = reference_price(row, reference_source)
    if price is None or not isinstance(price, (int, float)) or not math.isfinite(float(price)):
        errors.append("INVALID_REFERENCE_PRICE")
    elif price <= 0:
        errors.append("NON_POSITIVE_REFERENCE_PRICE")

    if reference_source == "bookTicker_mid":
        bid = row.get("best_bid")
        ask = row.get("best_ask")
        if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)):
            errors.append("INVALID_BOOKTICKER_BID_ASK")
        elif bid <= 0 or ask <= 0 or bid >= ask:
            errors.append("INVALID_BOOKTICKER_BID_ASK")

    quality_value = row.get("quality")
    quality = quality_value if isinstance(quality_value, dict) else {}
    if quality and quality.get("valid") is not True:
        errors.extend(str(reason) for reason in quality.get("errors", []))
    elif "quality" not in row:
        errors.append("MISSING_QUALITY")
    return sorted(set(errors))


def validate_reference_events(
    path: str | Path,
    *,
    reference_source: str,
    invalid_output_path: str | Path | None = None,
) -> ReferenceValidationResult:
    input_path = Path(path)
    invalid_events: list[dict[str, Any]] = []
    valid_events: list[dict[str, Any]] = []
    event_count = 0
    if not input_path.exists():
        result = ReferenceValidationResult(
            reference_source=reference_source,
            file_exists=False,
            reference_event_count=0,
            valid_reference_event_count=0,
            invalid_reference_event_count=0,
            valid_events=[],
            invalid_events=[],
            timestamp_monotonic_violations=0,
            quality=analyze_reference_gap_distribution([]),
        )
        _write_jsonl(invalid_output_path, invalid_events)
        return result

    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            event_count += 1
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                invalid_events.append({"line": line_number, "reason": f"INVALID_JSON:{exc}"})
                continue
            if not isinstance(row, dict):
                invalid_events.append({"line": line_number, "reason": "ROW_NOT_OBJECT"})
                continue
            errors = validate_reference_event_schema(row, reference_source)
            if errors:
                invalid_events.append(
                    {
                        "line": line_number,
                        "reference_source": reference_source,
                        "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                        "event_id": reference_event_id(row, reference_source),
                        "reason": errors,
                    }
                )
                continue
            valid_events.append(row)

    timestamp_violations = reference_timestamp_monotonic_violations(valid_events)
    quality = analyze_reference_gap_distribution(valid_events)
    result = ReferenceValidationResult(
        reference_source=reference_source,
        file_exists=True,
        reference_event_count=event_count,
        valid_reference_event_count=len(valid_events),
        invalid_reference_event_count=len(invalid_events),
        valid_events=valid_events,
        invalid_events=invalid_events,
        timestamp_monotonic_violations=timestamp_violations,
        quality=quality,
    )
    _write_jsonl(invalid_output_path, invalid_events)
    return result


def validate_depth_reference_events(clean_samples: list[dict[str, Any]]) -> ReferenceValidationResult:
    valid_events: list[dict[str, Any]] = []
    invalid_events: list[dict[str, Any]] = []
    for index, sample in enumerate(clean_samples):
        event = depth_reference_event(sample, reference_index=index)
        errors = validate_reference_event_schema(event, "depth_mid")
        if errors:
            invalid_events.append(
                {
                    "line": None,
                    "reference_source": "depth_mid",
                    "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
                    "event_id": sample.get("last_update_id"),
                    "reason": errors,
                }
            )
            continue
        valid_events.append(event)
    quality = analyze_reference_gap_distribution(valid_events)
    return ReferenceValidationResult(
        reference_source="depth_mid",
        file_exists=True,
        reference_event_count=len(clean_samples),
        valid_reference_event_count=len(valid_events),
        invalid_reference_event_count=len(invalid_events),
        valid_events=valid_events,
        invalid_events=invalid_events,
        timestamp_monotonic_violations=reference_timestamp_monotonic_violations(valid_events),
        quality=quality,
    )


def reference_timestamp_monotonic_violations(events: list[dict[str, Any]]) -> int:
    timestamps = [
        int(row["local_recv_monotonic_ns"])
        for row in events
        if isinstance(row.get("local_recv_monotonic_ns"), int)
    ]
    return sum(1 for index in range(1, len(timestamps)) if timestamps[index] < timestamps[index - 1])


def analyze_reference_gap_distribution(reference_events: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [
        int(row["local_recv_monotonic_ns"])
        for row in reference_events
        if isinstance(row.get("local_recv_monotonic_ns"), int)
    ]
    gaps = [(timestamps[index] - timestamps[index - 1]) / NS_PER_MS for index in range(1, len(timestamps))]
    non_negative_gaps = [gap for gap in gaps if gap >= 0]
    total_duration_ms = (timestamps[-1] - timestamps[0]) / NS_PER_MS if len(timestamps) >= 2 else 0.0
    bad_durations = [max(0.0, gap - REQUIRED_100MS_MAX_FUTURE_GAP_MS) for gap in non_negative_gaps]
    bad_total_ms = sum(bad_durations)
    duration_sec = total_duration_ms / 1000.0
    return {
        "reference_sample_rate_per_sec": len(timestamps) / duration_sec if duration_sec > 0 else 0.0,
        "gap_p50_ms": _percentile(non_negative_gaps, 0.50),
        "gap_p90_ms": _percentile(non_negative_gaps, 0.90),
        "gap_p95_ms": _percentile(non_negative_gaps, 0.95),
        "gap_p99_ms": _percentile(non_negative_gaps, 0.99),
        "gap_max_ms": max(non_negative_gaps) if non_negative_gaps else None,
        "gap_over_100ms_count": sum(1 for gap in non_negative_gaps if gap > REQUIRED_100MS_MAX_FUTURE_GAP_MS),
        "gap_over_100ms_total_duration_ms": bad_total_ms,
        "bad_time_coverage_ratio_100ms": bad_total_ms / total_duration_ms if total_duration_ms > 0 else 0.0,
        "timestamp_monotonic_violations": sum(1 for gap in gaps if gap < 0),
    }


def depth_reference_event(sample: dict[str, Any], *, reference_index: int) -> dict[str, Any]:
    mid = sample_mid_price(sample)
    spread_bps = sample_spread_bps(sample)
    errors: list[str] = []
    if mid is None or mid <= 0:
        errors.append("INVALID_REFERENCE_PRICE")
    if not isinstance(sample.get("local_recv_monotonic_ns"), int):
        errors.append("MISSING_LOCAL_RECV_MONOTONIC_NS")
    if not sample.get("local_recv_wall_ts"):
        errors.append("MISSING_LOCAL_RECV_WALL_TS")
    return {
        "schema_version": "depth_reference_v1",
        "symbol": sample.get("symbol"),
        "source": "orderbook_clean_samples",
        "reference_index": reference_index,
        "event_id": sample.get("last_update_id"),
        "last_update_id": sample.get("last_update_id"),
        "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
        "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
        "exchange_event_ts": sample.get("exchange_event_ts"),
        "price": mid,
        "mid_price": mid,
        "spread_bps": spread_bps,
        "quality": {
            "valid": not errors,
            "errors": sorted(set(errors)),
        },
    }


def generate_benchmark_rows(
    clean_samples: list[dict[str, Any]],
    references_by_source: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    sorted_references = {
        source: sorted(
            references_by_source.get(source, []),
            key=lambda row: int(row["local_recv_monotonic_ns"]),
        )
        for source in REFERENCE_SOURCES
    }
    reference_timestamps = {
        source: [int(row["local_recv_monotonic_ns"]) for row in rows]
        for source, rows in sorted_references.items()
    }
    rows: list[dict[str, Any]] = []
    for index, sample in enumerate(clean_samples):
        feature_ts = sample.get("local_recv_monotonic_ns")
        feature_mid = sample_mid_price(sample)
        feature_spread = sample_spread_bps(sample)
        reference_labels = {}
        for source in REFERENCE_SOURCES:
            reference_labels[source] = {
                HORIZON_NAME: build_reference_label(
                    reference_source=source,
                    feature_sample=sample,
                    feature_mid_price=feature_mid,
                    references=sorted_references[source],
                    reference_timestamps_ns=reference_timestamps[source],
                )
            }
        rows.append(
            {
                "schema_version": "orderbook_reference_benchmark_v1",
                "symbol": sample.get("symbol"),
                "source": sample.get("source"),
                "generation_id": sample.get("generation_id"),
                "state_version": sample.get("state_version"),
                "snapshot_version": sample.get("snapshot_version"),
                "last_update_id": sample.get("last_update_id"),
                "local_recv_monotonic_ns": feature_ts,
                "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
                "exchange_event_ts": sample.get("exchange_event_ts"),
                "feature_best_bid": _float_or_none(sample.get("best_bid")),
                "feature_best_ask": _float_or_none(sample.get("best_ask")),
                "feature_mid_price": feature_mid,
                "feature_spread_bps": feature_spread,
                "reference_labels": reference_labels,
                "quality": {
                    "input_clean_sample_valid": True,
                    "feature_source_indices": {},
                    "current_index": index,
                    "future_label_policy": "first_reference_event_at_or_after_target_time",
                    "max_future_gap_policy_ms": {HORIZON_NAME: REQUIRED_100MS_MAX_FUTURE_GAP_MS},
                },
            }
        )
    return rows


def select_future_reference_index(
    reference_timestamps_ns: list[int],
    *,
    feature_timestamp_ns: int,
    horizon_ms: int = HORIZON_MS,
) -> int | None:
    target_time = feature_timestamp_ns + horizon_ms * NS_PER_MS
    index = bisect_left(reference_timestamps_ns, target_time)
    return index if index < len(reference_timestamps_ns) else None


def build_reference_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    references: list[dict[str, Any]],
    reference_timestamps_ns: list[int],
) -> dict[str, Any]:
    feature_ts = feature_sample.get("local_recv_monotonic_ns")
    target_ts = feature_ts + HORIZON_MS * NS_PER_MS if isinstance(feature_ts, int) else None
    first_after_feature = (
        bisect_right(reference_timestamps_ns, feature_ts)
        if isinstance(feature_ts, int)
        else None
    )
    base = {
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
        "return_bps": None,
        "direction": None,
        "valid": False,
        "invalid_reason": None,
    }
    if not isinstance(feature_ts, int) or target_ts is None:
        return {**base, "invalid_reason": "FEATURE_TIMESTAMP_INVALID"}
    if (
        feature_mid_price is None
        or not isinstance(feature_mid_price, (int, float))
        or not math.isfinite(float(feature_mid_price))
        or feature_mid_price <= 0
    ):
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    future_index = select_future_reference_index(
        reference_timestamps_ns,
        feature_timestamp_ns=feature_ts,
        horizon_ms=HORIZON_MS,
    )
    if future_index is None:
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
        }
    )
    if future_gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    if (
        future_price is None
        or not isinstance(future_price, (int, float))
        or not math.isfinite(float(future_price))
        or future_price <= 0
    ):
        return {**base, "invalid_reason": "FUTURE_REFERENCE_PRICE_INVALID"}
    try:
        return_bps = compute_return_bps(float(feature_mid_price), float(future_price))
    except ValueError:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    return {
        **base,
        "return_bps": return_bps,
        "direction": direction_label(return_bps),
        "valid": True,
        "invalid_reason": None,
    }


def compute_source_metrics(
    *,
    reference_source: str,
    validation: ReferenceValidationResult,
    clean_samples: list[dict[str, Any]],
    benchmark_rows: list[dict[str, Any]],
    label_leakage_violations: int,
    capture_diagnostics: dict[str, Any] | None = None,
    symbol: str = "BTCUSDT",
) -> dict[str, Any]:
    labels = [
        row.get("reference_labels", {}).get(reference_source, {}).get(HORIZON_NAME)
        for row in benchmark_rows
        if isinstance(row.get("reference_labels"), dict)
    ]
    labels = [label for label in labels if isinstance(label, dict)]
    valid_count = sum(1 for label in labels if label.get("valid") is True)
    invalid_count = len(labels) - valid_count
    reason_counts = Counter(
        str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        for label in labels
        if label.get("valid") is not True
    )
    reason_counts.pop("None", None)
    future_gaps = [
        float(label["future_gap_ms"])
        for label in labels
        if isinstance(label.get("future_gap_ms"), (int, float))
        and math.isfinite(float(label["future_gap_ms"]))
    ]
    last_reference_ts = None
    reference_timestamps = [
        int(row["local_recv_monotonic_ns"])
        for row in validation.valid_events
        if isinstance(row.get("local_recv_monotonic_ns"), int)
    ]
    if reference_timestamps:
        last_reference_ts = max(reference_timestamps)
    eligible_count = 0
    if last_reference_ts is not None:
        eligible_count = sum(
            1
            for sample in clean_samples
            if isinstance(sample.get("local_recv_monotonic_ns"), int)
            and int(sample["local_recv_monotonic_ns"]) + HORIZON_MS * NS_PER_MS <= last_reference_ts
        )
    quality = validation.quality
    valid_rate_eligible = valid_count / eligible_count if eligible_count else 0.0
    metrics = {
        "reference_source": reference_source,
        "semantic_type": SEMANTIC_TYPES[reference_source],
        "semantic_description": SEMANTIC_DESCRIPTIONS[reference_source],
        "file_exists": validation.file_exists,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "reference_event_count": validation.reference_event_count,
        "valid_reference_event_count": validation.valid_reference_event_count,
        "invalid_reference_event_count": validation.invalid_reference_event_count,
        "reference_timestamp_monotonic_violations": validation.timestamp_monotonic_violations,
        "reference_sample_rate_per_sec": quality.get("reference_sample_rate_per_sec", 0.0),
        "gap_p50_ms": quality.get("gap_p50_ms"),
        "gap_p90_ms": quality.get("gap_p90_ms"),
        "gap_p95_ms": quality.get("gap_p95_ms"),
        "gap_p99_ms": quality.get("gap_p99_ms"),
        "gap_max_ms": quality.get("gap_max_ms"),
        "gap_over_100ms_count": quality.get("gap_over_100ms_count", 0),
        "gap_over_100ms_total_duration_ms": quality.get("gap_over_100ms_total_duration_ms", 0.0),
        "bad_time_coverage_ratio_100ms": quality.get("bad_time_coverage_ratio_100ms", 0.0),
        "eligible_count": eligible_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "valid_rate_all_rows": valid_count / len(labels) if labels else 0.0,
        "valid_rate_eligible_rows": valid_rate_eligible,
        "invalid_reason_counts": dict(sorted(reason_counts.items())),
        "future_gap_p50_ms": _percentile(future_gaps, 0.50),
        "future_gap_p90_ms": _percentile(future_gaps, 0.90),
        "future_gap_p95_ms": _percentile(future_gaps, 0.95),
        "future_gap_p99_ms": _percentile(future_gaps, 0.99),
        "future_gap_max_ms": max(future_gaps) if future_gaps else None,
        "label_leakage_violations": label_leakage_violations,
    }
    metrics["passes_100ms_gate"] = (
        metrics["max_future_gap_ms"] == REQUIRED_100MS_MAX_FUTURE_GAP_MS
        and metrics["valid_reference_event_count"] > 0
        and metrics["valid_rate_eligible_rows"] >= REQUIRED_100MS_VALID_RATE
        and metrics["label_leakage_violations"] == 0
        and metrics["reference_timestamp_monotonic_violations"] == 0
    )
    metrics["source_status"] = classify_source_status(
        reference_source=reference_source,
        metrics=metrics,
        capture_diagnostics=capture_diagnostics,
        symbol=symbol,
    )
    return metrics


def classify_source_status(
    *,
    reference_source: str,
    metrics: dict[str, Any],
    capture_diagnostics: dict[str, Any] | None,
    symbol: str,
) -> str:
    stream_name = stream_name_for_source(symbol=symbol, reference_source=reference_source)
    requested_streams = _list_of_str(_dict(capture_diagnostics).get("requested_streams"))
    message_counts = _dict(_dict(capture_diagnostics).get("message_count_by_stream"))
    message_count = _num(message_counts.get(stream_name)) if capture_diagnostics is not None else None

    if capture_diagnostics is not None and stream_name not in requested_streams:
        return "not_captured"
    if metrics.get("file_exists") is not True:
        return "not_captured"
    if capture_diagnostics is not None and message_count == 0:
        return "captured_but_empty"
    if capture_diagnostics is None and _num(metrics.get("reference_event_count")) <= 0:
        return "captured_but_empty"
    if _num(metrics.get("reference_event_count")) > 0 and _num(metrics.get("valid_reference_event_count")) <= 0:
        return "parser_failed_or_all_invalid"
    if metrics.get("passes_100ms_gate") is True:
        return "measured_pass"
    if _num(metrics.get("valid_reference_event_count")) > 0:
        return "measured_coverage_failed"
    return "not_captured"


def run_phase42c_leakage_check(
    benchmark_rows: list[dict[str, Any]],
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    label_by_source = {source: 0 for source in REFERENCE_SOURCES}
    for sample_index, row in enumerate(benchmark_rows):
        feature_ts = row.get("local_recv_monotonic_ns")
        quality_value = row.get("quality")
        quality = quality_value if isinstance(quality_value, dict) else {}
        feature_sources = quality.get("feature_source_indices", {})
        if isinstance(feature_sources, dict):
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
        labels = row.get("reference_labels")
        if not isinstance(labels, dict) or not isinstance(feature_ts, int):
            continue
        for source in REFERENCE_SOURCES:
            label = labels.get(source, {}).get(HORIZON_NAME) if isinstance(labels.get(source), dict) else None
            if not isinstance(label, dict):
                continue
            target_ts = label.get("target_local_recv_monotonic_ns")
            ref_ts = label.get("future_reference_local_recv_monotonic_ns")
            ref_index = label.get("future_reference_index")
            first_after_feature = label.get("first_reference_index_after_feature")
            reason = None
            if isinstance(ref_ts, int) and ref_ts <= feature_ts:
                reason = "future_reference_not_after_feature_timestamp"
            elif isinstance(ref_ts, int) and isinstance(target_ts, int) and ref_ts < target_ts:
                reason = "future_reference_timestamp_before_target"
            elif (
                isinstance(ref_index, int)
                and isinstance(first_after_feature, int)
                and ref_index < first_after_feature
            ):
                reason = "future_reference_before_first_event_after_feature"
            elif label.get("valid") is True:
                if not isinstance(ref_index, int):
                    reason = "valid_label_missing_future_reference_index"
                elif not isinstance(ref_ts, int):
                    reason = "valid_label_missing_future_reference_timestamp"
            if reason:
                label_by_source[source] += 1
                violations.append(
                    {
                        "type": "label",
                        "reference_source": source,
                        "sample_index": sample_index,
                        "horizon": HORIZON_NAME,
                        "reason": reason,
                        "future_reference_index": ref_index,
                        "future_reference_local_recv_monotonic_ns": ref_ts,
                        "target_local_recv_monotonic_ns": target_ts,
                    }
                )
    feature_count = sum(1 for violation in violations if violation["type"] == "feature")
    label_count = sum(1 for violation in violations if violation["type"] == "label")
    result = {
        "passed": feature_count == 0 and label_count == 0,
        "feature_leakage_violations": feature_count,
        "label_leakage_violations": label_count,
        "label_leakage_violations_by_source": label_by_source,
        "checked_samples": len(benchmark_rows),
        "checked_sources": list(REFERENCE_SOURCES),
        "checked_horizons": [HORIZON_NAME],
        "violations": violations,
    }
    _write_json(output_path, result)
    return result


def rank_reference_sources(source_metrics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, float, int]:
        source, metrics = item
        pass_rank = 0 if metrics.get("passes_100ms_gate") is True else 1
        return (pass_rank, -_num(metrics.get("valid_rate_eligible_rows")), SEMANTIC_TIE_BREAKER[source])

    ranking: list[dict[str, Any]] = []
    for source, metrics in sorted(source_metrics.items(), key=sort_key):
        ranking.append(
            {
                "reference_source": source,
                "valid_rate_eligible_rows": metrics.get("valid_rate_eligible_rows", 0.0),
                "gap_p95_ms": metrics.get("gap_p95_ms"),
                "gap_p99_ms": metrics.get("gap_p99_ms"),
                "passes_100ms_gate": bool(metrics.get("passes_100ms_gate", False)),
                "semantic_type": metrics.get("semantic_type"),
            }
        )
    return ranking


def build_phase42c_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    source_metrics: dict[str, dict[str, Any]],
    benchmark_rows: list[dict[str, Any]],
    leakage_result: dict[str, Any],
    depth_runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
    dataset_paths: dict[str, str | Path] | None = None,
) -> dict[str, Any]:
    normalized_metrics = {source: dict(source_metrics.get(source, {})) for source in REFERENCE_SOURCES}
    for source, metrics in normalized_metrics.items():
        metrics.setdefault("source_status", "measured_pass" if metrics.get("passes_100ms_gate") is True else "measured_coverage_failed")
    ranking = rank_reference_sources(source_metrics)
    selected = next(
        (item["reference_source"] for item in ranking if item.get("passes_100ms_gate") is True),
        None,
    )
    semantic_warning = None
    warnings: list[str] = []
    if selected in {"trade_price", "aggTrade_price"}:
        semantic_warning = f"{selected} label is transaction-price-based, not quote-mid-based"
        warnings.append("selected_reference_source_is_transaction_price")
    report = {
        "phase": PHASE,
        "status": "pass",
        "implementation_status": "pass",
        "runtime_status": "pass",
        "benchmark_status": "pass",
        "definition_of_done_status": "pass",
        "primary_failure": None,
        "failure_classifications": [],
        "symbol": symbol,
        "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
        "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
        "fixture_mode": bool(capture.get("fixture_mode", False)),
        "skip_capture": bool(capture.get("skip_capture", False)),
        "cleanup_performed": bool(capture.get("cleanup_performed", False)),
        "capture": {
            "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
            "fixture_mode": bool(capture.get("fixture_mode", False)),
            "skip_capture": bool(capture.get("skip_capture", False)),
            "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
            "depth_stream": str(capture.get("depth_stream", f"{symbol.lower()}@depth@100ms")),
            "requested_streams": list(
                capture.get(
                    "requested_streams",
                    required_streams(symbol),
                )
            ),
            "reference_streams": list(
                capture.get(
                    "reference_streams",
                    [
                        f"{symbol.lower()}@bookTicker",
                        f"{symbol.lower()}@trade",
                        f"{symbol.lower()}@aggTrade",
                    ],
                )
            ),
            "downsampling_enabled": bool(capture.get("downsampling_enabled", False)),
            "depth_clean_sample_count": int(capture.get("depth_clean_sample_count", len(clean_samples)) or 0),
            "reference_event_counts": dict(capture.get("reference_event_counts", {})),
        },
        "capture_diagnostics_path": _display_path(PHASE42C_CAPTURE_DIAGNOSTICS),
        "typecheck_report_path": _display_path(PHASE42C_TYPECHECK_REPORT),
        "cleanup_report_path": _display_path(PHASE42C_CLEANUP_REPORT),
        "capture_diagnostics": _dict(capture.get("capture_diagnostics")),
        "fresh_capture_required": fresh_capture_required,
        "dataset_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker_reference_quotes": _display_path(BOOKTICKER_REFERENCE_QUOTES),
            "trade_reference_events": _display_path(TRADE_REFERENCE_EVENTS),
            "aggtrade_reference_events": _display_path(AGGTRADE_REFERENCE_EVENTS),
            "benchmark_labels": _display_path(BENCHMARK_LABELS),
            **{key: _display_path(value) for key, value in (dataset_paths or {}).items()},
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": len(benchmark_rows),
        "reference_sources": normalized_metrics,
        "ranking": ranking,
        "selected_reference_source": selected,
        "selected_reference_source_status": "pass" if selected is not None else "fail",
        "semantic_warning": semantic_warning,
        "label_semantics": SEMANTIC_DESCRIPTIONS,
        "depth_runtime_quality": _normalize_depth_runtime_quality(depth_runtime_quality),
        "leakage_check": leakage_result,
        "hard_fail_reasons": [],
        "warning_reasons": warnings,
        "recommendation": _recommendation(selected),
    }
    return evaluate_phase42c_report(report)


def evaluate_phase42c_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard: list[str] = [str(reason) for reason in evaluated.get("hard_fail_reasons", [])]
    classifications: list[str] = [
        str(item) for item in evaluated.get("failure_classifications", []) if item
    ]
    warnings = [str(reason) for reason in evaluated.get("warning_reasons", [])]
    implementation_status = "pass"
    runtime_status = "pass"
    benchmark_status = "pass"
    primary: str | None = evaluated.get("primary_failure")

    def add(
        reason: str,
        classification: str,
        *,
        domain: str,
        primary_classification: str | None = None,
    ) -> None:
        nonlocal implementation_status, runtime_status, benchmark_status, primary
        hard.append(reason)
        if classification not in classifications:
            classifications.append(classification)
        primary = primary_classification or primary or classification
        if domain == "implementation":
            implementation_status = "fail"
        elif domain == "runtime":
            runtime_status = "fail"
        else:
            benchmark_status = "fail"

    schema_errors = validate_phase42c_report_schema(evaluated)
    if schema_errors:
        for error in schema_errors:
            add(f"report schema invalid: {error}", "REPORT_SCHEMA_FAILURE", domain="implementation")
    if evaluated.get("pytest_failed") is True:
        add("pytest failed", "TEST_FAILURE", domain="implementation")
    if evaluated.get("typecheck_failed") is True:
        add("typecheck/compileall failed", "TYPECHECK_FAILURE", domain="implementation")
    if evaluated.get("cleanup_failed") is True:
        add("artifact cleanup failed", "ARTIFACT_CLEANUP_FAILURE", domain="implementation")
    if evaluated.get("multi_feed_capture_failed") is True:
        add("fresh multi-feed capture failed", "MULTI_FEED_CAPTURE_INCOMPLETE", domain="runtime")

    capture = _dict(evaluated.get("capture"))
    fresh_required = bool(evaluated.get("fresh_capture_required", False))
    fresh_capture_performed = bool(evaluated.get("fresh_capture_performed", capture.get("fresh_capture_performed", False)))
    fixture_mode = bool(evaluated.get("fixture_mode", capture.get("fixture_mode", False)))
    skip_capture = bool(evaluated.get("skip_capture", capture.get("skip_capture", False)))
    duration_sec = _num(evaluated.get("duration_sec", capture.get("duration_sec")))
    cleanup_performed = bool(evaluated.get("cleanup_performed", False))

    if fresh_required and not cleanup_performed:
        add("artifact cleanup was not performed before final run", "ARTIFACT_CLEANUP_FAILURE", domain="implementation")
    if fresh_required and "MULTI_FEED_CAPTURE_INCOMPLETE" not in classifications and (
        fresh_capture_performed is not True or fixture_mode is True or skip_capture is True
    ):
        add(
            "final run did not perform a real fresh multi-feed capture",
            "FRESH_CAPTURE_NOT_PERFORMED",
            domain="runtime",
            primary_classification="MULTI_FEED_CAPTURE_INCOMPLETE",
        )
    if fresh_required and fresh_capture_performed and not fixture_mode and duration_sec < 1800:
        add("fresh capture duration_sec < 1800", "MULTI_FEED_CAPTURE_INCOMPLETE", domain="runtime")
    if capture.get("downsampling_enabled") is True:
        add("downsampling_enabled must be false", "DEPTH_RUNTIME_QUALITY_FAILURE", domain="runtime")

    diagnostics = _dict(evaluated.get("capture_diagnostics"))
    requested_streams = _list_of_str(
        diagnostics.get("requested_streams")
        if diagnostics
        else capture.get("requested_streams")
    )
    if fresh_required and fresh_capture_performed and not diagnostics:
        add("multi-feed capture diagnostics missing", "MULTI_FEED_CAPTURE_INCOMPLETE", domain="runtime")
    if fresh_required and fresh_capture_performed:
        missing_streams = [stream for stream in required_streams(str(evaluated.get("symbol") or "BTCUSDT")) if stream not in requested_streams]
        if missing_streams:
            add(
                f"required streams not requested: {','.join(missing_streams)}",
                "MULTI_FEED_CAPTURE_INCOMPLETE",
                domain="runtime",
            )

    runtime = _dict(evaluated.get("depth_runtime_quality"))
    for field in DEPTH_RUNTIME_ZERO_FIELDS:
        if _num(runtime.get(field)) > 0:
            add(f"{field} > 0: {runtime.get(field)}", "DEPTH_RUNTIME_QUALITY_FAILURE", domain="runtime")
    if _num(runtime.get("snapshot_copy_p99_us")) > 200.0:
        add(
            f"snapshot_copy_p99_us > 200: {runtime.get('snapshot_copy_p99_us')}",
            "DEPTH_RUNTIME_QUALITY_FAILURE",
            domain="runtime",
        )

    if _num(evaluated.get("clean_sample_count")) <= 0:
        add("clean sample count = 0", "DEPTH_RUNTIME_QUALITY_FAILURE", domain="runtime")
    if _num(evaluated.get("labeled_sample_count")) <= 0:
        add("benchmark labeled sample count = 0", "MULTI_FEED_CAPTURE_INCOMPLETE", domain="benchmark")

    reference_sources = _dict(evaluated.get("reference_sources"))
    missing_dataset_sources = [
        source for source in EXTERNAL_REFERENCE_SOURCES
        if _dict(reference_sources.get(source)).get("file_exists") is not True
    ]
    if missing_dataset_sources:
        add(
            f"reference datasets missing: {','.join(missing_dataset_sources)}",
            "MULTI_FEED_CAPTURE_INCOMPLETE",
            domain="benchmark",
        )
    source_statuses = {
        source: str(_dict(reference_sources.get(source)).get("source_status", "not_captured"))
        for source in REFERENCE_SOURCES
    }
    if fresh_required and fresh_capture_performed:
        not_captured = [source for source, status in source_statuses.items() if status == "not_captured"]
        if not_captured:
            add(
                f"required sources not captured: {','.join(not_captured)}",
                "MULTI_FEED_CAPTURE_INCOMPLETE",
                domain="benchmark",
            )
    parser_failed = [source for source, status in source_statuses.items() if status == "parser_failed_or_all_invalid"]
    if parser_failed:
        add(
            f"reference parser emitted zero valid events: {','.join(parser_failed)}",
            "REFERENCE_SCHEMA_FAILURE",
            domain="benchmark",
        )
    for source in REFERENCE_SOURCES:
        metrics = _dict(reference_sources.get(source))
        if int(metrics.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS:
            add(f"{source} horizon_100ms max_future_gap_ms != 100", "HORIZON_100MS_POLICY_RELAXED", domain="implementation")

    ranking = evaluated.get("ranking")
    if not isinstance(ranking, list) or not ranking:
        add("ranking missing or empty", "REPORT_SCHEMA_FAILURE", domain="implementation")
    selected = evaluated.get("selected_reference_source")
    if selected is None:
        if any(
            _num(_dict(reference_sources.get(source)).get("valid_reference_event_count")) > 0
            for source in REFERENCE_SOURCES
        ):
            add(
                "measured reference sources are below valid_rate_eligible_rows 0.95",
                "LABEL_VALID_RATE_FAILURE",
                domain="benchmark",
            )
        if "MULTI_FEED_CAPTURE_INCOMPLETE" not in classifications and "FRESH_CAPTURE_NOT_PERFORMED" not in classifications:
            empty_sources = [source for source, status in source_statuses.items() if status == "captured_but_empty"]
            if empty_sources:
                if "REFERENCE_FEED_EMPTY" not in classifications:
                    classifications.append("REFERENCE_FEED_EMPTY")
                benchmark_status = "fail"
            add(
                "no reference source achieved valid_rate_eligible_rows >= 0.95 with strict 100ms gate",
                "NO_REFERENCE_SOURCE_PASSED_100MS",
                domain="benchmark",
                primary_classification="NO_REFERENCE_SOURCE_PASSED_100MS",
            )
    elif selected not in REFERENCE_SOURCES:
        add("selected_reference_source invalid", "REPORT_SCHEMA_FAILURE", domain="implementation")
    else:
        selected_metrics = _dict(reference_sources.get(str(selected)))
        if _num(selected_metrics.get("valid_rate_eligible_rows")) < REQUIRED_100MS_VALID_RATE:
            add("selected reference source valid_rate_eligible_rows below 0.95", "LABEL_VALID_RATE_FAILURE", domain="benchmark")
        if _num(selected_metrics.get("label_leakage_violations")) > 0:
            add("selected source label_leakage_violations > 0", "LABEL_LEAKAGE_FAILURE", domain="implementation")
        if _num(selected_metrics.get("reference_timestamp_monotonic_violations")) > 0:
            add("selected source reference timestamp monotonic violations > 0", "REFERENCE_TIMESTAMP_MONOTONIC_FAILURE", domain="benchmark")
        if selected in {"trade_price", "aggTrade_price"} and not evaluated.get("semantic_warning"):
            add("trade/aggTrade selected without semantic warning", "REPORT_SCHEMA_FAILURE", domain="implementation")

    leakage = _dict(evaluated.get("leakage_check"))
    if _num(leakage.get("feature_leakage_violations")) > 0:
        add("global feature_leakage_violations > 0", "FEATURE_LEAKAGE_FAILURE", domain="implementation")

    if hard and primary is None:
        primary = classify_phase42c_failure({**evaluated, "hard_fail_reasons": hard, "failure_classifications": classifications})
    hard = list(dict.fromkeys(hard))
    evaluated["implementation_status"] = implementation_status
    evaluated["runtime_status"] = runtime_status
    evaluated["benchmark_status"] = benchmark_status
    evaluated["definition_of_done_status"] = "fail" if hard else "pass"
    evaluated["status"] = evaluated["definition_of_done_status"]
    evaluated["primary_failure"] = primary if hard else None
    evaluated["failure_classifications"] = sorted(set(classifications)) if hard else []
    evaluated["hard_fail_reasons"] = hard
    evaluated["warning_reasons"] = sorted(set(warnings))
    evaluated["selected_reference_source_status"] = "pass" if not hard and selected is not None else ("fail" if selected is None else evaluated.get("selected_reference_source_status", "fail"))
    return evaluated


def classify_phase42c_failure(report: dict[str, Any]) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "UNKNOWN_PHASE42C_FAILURE"
    primary = str(report.get("primary_failure") or "")
    classifications = [str(item) for item in report.get("failure_classifications", [])]
    if primary == "MULTI_FEED_CAPTURE_INCOMPLETE" and "FRESH_CAPTURE_NOT_PERFORMED" in classifications:
        return "FRESH_CAPTURE_NOT_PERFORMED"
    reasons = " ".join(str(reason) for reason in report.get("hard_fail_reasons", []))
    for classification in (
        "ARTIFACT_CLEANUP_FAILURE",
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "MULTI_FEED_CAPTURE_INCOMPLETE",
        "DEPTH_RUNTIME_QUALITY_FAILURE",
        "REFERENCE_FEED_EMPTY",
        "REFERENCE_SCHEMA_FAILURE",
        "REFERENCE_TIMESTAMP_MONOTONIC_FAILURE",
        "HORIZON_100MS_POLICY_RELAXED",
        "NO_REFERENCE_SOURCE_PASSED_100MS",
        "LABEL_VALID_RATE_FAILURE",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "REPORT_SCHEMA_FAILURE",
        "SELF_CHECK_FAILURE",
        "BUNDLE_FAILURE",
    ):
        if classification in primary:
            return classification
    for classification in (
        "ARTIFACT_CLEANUP_FAILURE",
        "TEST_FAILURE",
        "TYPECHECK_FAILURE",
        "FRESH_CAPTURE_NOT_PERFORMED",
        "MULTI_FEED_CAPTURE_INCOMPLETE",
        "REFERENCE_SCHEMA_FAILURE",
        "HORIZON_100MS_POLICY_RELAXED",
        "FEATURE_LEAKAGE_FAILURE",
        "LABEL_LEAKAGE_FAILURE",
        "REPORT_SCHEMA_FAILURE",
        "BUNDLE_FAILURE",
        "NO_REFERENCE_SOURCE_PASSED_100MS",
        "LABEL_VALID_RATE_FAILURE",
        "REFERENCE_FEED_EMPTY",
    ):
        if classification in classifications:
            return classification
    if "pytest failed" in reasons:
        return "TEST_FAILURE"
    if "typecheck" in reasons or "compileall" in reasons:
        return "TYPECHECK_FAILURE"
    if "cleanup" in reasons:
        return "ARTIFACT_CLEANUP_FAILURE"
    if "capture" in reasons:
        return "MULTI_FEED_CAPTURE_INCOMPLETE"
    if "snapshot_copy_p99_us" in reasons or any(field in reasons for field in DEPTH_RUNTIME_ZERO_FIELDS):
        return "DEPTH_RUNTIME_QUALITY_FAILURE"
    if "missing or empty" in reasons or "datasets missing" in reasons or "clean sample count = 0" in reasons:
        return "MULTI_FEED_CAPTURE_INCOMPLETE"
    if "schema validation failed" in reasons:
        return "REFERENCE_SCHEMA_FAILURE"
    if "monotonic" in reasons:
        return "REFERENCE_TIMESTAMP_MONOTONIC_FAILURE"
    if "max_future_gap_ms != 100" in reasons:
        return "HORIZON_100MS_POLICY_RELAXED"
    if "no reference source achieved" in reasons:
        return "NO_REFERENCE_SOURCE_PASSED_100MS"
    if "valid_rate_eligible_rows" in reasons:
        return "LABEL_VALID_RATE_FAILURE"
    if "feature_leakage" in reasons:
        return "FEATURE_LEAKAGE_FAILURE"
    if "label_leakage" in reasons:
        return "LABEL_LEAKAGE_FAILURE"
    if "report schema invalid" in reasons:
        return "REPORT_SCHEMA_FAILURE"
    if "bundle failure" in reasons:
        return "BUNDLE_FAILURE"
    return "UNKNOWN_PHASE42C_FAILURE"


def validate_phase42c_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42C_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in ("implementation_status", "runtime_status", "benchmark_status", "definition_of_done_status"):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    sources = report.get("reference_sources")
    if not isinstance(sources, dict):
        errors.append("missing required object: reference_sources")
    else:
        for source in REFERENCE_SOURCES:
            metrics = sources.get(source)
            if not isinstance(metrics, dict):
                errors.append(f"missing reference source: {source}")
                continue
            missing = sorted(REQUIRED_SOURCE_METRIC_FIELDS - set(metrics))
            errors.extend(f"missing {source} metric: {field}" for field in missing)
    ranking = report.get("ranking")
    if not isinstance(ranking, list):
        errors.append("ranking must be a list")
    selected = report.get("selected_reference_source")
    if selected is not None and selected not in REFERENCE_SOURCES:
        errors.append("selected_reference_source must be null or one of the benchmark sources")
    return errors


def run_phase42c_analysis(
    *,
    root: str | Path,
    symbol: str,
    clean_samples_path: str | Path,
    bookticker_path: str | Path,
    trade_path: str | Path,
    aggtrade_path: str | Path,
    benchmark_labels_path: str | Path,
    depth_runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
    capture_diagnostics: dict[str, Any] | None = None,
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
    references_by_source = {
        source: validation.valid_events
        for source, validation in validations.items()
    }
    benchmark_rows = generate_benchmark_rows(clean_samples, references_by_source) if clean_samples else []
    labels_path = _resolve(root_path, benchmark_labels_path)
    write_jsonl(labels_path, benchmark_rows)
    write_phase42c_invalid_cases(benchmark_rows, root_path / PHASE42C_INVALID_100MS)
    leakage = run_phase42c_leakage_check(benchmark_rows, output_path=root_path / PHASE42C_LEAKAGE_CHECK)
    by_source = leakage.get("label_leakage_violations_by_source", {})
    source_metrics = {
        source: compute_source_metrics(
            reference_source=source,
            validation=validation,
            clean_samples=clean_samples,
            benchmark_rows=benchmark_rows,
            label_leakage_violations=int(_dict(by_source).get(source, 0) or 0),
            capture_diagnostics=capture_diagnostics or _dict(capture.get("capture_diagnostics")),
            symbol=symbol,
        )
        for source, validation in validations.items()
    }
    report = build_phase42c_report(
        symbol=symbol,
        clean_samples=clean_samples,
        source_metrics=source_metrics,
        benchmark_rows=benchmark_rows,
        leakage_result=leakage,
        depth_runtime_quality=depth_runtime_quality,
        capture={**capture, "capture_diagnostics": capture_diagnostics or _dict(capture.get("capture_diagnostics"))},
        fresh_capture_required=fresh_capture_required,
        dataset_paths={
            "clean_samples": _relative_display(root_path, clean_path),
            "bookticker_reference_quotes": _relative_display(root_path, _resolve(root_path, bookticker_path)),
            "trade_reference_events": _relative_display(root_path, _resolve(root_path, trade_path)),
            "aggtrade_reference_events": _relative_display(root_path, _resolve(root_path, aggtrade_path)),
            "benchmark_labels": _relative_display(root_path, labels_path),
        },
    )
    if clean_validation.failure_classification:
        report["hard_fail_reasons"].append(f"clean sample validation failed: {clean_validation.failure_classification}")
        report["primary_failure"] = report.get("primary_failure") or "DEPTH_RUNTIME_QUALITY_FAILURE"
        report = evaluate_phase42c_report(report)
    return report


def write_phase42c_invalid_cases(
    benchmark_rows: list[dict[str, Any]],
    path: str | Path,
) -> None:
    cases: list[dict[str, Any]] = []
    for row in benchmark_rows:
        labels = row.get("reference_labels")
        if not isinstance(labels, dict):
            continue
        for source in REFERENCE_SOURCES:
            label = labels.get(source, {}).get(HORIZON_NAME) if isinstance(labels.get(source), dict) else None
            if not isinstance(label, dict) or label.get("valid") is True:
                continue
            cases.append(
                {
                    "symbol": row.get("symbol"),
                    "generation_id": row.get("generation_id"),
                    "last_update_id": row.get("last_update_id"),
                    "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                    "reference_source": source,
                    "horizon": HORIZON_NAME,
                    "invalid_reason": label.get("invalid_reason"),
                    "target_local_recv_monotonic_ns": label.get("target_local_recv_monotonic_ns"),
                    "future_reference_local_recv_monotonic_ns": label.get("future_reference_local_recv_monotonic_ns"),
                    "future_gap_ms": label.get("future_gap_ms"),
                }
            )
    write_jsonl(path, cases)


def write_phase42c_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42c_report(report)
    _write_json(root_path / PHASE42C_REPORT_JSON, report)
    _write_text(root_path / PHASE42C_REPORT_MD, render_phase42c_markdown(report))
    _write_json(root_path / PHASE42C_REFERENCE_SUMMARY, _reference_summary(report))
    _write_json(root_path / PHASE42C_GAP_DISTRIBUTION, _gap_distribution_summary(report))
    _write_json(root_path / PHASE42C_LEAKAGE_CHECK, report.get("leakage_check", {}))
    _write_text(root_path / PHASE42C_PYTEST_OUTPUT, pytest_output)
    _ensure_jsonl_exists(root_path / PHASE42C_INVALID_100MS)
    classification = None if report.get("definition_of_done_status") == "pass" else classify_phase42c_failure(report)
    self_check = {
        "phase": PHASE,
        "passed": report.get("definition_of_done_status") == "pass",
        "status": report.get("definition_of_done_status"),
        "definition_of_done_status": report.get("definition_of_done_status"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42C_REPORT_JSON),
        "report_md_path": _display_path(PHASE42C_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42C_PYTEST_OUTPUT),
        "bundle_path": _display_path(PHASE42C_BUNDLE),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42C_SELF_CHECK_JSON, self_check)
    if report.get("definition_of_done_status") != "pass":
        write_phase42c_failure_investigation(root=root_path, report=report, classification=classification)


def render_phase42c_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phase 4.2C Reference Feed Benchmark Report",
        "",
        f"Status: **{report.get('definition_of_done_status')}**",
        "",
        "## Status",
        "",
        f"- Implementation: `{report.get('implementation_status')}`",
        f"- Runtime: `{report.get('runtime_status')}`",
        f"- Benchmark: `{report.get('benchmark_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        "",
        "## Ranking",
        "",
    ]
    for item in report.get("ranking", []):
        lines.append(
            "- `{source}` valid_rate_eligible_rows=`{rate}` gap_p95_ms=`{p95}` "
            "gap_p99_ms=`{p99}` passes_100ms_gate=`{passed}` semantic=`{semantic}`".format(
                source=item.get("reference_source"),
                rate=item.get("valid_rate_eligible_rows"),
                p95=item.get("gap_p95_ms"),
                p99=item.get("gap_p99_ms"),
                passed=item.get("passes_100ms_gate"),
                semantic=item.get("semantic_type"),
            )
        )
    lines.extend(
        [
            "",
            "## Selected Source",
            "",
            f"- Selected reference source: `{report.get('selected_reference_source')}`",
            f"- Semantic warning: `{report.get('semantic_warning')}`",
            "",
            "## Sources",
            "",
        ]
    )
    sources = report.get("reference_sources", {})
    if isinstance(sources, dict):
        for source in REFERENCE_SOURCES:
            metrics = sources.get(source, {})
            if not isinstance(metrics, dict):
                continue
            lines.append(
                "- `{source}` events=`{events}` valid_events=`{valid_events}` "
                "eligible_rate=`{rate}` future_gap_p95/p99=`{fp95}`/`{fp99}` "
                "bad_time_ratio=`{bad}` leakage=`{leakage}`".format(
                    source=source,
                    events=metrics.get("reference_event_count"),
                    valid_events=metrics.get("valid_reference_event_count"),
                    rate=metrics.get("valid_rate_eligible_rows"),
                    fp95=metrics.get("future_gap_p95_ms"),
                    fp99=metrics.get("future_gap_p99_ms"),
                    bad=metrics.get("bad_time_coverage_ratio_100ms"),
                    leakage=metrics.get("label_leakage_violations"),
                )
            )
    lines.extend(["", "## Hard Fail Reasons", ""])
    reasons = report.get("hard_fail_reasons", [])
    lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- None")
    lines.extend(["", "## Recommendation", "", str(report.get("recommendation")), ""])
    return "\n".join(lines)


def create_phase42c_bundle(
    *,
    root: str | Path,
    source_root: str | Path = REPO_ROOT,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    source_path = Path(source_root)
    target = Path(bundle_path) if bundle_path is not None else root_path / PHASE42C_BUNDLE
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for directory_name in ("app", "tests", "scripts"):
            archive.writestr(f"{directory_name}/", "")
        _write_directory_to_archive(archive, source_path / "bot/app", "app")
        _write_directory_to_archive(archive, source_path / "tests", "tests")
        _write_directory_to_archive(archive, source_path / "scripts", "scripts")
        for relative in PHASE42C_REQUIRED_BUNDLE_FILES:
            if relative.endswith("/"):
                continue
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / PHASE42C_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42C_INVESTIGATION))
    missing = phase42c_bundle_missing_files(target)
    if missing:
        raise RuntimeError(f"Phase 4.2C bundle missing required files: {missing}")
    return target


def phase42c_bundle_missing_files(bundle_path: str | Path) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    return [name for name in PHASE42C_REQUIRED_BUNDLE_FILES if name not in names]


def write_phase42c_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2C Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Definition of Done status: `{report.get('definition_of_done_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Report path: `{_display_path(PHASE42C_REPORT_JSON)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Recommendation",
        "",
        _recommendation(report.get("selected_reference_source")),
        "",
        "## Phase Boundary",
        "",
        "No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.",
        "",
    ]
    _write_text(Path(root) / PHASE42C_INVESTIGATION, "\n".join(lines))


def reference_price(row: dict[str, Any], reference_source: str) -> float | None:
    field = "mid_price" if reference_source in {"depth_mid", "bookTicker_mid"} else "price"
    value = row.get(field)
    return _float_or_none(value)


def reference_event_id(row: dict[str, Any], reference_source: str) -> Any:
    if reference_source == "bookTicker_mid":
        return row.get("update_id")
    if reference_source == "trade_price":
        return row.get("trade_id")
    if reference_source == "aggTrade_price":
        return row.get("aggregate_trade_id")
    return row.get("event_id", row.get("last_update_id"))


def sample_mid_price(sample: dict[str, Any]) -> float | None:
    if isinstance(sample.get("mid_price"), (int, float, str)):
        value = _float_or_none(sample.get("mid_price"))
        if value is not None and value > 0:
            return value
    bid = _float_or_none(sample.get("best_bid"))
    ask = _float_or_none(sample.get("best_ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return (bid + ask) / 2.0


def sample_spread_bps(sample: dict[str, Any]) -> float | None:
    if isinstance(sample.get("spread_bps"), (int, float, str)):
        value = _float_or_none(sample.get("spread_bps"))
        if value is not None and value >= 0:
            return value
    bid = _float_or_none(sample.get("best_bid"))
    ask = _float_or_none(sample.get("best_ask"))
    mid = sample_mid_price(sample)
    if bid is None or ask is None or mid is None or mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def empty_phase42c_report(
    *,
    symbol: str,
    capture: dict[str, Any],
    classification: str,
    reason: str,
) -> dict[str, Any]:
    empty_quality = analyze_reference_gap_distribution([])
    source_metrics = {
        source: {
            "reference_source": source,
            "semantic_type": SEMANTIC_TYPES[source],
            "semantic_description": SEMANTIC_DESCRIPTIONS[source],
            "file_exists": False,
            "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
            "reference_event_count": 0,
            "valid_reference_event_count": 0,
            "invalid_reference_event_count": 0,
            "reference_timestamp_monotonic_violations": 0,
            "eligible_count": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "valid_rate_all_rows": 0.0,
            "valid_rate_eligible_rows": 0.0,
            "invalid_reason_counts": {},
            "future_gap_p50_ms": None,
            "future_gap_p90_ms": None,
            "future_gap_p95_ms": None,
            "future_gap_p99_ms": None,
            "future_gap_max_ms": None,
            "label_leakage_violations": 0,
            "passes_100ms_gate": False,
            "source_status": "not_captured",
            **empty_quality,
        }
        for source in REFERENCE_SOURCES
    }
    report = build_phase42c_report(
        symbol=symbol,
        clean_samples=[],
        source_metrics=source_metrics,
        benchmark_rows=[],
        leakage_result={
            "passed": False,
            "feature_leakage_violations": 0,
            "label_leakage_violations": 0,
            "label_leakage_violations_by_source": {source: 0 for source in REFERENCE_SOURCES},
            "checked_samples": 0,
            "checked_sources": list(REFERENCE_SOURCES),
            "checked_horizons": [HORIZON_NAME],
            "violations": [],
        },
        depth_runtime_quality={},
        capture=capture,
        fresh_capture_required=not bool(capture.get("fixture_mode", False)),
    )
    report["primary_failure"] = "MULTI_FEED_CAPTURE_INCOMPLETE" if classification == "FRESH_CAPTURE_NOT_PERFORMED" else classification
    report["failure_classifications"] = [classification]
    report["hard_fail_reasons"].append(reason)
    if classification == "TEST_FAILURE":
        report["pytest_failed"] = True
    if classification == "TYPECHECK_FAILURE":
        report["typecheck_failed"] = True
    if classification == "ARTIFACT_CLEANUP_FAILURE":
        report["cleanup_failed"] = True
    if classification in {"MULTI_FEED_CAPTURE_FAILURE", "MULTI_FEED_CAPTURE_INCOMPLETE"}:
        report["multi_feed_capture_failed"] = True
    return evaluate_phase42c_report(report)


def _reference_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "phase": PHASE,
        "status": report.get("status"),
        "selected_reference_source": report.get("selected_reference_source"),
        "reference_sources": {
            source: {
                "reference_event_count": _dict(metrics).get("reference_event_count"),
                "valid_reference_event_count": _dict(metrics).get("valid_reference_event_count"),
                "invalid_reference_event_count": _dict(metrics).get("invalid_reference_event_count"),
                "valid_rate_eligible_rows": _dict(metrics).get("valid_rate_eligible_rows"),
                "passes_100ms_gate": _dict(metrics).get("passes_100ms_gate"),
            }
            for source, metrics in _dict(report.get("reference_sources")).items()
        },
    }


def _gap_distribution_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        source: {
            "gap_p50_ms": _dict(metrics).get("gap_p50_ms"),
            "gap_p90_ms": _dict(metrics).get("gap_p90_ms"),
            "gap_p95_ms": _dict(metrics).get("gap_p95_ms"),
            "gap_p99_ms": _dict(metrics).get("gap_p99_ms"),
            "gap_max_ms": _dict(metrics).get("gap_max_ms"),
            "gap_over_100ms_count": _dict(metrics).get("gap_over_100ms_count"),
            "gap_over_100ms_total_duration_ms": _dict(metrics).get("gap_over_100ms_total_duration_ms"),
            "bad_time_coverage_ratio_100ms": _dict(metrics).get("bad_time_coverage_ratio_100ms"),
        }
        for source, metrics in _dict(report.get("reference_sources")).items()
    }


def _normalize_depth_runtime_quality(quality: dict[str, Any]) -> dict[str, Any]:
    normalized = {field: int(_num(quality.get(field))) for field in DEPTH_RUNTIME_ZERO_FIELDS}
    normalized["snapshot_copy_p99_us"] = _num(quality.get("snapshot_copy_p99_us"))
    return normalized


def _recommendation(selected_reference_source: Any) -> str:
    if selected_reference_source:
        return "Review the selected reference semantics before any later research phase."
    return (
        "Keep 100ms as a hard gate. Next engineering step: collect during a more active "
        "session, benchmark futures/SBE/paid feeds later, or improve capture locality. "
        "Do not move to strategy/model/execution/PnL from this failing benchmark."
    )


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "Phase 4.2C Definition of Done passed; pass bundle may be created."
    return f"Phase 4.2C failed with classification {classification}. No pass bundle was created."


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _relative_display(root: Path, path: Path) -> str:
    try:
        return _display_path(path.relative_to(root))
    except ValueError:
        return _display_path(path)


def _num(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_of_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _percentile(values: list[float], percentile: float) -> float | None:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = int(round((len(clean) - 1) * percentile))
    return clean[min(max(index, 0), len(clean) - 1)]


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _write_json(path: str | Path | None, payload: Any) -> None:
    if path is None:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: str | Path | None, rows: list[dict[str, Any]]) -> None:
    if path is None:
        return
    write_jsonl(path, rows)


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _ensure_jsonl_exists(path: str | Path) -> None:
    target = Path(path)
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("", encoding="utf-8")


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
