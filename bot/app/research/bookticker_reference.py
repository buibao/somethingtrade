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
    MAX_FUTURE_GAP_MS,
    NS_PER_MS,
    compute_return_bps,
    direction_label,
    generate_labeled_rows,
    spread_adjusted_direction_label,
    validate_clean_samples,
    validate_labeled_rows,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
REQUIRED_100MS_MAX_FUTURE_GAP_MS = 100
REQUIRED_100MS_VALID_RATE = 0.95
REFERENCE_GAP_P95_MAX_MS = 100.0
REFERENCE_GAP_P99_MAX_MS = 200.0
PHASE42B_REPORT_JSON = Path("data/reports/phase_4_2b_bookticker_reference_report.json")
PHASE42B_REPORT_MD = Path("data/reports/phase_4_2b_bookticker_reference_report.md")
PHASE42B_SELF_CHECK_JSON = Path("data/reports/phase42b_self_check.json")
PHASE42B_REFERENCE_SUMMARY = Path("data/debug/phase_4_2b_reference_feed_summary.json")
PHASE42B_INVALID_QUOTES = Path("data/debug/phase_4_2b_bookticker_reference_quotes_invalid.jsonl")
PHASE42B_INVALID_100MS = Path("data/debug/phase_4_2b_100ms_invalid_cases.jsonl")
PHASE42B_ALIGNMENT_CHECK = Path("data/debug/phase_4_2b_alignment_check.json")
PHASE42B_LEAKAGE_CHECK = Path("data/debug/phase_4_2b_leakage_check.json")
PHASE42B_PYTEST_OUTPUT = Path("data/debug/phase_4_2b_pytest_output.txt")
PHASE42B_INVESTIGATION = Path("data/debug/phase42b_failure_investigation.md")
PHASE42B_BUNDLE = Path("phase_4_2b_bookticker_reference_pass_bundle.zip")
BOOKTICKER_REFERENCE_QUOTES = Path("data/dataset/bookticker_reference_quotes.jsonl")

PHASE42B_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "implementation_status",
        "runtime_status",
        "reference_feed_status",
        "dataset_coverage_status",
        "definition_of_done_status",
        "primary_failure",
        "symbol",
        "inputs",
        "outputs",
        "capture",
        "depth_runtime_quality",
        "reference_feed_quality",
        "alignment_quality",
        "horizon_100ms",
        "leakage_check",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

PHASE42B_REQUIRED_BUNDLE_FILES = (
    "app/",
    "tests/",
    "scripts/",
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/bookticker_reference_quotes.jsonl",
    "data/dataset/orderbook_labeled_samples.jsonl",
    "data/reports/phase_4_2b_bookticker_reference_report.json",
    "data/reports/phase_4_2b_bookticker_reference_report.md",
    "data/reports/phase42b_self_check.json",
    "data/debug/phase_4_2b_reference_feed_summary.json",
    "data/debug/phase_4_2b_bookticker_reference_quotes_invalid.jsonl",
    "data/debug/phase_4_2b_100ms_invalid_cases.jsonl",
    "data/debug/phase_4_2b_alignment_check.json",
    "data/debug/phase_4_2b_leakage_check.json",
    "data/debug/phase_4_2b_pytest_output.txt",
)

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

REQUIRED_REFERENCE_FIELDS = frozenset(
    {
        "schema_version",
        "symbol",
        "source",
        "local_recv_monotonic_ns",
        "local_recv_wall_ts",
        "exchange_event_ts",
        "update_id",
        "best_bid",
        "best_bid_qty",
        "best_ask",
        "best_ask_qty",
        "mid_price",
        "spread",
        "spread_bps",
        "quality",
    }
)


@dataclass(frozen=True)
class ReferenceQuoteValidationResult:
    reference_quote_count: int
    valid_reference_quote_count: int
    invalid_reference_quote_count: int
    valid_quotes: list[dict[str, Any]]
    invalid_quotes: list[dict[str, Any]]
    quality: dict[str, Any]


def parse_bookticker_payload(
    payload: dict[str, Any],
    *,
    local_recv_monotonic_ns: int,
    local_recv_wall_ts: str,
) -> dict[str, Any]:
    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]
    errors: list[str] = []
    warnings: list[str] = []
    update_id = payload.get("u")
    symbol = payload.get("s")
    bid = _optional_float(payload.get("b"), "MISSING_BID", "INVALID_BID", errors)
    ask = _optional_float(payload.get("a"), "MISSING_ASK", "INVALID_ASK", errors)
    bid_qty = _optional_float(payload.get("B"), "MISSING_BID_QTY", "INVALID_BID_QTY", errors)
    ask_qty = _optional_float(payload.get("A"), "MISSING_ASK_QTY", "INVALID_ASK_QTY", errors)
    if bid_qty is not None and bid_qty < 0:
        errors.append("NEGATIVE_QTY")
    if ask_qty is not None and ask_qty < 0:
        errors.append("NEGATIVE_QTY")
    if bid_qty == 0 or ask_qty == 0:
        warnings.append("ZERO_QTY")
    mid_price = None
    spread = None
    spread_bps = None
    if bid is not None and ask is not None:
        if bid <= 0:
            errors.append("NON_POSITIVE_BID")
        if ask <= 0:
            errors.append("NON_POSITIVE_ASK")
        if bid >= ask:
            errors.append("CROSSED_QUOTE")
        mid_price = (bid + ask) / 2.0
        spread = ask - bid
        spread_bps = spread / mid_price * 10_000 if mid_price > 0 else None
    if update_id is None:
        errors.append("MISSING_UPDATE_ID")
    if not symbol:
        errors.append("MISSING_SYMBOL")
    if not isinstance(local_recv_monotonic_ns, int):
        errors.append("MISSING_LOCAL_RECV_MONOTONIC_NS")
    if not local_recv_wall_ts:
        errors.append("MISSING_LOCAL_RECV_WALL_TS")
    return {
        "schema_version": "bookticker_reference_v1",
        "symbol": symbol,
        "source": "binance_ws_bookTicker",
        "local_recv_monotonic_ns": local_recv_monotonic_ns,
        "local_recv_wall_ts": local_recv_wall_ts,
        "exchange_event_ts": payload.get("E"),
        "update_id": update_id,
        "best_bid": bid,
        "best_bid_qty": bid_qty,
        "best_ask": ask,
        "best_ask_qty": ask_qty,
        "mid_price": mid_price,
        "spread": spread,
        "spread_bps": spread_bps,
        "quality": {
            "valid": not errors,
            "errors": sorted(set(errors)),
            "warnings": sorted(set(warnings)),
            "zero_qty_policy": "allowed_with_warning",
        },
    }


def validate_reference_quotes(
    path: str | Path,
    *,
    invalid_output_path: str | Path | None = None,
) -> ReferenceQuoteValidationResult:
    quote_path = Path(path)
    quotes: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    if not quote_path.exists():
        invalid_rows.append({"line": None, "reason": "REFERENCE_FEED_MISSING"})
        if invalid_output_path is not None:
            write_jsonl(invalid_output_path, invalid_rows)
        return ReferenceQuoteValidationResult(
            reference_quote_count=0,
            valid_reference_quote_count=0,
            invalid_reference_quote_count=1,
            valid_quotes=[],
            invalid_quotes=invalid_rows,
            quality=analyze_reference_feed_quality([]),
        )
    with quote_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                invalid_rows.append({"line": line_number, "reason": f"INVALID_JSON:{exc}"})
                continue
            if not isinstance(row, dict):
                invalid_rows.append({"line": line_number, "reason": "ROW_NOT_OBJECT"})
                continue
            row = dict(row)
            errors = _reference_schema_errors(row)
            row_quality = row.get("quality")
            quality: dict[str, Any] = row_quality if isinstance(row_quality, dict) else {}
            if quality.get("valid") is not True:
                errors.extend(str(reason) for reason in quality.get("errors", []))
            if errors:
                invalid_rows.append(
                    {
                        "line": line_number,
                        "symbol": row.get("symbol"),
                        "update_id": row.get("update_id"),
                        "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                        "reason": sorted(set(errors)),
                    }
                )
            quotes.append(row)
    valid_quotes = [
        row
        for row in quotes
        if not _reference_schema_errors(row)
        and isinstance(row.get("quality"), dict)
        and row["quality"].get("valid") is True
    ]
    if invalid_output_path is not None:
        write_jsonl(invalid_output_path, invalid_rows)
    return ReferenceQuoteValidationResult(
        reference_quote_count=len(quotes),
        valid_reference_quote_count=len(valid_quotes),
        invalid_reference_quote_count=len(invalid_rows),
        valid_quotes=valid_quotes,
        invalid_quotes=invalid_rows,
        quality=analyze_reference_feed_quality(valid_quotes),
    )


def analyze_reference_feed_quality(reference_quotes: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [
        int(row["local_recv_monotonic_ns"])
        for row in reference_quotes
        if isinstance(row.get("local_recv_monotonic_ns"), int)
    ]
    gaps = [
        (timestamps[index] - timestamps[index - 1]) / NS_PER_MS
        for index in range(1, len(timestamps))
    ]
    non_negative_gaps = [gap for gap in gaps if gap >= 0]
    update_ids = [row.get("update_id") for row in reference_quotes if row.get("update_id") is not None]
    duplicate_update_ids = len(update_ids) - len(set(update_ids))
    invalid_reason_counts: Counter[str] = Counter()
    for row in reference_quotes:
        row_quality = row.get("quality")
        quality: dict[str, Any] = row_quality if isinstance(row_quality, dict) else {}
        invalid_reason_counts.update(str(reason) for reason in quality.get("errors", []))
    duration_sec = (
        (timestamps[-1] - timestamps[0]) / 1_000_000_000.0
        if len(timestamps) >= 2
        else 0.0
    )
    return {
        "reference_quote_count": len(reference_quotes),
        "valid_reference_quote_count": sum(
            1
            for row in reference_quotes
            if isinstance(row.get("quality"), dict) and row["quality"].get("valid") is True
        ),
        "invalid_reference_quote_count": sum(
            1
            for row in reference_quotes
            if not isinstance(row.get("quality"), dict) or row["quality"].get("valid") is not True
        ),
        "reference_sample_rate_per_sec": len(reference_quotes) / duration_sec if duration_sec > 0 else 0.0,
        "reference_gap_p50_ms": _percentile(non_negative_gaps, 0.50),
        "reference_gap_p90_ms": _percentile(non_negative_gaps, 0.90),
        "reference_gap_p95_ms": _percentile(non_negative_gaps, 0.95),
        "reference_gap_p99_ms": _percentile(non_negative_gaps, 0.99),
        "reference_gap_max_ms": max(non_negative_gaps) if non_negative_gaps else None,
        "duplicate_update_id_count": duplicate_update_ids,
        "non_monotonic_reference_timestamp_count": sum(1 for gap in gaps if gap < 0),
        "invalid_quote_reason_counts": dict(sorted(invalid_reason_counts.items())),
    }


def select_future_reference_index(
    reference_quotes: list[dict[str, Any]],
    *,
    feature_timestamp_ns: int,
    horizon_ms: int,
    reference_timestamps_ns: list[int] | None = None,
) -> int | None:
    timestamps = reference_timestamps_ns
    if timestamps is None:
        timestamps = [int(row["local_recv_monotonic_ns"]) for row in reference_quotes]
    target = feature_timestamp_ns + horizon_ms * NS_PER_MS
    index = bisect_left(timestamps, target)
    return index if index < len(reference_quotes) else None


def generate_labeled_rows_with_bookticker(
    clean_samples: list[dict[str, Any]],
    reference_quotes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    valid_references = [
        row
        for row in reference_quotes
        if isinstance(row.get("quality"), dict) and row["quality"].get("valid") is True
    ]
    rows = generate_labeled_rows(clean_samples)
    reference_timestamps = [int(row["local_recv_monotonic_ns"]) for row in valid_references]
    for index, row in enumerate(rows):
        feature_ts = int(row["local_recv_monotonic_ns"])
        first_after_feature = bisect_right(reference_timestamps, feature_ts)
        row["labels"]["horizon_100ms"] = _bookticker_label(
            row=row,
            valid_references=valid_references,
            valid_reference_timestamps=reference_timestamps,
            first_after_feature=first_after_feature,
        )
        row["quality"]["bookticker_reference_used_for_features"] = False
        row["quality"]["label_reference_source"] = "bookTicker"
    return rows


def run_bookticker_leakage_check(
    labeled_rows: list[dict[str, Any]],
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    for index, row in enumerate(labeled_rows):
        row_quality = row.get("quality")
        quality: dict[str, Any] = row_quality if isinstance(row_quality, dict) else {}
        feature_sources = quality.get("feature_source_indices", {})
        if isinstance(feature_sources, dict):
            for feature_name, source_index in feature_sources.items():
                if isinstance(source_index, int) and source_index > index:
                    violations.append(
                        {
                            "type": "feature",
                            "sample_index": index,
                            "feature": feature_name,
                            "source_index": source_index,
                            "reason": "past_feature_uses_future_sample",
                        }
                    )
        label = row.get("labels", {}).get("horizon_100ms")
        if not isinstance(label, dict):
            continue
        feature_ts = row.get("local_recv_monotonic_ns")
        target_ts = label.get("target_local_recv_monotonic_ns")
        ref_ts = label.get("future_reference_local_recv_monotonic_ns")
        future_reference_index = label.get("future_reference_index")
        first_after_feature = label.get("first_reference_index_after_feature")
        if label.get("valid") is True:
            reason = None
            if not isinstance(ref_ts, int):
                reason = "valid_label_missing_future_reference_timestamp"
            elif isinstance(feature_ts, int) and ref_ts <= feature_ts:
                reason = "future_reference_not_after_feature_timestamp"
            elif isinstance(target_ts, int) and ref_ts < target_ts:
                reason = "future_reference_timestamp_before_target"
            elif (
                isinstance(future_reference_index, int)
                and isinstance(first_after_feature, int)
                and future_reference_index < first_after_feature
            ):
                reason = "future_reference_before_first_quote_after_feature"
            if reason:
                violations.append(
                    {
                        "type": "label",
                        "sample_index": index,
                        "horizon": "horizon_100ms",
                        "reason": reason,
                        "future_reference_local_recv_monotonic_ns": ref_ts,
                        "target_local_recv_monotonic_ns": target_ts,
                    }
                )
    feature_count = sum(1 for item in violations if item["type"] == "feature")
    label_count = sum(1 for item in violations if item["type"] == "label")
    result = {
        "passed": feature_count == 0 and label_count == 0,
        "feature_leakage_violations": feature_count,
        "label_leakage_violations": label_count,
        "checked_samples": len(labeled_rows),
        "checked_horizons": ["horizon_100ms"],
        "violations": violations,
    }
    _write_json(output_path, result)
    return result


def build_phase42b_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    reference_quotes: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    leakage_result: dict[str, Any],
    depth_runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
    clean_samples_path: str | Path = "data/dataset/orderbook_clean_samples.jsonl",
    reference_quotes_path: str | Path = "data/dataset/bookticker_reference_quotes.jsonl",
    labeled_samples_path: str | Path = "data/dataset/orderbook_labeled_samples.jsonl",
    invalid_cases_path: str | Path | None = None,
) -> dict[str, Any]:
    reference_quality = analyze_reference_feed_quality(reference_quotes)
    horizon = _horizon_100ms_stats(
        clean_samples,
        reference_quotes,
        labeled_rows,
        invalid_cases_path=invalid_cases_path,
    )
    report = {
        "phase": "4.2B",
        "status": "pass",
        "implementation_status": "pass",
        "runtime_status": "pass",
        "reference_feed_status": "pass",
        "dataset_coverage_status": "pass",
        "definition_of_done_status": "pass",
        "primary_failure": None,
        "symbol": symbol,
        "inputs": {
            "clean_samples": _display_path(clean_samples_path),
            "bookticker_reference_quotes": _display_path(reference_quotes_path),
        },
        "outputs": {"labeled_samples": _display_path(labeled_samples_path)},
        "capture": {
            "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
            "fixture_mode": bool(capture.get("fixture_mode", False)),
            "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
            "depth_stream": str(capture.get("depth_stream", f"{symbol.lower()}@depth@100ms")),
            "reference_stream": str(capture.get("reference_stream", f"{symbol.lower()}@bookTicker")),
            "downsampling_enabled": bool(capture.get("downsampling_enabled", False)),
        },
        "fresh_capture_required": fresh_capture_required,
        "depth_runtime_quality": _normalize_depth_runtime_quality(depth_runtime_quality),
        "reference_feed_quality": reference_quality,
        "alignment_quality": _alignment_quality(labeled_rows),
        "horizon_100ms": horizon,
        "leakage_check": {
            "passed": bool(leakage_result.get("passed", False)),
            "feature_leakage_violations": int(leakage_result.get("feature_leakage_violations", 0) or 0),
            "label_leakage_violations": int(leakage_result.get("label_leakage_violations", 0) or 0),
            "violations": leakage_result.get("violations", []),
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": len(labeled_rows),
        "hard_fail_reasons": [],
        "warning_reasons": [],
        "bottleneck_assessment": _bottleneck_assessment(reference_quality, horizon),
    }
    return evaluate_phase42b_report(report)


def evaluate_phase42b_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard: list[str] = []
    implementation_status = "pass"
    runtime_status = "pass"
    reference_status = "pass"
    coverage_status = "pass"
    primary: str | None = None
    schema_errors = validate_phase42b_report_schema(evaluated)
    if schema_errors:
        implementation_status = "fail"
        hard.extend(f"report schema invalid: {error}" for error in schema_errors)
        primary = primary or "report_schema_invalid"
    if evaluated.get("pytest_failed") is True:
        implementation_status = "fail"
        hard.append("pytest failed")
        primary = "pytest_failed"
    capture = _dict(evaluated.get("capture"))
    if evaluated.get("fresh_capture_required") is True and capture.get("fresh_capture_performed") is not True:
        runtime_status = "fail"
        hard.append("fresh dual-feed capture required but not performed")
        primary = primary or "dual_feed_capture_not_performed"
    if capture.get("fresh_capture_performed") is True and _num(capture.get("duration_sec")) < 1800:
        runtime_status = "fail"
        hard.append("fresh capture duration_sec < 1800")
        primary = primary or "fresh_capture_duration_too_short"
    if capture.get("downsampling_enabled") is True:
        runtime_status = "fail"
        hard.append("downsampling_enabled must be false")
        primary = primary or "downsampling_enabled"
    runtime = _dict(evaluated.get("depth_runtime_quality"))
    for field in DEPTH_RUNTIME_ZERO_FIELDS:
        if _num(runtime.get(field)) > 0:
            runtime_status = "fail"
            hard.append(f"{field} > 0: {runtime.get(field)}")
            primary = primary or "depth_runtime_quality_failed"
    ref = _dict(evaluated.get("reference_feed_quality"))
    if evaluated.get("reference_feed_missing") is True:
        reference_status = "fail"
        hard.append("bookTicker reference feed missing")
        primary = primary or "reference_feed_missing"
    if _num(ref.get("reference_quote_count")) <= 0:
        reference_status = "fail"
        hard.append("bookTicker reference quote count = 0")
        primary = primary or "reference_feed_empty"
    if _num(ref.get("valid_reference_quote_count")) <= 0:
        reference_status = "fail"
        hard.append("valid reference quote count = 0")
        primary = primary or "valid_reference_quote_count_zero"
    if _num(ref.get("invalid_reference_quote_count")) > 0:
        reference_status = "fail"
        hard.append(
            "reference quote schema violations > 0: "
            f"{ref.get('invalid_reference_quote_count')}"
        )
        primary = primary or "reference_feed_schema_invalid"
    if _num(ref.get("non_monotonic_reference_timestamp_count")) > 0:
        reference_status = "fail"
        hard.append(
            "reference timestamp non-monotonic count > 0: "
            f"{ref.get('non_monotonic_reference_timestamp_count')}"
        )
        primary = primary or "reference_timestamp_non_monotonic"
    if _num(ref.get("reference_gap_p95_ms")) > REFERENCE_GAP_P95_MAX_MS:
        reference_status = "fail"
        hard.append(f"reference_gap_p95_ms > 100: {ref.get('reference_gap_p95_ms')}")
        primary = primary or "reference_feed_gap_p95"
    if _num(ref.get("reference_gap_p99_ms")) > REFERENCE_GAP_P99_MAX_MS:
        reference_status = "fail"
        hard.append(f"reference_gap_p99_ms > 200: {ref.get('reference_gap_p99_ms')}")
        primary = primary or "reference_feed_gap_p99"
    if _num(evaluated.get("clean_sample_count")) <= 0:
        coverage_status = "fail"
        hard.append("feature sample count = 0")
        primary = primary or "feature_sample_count_zero"
    if _num(evaluated.get("labeled_sample_count")) <= 0:
        coverage_status = "fail"
        hard.append("labeled sample count = 0")
        primary = primary or "labeled_sample_count_zero"
    horizon = _dict(evaluated.get("horizon_100ms"))
    policy_relaxed = int(horizon.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS
    if horizon.get("reference_source") != "bookTicker":
        coverage_status = "fail"
        hard.append("horizon_100ms reference_source != bookTicker")
        primary = primary or "horizon_100ms_reference_source_invalid"
    if policy_relaxed:
        coverage_status = "fail"
        hard.append("horizon_100ms max_future_gap_ms != 100")
        primary = "horizon_100ms_policy_relaxed"
    if _num(horizon.get("valid_rate_eligible_rows")) < REQUIRED_100MS_VALID_RATE:
        coverage_status = "fail"
        hard.append(
            "horizon_100ms valid_rate_eligible_rows "
            f"{_num(horizon.get('valid_rate_eligible_rows')):.6f} below threshold 0.95"
        )
        if not policy_relaxed:
            primary = "horizon_100ms_valid_rate_below_threshold"
    leakage = _dict(evaluated.get("leakage_check"))
    if _num(leakage.get("feature_leakage_violations")) > 0:
        implementation_status = "fail"
        hard.append(f"feature_leakage_violations > 0: {leakage.get('feature_leakage_violations')}")
        primary = primary or "feature_leakage"
    if _num(leakage.get("label_leakage_violations")) > 0:
        implementation_status = "fail"
        hard.append(f"label_leakage_violations > 0: {leakage.get('label_leakage_violations')}")
        primary = primary or "label_leakage"
    hard = list(dict.fromkeys(hard))
    evaluated["implementation_status"] = implementation_status
    evaluated["runtime_status"] = runtime_status
    evaluated["reference_feed_status"] = reference_status
    evaluated["dataset_coverage_status"] = coverage_status
    evaluated["definition_of_done_status"] = "fail" if hard else "pass"
    evaluated["status"] = evaluated["definition_of_done_status"]
    evaluated["primary_failure"] = primary if hard else None
    evaluated["hard_fail_reasons"] = hard
    evaluated["warning_reasons"] = sorted(set(str(item) for item in evaluated.get("warning_reasons", [])))
    return evaluated


def classify_phase42b_failure(report: dict[str, Any]) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "UNKNOWN_PHASE42B_FAILURE"
    reasons = " ".join(str(reason) for reason in report.get("hard_fail_reasons", []))
    primary = str(report.get("primary_failure"))
    if "pytest_failed" in primary or "pytest failed" in reasons:
        return "TEST_FAILURE"
    if "report schema invalid" in reasons:
        return "REPORT_SCHEMA_FAILURE"
    if "reference_feed_missing" in primary or "reference feed missing" in reasons:
        return "REFERENCE_FEED_MISSING"
    if "dual_feed_capture" in primary or "capture required" in reasons or "fresh_capture_duration" in primary:
        return "DUAL_FEED_CAPTURE_FAILURE"
    if "depth_runtime" in primary or any(field in reasons for field in DEPTH_RUNTIME_ZERO_FIELDS):
        return "DEPTH_RUNTIME_QUALITY_FAILURE"
    if "reference_feed_empty" in primary or "reference quote count = 0" in reasons:
        return "REFERENCE_FEED_EMPTY"
    if "valid_reference_quote_count_zero" in primary:
        return "REFERENCE_QUOTE_INVALID_FAILURE"
    if "reference_feed_schema_invalid" in primary or "reference quote schema violations" in reasons:
        return "REFERENCE_FEED_SCHEMA_FAILURE"
    if "reference_timestamp_non_monotonic" in primary:
        return "REFERENCE_TIMESTAMP_MONOTONIC_FAILURE"
    if "reference_feed_gap" in primary:
        return "REFERENCE_FEED_GAP_FAILURE"
    if "horizon_100ms_policy_relaxed" in primary or "max_future_gap_ms != 100" in reasons:
        return "HORIZON_100MS_POLICY_RELAXED"
    if "horizon_100ms_valid_rate_below_threshold" in primary or "valid_rate_eligible_rows" in reasons:
        return "LABEL_VALID_RATE_FAILURE"
    if "feature_leakage" in primary:
        return "FEATURE_LEAKAGE_FAILURE"
    if "label_leakage" in primary:
        return "LABEL_LEAKAGE_FAILURE"
    return "UNKNOWN_PHASE42B_FAILURE"


def validate_phase42b_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42B_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in (
        "implementation_status",
        "runtime_status",
        "reference_feed_status",
        "dataset_coverage_status",
        "definition_of_done_status",
    ):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    horizon = report.get("horizon_100ms")
    if not isinstance(horizon, dict):
        errors.append("missing required object: horizon_100ms")
    else:
        for field in ("reference_source", "max_future_gap_ms", "eligible_count", "valid_count", "valid_rate_eligible_rows"):
            if field not in horizon:
                errors.append(f"missing horizon_100ms field: {field}")
    if not isinstance(report.get("reference_feed_quality"), dict):
        errors.append("missing required object: reference_feed_quality")
    return errors


def run_phase42b_analysis(
    *,
    root: str | Path,
    symbol: str,
    clean_samples_path: str | Path,
    reference_quotes_path: str | Path,
    labeled_samples_path: str | Path,
    depth_runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
) -> dict[str, Any]:
    root_path = Path(root)
    clean_path = _resolve(root_path, clean_samples_path)
    ref_path = _resolve(root_path, reference_quotes_path)
    labeled_path = _resolve(root_path, labeled_samples_path)
    clean_validation = validate_clean_samples(root_path / clean_path if not clean_path.is_absolute() else clean_path)
    reference_file_path = root_path / ref_path if not ref_path.is_absolute() else ref_path
    reference_missing = not reference_file_path.exists()
    reference_validation = validate_reference_quotes(
        reference_file_path,
        invalid_output_path=root_path / PHASE42B_INVALID_QUOTES,
    )
    clean_samples = clean_validation.samples if clean_validation.valid else []
    references = reference_validation.valid_quotes
    labeled = generate_labeled_rows_with_bookticker(clean_samples, references) if clean_samples else []
    write_jsonl(root_path / labeled_path if not labeled_path.is_absolute() else labeled_path, labeled)
    validate_labeled_rows(labeled)
    leakage = run_bookticker_leakage_check(labeled, output_path=root_path / PHASE42B_LEAKAGE_CHECK)
    report = build_phase42b_report(
        symbol=symbol,
        clean_samples=clean_samples,
        reference_quotes=references,
        labeled_rows=labeled,
        leakage_result=leakage,
        depth_runtime_quality=depth_runtime_quality,
        capture=capture,
        fresh_capture_required=fresh_capture_required,
        clean_samples_path=clean_path,
        reference_quotes_path=ref_path,
        labeled_samples_path=labeled_path,
        invalid_cases_path=root_path / PHASE42B_INVALID_100MS,
    )
    report["reference_feed_quality"]["reference_quote_count"] = reference_validation.reference_quote_count
    report["reference_feed_quality"]["valid_reference_quote_count"] = reference_validation.valid_reference_quote_count
    report["reference_feed_quality"]["invalid_reference_quote_count"] = reference_validation.invalid_reference_quote_count
    report["reference_feed_missing"] = reference_missing
    if clean_validation.failure_classification:
        report["dataset_coverage_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = report["primary_failure"] or "clean_sample_validation_failed"
        report["hard_fail_reasons"].append(f"clean sample validation failed: {clean_validation.failure_classification}")
    if reference_validation.reference_quote_count == 0:
        report["reference_feed_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = report["primary_failure"] or "reference_feed_empty"
    return evaluate_phase42b_report(report)


def write_phase42b_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42b_report(report)
    _write_json(root_path / PHASE42B_REPORT_JSON, report)
    _write_text(root_path / PHASE42B_REPORT_MD, render_phase42b_markdown(report))
    _write_json(root_path / PHASE42B_REFERENCE_SUMMARY, report.get("reference_feed_quality", {}))
    _write_json(root_path / PHASE42B_ALIGNMENT_CHECK, report.get("alignment_quality", {}))
    _write_json(root_path / PHASE42B_LEAKAGE_CHECK, report.get("leakage_check", {}))
    _write_text(root_path / PHASE42B_PYTEST_OUTPUT, pytest_output)
    _ensure_jsonl_exists(root_path / PHASE42B_INVALID_QUOTES)
    _ensure_jsonl_exists(root_path / PHASE42B_INVALID_100MS)
    classification = None if report.get("definition_of_done_status") == "pass" else classify_phase42b_failure(report)
    self_check = {
        "phase": "4.2B",
        "passed": report.get("definition_of_done_status") == "pass",
        "status": report.get("definition_of_done_status"),
        "definition_of_done_status": report.get("definition_of_done_status"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42B_REPORT_JSON),
        "report_md_path": _display_path(PHASE42B_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42B_PYTEST_OUTPUT),
        "bundle_path": _display_path(PHASE42B_BUNDLE),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42B_SELF_CHECK_JSON, self_check)
    if report.get("definition_of_done_status") != "pass":
        write_phase42b_failure_investigation(root=root_path, report=report, classification=classification)


def render_phase42b_markdown(report: dict[str, Any]) -> str:
    horizon = report.get("horizon_100ms", {})
    ref = report.get("reference_feed_quality", {})
    lines = [
        "# Phase 4.2B BookTicker Reference Report",
        "",
        f"Status: **{report.get('definition_of_done_status')}**",
        "",
        "## Status Separation",
        "",
        f"- Implementation: `{report.get('implementation_status')}`",
        f"- Runtime: `{report.get('runtime_status')}`",
        f"- Reference feed: `{report.get('reference_feed_status')}`",
        f"- Dataset coverage: `{report.get('dataset_coverage_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        "",
        "## Reference Feed",
        "",
        f"- Reference quote count: `{ref.get('reference_quote_count')}`",
        f"- Valid reference quote count: `{ref.get('valid_reference_quote_count')}`",
        f"- Reference gap p95/p99 ms: `{ref.get('reference_gap_p95_ms')}` / `{ref.get('reference_gap_p99_ms')}`",
        "",
        "## 100ms Coverage",
        "",
        f"- Reference source: `{horizon.get('reference_source')}`",
        f"- Max future gap ms: `{horizon.get('max_future_gap_ms')}`",
        f"- Eligible rows: `{horizon.get('eligible_count')}`",
        f"- Valid rows: `{horizon.get('valid_count')}`",
        f"- Eligible valid rate: `{horizon.get('valid_rate_eligible_rows')}`",
        f"- Invalid reasons: `{json.dumps(horizon.get('invalid_reason_counts', {}), sort_keys=True)}`",
        "",
        "## Hard Fail Reasons",
        "",
    ]
    reasons = report.get("hard_fail_reasons", [])
    lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- None")
    lines.extend(["", "## Bottleneck Assessment", "", str(report.get("bottleneck_assessment")), ""])
    return "\n".join(lines)


def create_phase42b_bundle(
    *,
    root: str | Path,
    source_root: str | Path = REPO_ROOT,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    source_path = Path(source_root)
    target = Path(bundle_path) if bundle_path is not None else root_path / PHASE42B_BUNDLE
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for directory_name in ("app", "tests", "scripts"):
            archive.writestr(f"{directory_name}/", "")
        _write_directory_to_archive(archive, source_path / "bot/app", "app")
        _write_directory_to_archive(archive, source_path / "tests", "tests")
        _write_directory_to_archive(archive, source_path / "scripts", "scripts")
        for relative in PHASE42B_REQUIRED_BUNDLE_FILES:
            if relative.endswith("/"):
                continue
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / PHASE42B_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42B_INVESTIGATION))
    missing = phase42b_bundle_missing_files(target)
    if missing:
        raise RuntimeError(f"Phase 4.2B bundle missing required files: {missing}")
    return target


def phase42b_bundle_missing_files(bundle_path: str | Path) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    return [name for name in PHASE42B_REQUIRED_BUNDLE_FILES if name not in names]


def write_phase42b_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2B Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Definition of Done status: `{report.get('definition_of_done_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Report path: `{_display_path(PHASE42B_REPORT_JSON)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Bottleneck Assessment",
        "",
        str(report.get("bottleneck_assessment")),
        "",
        "## Recommendation",
        "",
        _recommendation(report),
        "",
        "## Fix Applied",
        "",
        "No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.",
        "",
    ]
    _write_text(Path(root) / PHASE42B_INVESTIGATION, "\n".join(lines))


def _bookticker_label(
    *,
    row: dict[str, Any],
    valid_references: list[dict[str, Any]],
    valid_reference_timestamps: list[int],
    first_after_feature: int,
) -> dict[str, Any]:
    feature_ts = int(row["local_recv_monotonic_ns"])
    target_ts = feature_ts + 100 * NS_PER_MS
    base = {
        "reference_source": "bookTicker",
        "horizon_ms": 100,
        "target_local_recv_monotonic_ns": target_ts,
        "max_future_gap_ms": MAX_FUTURE_GAP_MS["horizon_100ms"],
        "first_reference_index_after_feature": first_after_feature,
        "future_reference_index": None,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_update_id": None,
        "future_bid": None,
        "future_ask": None,
        "future_mid_price": None,
        "future_gap_ms": None,
        "future_index": None,
        "future_local_recv_monotonic_ns": None,
        "future_last_update_id": None,
        "return_bps": None,
        "direction": None,
        "spread_adjusted_direction": None,
        "valid": False,
        "invalid_reason": None,
    }
    current_mid = row.get("mid_price")
    spread_bps = row.get("spread_bps")
    best_bid = row.get("best_bid")
    best_ask = row.get("best_ask")
    if (
        not isinstance(best_bid, (int, float))
        or not isinstance(best_ask, (int, float))
        or not math.isfinite(float(best_bid))
        or not math.isfinite(float(best_ask))
        or best_bid <= 0
        or best_ask <= 0
        or best_bid >= best_ask
    ):
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    if not isinstance(current_mid, (int, float)) or not math.isfinite(float(current_mid)) or current_mid <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    ref_index = select_future_reference_index(
        valid_references,
        feature_timestamp_ns=feature_ts,
        horizon_ms=100,
        reference_timestamps_ns=valid_reference_timestamps,
    )
    if ref_index is None:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    ref = valid_references[ref_index]
    ref_ts = int(ref["local_recv_monotonic_ns"])
    gap_ms = (ref_ts - target_ts) / NS_PER_MS
    future_mid = ref.get("mid_price")
    base.update(
        {
            "future_reference_index": ref_index,
            "future_reference_local_recv_monotonic_ns": ref_ts,
            "future_reference_update_id": ref.get("update_id"),
            "future_bid": ref.get("best_bid"),
            "future_ask": ref.get("best_ask"),
            "future_mid_price": future_mid,
            "future_gap_ms": gap_ms,
            "future_index": ref_index,
            "future_local_recv_monotonic_ns": ref_ts,
            "future_last_update_id": ref.get("update_id"),
        }
    )
    if gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    if not isinstance(future_mid, (int, float)) or not math.isfinite(float(future_mid)) or future_mid <= 0:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_MID_INVALID"}
    if not isinstance(spread_bps, (int, float)) or not math.isfinite(float(spread_bps)) or spread_bps < 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    try:
        ret = compute_return_bps(float(current_mid), float(future_mid))
        direction = direction_label(ret)
        spread_direction = spread_adjusted_direction_label(ret, spread_bps=float(spread_bps))
    except (TypeError, ValueError):
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    return {
        **base,
        "return_bps": ret,
        "direction": direction,
        "spread_adjusted_direction": spread_direction,
        "valid": True,
        "invalid_reason": None,
    }


def _horizon_100ms_stats(
    clean_samples: list[dict[str, Any]],
    reference_quotes: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    *,
    invalid_cases_path: str | Path | None,
) -> dict[str, Any]:
    reference_timestamps = [
        int(row["local_recv_monotonic_ns"])
        for row in reference_quotes
        if isinstance(row.get("local_recv_monotonic_ns"), int)
    ]
    last_reference_ts = max(reference_timestamps) if reference_timestamps else None
    eligible_count = 0
    if last_reference_ts is not None:
        eligible_count = sum(
            1
            for sample in clean_samples
            if int(sample["local_recv_monotonic_ns"]) + 100 * NS_PER_MS <= last_reference_ts
        )
    valid_count = 0
    invalid_count = 0
    tail_no_future_count = 0
    reason_counts: Counter[str] = Counter()
    future_gaps: list[float] = []
    cases: list[dict[str, Any]] = []
    for row in labeled_rows:
        label = row.get("labels", {}).get("horizon_100ms")
        if not isinstance(label, dict):
            invalid_count += 1
            reason_counts["MISSING_LABEL"] += 1
            continue
        if isinstance(label.get("future_gap_ms"), (int, float)):
            future_gaps.append(float(label["future_gap_ms"]))
        if label.get("valid") is True:
            valid_count += 1
            continue
        invalid_count += 1
        reason = str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        reason_counts[reason] += 1
        target = label.get("target_local_recv_monotonic_ns")
        if reason == "NO_FUTURE_REFERENCE" and isinstance(target, int) and last_reference_ts is not None and target > last_reference_ts:
            tail_no_future_count += 1
        cases.append(
            {
                "symbol": row.get("symbol"),
                "generation_id": row.get("generation_id"),
                "last_update_id": row.get("last_update_id"),
                "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                "horizon": "horizon_100ms",
                "invalid_reason": reason,
                "target_local_recv_monotonic_ns": target,
                "future_reference_local_recv_monotonic_ns": label.get("future_reference_local_recv_monotonic_ns"),
                "future_gap_ms": label.get("future_gap_ms"),
            }
        )
    if invalid_cases_path is not None:
        write_jsonl(invalid_cases_path, cases)
    total = len(labeled_rows)
    return {
        "reference_source": "bookTicker",
        "max_future_gap_ms": MAX_FUTURE_GAP_MS["horizon_100ms"],
        "eligible_count": eligible_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "tail_no_future_count": tail_no_future_count,
        "valid_rate_all_rows": valid_count / total if total else 0.0,
        "valid_rate_eligible_rows": valid_count / eligible_count if eligible_count else 0.0,
        "invalid_reason_counts": dict(sorted(reason_counts.items())),
        "future_gap_ms_p50": _percentile(future_gaps, 0.50),
        "future_gap_ms_p90": _percentile(future_gaps, 0.90),
        "future_gap_ms_p95": _percentile(future_gaps, 0.95),
        "future_gap_ms_p99": _percentile(future_gaps, 0.99),
        "future_gap_ms_max": max(future_gaps) if future_gaps else None,
    }


def _alignment_quality(labeled_rows: list[dict[str, Any]]) -> dict[str, Any]:
    no_future = 0
    gap_too_large = 0
    for row in labeled_rows:
        label = row.get("labels", {}).get("horizon_100ms", {})
        if label.get("invalid_reason") == "NO_FUTURE_REFERENCE":
            no_future += 1
        if label.get("invalid_reason") == "FUTURE_REFERENCE_GAP_TOO_LARGE":
            gap_too_large += 1
    return {
        "feature_sample_count": len(labeled_rows),
        "labeled_sample_count": len(labeled_rows),
        "feature_to_reference_no_future_count": no_future,
        "feature_to_reference_gap_too_large_count": gap_too_large,
    }


def _reference_schema_errors(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(REQUIRED_REFERENCE_FIELDS):
        if field not in row or row.get(field) is None and field not in {"exchange_event_ts"}:
            errors.append(f"MISSING_{field.upper()}")
    for field in ("best_bid", "best_ask", "best_bid_qty", "best_ask_qty", "mid_price", "spread", "spread_bps"):
        value = row.get(field)
        if value is None or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            errors.append(f"INVALID_{field.upper()}")
    if isinstance(row.get("best_bid"), (int, float)) and isinstance(row.get("best_ask"), (int, float)):
        if row["best_bid"] >= row["best_ask"]:
            errors.append("CROSSED_QUOTE")
    if not isinstance(row.get("local_recv_monotonic_ns"), int):
        errors.append("MISSING_LOCAL_RECV_MONOTONIC_NS")
    if not row.get("local_recv_wall_ts"):
        errors.append("MISSING_LOCAL_RECV_WALL_TS")
    if row.get("update_id") is None:
        errors.append("MISSING_UPDATE_ID")
    return sorted(set(errors))


def _optional_float(value: Any, missing_reason: str, invalid_reason: str, errors: list[str]) -> float | None:
    if value is None:
        errors.append(missing_reason)
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        errors.append(invalid_reason)
        return None
    if not math.isfinite(result):
        errors.append(invalid_reason)
        return None
    return result


def _normalize_depth_runtime_quality(quality: dict[str, Any]) -> dict[str, Any]:
    return {field: int(_num(quality.get(field))) for field in DEPTH_RUNTIME_ZERO_FIELDS}


def _bottleneck_assessment(reference_quality: dict[str, Any], horizon: dict[str, Any]) -> str:
    if _num(horizon.get("valid_rate_eligible_rows")) >= REQUIRED_100MS_VALID_RATE:
        return "bookTicker reference feed satisfies the strict 100ms label coverage gate."
    if _num(reference_quality.get("reference_gap_p95_ms")) > REFERENCE_GAP_P95_MAX_MS or _num(reference_quality.get("reference_gap_p99_ms")) > REFERENCE_GAP_P99_MAX_MS:
        return "BookTicker reference feed cadence/network session gaps appear insufficient for strict 100ms labels."
    return "BookTicker alignment still fails 100ms coverage; inspect local capture timing and implementation alignment."


def _recommendation(report: dict[str, Any]) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "Proceed only after the pass bundle is reviewed."
    return "Keep 100ms as a hard gate; inspect bookTicker cadence, local capture timing, and network/session conditions before rerunning Phase 4.2B."


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "Phase 4.2B Definition of Done passed; pass bundle may be created."
    return f"Phase 4.2B failed with classification {classification}. No pass bundle was created."


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else candidate


def _num(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


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


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


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
