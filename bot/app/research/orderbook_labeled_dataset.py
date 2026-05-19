from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass
import json
import math
from pathlib import Path
import shutil
import time
from typing import Any
import zipfile


NS_PER_MS = 1_000_000
HORIZONS: dict[str, int] = {
    "horizon_100ms": 100,
    "horizon_250ms": 250,
    "horizon_500ms": 500,
    "horizon_1000ms": 1000,
    "horizon_2000ms": 2000,
    "horizon_5000ms": 5000,
}
MAX_FUTURE_GAP_MS: dict[str, int] = {
    "horizon_100ms": 100,
    "horizon_250ms": 150,
    "horizon_500ms": 250,
    "horizon_1000ms": 500,
    "horizon_2000ms": 1000,
    "horizon_5000ms": 2000,
}
MIN_VALID_RATE_ELIGIBLE_ROWS: dict[str, float] = {
    "horizon_100ms": 0.95,
    "horizon_250ms": 0.95,
    "horizon_500ms": 0.95,
    "horizon_1000ms": 0.95,
    "horizon_2000ms": 0.90,
    "horizon_5000ms": 0.90,
}
DEPTH_LEVELS = (1, 3, 5, 10, 20)
EPSILON = 1e-12
LARGE_GAP_THRESHOLD_MS = 1_000.0
DEFAULT_ROOT = Path(__file__).resolve().parents[3]

PAST_FEATURE_POLICY_MS: dict[str, int] = {
    "past_mid_return_100ms_bps": 100,
    "past_mid_return_500ms_bps": 500,
    "past_mid_return_1000ms_bps": 1000,
    "past_spread_change_500ms_bps": 500,
}
PAST_MAX_GAP_MS: dict[str, int] = {
    "past_mid_return_100ms_bps": 100,
    "past_mid_return_500ms_bps": 500,
    "past_mid_return_1000ms_bps": 1000,
    "past_spread_change_500ms_bps": 500,
}

REQUIRED_FEATURE_FIELDS = frozenset(
    {
        "mid_price",
        "spread",
        "spread_bps",
        "best_bid",
        "best_ask",
        "bid_size_l1",
        "ask_size_l1",
        "l1_size_imbalance",
        "microprice_l1",
        "microprice_edge_bps",
        "bid_slope_l5",
        "ask_slope_l5",
        "past_mid_return_100ms_bps",
        "past_mid_return_500ms_bps",
        "past_mid_return_1000ms_bps",
        "past_spread_change_500ms_bps",
    }
    | {
        f"{prefix}_l{level}"
        for level in DEPTH_LEVELS
        for prefix in (
            "bid_depth",
            "ask_depth",
            "total_depth",
            "depth_imbalance",
            "depth_ratio",
        )
    }
)
REQUIRED_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "symbol",
        "source",
        "generation_id",
        "state_version",
        "snapshot_version",
        "last_update_id",
        "local_recv_monotonic_ns",
        "local_recv_wall_ts",
        "best_bid",
        "best_ask",
        "mid_price",
        "spread",
        "spread_bps",
        "features",
        "labels",
        "quality",
    }
)
REQUIRED_VALID_LABEL_FIELDS = frozenset(
    {
        "future_local_recv_monotonic_ns",
        "future_last_update_id",
        "future_mid_price",
        "future_gap_ms",
        "return_bps",
        "direction",
        "spread_adjusted_direction",
        "valid",
        "invalid_reason",
    }
)
REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "status",
        "symbol",
        "source",
        "input_path",
        "output_path",
        "input_sample_count",
        "labeled_sample_count",
        "duration_sec",
        "sample_rate_per_sec",
        "hard_fail_reasons",
        "warning_reasons",
        "timestamp_quality",
        "input_schema_quality",
        "labeled_schema_quality",
        "feature_quality",
        "label_quality",
        "leakage_check",
    }
)
REQUIRED_BUNDLE_FILES = (
    "app/",
    "tests/",
    "scripts/",
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/orderbook_labeled_samples.jsonl",
    "data/reports/phase_4_2_dataset_quality_report.json",
    "data/reports/phase_4_2_dataset_quality_report.md",
    "data/reports/phase42_self_check.json",
    "data/debug/phase_4_2_label_generation_summary.json",
    "data/debug/phase_4_2_label_invalid_cases.jsonl",
    "data/debug/phase_4_2_leakage_check.json",
    "data/debug/phase_4_2_dataset_schema_violations.jsonl",
    "data/debug/phase_4_2_pytest_output.txt",
)


@dataclass(frozen=True)
class CleanSampleValidationResult:
    valid: bool
    samples: list[dict[str, Any]]
    invalid_clean_sample_count: int
    violations: list[dict[str, Any]]
    failure_classification: str | None = None


@dataclass(frozen=True)
class LabeledSchemaResult:
    valid: bool
    labeled_schema_violation_count: int
    violations: list[dict[str, Any]]


@dataclass(frozen=True)
class Phase42PipelineResult:
    report: dict[str, Any]
    labeled_rows: list[dict[str, Any]]
    input_validation: CleanSampleValidationResult
    labeled_schema_result: LabeledSchemaResult
    leakage_result: dict[str, Any]


def validate_clean_samples(
    path: str | Path,
    *,
    violation_output_path: str | Path | None = None,
) -> CleanSampleValidationResult:
    input_path = Path(path)
    violations: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    nonblank_lines = 0

    if not input_path.exists():
        violations.append(
            {
                "line": None,
                "sample_index": None,
                "reason": f"input_file_missing:{input_path}",
                "classification": "INPUT_FILE_MISSING",
            }
        )
        _write_jsonl(violation_output_path, violations)
        return CleanSampleValidationResult(
            valid=False,
            samples=[],
            invalid_clean_sample_count=1,
            violations=violations,
            failure_classification="INPUT_FILE_MISSING",
        )

    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            nonblank_lines += 1
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                violations.append(
                    {
                        "line": line_number,
                        "sample_index": len(samples),
                        "reason": f"invalid_json:{exc}",
                        "classification": "INPUT_SCHEMA_FAILURE",
                    }
                )
                continue
            if not isinstance(row, dict):
                violations.append(
                    {
                        "line": line_number,
                        "sample_index": len(samples),
                        "reason": "row_not_object",
                        "classification": "INPUT_SCHEMA_FAILURE",
                    }
                )
                continue
            row_errors = _clean_sample_errors(row)
            if row_errors:
                for reason in row_errors:
                    violations.append(
                        {
                            "line": line_number,
                            "sample_index": len(samples),
                            "symbol": row.get("symbol"),
                            "generation_id": row.get("generation_id"),
                            "last_update_id": row.get("last_update_id"),
                            "local_recv_monotonic_ns": row.get(
                                "local_recv_monotonic_ns"
                            ),
                            "reason": reason,
                            "classification": "INPUT_SCHEMA_FAILURE",
                        }
                    )
            samples.append(row)

    if nonblank_lines == 0:
        violations.append(
            {
                "line": None,
                "sample_index": None,
                "reason": "input_empty",
                "classification": "INPUT_EMPTY",
            }
        )
        _write_jsonl(violation_output_path, violations)
        return CleanSampleValidationResult(
            valid=False,
            samples=[],
            invalid_clean_sample_count=0,
            violations=violations,
            failure_classification="INPUT_EMPTY",
        )

    invalid_count = len({violation.get("line") for violation in violations})
    _write_jsonl(violation_output_path, violations)
    return CleanSampleValidationResult(
        valid=not violations,
        samples=samples,
        invalid_clean_sample_count=invalid_count,
        violations=violations,
        failure_classification=("INPUT_SCHEMA_FAILURE" if violations else None),
    )


def compute_timestamp_quality(samples: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [
        int(sample["local_recv_monotonic_ns"])
        for sample in samples
        if isinstance(sample.get("local_recv_monotonic_ns"), int)
    ]
    gaps_ms = [
        (timestamps[index] - timestamps[index - 1]) / NS_PER_MS
        for index in range(1, len(timestamps))
    ]
    monotonic_violations = sum(1 for gap in gaps_ms if gap < 0)
    duplicate_count = sum(1 for gap in gaps_ms if gap == 0)
    positive_gaps = [gap for gap in gaps_ms if gap >= 0]
    large_gaps = [gap for gap in positive_gaps if gap > LARGE_GAP_THRESHOLD_MS]
    return {
        "timestamp_monotonic_violations": monotonic_violations,
        "duplicate_timestamp_count": duplicate_count,
        "large_gap_count": len(large_gaps),
        "large_gap_threshold_ms": LARGE_GAP_THRESHOLD_MS,
        "max_gap_ms": max(positive_gaps) if positive_gaps else None,
        "p50_gap_ms": _percentile(positive_gaps, 0.50),
        "p95_gap_ms": _percentile(positive_gaps, 0.95),
        "p99_gap_ms": _percentile(positive_gaps, 0.99),
    }


def select_future_index(
    timestamps_ns: list[int],
    current_index: int,
    horizon_ms: int,
) -> int | None:
    target_time = timestamps_ns[current_index] + horizon_ms * NS_PER_MS
    index = bisect_left(timestamps_ns, target_time, lo=current_index + 1)
    return index if index < len(timestamps_ns) else None


def compute_return_bps(current_mid_price: float, future_mid_price: float) -> float:
    if (
        not math.isfinite(current_mid_price)
        or not math.isfinite(future_mid_price)
        or current_mid_price <= 0
    ):
        raise ValueError("current and future mid prices must be finite and current mid > 0")
    return ((future_mid_price - current_mid_price) / current_mid_price) * 10_000.0


def direction_label(return_bps: float, *, flat_threshold_bps: float = 0.0) -> int:
    if return_bps > flat_threshold_bps:
        return 1
    if return_bps < -flat_threshold_bps:
        return -1
    return 0


def spread_adjusted_direction_label(return_bps: float, *, spread_bps: float) -> int:
    if spread_bps < 0:
        raise ValueError("spread_bps must be non-negative")
    if return_bps > spread_bps:
        return 1
    if return_bps < -spread_bps:
        return -1
    return 0


def extract_current_features(
    sample: dict[str, Any],
    *,
    sample_index: int,
) -> tuple[dict[str, Any], list[str], dict[str, int]]:
    features: dict[str, Any] = {}
    warnings: list[str] = []
    source_indices: dict[str, int] = {}

    bids = _parse_levels(sample.get("bids"))
    asks = _parse_levels(sample.get("asks"))
    best_bid = _as_float(sample.get("best_bid"))
    best_ask = _as_float(sample.get("best_ask"))
    mid_price = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    spread_bps = (spread / mid_price) * 10_000.0 if mid_price > 0 else None

    _set_feature(features, source_indices, "best_bid", best_bid, sample_index)
    _set_feature(features, source_indices, "best_ask", best_ask, sample_index)
    _set_feature(features, source_indices, "mid_price", mid_price, sample_index)
    _set_feature(features, source_indices, "spread", spread, sample_index)
    _set_feature(features, source_indices, "spread_bps", spread_bps, sample_index)

    bid_size_l1 = bids[0][1] if bids else None
    ask_size_l1 = asks[0][1] if asks else None
    _set_feature(features, source_indices, "bid_size_l1", bid_size_l1, sample_index)
    _set_feature(features, source_indices, "ask_size_l1", ask_size_l1, sample_index)
    if bid_size_l1 is None or ask_size_l1 is None or (bid_size_l1 + ask_size_l1) == 0:
        l1_imbalance = None
        warnings.append("l1_size_imbalance_denominator_zero")
    else:
        l1_imbalance = (bid_size_l1 - ask_size_l1) / (bid_size_l1 + ask_size_l1)
    _set_feature(
        features,
        source_indices,
        "l1_size_imbalance",
        l1_imbalance,
        sample_index,
    )

    for level in DEPTH_LEVELS:
        bid_depth = sum(size for _, size in bids[:level])
        ask_depth = sum(size for _, size in asks[:level])
        total_depth = bid_depth + ask_depth
        _set_feature(features, source_indices, f"bid_depth_l{level}", bid_depth, sample_index)
        _set_feature(features, source_indices, f"ask_depth_l{level}", ask_depth, sample_index)
        _set_feature(
            features,
            source_indices,
            f"total_depth_l{level}",
            total_depth,
            sample_index,
        )
        if total_depth == 0:
            imbalance = None
            warnings.append(f"depth_imbalance_l{level}_denominator_zero")
        else:
            imbalance = (bid_depth - ask_depth) / total_depth
        if ask_depth == 0:
            ratio = None
            warnings.append(f"depth_ratio_l{level}_denominator_zero")
        else:
            ratio = bid_depth / ask_depth
        _set_feature(
            features,
            source_indices,
            f"depth_imbalance_l{level}",
            imbalance,
            sample_index,
        )
        _set_feature(
            features,
            source_indices,
            f"depth_ratio_l{level}",
            ratio,
            sample_index,
        )

    if bid_size_l1 is None or ask_size_l1 is None or (bid_size_l1 + ask_size_l1) == 0:
        microprice = None
        microprice_edge_bps = None
        warnings.append("microprice_l1_denominator_zero")
    else:
        microprice = (
            best_ask * bid_size_l1 + best_bid * ask_size_l1
        ) / (bid_size_l1 + ask_size_l1)
        microprice_edge_bps = ((microprice - mid_price) / mid_price) * 10_000.0
    _set_feature(features, source_indices, "microprice_l1", microprice, sample_index)
    _set_feature(
        features,
        source_indices,
        "microprice_edge_bps",
        microprice_edge_bps,
        sample_index,
    )

    if len(bids) < 5:
        bid_slope = None
        warnings.append("bid_slope_l5_insufficient_levels")
    else:
        bid_slope = abs(best_bid - bids[4][0]) / max(features["bid_depth_l5"], EPSILON)
    if len(asks) < 5:
        ask_slope = None
        warnings.append("ask_slope_l5_insufficient_levels")
    else:
        ask_slope = abs(asks[4][0] - best_ask) / max(features["ask_depth_l5"], EPSILON)
    _set_feature(features, source_indices, "bid_slope_l5", bid_slope, sample_index)
    _set_feature(features, source_indices, "ask_slope_l5", ask_slope, sample_index)

    for feature_name in PAST_FEATURE_POLICY_MS:
        _set_feature(features, source_indices, feature_name, None, sample_index)

    return features, sorted(set(warnings)), source_indices


def generate_labeled_rows(
    samples: list[dict[str, Any]],
    *,
    flat_threshold_bps: float = 0.0,
) -> list[dict[str, Any]]:
    timestamps = [int(sample["local_recv_monotonic_ns"]) for sample in samples]
    base_features: list[dict[str, Any]] = []
    base_warnings: list[list[str]] = []
    source_indices: list[dict[str, int | None]] = []
    for index, sample in enumerate(samples):
        features, warnings, feature_sources = extract_current_features(
            sample,
            sample_index=index,
        )
        base_features.append(features)
        base_warnings.append(warnings)
        source_indices.append(dict(feature_sources))

    rows: list[dict[str, Any]] = []
    for index, sample in enumerate(samples):
        features = dict(base_features[index])
        warnings = list(base_warnings[index])
        feature_sources = dict(source_indices[index])
        _add_past_features(
            samples=samples,
            timestamps=timestamps,
            all_features=base_features,
            current_index=index,
            features=features,
            warnings=warnings,
            source_indices=feature_sources,
        )
        labels = {
            horizon: _build_label(
                samples=samples,
                timestamps=timestamps,
                current_index=index,
                horizon=horizon,
                current_features=features,
                flat_threshold_bps=flat_threshold_bps,
            )
            for horizon in HORIZONS
        }
        row = {
            "schema_version": "orderbook_labeled_v1",
            "symbol": sample.get("symbol"),
            "source": sample.get("source"),
            "generation_id": sample.get("generation_id"),
            "state_version": sample.get("state_version"),
            "snapshot_version": sample.get("snapshot_version"),
            "last_update_id": sample.get("last_update_id"),
            "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
            "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
            "exchange_event_ts": sample.get("exchange_event_ts"),
            "best_bid": features["best_bid"],
            "best_ask": features["best_ask"],
            "mid_price": features["mid_price"],
            "spread": features["spread"],
            "spread_bps": features["spread_bps"],
            "features": features,
            "labels": labels,
            "quality": {
                "input_clean_sample_valid": True,
                "feature_warnings": sorted(set(warnings)),
                "label_warnings": _label_warnings(labels),
                "leakage_checked": True,
                "future_label_policy": "first_sample_at_or_after_target_time",
                "past_feature_policy": "latest_sample_at_or_before_target_time",
                "max_future_gap_policy_ms": MAX_FUTURE_GAP_MS,
                "current_index": index,
                "feature_source_indices": feature_sources,
            },
        }
        rows.append(row)
    return rows


def validate_labeled_rows(
    rows: list[dict[str, Any]],
    *,
    violation_output_path: str | Path | None = None,
) -> LabeledSchemaResult:
    violations: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        missing_top = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(row))
        for field in missing_top:
            violations.append(_labeled_violation(index, row, f"missing_top_level_{field}"))
        if row.get("schema_version") != "orderbook_labeled_v1":
            violations.append(_labeled_violation(index, row, "invalid_schema_version"))
        features = row.get("features")
        if not isinstance(features, dict):
            violations.append(_labeled_violation(index, row, "features_not_object"))
            features = {}
        for field in sorted(REQUIRED_FEATURE_FIELDS - set(features)):
            violations.append(_labeled_violation(index, row, f"missing_feature_{field}"))
        labels = row.get("labels")
        if not isinstance(labels, dict):
            violations.append(_labeled_violation(index, row, "labels_not_object"))
            labels = {}
        for horizon in HORIZONS:
            label = labels.get(horizon)
            if not isinstance(label, dict):
                violations.append(_labeled_violation(index, row, f"missing_label_{horizon}"))
                continue
            for field in sorted(REQUIRED_VALID_LABEL_FIELDS - set(label)):
                violations.append(
                    _labeled_violation(index, row, f"missing_label_field_{horizon}_{field}")
                )
            if label.get("valid") is True:
                for field in (
                    "future_local_recv_monotonic_ns",
                    "future_last_update_id",
                    "future_mid_price",
                    "future_gap_ms",
                    "return_bps",
                    "direction",
                    "spread_adjusted_direction",
                ):
                    if label.get(field) is None:
                        violations.append(
                            _labeled_violation(
                                index,
                                row,
                                f"valid_label_missing_{horizon}_{field}",
                            )
                        )
                if label.get("invalid_reason") is not None:
                    violations.append(
                        _labeled_violation(index, row, f"valid_label_has_invalid_reason_{horizon}")
                    )
            else:
                if not label.get("invalid_reason"):
                    violations.append(
                        _labeled_violation(
                            index,
                            row,
                            f"invalid_label_missing_reason_{horizon}",
                        )
                    )
    _write_jsonl(violation_output_path, violations)
    return LabeledSchemaResult(
        valid=not violations,
        labeled_schema_violation_count=len(violations),
        violations=violations,
    )


def run_leakage_check(
    labeled_rows: list[dict[str, Any]],
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    for index, row in enumerate(labeled_rows):
        current_timestamp = row.get("local_recv_monotonic_ns")
        if not isinstance(current_timestamp, int):
            continue
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
        row_labels = row.get("labels")
        labels: dict[str, Any] = row_labels if isinstance(row_labels, dict) else {}
        for horizon, horizon_ms in HORIZONS.items():
            label = labels.get(horizon)
            if not isinstance(label, dict):
                continue
            future_index = label.get("future_index")
            future_timestamp = label.get("future_local_recv_monotonic_ns")
            target_timestamp = current_timestamp + horizon_ms * NS_PER_MS
            reason = None
            if isinstance(future_index, int) and future_index <= index:
                reason = "future_index_not_greater_than_current_index"
            elif isinstance(future_timestamp, int) and future_timestamp <= current_timestamp:
                reason = "future_timestamp_not_after_current_timestamp"
            elif isinstance(future_timestamp, int) and future_timestamp < target_timestamp:
                reason = "future_timestamp_before_target_time"
            elif label.get("valid") is True:
                if not isinstance(future_index, int):
                    reason = "valid_label_missing_future_index"
                elif not isinstance(future_timestamp, int):
                    reason = "valid_label_missing_future_timestamp"
            if reason is not None:
                violations.append(
                    {
                        "type": "label",
                        "sample_index": index,
                        "horizon": horizon,
                        "future_index": future_index,
                        "future_local_recv_monotonic_ns": future_timestamp,
                        "target_local_recv_monotonic_ns": target_timestamp,
                        "reason": reason,
                    }
                )
    feature_count = sum(1 for violation in violations if violation["type"] == "feature")
    label_count = sum(1 for violation in violations if violation["type"] == "label")
    result = {
        "passed": feature_count == 0 and label_count == 0,
        "feature_leakage_violations": feature_count,
        "label_leakage_violations": label_count,
        "checked_samples": len(labeled_rows),
        "checked_horizons": list(HORIZONS),
        "violations": violations,
    }
    _write_json(output_path, result)
    return result


def build_quality_report(
    *,
    input_path: str | Path,
    output_path: str | Path,
    input_samples: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    input_validation_invalid_count: int,
    input_schema_violations: list[dict[str, Any]],
    labeled_schema_result: LabeledSchemaResult,
    leakage_result: dict[str, Any],
    min_valid_rate_by_horizon: dict[str, float] | None = None,
) -> dict[str, Any]:
    min_rates = min_valid_rate_by_horizon or MIN_VALID_RATE_ELIGIBLE_ROWS
    timestamp_quality = compute_timestamp_quality(input_samples)
    duration_sec = _duration_sec(input_samples)
    label_quality = _label_quality(input_samples, labeled_rows)
    feature_quality = _feature_quality(labeled_rows, leakage_result)
    hard_fail_reasons = _hard_fail_reasons(
        input_samples=input_samples,
        labeled_rows=labeled_rows,
        input_validation_invalid_count=input_validation_invalid_count,
        timestamp_quality=timestamp_quality,
        labeled_schema_result=labeled_schema_result,
        leakage_result=leakage_result,
        label_quality=label_quality,
        min_rates=min_rates,
    )
    warning_reasons = _warning_reasons(
        duration_sec=duration_sec,
        timestamp_quality=timestamp_quality,
        feature_quality=feature_quality,
        label_quality=label_quality,
    )
    report = {
        "phase": "4.2",
        "status": "fail" if hard_fail_reasons else "pass",
        "symbol": _first_value(input_samples, "symbol"),
        "source": _first_value(input_samples, "source"),
        "input_path": _display_path(input_path),
        "output_path": _display_path(output_path),
        "input_sample_count": len(input_samples),
        "labeled_sample_count": len(labeled_rows),
        "duration_sec": duration_sec,
        "sample_rate_per_sec": (len(input_samples) / duration_sec if duration_sec > 0 else 0.0),
        "hard_fail_reasons": hard_fail_reasons,
        "warning_reasons": warning_reasons,
        "timestamp_quality": timestamp_quality,
        "input_schema_quality": {
            "invalid_clean_sample_count": input_validation_invalid_count,
            "schema_violation_count": len(input_schema_violations),
        },
        "labeled_schema_quality": {
            "labeled_schema_violation_count": labeled_schema_result.labeled_schema_violation_count
        },
        "feature_quality": feature_quality,
        "label_quality": label_quality,
        "leakage_check": {
            "passed": bool(leakage_result.get("passed")),
            "feature_leakage_violations": int(
                leakage_result.get("feature_leakage_violations", 0)
            ),
            "label_leakage_violations": int(
                leakage_result.get("label_leakage_violations", 0)
            ),
            "violations": leakage_result.get("violations", []),
        },
        "policies": {
            "future_label_policy": "first_sample_at_or_after_target_time",
            "past_feature_policy": "latest_sample_at_or_before_target_time",
            "max_future_gap_policy_ms": MAX_FUTURE_GAP_MS,
            "direction_flat_threshold_bps": 0.0,
            "spread_adjusted_direction": "return_bps must strictly exceed current spread_bps",
            "eligible_row_valid_rate_thresholds": min_rates,
        },
        "report_schema_valid": REQUIRED_REPORT_FIELDS <= set(REQUIRED_REPORT_FIELDS),
    }
    missing = sorted(REQUIRED_REPORT_FIELDS - set(report))
    if missing:
        report["hard_fail_reasons"].append(f"report JSON missing required fields: {missing}")
        report["status"] = "fail"
        report["report_schema_valid"] = False
    return report


def render_quality_markdown(report: dict[str, Any]) -> str:
    label_lines = []
    for horizon, stats in report.get("label_quality", {}).get("horizons", {}).items():
        label_lines.append(
            "| {horizon} | {valid_rate:.4f} | {valid} | {invalid} | {up} | {down} | {flat} |".format(
                horizon=horizon,
                valid_rate=stats.get("valid_rate_eligible_rows", 0.0),
                valid=stats.get("valid_count", 0),
                invalid=stats.get("invalid_count", 0),
                up=stats.get("up_count", 0),
                down=stats.get("down_count", 0),
                flat=stats.get("flat_count", 0),
            )
        )
    lines = [
        "# Phase 4.2 Dataset Quality Report",
        "",
        f"Status: **{report.get('status')}**",
        "",
        "## Paths",
        "",
        f"- Input: `{report.get('input_path')}`",
        f"- Output: `{report.get('output_path')}`",
        "",
        "## Sample Coverage",
        "",
        f"- Input samples: `{report.get('input_sample_count')}`",
        f"- Labeled samples: `{report.get('labeled_sample_count')}`",
        f"- Duration seconds: `{report.get('duration_sec')}`",
        f"- Sample rate per second: `{report.get('sample_rate_per_sec')}`",
        "",
        "## Timestamp Quality",
        "",
        "```json",
        json.dumps(report.get("timestamp_quality", {}), indent=2, sort_keys=True),
        "```",
        "",
        "## Feature Quality",
        "",
        "```json",
        json.dumps(report.get("feature_quality", {}), indent=2, sort_keys=True),
        "```",
        "",
        "## Label Valid Rates",
        "",
        "| Horizon | Eligible valid rate | Valid | Invalid | Up | Down | Flat |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        *label_lines,
        "",
        "## Invalid Label Reasons",
        "",
        "```json",
        json.dumps(
            {
                horizon: stats.get("invalid_reason_counts", {})
                for horizon, stats in report.get("label_quality", {})
                .get("horizons", {})
                .items()
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Leakage Check",
        "",
        "```json",
        json.dumps(report.get("leakage_check", {}), indent=2, sort_keys=True),
        "```",
        "",
        "## Warnings And Limitations",
        "",
    ]
    warnings = report.get("warning_reasons", [])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Readiness",
            "",
            (
                "Dataset is ready for Phase 5 research."
                if report.get("status") == "pass"
                else "Dataset needs more cleanup or collection before Phase 5."
            ),
            "",
        ]
    )
    if report.get("hard_fail_reasons"):
        lines.extend(
            [
                "## Hard Fail Reasons",
                "",
                *[f"- {reason}" for reason in report["hard_fail_reasons"]],
                "",
            ]
        )
    return "\n".join(lines)


def run_phase42_pipeline(
    *,
    input_path: str | Path,
    output_path: str | Path,
    report_json_path: str | Path,
    report_md_path: str | Path,
    debug_dir: str | Path,
    min_valid_rate_by_horizon: dict[str, float] | None = None,
) -> Phase42PipelineResult:
    started = time.perf_counter()
    debug_path = Path(debug_dir)
    debug_path.mkdir(parents=True, exist_ok=True)
    schema_violations_path = debug_path / "phase_4_2_dataset_schema_violations.jsonl"
    invalid_cases_path = debug_path / "phase_4_2_label_invalid_cases.jsonl"
    leakage_path = debug_path / "phase_4_2_leakage_check.json"
    summary_path = debug_path / "phase_4_2_label_generation_summary.json"

    validation = validate_clean_samples(
        input_path,
        violation_output_path=schema_violations_path,
    )
    timestamp_quality = compute_timestamp_quality(validation.samples)
    may_generate = (
        validation.valid
        and timestamp_quality["timestamp_monotonic_violations"] == 0
        and timestamp_quality["duplicate_timestamp_count"] == 0
    )

    labeled_rows: list[dict[str, Any]] = []
    labeled_schema = LabeledSchemaResult(valid=True, labeled_schema_violation_count=0, violations=[])
    leakage = _empty_leakage_result(checked_samples=0)
    if may_generate:
        labeled_rows = generate_labeled_rows(validation.samples)
        write_jsonl(output_path, labeled_rows)
        write_invalid_label_cases(labeled_rows, invalid_cases_path)
        labeled_schema = validate_labeled_rows(
            labeled_rows,
            violation_output_path=schema_violations_path,
        )
        leakage = run_leakage_check(labeled_rows, output_path=leakage_path)
    else:
        _write_jsonl(invalid_cases_path, [])
        run_leakage_check([], output_path=leakage_path)

    report = build_quality_report(
        input_path=input_path,
        output_path=output_path,
        input_samples=validation.samples,
        labeled_rows=labeled_rows,
        input_validation_invalid_count=validation.invalid_clean_sample_count,
        input_schema_violations=validation.violations,
        labeled_schema_result=labeled_schema,
        leakage_result=leakage,
        min_valid_rate_by_horizon=min_valid_rate_by_horizon,
    )
    report["processing_wall_time_sec"] = round(time.perf_counter() - started, 6)
    write_report(report, report_json_path, report_md_path)
    write_label_generation_summary(
        summary_path,
        input_path=input_path,
        output_path=output_path,
        report=report,
        labeled_rows=labeled_rows,
    )
    return Phase42PipelineResult(
        report=report,
        labeled_rows=labeled_rows,
        input_validation=validation,
        labeled_schema_result=labeled_schema,
        leakage_result=leakage,
    )


def write_report(
    report: dict[str, Any],
    report_json_path: str | Path,
    report_md_path: str | Path,
) -> None:
    _write_json(report_json_path, report)
    path = Path(report_md_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_quality_markdown(report), encoding="utf-8")


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def write_invalid_label_cases(
    labeled_rows: list[dict[str, Any]],
    path: str | Path,
) -> None:
    cases: list[dict[str, Any]] = []
    for row in labeled_rows:
        labels = row.get("labels", {})
        if not isinstance(labels, dict):
            continue
        for horizon, label in labels.items():
            if not isinstance(label, dict) or label.get("valid") is True:
                continue
            cases.append(
                {
                    "symbol": row.get("symbol"),
                    "generation_id": row.get("generation_id"),
                    "last_update_id": row.get("last_update_id"),
                    "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                    "horizon": horizon,
                    "invalid_reason": label.get("invalid_reason"),
                    "target_local_recv_monotonic_ns": label.get(
                        "target_local_recv_monotonic_ns"
                    ),
                    "future_local_recv_monotonic_ns": label.get(
                        "future_local_recv_monotonic_ns"
                    ),
                    "future_gap_ms": label.get("future_gap_ms"),
                }
            )
    _write_jsonl(path, cases)


def write_label_generation_summary(
    path: str | Path,
    *,
    input_path: str | Path,
    output_path: str | Path,
    report: dict[str, Any],
    labeled_rows: list[dict[str, Any]],
) -> None:
    invalid_reason_counts: dict[str, dict[str, int]] = {}
    for horizon, stats in report.get("label_quality", {}).get("horizons", {}).items():
        invalid_reason_counts[horizon] = stats.get("invalid_reason_counts", {})
    summary = {
        "phase": "4.2",
        "status": report.get("status"),
        "input_path": _display_path(input_path),
        "output_path": _display_path(output_path),
        "input_sample_count": report.get("input_sample_count"),
        "labeled_sample_count": len(labeled_rows),
        "horizons": list(HORIZONS),
        "max_future_gap_policy_ms": MAX_FUTURE_GAP_MS,
        "invalid_reason_counts": invalid_reason_counts,
        "label_quality": report.get("label_quality", {}),
    }
    _write_json(path, summary)


def create_phase42_bundle(
    *,
    root: str | Path = DEFAULT_ROOT,
    source_root: str | Path = DEFAULT_ROOT,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    source_path = Path(source_root)
    target = Path(bundle_path) if bundle_path is not None else root_path / "phase_4_2_dataset_quality_bundle.zip"
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for directory_name in ("app", "tests", "scripts"):
            archive.writestr(f"{directory_name}/", "")
        _write_directory_to_archive(archive, source_path / "bot/app", "app")
        _write_directory_to_archive(archive, source_path / "tests", "tests")
        _write_directory_to_archive(archive, source_path / "scripts", "scripts")
        for relative in REQUIRED_BUNDLE_FILES:
            if relative.endswith("/"):
                continue
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / "data/debug/phase42_failure_investigation.md"
        if investigation.exists():
            archive.write(investigation, "data/debug/phase42_failure_investigation.md")
    missing = bundle_missing_files(target)
    if missing:
        raise RuntimeError(f"bundle missing required files: {missing}")
    return target


def bundle_missing_files(bundle_path: str | Path) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    return [name for name in REQUIRED_BUNDLE_FILES if name not in names]


def classify_report_failure(report: dict[str, Any]) -> str:
    reasons = " ".join(str(reason) for reason in report.get("hard_fail_reasons", []))
    if "input clean sample file missing" in reasons or "input_file_missing" in reasons:
        return "INPUT_FILE_MISSING"
    if "input_sample_count = 0" in reasons:
        return "INPUT_EMPTY"
    if "invalid_clean_sample_count" in reasons:
        return "INPUT_SCHEMA_FAILURE"
    if "timestamp_monotonic_violations" in reasons:
        return "TIMESTAMP_MONOTONIC_FAILURE"
    if "duplicate_timestamp_count" in reasons:
        return "DUPLICATE_TIMESTAMP_FAILURE"
    if "feature_leakage_violations" in reasons:
        return "FEATURE_LEAKAGE_FAILURE"
    if "label_leakage_violations" in reasons:
        return "LABEL_LEAKAGE_FAILURE"
    if "labeled_schema_violation_count" in reasons:
        return "LABELED_SCHEMA_FAILURE"
    if "valid_rate_eligible_rows" in reasons:
        return "LABEL_VALID_RATE_FAILURE"
    if "report JSON" in reasons:
        return "REPORT_SCHEMA_FAILURE"
    if report.get("status") != "pass":
        return "UNKNOWN_PHASE42_FAILURE"
    return "UNKNOWN_PHASE42_FAILURE"


def write_failure_investigation(
    path: str | Path,
    *,
    classification: str,
    failed_item: str,
    report_path: str | Path,
    debug_paths: list[str | Path],
    hypothesis: str,
    fix_applied: str = "No automatic source edit was applied by the self-check script.",
    rerun_result: str = "Not rerun by this script invocation.",
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Phase 4.2 Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Failed Definition of Done item: `{failed_item}`",
        f"- Report path: `{_display_path(report_path)}`",
        "",
        "## Debug Artifacts",
        "",
        *[f"- `{_display_path(debug_path)}`" for debug_path in debug_paths],
        "",
        "## Hypothesis",
        "",
        hypothesis or "-",
        "",
        "## Fix Applied",
        "",
        fix_applied,
        "",
        "## Rerun Result",
        "",
        rerun_result,
        "",
    ]
    target.write_text("\n".join(lines), encoding="utf-8")


def _clean_sample_errors(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = (
        "schema_version",
        "symbol",
        "source",
        "generation_id",
        "local_recv_monotonic_ns",
        "local_recv_wall_ts",
        "last_update_id",
        "best_bid",
        "best_ask",
        "bids",
        "asks",
        "quality",
        "lifecycle",
    )
    for field in required:
        if row.get(field) is None:
            errors.append(f"missing_or_null_{field}")
    generation_id = row.get("generation_id")
    if generation_id is None or isinstance(generation_id, bool) or not isinstance(generation_id, int):
        errors.append("invalid_generation_id")
    monotonic_ns = row.get("local_recv_monotonic_ns")
    if (
        monotonic_ns is None
        or isinstance(monotonic_ns, bool)
        or not isinstance(monotonic_ns, int)
    ):
        errors.append("invalid_local_recv_monotonic_ns")
    if not row.get("local_recv_wall_ts"):
        errors.append("invalid_local_recv_wall_ts")
    if row.get("last_update_id") is None:
        errors.append("invalid_last_update_id")
    try:
        best_bid = _as_float(row.get("best_bid"))
        best_ask = _as_float(row.get("best_ask"))
        if best_bid <= 0 or best_ask <= 0:
            errors.append("non_positive_best_price")
        if best_bid >= best_ask:
            errors.append("best_bid_not_less_than_best_ask")
    except ValueError:
        errors.append("invalid_best_bid_or_ask")

    try:
        bids = _parse_levels(row.get("bids"))
        asks = _parse_levels(row.get("asks"))
        if not bids:
            errors.append("bids_empty")
        if not asks:
            errors.append("asks_empty")
        if any(bids[index][0] < bids[index + 1][0] for index in range(len(bids) - 1)):
            errors.append("bids_not_sorted_descending")
        if any(asks[index][0] > asks[index + 1][0] for index in range(len(asks) - 1)):
            errors.append("asks_not_sorted_ascending")
    except ValueError as exc:
        errors.append(f"invalid_book_levels:{exc}")

    quality = row.get("quality")
    if not isinstance(quality, dict):
        errors.append("quality_not_object")
    else:
        if quality.get("errors") != []:
            errors.append("quality.errors_present")
        if quality.get("is_valid") is False:
            errors.append("quality.is_valid_false")
    lifecycle = row.get("lifecycle")
    if not isinstance(lifecycle, dict):
        errors.append("lifecycle_not_object")
    else:
        for field in ("snapshot_ready", "ready_to_emit", "sequence_continuous"):
            if lifecycle.get(field) is not True:
                errors.append(f"lifecycle.{field}_not_true")
    return sorted(set(errors))


def _parse_levels(value: Any) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        raise ValueError("levels_not_list")
    levels: list[tuple[float, float]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("level_not_pair")
        price = _as_float(item[0])
        size = _as_float(item[1])
        if not math.isfinite(price) or not math.isfinite(size):
            raise ValueError("level_non_finite")
        if price <= 0 or size < 0:
            raise ValueError("level_negative")
        levels.append((price, size))
    return levels


def _as_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"not_numeric:{value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"non_finite:{value!r}")
    return result


def _set_feature(
    features: dict[str, Any],
    source_indices: dict[str, int],
    name: str,
    value: Any,
    source_index: int,
) -> None:
    features[name] = _json_number(value)
    source_indices[name] = source_index


def _json_number(value: Any) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    return value


def _add_past_features(
    *,
    samples: list[dict[str, Any]],
    timestamps: list[int],
    all_features: list[dict[str, Any]],
    current_index: int,
    features: dict[str, Any],
    warnings: list[str],
    source_indices: dict[str, int | None],
) -> None:
    del samples
    current_mid = features["mid_price"]
    current_spread_bps = features["spread_bps"]
    for feature_name, lookback_ms in PAST_FEATURE_POLICY_MS.items():
        target_time = timestamps[current_index] - lookback_ms * NS_PER_MS
        past_index = bisect_right(timestamps, target_time, hi=current_index + 1) - 1
        source_indices[feature_name] = None
        if past_index < 0:
            features[feature_name] = None
            warnings.append(f"{feature_name}_no_past_sample")
            continue
        gap_ms = (target_time - timestamps[past_index]) / NS_PER_MS
        if gap_ms > PAST_MAX_GAP_MS[feature_name]:
            features[feature_name] = None
            warnings.append(f"{feature_name}_gap_too_large")
            continue
        past_features = all_features[past_index]
        if feature_name.startswith("past_mid_return"):
            past_mid = past_features.get("mid_price")
            if not isinstance(past_mid, (int, float)) or past_mid <= 0:
                features[feature_name] = None
                warnings.append(f"{feature_name}_past_mid_invalid")
            else:
                features[feature_name] = compute_return_bps(float(past_mid), float(current_mid))
                source_indices[feature_name] = past_index
        else:
            past_spread_bps = past_features.get("spread_bps")
            if not isinstance(past_spread_bps, (int, float)):
                features[feature_name] = None
                warnings.append(f"{feature_name}_past_spread_invalid")
            else:
                features[feature_name] = float(current_spread_bps) - float(past_spread_bps)
                source_indices[feature_name] = past_index


def _build_label(
    *,
    samples: list[dict[str, Any]],
    timestamps: list[int],
    current_index: int,
    horizon: str,
    current_features: dict[str, Any],
    flat_threshold_bps: float,
) -> dict[str, Any]:
    horizon_ms = HORIZONS[horizon]
    target_time = timestamps[current_index] + horizon_ms * NS_PER_MS
    base = {
        "horizon_ms": horizon_ms,
        "target_local_recv_monotonic_ns": target_time,
        "max_future_gap_ms": MAX_FUTURE_GAP_MS[horizon],
        "future_index": None,
        "future_local_recv_monotonic_ns": None,
        "future_last_update_id": None,
        "future_mid_price": None,
        "future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "spread_adjusted_direction": None,
        "valid": False,
        "invalid_reason": None,
    }
    current_mid = current_features.get("mid_price")
    current_spread_bps = current_features.get("spread_bps")
    if not isinstance(current_mid, (int, float)) or not math.isfinite(float(current_mid)) or current_mid <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    future_index = select_future_index(timestamps, current_index, horizon_ms)
    if future_index is None:
        return {**base, "invalid_reason": "NO_FUTURE_SAMPLE"}

    future_sample = samples[future_index]
    future_ts = int(future_sample["local_recv_monotonic_ns"])
    future_gap_ms = (future_ts - target_time) / NS_PER_MS
    future_mid = _sample_mid_price(future_sample)
    base.update(
        {
            "future_index": future_index,
            "future_local_recv_monotonic_ns": future_ts,
            "future_last_update_id": future_sample.get("last_update_id"),
            "future_mid_price": future_mid,
            "future_gap_ms": future_gap_ms,
        }
    )
    if future_gap_ms > MAX_FUTURE_GAP_MS[horizon]:
        return {**base, "invalid_reason": "FUTURE_GAP_TOO_LARGE"}
    if future_mid is None or not math.isfinite(future_mid) or future_mid <= 0:
        return {**base, "invalid_reason": "FUTURE_MID_INVALID"}
    if (
        not isinstance(current_spread_bps, (int, float))
        or not math.isfinite(float(current_spread_bps))
        or current_spread_bps < 0
    ):
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    try:
        return_bps = compute_return_bps(float(current_mid), future_mid)
        direction = direction_label(
            return_bps,
            flat_threshold_bps=flat_threshold_bps,
        )
        spread_adjusted = spread_adjusted_direction_label(
            return_bps,
            spread_bps=float(current_spread_bps),
        )
    except ValueError:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    return {
        **base,
        "return_bps": return_bps,
        "direction": direction,
        "spread_adjusted_direction": spread_adjusted,
        "valid": True,
        "invalid_reason": None,
    }


def _sample_mid_price(sample: dict[str, Any]) -> float | None:
    try:
        best_bid = _as_float(sample.get("best_bid"))
        best_ask = _as_float(sample.get("best_ask"))
    except ValueError:
        return None
    mid = (best_bid + best_ask) / 2.0
    return mid if math.isfinite(mid) else None


def _label_warnings(labels: dict[str, dict[str, Any]]) -> list[str]:
    return sorted(
        {
            f"{horizon}:{label.get('invalid_reason')}"
            for horizon, label in labels.items()
            if label.get("valid") is not True and label.get("invalid_reason")
        }
    )


def _labeled_violation(index: int, row: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "sample_index": index,
        "symbol": row.get("symbol"),
        "generation_id": row.get("generation_id"),
        "last_update_id": row.get("last_update_id"),
        "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
        "reason": reason,
        "classification": "LABELED_SCHEMA_FAILURE",
    }


def _empty_leakage_result(*, checked_samples: int) -> dict[str, Any]:
    return {
        "passed": True,
        "feature_leakage_violations": 0,
        "label_leakage_violations": 0,
        "checked_samples": checked_samples,
        "checked_horizons": list(HORIZONS),
        "violations": [],
    }


def _duration_sec(samples: list[dict[str, Any]]) -> float:
    if len(samples) < 2:
        return 0.0
    first = int(samples[0]["local_recv_monotonic_ns"])
    last = int(samples[-1]["local_recv_monotonic_ns"])
    return max(0.0, (last - first) / 1_000_000_000.0)


def _label_quality(
    input_samples: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    last_ts = (
        int(input_samples[-1]["local_recv_monotonic_ns"]) if input_samples else None
    )
    horizons: dict[str, Any] = {}
    for horizon, horizon_ms in HORIZONS.items():
        labels = [
            row.get("labels", {}).get(horizon)
            for row in labeled_rows
            if isinstance(row.get("labels"), dict)
        ]
        valid_labels = [label for label in labels if isinstance(label, dict) and label.get("valid") is True]
        invalid_labels = [label for label in labels if isinstance(label, dict) and label.get("valid") is not True]
        eligible_count = 0
        if last_ts is not None:
            eligible_count = sum(
                1
                for sample in input_samples
                if int(sample["local_recv_monotonic_ns"]) + horizon_ms * NS_PER_MS <= last_ts
            )
        valid_count = len(valid_labels)
        invalid_count = len(labels) - valid_count
        reason_counts = Counter(str(label.get("invalid_reason")) for label in invalid_labels)
        reason_counts.pop("None", None)
        directions = Counter(label.get("direction") for label in valid_labels)
        returns = [
            float(label["return_bps"])
            for label in valid_labels
            if isinstance(label.get("return_bps"), (int, float))
        ]
        abs_returns = sorted(abs(value) for value in returns)
        future_gaps = sorted(
            float(label["future_gap_ms"])
            for label in valid_labels
            if isinstance(label.get("future_gap_ms"), (int, float))
        )
        denominator = valid_count or 1
        horizons[horizon] = {
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "eligible_count": eligible_count,
            "valid_rate_all_rows": valid_count / len(labeled_rows) if labeled_rows else 0.0,
            "valid_rate_eligible_rows": valid_count / eligible_count if eligible_count else 0.0,
            "tail_no_future_count": reason_counts.get("NO_FUTURE_SAMPLE", 0),
            "invalid_reason_counts": dict(reason_counts),
            "up_count": directions.get(1, 0),
            "down_count": directions.get(-1, 0),
            "flat_count": directions.get(0, 0),
            "up_pct": directions.get(1, 0) / denominator,
            "down_pct": directions.get(-1, 0) / denominator,
            "flat_pct": directions.get(0, 0) / denominator,
            "return_bps_p50": _percentile(sorted(returns), 0.50),
            "return_bps_p95_abs": _percentile(abs_returns, 0.95),
            "future_gap_ms_p50": _percentile(future_gaps, 0.50),
            "future_gap_ms_p95": _percentile(future_gaps, 0.95),
            "future_gap_ms_p99": _percentile(future_gaps, 0.99),
        }
    return {
        "label_leakage_violations": 0,
        "horizons": horizons,
    }


def _feature_quality(
    labeled_rows: list[dict[str, Any]],
    leakage_result: dict[str, Any],
) -> dict[str, Any]:
    null_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    for row in labeled_rows:
        features = row.get("features", {})
        if isinstance(features, dict):
            for field in REQUIRED_FEATURE_FIELDS:
                if features.get(field) is None:
                    null_counts[field] += 1
        quality = row.get("quality", {})
        if isinstance(quality, dict):
            warning_counts.update(str(warning) for warning in quality.get("feature_warnings", []))
    return {
        "feature_leakage_violations": int(
            leakage_result.get("feature_leakage_violations", 0)
        ),
        "null_feature_counts": dict(sorted(null_counts.items())),
        "feature_warning_counts": dict(sorted(warning_counts.items())),
    }


def _hard_fail_reasons(
    *,
    input_samples: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    input_validation_invalid_count: int,
    timestamp_quality: dict[str, Any],
    labeled_schema_result: LabeledSchemaResult,
    leakage_result: dict[str, Any],
    label_quality: dict[str, Any],
    min_rates: dict[str, float],
) -> list[str]:
    reasons: list[str] = []
    if not input_samples:
        reasons.append("input_sample_count = 0")
    if not labeled_rows:
        reasons.append("labeled_sample_count = 0")
    if input_samples and labeled_rows and len(input_samples) != len(labeled_rows):
        reasons.append("labeled_sample_count != input_sample_count")
    if input_validation_invalid_count > 0:
        reasons.append(f"invalid_clean_sample_count > 0 ({input_validation_invalid_count})")
    if timestamp_quality["timestamp_monotonic_violations"] > 0:
        reasons.append(
            "timestamp_monotonic_violations > 0 "
            f"({timestamp_quality['timestamp_monotonic_violations']})"
        )
    if timestamp_quality["duplicate_timestamp_count"] > 0:
        reasons.append(
            f"duplicate_timestamp_count > 0 ({timestamp_quality['duplicate_timestamp_count']})"
        )
    if labeled_schema_result.labeled_schema_violation_count > 0:
        reasons.append(
            "labeled_schema_violation_count > 0 "
            f"({labeled_schema_result.labeled_schema_violation_count})"
        )
    if int(leakage_result.get("feature_leakage_violations", 0)) > 0:
        reasons.append(
            "feature_leakage_violations > 0 "
            f"({leakage_result.get('feature_leakage_violations')})"
        )
    if int(leakage_result.get("label_leakage_violations", 0)) > 0:
        reasons.append(
            "label_leakage_violations > 0 "
            f"({leakage_result.get('label_leakage_violations')})"
        )
    for horizon, threshold in min_rates.items():
        stats = label_quality["horizons"][horizon]
        rate = stats["valid_rate_eligible_rows"]
        if stats["eligible_count"] <= 0 or rate < threshold:
            reasons.append(
                f"{horizon} valid_rate_eligible_rows {rate:.6f} below threshold {threshold:.2f}"
            )
    return reasons


def _warning_reasons(
    *,
    duration_sec: float,
    timestamp_quality: dict[str, Any],
    feature_quality: dict[str, Any],
    label_quality: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if duration_sec < 60.0:
        warnings.append("short_dataset_duration")
    if timestamp_quality.get("large_gap_count", 0) > 0:
        warnings.append("large_but_non_failing_timestamp_gaps")
    null_counts = feature_quality.get("null_feature_counts", {})
    if any(name.startswith("past_") and count > 0 for name, count in null_counts.items()):
        warnings.append("null_past_change_features_near_beginning_or_sparse_gaps")
    for horizon, stats in label_quality.get("horizons", {}).items():
        valid = stats.get("valid_count", 0)
        if valid == 0:
            continue
        if stats.get("flat_pct", 0.0) >= 0.90:
            warnings.append(f"{horizon}:too_many_flat_labels_or_low_volatility_period")
        if max(stats.get("up_pct", 0.0), stats.get("down_pct", 0.0), stats.get("flat_pct", 0.0)) >= 0.90:
            warnings.append(f"{horizon}:class_imbalance")
        if stats.get("tail_no_future_count", 0) > 0:
            warnings.append(f"{horizon}:null_future_labels_near_end_of_file")
        if (stats.get("return_bps_p95_abs") or 0.0) < 0.01:
            warnings.append(f"{horizon}:low_volatility_period")
    return sorted(set(warnings))


def _percentile(values: list[float], percentile: float) -> float | None:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = int(round((len(clean) - 1) * percentile))
    return clean[min(max(index, 0), len(clean) - 1)]


def _first_value(rows: list[dict[str, Any]], field: str) -> Any:
    return rows[0].get(field) if rows else None


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
