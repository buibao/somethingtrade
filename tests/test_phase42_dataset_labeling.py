from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from app.research.orderbook_labeled_dataset import (
    HORIZONS,
    REQUIRED_FEATURE_FIELDS,
    build_quality_report,
    compute_return_bps,
    compute_timestamp_quality,
    direction_label,
    extract_current_features,
    generate_labeled_rows,
    run_leakage_check,
    select_future_index,
    spread_adjusted_direction_label,
    validate_clean_samples,
    validate_labeled_rows,
)


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample(
    ts_ms: int,
    *,
    best_bid: float = 100.0,
    best_ask: float = 101.0,
    generation_id: int | None = 1,
    last_update_id: int = 100,
    bid_sizes: list[float] | None = None,
    ask_sizes: list[float] | None = None,
) -> dict[str, object]:
    bid_sizes = bid_sizes or [10.0] * 20
    ask_sizes = ask_sizes or [5.0] * 20
    bids = [_level(best_bid - index, size) for index, size in enumerate(bid_sizes)]
    asks = [_level(best_ask + index, size) for index, size in enumerate(ask_sizes)]
    return {
        "schema_version": "phase_4_1_clean_orderbook_v1",
        "symbol": "BTCUSDT",
        "source": "binance_ws",
        "generation_id": generation_id,
        "state_version": last_update_id,
        "snapshot_version": last_update_id,
        "last_update_id": last_update_id,
        "local_recv_monotonic_ns": ts_ms * 1_000_000,
        "local_recv_wall_ts": "2026-05-19T17:58:54.000000+00:00",
        "exchange_event_ts": 1_779_213_534_814_000_000 + ts_ms,
        "best_bid": f"{best_bid:.8f}",
        "best_ask": f"{best_ask:.8f}",
        "bids": bids,
        "asks": asks,
        "quality": {"is_valid": True, "errors": [], "warnings": []},
        "lifecycle": {
            "snapshot_ready": True,
            "ready_to_emit": True,
            "sequence_continuous": True,
        },
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase42_rejects_missing_input_file(tmp_path: Path) -> None:
    result = validate_clean_samples(tmp_path / "missing.jsonl")

    assert result.valid is False
    assert result.invalid_clean_sample_count == 1
    assert result.failure_classification == "INPUT_FILE_MISSING"


def test_phase42_rejects_empty_input_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")

    result = validate_clean_samples(path)

    assert result.valid is False
    assert result.failure_classification == "INPUT_EMPTY"


def test_phase42_rejects_invalid_clean_sample_missing_generation_id(tmp_path: Path) -> None:
    path = tmp_path / "samples.jsonl"
    _write_jsonl(path, [_sample(0, generation_id=None)])

    result = validate_clean_samples(path)

    assert result.valid is False
    assert result.invalid_clean_sample_count == 1
    assert any("generation_id" in violation["reason"] for violation in result.violations)


def test_phase42_rejects_invalid_clean_sample_null_wall_ts(tmp_path: Path) -> None:
    row = _sample(0)
    row["local_recv_wall_ts"] = None
    path = tmp_path / "samples.jsonl"
    _write_jsonl(path, [row])

    result = validate_clean_samples(path)

    assert result.valid is False
    assert any("local_recv_wall_ts" in violation["reason"] for violation in result.violations)


def test_phase42_rejects_clean_sample_not_ready_lifecycle(tmp_path: Path) -> None:
    row = _sample(0)
    row["lifecycle"]["ready_to_emit"] = False  # type: ignore[index]
    path = tmp_path / "samples.jsonl"
    _write_jsonl(path, [row])

    result = validate_clean_samples(path)

    assert result.valid is False
    assert any("lifecycle.ready_to_emit" in violation["reason"] for violation in result.violations)


def test_phase42_rejects_clean_sample_with_quality_errors(tmp_path: Path) -> None:
    row = _sample(0)
    row["quality"]["errors"] = ["sequence_gap"]  # type: ignore[index]
    path = tmp_path / "samples.jsonl"
    _write_jsonl(path, [row])

    result = validate_clean_samples(path)

    assert result.valid is False
    assert any("quality.errors" in violation["reason"] for violation in result.violations)


def test_phase42_accepts_valid_clean_sample_fixture(tmp_path: Path) -> None:
    path = tmp_path / "samples.jsonl"
    _write_jsonl(path, [_sample(0), _sample(100, last_update_id=101)])

    result = validate_clean_samples(path)

    assert result.valid is True
    assert result.invalid_clean_sample_count == 0
    assert len(result.samples) == 2


def test_timestamp_monotonic_valid_sequence() -> None:
    quality = compute_timestamp_quality([_sample(0), _sample(100), _sample(200)])

    assert quality["timestamp_monotonic_violations"] == 0
    assert quality["duplicate_timestamp_count"] == 0
    assert quality["p50_gap_ms"] == 100.0


def test_timestamp_monotonic_violation_detected() -> None:
    quality = compute_timestamp_quality([_sample(100), _sample(90)])

    assert quality["timestamp_monotonic_violations"] == 1


def test_duplicate_timestamp_detected() -> None:
    quality = compute_timestamp_quality([_sample(100), _sample(100)])

    assert quality["duplicate_timestamp_count"] == 1


def test_large_gap_count_detected() -> None:
    quality = compute_timestamp_quality([_sample(0), _sample(2_500)])

    assert quality["large_gap_count"] == 1
    assert quality["max_gap_ms"] == 2500.0


def test_future_sample_uses_first_at_or_after_target_time() -> None:
    timestamps = [0, 90_000_000, 100_000_000, 150_000_000]

    assert select_future_index(timestamps, 0, 100) == 2


def test_future_sample_skips_before_target_time() -> None:
    timestamps = [0, 99_000_000, 101_000_000]

    assert select_future_index(timestamps, 0, 100) == 2


def test_future_sample_never_uses_current_or_past_sample() -> None:
    timestamps = [100_000_000, 150_000_000]

    assert select_future_index(timestamps, 0, 0) == 1


def test_label_invalid_when_no_future_sample() -> None:
    labeled = generate_labeled_rows([_sample(0), _sample(100)])

    assert labeled[-1]["labels"]["horizon_100ms"]["valid"] is False
    assert labeled[-1]["labels"]["horizon_100ms"]["invalid_reason"] == "NO_FUTURE_SAMPLE"


def test_label_invalid_when_future_gap_too_large() -> None:
    labeled = generate_labeled_rows([_sample(0), _sample(250)])

    label = labeled[0]["labels"]["horizon_100ms"]
    assert label["valid"] is False
    assert label["invalid_reason"] == "FUTURE_GAP_TOO_LARGE"


def test_future_gap_ms_calculated_correctly() -> None:
    labeled = generate_labeled_rows([_sample(0), _sample(125)])

    label = labeled[0]["labels"]["horizon_100ms"]
    assert label["valid"] is True
    assert label["future_gap_ms"] == 25.0


def test_return_bps_zero_positive_negative_and_invalid_mid() -> None:
    assert compute_return_bps(100.0, 100.0) == 0.0
    assert compute_return_bps(100.0, 101.0) == 100.0
    assert compute_return_bps(100.0, 99.0) == -100.0
    with pytest.raises(ValueError):
        compute_return_bps(0.0, 100.0)
    with pytest.raises(ValueError):
        compute_return_bps(math.nan, 100.0)


def test_direction_threshold_boundaries() -> None:
    assert direction_label(1.01, flat_threshold_bps=1.0) == 1
    assert direction_label(-1.01, flat_threshold_bps=1.0) == -1
    assert direction_label(1.0, flat_threshold_bps=1.0) == 0
    assert direction_label(-1.0, flat_threshold_bps=1.0) == 0
    assert direction_label(0.25, flat_threshold_bps=1.0) == 0


def test_spread_adjusted_direction_policy() -> None:
    assert spread_adjusted_direction_label(2.01, spread_bps=2.0) == 1
    assert spread_adjusted_direction_label(-2.01, spread_bps=2.0) == -1
    assert spread_adjusted_direction_label(2.0, spread_bps=2.0) == 0
    assert spread_adjusted_direction_label(0.01, spread_bps=0.0) == 1
    assert spread_adjusted_direction_label(0.0, spread_bps=0.0) == 0
    with pytest.raises(ValueError):
        spread_adjusted_direction_label(1.0, spread_bps=-0.1)


def test_feature_extraction_basic_depth_microprice_and_slope() -> None:
    features, warnings, source_indices = extract_current_features(
        _sample(
            0,
            best_bid=100.0,
            best_ask=102.0,
            bid_sizes=[10.0, 9.0, 8.0, 7.0, 6.0] + [1.0] * 15,
            ask_sizes=[5.0, 4.0, 3.0, 2.0, 1.0] + [1.0] * 15,
        ),
        sample_index=0,
    )

    assert REQUIRED_FEATURE_FIELDS <= set(features)
    assert features["mid_price"] == 101.0
    assert features["spread"] == 2.0
    assert features["spread_bps"] == pytest.approx(198.01980198)
    assert features["bid_depth_l1"] == 10.0
    assert features["ask_depth_l5"] == 15.0
    assert features["total_depth_l5"] == 55.0
    assert features["depth_imbalance_l1"] == pytest.approx((10 - 5) / 15)
    assert features["depth_ratio_l1"] == 2.0
    assert features["microprice_l1"] == pytest.approx((102 * 10 + 100 * 5) / 15)
    assert features["l1_size_imbalance"] == pytest.approx((10 - 5) / 15)
    assert features["bid_slope_l5"] == pytest.approx(4.0 / 40.0)
    assert features["ask_slope_l5"] == pytest.approx(4.0 / 15.0)
    assert not warnings
    assert all(index == 0 for index in source_indices.values())


def test_feature_extraction_null_ratio_microprice_and_slope_warnings() -> None:
    features, warnings, _ = extract_current_features(
        _sample(0, bid_sizes=[0.0, 0.0], ask_sizes=[0.0, 0.0]),
        sample_index=0,
    )

    assert features["depth_ratio_l1"] is None
    assert features["depth_imbalance_l1"] is None
    assert features["microprice_l1"] is None
    assert features["bid_slope_l5"] is None
    assert "depth_ratio_l1_denominator_zero" in warnings
    assert "microprice_l1_denominator_zero" in warnings
    assert "bid_slope_l5_insufficient_levels" in warnings


def test_past_feature_uses_latest_at_or_before_target_time() -> None:
    samples = [_sample(0), _sample(80, best_bid=101, best_ask=102), _sample(220, best_bid=103, best_ask=104)]

    labeled = generate_labeled_rows(samples)
    features = labeled[2]["features"]

    assert features["past_mid_return_100ms_bps"] == pytest.approx(((103.5 - 101.5) / 101.5) * 10000)
    assert labeled[2]["quality"]["feature_source_indices"]["past_mid_return_100ms_bps"] == 1


def test_past_feature_null_when_no_past_sample_or_gap_too_large() -> None:
    labeled = generate_labeled_rows([_sample(0), _sample(10_000)])

    assert labeled[0]["features"]["past_mid_return_100ms_bps"] is None
    assert labeled[1]["features"]["past_mid_return_100ms_bps"] is None
    assert "past_mid_return_100ms_bps_gap_too_large" in labeled[1]["quality"]["feature_warnings"]


def test_labeled_sample_schema_preserves_identity_and_horizons() -> None:
    source = _sample(0, generation_id=7, last_update_id=555)
    labeled = generate_labeled_rows([source, _sample(100, last_update_id=556)])

    row = labeled[0]
    assert row["generation_id"] == 7
    assert row["last_update_id"] == 555
    assert row["local_recv_monotonic_ns"] == source["local_recv_monotonic_ns"]
    assert row["local_recv_wall_ts"] == source["local_recv_wall_ts"]
    assert set(row["labels"]) == set(HORIZONS)
    assert row["labels"]["horizon_100ms"]["valid"] is True
    assert row["labels"]["horizon_100ms"]["invalid_reason"] is None
    assert row["labels"]["horizon_250ms"]["valid"] is False
    assert row["labels"]["horizon_250ms"]["invalid_reason"] == "NO_FUTURE_SAMPLE"


def test_labeled_schema_violation_count_increments() -> None:
    rows = generate_labeled_rows([_sample(0), _sample(100)])
    rows[0]["labels"].pop("horizon_100ms")

    result = validate_labeled_rows(rows)

    assert result.labeled_schema_violation_count == 1
    assert result.violations[0]["reason"] == "missing_label_horizon_100ms"


def test_leakage_check_passes_valid_dataset_and_writes_debug_file(tmp_path: Path) -> None:
    rows = generate_labeled_rows([_sample(0), _sample(100), _sample(250)])
    path = tmp_path / "leakage.json"

    result = run_leakage_check(rows, output_path=path)

    assert result["passed"] is True
    assert result["feature_leakage_violations"] == 0
    assert result["label_leakage_violations"] == 0
    assert json.loads(path.read_text(encoding="utf-8"))["passed"] is True


def test_label_leakage_violations_detected() -> None:
    rows = generate_labeled_rows([_sample(0), _sample(100), _sample(250)])
    rows[0]["labels"]["horizon_100ms"]["future_index"] = 0

    result = run_leakage_check(rows)

    assert result["passed"] is False
    assert result["label_leakage_violations"] == 1


def test_feature_leakage_violation_detected() -> None:
    rows = generate_labeled_rows([_sample(0), _sample(100), _sample(250)])
    rows[0]["quality"]["feature_source_indices"]["past_mid_return_100ms_bps"] = 1

    result = run_leakage_check(rows)

    assert result["passed"] is False
    assert result["feature_leakage_violations"] == 1


def test_dataset_quality_report_contains_required_fields_and_gates_on_eligible_rate(tmp_path: Path) -> None:
    samples = [_sample(index * 100, last_update_id=100 + index) for index in range(80)]
    labeled = generate_labeled_rows(samples)
    leakage = run_leakage_check(labeled)
    schema = validate_labeled_rows(labeled)
    report = build_quality_report(
        input_path=Path("input.jsonl"),
        output_path=Path("output.jsonl"),
        input_samples=samples,
        labeled_rows=labeled,
        input_validation_invalid_count=0,
        input_schema_violations=[],
        labeled_schema_result=schema,
        leakage_result=leakage,
    )

    assert {
        "phase",
        "status",
        "timestamp_quality",
        "input_schema_quality",
        "labeled_schema_quality",
        "feature_quality",
        "label_quality",
        "leakage_check",
    } <= set(report)
    assert report["label_quality"]["horizons"]["horizon_5000ms"]["eligible_count"] > 0
    assert report["status"] == "pass"


def test_report_hard_fails_on_schema_timestamp_and_leakage_failures() -> None:
    samples = [_sample(100), _sample(90)]
    labeled = generate_labeled_rows(samples)
    leakage = run_leakage_check(labeled)
    schema = validate_labeled_rows(labeled)
    report = build_quality_report(
        input_path=Path("input.jsonl"),
        output_path=Path("output.jsonl"),
        input_samples=samples,
        labeled_rows=labeled,
        input_validation_invalid_count=1,
        input_schema_violations=[{"reason": "bad"}],
        labeled_schema_result=schema,
        leakage_result={**leakage, "label_leakage_violations": 1, "passed": False},
    )

    assert report["status"] == "fail"
    assert any("invalid_clean_sample_count" in reason for reason in report["hard_fail_reasons"])
    assert any("timestamp_monotonic_violations" in reason for reason in report["hard_fail_reasons"])
    assert any("label_leakage_violations" in reason for reason in report["hard_fail_reasons"])
