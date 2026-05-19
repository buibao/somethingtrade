from __future__ import annotations

import importlib.util
from pathlib import Path


def _checker_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "check_phase41_report.py"
    spec = importlib.util.spec_from_file_location("check_phase41_report", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _report(**overrides):
    report = {
        "duration_sec": 600,
        "symbol": "BTCUSDT",
        "sequence_gap_count": 0,
        "invalid_delta_count": 0,
        "crossed_book_count": 0,
        "book_empty_count": 0,
        "one_side_missing_count": 0,
        "clean_sample_schema_violation_count": 0,
        "sample_before_ready_count": 0,
        "feed_receive_stale_count": 0,
        "bridge_missing_after_snapshot_count": 0,
        "first_delta_bridge_failed_count": 0,
        "post_capture_age_warning_count": 0,
        "market_status_mode": "not_applicable_for_binance_spot_orderbook",
        "snapshot_copy_p99_us": 50,
        "queue": {
            "queue_dropped_messages": 0,
            "enqueue_to_dequeue_lag_p95_ms": 10,
            "enqueue_to_dequeue_lag_p99_ms": 20,
            "processing_lag_p99_ms": 5,
        },
        "lifecycle": {
            "snapshot_loaded_count": 1,
            "snapshot_refresh_count": 1,
        },
    }
    report.update(overrides)
    return report


def test_evaluator_hard_fails_on_sample_before_ready() -> None:
    checker = _checker_module()
    result = checker.evaluate_report(_report(sample_before_ready_count=1), gate="2m")

    assert result["passed"] is False
    assert any("sample_before_ready_count" in reason for reason in result["hard_fail_reasons"])


def test_evaluator_hard_fails_on_feed_receive_stale() -> None:
    checker = _checker_module()
    result = checker.evaluate_report(_report(feed_receive_stale_count=1), gate="2m")

    assert result["passed"] is False
    assert any("feed_receive_stale_count" in reason for reason in result["hard_fail_reasons"])


def test_evaluator_warns_on_post_capture_age() -> None:
    checker = _checker_module()
    result = checker.evaluate_report(_report(post_capture_age_warning_count=5), gate="2m")

    assert result["passed"] is True
    assert any("post_capture_age_warning_count" in reason for reason in result["warning_reasons"])


def test_evaluator_market_status_not_applicable_not_fail() -> None:
    checker = _checker_module()
    result = checker.evaluate_report(_report(), gate="2m")

    assert result["passed"] is True
    assert "market_status_not_applicable_for_binance_spot_orderbook" in result["warning_reasons"]


def test_evaluator_fails_on_queue_lag_p99_over_gate_threshold() -> None:
    checker = _checker_module()
    report = _report(queue={"queue_dropped_messages": 0, "enqueue_to_dequeue_lag_p99_ms": 1000})
    result = checker.evaluate_report(report, gate="10m")

    assert result["passed"] is False
    assert any("queue_lag_p99_ms exceeded" in reason for reason in result["hard_fail_reasons"])
