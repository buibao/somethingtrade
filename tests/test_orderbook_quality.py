from __future__ import annotations

from app.marketdata.orderbook_quality import OrderbookQualityValidator
from app.marketdata.orderbook_state import OrderbookState
from orderbook_phase41_test_utils import make_state


def test_tc_22_strict_bid_mismatch_is_reported() -> None:
    state = make_state()
    result = OrderbookQualityValidator().validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        reported_best_bid="99.99",
    )
    assert result.strict_mismatch_details["bid_mismatch"] is True
    assert "reported_best_bid_mismatch" in result.errors


def test_tc_23_strict_ask_mismatch_is_reported() -> None:
    state = make_state()
    result = OrderbookQualityValidator().validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        reported_best_ask="101.02",
    )
    assert result.strict_mismatch_details["ask_mismatch"] is True
    assert "reported_best_ask_mismatch" in result.errors


def test_tc_24_tolerant_mismatch_within_threshold_suppresses_tolerant_flag() -> None:
    state = make_state()
    result = OrderbookQualityValidator().validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        reported_best_bid="99.995",
    )
    assert result.strict_mismatch_details["strict_mismatch"] is True
    assert result.tolerant_mismatch_details["tolerant_mismatch"] is False


def test_tc_25_tolerant_mismatch_above_threshold_remains_tolerant_mismatch() -> None:
    state = make_state()
    result = OrderbookQualityValidator().validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        reported_best_bid="99.50",
    )
    assert result.strict_mismatch_details["strict_mismatch"] is True
    assert result.tolerant_mismatch_details["tolerant_mismatch"] is True


def test_tc_26_stale_book_uses_monotonic_age() -> None:
    state = make_state()
    result = OrderbookQualityValidator(stale_after_ms=10).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_500_000_000,
    )
    assert "stale_book" in result.errors


def test_tc_27_old_exchange_timestamp_does_not_make_fresh_local_book_stale() -> None:
    state = make_state()
    result = OrderbookQualityValidator(stale_after_ms=10).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_001_500_000,
    )
    assert "stale_book" not in result.errors


def test_tc_28_future_exchange_timestamp_does_not_hide_old_local_book() -> None:
    state = make_state()
    result = OrderbookQualityValidator(stale_after_ms=10).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=2_000_000_000,
    )
    assert "stale_book" in result.errors


def test_tc_29_book_incomplete_has_specific_root_causes_not_generic_only() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[],
        asks=[("101", "1")],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    result = OrderbookQualityValidator().validate(state.copy_snapshot(), now_monotonic_ns=1)
    assert "one_side_missing" in result.errors
    assert "best_bid_missing" in result.errors
    assert "book_incomplete" not in result.errors


def test_tc_30_queue_lag_warning_is_reported_without_hard_error() -> None:
    state = make_state()
    result = OrderbookQualityValidator(
        queue_lag_warning_ms=5,
        queue_lag_severe_ms=50,
    ).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        queue_lag_ms=10,
    )
    assert "queue_lag_exceeded" in result.warnings
    assert "queue_lag_exceeded" not in result.errors


def test_tc_31_severe_queue_lag_blocks_sample_and_can_mark_not_ready() -> None:
    state = make_state()
    state.mark_not_ready("queue_lag_exceeded", local_recv_monotonic_ns=1_002_000_000)
    result = OrderbookQualityValidator(
        queue_lag_warning_ms=5,
        queue_lag_severe_ms=50,
    ).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=1_002_000_000,
        queue_lag_ms=100,
    )
    assert state.ready_to_emit is False
    assert "queue_lag_exceeded" in result.errors
