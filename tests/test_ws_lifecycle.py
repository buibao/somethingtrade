from __future__ import annotations

from app.marketdata.orderbook_quality import OrderbookQualityValidator
from app.marketdata.ws_lifecycle import WSLifecycleTracker
from orderbook_phase41_test_utils import make_processor, make_state, make_depth_update


def test_tc_42_delta_before_snapshot_is_counted_and_no_sample(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.cleanup_symbol("BTCUSDT")
    result = processor.process_depth_update(
        make_depth_update(first_update_id=1, final_update_id=1)
    )
    assert not result.accepted
    assert processor.lifecycle.report()["delta_before_snapshot_count"] == 1


def test_tc_43_reconnect_marks_state_not_ready() -> None:
    state = make_state()
    lifecycle = WSLifecycleTracker()
    lifecycle.on_reconnect(state)
    assert state.ready_to_emit is False
    assert lifecycle.report()["reconnect_count"] == 1


def test_tc_44_resubscribe_marks_state_not_ready() -> None:
    state = make_state()
    lifecycle = WSLifecycleTracker()
    lifecycle.on_resubscribe(state)
    assert state.ready_to_emit is False
    assert lifecycle.report()["resubscribe_count"] == 1


def test_tc_45_fresh_snapshot_after_reconnect_restores_after_valid_bridge() -> None:
    state = make_state()
    lifecycle = WSLifecycleTracker()
    lifecycle.on_reconnect(state)
    assert not state.ready_to_emit
    snapshot = state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=200,
        local_recv_monotonic_ns=2,
    )
    assert snapshot.accepted
    assert not state.ready_to_emit
    bridge = state.apply_delta(
        first_update_id=201,
        final_update_id=201,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert bridge.accepted
    assert state.ready_to_emit


def test_tc_46_messages_before_ready_counter_increments() -> None:
    lifecycle = WSLifecycleTracker()
    lifecycle.on_message_before_ready()
    assert lifecycle.report()["messages_before_ready_count"] == 1


def test_tc_47_binance_market_lifecycle_remains_unknown() -> None:
    report = WSLifecycleTracker().report()
    assert report["market_status_known"] is False
    assert report["market_paused_count"] == 0
    assert report["market_unpaused_count"] == 0
    assert report["market_resolved_count"] == 0


def test_tc_48_stale_gap_is_stale_not_market_pause() -> None:
    state = make_state()
    result = OrderbookQualityValidator(stale_after_ms=1).validate(
        state.copy_snapshot(),
        state=state,
        now_monotonic_ns=2_000_000_000,
    )
    report = WSLifecycleTracker().report()
    assert "stale_book" in result.errors
    assert report["market_paused_count"] == 0


def test_tc_49_sample_blocked_by_ready_guard_counter_increments() -> None:
    lifecycle = WSLifecycleTracker()
    lifecycle.on_sample_blocked_by_ready_guard()
    assert lifecycle.report()["sample_blocked_by_ready_guard"] == 1


def test_tc_50_clean_lifecycle_report_has_required_fields() -> None:
    report = WSLifecycleTracker().report()
    required = {
        "connect_count",
        "disconnect_count",
        "reconnect_count",
        "resubscribe_count",
        "snapshot_loaded_count",
        "sequence_gap_count",
        "duplicate_messages_skipped",
        "market_status_known",
    }
    assert required <= set(report)
