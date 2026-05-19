from __future__ import annotations

import json

from app.marketdata.orderbook_phase41 import evaluate_phase_4_1_pass
from orderbook_phase41_test_utils import (
    FakeMonotonicClock,
    make_depth_update,
    make_processor,
    make_state,
)


def _report(**overrides):
    report = {
        "sequence_gap_count": 0,
        "sequence_gap_or_reset_count": 0,
        "crossed_book_count": 0,
        "book_empty_count": 0,
        "one_side_missing_count": 0,
        "sample_before_ready_count": 0,
        "invalid_delta_count": 0,
        "stale_book_count": 0,
        "active_feed_stale_count": 0,
        "feed_receive_stale_count": 0,
        "post_capture_age_warning_count": 0,
        "ready_to_emit_violation_count": 0,
        "clean_sample_schema_violation_count": 0,
        "snapshot_copy_budget_met": True,
        "queue": {
            "queue_dropped_messages": 0,
            "queue_backpressure_events": 0,
        },
    }
    report.update(overrides)
    return report


def test_pass_fail_evaluator_sequence_gap_count_fails() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report(sequence_gap_count=1))
    assert passed is False
    assert reasons == ["sequence_gap_count > 0"]


def test_pass_fail_evaluator_sequence_gap_or_reset_count_fails() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report(sequence_gap_or_reset_count=1))
    assert passed is False
    assert "sequence_gap_or_reset_count > 0" in reasons


def test_pass_fail_evaluator_sample_before_ready_fails() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report(sample_before_ready_count=1))
    assert passed is False
    assert "sample_before_ready_count > 0" in reasons


def test_pass_fail_evaluator_invalid_delta_fails() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report(invalid_delta_count=1))
    assert passed is False
    assert "invalid_delta_count > 0" in reasons


def test_pass_fail_evaluator_crossed_book_fails() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report(crossed_book_count=1))
    assert passed is False
    assert "crossed_book_count > 0" in reasons


def test_pass_fail_evaluator_clean_report_passes() -> None:
    passed, reasons = evaluate_phase_4_1_pass(_report())
    assert passed is True
    assert reasons == []


def test_report_with_sequence_gap_is_false_and_has_failure_reason(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=110)
    )
    summary = processor.write_reports(duration_sec=1)
    markdown = (tmp_path / "phase_4_1_orderbook_quality_report.md").read_text()
    assert summary["sequence_gap_count"] == 1
    assert summary["phase_4_1_pass"] is False
    assert summary["phase_4_1_failure_reasons"] == ["sequence_gap_count > 0"]
    assert "Failure reasons" in markdown
    assert "sequence_gap_count > 0" in markdown


def test_sequence_recovery_trace_records_post_snapshot_ranges_and_gap(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=110)
    )
    rows = [
        json.loads(line)
        for line in (tmp_path / "sequence_recovery_trace.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert rows
    assert any(row["event"] == "snapshot_loaded" for row in rows)
    assert any(row["event"] == "post_snapshot_update_range" for row in rows)
    gap = next(row for row in rows if row["event"] == "sequence_gap_detected")
    assert gap["expected_next_update_id"] == 102
    assert gap["received_first_update_id"] == 105
    assert gap["gap_size"] == 3
    assert gap["updates_processed_since_snapshot"] >= 1


def test_sequence_recovery_trace_records_recovery_snapshot_and_ready_restored(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=110)
    )
    processor.load_snapshot(
        "BTCUSDT",
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=111,
        local_recv_monotonic_ns=1_010_000_000,
        recovery=True,
    )
    processor.process_depth_update(
        make_depth_update(
            first_update_id=112,
            final_update_id=112,
            recv_monotonic_ns=1_011_000_000,
        )
    )
    rows = [
        json.loads(line)
        for line in (tmp_path / "sequence_recovery_trace.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert any(row["event"] == "recovery_snapshot_loaded" for row in rows)
    assert any(row["event"] == "recovery_ready_restored" for row in rows)


def test_stale_uses_local_monotonic_not_exchange_timestamp(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=1_000)
    clock.set(1_003_000_000)
    processor.process_depth_update(
        make_depth_update(
            first_update_id=102,
            final_update_id=102,
            recv_monotonic_ns=1_003_000_000,
        )
    )
    summary = processor.summary(duration_sec=1)
    assert summary["stale_book_count"] == 0


def test_stale_detects_no_successful_update_after_fake_clock_advances(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    clock.advance_ms(250)
    stale_periods = processor.check_stale_periods()
    summary = processor.summary(duration_sec=1)
    assert stale_periods
    assert summary["stale_book_count"] == 1
    assert summary["active_feed_stale_count"] == 1
    assert summary["max_book_age_ms"] >= 250
    assert summary["last_book_update_age_ms_at_report"] >= 250
    assert summary["phase_4_1_pass"] is False
    assert "feed_receive_stale_count > 0" in summary["phase_4_1_failure_reasons"]
    row = json.loads((tmp_path / "stale_period_cases.jsonl").read_text().splitlines()[0])
    assert row["reason"] == "no_websocket_message_received"


def test_post_capture_age_warning_does_not_fail_by_itself(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    processor.set_capture_active(False)
    clock.advance_ms(250)
    summary = processor.summary(duration_sec=1)
    assert summary["active_feed_stale_count"] == 0
    assert summary["stale_book_count"] == 0
    assert summary["post_capture_age_warning_count"] == 1
    assert summary["post_capture_age_warnings"]
    assert summary["phase_4_1_pass"] is True


def test_active_stale_before_shutdown_remains_blocking(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    clock.advance_ms(250)
    processor.check_stale_periods(feed_active=True)
    processor.set_capture_active(False)
    summary = processor.summary(duration_sec=1)
    assert summary["active_feed_stale_count"] == 1
    assert summary["post_capture_age_warning_count"] == 0
    assert summary["phase_4_1_pass"] is False
    assert "feed_receive_stale_count > 0" in summary["phase_4_1_failure_reasons"]


def test_invalid_delta_does_not_refresh_last_successful_book_update_timestamp() -> None:
    state = make_state()
    previous = state.last_book_update_monotonic_ns
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_500_000_000,
    )
    assert result.status == "invalid_delta_levels"
    assert state.last_book_update_monotonic_ns == previous
    assert state.last_successful_apply_monotonic_ns == previous


def test_duplicate_skipped_update_does_not_refresh_last_successful_book_update_timestamp() -> None:
    state = make_state()
    previous = state.last_book_update_monotonic_ns
    result = state.apply_delta(
        first_update_id=101,
        final_update_id=101,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_500_000_000,
    )
    assert result.status == "duplicate_update"
    assert state.ready_to_emit is True
    assert state.last_book_update_monotonic_ns == previous


def test_expected_negative_price_delta_fails_closed() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.status == "invalid_delta_levels"
    assert state.ready_to_emit is False
    assert state.snapshot_ready is False


def test_expected_negative_size_delta_fails_closed() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100", "-1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.status == "invalid_delta_levels"
    assert state.ready_to_emit is False
    assert state.snapshot_ready is False


def test_expected_nan_and_infinity_delta_fails_closed() -> None:
    for price, size in (("NaN", "1"), ("Infinity", "1"), ("100", "NaN"), ("100", "Infinity")):
        state = make_state()
        result = state.apply_delta(
            first_update_id=102,
            final_update_id=102,
            bids=[(price, size)],
            asks=[],
            local_recv_monotonic_ns=1_002_000_000,
        )
        assert result.status == "invalid_delta_levels"
        assert state.ready_to_emit is False
        assert state.snapshot_ready is False


def test_expected_malformed_delta_fails_closed() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100",)],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.status == "invalid_delta_levels"
    assert state.ready_to_emit is False
    assert state.snapshot_ready is False


def test_old_duplicate_invalid_delta_does_not_fail_closed() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=101,
        final_update_id=101,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.status == "duplicate_update"
    assert state.ready_to_emit is True
    assert state.snapshot_ready is True


def test_future_gap_delta_fails_as_sequence_gap_not_invalid_delta() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=105,
        final_update_id=105,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.status == "sequence_gap_or_reset"
    assert state.ready_to_emit is False


def test_sample_emission_blocked_after_invalid_delta_until_fresh_snapshot(tmp_path) -> None:
    processor = make_processor(tmp_path)
    result = processor.process_depth_update(
        make_depth_update(
            first_update_id=102,
            final_update_id=102,
            bids=[("-1", "1")],
        )
    )
    summary = processor.summary(duration_sec=1)
    rows = (tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()
    assert result.status == "invalid_delta_levels"
    assert len(rows) == 1
    assert summary["invalid_delta_count"] == 1
    assert summary["phase_4_1_pass"] is False
    assert "invalid_delta_count > 0" in summary["phase_4_1_failure_reasons"]
    assert (tmp_path / "invalid_delta_cases.jsonl").read_text().strip()


def test_fresh_snapshot_recovers_readiness_after_invalid_delta() -> None:
    state = make_state()
    invalid = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert invalid.status == "invalid_delta_levels"
    snapshot = state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=103,
        local_recv_monotonic_ns=1_003_000_000,
    )
    bridge = state.apply_delta(
        first_update_id=104,
        final_update_id=104,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=1_004_000_000,
    )
    assert snapshot.accepted
    assert bridge.accepted
    assert state.ready_to_emit is True
