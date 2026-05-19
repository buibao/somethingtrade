from __future__ import annotations

import asyncio
import json

from app.core.events import DepthUpdate
from app.marketdata.orderbook_phase41 import (
    purge_queue_after_snapshot,
    purge_queued_depth_updates_after_snapshot,
)
from app.marketdata.queue_monitor import QueueBackpressureMonitor, QueueEnvelope
from orderbook_phase41_test_utils import FakeMonotonicClock, make_depth_update, make_processor


def _envelope(update: DepthUpdate, enqueue_ns: int = 1_000_000_000) -> QueueEnvelope:
    return QueueEnvelope(
        payload=update,
        enqueue_monotonic_ns=enqueue_ns,
        queue_size_at_enqueue=1,
    )


def _purge(*updates: DepthUpdate, snapshot_last_update_id: int):
    return purge_queued_depth_updates_after_snapshot(
        [_envelope(update) for update in updates],
        snapshot_last_update_id=snapshot_last_update_id,
        symbol="BTCUSDT",
    )


def test_post_snapshot_purge_drops_old_events() -> None:
    output = _purge(
        make_depth_update(first_update_id=80, final_update_id=90),
        make_depth_update(first_update_id=91, final_update_id=99),
        make_depth_update(first_update_id=95, final_update_id=100),
        snapshot_last_update_id=100,
    )

    assert output.result.old_events_dropped == 3
    assert output.result.bridge_candidate_found is False
    assert output.result.queue_size_after == 0
    assert output.preserved == ()


def test_post_snapshot_purge_preserves_bridge_event() -> None:
    output = _purge(
        make_depth_update(first_update_id=80, final_update_id=90),
        make_depth_update(first_update_id=95, final_update_id=100),
        make_depth_update(first_update_id=99, final_update_id=105),
        make_depth_update(first_update_id=106, final_update_id=110),
        snapshot_last_update_id=100,
    )

    preserved = [envelope.payload for envelope in output.preserved]
    assert output.result.old_events_dropped == 2
    assert output.result.bridge_candidate_found is True
    assert output.result.bridge_first_update_id == 99
    assert output.result.bridge_final_update_id == 105
    assert [(event.first_update_id, event.final_update_id) for event in preserved] == [
        (99, 105),
        (106, 110),
    ]


def test_post_snapshot_purge_preserves_newer_events_after_bridge() -> None:
    output = _purge(
        make_depth_update(first_update_id=150, final_update_id=180),
        make_depth_update(first_update_id=190, final_update_id=205),
        make_depth_update(first_update_id=206, final_update_id=210),
        make_depth_update(first_update_id=211, final_update_id=220),
        snapshot_last_update_id=200,
    )

    preserved = [envelope.payload for envelope in output.preserved]
    assert output.result.old_events_dropped == 1
    assert output.result.bridge_candidate_found is True
    assert [(event.first_update_id, event.final_update_id) for event in preserved] == [
        (190, 205),
        (206, 210),
        (211, 220),
    ]


def test_post_snapshot_purge_no_bridge_found_drops_future_events() -> None:
    output = _purge(
        make_depth_update(first_update_id=105, final_update_id=110),
        make_depth_update(first_update_id=111, final_update_id=120),
        snapshot_last_update_id=100,
    )

    assert output.result.bridge_candidate_found is False
    assert output.result.bridge_missing_after_snapshot is True
    assert output.result.future_events_dropped == 2
    assert output.result.queue_size_after == 0


def test_snapshot_purge_logs_structured_trace(tmp_path) -> None:
    clock = FakeMonotonicClock(1_500_000_000)
    processor = make_processor(tmp_path, clock=clock)
    queue: asyncio.Queue[QueueEnvelope] = asyncio.Queue()
    queue.put_nowait(_envelope(make_depth_update(first_update_id=99, final_update_id=105)))

    result = purge_queue_after_snapshot(
        queue=queue,
        processor=processor,
        symbol="BTCUSDT",
        snapshot_last_update_id=100,
    )

    assert result.bridge_candidate_found is True
    rows = [
        json.loads(line)
        for line in (tmp_path / "sequence_recovery_trace.jsonl").read_text().splitlines()
        if line.strip()
    ]
    purge = next(row for row in rows if row["event"] == "post_snapshot_queue_purge")
    assert purge["snapshot_last_update_id"] == 100
    assert purge["queue_size_before"] == 1
    assert purge["old_events_dropped"] == 0
    assert purge["bridge_candidate_found"] is True
    assert purge["queue_size_after"] == 1
    assert "generation" in purge


def test_feed_receive_stale_marks_runtime_blocker(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    clock.advance_ms(250)

    stale = processor.check_stale_periods(feed_active=True, queue_size=0)
    summary = processor.summary(duration_sec=1)

    assert stale
    assert summary["feed_receive_stale_count"] == 1
    assert summary["active_feed_stale_count"] == 1
    assert summary["stale_reset_count"] == 1
    assert summary["phase_4_1_pass"] is False


def test_processor_apply_stale_does_not_immediately_reset_generation(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    state = processor.state_for("BTCUSDT")
    generation = state.generation
    clock.advance_ms(250)
    processor.record_ws_message_recv("BTCUSDT", clock())

    processor.check_stale_periods(feed_active=True, queue_size=1)
    summary = processor.summary(duration_sec=1)

    assert summary["processor_apply_stale_count"] == 1
    assert summary["feed_receive_stale_count"] == 0
    assert summary["stale_reset_count"] == 0
    assert state.generation == generation


def test_stale_check_ignored_during_snapshot_load(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    state = processor.state_for("BTCUSDT")
    generation = state.generation
    processor.mark_snapshot_recovery_active("BTCUSDT", active=True, monotonic_ns=clock())
    clock.advance_ms(250)

    processor.check_stale_periods(feed_active=True, queue_size=2)

    assert processor.summary(duration_sec=1)["feed_receive_stale_count"] == 0
    assert state.generation == generation


def test_phase411_post_capture_age_warning_is_non_blocking(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=100)
    processor.set_capture_active(False)
    clock.advance_ms(250)

    summary = processor.summary(duration_sec=1)

    assert summary["post_capture_age_warning_count"] == 1
    assert summary["feed_receive_stale_count"] == 0
    assert summary["phase_4_1_pass"] is True


def test_queue_size_backpressure_counter_only_for_size_pressure() -> None:
    monitor = QueueBackpressureMonitor(capacity=10, severe_lag_ms=250)
    monitor.record_enqueue("x", enqueue_monotonic_ns=0, queue_size=8)
    report = monitor.report()

    assert report["queue_size_backpressure_events"] == 1
    assert report["queue_lag_backpressure_events"] == 0


def test_queue_lag_backpressure_counter_only_for_lag_pressure() -> None:
    monitor = QueueBackpressureMonitor(capacity=100, severe_lag_ms=250)
    envelope = monitor.record_enqueue("x", enqueue_monotonic_ns=0, queue_size=1)
    monitor.record_dequeue(envelope, dequeue_monotonic_ns=300_000_000, queue_size=0)
    report = monitor.report()

    assert report["queue_lag_backpressure_events"] == 1
    assert report["queue_size_backpressure_events"] == 0


def test_snapshot_blocking_lag_counter() -> None:
    monitor = QueueBackpressureMonitor(snapshot_blocking_lag_ms=50)
    monitor.record_snapshot_request_duration(100)
    report = monitor.report()

    assert report["snapshot_blocking_lag_events"] == 1


def test_processing_lag_histogram_records_apply_duration() -> None:
    monitor = QueueBackpressureMonitor(processing_lag_severe_ms=50)
    monitor.record_processing_done(
        dequeue_monotonic_ns=0,
        processing_done_monotonic_ns=100_000_000,
    )
    report = monitor.report()

    assert report["processing_lag_backpressure_events"] == 1
    assert report["processing_lag_p95_ms"] == 100


def test_queue_metrics_backward_compatible_sum() -> None:
    monitor = QueueBackpressureMonitor(capacity=10, severe_lag_ms=250, processing_lag_severe_ms=50)
    envelope = monitor.record_enqueue("x", enqueue_monotonic_ns=0, queue_size=8)
    monitor.record_dequeue(envelope, dequeue_monotonic_ns=300_000_000, queue_size=0)
    monitor.record_processing_done(
        dequeue_monotonic_ns=300_000_000,
        processing_done_monotonic_ns=400_000_000,
    )
    monitor.record_snapshot_blocking_lag()
    report = monitor.report()

    assert report["queue_backpressure_events"] == (
        report["queue_size_backpressure_events"]
        + report["queue_lag_backpressure_events"]
        + report["processing_lag_backpressure_events"]
        + report["snapshot_blocking_lag_events"]
    )


def test_clean_sample_bid_ask_sorted(tmp_path) -> None:
    make_processor(tmp_path)
    row = json.loads((tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()[-1])
    bid_prices = [float(price) for price, _ in row["bids"]]
    ask_prices = [float(price) for price, _ in row["asks"]]

    assert bid_prices == sorted(bid_prices, reverse=True)
    assert ask_prices == sorted(ask_prices)
    assert row["local_recv_monotonic_ns"] is not None
    assert "local_recv_wall_ts" in row


def test_duplicate_update_skipped_without_generation_reset(tmp_path) -> None:
    processor = make_processor(tmp_path)
    state = processor.state_for("BTCUSDT")
    generation = state.generation

    result = processor.process_depth_update(
        make_depth_update(first_update_id=90, final_update_id=100)
    )

    assert result.status == "duplicate_update"
    assert state.generation == generation
    assert state.ready_to_emit is True


def test_old_update_skipped_without_sample_emit(tmp_path) -> None:
    processor = make_processor(tmp_path)
    before = processor.summary(duration_sec=1)["samples_emitted"]

    processor.process_depth_update(make_depth_update(first_update_id=80, final_update_id=90))

    assert processor.summary(duration_sec=1)["samples_emitted"] == before
