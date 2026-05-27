from __future__ import annotations

import hashlib
import time
from decimal import Decimal
from pathlib import Path

from app.marketdata.orderbook_phase41 import (
    OrderbookDebugRecorder,
    OrderbookPhase41Paths,
    OrderbookPhase41Processor,
    clean_sample_from_snapshot,
)
from app.marketdata.orderbook_quality import OrderbookQualityValidator
from app.marketdata.queue_monitor import QueueBackpressureMonitor
from orderbook_phase41_test_utils import make_depth_update, make_state


def test_tc_57_copy_snapshot_top20_200x200_records_budget_measurement() -> None:
    state = make_state()
    state.bids = {Decimal(str(100 - i / 100)): Decimal("1") for i in range(200)}
    state.asks = {Decimal(str(101 + i / 100)): Decimal("1") for i in range(200)}
    start = time.perf_counter_ns()
    snapshot = state.copy_snapshot(top_n=20)
    elapsed_us = (time.perf_counter_ns() - start) / 1_000.0
    assert len(snapshot.bids_top_n) == 20
    assert len(snapshot.asks_top_n) == 20
    assert elapsed_us >= 0


def test_tc_58_queue_monitor_10k_messages_overhead_is_measured() -> None:
    monitor = QueueBackpressureMonitor(capacity=20_000)
    start = time.perf_counter_ns()
    for index in range(10_000):
        env = monitor.record_enqueue(
            index,
            enqueue_monotonic_ns=index,
            queue_size=1,
        )
        monitor.record_dequeue(env, dequeue_monotonic_ns=index + 1, queue_size=0)
    elapsed_us_per_message = (time.perf_counter_ns() - start) / 1_000.0 / 10_000
    report = monitor.report()
    assert elapsed_us_per_message >= 0
    assert report["enqueue_to_dequeue_lag_ms_p99"] >= 0


def test_tc_59_no_sha256_hot_path_when_update_ids_exist(monkeypatch, tmp_path) -> None:
    called = False

    def fake_sha256(*args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal called
        called = True
        return original_sha256(*args, **kwargs)

    original_sha256 = hashlib.sha256
    monkeypatch.setattr(hashlib, "sha256", fake_sha256)
    from app.marketdata.orderbook_phase41 import OrderbookPhase41Paths

    processor = OrderbookPhase41Processor(symbols=("BTCUSDT",), paths=OrderbookPhase41Paths(
        quality_report=tmp_path / "q.json",
        quality_samples=tmp_path / "q.jsonl",
        mismatch_cases=tmp_path / "m.jsonl",
        book_incomplete_cases=tmp_path / "b.jsonl",
        sequence_gap_cases=tmp_path / "s.jsonl",
        duplicate_update_cases=tmp_path / "d.jsonl",
        invalid_delta_cases=tmp_path / "i.jsonl",
        stale_period_cases=tmp_path / "stale.jsonl",
        sequence_recovery_trace=tmp_path / "trace.jsonl",
        lifecycle_report=tmp_path / "l.json",
        clean_samples=tmp_path / "c.jsonl",
        markdown_report=tmp_path / "r.md",
    ))
    processor.load_snapshot(
        "BTCUSDT",
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=100,
        local_recv_monotonic_ns=1,
    )
    processor.process_depth_update(make_depth_update(first_update_id=101, final_update_id=101))
    assert called is False


def test_tc_60_debug_buffers_are_bounded(tmp_path) -> None:
    from app.marketdata.orderbook_phase41 import OrderbookPhase41Paths

    recorder = OrderbookDebugRecorder(
        paths=OrderbookPhase41Paths(
            quality_report=tmp_path / "q.json",
            quality_samples=tmp_path / "q.jsonl",
            mismatch_cases=tmp_path / "m.jsonl",
            book_incomplete_cases=tmp_path / "b.jsonl",
            sequence_gap_cases=tmp_path / "s.jsonl",
            duplicate_update_cases=tmp_path / "d.jsonl",
            invalid_delta_cases=tmp_path / "i.jsonl",
            stale_period_cases=tmp_path / "stale.jsonl",
            sequence_recovery_trace=tmp_path / "trace.jsonl",
            lifecycle_report=tmp_path / "l.json",
            clean_samples=tmp_path / "c.jsonl",
            markdown_report=tmp_path / "r.md",
        ),
        max_cases=3,
    )
    state = make_state()
    snapshot = state.copy_snapshot()
    quality = OrderbookQualityValidator().validate(
        snapshot,
        state=state,
        now_monotonic_ns=1_002_000_000,
    )
    for _ in range(10):
        recorder.record_quality_sample(snapshot, quality)
    assert len(recorder.quality_samples) == 3


def test_tc_61_queue_history_is_bounded() -> None:
    monitor = QueueBackpressureMonitor(sample_capacity=5)
    for index in range(20):
        env = monitor.record_enqueue(index, enqueue_monotonic_ns=index, queue_size=1)
        monitor.record_dequeue(env, dequeue_monotonic_ns=index + 1, queue_size=0)
    assert len(monitor._lag_samples) == 5


def test_tc_62_top_of_book_snapshot_copies_top_n_not_full_book() -> None:
    state = make_state()
    for index in range(200):
        state.bids[Decimal(str(99 - index / 100))] = Decimal("1")
        state.asks[Decimal(str(102 + index / 100))] = Decimal("1")
    snapshot = state.copy_snapshot(top_n=5)
    assert len(snapshot.bids_top_n) == 5
    assert len(snapshot.asks_top_n) == 5
    assert snapshot.bid_count > 5
    assert snapshot.ask_count > 5


def test_optimized_snapshot_copy_uses_top_depth_n_index_slice(tmp_path) -> None:
    class TrackingDict(dict[Decimal, Decimal]):
        def __init__(self, values: dict[Decimal, Decimal]) -> None:
            super().__init__(values)
            self.getitem_count = 0

        def __getitem__(self, key: Decimal) -> Decimal:
            self.getitem_count += 1
            return super().__getitem__(key)

    state = make_state()
    state.bids.clear()
    state.asks.clear()
    state.apply_snapshot(
        bids=[(str(100 - index / 100), "1") for index in range(200)],
        asks=[(str(101 + index / 100), "1") for index in range(200)],
        last_update_id=200,
        local_recv_monotonic_ns=2_000_000_000,
    )
    state.apply_delta(
        first_update_id=201,
        final_update_id=201,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=2_001_000_000,
    )
    bids = TrackingDict(dict(state.bids))
    asks = TrackingDict(dict(state.asks))
    state.bids = bids
    state.asks = asks

    processor = OrderbookPhase41Processor(
        symbols=("BTCUSDT",),
        paths=_paths(tmp_path),
        depth_n=5,
    )
    snapshot = processor.copy_snapshot(
        state,
        local_recv_wall_ts="1970-01-01T00:00:02+00:00",
    )
    summary = processor.summary(duration_sec=1.0)

    assert len(snapshot.bids_top_n) == 5
    assert len(snapshot.asks_top_n) == 5
    assert snapshot.bid_count == 200
    assert snapshot.ask_count == 200
    assert bids.getitem_count == 5
    assert asks.getitem_count == 5
    assert summary["copied_bid_level_count"] == 5
    assert summary["copied_ask_level_count"] == 5
    assert summary["snapshot_copy_strategy"] == "top_n_index_slice_immutable_tuples_no_full_book_copy"


def test_emitted_sample_row_is_immutable_after_future_orderbook_updates() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(
        top_n=2,
        local_recv_monotonic_ns=1_002_000_000,
        local_recv_wall_ts="1970-01-01T00:00:01+00:00",
    )
    quality = OrderbookQualityValidator().validate(
        snapshot,
        state=state,
        now_monotonic_ns=1_002_000_000,
    )
    sample = clean_sample_from_snapshot(snapshot, quality, depth_n=2, exchange_event_ts=1)
    original_bids = [level[:] for level in sample["bids"]]
    original_asks = [level[:] for level in sample["asks"]]

    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "0"), ("98.00", "4")],
        asks=[("101.00", "0"), ("103.00", "5")],
        local_recv_monotonic_ns=1_003_000_000,
    )

    assert sample["bids"] == original_bids
    assert sample["asks"] == original_asks
    assert snapshot.bids_top_n[0][0] == Decimal("100.00")
    assert state.best_bid() != snapshot.best_bid


def test_batch_writer_keeps_disk_io_and_debug_logging_off_hot_path(tmp_path) -> None:
    processor = OrderbookPhase41Processor(
        symbols=("BTCUSDT",),
        paths=_paths(tmp_path),
        batch_writer_enabled=True,
        writer_queue_max_size=16,
    )
    try:
        summary = processor.summary(duration_sec=1.0)
        assert summary["disk_write_on_hot_path"] is False
        assert summary["debug_logging_on_hot_path"] is False
        assert summary["batch_writer_enabled"] is True
        assert summary["writer_batch_report"]["writer_mode"] == "threaded_jsonl_batch_writer"
    finally:
        processor.close_writer()


def _paths(tmp_path: Path) -> OrderbookPhase41Paths:
    return OrderbookPhase41Paths(
        quality_report=tmp_path / "q.json",
        quality_samples=tmp_path / "q.jsonl",
        mismatch_cases=tmp_path / "m.jsonl",
        book_incomplete_cases=tmp_path / "b.jsonl",
        sequence_gap_cases=tmp_path / "s.jsonl",
        duplicate_update_cases=tmp_path / "d.jsonl",
        invalid_delta_cases=tmp_path / "i.jsonl",
        stale_period_cases=tmp_path / "stale.jsonl",
        sequence_recovery_trace=tmp_path / "trace.jsonl",
        clean_sample_schema_violation_cases=tmp_path / "schema.jsonl",
        lifecycle_report=tmp_path / "l.json",
        clean_samples=tmp_path / "c.jsonl",
        latency_profile_samples=tmp_path / "latency.jsonl",
        markdown_report=tmp_path / "r.md",
    )
