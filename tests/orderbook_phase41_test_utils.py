from __future__ import annotations

from decimal import Decimal

from app.core.events import BookLevel, DepthUpdate
from app.marketdata.orderbook_phase41 import OrderbookPhase41Processor
from app.marketdata.orderbook_state import OrderbookState


def make_state(*, last_update_id: int = 100) -> OrderbookState:
    state = OrderbookState("BTCUSDT", ready_false_warning_after_sec=0.001)
    result = state.apply_snapshot(
        bids=[("100.00", "1.0"), ("99.00", "2.0")],
        asks=[("101.00", "1.5"), ("102.00", "2.5")],
        last_update_id=last_update_id,
        local_recv_monotonic_ns=1_000_000_000,
    )
    assert result.accepted
    bridge = state.apply_delta(
        first_update_id=last_update_id + 1,
        final_update_id=last_update_id + 1,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=1_001_000_000,
    )
    assert bridge.accepted
    assert state.ready_to_emit
    return state


def make_depth_update(
    *,
    first_update_id: int,
    final_update_id: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    recv_monotonic_ns: int = 1_002_000_000,
) -> DepthUpdate:
    return DepthUpdate(
        source="binance",
        symbol="BTCUSDT",
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        bids=[BookLevel(price=float(price), size=float(size)) for price, size in (bids or [])],
        asks=[BookLevel(price=float(price), size=float(size)) for price, size in (asks or [])],
        recv_monotonic_ns=recv_monotonic_ns,
        exchange_event_ts=1_700_000_000_000_000_000,
    )


def make_processor(tmp_path) -> OrderbookPhase41Processor:
    from app.marketdata.orderbook_phase41 import OrderbookPhase41Paths

    paths = OrderbookPhase41Paths(
        quality_report=tmp_path / "orderbook_quality_report.json",
        quality_samples=tmp_path / "orderbook_quality_samples.jsonl",
        mismatch_cases=tmp_path / "orderbook_mismatch_cases.jsonl",
        book_incomplete_cases=tmp_path / "book_incomplete_cases.jsonl",
        sequence_gap_cases=tmp_path / "sequence_gap_cases.jsonl",
        duplicate_update_cases=tmp_path / "duplicate_update_cases.jsonl",
        lifecycle_report=tmp_path / "ws_lifecycle_report.json",
        clean_samples=tmp_path / "orderbook_clean_samples.jsonl",
        markdown_report=tmp_path / "phase_4_1_orderbook_quality_report.md",
    )
    processor = OrderbookPhase41Processor(symbols=("BTCUSDT",), paths=paths)
    processor.load_snapshot(
        "BTCUSDT",
        bids=[("100.00", "1.0"), ("99.00", "2.0")],
        asks=[("101.00", "1.5"), ("102.00", "2.5")],
        last_update_id=100,
        local_recv_monotonic_ns=1_000_000_000,
    )
    processor.process_depth_update(
        make_depth_update(first_update_id=101, final_update_id=101)
    )
    return processor


def dec(value: str) -> Decimal:
    return Decimal(value)
