from __future__ import annotations

import random

from app.marketdata.orderbook_quality import OrderbookQualityValidator
from app.marketdata.orderbook_state import OrderbookState
from orderbook_phase41_test_utils import make_state


def test_tc_63_random_structurally_valid_deltas_do_not_crash() -> None:
    random.seed(41)
    state = make_state()
    next_update = 102
    for _ in range(1000):
        side = random.choice(["bid", "ask"])
        price = (
            99 + random.random()
            if side == "bid"
            else 101 + random.random()
        )
        level = [(f"{price:.2f}", f"{1 + random.random():.4f}")]
        result = state.apply_delta(
            first_update_id=next_update,
            final_update_id=next_update,
            bids=level if side == "bid" else [],
            asks=level if side == "ask" else [],
            local_recv_monotonic_ns=next_update,
        )
        assert result.accepted
        next_update += 1


def test_tc_64_random_invalid_prices_sizes_are_classified_no_crash() -> None:
    state = make_state()
    invalid_values = ["NaN", "Infinity", "-1", "bad"]
    for offset, value in enumerate(invalid_values, start=102):
        result = state.apply_delta(
            first_update_id=offset,
            final_update_id=offset,
            bids=[(value, "1")],
            asks=[("101", value)],
            local_recv_monotonic_ns=offset,
        )
        assert not result.accepted
        assert result.errors


def test_tc_65_random_zero_size_removals_leave_no_zero_active_levels() -> None:
    random.seed(42)
    state = make_state()
    next_update = 102
    for _ in range(100):
        price = random.choice(["100.00", "99.00", "98.00", "97.00"])
        state.apply_delta(
            first_update_id=next_update,
            final_update_id=next_update,
            bids=[(price, "0")],
            asks=[],
            local_recv_monotonic_ns=next_update,
        )
        next_update += 1
        if not state.snapshot_ready:
            break
    assert all(size != 0 for size in state.bids.values())


def test_tc_66_random_sequence_gaps_and_recovery_bridge_violations_block_readiness() -> None:
    state = make_state()
    gap = state.apply_delta(
        first_update_id=110,
        final_update_id=110,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert not gap.accepted
    assert state.ready_to_emit is False
    assert state.generation > 0
    stale_snapshot = state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=100,
        local_recv_monotonic_ns=4,
    )
    assert not stale_snapshot.accepted
    fresh_snapshot = state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=111,
        local_recv_monotonic_ns=5,
    )
    assert fresh_snapshot.accepted
    bad_bridge = state.apply_delta(
        first_update_id=120,
        final_update_id=121,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=6,
    )
    assert not bad_bridge.accepted
    assert bad_bridge.status == "sequence_bridge_failed"
    assert state.ready_to_emit is False


def test_tc_67_random_duplicate_updates_are_skipped() -> None:
    state = make_state()
    version = state.state_version
    for _ in range(10):
        result = state.apply_delta(
            first_update_id=101,
            final_update_id=101,
            bids=[("100.50", "1")],
            asks=[],
            local_recv_monotonic_ns=3,
        )
        assert result.status == "duplicate_update"
        assert state.state_version == version


def test_tc_68_random_updates_never_produce_clean_crossed_book() -> None:
    random.seed(43)
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    state.apply_delta(
        first_update_id=2,
        final_update_id=2,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=2,
    )
    validator = OrderbookQualityValidator()
    next_update = 3
    for _ in range(100):
        bid = 101 + random.random()
        state.apply_delta(
            first_update_id=next_update,
            final_update_id=next_update,
            bids=[(f"{bid:.4f}", "1")],
            asks=[],
            local_recv_monotonic_ns=next_update,
        )
        result = validator.validate(
            state.copy_snapshot(),
            state=state,
            now_monotonic_ns=next_update,
        )
        if "crossed_book" in result.errors:
            assert not result.is_valid
            assert state.ready_to_emit is False or not result.is_valid
            break
        next_update += 1
