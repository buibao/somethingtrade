from __future__ import annotations

from app.marketdata.orderbook_quality import OrderbookQualityValidator
from app.marketdata.orderbook_state import OrderbookState
from orderbook_phase41_test_utils import dec, make_state


def _validate_state(state: OrderbookState):
    return OrderbookQualityValidator().validate(
        state.copy_snapshot(top_n=20, local_recv_monotonic_ns=1_002_000_000),
        state=state,
        now_monotonic_ns=1_002_000_000,
    )


def test_tc_01_valid_two_sided_snapshot_computes_best_and_spread() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(top_n=20)
    result = _validate_state(state)
    assert result.is_valid
    assert snapshot.best_bid == dec("100.00")
    assert snapshot.best_ask == dec("101.00")
    assert snapshot.spread == dec("1.00")


def test_tc_02_empty_book_is_invalid_book_empty() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(bids=[], asks=[], last_update_id=1, local_recv_monotonic_ns=1)
    result = OrderbookQualityValidator().validate(state.copy_snapshot(), now_monotonic_ns=1)
    assert not result.is_valid
    assert "book_empty" in result.errors


def test_tc_03_missing_bid_side_is_split_root_cause() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[],
        asks=[("101", "1")],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    result = OrderbookQualityValidator().validate(state.copy_snapshot(), now_monotonic_ns=1)
    assert {"one_side_missing", "best_bid_missing"} <= set(result.errors)


def test_tc_04_missing_ask_side_is_split_root_cause() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    result = OrderbookQualityValidator().validate(state.copy_snapshot(), now_monotonic_ns=1)
    assert {"one_side_missing", "best_ask_missing"} <= set(result.errors)


def test_tc_05_crossed_book_is_invalid() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("101", "1")],
        asks=[("100", "1")],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    result = OrderbookQualityValidator().validate(state.copy_snapshot(), now_monotonic_ns=1)
    assert not result.is_valid
    assert "crossed_book" in result.errors


def test_tc_06_insert_new_best_bid_updates_best_bid() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.50", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.best_bid() == dec("100.50")


def test_tc_07_insert_new_best_ask_updates_best_ask() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[],
        asks=[("100.75", "1")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.best_ask() == dec("100.75")


def test_tc_08_update_existing_level_size_keeps_price() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "3.25")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.bids[dec("100.00")] == dec("3.25")


def test_tc_09_zero_size_removes_bid_level() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "0")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert dec("100.00") not in state.bids


def test_tc_10_zero_size_removes_ask_level() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[],
        asks=[("101.00", "0")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert dec("101.00") not in state.asks


def test_tc_11_zero_size_missing_level_does_not_insert_zero() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("95.00", "0")],
        asks=[("110.00", "0")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert dec("95.00") not in state.bids
    assert dec("110.00") not in state.asks


def test_tc_12_negative_size_rejected_and_classified() -> None:
    state = make_state()
    version = state.state_version
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "-1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert not result.accepted
    assert {"negative_size", "invalid_size_level"} <= set(result.errors)
    assert state.state_version == version


def test_tc_13_negative_price_rejected_and_classified() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("-1", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert not result.accepted
    assert {"negative_price", "invalid_price_level"} <= set(result.errors)


def test_tc_14_nan_price_size_rejected_and_classified() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("NaN", "1")],
        asks=[("101", "NaN")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert not result.accepted
    assert {"non_finite_price", "non_finite_size"} <= set(result.errors)


def test_tc_15_duplicate_price_in_snapshot_uses_deterministic_last_write() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1"), ("100", "2")],
        asks=[("101", "1")],
        last_update_id=1,
        local_recv_monotonic_ns=1,
    )
    assert state.bids[dec("100")] == dec("2")


def test_tc_16_best_bid_after_current_best_removed() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "0")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.best_bid() == dec("99.00")


def test_tc_17_best_ask_after_current_best_removed() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[],
        asks=[("101.00", "0")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.best_ask() == dec("102.00")


def test_tc_18_multiple_level_removals_recalculate_best() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "0"), ("99.00", "0"), ("98.00", "1")],
        asks=[("101.00", "0"), ("102.00", "0"), ("103.00", "1")],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert state.best_bid() == dec("98.00")
    assert state.best_ask() == dec("103.00")


def test_tc_19_snapshot_copy_has_no_zero_size_levels() -> None:
    state = make_state()
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.00", "0")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    snapshot = state.copy_snapshot(top_n=20)
    assert all(size != dec("0") for _price, size in snapshot.bids_top_n)


def test_tc_20_state_version_increments_once_per_accepted_mutation() -> None:
    state = make_state()
    before = state.state_version
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.25", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert result.accepted
    assert state.state_version == before + 1


def test_tc_21_rejected_delta_does_not_increment_state_version() -> None:
    state = make_state()
    before = state.state_version
    result = state.apply_delta(
        first_update_id=105,
        final_update_id=105,
        bids=[("100.25", "1")],
        asks=[],
        local_recv_monotonic_ns=1_002_000_000,
    )
    assert not result.accepted
    assert state.state_version == before
