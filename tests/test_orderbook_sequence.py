from __future__ import annotations

import json

from app.marketdata.orderbook_state import OrderbookState
from orderbook_phase41_test_utils import make_processor, make_state, make_depth_update


def test_tc_32_drop_delta_where_final_update_not_newer_than_snapshot() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=100,
        local_recv_monotonic_ns=1,
    )
    result = state.apply_delta(
        first_update_id=90,
        final_update_id=100,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=2,
    )
    assert not result.accepted
    assert result.status == "duplicate_update"


def test_tc_33_first_delta_bridges_snapshot() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=100,
        local_recv_monotonic_ns=1,
    )
    result = state.apply_delta(
        first_update_id=99,
        final_update_id=101,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=2,
    )
    assert result.accepted
    assert state.ready_to_emit


def test_tc_34_first_delta_that_does_not_bridge_marks_not_ready() -> None:
    state = OrderbookState("BTCUSDT")
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=100,
        local_recv_monotonic_ns=1,
    )
    result = state.apply_delta(
        first_update_id=105,
        final_update_id=110,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=2,
    )
    assert not result.accepted
    assert result.status == "sequence_bridge_failed"
    assert not state.snapshot_ready
    assert not state.ready_to_emit


def test_tc_35_continuous_next_delta_is_accepted() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.25", "1")],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert result.accepted
    assert state.last_update_id == 102


def test_tc_36_sequence_gap_blocks_readiness() -> None:
    state = make_state()
    result = state.apply_delta(
        first_update_id=105,
        final_update_id=105,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert not result.accepted
    assert result.status == "sequence_gap_or_reset"
    assert not state.snapshot_ready
    assert not state.ready_to_emit


def test_tc_37_old_update_is_skipped() -> None:
    state = make_state()
    version = state.state_version
    result = state.apply_delta(
        first_update_id=100,
        final_update_id=101,
        bids=[("100.50", "1")],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert result.status == "duplicate_update"
    assert state.state_version == version


def test_tc_38_duplicate_same_final_update_id_is_skipped_without_mutation() -> None:
    state = make_state()
    version = state.state_version
    result = state.apply_delta(
        first_update_id=101,
        final_update_id=101,
        bids=[("100.50", "1")],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert not result.accepted
    assert state.state_version == version


def test_tc_39_gap_recovery_requires_fresh_snapshot_and_valid_bridge() -> None:
    state = make_state()
    gap = state.apply_delta(
        first_update_id=105,
        final_update_id=110,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    assert not gap.accepted
    stale_snapshot = state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=109,
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
    bridge = state.apply_delta(
        first_update_id=112,
        final_update_id=112,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=6,
    )
    assert bridge.accepted
    assert state.ready_to_emit


def test_tc_40_gap_does_not_emit_clean_sample(tmp_path) -> None:
    processor = make_processor(tmp_path)
    result = processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=105)
    )
    assert not result.accepted
    rows = (tmp_path / "orderbook_clean_samples.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1


def test_tc_41_gap_debug_case_includes_expected_ids(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=110)
    )
    row = json.loads((tmp_path / "sequence_gap_cases.jsonl").read_text().splitlines()[-1])
    assert row["previous_last_update_id"] == 101
    assert row["expected_next_update_id"] == 102
    assert row["received_first_update_id"] == 105
    assert row["received_final_update_id"] == 110
    assert row["state_version_before"] == row["state_version_after"]
