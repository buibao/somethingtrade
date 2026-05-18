from __future__ import annotations

import asyncio
import json

from app.marketdata.orderbook_quality import OrderbookQualityValidator
from orderbook_phase41_test_utils import make_processor, make_state


def test_tc_51_snapshot_immutable_after_later_update() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(top_n=20)
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.50", "1")],
        asks=[],
        local_recv_monotonic_ns=2,
    )
    assert snapshot.best_bid != state.best_bid()
    assert snapshot.best_bid == snapshot.bids_top_n[0][0]


def test_tc_52_sample_fields_come_from_same_snapshot_version(tmp_path) -> None:
    processor = make_processor(tmp_path)
    row = json.loads((tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()[-1])
    assert row["snapshot_version"] == row["state_version"]


def test_tc_53_debug_writer_uses_passed_snapshot_not_live_state(tmp_path) -> None:
    processor = make_processor(tmp_path)
    state = processor.state_for("BTCUSDT")
    snapshot = state.copy_snapshot(top_n=20)
    quality = OrderbookQualityValidator().validate(
        snapshot,
        state=state,
        now_monotonic_ns=1_002_000_000,
        reported_best_bid="99.50",
    )
    state.apply_delta(
        first_update_id=102,
        final_update_id=102,
        bids=[("100.50", "1")],
        asks=[],
        local_recv_monotonic_ns=3,
    )
    processor.debug.record_mismatch_case(snapshot, quality)
    row = json.loads((tmp_path / "orderbook_mismatch_cases.jsonl").read_text().splitlines()[-1])
    assert row["computed_best_bid"] == "100.00"
    assert row["snapshot_version"] == snapshot.snapshot_version


def test_tc_54_concurrent_async_snapshot_access_stays_unchanged() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(top_n=20)

    async def mutate() -> None:
        await asyncio.sleep(0)
        state.apply_delta(
            first_update_id=102,
            final_update_id=102,
            bids=[("100.75", "1")],
            asks=[],
            local_recv_monotonic_ns=3,
        )

    async def run() -> None:
        await mutate()

    asyncio.run(run())
    assert snapshot.best_bid == snapshot.bids_top_n[0][0]
    assert snapshot.best_bid != state.best_bid()


def test_tc_55_snapshot_version_equals_state_version_at_copy_time() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(top_n=20)
    assert snapshot.snapshot_version == state.state_version
    assert snapshot.state_version == state.state_version


def test_tc_56_state_version_is_monotonic() -> None:
    state = make_state()
    versions = [state.state_version]
    for index in range(102, 110):
        state.apply_delta(
            first_update_id=index,
            final_update_id=index,
            bids=[(f"100.{index}", "1")],
            asks=[],
            local_recv_monotonic_ns=index,
        )
        versions.append(state.state_version)
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)
