from __future__ import annotations

from datetime import UTC, datetime

import orjson
import pytest

from app.core.events import MarketTick, PolymarketQuote
from app.main import GapRuntimeSummary, RuntimeSummaryJsonlWriter
from app.marketdata.market_universe import (
    build_market_universe_diff,
    select_runtime_market_universe,
)
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.marketdata.polymarket_ws import PolymarketWSClient
from app.state.market_state import MarketState
from app.strategy.gap_detector import GapDetector, GapMonitorStats


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")


def _market(
    market_id: str,
    *,
    start_ts: int,
    duration_s: int = 300,
    base_asset: str = "BTC",
    closed: bool = False,
) -> PolymarketMarketMetadata:
    up = f"{market_id}-up"
    down = f"{market_id}-down"
    duration_label = "5m" if duration_s == 300 else "15m"
    return PolymarketMarketMetadata(
        condition_id=f"{market_id}-condition",
        market_id=market_id,
        market_slug=f"{base_asset.lower()}-updown-{duration_label}-{start_ts}",
        question=f"{base_asset} Up or Down",
        event_start_time=_iso(start_ts),
        end_time=_iso(start_ts + duration_s),
        up_token_id=up,
        down_token_id=down,
        token_outcomes={up: "Up", down: "Down"},
        tick_size=0.01,
        min_order_size=5.0,
        active=True,
        closed=closed,
        accepting_orders=True,
        enable_order_book=True,
        base_asset=base_asset,
        duration_minutes=duration_s // 60,
    )


def _quote(token_id: str, market_id: str, *, ts: int) -> PolymarketQuote:
    return PolymarketQuote(
        market_id=market_id,
        token_id=token_id,
        side_label="UP",
        best_bid=0.49,
        best_bid_size=100.0,
        best_ask=0.51,
        best_ask_size=100.0,
        mid_price=0.50,
        spread=0.02,
        event_ts=ts,
        received_ts=ts,
        local_received_ts=ts,
        book_complete=True,
    )


def _stats(**overrides: object) -> GapMonitorStats:
    base = dict(
        detected_gaps=0,
        completed_gaps=0,
        fillable_at_detection_count=0,
        non_fillable_at_detection_count=0,
        median_mid_repricing_delay_ms=None,
        p95_mid_repricing_delay_ms=None,
        median_executable_repricing_delay_ms=None,
        p95_executable_repricing_delay_ms=None,
        median_tradable_window_ms=None,
        p95_tradable_window_ms=None,
        average_estimated_edge=None,
        reject_count_by_reason={},
        reject_count_by_stage={},
        stale_feed_count=0,
    )
    base.update(overrides)
    return GapMonitorStats(**base)  # type: ignore[arg-type]


def test_rolling_market_rotation_removes_expired_and_promotes_next() -> None:
    current = _market("current", start_ts=900)
    next_market = _market("next", start_ts=1200)
    future = _market("future", start_ts=1500)

    before = select_runtime_market_universe(
        (current, next_market, future),
        now_ts=1_000,
        lookahead_windows=2,
    )
    after = select_runtime_market_universe(
        (current, next_market, future),
        now_ts=1_201,
        lookahead_windows=2,
    )
    diff = build_market_universe_diff(before, after, now_ts=1_201)

    assert [market.market_id for market in before] == ["current", "next", "future"]
    assert [market.market_id for market in after][:2] == ["next", "future"]
    assert after[0].runtime_selection_reason == "current_signal"
    assert diff.expired_markets[0]["market_id"] == "current"


def test_closed_market_is_removed_from_runtime_universe() -> None:
    open_market = _market("open", start_ts=900)
    closed_market = _market("closed", start_ts=900, closed=True)

    selected = select_runtime_market_universe(
        (open_market, closed_market),
        now_ts=1_000,
        lookahead_windows=2,
    )

    assert [market.market_id for market in selected] == ["open"]


def test_gap_detector_update_markets_closes_removed_pending_observation() -> None:
    market = _market("old", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    base_ns = 1_000_000_000_000
    state.apply(_quote(market.up_token_id or "", market.market_id, ts=base_ns))
    first = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=100.0,
            size=1.0,
            exchange_event_ts=base_ns,
            local_received_ts=base_ns,
        )
    )
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=base_ns + 1_000_000_000,
            local_received_ts=base_ns + 1_000_000_000,
        )
    )
    assert second is not None
    detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)

    detector.update_markets(())
    closed = detector.drain_market_update_observations()

    assert len(closed) == 1
    assert closed[0].reject_stage == "lifecycle"
    assert closed[0].reject_reason == "market_expired"
    assert detector.stats(state).pending_observation_count == 0


def test_polymarket_ws_update_markets_preserves_active_book_and_adds_tokens() -> None:
    old_market = _market("old", start_ts=900)
    new_market = _market("new", start_ts=1_200)
    client = PolymarketWSClient(markets=(old_market,), mismatch_sample_path=None)
    client.normalize_message(
        orjson.dumps(
            {
                "event_type": "book",
                "asset_id": old_market.up_token_id,
                "market": old_market.market_id,
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.51", "size": "10"}],
            }
        ),
        received_ts=1_000_000_000,
    )

    client.update_markets((old_market, new_market))
    readiness = client.book_readiness_snapshot(now_ts=1_001_000_000)

    assert client.active_ws_token_subscription_count == 4
    assert set(client.token_ids) == {
        old_market.up_token_id,
        old_market.down_token_id,
        new_market.up_token_id,
        new_market.down_token_id,
    }
    assert readiness["tokens"][old_market.up_token_id]["first_book_snapshot_ts_ns"] == 1_000_000_000
    assert new_market.up_token_id in readiness["tokens"]
    assert client.subscription_diagnostics()["subscription_update_count"] == 1


def test_no_event_warning_reports_no_signal_while_moves_continue() -> None:
    summary = GapRuntimeSummary(())
    summary._last_gap_event_change_ts_ns = 1  # noqa: SLF001
    stats = _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1})
    summary.record_binance_event(
        MarketTick(source="binance", symbol="BTCUSDT", price=100.0, size=1.0)
    )

    payload = summary.snapshot_payload(
        stats,
        {"markets": []},
        ws_diagnostics={"runtime_token_count": 0, "active_ws_token_subscription_count": 0},
    )

    assert "no_signal_enabled_markets_while_binance_moves_continue" in payload["no_event_warnings"]


def test_runtime_summary_jsonl_writes_utf8_jsonl(tmp_path) -> None:
    path = tmp_path / "runtime.jsonl"
    writer = RuntimeSummaryJsonlWriter(str(path))

    writer.write({"event_type": "runtime_summary", "current_market_slugs_by_base_asset": {"BTC": ["btc"]}})

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert row["event_type"] == "runtime_summary"
    assert row["current_market_slugs_by_base_asset"]["BTC"] == ["btc"]


def test_missing_quote_age_fields_are_not_rendered_as_zero(tmp_path) -> None:
    from app.backtest.dataset_quality_phase4 import build_phase4_dataset_quality_report

    path = tmp_path / "gap_events.jsonl"
    path.write_bytes(
        orjson.dumps(
            {
                "event_type": "tradable_gap_observation",
                "symbol": "BTCUSDT",
                "market_id": "m",
                "token_id": "t",
                "direction": "UP",
                "binance_move_pct": 1.0,
                "detected_ts_ns": 1,
                "quote_was_fillable": False,
                "reject_stage": "pre_entry",
                "reject_reason": "missing_quote",
                "validation_mode": "tolerant",
                "data_quality_tier": "D",
            },
            option=orjson.OPT_APPEND_NEWLINE,
        )
    )

    report = build_phase4_dataset_quality_report(path)

    assert report["stale_feed_analysis"]["staleness_status"] == "unknown_missing_quote_age_fields"
    assert report["stale_feed_analysis"]["quote_stale_rate"] is None
