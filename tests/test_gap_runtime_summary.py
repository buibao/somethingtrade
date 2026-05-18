from __future__ import annotations

from app.core.events import MarketTick
from app.main import GapRuntimeSummary, parse_args
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.state.market_state import MarketState
from app.strategy.gap_detector import GapDetector, GapMonitorStats


def _market(
    *,
    market_id: str = "btc-market",
    base_asset: str = "BTC",
    symbol_slug: str = "bitcoin",
    up_token_id: str = "btc-up-token",
    down_token_id: str = "btc-down-token",
) -> PolymarketMarketMetadata:
    return PolymarketMarketMetadata(
        condition_id=f"{market_id}-condition",
        market_id=market_id,
        market_slug=f"{symbol_slug}-updown-15m",
        question=f"{base_asset} Up or Down - 15 minute",
        end_time="2099-05-15T12:15:00Z",
        event_start_time="2000-01-01T00:00:00Z",
        up_token_id=up_token_id,
        down_token_id=down_token_id,
        token_outcomes={up_token_id: "Up", down_token_id: "Down"},
        tick_size=0.01,
        min_order_size=5.0,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
        classification="current_signal",
        selected_for_runtime=True,
        signal_enabled=True,
        base_asset=base_asset,
        duration_minutes=15,
    )


def test_gap_monitor_runtime_summary_interval_arg() -> None:
    args = parse_args(["gap-monitor", "--runtime-summary-interval-ms", "5000"])

    assert args.runtime_summary_interval_ms == 5000


def test_gap_runtime_summary_formats_required_counters() -> None:
    market = _market()
    summary = GapRuntimeSummary((market,))
    summary.record_binance_event(
        MarketTick(source="binance", symbol="BTCUSDT", price=100.0, size=1.0)
    )
    stats = GapMonitorStats(
        detected_gaps=1,
        completed_gaps=1,
        fillable_at_detection_count=0,
        non_fillable_at_detection_count=1,
        median_mid_repricing_delay_ms=None,
        p95_mid_repricing_delay_ms=None,
        median_executable_repricing_delay_ms=None,
        p95_executable_repricing_delay_ms=None,
        median_tradable_window_ms=None,
        p95_tradable_window_ms=None,
        average_estimated_edge=None,
        reject_count_by_reason={"missing_quote": 1},
        reject_count_by_stage={"pre_entry": 1},
        stale_feed_count=0,
        binance_moves_detected_by_symbol={"BTCUSDT": 1},
        candidates_created_by_symbol={"BTCUSDT": 1},
        pre_entry_rejects_by_symbol={"BTCUSDT": 1},
        top_reject_reasons_by_symbol={"BTCUSDT": {"missing_quote": 1}},
    )
    readiness = {
        "markets": [
            {
                "market_id": market.market_id,
                "signal_enabled_at_now": True,
                "up_token_id": market.up_token_id,
                "down_token_id": market.down_token_id,
                "up_token_book_complete": True,
                "down_token_book_complete": False,
            }
        ]
    }

    text = summary.format(stats, readiness)

    assert "binance_events_seen_by_symbol=BTCUSDT:1" in text
    assert "binance_moves_detected_by_symbol=BTCUSDT:1" in text
    assert "runtime_markets_selected_by_base_asset=BTC:1" in text
    assert "signal_enabled_markets_by_base_asset=BTC:1" in text
    assert "book_ready_tokens_by_base_asset=BTC:1/2" in text
    assert "candidates_created_by_symbol=BTCUSDT:1" in text
    assert "pre_entry_rejects_by_symbol=BTCUSDT:1" in text
    assert "top_reject_reasons_by_symbol=BTCUSDT[missing_quote:1]" in text


def test_gap_detector_runtime_counters_do_not_change_candidate_semantics() -> None:
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        (market,),
        min_move_pct=0.1,
        require_book_ready=False,
        pre_entry_log_cooldown_ms=0.0,
    )
    first_ts = 1_700_000_000_000_000_000
    second_ts = first_ts + 1_000_000_000
    first = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=100.0,
            size=1.0,
            exchange_event_ts=first_ts,
            local_received_ts=first_ts,
        )
    )
    assert first is not None
    detector.on_market_event(first, state, now_ts=first_ts)
    second = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=second_ts,
            local_received_ts=second_ts,
        )
    )
    assert second is not None

    observations = detector.on_market_event(second, state, now_ts=second_ts)
    stats = detector.stats(state, now_ts=second_ts)

    assert len(observations) == 1
    assert observations[0].reject_reason == "missing_quote"
    assert stats.binance_moves_detected_by_symbol == {"BTCUSDT": 1}
    assert stats.candidates_created_by_symbol == {"BTCUSDT": 1}
    assert stats.pre_entry_rejects_by_symbol == {"BTCUSDT": 1}
    assert stats.top_reject_reasons_by_symbol == {"BTCUSDT": {"missing_quote": 1}}
