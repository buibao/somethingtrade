import orjson
import pytest

from app.core.clock import utc_now_ns
from app.core.events import GapEvent, MarketTick, OrderBookTop, PolymarketQuote
from app.logging.event_logger import AsyncJsonlEventLogger
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.state.market_state import MarketState
from app.strategy.gap_detector import GapDetector, build_move_snapshot


def _market() -> PolymarketMarketMetadata:
    return PolymarketMarketMetadata(
        condition_id="0xcondition",
        market_id="0xmarket",
        market_slug="bitcoin-up-or-down-15m",
        question="Bitcoin Up or Down - 15 minute",
        end_time="2026-05-15T12:15:00Z",
        yes_token_id="yes-token",
        no_token_id="no-token",
        tick_size=0.01,
        min_order_size=5.0,
        base_asset="BTC",
        duration_minutes=15,
    )


def _quote(*, mid: float, ts: int) -> PolymarketQuote:
    return PolymarketQuote(
        market_id="0xmarket",
        condition_id="0xcondition",
        token_id="yes-token",
        side_label="YES",
        best_bid=mid - 0.01,
        best_ask=mid + 0.01,
        mid_price=mid,
        spread=0.02,
        available_liquidity_at_best=100.0,
        event_ts=ts,
        received_ts=ts,
        exchange_event_ts=ts,
        local_received_ts=ts,
    )


def test_gap_detector_logs_delayed_polymarket_repricing() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.10,
        reprice_threshold=0.01,
        stale_feed_ms=60_000.0,
    )

    initial_quote = state.apply(_quote(mid=0.50, ts=base_ts + 10_000_000))
    assert isinstance(initial_quote, PolymarketQuote)

    first_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=100.0,
            size=1.0,
            exchange_event_ts=base_ts,
            local_received_ts=base_ts,
        )
    )
    assert isinstance(first_tick, MarketTick)
    assert detector.on_market_event(first_tick, state, now_ts=base_ts) == ()

    state.apply(
        OrderBookTop(
            source="binance",
            symbol="BTCUSDT",
            bid_price=99.99,
            bid_size=1.0,
            ask_price=100.01,
            ask_size=1.0,
            local_received_ts=base_ts,
        )
    )
    second_ts = base_ts + 1_000_000_000
    second_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=second_ts,
            local_received_ts=second_ts,
        )
    )

    assert isinstance(second_tick, MarketTick)
    assert detector.on_market_event(second_tick, state, now_ts=second_ts) == ()
    assert detector.stats(state, now_ts=second_ts).detected_gaps == 1

    repriced_ts = second_ts + 250_000_000
    repriced_quote = state.apply(_quote(mid=0.53, ts=repriced_ts))
    assert isinstance(repriced_quote, PolymarketQuote)

    gaps = detector.on_market_event(repriced_quote, state, now_ts=repriced_ts)

    assert len(gaps) == 1
    gap = gaps[0]
    assert gap.symbol == "BTCUSDT"
    assert gap.timeframe == "15m"
    assert gap.direction == "UP"
    assert gap.binance_move_pct == pytest.approx(1.0)
    assert gap.poly_market_price_before == pytest.approx(0.50)
    assert gap.poly_market_price_after == pytest.approx(0.53)
    assert gap.gap_duration_ms == pytest.approx(250.0)
    assert gap.estimated_edge == pytest.approx(0.01)

    stats = detector.stats(state, now_ts=repriced_ts)
    assert stats.detected_gaps == 1
    assert stats.completed_gaps == 1
    assert stats.median_gap_duration_ms == pytest.approx(250.0)
    assert stats.p95_gap_duration_ms == pytest.approx(250.0)
    assert stats.average_estimated_edge == pytest.approx(0.01)
    assert stats.stale_feed_count == 1


def test_gap_detector_uses_micro_move_inputs_from_state() -> None:
    base_ts = utc_now_ns()
    state = MarketState()

    for index, price in enumerate((100.0, 101.0, 100.5)):
        state.apply(
            MarketTick(
                source="binance",
                symbol="BTCUSDT",
                price=price,
                size=1.0,
                exchange_event_ts=base_ts + index * 1_000_000_000,
                local_received_ts=base_ts + index * 1_000_000_000,
            )
        )
    state.apply(
        OrderBookTop(
            source="binance",
            symbol="BTCUSDT",
            bid_price=100.49,
            bid_size=1.0,
            ask_price=100.51,
            ask_size=1.0,
        )
    )

    snapshot = build_move_snapshot(state.symbols["BTCUSDT"])

    assert snapshot.return_1s == pytest.approx(-0.004950495)
    assert snapshot.return_5s is None
    assert snapshot.return_15s is None
    assert snapshot.return_30s is None
    assert snapshot.volatility_30s is not None
    assert snapshot.bid_ask_spread == pytest.approx(0.02)


@pytest.mark.asyncio
async def test_async_gap_event_logger_writes_jsonl(tmp_path) -> None:
    logger = AsyncJsonlEventLogger(log_dir=tmp_path)
    event = GapEvent(
        symbol="BTCUSDT",
        timeframe="15m",
        direction="UP",
        binance_move_pct=1.0,
        poly_market_price_before=0.50,
        poly_market_price_after=0.53,
        detected_ts=100,
        repriced_ts=250_000_100,
        gap_duration_ms=250.0,
        estimated_edge=0.01,
    )

    logger.start()
    await logger.log(event)
    await logger.close()

    files = list(tmp_path.glob("gap_events_*.jsonl"))
    assert len(files) == 1
    payload = orjson.loads(files[0].read_bytes().splitlines()[0])
    assert payload["event_type"] == "gap_event"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["gap_duration_ms"] == 250.0
