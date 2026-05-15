import orjson

from app.core.events import (
    BookLevel,
    DepthUpdate,
    ExecutionReport,
    ExecutionStatus,
    GapEvent,
    LatencyTrace,
    MarketTick,
    OrderBookTop,
    OrderIntent,
    PolymarketQuote,
    Side,
    SignalEvent,
)


def test_market_tick_round_trips_json() -> None:
    tick = MarketTick(source="binance", symbol="BTCUSDT", price=0.61, size=2.5)

    payload = tick.to_json_bytes()
    decoded = MarketTick.model_validate_json(payload)

    assert decoded == tick
    assert orjson.loads(payload)["event_type"] == "market_tick"


def test_all_required_events_are_json_serializable() -> None:
    events = [
        MarketTick(source="binance", symbol="BTCUSDT", price=0.61, size=2.5),
        OrderBookTop(
            source="binance",
            symbol="BTCUSDT",
            bid_price=0.60,
            bid_size=10.0,
            ask_price=0.62,
            ask_size=12.0,
        ),
        DepthUpdate(
            symbol="BTCUSDT",
            first_update_id=100,
            final_update_id=101,
            bids=[BookLevel(price=100.0, size=1.0)],
            asks=[BookLevel(price=101.0, size=2.0)],
        ),
        PolymarketQuote(
            market_id="market-1",
            token_id="token-yes",
            side_label="YES",
            best_bid=0.58,
            best_ask=0.60,
            mid_price=0.59,
            spread=0.02,
            available_liquidity_at_best=900.0,
            event_ts=100,
            received_ts=110,
        ),
        SignalEvent(
            strategy_id="test",
            market_id="market-1",
            token_id="token-yes",
            side=Side.BUY,
            fair_probability=0.65,
            quoted_probability=0.60,
            edge_bps=500.0,
            confidence=0.75,
        ),
        OrderIntent(
            strategy_id="test",
            market_id="market-1",
            token_id="token-yes",
            side=Side.BUY,
            limit_price=0.60,
            size=1.0,
            reason="unit-test",
        ),
        ExecutionReport(
            client_order_id="client-1",
            status=ExecutionStatus.ACCEPTED,
            raw={"mode": "paper"},
        ),
        LatencyTrace(trace_id="trace-1", recv_ns=100, execution_ns=150),
        GapEvent(
            symbol="BTCUSDT",
            timeframe="15m",
            direction="UP",
            binance_move_pct=1.0,
            poly_market_price_before=0.50,
            poly_market_price_after=0.53,
            detected_ts=100,
            repriced_ts=250,
            gap_duration_ms=0.00015,
            estimated_edge=0.01,
        ),
    ]

    for event in events:
        payload = event.to_json_bytes()
        decoded = orjson.loads(payload)
        assert decoded["event_id"] == event.event_id
        assert decoded["event_type"] == event.event_type


def test_latency_trace_total_ns() -> None:
    trace = LatencyTrace(trace_id="trace-1", recv_ns=100, strategy_ns=125, execution_ns=175)

    assert trace.total_ns == 75
