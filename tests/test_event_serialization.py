import orjson

from app.core.events import (
    BookLevel,
    DepthUpdate,
    ExecutionReport,
    ExecutionStatus,
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
            outcome="YES",
            bid_probability=0.58,
            ask_probability=0.60,
            bid_size=500.0,
            ask_size=400.0,
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
    ]

    for event in events:
        payload = event.to_json_bytes()
        decoded = orjson.loads(payload)
        assert decoded["event_id"] == event.event_id
        assert decoded["event_type"] == event.event_type


def test_latency_trace_total_ns() -> None:
    trace = LatencyTrace(trace_id="trace-1", recv_ns=100, strategy_ns=125, execution_ns=175)

    assert trace.total_ns == 75
