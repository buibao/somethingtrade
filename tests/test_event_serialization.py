import orjson

from app.core.events import (
    BookLevel,
    DepthUpdate,
    Event,
    ExecutionReport,
    ExecutionStatus,
    LatencyTrace,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    OrderIntent,
    PolymarketQuote,
    Side,
    SignalEvent,
    TradableGapObservation,
)


def test_market_tick_round_trips_json() -> None:
    tick = MarketTick(source="binance", symbol="BTCUSDT", price=0.61, size=2.5)

    payload = tick.to_json_bytes()
    decoded = MarketTick.model_validate_json(payload)

    assert decoded == tick
    assert orjson.loads(payload)["event_type"] == "market_tick"


def test_all_required_events_are_json_serializable() -> None:
    events: list[Event] = [
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
            best_bid_size=500.0,
            best_ask=0.60,
            best_ask_size=400.0,
            mid_price=0.59,
            spread=0.02,
            event_ts=100,
            received_ts=110,
            book_complete=True,
            book_stale=False,
            book_hash="hash-1",
            validation_error=None,
            recv_monotonic_ns=10,
            parse_done_monotonic_ns=20,
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
        TradableGapObservation(
            symbol="BTCUSDT",
            market_id="market-1",
            token_id="token-up",
            direction="UP",
            binance_move_pct=1.0,
            detected_ts_ns=100,
            binance_event_ts_ns=90,
            poly_quote_ts_ns=250,
            before_best_bid=0.49,
            before_best_ask=0.51,
            before_best_bid_size=10.0,
            before_best_ask_size=20.0,
            before_mid=0.50,
            after_best_bid=0.53,
            after_best_ask=0.55,
            after_mid=0.54,
            spread_before=0.02,
            spread_after=0.02,
            mid_repricing_delay_ms=100.0,
            executable_repricing_delay_ms=0.00015,
            first_mid_repriced_ts_ns=200,
            first_executable_repriced_ts_ns=250,
            executable_exit_bid=0.53,
            entry_ask=0.51,
            entry_ask_size=20.0,
            exit_edge_after_spread=0.02,
            repricing_delay_ms=0.00015,
            tradable_window_ms=0.00015,
            hypothetical_entry_price=0.51,
            hypothetical_exit_price=0.53,
            quote_was_fillable=True,
            estimated_edge_raw=0.04,
            estimated_edge_after_spread=0.02,
            pre_entry_reject_reason=None,
            window_end_reason=None,
            exit_reject_reason=None,
            reject_stage="none",
            reject_reason=None,
        ),
        MarketLifecycleEvent(
            market_id="market-1",
            token_id="token-up",
            lifecycle_type="tick_size_change",
            old_tick_size=0.01,
            new_tick_size=0.001,
            event_ts=100,
            received_ts=110,
            raw_metadata={"event_type": "tick_size_change"},
        ),
    ]

    for event in events:
        payload = event.to_json_bytes()
        decoded = orjson.loads(payload)
        assert decoded["event_id"] == event.event_id
        assert decoded["event_type"] == event.event_type


def test_polymarket_quote_constructs_with_phase37_fields() -> None:
    quote = PolymarketQuote(
        market_id="market-1",
        condition_id="condition-1",
        token_id="token-up",
        side_label="UP",
        best_bid=0.49,
        best_bid_size=10.0,
        best_ask=0.51,
        best_ask_size=20.0,
        mid_price=0.50,
        spread=0.02,
        event_ts=100,
        received_ts=110,
        book_complete=True,
        book_stale=False,
        book_hash="hash-1",
        validation_error=None,
        book_has_snapshot=True,
        book_structurally_complete=True,
        reported_best_validation_ok=True,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_100,
    )

    assert quote.book_complete is True
    assert quote.book_has_snapshot is True
    assert quote.book_structurally_complete is True
    assert quote.reported_best_validation_ok is True
    assert quote.available_liquidity_at_best == 30.0
    assert quote.book_hash == "hash-1"


def test_market_lifecycle_event_constructs_with_latency_fields() -> None:
    event = MarketLifecycleEvent(
        market_id="market-1",
        token_id="token-up",
        lifecycle_type="market_resolved",
        old_tick_size=0.01,
        new_tick_size=0.001,
        raw_metadata={"event_type": "market_resolved"},
        event_ts=100,
        received_ts=110,
        exchange_event_ts=100,
        local_received_ts=110,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_100,
        latency_ms=0.0001,
    )

    assert event.event_type == "market_lifecycle"
    assert event.lifecycle_type == "market_resolved"


def test_tradable_gap_observation_constructs_with_phase37_fields() -> None:
    event = TradableGapObservation(
        symbol="BTCUSDT",
        market_id="market-1",
        market_slug="btc-updown-15m-1778866200",
        base_asset="BTC",
        duration_minutes=15,
        token_id="token-up",
        direction="UP",
        binance_move_pct=1.0,
        detected_ts_ns=100,
        binance_event_ts_ns=90,
        poly_quote_ts_ns=250,
        before_best_bid=0.49,
        before_best_ask=0.51,
        before_best_bid_size=10.0,
        before_best_ask_size=20.0,
        before_mid=0.50,
        after_best_bid=0.53,
        after_best_ask=0.55,
        after_mid=0.54,
        spread_before=0.02,
        spread_after=0.02,
        mid_repricing_delay_ms=120.0,
        executable_repricing_delay_ms=150.0,
        first_mid_repriced_ts_ns=220,
        first_executable_repriced_ts_ns=250,
        executable_exit_bid=0.53,
        entry_ask=0.51,
        entry_ask_size=20.0,
        exit_edge_after_spread=0.02,
        repricing_delay_ms=150.0,
        tradable_window_ms=100.0,
        hypothetical_entry_price=0.51,
        hypothetical_exit_price=0.53,
        quote_was_fillable=True,
        estimated_edge_raw=0.04,
        estimated_edge_after_spread=0.02,
        market_classification_at_detection="current",
        signal_enabled_at_detection=True,
        book_complete_at_detection=True,
        book_has_snapshot_at_detection=True,
        book_structurally_complete_at_detection=True,
        reported_best_validation_ok_at_detection=True,
        book_validation_error_at_detection=None,
        stale_source=None,
        binance_quote_age_ms=None,
        polymarket_quote_age_ms=None,
        now_monotonic_ns=300,
        last_binance_update_monotonic_ns=250,
        last_polymarket_update_monotonic_ns=240,
        pre_entry_reject_reason=None,
        window_end_reason=None,
        exit_reject_reason=None,
        reject_stage="none",
        reject_reason=None,
    )

    assert event.repricing_delay_ms == 150.0
    assert event.executable_repricing_delay_ms == 150.0
    assert event.gap_duration_ms == 150.0
    assert event.market_slug == "btc-updown-15m-1778866200"
    assert event.base_asset == "BTC"
    assert event.duration_minutes == 15
    assert event.book_has_snapshot_at_detection is True


def test_latency_trace_total_ns() -> None:
    trace = LatencyTrace(trace_id="trace-1", recv_ns=100, strategy_ns=125, execution_ns=175)

    assert trace.total_ns == 75
