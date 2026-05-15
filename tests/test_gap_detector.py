from datetime import UTC, datetime

import orjson
import pytest

from app.core.clock import utc_now_ns
from app.core.events import (
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    TradableGapObservation,
)
from app.logging.event_logger import AsyncJsonlEventLogger
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.state.market_state import MarketState
from app.strategy.gap_detector import GapDetector, build_move_snapshot


def _market(
    *,
    up_token_id: str = "up-token",
    down_token_id: str = "down-token",
    min_order_size: float = 5.0,
    market_id: str = "0xmarket",
    market_slug: str = "bitcoin-up-or-down-15m",
    event_start_time: str | None = "2000-01-01T00:00:00Z",
    end_time: str = "2099-05-15T12:15:00Z",
    selected_for_runtime: bool = True,
    signal_enabled: bool = True,
    classification: str | None = "current",
) -> PolymarketMarketMetadata:
    return PolymarketMarketMetadata(
        condition_id="0xcondition",
        market_id=market_id,
        market_slug=market_slug,
        question="Bitcoin Up or Down - 15 minute",
        end_time=end_time,
        event_start_time=event_start_time,
        up_token_id=up_token_id,
        down_token_id=down_token_id,
        token_outcomes={up_token_id: "Up", down_token_id: "Down"},
        tick_size=0.01,
        min_order_size=min_order_size,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
        classification=classification,  # type: ignore[arg-type]
        selected_for_runtime=selected_for_runtime,
        signal_enabled=signal_enabled,
        base_asset="BTC",
        duration_minutes=15,
    )


def _quote(
    *,
    token_id: str = "up-token",
    side_label: str = "UP",
    mid: float,
    ts: int,
    spread: float = 0.02,
    bid_size: float | None = 100.0,
    ask_size: float | None = 100.0,
    book_complete: bool = True,
    recv_monotonic_ns: int | None = None,
    book_update_type: str | None = None,
    book_has_snapshot: bool = False,
    book_structurally_complete: bool = True,
    reported_best_validation_ok: bool = True,
    validation_error: str | None = None,
) -> PolymarketQuote:
    half_spread = spread / 2.0
    return PolymarketQuote(
        market_id="0xmarket",
        condition_id="0xcondition",
        token_id=token_id,
        side_label=side_label,  # type: ignore[arg-type]
        best_bid=mid - half_spread,
        best_bid_size=bid_size,
        best_ask=mid + half_spread,
        best_ask_size=ask_size,
        mid_price=mid,
        spread=spread,
        event_ts=ts,
        received_ts=ts,
        exchange_event_ts=ts,
        local_received_ts=ts,
        book_complete=book_complete,
        validation_error=validation_error,
        recv_monotonic_ns=recv_monotonic_ns,
        book_update_type=book_update_type,  # type: ignore[arg-type]
        book_has_snapshot=book_has_snapshot,
        book_structurally_complete=book_structurally_complete,
        reported_best_validation_ok=reported_best_validation_ok,
    )


def _book_quote(
    *,
    token_id: str = "up-token",
    side_label: str = "UP",
    best_bid: float | None,
    best_ask: float | None,
    ts: int,
    bid_size: float | None = 100.0,
    ask_size: float | None = 100.0,
    book_complete: bool = True,
    book_stale: bool = False,
    recv_monotonic_ns: int | None = None,
    book_update_type: str | None = None,
    book_has_snapshot: bool = False,
    book_structurally_complete: bool = True,
    reported_best_validation_ok: bool = True,
    validation_error: str | None = None,
) -> PolymarketQuote:
    mid = None if best_bid is None or best_ask is None else (best_bid + best_ask) / 2.0
    spread = None if best_bid is None or best_ask is None else best_ask - best_bid
    return PolymarketQuote(
        market_id="0xmarket",
        condition_id="0xcondition",
        token_id=token_id,
        side_label=side_label,  # type: ignore[arg-type]
        best_bid=best_bid,
        best_bid_size=bid_size,
        best_ask=best_ask,
        best_ask_size=ask_size,
        mid_price=mid,
        spread=spread,
        event_ts=ts,
        received_ts=ts,
        exchange_event_ts=ts,
        local_received_ts=ts,
        book_complete=book_complete,
        book_stale=book_stale,
        validation_error=validation_error,
        recv_monotonic_ns=recv_monotonic_ns,
        book_update_type=book_update_type,  # type: ignore[arg-type]
        book_has_snapshot=book_has_snapshot,
        book_structurally_complete=book_structurally_complete,
        reported_best_validation_ok=reported_best_validation_ok,
    )


def _iso_from_ns(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00",
        "Z",
    )


def _seed_ready_quotes(
    state: MarketState,
    *,
    ts: int,
    recv_monotonic_ns: int | None = None,
) -> None:
    state.apply(
        _book_quote(
            token_id="up-token",
            side_label="UP",
            best_bid=0.49,
            best_ask=0.51,
            ts=ts,
            recv_monotonic_ns=recv_monotonic_ns,
            book_update_type="book",
            book_has_snapshot=True,
        )
    )
    state.apply(
        _book_quote(
            token_id="down-token",
            side_label="DOWN",
            best_bid=0.49,
            best_ask=0.51,
            ts=ts,
            recv_monotonic_ns=recv_monotonic_ns,
            book_update_type="book",
            book_has_snapshot=True,
        )
    )


def _apply_binance_move(
    state: MarketState,
    detector: GapDetector,
    *,
    base_ts: int,
    symbol: str = "BTCUSDT",
    start_price: float = 100.0,
    end_price: float = 101.0,
    inspect_first_tick: bool = True,
    expected_second_observations: int = 0,
    first_recv_monotonic_ns: int | None = None,
    second_recv_monotonic_ns: int | None = None,
) -> int:
    first_tick = state.apply(
        MarketTick(
            source="binance",
            symbol=symbol,
            price=start_price,
            size=1.0,
            exchange_event_ts=base_ts,
            local_received_ts=base_ts,
            recv_monotonic_ns=first_recv_monotonic_ns,
        )
    )
    assert isinstance(first_tick, MarketTick)
    if inspect_first_tick:
        assert detector.on_market_event(first_tick, state, now_ts=base_ts) == ()

    state.apply(
        OrderBookTop(
            source="binance",
            symbol=symbol,
            bid_price=start_price - 0.01,
            bid_size=1.0,
            ask_price=start_price + 0.01,
            ask_size=1.0,
            local_received_ts=base_ts,
            recv_monotonic_ns=first_recv_monotonic_ns,
        )
    )

    second_ts = base_ts + 1_000_000_000
    second_tick = state.apply(
        MarketTick(
            source="binance",
            symbol=symbol,
            price=end_price,
            size=1.0,
            exchange_event_ts=second_ts,
            local_received_ts=second_ts,
            recv_monotonic_ns=second_recv_monotonic_ns,
        )
    )
    assert isinstance(second_tick, MarketTick)
    second_observations = detector.on_market_event(second_tick, state, now_ts=second_ts)
    assert len(second_observations) == expected_second_observations
    return second_ts


def test_gap_detector_records_tradable_observation_for_delayed_repricing() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.10,
        reprice_threshold=0.01,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        measurement_stale_ms=60_000.0,
    )

    initial_quote = state.apply(_quote(mid=0.50, ts=base_ts + 10_000_000))
    assert isinstance(initial_quote, PolymarketQuote)

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    assert detector.stats(state, now_ts=detected_ts).detected_gaps == 1

    repriced_ts = detected_ts + 250_000_000
    repriced_quote = state.apply(_quote(mid=0.54, ts=repriced_ts))
    assert isinstance(repriced_quote, PolymarketQuote)

    observations = detector.on_market_event(repriced_quote, state, now_ts=repriced_ts)

    assert len(observations) == 1
    observation = observations[0]
    assert observation.symbol == "BTCUSDT"
    assert observation.market_id == "0xmarket"
    assert observation.market_slug == "bitcoin-up-or-down-15m"
    assert observation.base_asset == "BTC"
    assert observation.duration_minutes == 15
    assert observation.token_id == "up-token"
    assert observation.direction == "UP"
    assert observation.binance_move_pct == pytest.approx(1.0)
    assert observation.before_mid == pytest.approx(0.50)
    assert observation.after_mid == pytest.approx(0.54)
    assert observation.before_best_ask == pytest.approx(0.51)
    assert observation.before_best_ask_size == pytest.approx(100.0)
    assert observation.after_best_bid == pytest.approx(0.53)
    assert observation.repricing_delay_ms == pytest.approx(250.0)
    assert observation.mid_repricing_delay_ms == pytest.approx(250.0)
    assert observation.executable_repricing_delay_ms == pytest.approx(250.0)
    assert observation.first_mid_repriced_ts_ns == repriced_ts
    assert observation.first_executable_repriced_ts_ns == repriced_ts
    assert observation.tradable_window_ms == pytest.approx(250.0)
    assert observation.entry_ask == pytest.approx(0.51)
    assert observation.entry_ask_size == pytest.approx(100.0)
    assert observation.executable_exit_bid == pytest.approx(0.53)
    assert observation.hypothetical_entry_price == pytest.approx(0.51)
    assert observation.hypothetical_exit_price == pytest.approx(0.53)
    assert observation.quote_was_fillable is True
    assert observation.estimated_edge_raw == pytest.approx(0.04)
    assert observation.estimated_edge_after_spread == pytest.approx(0.02)
    assert observation.exit_edge_after_spread == pytest.approx(0.02)
    assert observation.market_classification_at_detection == "current"
    assert observation.signal_enabled_at_detection is True
    assert observation.book_complete_at_detection is True
    assert observation.book_has_snapshot_at_detection is False
    assert observation.book_structurally_complete_at_detection is False
    assert observation.reported_best_validation_ok_at_detection is True
    assert observation.book_validation_error_at_detection is None
    assert observation.book_warmup_ms_at_detection is not None
    assert observation.reject_stage == "none"
    assert observation.reject_reason is None


def test_up_move_maps_to_up_token() -> None:
    base_ts = utc_now_ns()
    market = _market(up_token_id="token-up", down_token_id="token-down")
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(token_id="token-up", side_label="UP", mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    after = state.apply(_quote(token_id="token-up", side_label="UP", mid=0.54, ts=detected_ts + 1))
    assert isinstance(after, PolymarketQuote)

    observations = detector.on_market_event(after, state, now_ts=detected_ts + 1)

    assert len(observations) == 1
    assert observations[0].token_id == "token-up"
    assert observations[0].direction == "UP"


def test_down_move_maps_to_down_token() -> None:
    base_ts = utc_now_ns()
    market = _market(up_token_id="token-up", down_token_id="token-down")
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(token_id="token-down", side_label="DOWN", mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        start_price=100.0,
        end_price=99.0,
    )
    after = state.apply(
        _quote(token_id="token-down", side_label="DOWN", mid=0.54, ts=detected_ts + 1)
    )
    assert isinstance(after, PolymarketQuote)

    observations = detector.on_market_event(after, state, now_ts=detected_ts + 1)

    assert len(observations) == 1
    assert observations[0].token_id == "token-down"
    assert observations[0].direction == "DOWN"


def test_reversed_outcomes_still_use_token_for_direction() -> None:
    base_ts = utc_now_ns()
    market = PolymarketMarketMetadata(
        condition_id="0xcondition",
        market_id="0xmarket",
        market_slug="bitcoin-up-or-down-15m",
        question="Bitcoin Up or Down - 15 minute",
        end_time="2099-05-15T12:15:00Z",
        event_start_time="2000-01-01T00:00:00Z",
        up_token_id="token-b",
        down_token_id="token-a",
        token_outcomes={"token-a": "Down", "token-b": "Up"},
        tick_size=0.01,
        min_order_size=5.0,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
        classification="current",
        selected_for_runtime=True,
        signal_enabled=True,
        base_asset="BTC",
        duration_minutes=15,
    )
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(token_id="token-b", side_label="UP", mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    after = state.apply(_quote(token_id="token-b", side_label="UP", mid=0.54, ts=detected_ts + 1))
    assert isinstance(after, PolymarketQuote)
    observations = detector.on_market_event(after, state, now_ts=detected_ts + 1)

    assert len(observations) == 1
    assert observations[0].token_id == "token-b"
    assert observations[0].direction == "UP"


def test_next_market_receives_quote_but_does_not_create_candidate() -> None:
    base_ts = utc_now_ns()
    start_ts = base_ts + 600_000_000_000
    market = _market(
        event_start_time=_iso_from_ns(start_ts),
        end_time=_iso_from_ns(start_ts + 900_000_000_000),
        selected_for_runtime=True,
        signal_enabled=False,
        classification="next",
    )
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    quote = state.apply(_quote(mid=0.50, ts=base_ts))
    assert isinstance(quote, PolymarketQuote)

    assert detector.on_market_event(quote, state, now_ts=base_ts) == ()
    _apply_binance_move(state, detector, base_ts=base_ts, expected_second_observations=0)

    stats = detector.stats(state, now_ts=base_ts + 1_000_000_000)
    assert stats.detected_gaps == 0
    assert stats.completed_gaps == 0
    assert stats.warmup_quotes_received == 1
    assert stats.signal_enabled_markets == 0
    assert stats.warmup_only_markets == 1


def test_current_market_can_create_candidate() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)

    assert detector.stats(state, now_ts=detected_ts).detected_gaps == 1
    assert detector.stats(state, now_ts=detected_ts).fillable_at_detection_count == 1


def test_signal_candidate_suppressed_before_book_ready() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        book_warmup_max_ms=3_000.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    stats = detector.stats(state, now_ts=detected_ts)

    assert stats.detected_gaps == 0
    assert stats.completed_gaps == 0
    assert stats.book_warmup_suppressed == 1


def test_after_book_ready_candidate_can_be_created() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(
        _book_quote(
            token_id="up-token",
            side_label="UP",
            best_bid=0.49,
            best_ask=0.51,
            ts=base_ts,
            book_update_type="book",
            book_has_snapshot=True,
        )
    )
    state.apply(
        _book_quote(
            token_id="down-token",
            side_label="DOWN",
            best_bid=0.49,
            best_ask=0.51,
            ts=base_ts,
            book_update_type="book",
            book_has_snapshot=True,
        )
    )

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    stats = detector.stats(state, now_ts=detected_ts)

    assert stats.detected_gaps == 1
    assert stats.fillable_at_detection_count == 1
    assert stats.book_warmup_suppressed == 0


def test_after_warmup_timeout_book_incomplete_observation_can_be_written() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        book_warmup_max_ms=100.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(
        _book_quote(
            token_id="up-token",
            side_label="UP",
            best_bid=0.49,
            best_ask=0.51,
            ts=base_ts,
            book_complete=False,
            book_update_type="price_change",
            book_has_snapshot=False,
            validation_error="no_snapshot",
        )
    )
    state.apply(
        _book_quote(
            token_id="down-token",
            side_label="DOWN",
            best_bid=0.49,
            best_ask=0.51,
            ts=base_ts,
            book_complete=False,
            book_update_type="price_change",
            book_has_snapshot=False,
            validation_error="no_snapshot",
        )
    )

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
    )
    stats = detector.stats(state, now_ts=detected_ts)

    assert stats.detected_gaps == 1
    assert stats.completed_gaps == 1
    assert stats.book_warmup_suppressed == 0
    assert stats.reject_count_by_reason["book_incomplete"] == 1
    assert stats.reject_count_by_stage["pre_entry"] == 1
    observation = detector._completed[-1]  # type: ignore[attr-defined]
    assert observation.book_warmup_timeout is True
    assert observation.book_complete_at_detection is False
    assert observation.book_validation_error_at_detection == "no_snapshot"


def test_next_market_becomes_current_after_event_start_time_can_create_candidate() -> None:
    base_ts = utc_now_ns()
    start_ts = base_ts + 2_000_000_000
    market = _market(
        event_start_time=_iso_from_ns(start_ts),
        end_time=_iso_from_ns(start_ts + 900_000_000_000),
        selected_for_runtime=True,
        signal_enabled=False,
        classification="next",
    )
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts + 1_000_000_000))
    _apply_binance_move(state, detector, base_ts=base_ts, expected_second_observations=0)

    active_ts = start_ts + 10_000_000
    state.apply(_quote(mid=0.50, ts=active_ts))
    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=active_ts,
        start_price=101.0,
        end_price=102.0,
        inspect_first_tick=False,
    )

    stats = detector.stats(state, now_ts=detected_ts)
    assert stats.detected_gaps == 1
    assert stats.fillable_at_detection_count == 1
    assert stats.signal_enabled_markets == 1
    assert stats.warmup_only_markets == 0


def test_current_expires_and_next_market_promotes_without_restart() -> None:
    base_ts = utc_now_ns()
    next_start_ts = base_ts + 900_000_000_000
    current = _market(
        market_id="current",
        market_slug="btc-updown-15m-current",
        up_token_id="current-up",
        down_token_id="current-down",
        event_start_time=_iso_from_ns(base_ts - 100_000_000_000),
        end_time=_iso_from_ns(next_start_ts),
        selected_for_runtime=True,
        signal_enabled=True,
        classification="current",
    )
    next_market = _market(
        market_id="next",
        market_slug="btc-updown-15m-next",
        up_token_id="next-up",
        down_token_id="next-down",
        event_start_time=_iso_from_ns(next_start_ts),
        end_time=_iso_from_ns(next_start_ts + 900_000_000_000),
        selected_for_runtime=True,
        signal_enabled=False,
        classification="next",
    )
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(current, next_market),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )

    initial_stats = detector.stats(state, now_ts=base_ts)
    assert initial_stats.signal_enabled_markets == 1
    assert initial_stats.warmup_only_markets == 1

    promoted_ts = next_start_ts + 10_000_000
    state.apply(_quote(token_id="next-up", mid=0.50, ts=promoted_ts))
    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=promoted_ts,
        start_price=100.0,
        end_price=101.0,
    )

    stats = detector.stats(state, now_ts=detected_ts)
    assert stats.detected_gaps == 1
    assert stats.signal_enabled_markets == 1
    assert stats.warmup_only_markets == 0


def test_quote_with_no_best_ask_size_is_not_fillable_for_buy() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, ask_size=None))

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
    )
    stats = detector.stats(state, now_ts=detected_ts)

    assert stats.detected_gaps == 1
    assert stats.fillable_at_detection_count == 0
    assert stats.non_fillable_at_detection_count == 1
    assert stats.reject_count_by_reason["missing_best_ask_size"] == 1
    assert stats.reject_count_by_stage["pre_entry"] == 1
    observation = detector._completed[-1]  # type: ignore[attr-defined]
    assert observation.market_slug == "bitcoin-up-or-down-15m"
    assert observation.base_asset == "BTC"
    assert observation.duration_minutes == 15


def test_repeated_pre_entry_rejects_within_cooldown_write_one_observation() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        pre_entry_log_cooldown_ms=5_000.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, ask_size=None))

    first_detected = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
    )
    second_detected = _apply_binance_move(
        state,
        detector,
        base_ts=first_detected + 100_000_000,
        start_price=101.0,
        end_price=102.0,
        inspect_first_tick=False,
        expected_second_observations=0,
    )

    stats = detector.stats(state, now_ts=second_detected)
    assert stats.completed_gaps == 1
    assert stats.non_fillable_at_detection_count == 2
    assert stats.pre_entry_observations_written == 1
    assert stats.pre_entry_observations_suppressed == 1


def test_pre_entry_reject_after_cooldown_writes_another_observation() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        pre_entry_log_cooldown_ms=500.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, ask_size=None))

    first_detected = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
    )
    second_detected = _apply_binance_move(
        state,
        detector,
        base_ts=first_detected + 1_000_000_000,
        start_price=101.0,
        end_price=102.0,
        inspect_first_tick=False,
        expected_second_observations=1,
    )

    stats = detector.stats(state, now_ts=second_detected)
    assert stats.completed_gaps == 2
    assert stats.pre_entry_observations_written == 2
    assert stats.pre_entry_observations_suppressed == 0


def test_fillable_observations_are_never_pre_entry_throttled() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        pre_entry_log_cooldown_ms=60_000.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )

    state.apply(_quote(mid=0.50, ts=base_ts))
    first_detected = _apply_binance_move(state, detector, base_ts=base_ts)
    first_reprice = state.apply(_quote(mid=0.54, ts=first_detected + 1))
    assert isinstance(first_reprice, PolymarketQuote)
    assert len(detector.on_market_event(first_reprice, state, now_ts=first_detected + 1)) == 1

    second_base = first_detected + 100_000_000
    state.apply(_quote(mid=0.50, ts=second_base))
    second_detected = _apply_binance_move(
        state,
        detector,
        base_ts=second_base,
        start_price=101.0,
        end_price=102.0,
        inspect_first_tick=False,
    )
    second_reprice = state.apply(_quote(mid=0.54, ts=second_detected + 1))
    assert isinstance(second_reprice, PolymarketQuote)
    assert len(detector.on_market_event(second_reprice, state, now_ts=second_detected + 1)) == 1

    stats = detector.stats(state, now_ts=second_detected + 1)
    assert stats.fillable_at_detection_count == 2
    assert stats.pre_entry_observations_written == 0
    assert stats.pre_entry_observations_suppressed == 0


def test_wide_spread_is_marked_non_tradable() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_entry_spread=0.03,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, spread=0.10))

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
    )
    stats = detector.stats(state, now_ts=detected_ts)

    assert stats.detected_gaps == 1
    assert stats.fillable_at_detection_count == 0
    assert stats.non_fillable_at_detection_count == 1
    assert stats.reject_count_by_reason["spread_too_wide"] == 1


def test_stale_binance_state_rejects_candidate() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=500.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))
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
    detector.on_market_event(first_tick, state, now_ts=base_ts)

    second_ts = base_ts + 1_000_000_000
    second_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=second_ts,
            local_received_ts=base_ts,
        )
    )

    assert isinstance(second_tick, MarketTick)
    detector.on_market_event(second_tick, state, now_ts=second_ts)

    assert detector.stats(state, now_ts=second_ts).detected_gaps == 0


def test_stale_polymarket_quote_rejects_candidate() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=500.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts + 1_000_000_000,
        expected_second_observations=1,
    )

    stats = detector.stats(state, now_ts=detected_ts)
    assert stats.detected_gaps == 1
    assert stats.fillable_at_detection_count == 0
    assert stats.non_fillable_at_detection_count == 1
    assert stats.reject_count_by_reason["quote_stale"] == 1


def test_quote_stale_due_to_polymarket_sets_stale_source() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=500.0,
    )
    _seed_ready_quotes(state, ts=base_ts, recv_monotonic_ns=1_000_000_000)

    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        expected_second_observations=1,
        first_recv_monotonic_ns=1_900_000_000,
        second_recv_monotonic_ns=2_000_000_000,
    )

    observation = detector._completed[-1]  # type: ignore[attr-defined]
    assert observation.reject_reason == "quote_stale"
    assert observation.stale_source == "polymarket"
    assert observation.polymarket_quote_age_ms == pytest.approx(1000.0)
    assert observation.binance_quote_age_ms == pytest.approx(0.0)
    assert observation.now_monotonic_ns == 2_000_000_000
    assert observation.last_binance_update_monotonic_ns == 2_000_000_000
    assert observation.last_polymarket_update_monotonic_ns == 1_000_000_000
    assert detector.stats(state, now_ts=detected_ts).reject_count_by_reason["quote_stale"] == 1


def test_quote_stale_due_to_binance_sets_stale_source() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=500.0,
        polymarket_stale_ms=60_000.0,
    )
    _seed_ready_quotes(state, ts=base_ts, recv_monotonic_ns=2_000_000_000)
    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        first_recv_monotonic_ns=1_900_000_000,
        second_recv_monotonic_ns=2_000_000_000,
    )

    fresh_poly = state.apply(
        _book_quote(
            best_bid=0.49,
            best_ask=0.51,
            ts=detected_ts + 1_000_000_000,
            recv_monotonic_ns=3_000_000_000,
        )
    )
    assert isinstance(fresh_poly, PolymarketQuote)
    observations = detector.on_market_event(fresh_poly, state, now_ts=detected_ts + 1_000_000_000)

    assert len(observations) == 1
    assert observations[0].reject_reason == "quote_stale"
    assert observations[0].stale_source == "binance"
    assert observations[0].binance_quote_age_ms == pytest.approx(1000.0)
    assert observations[0].polymarket_quote_age_ms == pytest.approx(0.0)


def test_quote_stale_due_to_both_sets_stale_source() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=500.0,
        polymarket_stale_ms=500.0,
    )
    _seed_ready_quotes(state, ts=base_ts, recv_monotonic_ns=2_000_000_000)
    detected_ts = _apply_binance_move(
        state,
        detector,
        base_ts=base_ts,
        first_recv_monotonic_ns=1_900_000_000,
        second_recv_monotonic_ns=2_000_000_000,
    )

    stale_poly = state.apply(
        _book_quote(
            best_bid=0.49,
            best_ask=0.51,
            ts=detected_ts + 1_000_000_000,
            recv_monotonic_ns=3_000_000_000,
            book_stale=True,
        )
    )
    assert isinstance(stale_poly, PolymarketQuote)
    observations = detector.on_market_event(stale_poly, state, now_ts=detected_ts + 1_000_000_000)

    assert len(observations) == 1
    assert observations[0].reject_reason == "quote_stale"
    assert observations[0].stale_source == "both"


def test_quote_stale_missing_monotonic_timestamps_sets_unknown_source() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        require_book_ready=False,
        binance_stale_ms=500.0,
        polymarket_stale_ms=500.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts + 1_000_000_000))
    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    stale = state.apply(
        _book_quote(
            best_bid=0.495,
            best_ask=0.515,
            ts=detected_ts + 100_000_000,
            book_stale=True,
        )
    )
    assert isinstance(stale, PolymarketQuote)
    observations = detector.on_market_event(stale, state, now_ts=detected_ts + 100_000_000)

    assert len(observations) == 1
    assert observations[0].reject_reason == "quote_stale"
    assert observations[0].stale_source == "unknown"


def test_incomplete_book_rejects_candidate() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, book_complete=False))

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

    detected_ts = base_ts + 1_000_000_000
    second_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=detected_ts,
            local_received_ts=detected_ts,
        )
    )
    assert isinstance(second_tick, MarketTick)
    observations = detector.on_market_event(second_tick, state, now_ts=detected_ts)
    stats = detector.stats(state, now_ts=detected_ts)

    assert len(observations) == 1
    assert observations[0].reject_stage == "pre_entry"
    assert observations[0].pre_entry_reject_reason == "book_incomplete"
    assert observations[0].reject_reason == "book_incomplete"
    assert observations[0].quote_was_fillable is False
    assert stats.detected_gaps == 1
    assert stats.completed_gaps == 1
    assert stats.fillable_at_detection_count == 0
    assert stats.non_fillable_at_detection_count == 1
    assert stats.reject_count_by_reason["book_incomplete"] == 1
    assert stats.reject_count_by_stage["pre_entry"] == 1


def test_repricing_in_opposite_direction_does_not_close_gap() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    opposite_quote = state.apply(_quote(mid=0.48, ts=detected_ts + 100_000_000))
    assert isinstance(opposite_quote, PolymarketQuote)

    observations = detector.on_market_event(opposite_quote, state, now_ts=detected_ts + 100_000_000)

    assert observations == ()
    assert detector.stats(state, now_ts=detected_ts + 100_000_000).completed_gaps == 0


def test_current_bid_below_entry_ask_does_not_end_tradable_window() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    still_stale = state.apply(
        _book_quote(best_bid=0.50, best_ask=0.52, ts=detected_ts + 100_000_000)
    )
    assert isinstance(still_stale, PolymarketQuote)
    observations = detector.on_market_event(still_stale, state, now_ts=detected_ts + 100_000_000)

    assert observations == ()
    stats = detector.stats(state, now_ts=detected_ts + 100_000_000)
    assert stats.completed_gaps == 0
    assert stats.fillable_at_detection_count == 1


def test_mid_repricing_without_profitable_bid_waits_until_timeout() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_pending_gap_ms=200.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    mid_only_ts = detected_ts + 100_000_000
    mid_only = state.apply(_book_quote(best_bid=0.50, best_ask=0.52, ts=mid_only_ts))
    assert isinstance(mid_only, PolymarketQuote)
    assert detector.on_market_event(mid_only, state, now_ts=mid_only_ts) == ()

    timeout_ts = detected_ts + 300_000_000
    timeout_quote = state.apply(_book_quote(best_bid=0.50, best_ask=0.52, ts=timeout_ts))
    assert isinstance(timeout_quote, PolymarketQuote)
    observations = detector.on_market_event(timeout_quote, state, now_ts=timeout_ts)

    assert len(observations) == 1
    assert observations[0].mid_repricing_delay_ms == pytest.approx(100.0)
    assert observations[0].executable_repricing_delay_ms is None
    assert observations[0].repricing_delay_ms is None
    assert observations[0].first_executable_repriced_ts_ns is None
    assert observations[0].reject_stage == "timeout"
    assert observations[0].reject_reason == "max_observation_lifetime_reached"
    assert observations[0].exit_reject_reason == "no_executable_repricing_before_timeout"


def test_mid_repricing_then_executable_repricing_records_two_delays() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        min_exit_edge=0.005,
        max_entry_price_move=0.04,
        max_pending_gap_ms=1_000.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_book_quote(best_bid=0.49, best_ask=0.51, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)

    mid_reprice_ts = detected_ts + 100_000_000
    mid_reprice_only = state.apply(
        _book_quote(best_bid=0.505, best_ask=0.535, ts=mid_reprice_ts)
    )
    assert isinstance(mid_reprice_only, PolymarketQuote)
    assert detector.on_market_event(mid_reprice_only, state, now_ts=mid_reprice_ts) == ()

    executable_ts = detected_ts + 250_000_000
    executable = state.apply(_book_quote(best_bid=0.516, best_ask=0.536, ts=executable_ts))
    assert isinstance(executable, PolymarketQuote)
    observations = detector.on_market_event(executable, state, now_ts=executable_ts)

    assert len(observations) == 1
    observation = observations[0]
    assert observation.mid_repricing_delay_ms == pytest.approx(100.0)
    assert observation.executable_repricing_delay_ms == pytest.approx(250.0)
    assert observation.repricing_delay_ms == observation.executable_repricing_delay_ms
    assert observation.first_mid_repriced_ts_ns == mid_reprice_ts
    assert observation.first_executable_repriced_ts_ns == executable_ts
    assert observation.executable_exit_bid == pytest.approx(0.516)
    assert observation.exit_edge_after_spread == pytest.approx(0.006)
    assert observation.reject_stage == "none"
    assert observation.reject_reason is None


def test_timeout_without_mid_repricing_records_no_mid_repricing_reason() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_pending_gap_ms=200.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    timeout_ts = detected_ts + 300_000_000
    unchanged = state.apply(_book_quote(best_bid=0.49, best_ask=0.51, ts=timeout_ts))
    assert isinstance(unchanged, PolymarketQuote)
    observations = detector.on_market_event(unchanged, state, now_ts=timeout_ts)

    assert len(observations) == 1
    assert observations[0].mid_repricing_delay_ms is None
    assert observations[0].executable_repricing_delay_ms is None
    assert observations[0].first_mid_repriced_ts_ns is None
    assert observations[0].first_executable_repriced_ts_ns is None
    assert observations[0].reject_stage == "timeout"
    assert observations[0].reject_reason == "max_observation_lifetime_reached"
    assert observations[0].exit_reject_reason == "no_mid_repricing_before_timeout"


def test_timeout_precedence_over_window_end_when_same_event() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_pending_gap_ms=100.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_book_quote(best_bid=0.49, best_ask=0.51, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    timeout_ts = detected_ts + 200_000_000
    no_size = state.apply(
        _book_quote(best_bid=0.49, best_ask=0.51, ts=timeout_ts, ask_size=0.0)
    )
    assert isinstance(no_size, PolymarketQuote)
    observations = detector.on_market_event(no_size, state, now_ts=timeout_ts)

    assert len(observations) == 1
    assert observations[0].reject_stage == "timeout"
    assert observations[0].reject_reason == "max_observation_lifetime_reached"
    assert observations[0].exit_reject_reason == "no_mid_repricing_before_timeout"
    assert observations[0].window_end_reason is None


def test_executable_repricing_requires_bid_above_entry_plus_min_edge() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        min_exit_edge=0.005,
        max_pending_gap_ms=1_000.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    not_enough_ts = detected_ts + 100_000_000
    not_enough = state.apply(
        _book_quote(best_bid=0.515, best_ask=0.52, ts=not_enough_ts)
    )
    assert isinstance(not_enough, PolymarketQuote)
    assert detector.on_market_event(not_enough, state, now_ts=not_enough_ts) == ()

    executable_ts = detected_ts + 250_000_000
    executable = state.apply(
        _book_quote(best_bid=0.516, best_ask=0.52, ts=executable_ts)
    )
    assert isinstance(executable, PolymarketQuote)
    observations = detector.on_market_event(executable, state, now_ts=executable_ts)

    assert len(observations) == 1
    assert observations[0].executable_repricing_delay_ms == pytest.approx(250.0)
    assert observations[0].repricing_delay_ms == observations[0].executable_repricing_delay_ms
    assert observations[0].executable_exit_bid == pytest.approx(0.516)
    assert observations[0].exit_edge_after_spread == pytest.approx(0.006)
    assert observations[0].reject_stage == "none"
    assert observations[0].reject_reason is None


def test_monotonic_timestamps_drive_window_duration_despite_wall_clock_drift() -> None:
    base_ts = 1_700_000_000_000_000_000
    base_mono = 10_000_000_000
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts, recv_monotonic_ns=base_mono + 50_000_000))

    first_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=100.0,
            size=1.0,
            exchange_event_ts=base_ts,
            local_received_ts=base_ts,
            recv_monotonic_ns=base_mono + 100_000_000,
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
    second_tick = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=base_ts + 1_000_000_000,
            local_received_ts=base_ts + 1_000_000_000,
            recv_monotonic_ns=base_mono + 200_000_000,
        )
    )
    assert isinstance(second_tick, MarketTick)
    assert detector.on_market_event(second_tick, state, now_ts=base_ts + 1_000_000_000) == ()

    drifted_quote = state.apply(
        _book_quote(
            best_bid=0.53,
            best_ask=0.55,
            ts=base_ts - 60_000_000_000,
            recv_monotonic_ns=base_mono + 450_000_000,
        )
    )
    assert isinstance(drifted_quote, PolymarketQuote)
    observations = detector.on_market_event(drifted_quote, state, now_ts=base_ts - 60_000_000_000)

    assert len(observations) == 1
    assert observations[0].tradable_window_ms == pytest.approx(250.0)
    assert observations[0].executable_repricing_delay_ms == pytest.approx(250.0)


def test_tradable_window_ends_before_repricing_when_ask_jumps() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_entry_spread=0.20,
        max_entry_price_move=0.02,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    ask_jump_ts = detected_ts + 100_000_000
    ask_jump = state.apply(_book_quote(best_bid=0.45, best_ask=0.56, ts=ask_jump_ts))
    assert isinstance(ask_jump, PolymarketQuote)
    observations = detector.on_market_event(ask_jump, state, now_ts=ask_jump_ts)

    assert len(observations) == 1
    assert observations[0].repricing_delay_ms is None
    assert observations[0].tradable_window_ms == pytest.approx(100.0)
    assert observations[0].repricing_delay_ms != observations[0].tradable_window_ms
    assert observations[0].reject_stage == "window"
    assert observations[0].window_end_reason == "entry_price_moved"
    assert observations[0].reject_reason == "entry_price_moved"
    assert observations[0].estimated_edge_after_spread is None
    assert observations[0].market_slug == "bitcoin-up-or-down-15m"
    assert observations[0].base_asset == "BTC"
    assert observations[0].duration_minutes == 15


def test_tradable_window_ends_when_best_ask_size_disappears() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    no_size_ts = detected_ts + 100_000_000
    no_size = state.apply(_quote(mid=0.505, ts=no_size_ts, ask_size=0.0))
    assert isinstance(no_size, PolymarketQuote)
    observations = detector.on_market_event(no_size, state, now_ts=no_size_ts)

    assert len(observations) == 1
    assert observations[0].repricing_delay_ms is None
    assert observations[0].tradable_window_ms == pytest.approx(100.0)
    assert observations[0].reject_stage == "window"
    assert observations[0].window_end_reason == "insufficient_best_ask_size"
    assert observations[0].reject_reason == "insufficient_best_ask_size"


def test_market_resolved_cancels_pending_gap() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id="0xmarket",
            lifecycle_type="market_resolved",
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert len(observations) == 1
    assert observations[0].reject_reason == "market_resolved"
    assert observations[0].reject_stage == "lifecycle"
    assert observations[0].window_end_reason == "market_resolved"
    assert observations[0].tradable_window_ms == pytest.approx(100.0)
    assert detector.stats(state, now_ts=detected_ts + 250_000_000).completed_count == 1


def test_tick_size_change_marks_market_invalid() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id="0xmarket",
            lifecycle_type="tick_size_change",
            old_tick_size=0.01,
            new_tick_size=0.001,
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert "0xmarket" in state.invalid_polymarket_markets
    assert len(observations) == 1
    assert observations[0].reject_reason == "tick_size_change"
    assert observations[0].reject_stage == "lifecycle"
    assert observations[0].window_end_reason == "tick_size_change"
    assert detector.stats(state, now_ts=detected_ts + 100_000_000).completed_count == 1


def test_stale_quote_ends_tradable_window() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    stale_ts = detected_ts + 100_000_000
    stale = state.apply(_book_quote(best_bid=0.495, best_ask=0.515, ts=stale_ts, book_stale=True))
    assert isinstance(stale, PolymarketQuote)
    observations = detector.on_market_event(stale, state, now_ts=stale_ts)

    assert len(observations) == 1
    assert observations[0].tradable_window_ms == pytest.approx(100.0)
    assert observations[0].reject_stage == "window"
    assert observations[0].reject_reason == "quote_stale"


def test_max_pending_gap_ms_closes_stale_pending_gap() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        max_pending_gap_ms=100.0,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
    )
    state.apply(_quote(mid=0.50, ts=base_ts))

    detected_ts = _apply_binance_move(state, detector, base_ts=base_ts)
    heartbeat = state.apply(
        OrderBookTop(
            source="binance",
            symbol="BTCUSDT",
            bid_price=101.0,
            bid_size=1.0,
            ask_price=101.1,
            ask_size=1.0,
            local_received_ts=detected_ts + 200_000_000,
        )
    )
    assert isinstance(heartbeat, OrderBookTop)
    observations = detector.on_market_event(heartbeat, state, now_ts=detected_ts + 200_000_000)

    assert len(observations) == 1
    assert observations[0].reject_stage == "timeout"
    assert observations[0].reject_reason == "max_observation_lifetime_reached"
    assert observations[0].tradable_window_ms == pytest.approx(200.0)
    assert observations[0].market_slug == "bitcoin-up-or-down-15m"
    assert observations[0].base_asset == "BTC"
    assert observations[0].duration_minutes == 15


def test_p95_stats_with_multiple_completed_observations() -> None:
    base_ts = utc_now_ns()
    market = _market()
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    detector = GapDetector(
        markets=(market,),
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        measurement_stale_ms=60_000.0,
    )

    for index, duration_ms in enumerate((100.0, 200.0, 300.0)):
        cycle_base = base_ts + index * 10_000_000_000
        state.apply(_quote(mid=0.50, ts=cycle_base))
        detected_ts = _apply_binance_move(
            state,
            detector,
            base_ts=cycle_base,
            start_price=100.0 + index,
            end_price=101.0 + index,
            inspect_first_tick=False,
        )
        after_ts = detected_ts + int(duration_ms * 1_000_000)
        after = state.apply(_quote(mid=0.54, ts=after_ts))
        assert isinstance(after, PolymarketQuote)
        detector.on_market_event(after, state, now_ts=after_ts)

    stats = detector.stats(state, now_ts=base_ts + 30_000_000_000)

    assert stats.detected_gaps == 3
    assert stats.completed_gaps == 3
    assert stats.fillable_at_detection_count == 3
    assert stats.non_fillable_at_detection_count == 0
    assert stats.median_mid_repricing_delay_ms == pytest.approx(200.0)
    assert stats.p95_mid_repricing_delay_ms == pytest.approx(300.0)
    assert stats.median_executable_repricing_delay_ms == pytest.approx(200.0)
    assert stats.p95_executable_repricing_delay_ms == pytest.approx(300.0)
    assert stats.median_tradable_window_ms == pytest.approx(200.0)
    assert stats.p95_tradable_window_ms == pytest.approx(300.0)
    assert stats.reject_count_by_reason == {}
    assert stats.reject_count_by_stage == {}
    assert stats.average_estimated_edge == pytest.approx(0.02)


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
    event = TradableGapObservation(
        symbol="BTCUSDT",
        market_id="0xmarket",
        token_id="up-token",
        direction="UP",
        binance_move_pct=1.0,
        detected_ts_ns=100,
        binance_event_ts_ns=90,
        poly_quote_ts_ns=250_000_100,
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
        repricing_delay_ms=250.0,
        tradable_window_ms=250.0,
        hypothetical_entry_price=0.51,
        hypothetical_exit_price=0.53,
        quote_was_fillable=True,
        estimated_edge_raw=0.04,
        estimated_edge_after_spread=0.02,
        reject_reason=None,
    )

    logger.start()
    await logger.log(event)
    await logger.close()

    files = list(tmp_path.glob("gap_events_*.jsonl"))
    assert len(files) == 1
    payload = orjson.loads(files[0].read_bytes().splitlines()[0])
    assert payload["event_type"] == "tradable_gap_observation"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["repricing_delay_ms"] == 250.0
