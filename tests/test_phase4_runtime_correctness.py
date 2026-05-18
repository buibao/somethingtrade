from __future__ import annotations

import asyncio
from pathlib import Path
from collections.abc import Sequence
from datetime import UTC, datetime
from types import TracebackType

import orjson
import pytest

from app.core.events import MarketLifecycleEvent, MarketTick, OrderBookTop, PolymarketQuote
from app.backtest.dataset_quality_phase4 import (
    build_phase4_dataset_quality_report,
    render_phase4_markdown_report,
)
from app.main import (
    GapRuntimeSummary,
    RuntimeSummaryJsonlWriter,
    _apply_market_universe_refresh,
    should_force_market_refresh,
)
from app.marketdata.market_universe import (
    RuntimeMarketUniverseManager,
    build_market_universe_diff,
    select_runtime_market_universe,
)
from app.marketdata.polymarket_discovery import PolymarketDiscoveryClient
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata, parse_market_metadata
from app.marketdata.polymarket_orderbook import PolymarketLocalOrderBook, TokenBookMetadata
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


def _current_market(market_id: str, *, base_asset: str = "BTC") -> PolymarketMarketMetadata:
    from app.core.clock import utc_now_ns

    now_ts = utc_now_ns() // 1_000_000_000
    return _market(market_id, start_ts=now_ts - 60, duration_s=300, base_asset=base_asset)


def _quote(
    token_id: str,
    market_id: str,
    *,
    ts: int,
    recv_monotonic_ns: int | None = None,
    best_bid: float | None = 0.49,
    best_ask: float | None = 0.51,
    best_bid_size: float | None = 100.0,
    best_ask_size: float | None = 100.0,
    book_complete: bool = True,
    book_stale: bool = False,
    validation_error: str | None = None,
    book_has_snapshot: bool = True,
    book_structurally_complete: bool = True,
    reported_best_validation_ok: bool = True,
) -> PolymarketQuote:
    mid = None if best_bid is None or best_ask is None else (best_bid + best_ask) / 2.0
    spread = None if best_bid is None or best_ask is None else best_ask - best_bid
    return PolymarketQuote(
        market_id=market_id,
        token_id=token_id,
        side_label="UP",
        best_bid=best_bid,
        best_bid_size=best_bid_size,
        best_ask=best_ask,
        best_ask_size=best_ask_size,
        mid_price=mid,
        spread=spread,
        event_ts=ts,
        received_ts=ts,
        exchange_event_ts=ts,
        local_received_ts=ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=recv_monotonic_ns,
        book_complete=book_complete,
        book_stale=book_stale,
        validation_error=validation_error,
        book_has_snapshot=book_has_snapshot,
        book_structurally_complete=book_structurally_complete,
        reported_best_validation_ok=reported_best_validation_ok,
    )


def _book_payload(token_id: str, market_id: str) -> bytes:
    return orjson.dumps(
        {
            "event_type": "book",
            "asset_id": token_id,
            "market": market_id,
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.51", "size": "10"}],
            "timestamp": "1700000000000",
        }
    )


class FakeWebSocket:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self.messages = list(messages)
        self.sent: list[str | bytes] = []
        self.closed = False

    async def recv(self) -> str | bytes:
        if self.closed:
            raise RuntimeError("websocket closed")
        if not self.messages:
            await asyncio.sleep(0)
            raise RuntimeError("no fake websocket messages left")
        return self.messages.pop(0)

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)

    async def ping(self) -> float:
        return 0.001

    async def close(self) -> None:
        self.closed = True


class FakeConnectContext:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return False


class FakeConnectFactory:
    def __init__(self, websockets: Sequence[FakeWebSocket]) -> None:
        self.websockets = list(websockets)
        self.connections: list[FakeWebSocket] = []

    def __call__(self, url: str, **kwargs: object) -> FakeConnectContext:
        del url, kwargs
        websocket = self.websockets.pop(0)
        self.connections.append(websocket)
        return FakeConnectContext(websocket)


class FakeLogger:
    def __init__(self) -> None:
        self.logged: list[object] = []

    async def log(self, event: object) -> None:
        self.logged.append(event)


class FakeDiscovery(PolymarketDiscoveryClient):
    def __init__(self, markets: Sequence[PolymarketMarketMetadata]) -> None:
        super().__init__(enable_direct_slug_lookup=False)
        self.markets = tuple(markets)
        self.discover_calls = 0

    async def discover(
        self,
        *,
        write_cache: bool = True,
        now_ts: int | None = None,
        rolling_lookahead_windows: int = 2,
        market_cache_ttl_ms: int = 60_000,
        discovery_debug_jsonl: str | None = None,
    ) -> tuple[PolymarketMarketMetadata, ...]:
        del write_cache, rolling_lookahead_windows, market_cache_ttl_ms, discovery_debug_jsonl
        self.discover_calls += 1
        current_ts = now_ts or 1_778_833_000
        from app.marketdata.polymarket_discovery import annotate_runtime_market_roles

        return annotate_runtime_market_roles(self.markets, now_ts=current_ts)


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


def _binance_tick(
    *,
    symbol: str = "BTCUSDT",
    price: float,
    ts: int,
    recv_monotonic_ns: int | None = None,
) -> MarketTick:
    return MarketTick(
        source="binance",
        symbol=symbol,
        price=price,
        size=1.0,
        exchange_event_ts=ts,
        exchange_ts_ns=ts,
        local_received_ts=ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=recv_monotonic_ns,
    )


def _order_top(
    *,
    symbol: str = "BTCUSDT",
    ts: int,
    recv_monotonic_ns: int | None = None,
) -> OrderBookTop:
    return OrderBookTop(
        source="binance",
        symbol=symbol,
        bid_price=100.0,
        bid_size=1.0,
        ask_price=100.1,
        ask_size=1.0,
        exchange_event_ts=ts,
        exchange_ts_ns=ts,
        local_received_ts=ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=recv_monotonic_ns,
    )


def _seed_pending_gap(
    *,
    market: PolymarketMarketMetadata | None = None,
    max_pending_gap_ms: float = 5_000.0,
    polymarket_stale_ms: float = 60_000.0,
    binance_stale_ms: float = 60_000.0,
    close_pending_on_tick_size_change: bool = False,
) -> tuple[MarketState, GapDetector, PolymarketMarketMetadata, int]:
    runtime_market = market or _market("quote-age", start_ts=900)
    base_ns = 1_000_000_000_000
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(runtime_market,),
        min_move_pct=0.1,
        reprice_threshold=0.01,
        min_exit_edge=0.0,
        max_pending_gap_ms=max_pending_gap_ms,
        max_entry_spread=0.03,
        require_book_ready=False,
        binance_stale_ms=binance_stale_ms,
        polymarket_stale_ms=polymarket_stale_ms,
        pre_entry_log_cooldown_ms=0.0,
        close_pending_on_tick_size_change=close_pending_on_tick_size_change,
    )
    state.apply(
        _quote(
            runtime_market.up_token_id or "",
            runtime_market.market_id,
            ts=base_ns,
            recv_monotonic_ns=1_000_000_000,
        )
    )
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_900_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(
        _binance_tick(
            price=101.0,
            ts=base_ns + 1_000_000_000,
            recv_monotonic_ns=2_000_000_000,
        )
    )
    assert second is not None
    observations = detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)
    assert observations == ()
    assert detector.stats(state, now_ts=base_ns + 1_000_000_000).pending_observation_count == 1
    return state, detector, runtime_market, base_ns + 1_000_000_000


def _orderbook(*, mode: str = "strict", tolerance_ticks: int = 1) -> PolymarketLocalOrderBook:
    return PolymarketLocalOrderBook(
        token_metadata={
            "token-up": TokenBookMetadata(
                condition_id="0xcondition",
                market_id="0xmarket",
                side_label="UP",
                market_slug="btc-updown-5m-900",
                base_asset="BTC",
                duration_minutes=5,
                token_outcome="Up",
                tick_size=0.01,
            )
        },
        stale_after_ms=60_000.0,
        best_validation_mode=mode,  # type: ignore[arg-type]
        best_validation_tolerance_ticks=tolerance_ticks,
        mismatch_sample_path=None,
    )


def _orderbook_market() -> PolymarketMarketMetadata:
    return _market("0xmarket", start_ts=900).model_copy(
        update={
            "up_token_id": "token-up",
            "down_token_id": "token-down",
            "token_outcomes": {"token-up": "Up", "token-down": "Down"},
            "market_slug": "btc-updown-5m-900",
        }
    )


def _market_payload_for_slug(
    slug: str,
    *,
    market_id: str,
    closed: bool = False,
) -> dict[str, object]:
    parts = slug.split("-")
    horizon = parts[2]
    start_ts = int(parts[3])
    duration_s = 300 if horizon == "5m" else 900
    return {
        "conditionId": f"{market_id}-condition",
        "market": market_id,
        "slug": slug,
        "question": f"{slug} up or down",
        "endDate": _iso(start_ts + duration_s),
        "eventStartTime": _iso(start_ts),
        "clobTokenIds": '["up-token","down-token"]',
        "outcomes": '["Up","Down"]',
        "order_price_min_tick_size": "0.01",
        "minimum_order_size": "5",
        "active": True,
        "closed": closed,
        "acceptingOrders": not closed,
        "enableOrderBook": True,
    }


def _orderbook_snapshot(orderbook: PolymarketLocalOrderBook, *, bid: str = "0.50", ask: str = "0.52") -> PolymarketQuote:
    return orderbook.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-up",
            "market": "0xmarket",
            "bids": [{"price": bid, "size": "15"}],
            "asks": [{"price": ask, "size": "25"}],
        },
        received_ts=1_000,
        parse_done_ts=1_001,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_001,
        event_ts=1_000,
        sequence="snapshot",
    )


def _orderbook_summary(orderbook: PolymarketLocalOrderBook) -> dict[str, object]:
    return orderbook.market_readiness_snapshot(
        (_orderbook_market(),),
        now_ts=1_700_000_001_000_000_000,
    )["summary"]


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(orjson.dumps(row).decode("utf-8") + "\n" for row in rows), encoding="utf-8")


def _report_row(
    index: int,
    *,
    tier: str = "A",
    reject_stage: str = "none",
    reject_reason: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": "BTCUSDT",
        "market_id": f"market-{index}",
        "market_slug": f"btc-updown-5m-{index}",
        "token_id": f"token-{index}",
        "direction": "UP",
        "duration_minutes": 5,
        "detected_ts_ns": 1_700_000_000_000_000_000 + index,
        "validation_mode": "tolerant",
        "data_quality_tier": tier,
        "data_quality_reason": "clean_validated" if tier == "A" else "diagnostic_only",
        "reject_stage": reject_stage,
        "quote_was_fillable": reject_stage == "none",
        "before_best_bid": 0.49,
        "before_best_ask": 0.50,
        "before_best_bid_size": 100.0,
        "before_best_ask_size": 100.0,
        "before_mid": 0.495,
        "after_best_bid": 0.52,
        "after_best_ask": 0.53,
        "after_mid": 0.525,
        "spread_before": 0.01,
        "spread_after": 0.01,
        "entry_ask": 0.50,
        "entry_ask_size": 100.0,
        "executable_exit_bid": 0.52,
        "exit_edge_after_spread": 0.02,
        "estimated_edge_after_spread": 0.02,
        "mid_repricing_delay_ms": 10.0,
        "executable_repricing_delay_ms": 20.0,
        "tradable_window_ms": 100.0,
        "tick_size_at_detection": 0.01,
        "exit_edge_ticks": 2.0,
        "spread_ticks_at_detection": 1.0,
        "reported_best_validation_ok_at_detection": True,
        "book_structurally_complete_at_detection": True,
        "book_has_snapshot_at_detection": True,
        "book_complete_at_detection": True,
        "market_quote_complete_rate_at_detection": 1.0,
        "token_quote_complete_rate_at_detection": 1.0,
        "stale_source": "none",
        "binance_quote_age_ms": 5.0,
        "polymarket_quote_age_ms": 8.0,
        "binance_move_pct": 0.08,
    }
    if reject_reason is not None:
        row["reject_reason"] = reject_reason
    return row


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


@pytest.mark.asyncio
async def test_polymarket_ws_reconnect_sends_new_subscription_after_market_update() -> None:
    stay = _current_market("stay")
    removed = _current_market("removed", base_asset="ETH")
    added = _current_market("added")
    first_ws = FakeWebSocket([_book_payload(stay.up_token_id or "", stay.market_id)])
    second_ws = FakeWebSocket([_book_payload(added.up_token_id or "", added.market_id)])
    factory = FakeConnectFactory([first_ws, second_ws])
    client = PolymarketWSClient(
        markets=(stay, removed),
        connect_factory=factory,
        mismatch_sample_path=None,
    )
    stream = client.stream(max_events=2, max_reconnect_attempts=0)

    first_event = await anext(stream)
    first_subscription = orjson.loads(first_ws.sent[0])
    client.update_markets((stay, added))
    second_event = await anext(stream)
    second_subscription = orjson.loads(second_ws.sent[0])

    assert isinstance(first_event, PolymarketQuote)
    assert isinstance(second_event, PolymarketQuote)
    assert first_subscription["assets_ids"] == list(stay.token_ids + removed.token_ids)
    assert set(second_subscription["assets_ids"]) == set(stay.token_ids + added.token_ids)
    assert first_ws.closed is True


@pytest.mark.asyncio
async def test_polymarket_ws_subscription_payload_matches_runtime_token_universe() -> None:
    stay = _current_market("stay")
    added = _current_market("added")
    first_ws = FakeWebSocket([_book_payload(stay.up_token_id or "", stay.market_id)])
    second_ws = FakeWebSocket([_book_payload(added.up_token_id or "", added.market_id)])
    client = PolymarketWSClient(
        markets=(stay,),
        connect_factory=FakeConnectFactory([first_ws, second_ws]),
        mismatch_sample_path=None,
    )
    stream = client.stream(max_events=2, max_reconnect_attempts=0)

    await anext(stream)
    client.update_markets((stay, added))
    await anext(stream)

    subscription = orjson.loads(second_ws.sent[0])
    assert set(subscription["assets_ids"]) == set(client.token_ids)
    assert set(client.active_subscription_token_ids) == set(client.token_ids)
    assert client.active_ws_token_subscription_count == len(client.token_ids)


@pytest.mark.asyncio
async def test_polymarket_ws_preserves_still_active_books_across_reconnect() -> None:
    stay = _current_market("stay")
    added = _current_market("added")
    first_ws = FakeWebSocket([_book_payload(stay.up_token_id or "", stay.market_id)])
    second_ws = FakeWebSocket([_book_payload(added.up_token_id or "", added.market_id)])
    client = PolymarketWSClient(
        markets=(stay,),
        connect_factory=FakeConnectFactory([first_ws, second_ws]),
        mismatch_sample_path=None,
    )
    stream = client.stream(max_events=2, max_reconnect_attempts=0)

    await anext(stream)
    client.update_markets((stay, added))
    await anext(stream)
    readiness = client.book_readiness_snapshot()

    assert readiness["tokens"][stay.up_token_id]["first_book_snapshot_ts_ns"] is not None


@pytest.mark.asyncio
async def test_polymarket_ws_initializes_new_token_books_after_reconnect() -> None:
    stay = _current_market("stay")
    added = _current_market("added")
    first_ws = FakeWebSocket([_book_payload(stay.up_token_id or "", stay.market_id)])
    second_ws = FakeWebSocket([_book_payload(added.up_token_id or "", added.market_id)])
    client = PolymarketWSClient(
        markets=(stay,),
        connect_factory=FakeConnectFactory([first_ws, second_ws]),
        mismatch_sample_path=None,
    )
    stream = client.stream(max_events=2, max_reconnect_attempts=0)

    await anext(stream)
    client.update_markets((stay, added))
    await anext(stream)
    readiness = client.book_readiness_snapshot()

    assert added.up_token_id in readiness["tokens"]
    assert added.down_token_id in readiness["tokens"]


@pytest.mark.asyncio
async def test_polymarket_ws_removed_tokens_not_active_after_refresh() -> None:
    stay = _current_market("stay")
    removed = _current_market("removed", base_asset="ETH")
    added = _current_market("added")
    first_ws = FakeWebSocket([_book_payload(stay.up_token_id or "", stay.market_id)])
    second_ws = FakeWebSocket([_book_payload(added.up_token_id or "", added.market_id)])
    client = PolymarketWSClient(
        markets=(stay, removed),
        connect_factory=FakeConnectFactory([first_ws, second_ws]),
        mismatch_sample_path=None,
    )
    stream = client.stream(max_events=2, max_reconnect_attempts=0)

    await anext(stream)
    client.update_markets((stay, added))
    await anext(stream)

    assert not (set(removed.token_ids) & set(client.active_subscription_token_ids))


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


def test_force_refresh_triggers_when_no_signal_and_binance_moves_continue() -> None:
    assert should_force_market_refresh(
        enabled=True,
        signal_enabled_markets=0,
        binance_move_total=7,
        last_forced_refresh_move_total=6,
        now_s=120.0,
        next_forced_refresh_allowed_at_s=60.0,
    )


@pytest.mark.asyncio
async def test_force_refresh_recovers_signal_markets_when_discovery_has_current_markets() -> None:
    market = _current_market("force-recovered")
    discovery = FakeDiscovery([market])
    manager = RuntimeMarketUniverseManager(
        discovery,
        (),
        refresh_interval_ms=60_000,
        lookahead_windows=1,
    )
    now_ts = int(datetime.now(tz=UTC).timestamp())

    diff = await manager.refresh(now_ts=now_ts, forced=True)
    snapshot = manager.snapshot(now_ts=now_ts)
    summary = GapRuntimeSummary(())
    detector = GapDetector(markets=())
    ws = PolymarketWSClient(markets=(), mismatch_sample_path=None)
    await _apply_market_universe_refresh(
        detector=detector,
        polymarket=ws,
        logger=FakeLogger(),
        runtime_summary=summary,
        snapshot=snapshot,
        diff=diff,
    )
    payload = summary.snapshot_payload(
        _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
        {"markets": []},
        ws_diagnostics=ws.subscription_diagnostics(),
    )

    assert discovery.discover_calls == 1
    assert manager.forced_market_refresh_count == 1
    assert payload["signal_enabled_markets_by_base_asset"]["BTC"] >= 1


def test_force_refresh_does_not_trigger_when_no_binance_moves() -> None:
    assert not should_force_market_refresh(
        enabled=True,
        signal_enabled_markets=0,
        binance_move_total=0,
        last_forced_refresh_move_total=0,
        now_s=120.0,
        next_forced_refresh_allowed_at_s=60.0,
    )


def test_force_refresh_respects_refresh_cooldown() -> None:
    assert not should_force_market_refresh(
        enabled=True,
        signal_enabled_markets=0,
        binance_move_total=7,
        last_forced_refresh_move_total=6,
        now_s=59.0,
        next_forced_refresh_allowed_at_s=60.0,
    )


@pytest.mark.asyncio
async def test_force_refresh_calls_detector_and_ws_market_update() -> None:
    market = _current_market("force-calls")
    discovery = FakeDiscovery([market])
    manager = RuntimeMarketUniverseManager(discovery, (), refresh_interval_ms=60_000)
    now_ts = int(datetime.now(tz=UTC).timestamp())
    diff = await manager.refresh(now_ts=now_ts, forced=True)
    snapshot = manager.snapshot(now_ts=now_ts)
    detector = GapDetector(markets=())
    ws = PolymarketWSClient(markets=(), mismatch_sample_path=None)

    await _apply_market_universe_refresh(
        detector=detector,
        polymarket=ws,
        logger=FakeLogger(),
        runtime_summary=GapRuntimeSummary(()),
        snapshot=snapshot,
        diff=diff,
    )

    assert detector.markets == snapshot.markets
    assert set(ws.token_ids) == set(snapshot.token_ids)
    assert ws.subscription_diagnostics()["runtime_token_count"] == len(snapshot.token_ids)


@pytest.mark.asyncio
async def test_runtime_refresh_preserves_previous_universe_on_transient_all_closed_discovery() -> None:
    now_ts = int(datetime.now(tz=UTC).timestamp())
    previous = _market("previous", start_ts=now_ts - 60, duration_s=300)
    closed = _market("closed", start_ts=now_ts - 60, duration_s=300, closed=True)
    discovery = FakeDiscovery([closed])
    manager = RuntimeMarketUniverseManager(
        discovery,
        (previous,),
        refresh_interval_ms=60_000,
        lookahead_windows=1,
    )

    diff = await manager.refresh(now_ts=now_ts, forced=True)

    assert [market.market_id for market in manager.markets] == ["previous"]
    assert diff.error == "market_discovery_failed_preserving_previous_universe"


@pytest.mark.asyncio
async def test_runtime_refresh_replaces_universe_when_active_events_fallback_finds_new_current(
    tmp_path: Path,
) -> None:
    now_ts = 1_778_832_999
    old = _market("old", start_ts=now_ts - 60, duration_s=300)

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _market_payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {
                    "markets": [
                        _market_payload_for_slug(
                            "btc-updown-5m-1778832900",
                            market_id="active-events-new",
                        )
                    ]
                }
            ]

    manager = RuntimeMarketUniverseManager(
        Client(cache_path=tmp_path / "cache.json"),
        (old,),
        refresh_interval_ms=60_000,
        lookahead_windows=1,
    )

    diff = await manager.refresh(now_ts=now_ts, forced=True)

    assert [market.market_id for market in manager.markets] == ["active-events-new"]
    assert diff.error is None


def test_runtime_summary_detects_subscription_token_divergence() -> None:
    market = _current_market("divergence")
    summary = GapRuntimeSummary((market,))
    diagnostics = {
        "runtime_token_count": 2,
        "active_ws_token_subscription_count": 1,
        "missing_active_tokens": [market.down_token_id],
        "extra_active_tokens": [],
        "subscription_out_of_sync": True,
        "subscription_transition_active": False,
        "subscription_update_count": 1,
        "websocket_reconnect_count": 0,
    }
    payload = summary.snapshot_payload(
        _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
        {"markets": []},
        ws_diagnostics=diagnostics,
    )

    assert payload["subscription_token_set_matches_runtime_universe"] is False
    assert payload["missing_subscription_token_count"] == 1
    assert payload["extra_subscription_token_count"] == 0
    assert payload["missing_subscription_tokens_sample"] == [market.down_token_id]


def test_runtime_summary_clears_subscription_divergence_after_reconnect() -> None:
    market = _current_market("divergence-clear")
    summary = GapRuntimeSummary((market,))
    summary.snapshot_payload(
        _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
        {"markets": []},
        ws_diagnostics={
            "runtime_token_count": 2,
            "active_ws_token_subscription_count": 1,
            "missing_active_tokens": [market.down_token_id],
            "extra_active_tokens": [],
            "subscription_out_of_sync": True,
            "subscription_transition_active": False,
        },
    )
    payload = summary.snapshot_payload(
        _stats(binance_moves_detected_by_symbol={"BTCUSDT": 2}),
        {"markets": []},
        ws_diagnostics={
            "runtime_token_count": 2,
            "active_ws_token_subscription_count": 2,
            "missing_active_tokens": [],
            "extra_active_tokens": [],
            "subscription_out_of_sync": False,
            "subscription_transition_active": False,
            "subscription_update_count": 1,
            "websocket_reconnect_count": 1,
        },
    )

    assert payload["subscription_token_set_matches_runtime_universe"] is True
    assert payload["missing_subscription_token_count"] == 0
    assert payload["ws_reconnect_count"] == 1


def test_no_event_warning_market_subscriptions_stale_when_token_sets_diverge() -> None:
    market = _current_market("divergence-warning")
    summary = GapRuntimeSummary((market,))
    summary._last_gap_event_change_ts_ns = 1  # noqa: SLF001
    summary._last_summary_ts_ns = 2  # noqa: SLF001
    summary._subscription_divergence_first_seen_ns = 1  # noqa: SLF001
    summary.record_binance_event(
        MarketTick(source="binance", symbol="BTCUSDT", price=100.0, size=1.0)
    )
    payload = summary.snapshot_payload(
        _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
        {"markets": []},
        ws_diagnostics={
            "runtime_token_count": 2,
            "active_ws_token_subscription_count": 1,
            "missing_active_tokens": [market.down_token_id],
            "extra_active_tokens": [],
            "subscription_out_of_sync": True,
            "subscription_transition_active": False,
        },
    )

    assert "market_subscriptions_stale" in payload["no_event_warnings"]
    assert "websocket_subscription_out_of_sync" in payload["no_event_warnings"]


def test_quote_age_populated_on_success_observation() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    quote = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
            best_bid=0.53,
            best_ask=0.55,
        )
    )
    assert isinstance(quote, PolymarketQuote)
    observations = detector.on_market_event(quote, state, now_ts=detected_ts + 200_000_000)

    assert len(observations) == 1
    observation = observations[0]
    assert observation.reject_stage == "none"
    assert observation.binance_quote_age_ms == pytest.approx(200.0)
    assert observation.polymarket_quote_age_ms == pytest.approx(0.0)
    assert observation.now_monotonic_ns == 2_200_000_000
    assert observation.last_binance_update_monotonic_ns == 2_000_000_000
    assert observation.last_polymarket_update_monotonic_ns == 2_200_000_000
    assert observation.binance_event_ts_ns == detected_ts
    assert observation.binance_local_received_ts_ns == detected_ts
    assert observation.polymarket_event_ts_ns == detected_ts + 200_000_000
    assert observation.polymarket_local_received_ts_ns == detected_ts + 200_000_000
    assert observation.state_updated_monotonic_ns is not None
    assert observation.detector_processed_monotonic_ns == 2_200_000_000


def test_quote_age_populated_on_timeout_observation() -> None:
    state, detector, _market_obj, detected_ts = _seed_pending_gap(max_pending_gap_ms=100.0)
    heartbeat = state.apply(
        _order_top(ts=detected_ts + 200_000_000, recv_monotonic_ns=2_200_000_000)
    )
    assert heartbeat is not None
    observations = detector.on_market_event(heartbeat, state, now_ts=detected_ts + 200_000_000)

    assert len(observations) == 1
    assert observations[0].reject_stage == "timeout"
    assert observations[0].binance_quote_age_ms == pytest.approx(0.0)
    assert observations[0].polymarket_quote_age_ms == pytest.approx(1_200.0)


def test_quote_age_populated_on_window_reject_observation() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    quote = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
            best_bid=0.49,
            best_ask=0.60,
        )
    )
    assert isinstance(quote, PolymarketQuote)
    observations = detector.on_market_event(quote, state, now_ts=detected_ts + 200_000_000)

    assert len(observations) == 1
    assert observations[0].reject_stage == "window"
    assert observations[0].reject_reason == "spread_too_wide"
    assert observations[0].binance_quote_age_ms == pytest.approx(200.0)
    assert observations[0].polymarket_quote_age_ms == pytest.approx(0.0)


def test_quote_age_populated_on_pre_entry_reject_observation() -> None:
    market = _market("pre-entry", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_000_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(
        _binance_tick(
            price=101.0,
            ts=base_ns + 1_000_000_000,
            recv_monotonic_ns=2_000_000_000,
        )
    )
    assert second is not None
    observations = detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)

    assert len(observations) == 1
    assert observations[0].reject_stage == "pre_entry"
    assert observations[0].reject_reason == "missing_quote"
    assert observations[0].binance_quote_age_ms == pytest.approx(0.0)
    assert observations[0].polymarket_quote_age_ms is None


def test_quote_age_populated_on_lifecycle_close_observation() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="market_resolved",
            event_ts=detected_ts + 200_000_000,
            received_ts=detected_ts + 200_000_000,
            local_received_ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 200_000_000)

    assert len(observations) == 1
    assert observations[0].reject_stage == "lifecycle"
    assert observations[0].binance_quote_age_ms == pytest.approx(200.0)
    assert observations[0].polymarket_quote_age_ms == pytest.approx(1_200.0)


def test_missing_quote_age_is_null_not_zero() -> None:
    market = _market("missing-age", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    state.apply(_quote(market.up_token_id or "", market.market_id, ts=base_ns))
    first = state.apply(_binance_tick(price=100.0, ts=base_ns))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(_binance_tick(price=101.0, ts=base_ns + 1_000_000_000))
    assert second is not None
    detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)
    quote = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=base_ns + 1_200_000_000,
            best_bid=0.53,
            best_ask=0.55,
        )
    )
    assert isinstance(quote, PolymarketQuote)
    observations = detector.on_market_event(quote, state, now_ts=base_ns + 1_200_000_000)

    assert observations[0].binance_quote_age_ms is None
    assert observations[0].polymarket_quote_age_ms is None


def test_quote_age_uses_monotonic_timestamps_not_wall_clock_mixed() -> None:
    market = _market("mixed-age", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    state.apply(_quote(market.up_token_id or "", market.market_id, ts=base_ns))
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_000_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(
        _binance_tick(
            price=101.0,
            ts=base_ns + 1_000_000_000,
            recv_monotonic_ns=2_000_000_000,
        )
    )
    assert second is not None
    observations = detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)

    assert observations == ()
    quote = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=base_ns + 1_200_000_000,
            recv_monotonic_ns=None,
            best_bid=0.53,
            best_ask=0.55,
        )
    )
    assert isinstance(quote, PolymarketQuote)
    observations = detector.on_market_event(quote, state, now_ts=base_ns + 1_200_000_000)

    assert observations[0].binance_quote_age_ms is None
    assert observations[0].polymarket_quote_age_ms is None


def test_stale_polymarket_quote_is_visible_to_detector() -> None:
    market = _market("stale-visible", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=1.0)
    detector = GapDetector(markets=(market,), require_book_ready=False)
    quote = state.apply(_quote(market.up_token_id or "", market.market_id, ts=1))

    assert isinstance(quote, PolymarketQuote)
    assert quote.book_stale is True
    assert quote.validation_error == "quote_stale"
    detector.on_market_event(quote, state, now_ts=1_000_000_000_000)


def test_stale_polymarket_quote_not_used_as_clean_tradable_data() -> None:
    market = _market("stale-not-clean", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=1.0)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=1.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    state.apply(_quote(market.up_token_id or "", market.market_id, ts=base_ns - 10_000_000))
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_000_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    second = state.apply(
        _binance_tick(
            price=101.0,
            ts=base_ns + 1_000_000_000,
            recv_monotonic_ns=2_000_000_000,
        )
    )
    assert second is not None
    observations = detector.on_market_event(second, state, now_ts=base_ns + 1_000_000_000)

    assert observations[0].reject_reason in {"quote_stale", "book_incomplete"}
    assert observations[0].quote_was_fillable is False
    assert observations[0].data_quality_tier == "D"


def test_pending_gap_closes_with_quote_stale_when_polymarket_quote_stale() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    detector.polymarket_stale_ms = 50.0
    stale = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
            book_stale=True,
            book_complete=False,
            validation_error="quote_stale",
        )
    )
    assert isinstance(stale, PolymarketQuote)
    observations = detector.on_market_event(stale, state, now_ts=detected_ts + 200_000_000)

    assert observations[0].reject_stage == "window"
    assert observations[0].reject_reason == "quote_stale"


def test_stale_quote_does_not_become_generic_timeout_when_stale_is_cause() -> None:
    state, detector, market, detected_ts = _seed_pending_gap(max_pending_gap_ms=5_000.0)
    detector.polymarket_stale_ms = 50.0
    stale = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
            book_stale=True,
            book_complete=False,
            validation_error="quote_stale",
        )
    )
    assert isinstance(stale, PolymarketQuote)
    observations = detector.on_market_event(stale, state, now_ts=detected_ts + 200_000_000)

    assert observations[0].reject_reason == "quote_stale"
    assert observations[0].reject_reason != "max_observation_lifetime_reached"


def test_stale_quote_diagnostic_includes_quote_age_when_available() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    detector.polymarket_stale_ms = 50.0
    stale = state.apply(
        _quote(
            market.up_token_id or "",
            market.market_id,
            ts=detected_ts + 200_000_000,
            recv_monotonic_ns=2_200_000_000,
            book_stale=True,
            book_complete=False,
            validation_error="quote_stale",
        )
    )
    assert isinstance(stale, PolymarketQuote)
    observations = detector.on_market_event(stale, state, now_ts=detected_ts + 200_000_000)

    assert observations[0].polymarket_quote_age_ms == pytest.approx(0.0)
    assert observations[0].binance_quote_age_ms == pytest.approx(200.0)


def test_tick_size_change_updates_tick_size_without_market_invalid_by_default() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="tick_size_change",
            old_tick_size=0.01,
            new_tick_size=0.001,
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert observations == ()
    assert market.market_id not in state.invalid_polymarket_markets
    assert detector.stats(state, now_ts=detected_ts + 100_000_000).pending_observation_count == 1
    assert detector._tick_size_for_market(market) == pytest.approx(0.001)  # type: ignore[attr-defined]


def test_tick_size_change_closes_pending_only_when_assumption_invalidated() -> None:
    state, detector, market, detected_ts = _seed_pending_gap(close_pending_on_tick_size_change=True)
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="tick_size_change",
            old_tick_size=0.01,
            new_tick_size=0.001,
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert observations[0].reject_stage == "lifecycle"
    assert observations[0].reject_reason == "tick_size_change"
    assert market.market_id not in state.invalid_polymarket_markets


def test_market_resolved_invalidates_market_and_closes_pending() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="market_resolved",
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert market.market_id in state.invalid_polymarket_markets
    assert observations[0].reject_reason == "market_resolved"


def test_market_closed_invalidates_market_and_closes_pending() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="closed",
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert market.market_id in state.invalid_polymarket_markets
    assert observations[0].reject_reason == "closed"


def test_market_expired_invalidates_market_and_closes_pending() -> None:
    state, detector, market, detected_ts = _seed_pending_gap()
    lifecycle = state.apply(
        MarketLifecycleEvent(
            market_id=market.market_id,
            lifecycle_type="expired",
            event_ts=detected_ts + 100_000_000,
            received_ts=detected_ts + 100_000_000,
        )
    )
    assert isinstance(lifecycle, MarketLifecycleEvent)
    observations = detector.on_market_event(lifecycle, state, now_ts=detected_ts + 100_000_000)

    assert market.market_id in state.invalid_polymarket_markets
    assert observations[0].reject_reason == "expired"


def test_new_market_lifecycle_triggers_refresh_needed() -> None:
    summary = GapRuntimeSummary(())
    summary.record_polymarket_event(
        MarketLifecycleEvent(market_id="new-market", lifecycle_type="new_market")
    )

    assert summary.consume_lifecycle_refresh_request() is True
    assert summary.consume_lifecycle_refresh_request() is False


def test_orderbook_reports_bid_mismatch() -> None:
    orderbook = _orderbook()
    _orderbook_snapshot(orderbook)
    orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.47", "best_ask": "0.52"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert _orderbook_summary(orderbook)["reported_best_bid_mismatch"] == 1


def test_orderbook_reports_ask_mismatch() -> None:
    orderbook = _orderbook()
    _orderbook_snapshot(orderbook)
    orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.50", "best_ask": "0.55"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert _orderbook_summary(orderbook)["reported_best_ask_mismatch"] == 1


def test_orderbook_reports_best_bid_size_unknown() -> None:
    orderbook = _orderbook()
    _orderbook_snapshot(orderbook)
    orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.47", "best_ask": "0.52"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert _orderbook_summary(orderbook)["best_bid_size_unknown"] == 1


def test_orderbook_reports_best_ask_size_unknown() -> None:
    orderbook = _orderbook()
    _orderbook_snapshot(orderbook)
    orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.50", "best_ask": "0.55"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert _orderbook_summary(orderbook)["best_ask_size_unknown"] == 1


def test_orderbook_reports_price_change_before_snapshot() -> None:
    orderbook = _orderbook()
    quote = orderbook.apply_price_change(
        {"asset_id": "token-up", "side": "BUY", "price": "0.50", "size": "15"},
        parent_payload={"market": "0xmarket"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert quote.book_complete is False
    assert _orderbook_summary(orderbook)["price_change_before_snapshot"] == 1


def test_orderbook_reports_best_bid_ask_before_snapshot() -> None:
    orderbook = _orderbook()
    orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.50", "best_ask": "0.52"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert _orderbook_summary(orderbook)["best_bid_ask_before_snapshot"] == 1


def test_orderbook_reports_crossed_book() -> None:
    orderbook = _orderbook()
    quote = _orderbook_snapshot(orderbook, bid="0.55", ask="0.52")

    assert quote.validation_error == "crossed_book"
    assert _orderbook_summary(orderbook)["crossed_book"] == 1


def test_orderbook_reports_empty_book() -> None:
    orderbook = _orderbook()
    quote = orderbook.apply_book(
        {"event_type": "book", "asset_id": "token-up", "market": "0xmarket", "bids": [], "asks": []},
        received_ts=1_000,
        parse_done_ts=1_001,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_001,
        event_ts=1_000,
        sequence=None,
    )

    assert quote.validation_error == "empty_book"
    assert _orderbook_summary(orderbook)["empty_book"] == 1


def test_one_tick_mismatch_tolerated_only_in_tolerant_mode() -> None:
    strict = _orderbook(mode="strict")
    tolerant = _orderbook(mode="tolerant")
    _orderbook_snapshot(strict)
    _orderbook_snapshot(tolerant)
    payload = {
        "event_type": "best_bid_ask",
        "asset_id": "token-up",
        "best_bid": "0.49",
        "best_ask": "0.52",
    }

    strict_quote = strict.apply_best_bid_ask(
        payload,
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )
    tolerant_quote = tolerant.apply_best_bid_ask(
        payload,
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert strict_quote.book_complete is False
    assert tolerant_quote.book_complete is True


def test_above_tolerance_mismatch_not_clean() -> None:
    orderbook = _orderbook(mode="tolerant")
    _orderbook_snapshot(orderbook)
    quote = orderbook.apply_best_bid_ask(
        {"event_type": "best_bid_ask", "asset_id": "token-up", "best_bid": "0.47", "best_ask": "0.52"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert quote.reported_best_validation_ok is False
    assert quote.book_complete is False


def test_structurally_incomplete_book_not_tier_a_or_b() -> None:
    market = _market("structural", start_ts=900)
    detector = GapDetector(markets=(market,))
    tier, reason = detector._data_quality(  # type: ignore[attr-defined]
        market=market,
        quote_was_fillable=True,
        book_has_snapshot=True,
        book_structurally_complete=False,
        reported_best_validation_ok=True,
        validation_error="crossed_book",
        validation_mode="tolerant",
        market_quote_complete_rate=1.0,
        best_ask_size=100.0,
        best_bid_size=100.0,
        tick_size=0.01,
    )

    assert tier == "D"
    assert reason == "structurally_incomplete"


def test_d_tier_rows_excluded_from_primary_empirical_buckets(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(
        path,
        [
            _report_row(1, tier="A"),
            _report_row(2, tier="D", reject_stage="pre_entry", reject_reason="book_incomplete"),
        ],
    )

    report = build_phase4_dataset_quality_report(path)

    assert report["dataset_health"]["primary_rows"] == 1
    assert report["empirical_bucket_analysis"]["primary_row_count"] == 1


def test_candidate_episode_id_stable_for_same_source_move_window() -> None:
    market = _market("episode", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_000_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    ids: list[str] = []
    for offset_ns in (1_100_000_000, 1_200_000_000):
        tick = state.apply(
            _binance_tick(
                price=101.0 + offset_ns / 1_000_000_000_000,
                ts=base_ns + offset_ns,
                recv_monotonic_ns=1_000_000_000 + offset_ns,
            )
        )
        assert tick is not None
        observations = detector.on_market_event(tick, state, now_ts=base_ns + offset_ns)
        ids.extend(obs.move_episode_id or "" for obs in observations)

    assert len(ids) == 2
    assert len(set(ids)) == 1


def test_candidate_episode_id_changes_for_new_source_move_window() -> None:
    market = _market("episode-change", start_ts=900)
    state = MarketState(max_polymarket_quote_age_ms=10**15)
    detector = GapDetector(
        markets=(market,),
        min_move_pct=0.1,
        require_book_ready=False,
        binance_stale_ms=60_000.0,
        polymarket_stale_ms=60_000.0,
        pre_entry_log_cooldown_ms=0.0,
    )
    base_ns = 1_000_000_000_000
    first = state.apply(_binance_tick(price=100.0, ts=base_ns, recv_monotonic_ns=1_000_000_000))
    assert first is not None
    detector.on_market_event(first, state, now_ts=base_ns)
    first_move = state.apply(
        _binance_tick(price=101.0, ts=base_ns + 1_000_000_000, recv_monotonic_ns=2_000_000_000)
    )
    assert first_move is not None
    first_observation = detector.on_market_event(first_move, state, now_ts=base_ns + 1_000_000_000)[0]
    second_move = state.apply(
        _binance_tick(price=102.0, ts=base_ns + 2_000_000_000, recv_monotonic_ns=3_000_000_000)
    )
    assert second_move is not None
    second_observation = detector.on_market_event(second_move, state, now_ts=base_ns + 2_000_000_000)[0]

    assert first_observation.move_episode_id != second_observation.move_episode_id


def test_candidate_duplicate_suppressed_counter_increments() -> None:
    state, detector, _market_obj, detected_ts = _seed_pending_gap()
    duplicate = state.apply(
        _binance_tick(
            price=102.0,
            ts=detected_ts + 100_000_000,
            recv_monotonic_ns=2_100_000_000,
        )
    )
    assert duplicate is not None
    detector.on_market_event(duplicate, state, now_ts=detected_ts + 100_000_000)

    assert detector.stats(state).candidate_duplicate_suppressed_count == 1


def test_candidates_per_market_window_counter_updates() -> None:
    state, detector, market, _detected_ts = _seed_pending_gap()
    stats = detector.stats(state)

    assert any(key.startswith(f"{market.market_id}:") for key in stats.candidates_per_market_window)


def test_runtime_summary_jsonl_is_utf8_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    writer = RuntimeSummaryJsonlWriter(str(path))
    writer.write({"event_type": "runtime_summary", "message": "ascii diagnostic"})

    decoded = path.read_bytes().decode("utf-8")
    assert decoded.endswith("\n")
    assert orjson.loads(decoded.splitlines()[0])["event_type"] == "runtime_summary"


def test_runtime_summary_jsonl_contains_lifecycle_fields(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    market = _current_market("jsonl-life")
    summary = GapRuntimeSummary((market,))
    writer = RuntimeSummaryJsonlWriter(str(path))
    payload = summary.snapshot_payload(_stats(), {"markets": []}, ws_diagnostics={})
    writer.write(payload)

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert "current_signal_markets_by_base_asset" in row
    assert "next_warmup_markets_by_base_asset" in row
    assert "market_lifecycle_diff" in row


def test_runtime_summary_jsonl_contains_subscription_fields(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    market = _current_market("jsonl-sub")
    summary = GapRuntimeSummary((market,))
    writer = RuntimeSummaryJsonlWriter(str(path))
    payload = summary.snapshot_payload(
        _stats(),
        {"markets": []},
        ws_diagnostics={
            "runtime_token_count": 2,
            "active_ws_token_subscription_count": 2,
            "subscription_out_of_sync": False,
            "subscription_transition_active": False,
        },
    )
    writer.write(payload)

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert row["runtime_token_count"] == 2
    assert row["subscription_token_set_matches_runtime_universe"] is True
    assert "subscription_diagnostics" in row


def test_runtime_summary_jsonl_contains_symbol_counters(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    summary = GapRuntimeSummary(())
    summary.record_binance_event(
        MarketTick(source="binance", symbol="BTCUSDT", price=100.0, size=1.0)
    )
    writer = RuntimeSummaryJsonlWriter(str(path))
    writer.write(
        summary.snapshot_payload(
            _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
            {"markets": []},
            ws_diagnostics={},
        )
    )

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert row["binance_events_seen_by_symbol"] == {"BTCUSDT": 1}
    assert row["binance_moves_detected_by_symbol"] == {"BTCUSDT": 1}
    assert "candidates_created_by_symbol" in row


def test_runtime_summary_jsonl_contains_no_event_warnings(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    summary = GapRuntimeSummary(())
    summary._last_gap_event_change_ts_ns = 1  # noqa: SLF001
    summary.record_binance_event(
        MarketTick(source="binance", symbol="BTCUSDT", price=100.0, size=1.0)
    )
    writer = RuntimeSummaryJsonlWriter(str(path))
    writer.write(
        summary.snapshot_payload(
            _stats(binance_moves_detected_by_symbol={"BTCUSDT": 1}),
            {"markets": []},
            ws_diagnostics={"runtime_token_count": 0, "active_ws_token_subscription_count": 0},
        )
    )

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert "no_signal_enabled_markets_while_binance_moves_continue" in row["no_event_warnings"]


def test_final_runtime_summary_is_written_on_shutdown_if_writer_enabled(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    writer = RuntimeSummaryJsonlWriter(str(path))
    summary = GapRuntimeSummary(())
    writer.write(summary.snapshot_payload(_stats(), {"markets": []}, ws_diagnostics={}, final=True))

    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert row["final"] is True


def test_no_duplicate_periodic_summary_blocks(tmp_path: Path) -> None:
    path = tmp_path / "runtime.jsonl"
    writer = RuntimeSummaryJsonlWriter(str(path))
    writer.write({"event_type": "runtime_summary", "final": False})

    assert len(path.read_bytes().splitlines()) == 1


def test_cohort_sensitivity_small_sample_is_insufficient_data(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_report_row(index) for index in range(5)])

    report = build_phase4_dataset_quality_report(path)

    assert report["cohort_sensitivity"]["conclusion"] == "insufficient_data"


def test_staleness_unknown_when_quote_age_missing(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    row = _report_row(1)
    row.pop("stale_source")
    row.pop("binance_quote_age_ms")
    row.pop("polymarket_quote_age_ms")
    _write_rows(path, [row])

    report = build_phase4_dataset_quality_report(path)

    assert report["stale_feed_analysis"]["staleness_status"] == "unknown_missing_quote_age_fields"
    assert report["stale_feed_analysis"]["quote_stale_rate"] is None


def test_mismatch_summary_rendered_in_markdown(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    samples = tmp_path / "mismatch_samples.jsonl"
    _write_rows(path, [_report_row(1, tier="B")])
    _write_rows(
        samples,
        [
            {
                "market_id": "m1",
                "token_id": "t1",
                "error_type": "reported_best_bid_mismatch",
                "tick_diff": 1,
            },
            {
                "market_id": "m2",
                "token_id": "t2",
                "error_type": "reported_best_ask_mismatch",
                "tick_diff": 3,
            },
        ],
    )

    report = build_phase4_dataset_quality_report(path, mismatch_samples_path=samples)
    markdown = render_phase4_markdown_report(report)

    assert "Mismatch sample total: 2" in markdown
    assert "Mismatch by error type" in markdown
    assert "Pct within 1 tick" in markdown
    assert "Pct above 2 ticks" in markdown
    assert "Top affected markets" in markdown
    assert "Top affected tokens" in markdown


def test_report_remains_needs_more_data_for_small_dirty_dataset(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(
        path,
        [
            _report_row(1, tier="D", reject_stage="pre_entry", reject_reason="book_incomplete"),
            _report_row(2, tier="D", reject_stage="window", reject_reason="quote_stale"),
        ],
    )

    report = build_phase4_dataset_quality_report(path, include_diagnostic=True)

    assert report["readiness_assessment"]["classification"] in {"NOT_READY", "NEEDS_MORE_DATA"}
    assert report["readiness_assessment"]["classification"] != "READY_FOR_BASELINE_MODEL_RESEARCH"
