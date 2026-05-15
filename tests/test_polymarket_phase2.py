import asyncio
from collections.abc import Sequence
from types import TracebackType

import orjson
import pytest

from app.core.clock import utc_now_ns
from app.core.events import MarketLifecycleEvent, MarketTick, PolymarketQuote
from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    PolymarketMarketMetadata,
    flatten_token_ids,
    parse_market_metadata,
)
from app.marketdata.polymarket_ws import PolymarketWSClient
from app.state.market_state import MarketState


class FakeWebSocket:
    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self.messages = list(messages)
        self.sent: list[str | bytes] = []
        self.ping_calls = 0
        self.closed = False

    async def recv(self) -> str | bytes:
        if not self.messages:
            raise AssertionError("fake websocket has no messages left")
        return self.messages.pop(0)

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)

    async def ping(self) -> float:
        self.ping_calls += 1
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
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket
        self.urls: list[str] = []

    def __call__(self, url: str, **kwargs: object) -> FakeConnectContext:
        self.urls.append(url)
        return FakeConnectContext(self.websocket)


def _metadata() -> PolymarketMarketMetadata:
    return PolymarketMarketMetadata(
        condition_id="0xcondition",
        market_id="0xmarket",
        market_slug="bitcoin-up-or-down-15m",
        question="Bitcoin Up or Down - 15 minute",
        end_time="2026-05-15T12:15:00Z",
        up_token_id="yes-token",
        down_token_id="no-token",
        token_outcomes={"yes-token": "Up", "no-token": "Down"},
        tick_size=0.01,
        min_order_size=5.0,
        base_asset="BTC",
        duration_minutes=15,
    )


def test_discovery_parses_and_caches_short_duration_markets(tmp_path) -> None:
    payload = {
        "conditionId": "0xcondition",
        "market": "0xmarket",
        "slug": "bitcoin-up-or-down-15m",
        "question": "Bitcoin Up or Down - 15 minute",
        "endDateIso": "2026-05-15T12:15:00Z",
        "clobTokenIds": '["yes-token","no-token"]',
        "outcomes": '["Up","Down"]',
        "order_price_min_tick_size": "0.01",
        "minimum_order_size": "5",
        "active": True,
        "closed": False,
    }
    cache_path = tmp_path / "polymarket_markets.json"
    client = PolymarketDiscoveryClient(
        cache_path=cache_path,
        fetch_markets=lambda: [
            payload,
            {**payload, "slug": "random-election-market", "question": "Random election"},
        ],
    )

    markets = asyncio.run(client.discover())
    cached = client.read_cache()

    assert len(markets) == 1
    assert parse_market_metadata(payload) == markets[0]
    assert markets[0].up_token_id == "yes-token"
    assert markets[0].down_token_id == "no-token"
    assert markets[0].token_for_direction("UP") == "yes-token"
    assert markets[0].token_for_direction("DOWN") == "no-token"
    assert markets[0].duration_minutes == 15
    assert cached.markets == list(markets)
    assert "replace-me" not in cache_path.read_text()


def test_normalizes_polymarket_book_price_change_and_trade() -> None:
    market = _metadata()
    client = PolymarketWSClient(markets=(market,), token_ids=flatten_token_ids((market,)))
    received_ts = 1_700_000_000_100_000_000

    book = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "book",
                "asset_id": "yes-token",
                "market": "0xmarket",
                "bids": [
                    {"price": ".48", "size": "30"},
                    {"price": ".50", "size": "15"},
                ],
                "asks": [
                    {"price": ".52", "size": "25"},
                    {"price": ".53", "size": "60"},
                ],
                "timestamp": "1700000000000",
                "hash": "0xabc",
            }
        ),
        received_ts=received_ts,
    )[0]
    price_change = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "price_change",
                "market": "0xmarket",
                "timestamp": "1700000000001",
                "price_changes": [
                    {
                        "asset_id": "no-token",
                        "price": "0.49",
                        "size": "200",
                        "side": "SELL",
                        "hash": "0xdef",
                        "best_bid": "0.48",
                        "best_ask": "0.51",
                    }
                ],
            }
        ),
        received_ts=received_ts,
    )[0]
    trade = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "last_trade_price",
                "asset_id": "yes-token",
                "market": "0xmarket",
                "price": "0.456",
                "side": "BUY",
                "size": "219.217767",
                "timestamp": "1700000000002",
            }
        ),
        received_ts=received_ts,
    )[0]

    assert isinstance(book, PolymarketQuote)
    assert book.side_label == "UP"
    assert book.best_bid == 0.50
    assert book.best_ask == 0.52
    assert book.mid_price == pytest.approx(0.51)
    assert book.spread == pytest.approx(0.02)
    assert book.available_liquidity_at_best == 40.0
    assert book.event_ts == 1_700_000_000_000_000_000

    assert isinstance(price_change, PolymarketQuote)
    assert price_change.side_label == "DOWN"
    assert price_change.best_bid is None
    assert price_change.best_ask == 0.49
    assert price_change.best_bid_size is None
    assert price_change.best_ask_size == 200.0
    assert price_change.book_complete is False
    assert price_change.available_liquidity_at_best == 200.0

    assert isinstance(trade, MarketTick)
    assert trade.source == "polymarket"
    assert trade.symbol == "yes-token"
    assert trade.price == 0.456
    assert trade.size == pytest.approx(219.217767)


@pytest.mark.asyncio
async def test_polymarket_stream_sends_subscription_and_yields_events() -> None:
    market = _metadata()
    websocket = FakeWebSocket(
        [
            orjson.dumps(
                {
                    "event_type": "best_bid_ask",
                    "market": "0xmarket",
                    "asset_id": "yes-token",
                    "best_bid": "0.73",
                    "best_ask": "0.77",
                    "spread": "0.04",
                    "timestamp": "1700000000000",
                }
            )
        ]
    )
    factory = FakeConnectFactory(websocket)
    client = PolymarketWSClient(
        markets=(market,),
        connect_factory=factory,
    )

    events = [event async for event in client.stream(max_events=1)]

    subscription = orjson.loads(websocket.sent[0])
    assert factory.urls == ["wss://ws-subscriptions-clob.polymarket.com/ws/market"]
    assert subscription["assets_ids"] == ["yes-token", "no-token"]
    assert subscription["type"] == "market"
    assert subscription["custom_feature_enabled"] is True
    assert isinstance(events[0], PolymarketQuote)
    assert events[0].best_bid == 0.73
    assert events[0].best_ask == 0.77
    assert events[0].best_bid_size is None
    assert events[0].best_ask_size is None
    assert events[0].book_complete is False


def test_polymarket_lifecycle_events_are_not_silently_ignored() -> None:
    market = _metadata()
    client = PolymarketWSClient(markets=(market,), token_ids=flatten_token_ids((market,)))

    event = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "tick_size_change",
                "market": "0xmarket",
                "asset_id": "yes-token",
                "old_tick_size": "0.01",
                "new_tick_size": "0.001",
                "timestamp": "1700000000000",
            }
        ),
        received_ts=1_700_000_000_100_000_000,
    )[0]

    assert isinstance(event, MarketLifecycleEvent)
    assert event.lifecycle_type == "tick_size_change"
    assert event.market_id == "0xmarket"
    assert event.token_id == "yes-token"
    assert event.old_tick_size == pytest.approx(0.01)
    assert event.new_tick_size == pytest.approx(0.001)


def test_polymarket_market_resolved_lifecycle_invalidates_book() -> None:
    market = _metadata()
    client = PolymarketWSClient(markets=(market,), token_ids=flatten_token_ids((market,)))
    received_ts = 1_700_000_000_100_000_000

    lifecycle = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "market_resolved",
                "market": "0xmarket",
                "asset_id": "yes-token",
                "timestamp": "1700000000000",
            }
        ),
        received_ts=received_ts,
    )[0]
    quote = client.normalize_message(
        orjson.dumps(
            {
                "event_type": "book",
                "asset_id": "yes-token",
                "market": "0xmarket",
                "bids": [{"price": ".48", "size": "30"}],
                "asks": [{"price": ".52", "size": "25"}],
                "timestamp": "1700000000001",
                "hash": "0xabc",
            }
        ),
        received_ts=received_ts + 1,
    )[0]

    assert isinstance(lifecycle, MarketLifecycleEvent)
    assert lifecycle.lifecycle_type == "market_resolved"
    assert isinstance(quote, PolymarketQuote)
    assert quote.book_complete is False


def test_market_state_rejects_stale_polymarket_quote() -> None:
    state = MarketState(max_polymarket_quote_age_ms=1.0)
    stale = PolymarketQuote(
        market_id="0xmarket",
        token_id="yes-token",
        side_label="YES",
        best_bid=0.40,
        best_bid_size=10.0,
        best_ask=0.42,
        best_ask_size=10.0,
        mid_price=0.41,
        spread=0.02,
        event_ts=1,
        received_ts=1,
    )
    fresh_ts = utc_now_ns()
    fresh = stale.model_copy(
        update={
            "event_ts": fresh_ts,
            "received_ts": fresh_ts,
        }
    )

    assert state.apply(stale) is None
    assert "yes-token" not in state.polymarket_quotes

    updated = state.apply(fresh)
    assert isinstance(updated, PolymarketQuote)
    assert state.polymarket_quotes["yes-token"].best_bid == 0.40


def test_market_state_applies_lifecycle_and_marks_market_invalid() -> None:
    state = MarketState(max_polymarket_quote_age_ms=60_000.0)
    quote = PolymarketQuote(
        market_id="0xmarket",
        token_id="yes-token",
        side_label="UP",
        best_bid=0.40,
        best_bid_size=10.0,
        best_ask=0.42,
        best_ask_size=10.0,
        mid_price=0.41,
        spread=0.02,
        event_ts=utc_now_ns(),
        received_ts=utc_now_ns(),
        book_complete=True,
    )
    assert isinstance(state.apply(quote), PolymarketQuote)

    lifecycle = MarketLifecycleEvent(
        market_id="0xmarket",
        token_id="yes-token",
        lifecycle_type="market_resolved",
        event_ts=quote.event_ts,
        received_ts=quote.received_ts,
    )
    updated = state.apply(lifecycle)

    assert isinstance(updated, MarketLifecycleEvent)
    assert state.is_market_invalid("0xmarket") is True
    assert state.lifecycle_events_by_market["0xmarket"].lifecycle_type == "market_resolved"
    assert state.polymarket_quotes["yes-token"].book_complete is False
    assert state.polymarket_quotes["yes-token"].validation_error == "market_invalidated"
