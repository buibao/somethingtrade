import asyncio
from collections.abc import Sequence
from types import TracebackType

import orjson
import pytest

from app.core.events import DepthUpdate, MarketTick, OrderBookTop
from app.marketdata.binance_ws import BinanceWSClient
from app.state.market_state import MarketState


class FakeWebSocket:
    def __init__(
        self,
        messages: Sequence[str | bytes],
        *,
        recv_error: Exception | None = None,
        timeout_first_recv: bool = False,
    ) -> None:
        self.messages = list(messages)
        self.recv_error = recv_error
        self.timeout_first_recv = timeout_first_recv
        self.recv_calls = 0
        self.ping_calls = 0
        self.closed = False

    async def recv(self) -> str | bytes:
        self.recv_calls += 1
        if self.recv_error is not None:
            raise self.recv_error
        if self.timeout_first_recv and self.recv_calls == 1:
            await asyncio.sleep(0.05)
        if not self.messages:
            raise AssertionError("fake websocket has no messages left")
        return self.messages.pop(0)

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
    def __init__(self, websockets: list[FakeWebSocket]) -> None:
        self.websockets = websockets
        self.urls: list[str] = []

    def __call__(self, url: str, **kwargs: object) -> FakeConnectContext:
        self.urls.append(url)
        if not self.websockets:
            raise AssertionError("fake connect factory has no websockets left")
        return FakeConnectContext(self.websockets.pop(0))


def _combined(stream: str, data: dict[str, object]) -> bytes:
    return orjson.dumps({"stream": stream, "data": data})


def test_normalizes_binance_combined_stream_messages() -> None:
    client = BinanceWSClient(symbols=("BTCUSDT",))
    local_received_ts = 1_700_000_000_100_000_000

    tick = client.normalize_message(
        _combined(
            "btcusdt@aggTrade",
            {
                "e": "aggTrade",
                "E": 1_700_000_000_000,
                "s": "BTCUSDT",
                "a": 42,
                "p": "50000.25",
                "q": "0.010",
            },
        ),
        local_received_ts=local_received_ts,
    )[0]
    top = client.normalize_message(
        _combined(
            "btcusdt@bookTicker",
            {
                "u": 101,
                "s": "BTCUSDT",
                "b": "50000.00",
                "B": "1.5",
                "a": "50000.50",
                "A": "2.5",
            },
        ),
        local_received_ts=local_received_ts,
    )[0]
    depth = client.normalize_message(
        _combined(
            "btcusdt@depth@100ms",
            {
                "e": "depthUpdate",
                "E": 1_700_000_000_001,
                "s": "BTCUSDT",
                "U": 102,
                "u": 105,
                "b": [["49999.50", "0.75"]],
                "a": [["50001.00", "1.25"]],
            },
        ),
        local_received_ts=local_received_ts,
    )[0]

    assert isinstance(tick, MarketTick)
    assert tick.symbol == "BTCUSDT"
    assert tick.price == 50000.25
    assert tick.exchange_event_ts == 1_700_000_000_000_000_000
    assert tick.local_received_ts == local_received_ts
    assert tick.parse_done_ts is not None

    assert isinstance(top, OrderBookTop)
    assert top.bid_price == 50000.0
    assert top.ask_price == 50000.5
    assert top.sequence == 101

    assert isinstance(depth, DepthUpdate)
    assert depth.first_update_id == 102
    assert depth.final_update_id == 105
    assert depth.bids[0].price == 49999.5
    assert depth.asks[0].size == 1.25


@pytest.mark.asyncio
async def test_stream_uses_mocked_websocket_messages() -> None:
    messages = [
        _combined(
            "btcusdt@aggTrade",
            {
                "e": "aggTrade",
                "E": 1_700_000_000_000,
                "s": "BTCUSDT",
                "a": 1,
                "p": "100.00",
                "q": "0.25",
            },
        ),
        _combined(
            "btcusdt@bookTicker",
            {
                "u": 2,
                "s": "BTCUSDT",
                "b": "99.50",
                "B": "3.0",
                "a": "100.50",
                "A": "4.0",
            },
        ),
        _combined(
            "btcusdt@depth@100ms",
            {
                "e": "depthUpdate",
                "E": 1_700_000_000_010,
                "s": "BTCUSDT",
                "U": 3,
                "u": 4,
                "b": [["99.00", "1.0"]],
                "a": [["101.00", "1.0"]],
            },
        ),
    ]
    factory = FakeConnectFactory([FakeWebSocket(messages)])
    client = BinanceWSClient(symbols=("BTCUSDT",), connect_factory=factory)

    events = [event async for event in client.stream(max_events=3)]

    assert [event.event_type for event in events] == [
        "market_tick",
        "order_book_top",
        "depth_update",
    ]
    assert "btcusdt@aggTrade" in factory.urls[0]
    assert "btcusdt@bookTicker" in factory.urls[0]
    assert "btcusdt@depth@100ms" in factory.urls[0]


@pytest.mark.asyncio
async def test_stream_reconnects_with_backoff() -> None:
    factory = FakeConnectFactory(
        [
            FakeWebSocket([], recv_error=OSError("connection dropped")),
            FakeWebSocket(
                [
                    _combined(
                        "ethusdt@bookTicker",
                        {
                            "u": 5,
                            "s": "ETHUSDT",
                            "b": "4000.00",
                            "B": "1.0",
                            "a": "4001.00",
                            "A": "2.0",
                        },
                    )
                ]
            ),
        ]
    )
    client = BinanceWSClient(
        symbols=("ETHUSDT",),
        connect_factory=factory,
        initial_backoff_sec=0.0,
    )

    events = [event async for event in client.stream(max_events=1, max_reconnect_attempts=1)]

    assert len(factory.urls) == 2
    assert isinstance(events[0], OrderBookTop)
    assert events[0].symbol == "ETHUSDT"


@pytest.mark.asyncio
async def test_heartbeat_ping_runs_when_recv_times_out() -> None:
    websocket = FakeWebSocket(
        [
            _combined(
                "btcusdt@bookTicker",
                {
                    "u": 10,
                    "s": "BTCUSDT",
                    "b": "100.00",
                    "B": "1.0",
                    "a": "101.00",
                    "A": "1.0",
                },
            )
        ],
        timeout_first_recv=True,
    )
    client = BinanceWSClient(
        symbols=("BTCUSDT",),
        connect_factory=FakeConnectFactory([websocket]),
        heartbeat_timeout_sec=0.001,
        ping_timeout_sec=0.01,
    )

    events = [event async for event in client.stream(max_events=1)]

    assert len(events) == 1
    assert websocket.ping_calls == 1


def test_market_state_tracks_latest_values_and_returns() -> None:
    state = MarketState()
    base_ts = 1_700_000_000_000_000_000

    first = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=100.0,
            size=1.0,
            exchange_event_ts=base_ts,
            local_received_ts=base_ts + 1_000_000,
            parse_done_ts=base_ts + 2_000_000,
        )
    )
    second = state.apply(
        MarketTick(
            source="binance",
            symbol="BTCUSDT",
            price=101.0,
            size=1.0,
            exchange_event_ts=base_ts + 1_000_000_000,
            local_received_ts=base_ts + 1_001_000_000,
            parse_done_ts=base_ts + 1_002_000_000,
        )
    )
    top = state.apply(
        OrderBookTop(
            source="binance",
            symbol="BTCUSDT",
            bid_price=100.5,
            bid_size=3.0,
            ask_price=101.5,
            ask_size=4.0,
            local_received_ts=base_ts + 1_003_000_000,
        )
    )

    symbol_state = state.symbols["BTCUSDT"]
    assert isinstance(first, MarketTick)
    assert isinstance(second, MarketTick)
    assert isinstance(top, OrderBookTop)
    assert second.state_updated_ts is not None
    assert second.latency_ms is not None
    assert symbol_state.latest_price == 101.0
    assert symbol_state.best_bid == 100.5
    assert symbol_state.best_ask == 101.5
    assert symbol_state.rolling_returns["1s"] == pytest.approx(0.01)
    assert symbol_state.rolling_returns["5s"] is None
