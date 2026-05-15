import asyncio
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import AbstractAsyncContextManager, suppress
from typing import Any, Protocol, cast

import orjson
import structlog
import websockets

from app.core.clock import utc_now_ns
from app.core.events import BinanceMarketEvent, BookLevel, DepthUpdate, MarketTick, OrderBookTop

DEFAULT_BINANCE_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_BINANCE_STREAMS = ("aggTrade", "bookTicker", "depth@100ms")


class WebSocketConnection(Protocol):
    async def recv(self) -> str | bytes:
        ...

    async def ping(self) -> float:
        ...

    async def close(self) -> None:
        ...


ConnectFactory = Callable[..., AbstractAsyncContextManager[WebSocketConnection]]


class BinanceHeartbeatError(RuntimeError):
    """Raised when the websocket heartbeat cannot be confirmed."""


class BinanceMessageError(ValueError):
    """Raised when a Binance websocket payload cannot be normalized."""


class BinanceWSClient:
    """Async Binance combined-stream client with reconnect and heartbeat support."""

    def __init__(
        self,
        url: str = "wss://stream.binance.com:9443/ws",
        *,
        symbols: Iterable[str] = DEFAULT_BINANCE_SYMBOLS,
        streams: Iterable[str] = DEFAULT_BINANCE_STREAMS,
        connect_factory: ConnectFactory | None = None,
        heartbeat_timeout_sec: float = 30.0,
        ping_timeout_sec: float = 5.0,
        initial_backoff_sec: float = 0.25,
        max_backoff_sec: float = 8.0,
        max_queue: int = 1024,
    ) -> None:
        self.base_url = url
        self.symbols = tuple(symbol.upper() for symbol in symbols)
        self.streams = tuple(stream for stream in streams)
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.ping_timeout_sec = ping_timeout_sec
        self.initial_backoff_sec = initial_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.max_queue = max_queue
        self._connect_factory = cast(ConnectFactory, connect_factory or websockets.connect)
        self._logger = structlog.get_logger("binance_ws")

    @property
    def stream_names(self) -> tuple[str, ...]:
        return tuple(
            f"{symbol.lower()}@{stream}"
            for symbol in self.symbols
            for stream in self.streams
        )

    @property
    def stream_url(self) -> str:
        if "streams=" in self.base_url:
            return self.base_url

        base = self.base_url.rstrip("/")
        if base.endswith("/ws"):
            base = base[:-3]
        if not base.endswith("/stream"):
            base = f"{base}/stream"

        return f"{base}?streams={'/'.join(self.stream_names)}"

    async def stream(
        self,
        *,
        max_events: int | None = None,
        max_reconnect_attempts: int | None = None,
    ) -> AsyncIterator[BinanceMarketEvent]:
        """Yield normalized Binance events forever, reconnecting with backoff."""

        yielded = 0
        reconnect_attempt = 0

        while True:
            try:
                async with self._connect_factory(
                    self.stream_url,
                    ping_interval=None,
                    close_timeout=1,
                    max_queue=self.max_queue,
                ) as websocket:
                    self._logger.info("binance_ws_connected", url=self.stream_url)
                    reconnect_attempt = 0

                    while True:
                        raw_message = await self._recv_with_heartbeat(websocket)
                        local_received_ts = utc_now_ns()
                        for event in self.normalize_message(
                            raw_message,
                            local_received_ts=local_received_ts,
                        ):
                            yield event
                            yielded += 1
                            if max_events is not None and yielded >= max_events:
                                return

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if (
                    max_reconnect_attempts is not None
                    and reconnect_attempt >= max_reconnect_attempts
                ):
                    raise

                delay = self._backoff_delay(reconnect_attempt)
                self._logger.warning(
                    "binance_ws_reconnecting",
                    error=str(exc),
                    delay_sec=delay,
                    attempt=reconnect_attempt + 1,
                )
                reconnect_attempt += 1
                await asyncio.sleep(delay)

    async def _recv_with_heartbeat(self, websocket: WebSocketConnection) -> str | bytes:
        while True:
            try:
                return await asyncio.wait_for(
                    websocket.recv(),
                    timeout=self.heartbeat_timeout_sec,
                )
            except TimeoutError:
                try:
                    await asyncio.wait_for(websocket.ping(), timeout=self.ping_timeout_sec)
                    self._logger.debug("binance_ws_heartbeat_ok")
                except Exception as exc:
                    with suppress(Exception):
                        await websocket.close()
                    raise BinanceHeartbeatError("Binance websocket heartbeat failed") from exc

    def normalize_message(
        self,
        raw_message: str | bytes,
        *,
        local_received_ts: int | None = None,
    ) -> tuple[BinanceMarketEvent, ...]:
        local_ts = local_received_ts or utc_now_ns()
        payload = _decode_payload(raw_message)
        data = _message_data(payload)
        stream_name = str(payload.get("stream", ""))
        parse_done_ts = utc_now_ns()

        if _is_agg_trade(data, stream_name):
            return (
                _normalize_agg_trade(
                    data,
                    local_received_ts=local_ts,
                    parse_done_ts=parse_done_ts,
                ),
            )

        if _is_book_ticker(data, stream_name):
            return (
                _normalize_book_ticker(
                    data,
                    stream_name=stream_name,
                    local_received_ts=local_ts,
                    parse_done_ts=parse_done_ts,
                ),
            )

        if _is_depth_update(data, stream_name):
            return (
                _normalize_depth_update(
                    data,
                    local_received_ts=local_ts,
                    parse_done_ts=parse_done_ts,
                ),
            )

        raise BinanceMessageError(f"unsupported Binance message: {data!r}")

    def _backoff_delay(self, attempt: int) -> float:
        return min(self.max_backoff_sec, self.initial_backoff_sec * (2**attempt))


def _decode_payload(raw_message: str | bytes) -> dict[str, Any]:
    decoded = orjson.loads(raw_message)
    if not isinstance(decoded, dict):
        raise BinanceMessageError("Binance websocket payload must be a JSON object")
    return cast(dict[str, Any], decoded)


def _message_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise BinanceMessageError("Binance websocket data must be a JSON object")
    return cast(dict[str, Any], data)


def _is_agg_trade(data: dict[str, Any], stream_name: str) -> bool:
    return data.get("e") == "aggTrade" or stream_name.endswith("@aggTrade")


def _is_book_ticker(data: dict[str, Any], stream_name: str) -> bool:
    return stream_name.endswith("@bookTicker") or {"u", "b", "B", "a", "A"}.issubset(data)


def _is_depth_update(data: dict[str, Any], stream_name: str) -> bool:
    return data.get("e") == "depthUpdate" or "@depth" in stream_name


def _normalize_agg_trade(
    data: dict[str, Any],
    *,
    local_received_ts: int,
    parse_done_ts: int,
) -> MarketTick:
    exchange_event_ts = _event_ts_ms_to_ns(data.get("E") or data.get("T"))
    return MarketTick(
        source="binance",
        symbol=str(data["s"]).upper(),
        price=float(data["p"]),
        size=float(data["q"]),
        exchange_event_ts=exchange_event_ts,
        exchange_ts_ns=exchange_event_ts,
        local_received_ts=local_received_ts,
        parse_done_ts=parse_done_ts,
        latency_ms=_latency_ms(exchange_event_ts or local_received_ts, parse_done_ts),
        sequence=_int_or_none(data.get("a")),
    )


def _normalize_book_ticker(
    data: dict[str, Any],
    *,
    stream_name: str,
    local_received_ts: int,
    parse_done_ts: int,
) -> OrderBookTop:
    symbol = str(data.get("s") or stream_name.split("@", maxsplit=1)[0]).upper()
    return OrderBookTop(
        source="binance",
        symbol=symbol,
        bid_price=float(data["b"]),
        bid_size=float(data["B"]),
        ask_price=float(data["a"]),
        ask_size=float(data["A"]),
        local_received_ts=local_received_ts,
        parse_done_ts=parse_done_ts,
        latency_ms=_latency_ms(local_received_ts, parse_done_ts),
        sequence=_int_or_none(data.get("u")),
    )


def _normalize_depth_update(
    data: dict[str, Any],
    *,
    local_received_ts: int,
    parse_done_ts: int,
) -> DepthUpdate:
    exchange_event_ts = _event_ts_ms_to_ns(data.get("E"))
    return DepthUpdate(
        source="binance",
        symbol=str(data["s"]).upper(),
        first_update_id=int(data["U"]),
        final_update_id=int(data["u"]),
        previous_final_update_id=_int_or_none(data.get("pu")),
        bids=_book_levels(data.get("b", [])),
        asks=_book_levels(data.get("a", [])),
        exchange_event_ts=exchange_event_ts,
        exchange_ts_ns=exchange_event_ts,
        local_received_ts=local_received_ts,
        parse_done_ts=parse_done_ts,
        latency_ms=_latency_ms(exchange_event_ts or local_received_ts, parse_done_ts),
        sequence=_int_or_none(data.get("u")),
    )


def _book_levels(rows: object) -> list[BookLevel]:
    if not isinstance(rows, list):
        raise BinanceMessageError("depth levels must be a list")

    levels: list[BookLevel] = []
    for row in rows:
        if not isinstance(row, list | tuple) or len(row) < 2:
            raise BinanceMessageError("depth level must contain price and size")
        levels.append(BookLevel(price=_float_from(row[0]), size=_float_from(row[1])))
    return levels


def _event_ts_ms_to_ns(value: object) -> int | None:
    if value is None:
        return None
    return _int_from(value) * 1_000_000


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    return _int_from(value)


def _int_from(value: object) -> int:
    if isinstance(value, int | float | str | bytes):
        return int(value)
    raise BinanceMessageError(f"expected integer-like value, got {value!r}")


def _float_from(value: object) -> float:
    if isinstance(value, int | float | str | bytes):
        return float(value)
    raise BinanceMessageError(f"expected float-like value, got {value!r}")


def _latency_ms(start_ts: int | None, end_ts: int | None) -> float | None:
    if start_ts is None or end_ts is None:
        return None
    return (end_ts - start_ts) / 1_000_000.0
