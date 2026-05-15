from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from typing import Any, Protocol, cast

import orjson
import structlog
import websockets

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import MarketTick, PolymarketQuote, PolymarketSideLabel
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata

DEFAULT_POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class WebSocketConnection(Protocol):
    async def recv(self) -> str | bytes:
        ...

    async def send(self, message: str | bytes) -> None:
        ...

    async def ping(self) -> float:
        ...

    async def close(self) -> None:
        ...


ConnectFactory = Callable[..., AbstractAsyncContextManager[WebSocketConnection]]
PolymarketStreamEvent = PolymarketQuote | MarketTick


class PolymarketHeartbeatError(RuntimeError):
    """Raised when the market websocket heartbeat fails."""


class PolymarketMessageError(ValueError):
    """Raised when a Polymarket market message cannot be normalized."""


class PolymarketWSClient:
    """Async Polymarket market-channel client for public CLOB data."""

    def __init__(
        self,
        url: str = DEFAULT_POLYMARKET_WS_URL,
        *,
        token_ids: Iterable[str] = (),
        markets: Iterable[PolymarketMarketMetadata] = (),
        token_side_labels: Mapping[str, PolymarketSideLabel] | None = None,
        connect_factory: ConnectFactory | None = None,
        heartbeat_timeout_sec: float = 30.0,
        ping_timeout_sec: float = 5.0,
        initial_backoff_sec: float = 0.25,
        max_backoff_sec: float = 8.0,
        max_queue: int = 1024,
    ) -> None:
        self.url = _market_ws_url(url)
        self.market_metadata = tuple(markets)
        self.token_ids = tuple(str(token_id) for token_id in token_ids) or tuple(
            token_id for market in self.market_metadata for token_id in market.token_ids
        )
        self._token_info = _build_token_info(self.market_metadata, self.token_ids, token_side_labels)
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.ping_timeout_sec = ping_timeout_sec
        self.initial_backoff_sec = initial_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.max_queue = max_queue
        self._connect_factory = cast(ConnectFactory, connect_factory or websockets.connect)
        self._logger = structlog.get_logger("polymarket_ws")

    def subscription_message(self) -> bytes:
        return orjson.dumps(
            {
                "assets_ids": list(self.token_ids),
                "type": "market",
                "custom_feature_enabled": True,
            }
        )

    async def stream(
        self,
        *,
        max_events: int | None = None,
        max_reconnect_attempts: int | None = None,
    ) -> AsyncIterator[PolymarketStreamEvent]:
        if not self.token_ids:
            return

        yielded = 0
        reconnect_attempt = 0

        while True:
            try:
                async with self._connect_factory(
                    self.url,
                    ping_interval=None,
                    close_timeout=1,
                    max_queue=self.max_queue,
                ) as websocket:
                    await websocket.send(self.subscription_message())
                    self._logger.info(
                        "polymarket_ws_connected",
                        url=self.url,
                        token_count=len(self.token_ids),
                    )
                    reconnect_attempt = 0

                    while True:
                        raw_message = await self._recv_with_heartbeat(websocket)
                        received_ts = utc_now_ns()
                        recv_monotonic_ns = monotonic_now_ns()
                        for event in self.normalize_message(
                            raw_message,
                            received_ts=received_ts,
                            recv_monotonic_ns=recv_monotonic_ns,
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
                    "polymarket_ws_reconnecting",
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
                    self._logger.debug("polymarket_ws_heartbeat_ok")
                except Exception as exc:
                    with suppress(Exception):
                        await websocket.close()
                    raise PolymarketHeartbeatError("Polymarket websocket heartbeat failed") from exc

    def normalize_message(
        self,
        raw_message: str | bytes,
        *,
        received_ts: int | None = None,
        recv_monotonic_ns: int | None = None,
    ) -> tuple[PolymarketStreamEvent, ...]:
        local_received_ts = received_ts or utc_now_ns()
        recv_mono = recv_monotonic_ns or monotonic_now_ns()
        payloads = _decode_payloads(raw_message)
        parse_done_ts = utc_now_ns()
        parse_done_mono = monotonic_now_ns()
        events: list[PolymarketStreamEvent] = []

        for payload in payloads:
            event_type = str(payload.get("event_type", ""))
            if event_type == "book":
                events.append(
                    _quote_from_book(
                        payload,
                        token_info=self._token_info,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                    )
                )
            elif event_type in {"price_change", "best_bid_ask"}:
                events.extend(
                    _quotes_from_price_message(
                        payload,
                        token_info=self._token_info,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                    )
                )
            elif event_type in {"last_trade_price", "trade"}:
                events.append(
                    _tick_from_trade(
                        payload,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                    )
                )
            elif event_type in {"tick_size_change", "new_market", "market_resolved"}:
                continue
            else:
                raise PolymarketMessageError(f"unsupported Polymarket message: {payload!r}")

        return tuple(events)

    def _backoff_delay(self, attempt: int) -> float:
        return min(self.max_backoff_sec, self.initial_backoff_sec * (2**attempt))


type TokenInfo = tuple[str | None, str, PolymarketSideLabel]


def _build_token_info(
    markets: Iterable[PolymarketMarketMetadata],
    token_ids: Iterable[str],
    overrides: Mapping[str, PolymarketSideLabel] | None,
) -> dict[str, TokenInfo]:
    token_info: dict[str, TokenInfo] = {}
    for market in markets:
        for token_id, outcome in market.token_outcomes.items():
            token_info[token_id] = (
                market.condition_id,
                market.market_id,
                _side_label_for_outcome(outcome),
            )

    if overrides:
        for token_id, side_label in overrides.items():
            condition_id, market_id, _ = token_info.get(token_id, (None, "", side_label))
            token_info[token_id] = (condition_id, market_id, side_label)

    for index, token_id in enumerate(token_ids):
        token_info.setdefault(
            token_id,
            (None, "", "UNKNOWN"),
        )

    return token_info


def _market_ws_url(url: str) -> str:
    cleaned = url.rstrip("/")
    if cleaned.endswith("/market"):
        return cleaned
    if cleaned.endswith("/ws"):
        return f"{cleaned}/market"
    return cleaned


def _decode_payloads(raw_message: str | bytes) -> tuple[dict[str, Any], ...]:
    decoded = orjson.loads(raw_message)
    if isinstance(decoded, list):
        return tuple(item for item in decoded if isinstance(item, dict))
    if isinstance(decoded, dict):
        return (decoded,)
    raise PolymarketMessageError("Polymarket websocket payload must be an object or list")


def _quote_from_book(
    payload: dict[str, Any],
    *,
    token_info: Mapping[str, TokenInfo],
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
) -> PolymarketQuote:
    token_id = str(payload["asset_id"])
    bids = _levels(payload.get("bids", []))
    asks = _levels(payload.get("asks", []))
    best_bid, best_bid_size = _best_bid(bids)
    best_ask, best_ask_size = _best_ask(asks)
    event_ts = _timestamp_to_ns(payload.get("timestamp"))
    condition_id, market_id, side_label = _token_info(token_info, token_id, payload)
    return _quote(
        condition_id=condition_id,
        market_id=market_id,
        token_id=token_id,
        side_label=side_label,
        best_bid=best_bid,
        best_ask=best_ask,
        best_bid_size=best_bid_size,
        best_ask_size=best_ask_size,
        event_ts=event_ts,
        received_ts=received_ts,
        parse_done_ts=parse_done_ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=parse_done_monotonic_ns,
        sequence=payload.get("hash"),
    )


def _quotes_from_price_message(
    payload: dict[str, Any],
    *,
    token_info: Mapping[str, TokenInfo],
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
) -> tuple[PolymarketQuote, ...]:
    event_ts = _timestamp_to_ns(payload.get("timestamp"))
    changes = payload.get("price_changes")
    if isinstance(changes, list):
        rows = [row for row in changes if isinstance(row, dict)]
    else:
        rows = [payload]

    quotes: list[PolymarketQuote] = []
    for row in rows:
        token_id = str(row.get("asset_id") or payload.get("asset_id"))
        condition_id, market_id, side_label = _token_info(token_info, token_id, payload)
        best_bid = _optional_float(row.get("best_bid"))
        best_ask = _optional_float(row.get("best_ask"))
        size = _optional_float(row.get("size"))
        quotes.append(
            _quote(
                condition_id=condition_id,
                market_id=market_id,
                token_id=token_id,
                side_label=side_label,
                best_bid=best_bid,
                best_ask=best_ask,
                best_bid_size=size if str(row.get("side", "")).upper() == "BUY" else None,
                best_ask_size=size if str(row.get("side", "")).upper() == "SELL" else None,
                event_ts=event_ts,
                received_ts=received_ts,
                parse_done_ts=parse_done_ts,
                recv_monotonic_ns=recv_monotonic_ns,
                parse_done_monotonic_ns=parse_done_monotonic_ns,
                sequence=row.get("hash"),
            )
        )
    return tuple(quotes)


def _tick_from_trade(
    payload: dict[str, Any],
    *,
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
) -> MarketTick:
    event_ts = _timestamp_to_ns(payload.get("timestamp"))
    return MarketTick(
        source="polymarket",
        symbol=str(payload.get("asset_id") or payload.get("token_id")),
        price=_float_from(payload["price"]),
        size=_float_from(payload.get("size", 0.0)),
        exchange_event_ts=event_ts,
        exchange_ts_ns=event_ts,
        local_received_ts=received_ts,
        parse_done_ts=parse_done_ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=parse_done_monotonic_ns,
        latency_ms=_latency_ms(recv_monotonic_ns, parse_done_monotonic_ns),
    )


def _quote(
    *,
    condition_id: str | None,
    market_id: str,
    token_id: str,
    side_label: PolymarketSideLabel,
    best_bid: float | None,
    best_ask: float | None,
    best_bid_size: float | None,
    best_ask_size: float | None,
    event_ts: int | None,
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
    sequence: object,
) -> PolymarketQuote:
    mid_price = _mid(best_bid, best_ask)
    spread = None if best_bid is None or best_ask is None else max(0.0, best_ask - best_bid)
    return PolymarketQuote(
        market_id=market_id,
        condition_id=condition_id,
        token_id=token_id,
        side_label=side_label,
        best_bid=best_bid,
        best_bid_size=best_bid_size,
        best_ask=best_ask,
        best_ask_size=best_ask_size,
        mid_price=mid_price,
        spread=spread,
        event_ts=event_ts,
        received_ts=received_ts,
        exchange_event_ts=event_ts,
        exchange_ts_ns=event_ts,
        local_received_ts=received_ts,
        parse_done_ts=parse_done_ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=parse_done_monotonic_ns,
        latency_ms=_latency_ms(recv_monotonic_ns, parse_done_monotonic_ns),
        sequence=_sequence_or_none(sequence),
    )


def _token_info(
    token_info: Mapping[str, TokenInfo],
    token_id: str,
    payload: Mapping[str, Any],
) -> TokenInfo:
    condition_id, mapped_market_id, side_label = token_info.get(token_id, (None, "", "YES"))
    market_id = mapped_market_id or str(payload.get("market") or "")
    return condition_id, market_id, side_label


def _levels(rows: object) -> list[tuple[float, float]]:
    if not isinstance(rows, list):
        raise PolymarketMessageError("book levels must be a list")
    levels: list[tuple[float, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise PolymarketMessageError("book level must be an object")
        levels.append((_float_from(row["price"]), _float_from(row["size"])))
    return levels


def _best_bid(levels: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    price, size = max(levels, key=lambda level: level[0])
    return price, size


def _best_ask(levels: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    price, size = min(levels, key=lambda level: level[0])
    return price, size


def _timestamp_to_ns(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value)
    if not raw:
        return None
    timestamp = int(raw)
    digits = len(raw.lstrip("-"))
    if digits <= 10:
        return timestamp * 1_000_000_000
    if digits <= 13:
        return timestamp * 1_000_000
    if digits <= 16:
        return timestamp * 1_000
    return timestamp


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _float_from(value)


def _float_from(value: object) -> float:
    if isinstance(value, int | float | str | bytes):
        return float(value)
    raise PolymarketMessageError(f"expected float-like value, got {value!r}")


def _mid(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / 2.0


def _latency_ms(start_ts: int | None, end_ts: int | None) -> float | None:
    if start_ts is None or end_ts is None:
        return None
    return (end_ts - start_ts) / 1_000_000.0


def _sequence_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return abs(hash(str(value)))


def _side_label_for_outcome(outcome: str) -> PolymarketSideLabel:
    normalized = outcome.strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in {"YES", "NO", "UP", "DOWN", "ABOVE", "BELOW", "HIGHER", "LOWER"}:
        return cast(PolymarketSideLabel, normalized)
    return "UNKNOWN"
