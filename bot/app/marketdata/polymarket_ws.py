from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from typing import Any, Protocol, cast

import orjson
import structlog
import websockets

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import (
    MarketLifecycleEvent,
    MarketLifecycleType,
    MarketTick,
    PolymarketQuote,
    PolymarketSideLabel,
)
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.marketdata.polymarket_orderbook import PolymarketLocalOrderBook, TokenBookMetadata

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
PolymarketStreamEvent = PolymarketQuote | MarketTick | MarketLifecycleEvent


class PolymarketHeartbeatError(RuntimeError):
    """Raised when the market websocket heartbeat fails."""


class PolymarketMessageError(ValueError):
    """Raised when a Polymarket market message cannot be normalized."""


class PolymarketWSClient:
    """Async Polymarket market-channel client backed by a local CLOB book."""

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
        orderbook_stale_after_ms: float = 1_000.0,
        best_validation_mode: str = "tolerant",
        best_validation_tolerance_ticks: int = 1,
        mismatch_sample_path: str | None = "data/debug/polymarket_orderbook_mismatch_samples.jsonl",
        mismatch_sample_per_token_per_min: int = 20,
    ) -> None:
        self.url = _market_ws_url(url)
        self.market_metadata = tuple(markets)
        self.token_ids = tuple(str(token_id) for token_id in token_ids) or tuple(
            token_id for market in self.market_metadata for token_id in market.token_ids
        )
        self._token_side_label_overrides = token_side_labels
        token_metadata = _build_token_metadata(
            self.market_metadata,
            self.token_ids,
            self._token_side_label_overrides,
        )
        self._orderbooks = PolymarketLocalOrderBook(
            token_metadata=token_metadata,
            stale_after_ms=orderbook_stale_after_ms,
            best_validation_mode=best_validation_mode,  # type: ignore[arg-type]
            best_validation_tolerance_ticks=best_validation_tolerance_ticks,
            mismatch_sample_path=mismatch_sample_path,
            mismatch_sample_per_token_per_min=mismatch_sample_per_token_per_min,
        )
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.ping_timeout_sec = ping_timeout_sec
        self.initial_backoff_sec = initial_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.max_queue = max_queue
        self._connect_factory = cast(ConnectFactory, connect_factory or websockets.connect)
        self._logger = structlog.get_logger("polymarket_ws")
        self._subscription_version = 0
        self._active_subscription_version = -1
        self._active_subscription_token_ids: tuple[str, ...] = ()
        self._subscription_transition_active = False
        self._subscription_update_count = 0
        self._subscription_reconnect_count = 0
        self._active_websocket: WebSocketConnection | None = None

    def subscription_message(self) -> bytes:
        return orjson.dumps(
            {
                "assets_ids": list(self.token_ids),
                "type": "market",
                "custom_feature_enabled": True,
            }
        )

    def update_markets(
        self,
        markets: Iterable[PolymarketMarketMetadata],
        *,
        token_ids: Iterable[str] | None = None,
    ) -> None:
        new_markets = tuple(markets)
        new_tokens = tuple(str(token_id) for token_id in (token_ids or ())) or tuple(
            token_id for market in new_markets for token_id in market.token_ids
        )
        old_tokens = set(self.token_ids)
        if tuple(new_tokens) == self.token_ids and new_markets == self.market_metadata:
            return

        self.market_metadata = new_markets
        self.token_ids = tuple(new_tokens)
        token_metadata = _build_token_metadata(
            self.market_metadata,
            self.token_ids,
            self._token_side_label_overrides,
        )
        self._orderbooks.update_token_metadata(
            token_metadata,
            active_token_ids=self.token_ids,
        )
        self._subscription_version += 1
        self._subscription_update_count += 1
        self._subscription_transition_active = True
        self._logger.info(
            "polymarket_ws_subscription_update_requested",
            token_count=len(self.token_ids),
            added_tokens=sorted(set(self.token_ids) - old_tokens),
            removed_tokens=sorted(old_tokens - set(self.token_ids)),
            subscription_version=self._subscription_version,
        )
        if self._active_websocket is not None:
            try:
                asyncio.get_running_loop().create_task(self._active_websocket.close())
            except RuntimeError:
                pass

    @property
    def active_subscription_token_ids(self) -> tuple[str, ...]:
        return self._active_subscription_token_ids

    @property
    def active_ws_token_subscription_count(self) -> int:
        return len(self.active_subscription_token_ids)

    @property
    def subscription_transition_active(self) -> bool:
        return self._subscription_transition_active

    @property
    def websocket_reconnect_count(self) -> int:
        return self._subscription_reconnect_count

    def subscription_diagnostics(self) -> dict[str, Any]:
        runtime_tokens = set(self.token_ids)
        active_tokens = set(self.active_subscription_token_ids)
        active_established = self._active_subscription_version >= 0
        if not active_established:
            status = "pending"
            out_of_sync: bool | None = None
        elif self._subscription_transition_active:
            status = "transition"
            out_of_sync = runtime_tokens != active_tokens
        elif runtime_tokens == active_tokens:
            status = "active"
            out_of_sync = False
        else:
            status = "out_of_sync"
            out_of_sync = True
        return {
            "runtime_token_count": len(runtime_tokens),
            "active_ws_token_subscription_count": len(active_tokens),
            "active_subscription_established": active_established,
            "subscription_status": status,
            "subscription_transition_active": self._subscription_transition_active,
            "subscription_update_count": self._subscription_update_count,
            "websocket_reconnect_count": self._subscription_reconnect_count,
            "missing_active_tokens": sorted(runtime_tokens - active_tokens),
            "extra_active_tokens": sorted(active_tokens - runtime_tokens),
            "subscription_out_of_sync": out_of_sync,
        }

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
                    self._active_websocket = websocket
                    if reconnect_attempt > 0:
                        self._orderbooks.record_reconnect()
                        self._subscription_reconnect_count += 1
                    await websocket.send(self.subscription_message())
                    self._active_subscription_token_ids = tuple(self.token_ids)
                    self._active_subscription_version = self._subscription_version
                    self._subscription_transition_active = False
                    self._logger.info(
                        "polymarket_ws_connected",
                        url=self.url,
                        token_count=len(self.token_ids),
                        subscription_version=self._active_subscription_version,
                    )
                    reconnect_attempt = 0

                    while True:
                        if self._active_subscription_version != self._subscription_version:
                            self._subscription_transition_active = True
                            self._logger.info(
                                "polymarket_ws_subscription_reconnect",
                                active_version=self._active_subscription_version,
                                target_version=self._subscription_version,
                                active_token_count=len(self._active_subscription_token_ids),
                                target_token_count=len(self.token_ids),
                            )
                            await websocket.close()
                            break
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
                    continue

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._active_websocket = None
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
            finally:
                self._active_websocket = None

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
            event_ts = _timestamp_to_ns(payload.get("timestamp"))
            if event_type == "book":
                events.append(
                    self._orderbooks.apply_book(
                        payload,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                        event_ts=event_ts,
                        sequence=payload.get("hash"),
                    )
                )
            elif event_type == "price_change":
                events.extend(
                    self._quotes_from_price_change(
                        payload,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                        event_ts=event_ts,
                    )
                )
            elif event_type == "best_bid_ask":
                events.append(
                    self._orderbooks.apply_best_bid_ask(
                        payload,
                        received_ts=local_received_ts,
                        parse_done_ts=parse_done_ts,
                        recv_monotonic_ns=recv_mono,
                        parse_done_monotonic_ns=parse_done_mono,
                        event_ts=event_ts,
                        sequence=payload.get("hash"),
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
                        event_ts=event_ts,
                    )
                )
            elif event_type in {
                "tick_size_change",
                "new_market",
                "market_resolved",
                "closed",
                "expired",
            }:
                lifecycle = _lifecycle_event(
                    payload,
                    received_ts=local_received_ts,
                    parse_done_ts=parse_done_ts,
                    recv_monotonic_ns=recv_mono,
                    parse_done_monotonic_ns=parse_done_mono,
                    event_ts=event_ts,
                )
                if lifecycle.lifecycle_type == "tick_size_change" and lifecycle.new_tick_size:
                    self._orderbooks.update_tick_size(
                        lifecycle.market_id,
                        token_id=lifecycle.token_id,
                        tick_size=lifecycle.new_tick_size,
                    )
                elif lifecycle.lifecycle_type in {"market_resolved", "closed", "expired"}:
                    self._orderbooks.mark_market_invalid(lifecycle.market_id)
                events.append(lifecycle)
            else:
                raise PolymarketMessageError(f"unsupported Polymarket message: {payload!r}")

        return tuple(events)

    def _quotes_from_price_change(
        self,
        payload: dict[str, Any],
        *,
        received_ts: int,
        parse_done_ts: int,
        recv_monotonic_ns: int,
        parse_done_monotonic_ns: int,
        event_ts: int | None,
    ) -> tuple[PolymarketQuote, ...]:
        changes = payload.get("price_changes")
        rows = (
            [row for row in changes if isinstance(row, dict)]
            if isinstance(changes, list)
            else [payload]
        )
        quotes: list[PolymarketQuote] = []
        for row in rows:
            quotes.append(
                self._orderbooks.apply_price_change(
                    row,
                    parent_payload=payload,
                    received_ts=received_ts,
                    parse_done_ts=parse_done_ts,
                    recv_monotonic_ns=recv_monotonic_ns,
                    parse_done_monotonic_ns=parse_done_monotonic_ns,
                    event_ts=event_ts,
                    sequence=row.get("hash") or payload.get("hash"),
                )
            )
        return tuple(quotes)

    def _backoff_delay(self, attempt: int) -> float:
        return min(self.max_backoff_sec, self.initial_backoff_sec * (2**attempt))

    def book_readiness_snapshot(self, *, now_ts: int | None = None) -> dict[str, Any]:
        return self._orderbooks.market_readiness_snapshot(
            self.market_metadata,
            now_ts=now_ts or utc_now_ns(),
        )


def _build_token_metadata(
    markets: Iterable[PolymarketMarketMetadata],
    token_ids: Iterable[str],
    overrides: Mapping[str, PolymarketSideLabel] | None,
) -> dict[str, TokenBookMetadata]:
    metadata: dict[str, TokenBookMetadata] = {}
    for market in markets:
        for token_id, outcome in market.token_outcomes.items():
            metadata[token_id] = TokenBookMetadata(
                condition_id=market.condition_id,
                market_id=market.market_id,
                side_label=_side_label_for_outcome(outcome),
                market_slug=market.market_slug,
                base_asset=market.base_asset,
                duration_minutes=market.duration_minutes,
                token_outcome=outcome,
                tick_size=market.tick_size,
            )

    if overrides:
        for token_id, side_label in overrides.items():
            previous = metadata.get(token_id)
            metadata[token_id] = TokenBookMetadata(
                condition_id=None if previous is None else previous.condition_id,
                market_id="" if previous is None else previous.market_id,
                side_label=side_label,
                market_slug=None if previous is None else previous.market_slug,
                base_asset=None if previous is None else previous.base_asset,
                duration_minutes=None if previous is None else previous.duration_minutes,
                token_outcome=None if previous is None else previous.token_outcome,
                tick_size=0.01 if previous is None else previous.tick_size,
            )

    for token_id in token_ids:
        metadata.setdefault(
            token_id,
            TokenBookMetadata(condition_id=None, market_id="", side_label="UNKNOWN"),
        )
    return metadata


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


def _tick_from_trade(
    payload: dict[str, Any],
    *,
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
    event_ts: int | None,
) -> MarketTick:
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
        latency_ms=(parse_done_monotonic_ns - recv_monotonic_ns) / 1_000_000.0,
    )


def _lifecycle_event(
    payload: dict[str, Any],
    *,
    received_ts: int,
    parse_done_ts: int,
    recv_monotonic_ns: int,
    parse_done_monotonic_ns: int,
    event_ts: int | None,
) -> MarketLifecycleEvent:
    lifecycle_type = str(payload["event_type"])
    if lifecycle_type not in {
        "tick_size_change",
        "market_resolved",
        "new_market",
        "closed",
        "expired",
    }:
        raise PolymarketMessageError(f"unsupported lifecycle type: {lifecycle_type}")
    return MarketLifecycleEvent(
        market_id=str(payload.get("market") or payload.get("market_id") or ""),
        token_id=(
            None
            if payload.get("asset_id") is None and payload.get("token_id") is None
            else str(payload.get("asset_id") or payload.get("token_id"))
        ),
        lifecycle_type=cast(MarketLifecycleType, lifecycle_type),
        old_tick_size=_optional_float(payload.get("old_tick_size")),
        new_tick_size=_optional_float(payload.get("new_tick_size") or payload.get("tick_size")),
        raw_metadata=payload,
        event_ts=event_ts,
        received_ts=received_ts,
        exchange_event_ts=event_ts,
        exchange_ts_ns=event_ts,
        local_received_ts=received_ts,
        parse_done_ts=parse_done_ts,
        recv_monotonic_ns=recv_monotonic_ns,
        parse_done_monotonic_ns=parse_done_monotonic_ns,
        latency_ms=(parse_done_monotonic_ns - recv_monotonic_ns) / 1_000_000.0,
    )


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


def _side_label_for_outcome(outcome: str) -> PolymarketSideLabel:
    normalized = outcome.strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in {"YES", "NO", "UP", "DOWN", "ABOVE", "BELOW", "HIGHER", "LOWER"}:
        return cast(PolymarketSideLabel, normalized)
    return "UNKNOWN"
