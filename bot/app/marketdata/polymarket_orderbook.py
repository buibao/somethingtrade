from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.events import PolymarketQuote, PolymarketSideLabel


@dataclass(frozen=True, slots=True)
class TokenBookMetadata:
    condition_id: str | None
    market_id: str
    side_label: PolymarketSideLabel


@dataclass(slots=True)
class _TokenBook:
    token_id: str
    metadata: TokenBookMetadata
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_hash: str | None = None
    last_event_ts: int | None = None
    last_received_ts: int | None = None
    update_count: int = 0
    has_snapshot: bool = False
    incomplete: bool = True
    invalid: bool = False
    validation_error: str | None = None
    best_bid_override: float | None = None
    best_ask_override: float | None = None


class PolymarketLocalOrderBook:
    """Local top-of-book source of truth for Polymarket CLOB tokens."""

    def __init__(
        self,
        *,
        token_metadata: dict[str, TokenBookMetadata],
        stale_after_ms: float = 1_000.0,
    ) -> None:
        self.token_metadata = token_metadata
        self.stale_after_ms = stale_after_ms
        self._books: dict[str, _TokenBook] = {}
        self._market_invalid: set[str] = set()

    def apply_book(
        self,
        payload: dict[str, Any],
        *,
        received_ts: int,
        parse_done_ts: int,
        recv_monotonic_ns: int,
        parse_done_monotonic_ns: int,
        event_ts: int | None,
        sequence: object,
    ) -> PolymarketQuote:
        token_id = str(payload["asset_id"])
        book = self._book(token_id, payload)
        book.bids = _levels_to_dict(payload.get("bids", []))
        book.asks = _levels_to_dict(payload.get("asks", []))
        book.has_snapshot = True
        book.incomplete = False
        book.invalid = book.metadata.market_id in self._market_invalid
        book.best_bid_override = None
        book.best_ask_override = None
        book.validation_error = None
        self._touch(book, event_ts=event_ts, received_ts=received_ts, sequence=sequence)
        return self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
        )

    def apply_price_change(
        self,
        row: dict[str, Any],
        *,
        parent_payload: dict[str, Any],
        received_ts: int,
        parse_done_ts: int,
        recv_monotonic_ns: int,
        parse_done_monotonic_ns: int,
        event_ts: int | None,
        sequence: object,
    ) -> PolymarketQuote:
        token_id = str(row.get("asset_id") or parent_payload.get("asset_id"))
        book = self._book(token_id, parent_payload)
        side = str(row.get("side", "")).upper()
        price = _float_from(row["price"])
        size = _float_from(row.get("size", 0.0))

        if side == "BUY":
            _set_level(book.bids, price, size)
        elif side == "SELL":
            _set_level(book.asks, price, size)
        else:
            book.validation_error = f"unsupported_price_change_side:{side}"

        if not book.has_snapshot:
            book.incomplete = True
        book.invalid = book.metadata.market_id in self._market_invalid
        self._clear_resolved_overrides(book)
        self._validate_reported_best(book, row)
        self._touch(book, event_ts=event_ts, received_ts=received_ts, sequence=sequence)
        return self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
        )

    def apply_best_bid_ask(
        self,
        payload: dict[str, Any],
        *,
        received_ts: int,
        parse_done_ts: int,
        recv_monotonic_ns: int,
        parse_done_monotonic_ns: int,
        event_ts: int | None,
        sequence: object,
    ) -> PolymarketQuote:
        token_id = str(payload["asset_id"])
        book = self._book(token_id, payload)
        reported_bid = _optional_float(payload.get("best_bid"))
        reported_ask = _optional_float(payload.get("best_ask"))
        book.incomplete = not book.has_snapshot
        book.validation_error = None

        if reported_bid is not None:
            local_bid, _ = _best_bid(book.bids)
            if local_bid is None:
                book.best_bid_override = reported_bid
                book.incomplete = True
                book.validation_error = "best_bid_size_unknown"
            elif local_bid == reported_bid:
                book.best_bid_override = None
            else:
                book.best_bid_override = reported_bid
                book.incomplete = True
                book.validation_error = "reported_best_bid_mismatch"

        if reported_ask is not None:
            local_ask, _ = _best_ask(book.asks)
            if local_ask is None:
                book.best_ask_override = reported_ask
                book.incomplete = True
                book.validation_error = "best_ask_size_unknown"
            elif local_ask == reported_ask:
                book.best_ask_override = None
            else:
                book.best_ask_override = reported_ask
                book.incomplete = True
                book.validation_error = "reported_best_ask_mismatch"

        if not book.has_snapshot:
            book.incomplete = True
        book.invalid = book.metadata.market_id in self._market_invalid
        self._touch(book, event_ts=event_ts, received_ts=received_ts, sequence=sequence)
        return self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
        )

    def mark_market_invalid(self, market_id: str) -> None:
        self._market_invalid.add(market_id)
        for book in self._books.values():
            if book.metadata.market_id == market_id:
                book.invalid = True
                book.incomplete = True
                book.validation_error = "market_invalidated"

    def _book(self, token_id: str, payload: dict[str, Any]) -> _TokenBook:
        if token_id in self._books:
            return self._books[token_id]
        metadata = self.token_metadata.get(
            token_id,
            TokenBookMetadata(
                condition_id=None,
                market_id=str(payload.get("market") or ""),
                side_label="UNKNOWN",
            ),
        )
        book = _TokenBook(token_id=token_id, metadata=metadata)
        self._books[token_id] = book
        return book

    def _touch(
        self,
        book: _TokenBook,
        *,
        event_ts: int | None,
        received_ts: int,
        sequence: object,
    ) -> None:
        book.last_event_ts = event_ts
        book.last_received_ts = received_ts
        book.last_hash = None if sequence is None else str(sequence)
        book.update_count += 1

    def _quote(
        self,
        book: _TokenBook,
        *,
        received_ts: int,
        parse_done_ts: int,
        recv_monotonic_ns: int,
        parse_done_monotonic_ns: int,
        event_ts: int | None,
    ) -> PolymarketQuote:
        local_bid, local_bid_size = _best_bid(book.bids)
        local_ask, local_ask_size = _best_ask(book.asks)

        best_bid = book.best_bid_override if book.best_bid_override is not None else local_bid
        best_ask = book.best_ask_override if book.best_ask_override is not None else local_ask
        best_bid_size = None if book.best_bid_override is not None else local_bid_size
        best_ask_size = None if book.best_ask_override is not None else local_ask_size
        mid = None if best_bid is None or best_ask is None else (best_bid + best_ask) / 2.0
        spread = None if best_bid is None or best_ask is None else max(0.0, best_ask - best_bid)
        stale = self._is_stale(book, now_ts=received_ts)
        incomplete = book.incomplete or book.invalid or best_bid is None or best_ask is None

        return PolymarketQuote(
            market_id=book.metadata.market_id,
            condition_id=book.metadata.condition_id,
            token_id=book.token_id,
            side_label=book.metadata.side_label,
            best_bid=best_bid,
            best_bid_size=best_bid_size,
            best_ask=best_ask,
            best_ask_size=best_ask_size,
            mid_price=mid,
            spread=spread,
            event_ts=event_ts,
            received_ts=received_ts,
            exchange_event_ts=event_ts,
            exchange_ts_ns=event_ts,
            local_received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            latency_ms=(parse_done_monotonic_ns - recv_monotonic_ns) / 1_000_000.0,
            sequence=book.update_count,
            book_complete=not incomplete,
            book_stale=stale,
            book_hash=book.last_hash,
            validation_error=book.validation_error,
        )

    def _validate_reported_best(self, book: _TokenBook, row: dict[str, Any]) -> None:
        reported_bid = _optional_float(row.get("best_bid"))
        reported_ask = _optional_float(row.get("best_ask"))
        local_bid, _ = _best_bid(book.bids)
        local_ask, _ = _best_ask(book.asks)
        if reported_bid is not None and local_bid is not None and reported_bid != local_bid:
            book.validation_error = "reported_best_bid_mismatch"
            book.incomplete = True
        if reported_ask is not None and local_ask is not None and reported_ask != local_ask:
            book.validation_error = "reported_best_ask_mismatch"
            book.incomplete = True

    def _clear_resolved_overrides(self, book: _TokenBook) -> None:
        local_bid, _ = _best_bid(book.bids)
        local_ask, _ = _best_ask(book.asks)
        if book.best_bid_override is not None and book.best_bid_override == local_bid:
            book.best_bid_override = None
        if book.best_ask_override is not None and book.best_ask_override == local_ask:
            book.best_ask_override = None

    def _is_stale(self, book: _TokenBook, *, now_ts: int) -> bool:
        if book.last_received_ts is None:
            return True
        return (now_ts - book.last_received_ts) / 1_000_000.0 > self.stale_after_ms


def _levels_to_dict(rows: object) -> dict[float, float]:
    if not isinstance(rows, list):
        return {}
    levels: dict[float, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        price = _float_from(row["price"])
        size = _float_from(row["size"])
        if size > 0.0:
            levels[price] = size
    return levels


def _set_level(levels: dict[float, float], price: float, size: float) -> None:
    if size <= 0.0:
        levels.pop(price, None)
    else:
        levels[price] = size


def _best_bid(levels: dict[float, float]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    price = max(levels)
    return price, levels[price]


def _best_ask(levels: dict[float, float]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    price = min(levels)
    return price, levels[price]


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _float_from(value)


def _float_from(value: object) -> float:
    if isinstance(value, int | float | str | bytes):
        return float(value)
    raise ValueError(f"expected float-like value, got {value!r}")
