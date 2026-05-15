from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.events import PolymarketQuote, PolymarketSideLabel
from app.marketdata.polymarket_discovery import (
    PolymarketMarketMetadata,
    classify_market_window,
    is_runtime_tradable_market,
)


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


@dataclass(slots=True)
class _TokenBookReadiness:
    token_id: str
    market_id: str
    first_ws_message_ts_ns: int | None = None
    first_book_snapshot_ts_ns: int | None = None
    first_complete_quote_ts_ns: int | None = None
    book_complete_true_count: int = 0
    book_complete_false_count: int = 0
    validation_error_count_by_reason: dict[str, int] = field(default_factory=dict)
    price_change_before_snapshot_count: int = 0
    best_bid_ask_before_snapshot_count: int = 0
    snapshot_count: int = 0
    delta_count: int = 0
    reconnect_count: int = 0
    last_validation_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "market_id": self.market_id,
            "first_ws_message_ts_ns": self.first_ws_message_ts_ns,
            "first_book_snapshot_ts_ns": self.first_book_snapshot_ts_ns,
            "first_complete_quote_ts_ns": self.first_complete_quote_ts_ns,
            "book_complete_true_count": self.book_complete_true_count,
            "book_complete_false_count": self.book_complete_false_count,
            "validation_error_count_by_reason": dict(self.validation_error_count_by_reason),
            "price_change_before_snapshot_count": self.price_change_before_snapshot_count,
            "best_bid_ask_before_snapshot_count": self.best_bid_ask_before_snapshot_count,
            "snapshot_count": self.snapshot_count,
            "delta_count": self.delta_count,
            "reconnect_count": self.reconnect_count,
            "last_validation_error": self.last_validation_error,
        }


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
        self._readiness: dict[str, _TokenBookReadiness] = {
            token_id: _TokenBookReadiness(
                token_id=token_id,
                market_id=metadata.market_id,
            )
            for token_id, metadata in token_metadata.items()
        }
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
        quote = self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
            update_type="book",
        )
        self._record_readiness(book, quote, received_ts=received_ts, update_type="book")
        return quote

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

        before_snapshot = not book.has_snapshot
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
        quote = self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
            update_type="price_change",
        )
        self._record_readiness(
            book,
            quote,
            received_ts=received_ts,
            update_type="price_change",
            before_snapshot=before_snapshot,
        )
        return quote

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
        before_snapshot = not book.has_snapshot
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
        quote = self._quote(
            book,
            received_ts=received_ts,
            parse_done_ts=parse_done_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            parse_done_monotonic_ns=parse_done_monotonic_ns,
            event_ts=event_ts,
            update_type="best_bid_ask",
        )
        self._record_readiness(
            book,
            quote,
            received_ts=received_ts,
            update_type="best_bid_ask",
            before_snapshot=before_snapshot,
        )
        return quote

    def mark_market_invalid(self, market_id: str) -> None:
        self._market_invalid.add(market_id)
        for book in self._books.values():
            if book.metadata.market_id == market_id:
                book.invalid = True
                book.incomplete = True
                book.validation_error = "market_invalidated"

    def record_reconnect(self) -> None:
        for readiness in self._readiness.values():
            readiness.reconnect_count += 1

    def token_readiness_snapshot(self) -> dict[str, dict[str, Any]]:
        for token_id, book in self._books.items():
            self._readiness_for(token_id, book.metadata.market_id)
        return {
            token_id: readiness.snapshot()
            for token_id, readiness in sorted(self._readiness.items())
        }

    def market_readiness_snapshot(
        self,
        markets: tuple[PolymarketMarketMetadata, ...],
        *,
        now_ts: int,
    ) -> dict[str, Any]:
        token_stats = self.token_readiness_snapshot()
        market_rows: list[dict[str, Any]] = []
        validation_errors: dict[str, int] = {}
        for readiness in token_stats.values():
            for reason, count in readiness["validation_error_count_by_reason"].items():
                validation_errors[reason] = validation_errors.get(reason, 0) + int(count)

        for market in markets:
            up_stats = token_stats.get(market.up_token_id or "", {})
            down_stats = token_stats.get(market.down_token_id or "", {})
            up_complete = bool(up_stats.get("first_book_snapshot_ts_ns"))
            down_complete = bool(down_stats.get("first_book_snapshot_ts_ns"))
            both_complete = up_complete and down_complete
            first_ws = _min_optional(
                _int_or_none(up_stats.get("first_ws_message_ts_ns")),
                _int_or_none(down_stats.get("first_ws_message_ts_ns")),
            )
            first_complete = (
                _max_optional(
                    _int_or_none(up_stats.get("first_complete_quote_ts_ns")),
                    _int_or_none(down_stats.get("first_complete_quote_ts_ns")),
                )
                if both_complete
                else None
            )
            warmup_ms = _duration_ms(first_ws, now_ts)
            time_to_complete_ms = _duration_ms(first_ws, first_complete)
            classification = classify_market_window(
                market,
                now_ts=now_ts // 1_000_000_000,
            )
            signal_enabled = (
                is_runtime_tradable_market(market, now_ts=now_ts // 1_000_000_000)
                and classification == "current"
            )
            market_rows.append(
                {
                    "market_id": market.market_id,
                    "market_slug": market.market_slug,
                    "classification_at_now": classification,
                    "signal_enabled_at_now": signal_enabled,
                    "up_token_id": market.up_token_id,
                    "down_token_id": market.down_token_id,
                    "up_token_book_complete": up_complete,
                    "down_token_book_complete": down_complete,
                    "both_tokens_complete": both_complete,
                    "warmup_duration_ms": warmup_ms,
                    "time_to_first_complete_quote_ms": time_to_complete_ms,
                    "last_validation_error": _last_validation_error(up_stats, down_stats),
                }
            )

        completed_times = [
            row["time_to_first_complete_quote_ms"]
            for row in market_rows
            if row["time_to_first_complete_quote_ms"] is not None
        ]
        return {
            "generated_ts_ns": now_ts,
            "tokens": token_stats,
            "markets": market_rows,
            "summary": {
                "selected_runtime_markets": sum(
                    1 for market in markets if market.selected_for_runtime
                ),
                "signal_enabled_markets": sum(
                    1 for row in market_rows if row["signal_enabled_at_now"]
                ),
                "warmup_only_markets": sum(
                    1
                    for market, row in zip(markets, market_rows, strict=True)
                    if market.selected_for_runtime and not row["signal_enabled_at_now"]
                ),
                "complete_markets": sum(1 for row in market_rows if row["both_tokens_complete"]),
                "incomplete_markets": sum(
                    1 for row in market_rows if not row["both_tokens_complete"]
                ),
                "average_time_to_first_complete_quote_ms": (
                    sum(completed_times) / len(completed_times)
                    if completed_times
                    else None
                ),
                "top_validation_errors": dict(
                    sorted(validation_errors.items(), key=lambda item: (-item[1], item[0]))[:5]
                ),
            },
        }

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
        update_type: str,
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
            book_update_type=update_type,  # type: ignore[arg-type]
            book_has_snapshot=book.has_snapshot,
        )

    def _record_readiness(
        self,
        book: _TokenBook,
        quote: PolymarketQuote,
        *,
        received_ts: int,
        update_type: str,
        before_snapshot: bool = False,
    ) -> None:
        readiness = self._readiness_for(book.token_id, book.metadata.market_id)
        if readiness.first_ws_message_ts_ns is None:
            readiness.first_ws_message_ts_ns = received_ts
        if update_type == "book":
            readiness.snapshot_count += 1
            if readiness.first_book_snapshot_ts_ns is None:
                readiness.first_book_snapshot_ts_ns = received_ts
        elif update_type == "price_change":
            readiness.delta_count += 1
            if before_snapshot:
                readiness.price_change_before_snapshot_count += 1
        elif update_type == "best_bid_ask" and before_snapshot:
            readiness.best_bid_ask_before_snapshot_count += 1

        if quote.book_complete:
            readiness.book_complete_true_count += 1
            if readiness.first_complete_quote_ts_ns is None:
                readiness.first_complete_quote_ts_ns = received_ts
        else:
            readiness.book_complete_false_count += 1

        if quote.validation_error is not None:
            readiness.last_validation_error = quote.validation_error
            readiness.validation_error_count_by_reason[quote.validation_error] = (
                readiness.validation_error_count_by_reason.get(quote.validation_error, 0) + 1
            )

    def _readiness_for(self, token_id: str, market_id: str) -> _TokenBookReadiness:
        if token_id not in self._readiness:
            self._readiness[token_id] = _TokenBookReadiness(
                token_id=token_id,
                market_id=market_id,
            )
        return self._readiness[token_id]

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


def _int_or_none(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _min_optional(left: int | None, right: int | None) -> int | None:
    values = [value for value in (left, right) if value is not None]
    return min(values) if values else None


def _max_optional(left: int | None, right: int | None) -> int | None:
    values = [value for value in (left, right) if value is not None]
    return max(values) if values else None


def _duration_ms(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    return max(0.0, (end_ns - start_ns) / 1_000_000.0)


def _last_validation_error(
    up_stats: dict[str, Any],
    down_stats: dict[str, Any],
) -> str | None:
    down_error = down_stats.get("last_validation_error")
    up_error = up_stats.get("last_validation_error")
    if isinstance(down_error, str):
        return down_error
    return up_error if isinstance(up_error, str) else None
