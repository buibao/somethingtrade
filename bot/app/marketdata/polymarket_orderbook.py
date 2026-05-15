from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import orjson

from app.core.events import PolymarketQuote, PolymarketSideLabel
from app.marketdata.polymarket_discovery import (
    PolymarketMarketMetadata,
    classify_market_window,
    is_runtime_tradable_market,
)

BestValidationMode = Literal["strict", "tolerant", "diagnostic"]
SAMPLE_VALIDATION_ERRORS = {
    "reported_best_bid_mismatch",
    "reported_best_ask_mismatch",
    "best_bid_size_unknown",
    "best_ask_size_unknown",
    "missing_snapshot",
}


@dataclass(frozen=True, slots=True)
class TokenBookMetadata:
    condition_id: str | None
    market_id: str
    side_label: PolymarketSideLabel
    market_slug: str | None = None
    base_asset: str | None = None
    duration_minutes: int | None = None
    token_outcome: str | None = None
    tick_size: float = 0.01


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
    reported_best_validation_ok: bool = True
    best_bid_override: float | None = None
    best_ask_override: float | None = None
    last_snapshot_hash: str | None = None
    last_book_hash: str | None = None


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
    quote_stale_count: int = 0
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
            "quote_stale_count": self.quote_stale_count,
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
        best_validation_mode: BestValidationMode = "tolerant",
        best_validation_tolerance_ticks: int = 1,
        mismatch_sample_path: Path | str | None = "data/debug/polymarket_orderbook_mismatch_samples.jsonl",
        mismatch_sample_per_token_per_min: int = 20,
    ) -> None:
        self.token_metadata = token_metadata
        self.stale_after_ms = stale_after_ms
        self.best_validation_mode = best_validation_mode
        self.best_validation_tolerance_ticks = best_validation_tolerance_ticks
        self.mismatch_sample_path = None if mismatch_sample_path is None else Path(mismatch_sample_path)
        self.mismatch_sample_per_token_per_min = mismatch_sample_per_token_per_min
        self._sample_counts: dict[tuple[str, int], int] = {}
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
        book.reported_best_validation_ok = True
        book.last_snapshot_hash = None if sequence is None else str(sequence)
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

        book.reported_best_validation_ok = True
        if not book.has_snapshot:
            book.incomplete = True
            if book.validation_error is None:
                book.validation_error = "missing_snapshot"
        book.invalid = book.metadata.market_id in self._market_invalid
        self._clear_resolved_overrides(book)
        self._validate_reported_best(
            book,
            row,
            received_ts=received_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            update_type="price_change",
        )
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
        token_id = str(payload.get("asset_id") or payload.get("token_id") or "")
        book = self._book(token_id, payload)
        reported_bid = _optional_float(payload.get("best_bid"))
        reported_ask = _optional_float(payload.get("best_ask"))
        before_snapshot = not book.has_snapshot
        book.incomplete = not book.has_snapshot
        book.validation_error = None
        book.reported_best_validation_ok = True

        if not book.has_snapshot:
            book.validation_error = "missing_snapshot"

        self._apply_reported_best(
            book,
            reported_bid=reported_bid,
            reported_ask=reported_ask,
            payload=payload,
            received_ts=received_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            update_type="best_bid_ask",
        )
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

    def update_tick_size(
        self,
        market_id: str,
        *,
        token_id: str | None,
        tick_size: float,
    ) -> None:
        if tick_size <= 0.0:
            return
        token_ids = (
            (token_id,)
            if token_id is not None
            else tuple(
                token
                for token, metadata in self.token_metadata.items()
                if metadata.market_id == market_id
            )
        )
        for token in token_ids:
            metadata = self.token_metadata.get(token)
            if metadata is None:
                continue
            updated = replace(metadata, tick_size=tick_size)
            self.token_metadata[token] = updated
            book = self._books.get(token)
            if book is not None:
                book.metadata = updated

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
        validation_error_count_by_token: dict[str, dict[str, int]] = {}
        quote_stale_count_by_token: dict[str, int] = {}
        mismatch_rate_by_token: dict[str, float] = {}
        quote_complete_rate_by_token: dict[str, float] = {}
        for readiness in token_stats.values():
            token_id = str(readiness["token_id"])
            reason_counts = {
                str(reason): int(count)
                for reason, count in readiness["validation_error_count_by_reason"].items()
            }
            validation_error_count_by_token[token_id] = reason_counts
            quote_stale_count_by_token[token_id] = int(readiness["quote_stale_count"])
            total_quotes = int(readiness["book_complete_true_count"]) + int(
                readiness["book_complete_false_count"]
            )
            mismatch_count = sum(
                count
                for reason, count in reason_counts.items()
                if reason.startswith("reported_best_")
            )
            mismatch_rate_by_token[token_id] = mismatch_count / total_quotes if total_quotes else 0.0
            quote_complete_rate_by_token[token_id] = (
                int(readiness["book_complete_true_count"]) / total_quotes
                if total_quotes
                else 0.0
            )
            for reason, count in reason_counts.items():
                validation_errors[reason] = validation_errors.get(reason, 0) + int(count)

        validation_error_count_by_market: dict[str, dict[str, int]] = {}
        quote_stale_count_by_market: dict[str, int] = {}
        mismatch_rate_by_market: dict[str, float] = {}
        quote_complete_rate_by_market: dict[str, float] = {}
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
            market_token_stats = [
                stats
                for stats in (
                    token_stats.get(market.up_token_id or ""),
                    token_stats.get(market.down_token_id or ""),
                )
                if isinstance(stats, dict)
            ]
            validation_error_count_by_market[market.market_id] = _merge_reason_counts(
                stats.get("validation_error_count_by_reason", {}) for stats in market_token_stats
            )
            quote_stale_count_by_market[market.market_id] = sum(
                int(stats.get("quote_stale_count", 0)) for stats in market_token_stats
            )
            total_market_quotes = sum(
                int(stats.get("book_complete_true_count", 0))
                + int(stats.get("book_complete_false_count", 0))
                for stats in market_token_stats
            )
            market_mismatches = sum(
                count
                for reason, count in validation_error_count_by_market[market.market_id].items()
                if reason.startswith("reported_best_")
            )
            market_complete = sum(
                int(stats.get("book_complete_true_count", 0)) for stats in market_token_stats
            )
            mismatch_rate_by_market[market.market_id] = (
                market_mismatches / total_market_quotes if total_market_quotes else 0.0
            )
            quote_complete_rate_by_market[market.market_id] = (
                market_complete / total_market_quotes if total_market_quotes else 0.0
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
                "validation_error_count_by_market": validation_error_count_by_market,
                "validation_error_count_by_token": validation_error_count_by_token,
                "mismatch_rate_by_market": mismatch_rate_by_market,
                "mismatch_rate_by_token": mismatch_rate_by_token,
                "quote_complete_rate_by_market": quote_complete_rate_by_market,
                "quote_complete_rate_by_token": quote_complete_rate_by_token,
                "quote_stale_count_by_market": quote_stale_count_by_market,
                "quote_stale_count_by_token": quote_stale_count_by_token,
                "sampled_mismatch_file_path": (
                    None if self.mismatch_sample_path is None else str(self.mismatch_sample_path)
                ),
                "validation_mode": self.best_validation_mode,
                "validation_tolerance_ticks": self.best_validation_tolerance_ticks,
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
        book.last_book_hash = book.last_hash
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
        token_mismatch_rate, token_complete_rate = self._quality_rates_for_token(book.token_id)
        market_mismatch_rate, market_complete_rate = self._quality_rates_for_market(
            book.metadata.market_id
        )
        structurally_complete = (
            book.has_snapshot
            and not book.invalid
            and local_bid is not None
            and local_ask is not None
            and local_bid_size is not None
            and local_ask_size is not None
        )
        incomplete = book.incomplete or book.invalid or not structurally_complete

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
            book_structurally_complete=structurally_complete,
            reported_best_validation_ok=book.reported_best_validation_ok,
            validation_mode=self.best_validation_mode,
            validation_tolerance_ticks=self.best_validation_tolerance_ticks,
            market_mismatch_rate=market_mismatch_rate,
            token_mismatch_rate=token_mismatch_rate,
            market_quote_complete_rate=market_complete_rate,
            token_quote_complete_rate=token_complete_rate,
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
        if quote.book_stale:
            readiness.quote_stale_count += 1

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

    def _validate_reported_best(
        self,
        book: _TokenBook,
        row: dict[str, Any],
        *,
        received_ts: int,
        recv_monotonic_ns: int,
        update_type: str,
    ) -> None:
        reported_bid = _optional_float(row.get("best_bid"))
        reported_ask = _optional_float(row.get("best_ask"))
        self._apply_reported_best(
            book,
            reported_bid=reported_bid,
            reported_ask=reported_ask,
            payload=row,
            received_ts=received_ts,
            recv_monotonic_ns=recv_monotonic_ns,
            update_type=update_type,
        )

    def _apply_reported_best(
        self,
        book: _TokenBook,
        *,
        reported_bid: float | None,
        reported_ask: float | None,
        payload: dict[str, Any],
        received_ts: int,
        recv_monotonic_ns: int,
        update_type: str,
    ) -> None:
        if not book.has_snapshot:
            book.validation_error = "missing_snapshot"
            book.reported_best_validation_ok = False
            book.incomplete = True
            if update_type == "best_bid_ask":
                book.best_bid_override = reported_bid
                book.best_ask_override = reported_ask
            self._sample_validation_error(
                book,
                payload=payload,
                received_ts=received_ts,
                recv_monotonic_ns=recv_monotonic_ns,
                update_type=update_type,
            )
            return
        if reported_bid is not None:
            self._apply_one_reported_best(
                book,
                side="bid",
                reported=reported_bid,
                payload=payload,
                received_ts=received_ts,
                recv_monotonic_ns=recv_monotonic_ns,
                update_type=update_type,
            )
        if reported_ask is not None:
            self._apply_one_reported_best(
                book,
                side="ask",
                reported=reported_ask,
                payload=payload,
                received_ts=received_ts,
                recv_monotonic_ns=recv_monotonic_ns,
                update_type=update_type,
            )
        if book.validation_error in SAMPLE_VALIDATION_ERRORS:
            self._sample_validation_error(
                book,
                payload=payload,
                received_ts=received_ts,
                recv_monotonic_ns=recv_monotonic_ns,
                update_type=update_type,
            )

    def _apply_one_reported_best(
        self,
        book: _TokenBook,
        *,
        side: Literal["bid", "ask"],
        reported: float,
        payload: dict[str, Any],
        received_ts: int,
        recv_monotonic_ns: int,
        update_type: str,
    ) -> None:
        local, _ = _best_bid(book.bids) if side == "bid" else _best_ask(book.asks)
        if local is None:
            reason = "missing_snapshot" if not book.has_snapshot else f"best_{side}_size_unknown"
            book.validation_error = reason
            book.reported_best_validation_ok = False
            book.incomplete = True
            if side == "bid":
                book.best_bid_override = reported
            else:
                book.best_ask_override = reported
            return

        if _prices_equal(local, reported):
            if side == "bid":
                book.best_bid_override = None
            else:
                book.best_ask_override = None
            return

        reason = f"reported_best_{side}_mismatch"
        book.validation_error = reason
        within_tolerance = self._within_best_tolerance(local, reported, book.metadata.tick_size)
        if self.best_validation_mode == "diagnostic":
            book.reported_best_validation_ok = False
            return
        if self.best_validation_mode == "tolerant" and within_tolerance:
            book.reported_best_validation_ok = True
            return

        book.reported_best_validation_ok = False
        book.incomplete = True
        if side == "bid":
            book.best_bid_override = reported
        else:
            book.best_ask_override = reported

    def _within_best_tolerance(
        self,
        local: float,
        reported: float,
        tick_size: float,
    ) -> bool:
        tolerance = max(0.0, self.best_validation_tolerance_ticks * tick_size)
        return abs(local - reported) <= tolerance + 1e-12

    def _sample_validation_error(
        self,
        book: _TokenBook,
        *,
        payload: dict[str, Any],
        received_ts: int,
        recv_monotonic_ns: int,
        update_type: str,
    ) -> None:
        if self.mismatch_sample_path is None or self.mismatch_sample_per_token_per_min <= 0:
            return
        bucket = received_ts // 60_000_000_000
        key = (book.token_id, bucket)
        count = self._sample_counts.get(key, 0)
        if count >= self.mismatch_sample_per_token_per_min:
            return
        self._sample_counts[key] = count + 1

        local_bid, local_bid_size = _best_bid(book.bids)
        local_ask, local_ask_size = _best_ask(book.asks)
        sample = {
            "market_id": book.metadata.market_id,
            "market_slug": book.metadata.market_slug,
            "base_asset": book.metadata.base_asset,
            "duration_minutes": book.metadata.duration_minutes,
            "token_id": book.token_id,
            "token_outcome": book.metadata.token_outcome,
            "update_type": update_type,
            "local_best_bid": local_bid,
            "local_best_ask": local_ask,
            "local_best_bid_size": local_bid_size,
            "local_best_ask_size": local_ask_size,
            "reported_best_bid": _optional_float(payload.get("best_bid")),
            "reported_best_ask": _optional_float(payload.get("best_ask")),
            "reported_best_bid_size": _optional_float(payload.get("best_bid_size")),
            "reported_best_ask_size": _optional_float(payload.get("best_ask_size")),
            "payload_hash": payload.get("hash"),
            "last_snapshot_hash": book.last_snapshot_hash,
            "last_book_hash": book.last_book_hash,
            "local_sequence": book.update_count + 1,
            "recv_monotonic_ns": recv_monotonic_ns,
            "validation_error": book.validation_error,
            "raw_payload_compact": _compact_payload(payload),
        }
        self.mismatch_sample_path.parent.mkdir(parents=True, exist_ok=True)
        with self.mismatch_sample_path.open("ab") as handle:
            handle.write(orjson.dumps(sample, option=orjson.OPT_APPEND_NEWLINE))

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

    def _quality_rates_for_token(self, token_id: str) -> tuple[float | None, float | None]:
        readiness = self._readiness.get(token_id)
        if readiness is None:
            return None, None
        return _quality_rates(readiness)

    def _quality_rates_for_market(self, market_id: str) -> tuple[float | None, float | None]:
        stats = [
            readiness
            for readiness in self._readiness.values()
            if readiness.market_id == market_id
        ]
        if not stats:
            return None, None
        true_count = sum(item.book_complete_true_count for item in stats)
        false_count = sum(item.book_complete_false_count for item in stats)
        total = true_count + false_count
        if total == 0:
            return None, None
        mismatch_count = sum(
            count
            for item in stats
            for reason, count in item.validation_error_count_by_reason.items()
            if reason.startswith("reported_best_")
        )
        return mismatch_count / total, true_count / total


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


def _prices_equal(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-12


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "event_type",
        "asset_id",
        "token_id",
        "market",
        "side",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "best_bid_size",
        "best_ask_size",
        "timestamp",
        "hash",
    )
    compact: dict[str, Any] = {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str | int | float | bool) or value is None:
            compact[key] = value
    return compact


def _merge_reason_counts(reason_counts: Iterable[object]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for counts in reason_counts:
        if not isinstance(counts, dict):
            continue
        for reason, count in counts.items():
            if isinstance(reason, str) and isinstance(count, int):
                merged[reason] = merged.get(reason, 0) + count
    return merged


def _quality_rates(readiness: _TokenBookReadiness) -> tuple[float | None, float | None]:
    total = readiness.book_complete_true_count + readiness.book_complete_false_count
    if total == 0:
        return None, None
    mismatch_count = sum(
        count
        for reason, count in readiness.validation_error_count_by_reason.items()
        if reason.startswith("reported_best_")
    )
    return mismatch_count / total, readiness.book_complete_true_count / total


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
