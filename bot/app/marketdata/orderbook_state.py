from __future__ import annotations

import bisect
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, MutableMapping

from app.core.clock import monotonic_now_ns


DECIMAL_ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class ParsedLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class OrderbookSnapshot:
    symbol: str
    snapshot_version: int
    state_version: int
    bids_top_n: tuple[tuple[Decimal, Decimal], ...]
    asks_top_n: tuple[tuple[Decimal, Decimal], ...]
    bid_count: int
    ask_count: int
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    mid: Decimal | None
    last_update_id: int | None
    last_book_update_monotonic_ns: int | None
    local_recv_monotonic_ns: int
    local_recv_wall_ts: str | None = None


@dataclass(frozen=True, slots=True)
class OrderbookApplyResult:
    accepted: bool
    status: str
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    first_update_id: int | None = None
    final_update_id: int | None = None
    previous_last_update_id: int | None = None
    expected_next_update_id: int | None = None
    state_version_before: int = 0
    state_version_after: int = 0
    snapshot_ready_before: bool = False
    snapshot_ready_after: bool = False
    ready_to_emit_after: bool = False
    generation_before: int = 0
    generation_after: int = 0


class OrderbookState:
    """Authoritative Binance orderbook state for one symbol.

    The object mutates only through accepted snapshots and deltas. Snapshots
    copied from it contain only immutable top-N tuples and counts.
    """

    def __init__(
        self,
        symbol: str,
        *,
        ready_false_warning_after_sec: float = 300.0,
    ) -> None:
        self.symbol = symbol.upper()
        self.bids: MutableMapping[Decimal, Decimal] = {}
        self.asks: MutableMapping[Decimal, Decimal] = {}
        self._bid_prices_asc: list[Decimal] = []
        self._ask_prices_asc: list[Decimal] = []
        self.snapshot_ready = False
        self.ready_to_emit = False
        self.last_update_id: int | None = None
        self.snapshot_last_update_id: int | None = None
        self.required_fresh_snapshot_after_update_id: int | None = None
        self.state_version = 0
        self.generation = 0
        self.sequence_continuous = False
        self.awaiting_first_delta_after_snapshot = False
        self.last_not_ready_reason: str | None = "initial_snapshot_missing"
        self.last_book_update_monotonic_ns: int | None = None
        self.last_local_recv_monotonic_ns: int | None = None
        self.ready_false_since_monotonic_ns: int | None = monotonic_now_ns()
        self.ready_false_warning_after_ns = int(ready_false_warning_after_sec * 1_000_000_000)
        self.ready_to_emit_false_duration_ms_max = 0.0
        self.ready_to_emit_false_warning_count = 0

    def apply_snapshot(
        self,
        *,
        bids: Iterable[Any],
        asks: Iterable[Any],
        last_update_id: int,
        local_recv_monotonic_ns: int,
        local_recv_wall_ts: str | None = None,
        generation: int | None = None,
    ) -> OrderbookApplyResult:
        del local_recv_wall_ts
        before = self._result_base()
        if generation is not None and generation != self.generation:
            return self._result(
                before,
                accepted=False,
                status="stale_snapshot_generation",
                errors=("snapshot_inconsistent",),
            )
        if (
            self.required_fresh_snapshot_after_update_id is not None
            and last_update_id <= self.required_fresh_snapshot_after_update_id
        ):
            return self._result(
                before,
                accepted=False,
                status="stale_snapshot_before_gap_boundary",
                errors=("sequence_gap_or_reset",),
            )

        parsed_bids, bid_errors = _parse_levels(bids, allow_zero=False)
        parsed_asks, ask_errors = _parse_levels(asks, allow_zero=False)
        errors = tuple(sorted(set(bid_errors + ask_errors)))
        if errors:
            self.mark_not_ready(
                "snapshot_invalid_levels",
                local_recv_monotonic_ns=local_recv_monotonic_ns,
                advance_generation=False,
            )
            return self._result(
                before,
                accepted=False,
                status="snapshot_invalid_levels",
                errors=errors,
            )

        self.bids = {level.price: level.size for level in parsed_bids}
        self.asks = {level.price: level.size for level in parsed_asks}
        self._rebuild_price_indexes()
        self.snapshot_ready = True
        self.sequence_continuous = False
        self.awaiting_first_delta_after_snapshot = True
        self.ready_to_emit = False
        self.last_not_ready_reason = "waiting_for_first_delta_bridge"
        self.last_update_id = int(last_update_id)
        self.snapshot_last_update_id = int(last_update_id)
        self.required_fresh_snapshot_after_update_id = None
        self.last_book_update_monotonic_ns = local_recv_monotonic_ns
        self.last_local_recv_monotonic_ns = local_recv_monotonic_ns
        self.state_version += 1
        self._record_ready_false_age(local_recv_monotonic_ns)
        return self._result(before, accepted=True, status="snapshot_loaded")

    def apply_delta(
        self,
        *,
        first_update_id: int,
        final_update_id: int,
        bids: Iterable[Any],
        asks: Iterable[Any],
        local_recv_monotonic_ns: int,
        previous_final_update_id: int | None = None,
    ) -> OrderbookApplyResult:
        del previous_final_update_id
        before = self._result_base(
            first_update_id=first_update_id,
            final_update_id=final_update_id,
        )
        if not self.snapshot_ready or self.last_update_id is None:
            self._record_ready_false_age(local_recv_monotonic_ns)
            return self._result(
                before,
                accepted=False,
                status="delta_before_snapshot",
                errors=("sample_before_book_ready",),
            )

        previous_last_update_id = self.last_update_id
        if final_update_id <= previous_last_update_id:
            self._record_ready_false_age(local_recv_monotonic_ns)
            return self._result(
                before,
                accepted=False,
                status="duplicate_update",
                errors=("duplicate_update",),
                expected_next_update_id=previous_last_update_id + 1,
            )

        expected_next_update_id = previous_last_update_id + 1
        if self.awaiting_first_delta_after_snapshot:
            snapshot_last_update_id = self.snapshot_last_update_id
            if snapshot_last_update_id is None or not (
                first_update_id <= snapshot_last_update_id + 1 <= final_update_id
            ):
                self.mark_not_ready(
                    "sequence_bridge_failed",
                    local_recv_monotonic_ns=local_recv_monotonic_ns,
                    gap_boundary_update_id=final_update_id,
                )
                return self._result(
                    before,
                    accepted=False,
                    status="sequence_bridge_failed",
                    errors=("sequence_gap_or_reset",),
                    expected_next_update_id=expected_next_update_id,
                )
        elif first_update_id != expected_next_update_id:
            self.mark_not_ready(
                "sequence_gap_or_reset",
                local_recv_monotonic_ns=local_recv_monotonic_ns,
                gap_boundary_update_id=final_update_id,
            )
            return self._result(
                before,
                accepted=False,
                status="sequence_gap_or_reset",
                errors=("sequence_gap_or_reset",),
                expected_next_update_id=expected_next_update_id,
            )

        parsed_bids, bid_errors = _parse_levels(bids, allow_zero=True)
        parsed_asks, ask_errors = _parse_levels(asks, allow_zero=True)
        errors = tuple(sorted(set(bid_errors + ask_errors)))
        if errors:
            self._record_ready_false_age(local_recv_monotonic_ns)
            return self._result(
                before,
                accepted=False,
                status="invalid_delta_levels",
                errors=errors,
                expected_next_update_id=expected_next_update_id,
            )

        for level in parsed_bids:
            _apply_level(self.bids, self._bid_prices_asc, level)
        for level in parsed_asks:
            _apply_level(self.asks, self._ask_prices_asc, level)

        self.last_update_id = int(final_update_id)
        self.last_book_update_monotonic_ns = local_recv_monotonic_ns
        self.last_local_recv_monotonic_ns = local_recv_monotonic_ns
        self.awaiting_first_delta_after_snapshot = False
        self.sequence_continuous = True
        self.state_version += 1
        self._refresh_ready_to_emit(local_recv_monotonic_ns)
        return self._result(
            before,
            accepted=True,
            status="delta_applied",
            expected_next_update_id=expected_next_update_id,
        )

    def mark_not_ready(
        self,
        reason: str,
        *,
        local_recv_monotonic_ns: int | None = None,
        advance_generation: bool = True,
        gap_boundary_update_id: int | None = None,
    ) -> None:
        now_ns = local_recv_monotonic_ns or monotonic_now_ns()
        if self.ready_false_since_monotonic_ns is None:
            self.ready_false_since_monotonic_ns = now_ns
        self.snapshot_ready = False
        self.ready_to_emit = False
        self.sequence_continuous = False
        self.awaiting_first_delta_after_snapshot = False
        self.last_not_ready_reason = reason
        if advance_generation:
            self.generation += 1
        if gap_boundary_update_id is not None:
            self.required_fresh_snapshot_after_update_id = gap_boundary_update_id
        self._record_ready_false_age(now_ns)

    def mark_ready(self, *, local_recv_monotonic_ns: int | None = None) -> bool:
        now_ns = local_recv_monotonic_ns or monotonic_now_ns()
        self._refresh_ready_to_emit(now_ns)
        return self.ready_to_emit

    def best_bid(self) -> Decimal | None:
        self._ensure_price_indexes()
        return self._bid_prices_asc[-1] if self._bid_prices_asc else None

    def best_ask(self) -> Decimal | None:
        self._ensure_price_indexes()
        return self._ask_prices_asc[0] if self._ask_prices_asc else None

    def top_n(
        self,
        *,
        side: str,
        n: int = 20,
    ) -> tuple[tuple[Decimal, Decimal], ...]:
        book = self.bids if side == "bid" else self.asks
        self._ensure_price_indexes()
        if n <= 0:
            return ()
        prices = (
            list(reversed(self._bid_prices_asc[-n:]))
            if side == "bid"
            else self._ask_prices_asc[:n]
        )
        return tuple(
            (price, book[price])
            for price in prices
            if book[price] != DECIMAL_ZERO
        )

    def copy_snapshot(
        self,
        *,
        top_n: int = 20,
        local_recv_monotonic_ns: int | None = None,
        local_recv_wall_ts: str | None = None,
    ) -> OrderbookSnapshot:
        bids_top_n = self.top_n(side="bid", n=top_n)
        asks_top_n = self.top_n(side="ask", n=top_n)
        bid = bids_top_n[0][0] if bids_top_n else None
        ask = asks_top_n[0][0] if asks_top_n else None
        spread = ask - bid if bid is not None and ask is not None else None
        mid = (bid + ask) / Decimal("2") if bid is not None and ask is not None else None
        version = self.state_version
        return OrderbookSnapshot(
            symbol=self.symbol,
            snapshot_version=version,
            state_version=version,
            bids_top_n=bids_top_n,
            asks_top_n=asks_top_n,
            bid_count=len(self.bids),
            ask_count=len(self.asks),
            best_bid=bid,
            best_ask=ask,
            spread=spread,
            mid=mid,
            last_update_id=self.last_update_id,
            last_book_update_monotonic_ns=self.last_book_update_monotonic_ns,
            local_recv_monotonic_ns=local_recv_monotonic_ns or monotonic_now_ns(),
            local_recv_wall_ts=local_recv_wall_ts,
        )

    def validate(self) -> tuple[str, ...]:
        errors = list(_active_level_errors(self.bids))
        errors.extend(_active_level_errors(self.asks))
        bid = self.best_bid()
        ask = self.best_ask()
        if not self.bids and not self.asks:
            errors.append("book_empty")
        if not self.bids or not self.asks:
            errors.append("one_side_missing")
        if bid is None:
            errors.append("best_bid_missing")
        if ask is None:
            errors.append("best_ask_missing")
        if bid is not None and ask is not None and bid >= ask:
            errors.append("crossed_book")
        return tuple(sorted(set(errors)))

    def cleanup(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self._bid_prices_asc.clear()
        self._ask_prices_asc.clear()
        self.snapshot_ready = False
        self.ready_to_emit = False
        self.sequence_continuous = False
        self.awaiting_first_delta_after_snapshot = False
        self.last_not_ready_reason = "symbol_removed"

    def _refresh_ready_to_emit(self, now_monotonic_ns: int) -> None:
        valid = not self.validate()
        self.ready_to_emit = (
            self.snapshot_ready
            and self.sequence_continuous
            and not self.awaiting_first_delta_after_snapshot
            and valid
        )
        if self.ready_to_emit:
            self.ready_false_since_monotonic_ns = None
            self.last_not_ready_reason = None
        else:
            if self.ready_false_since_monotonic_ns is None:
                self.ready_false_since_monotonic_ns = now_monotonic_ns
            self.last_not_ready_reason = self.last_not_ready_reason or "quality_gate_failed"
            self._record_ready_false_age(now_monotonic_ns)

    def _rebuild_price_indexes(self) -> None:
        self._bid_prices_asc = sorted(self.bids.keys())
        self._ask_prices_asc = sorted(self.asks.keys())

    def _ensure_price_indexes(self) -> None:
        if len(self._bid_prices_asc) != len(self.bids) or len(self._ask_prices_asc) != len(self.asks):
            self._rebuild_price_indexes()

    def _record_ready_false_age(self, now_monotonic_ns: int) -> None:
        if self.ready_to_emit or self.ready_false_since_monotonic_ns is None:
            return
        duration_ms = (
            now_monotonic_ns - self.ready_false_since_monotonic_ns
        ) / 1_000_000.0
        if duration_ms > self.ready_to_emit_false_duration_ms_max:
            self.ready_to_emit_false_duration_ms_max = duration_ms
        duration_ns = now_monotonic_ns - self.ready_false_since_monotonic_ns
        threshold = self.ready_false_warning_after_ns
        if threshold > 0 and duration_ns >= (
            self.ready_to_emit_false_warning_count + 1
        ) * threshold:
            self.ready_to_emit_false_warning_count += 1

    def _result_base(
        self,
        *,
        first_update_id: int | None = None,
        final_update_id: int | None = None,
    ) -> Mapping[str, Any]:
        return {
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "previous_last_update_id": self.last_update_id,
            "state_version_before": self.state_version,
            "snapshot_ready_before": self.snapshot_ready,
            "generation_before": self.generation,
        }

    def _result(
        self,
        before: Mapping[str, Any],
        *,
        accepted: bool,
        status: str,
        errors: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
        expected_next_update_id: int | None = None,
    ) -> OrderbookApplyResult:
        return OrderbookApplyResult(
            accepted=accepted,
            status=status,
            errors=errors,
            warnings=warnings,
            first_update_id=before.get("first_update_id"),
            final_update_id=before.get("final_update_id"),
            previous_last_update_id=before.get("previous_last_update_id"),
            expected_next_update_id=expected_next_update_id,
            state_version_before=int(before["state_version_before"]),
            state_version_after=self.state_version,
            snapshot_ready_before=bool(before["snapshot_ready_before"]),
            snapshot_ready_after=self.snapshot_ready,
            ready_to_emit_after=self.ready_to_emit,
            generation_before=int(before["generation_before"]),
            generation_after=self.generation,
        )


def _parse_levels(
    levels: Iterable[Any],
    *,
    allow_zero: bool,
) -> tuple[list[ParsedLevel], list[str]]:
    parsed: list[ParsedLevel] = []
    errors: list[str] = []
    for row in levels:
        price_raw: Any
        size_raw: Any
        if hasattr(row, "price") and hasattr(row, "size"):
            price_raw = row.price
            size_raw = row.size
        elif isinstance(row, Mapping):
            price_raw = row.get("price")
            size_raw = row.get("size")
        elif isinstance(row, tuple | list) and len(row) >= 2:
            price_raw = row[0]
            size_raw = row[1]
        else:
            errors.append("invalid_price_level")
            errors.append("invalid_size_level")
            continue
        price, price_errors = _parse_price(price_raw)
        size, size_errors = _parse_size(size_raw, allow_zero=allow_zero)
        errors.extend(price_errors)
        errors.extend(size_errors)
        if price_errors or size_errors or price is None or size is None:
            continue
        parsed.append(ParsedLevel(price=price, size=size))
    return parsed, errors


def _parse_price(value: Any) -> tuple[Decimal | None, list[str]]:
    errors: list[str] = []
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None, ["invalid_price_level"]
    if not parsed.is_finite():
        errors.extend(("non_finite_price", "invalid_price_level"))
    elif parsed <= DECIMAL_ZERO:
        errors.extend(("negative_price", "invalid_price_level"))
    return (None if errors else parsed), errors


def _parse_size(value: Any, *, allow_zero: bool) -> tuple[Decimal | None, list[str]]:
    errors: list[str] = []
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None, ["invalid_size_level"]
    if not parsed.is_finite():
        errors.extend(("non_finite_size", "invalid_size_level"))
    elif parsed < DECIMAL_ZERO or (parsed == DECIMAL_ZERO and not allow_zero):
        errors.extend(("negative_size", "invalid_size_level"))
    return (None if errors else parsed), errors


def _decimal_or_none(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _apply_level(
    book: MutableMapping[Decimal, Decimal],
    sorted_prices_asc: list[Decimal],
    level: ParsedLevel,
) -> None:
    exists = level.price in book
    if level.size == DECIMAL_ZERO:
        if exists:
            book.pop(level.price, None)
            index = bisect.bisect_left(sorted_prices_asc, level.price)
            if index < len(sorted_prices_asc) and sorted_prices_asc[index] == level.price:
                sorted_prices_asc.pop(index)
    else:
        if not exists:
            bisect.insort(sorted_prices_asc, level.price)
        book[level.price] = level.size


def _active_level_errors(book: Mapping[Decimal, Decimal]) -> tuple[str, ...]:
    errors: list[str] = []
    for price, size in book.items():
        if not price.is_finite():
            errors.extend(("non_finite_price", "invalid_price_level"))
        if not size.is_finite():
            errors.extend(("non_finite_size", "invalid_size_level"))
        if price <= DECIMAL_ZERO:
            errors.extend(("negative_price", "invalid_price_level"))
        if size <= DECIMAL_ZERO:
            errors.extend(("negative_size", "invalid_size_level"))
    return tuple(errors)
