from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.marketdata.orderbook_state import OrderbookSnapshot, OrderbookState

ORDERBOOK_PRICE_TOLERANCE = Decimal("0.01")
ORDERBOOK_SIZE_TOLERANCE = Decimal("0.000000001")


@dataclass(frozen=True, slots=True)
class OrderbookQualityResult:
    is_valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    bid_count: int
    ask_count: int
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    mid: Decimal | None
    book_age_ms: float | None
    strict_mismatch_details: dict[str, Any]
    tolerant_mismatch_details: dict[str, Any]
    lifecycle_flags: dict[str, Any]


class OrderbookQualityValidator:
    def __init__(
        self,
        *,
        stale_after_ms: float = 1_000.0,
        queue_lag_warning_ms: float = 50.0,
        queue_lag_severe_ms: float = 250.0,
        price_tolerance: Decimal = ORDERBOOK_PRICE_TOLERANCE,
        size_tolerance: Decimal = ORDERBOOK_SIZE_TOLERANCE,
    ) -> None:
        self.stale_after_ms = stale_after_ms
        self.queue_lag_warning_ms = queue_lag_warning_ms
        self.queue_lag_severe_ms = queue_lag_severe_ms
        self.price_tolerance = price_tolerance
        self.size_tolerance = size_tolerance

    def validate(
        self,
        snapshot: OrderbookSnapshot,
        *,
        state: OrderbookState | None = None,
        now_monotonic_ns: int | None = None,
        reported_best_bid: Decimal | str | float | None = None,
        reported_best_ask: Decimal | str | float | None = None,
        queue_lag_ms: float | None = None,
    ) -> OrderbookQualityResult:
        errors: list[str] = []
        warnings: list[str] = []
        if state is not None:
            errors.extend(state.validate())
            if not state.snapshot_ready or not state.ready_to_emit:
                errors.append("sample_before_book_ready")
            if not state.sequence_continuous:
                errors.append("sequence_gap_or_reset")
        else:
            errors.extend(_snapshot_structure_errors(snapshot))

        if snapshot.snapshot_version != snapshot.state_version:
            errors.append("snapshot_inconsistent")
        errors.extend(_top_level_errors(snapshot))

        book_age_ms = _book_age_ms(snapshot, now_monotonic_ns=now_monotonic_ns)
        if book_age_ms is not None and book_age_ms > self.stale_after_ms:
            errors.append("stale_book")

        if queue_lag_ms is not None:
            if queue_lag_ms > self.queue_lag_severe_ms:
                errors.append("queue_lag_exceeded")
            elif queue_lag_ms > self.queue_lag_warning_ms:
                warnings.append("queue_lag_exceeded")

        strict_details, tolerant_details = self._mismatch_details(
            snapshot,
            reported_best_bid=reported_best_bid,
            reported_best_ask=reported_best_ask,
        )
        for reason, mismatch in (
            ("reported_best_bid_mismatch", strict_details["bid_mismatch"]),
            ("reported_best_ask_mismatch", strict_details["ask_mismatch"]),
        ):
            if mismatch:
                errors.append(reason)

        errors = sorted(set(errors))
        warnings = sorted(set(warnings))
        return OrderbookQualityResult(
            is_valid=not errors,
            errors=tuple(errors),
            warnings=tuple(warnings),
            bid_count=snapshot.bid_count,
            ask_count=snapshot.ask_count,
            best_bid=snapshot.best_bid,
            best_ask=snapshot.best_ask,
            spread=snapshot.spread,
            mid=snapshot.mid,
            book_age_ms=book_age_ms,
            strict_mismatch_details=strict_details,
            tolerant_mismatch_details=tolerant_details,
            lifecycle_flags=_lifecycle_flags(state),
        )

    def _mismatch_details(
        self,
        snapshot: OrderbookSnapshot,
        *,
        reported_best_bid: Decimal | str | float | None,
        reported_best_ask: Decimal | str | float | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        reported_bid = _decimal_or_none(reported_best_bid)
        reported_ask = _decimal_or_none(reported_best_ask)
        bid_diff = (
            None
            if snapshot.best_bid is None or reported_bid is None
            else abs(snapshot.best_bid - reported_bid)
        )
        ask_diff = (
            None
            if snapshot.best_ask is None or reported_ask is None
            else abs(snapshot.best_ask - reported_ask)
        )
        strict_bid_mismatch = bid_diff is not None and bid_diff != Decimal("0")
        strict_ask_mismatch = ask_diff is not None and ask_diff != Decimal("0")
        tolerant_bid_mismatch = (
            bid_diff is not None and bid_diff > self.price_tolerance
        )
        tolerant_ask_mismatch = (
            ask_diff is not None and ask_diff > self.price_tolerance
        )
        strict = {
            "bid_mismatch": strict_bid_mismatch,
            "ask_mismatch": strict_ask_mismatch,
            "strict_mismatch": strict_bid_mismatch or strict_ask_mismatch,
            "computed_best_bid": snapshot.best_bid,
            "computed_best_ask": snapshot.best_ask,
            "reported_best_bid": reported_bid,
            "reported_best_ask": reported_ask,
            "bid_diff": bid_diff,
            "ask_diff": ask_diff,
        }
        tolerant = {
            "bid_mismatch": tolerant_bid_mismatch,
            "ask_mismatch": tolerant_ask_mismatch,
            "tolerant_mismatch": tolerant_bid_mismatch or tolerant_ask_mismatch,
            "price_tolerance": self.price_tolerance,
            "size_tolerance": self.size_tolerance,
            "bid_diff": bid_diff,
            "ask_diff": ask_diff,
        }
        return strict, tolerant


def _snapshot_structure_errors(snapshot: OrderbookSnapshot) -> list[str]:
    errors: list[str] = []
    if snapshot.bid_count == 0 and snapshot.ask_count == 0:
        errors.append("book_empty")
    if snapshot.bid_count == 0 or snapshot.ask_count == 0:
        errors.append("one_side_missing")
    if snapshot.best_bid is None:
        errors.append("best_bid_missing")
    if snapshot.best_ask is None:
        errors.append("best_ask_missing")
    if (
        snapshot.best_bid is not None
        and snapshot.best_ask is not None
        and snapshot.best_bid >= snapshot.best_ask
    ):
        errors.append("crossed_book")
    return errors


def _top_level_errors(snapshot: OrderbookSnapshot) -> list[str]:
    errors: list[str] = []
    for price, size in snapshot.bids_top_n + snapshot.asks_top_n:
        if not price.is_finite():
            errors.extend(("non_finite_price", "invalid_price_level"))
        if not size.is_finite():
            errors.extend(("non_finite_size", "invalid_size_level"))
        if price <= Decimal("0"):
            errors.extend(("negative_price", "invalid_price_level"))
        if size <= Decimal("0"):
            errors.extend(("negative_size", "invalid_size_level"))
    return errors


def _book_age_ms(
    snapshot: OrderbookSnapshot,
    *,
    now_monotonic_ns: int | None,
) -> float | None:
    if now_monotonic_ns is None or snapshot.last_book_update_monotonic_ns is None:
        return None
    return (
        now_monotonic_ns - snapshot.last_book_update_monotonic_ns
    ) / 1_000_000.0


def _lifecycle_flags(state: OrderbookState | None) -> dict[str, Any]:
    return {
        "snapshot_ready": bool(state.snapshot_ready) if state is not None else False,
        "ready_to_emit": bool(state.ready_to_emit) if state is not None else False,
        "after_reconnect": False,
        "sequence_continuous": (
            bool(state.sequence_continuous) if state is not None else False
        ),
        "market_status_known": False,
    }


def _decimal_or_none(value: Decimal | str | float | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))
