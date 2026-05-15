from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Literal


def price_to_ticks(price: float, tick_size: float) -> float:
    """Convert an absolute price to fractional ticks."""

    _validate_tick_size(tick_size)
    return float(_decimal(price) / _decimal(tick_size))


def diff_to_ticks(diff: float, tick_size: float) -> float:
    """Convert a price difference to fractional ticks."""

    _validate_tick_size(tick_size)
    return float(_decimal(diff) / _decimal(tick_size))


def round_price_to_tick(
    price: float,
    tick_size: float,
    side: Literal["buy", "sell"] | None = None,
) -> float:
    """Round a price to the tick grid without crossing unintentionally.

    For `buy`, round down so measurement/execution helpers do not overpay by
    accident. For `sell`, round up so they do not underprice by accident. With
    no side, use nearest tick.
    """

    _validate_tick_size(tick_size)
    price_decimal = _decimal(price)
    tick_decimal = _decimal(tick_size)
    ticks = price_decimal / tick_decimal
    if side == "buy":
        rounded_ticks = ticks.to_integral_value(rounding=ROUND_FLOOR)
    elif side == "sell":
        rounded_ticks = ticks.to_integral_value(rounding=ROUND_CEILING)
    else:
        rounded_ticks = ticks.to_integral_value(rounding=ROUND_HALF_UP)
    return float(rounded_ticks * tick_decimal)


def is_price_on_tick(price: float, tick_size: float, tolerance: float = 1e-9) -> bool:
    """Return true when `price` is on the tick grid within tolerance."""

    _validate_tick_size(tick_size)
    rounded = round_price_to_tick(price, tick_size)
    return abs(rounded - price) <= tolerance


def _validate_tick_size(tick_size: float) -> None:
    if tick_size <= 0.0:
        raise ValueError("tick_size must be positive")


def _decimal(value: float) -> Decimal:
    return Decimal(str(value))
