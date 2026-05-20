from __future__ import annotations

import math
from typing import Any


def parse_aggtrade_payload(
    payload: dict[str, Any],
    *,
    local_recv_monotonic_ns: int,
    local_recv_wall_ts: str,
) -> dict[str, Any]:
    """Normalize a Binance aggregate trade websocket payload for Phase 4.2C."""

    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    errors: list[str] = []
    aggregate_trade_id = payload.get("a")
    symbol = payload.get("s")
    price = _optional_positive_float(
        payload.get("p"),
        missing_reason="MISSING_PRICE",
        invalid_reason="INVALID_PRICE",
        non_positive_reason="NON_POSITIVE_PRICE",
        errors=errors,
    )
    quantity = _optional_float(
        payload.get("q"),
        missing_reason="MISSING_QUANTITY",
        invalid_reason="INVALID_QUANTITY",
        errors=errors,
    )
    if quantity is not None and quantity < 0:
        errors.append("NEGATIVE_QUANTITY")
    if aggregate_trade_id is None:
        errors.append("MISSING_AGGREGATE_TRADE_ID")
    elif not isinstance(aggregate_trade_id, int) or isinstance(aggregate_trade_id, bool):
        try:
            aggregate_trade_id = int(aggregate_trade_id)
        except (TypeError, ValueError):
            errors.append("INVALID_AGGREGATE_TRADE_ID")
    if not symbol:
        errors.append("MISSING_SYMBOL")
    if not isinstance(local_recv_monotonic_ns, int) or isinstance(local_recv_monotonic_ns, bool):
        errors.append("MISSING_LOCAL_RECV_MONOTONIC_NS")
    if not local_recv_wall_ts:
        errors.append("MISSING_LOCAL_RECV_WALL_TS")

    return {
        "schema_version": "aggtrade_reference_v1",
        "symbol": str(symbol).upper() if symbol else symbol,
        "source": "binance_ws_aggTrade",
        "local_recv_monotonic_ns": local_recv_monotonic_ns,
        "local_recv_wall_ts": local_recv_wall_ts,
        "exchange_event_ts": payload.get("E"),
        "trade_time": payload.get("T"),
        "aggregate_trade_id": aggregate_trade_id,
        "first_trade_id": payload.get("f"),
        "last_trade_id": payload.get("l"),
        "price": price,
        "quantity": quantity,
        "is_buyer_market_maker": payload.get("m"),
        "quality": {
            "valid": not errors,
            "errors": sorted(set(errors)),
        },
    }


def _optional_float(
    value: Any,
    *,
    missing_reason: str,
    invalid_reason: str,
    errors: list[str],
) -> float | None:
    if value is None:
        errors.append(missing_reason)
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        errors.append(invalid_reason)
        return None
    if not math.isfinite(result):
        errors.append(invalid_reason)
        return None
    return result


def _optional_positive_float(
    value: Any,
    *,
    missing_reason: str,
    invalid_reason: str,
    non_positive_reason: str,
    errors: list[str],
) -> float | None:
    result = _optional_float(
        value,
        missing_reason=missing_reason,
        invalid_reason=invalid_reason,
        errors=errors,
    )
    if result is not None and result <= 0:
        errors.append(non_positive_reason)
    return result
