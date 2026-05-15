from typing import Any, cast

from app.core.events import MarketTick, OrderBookTop, PolymarketQuote, PolymarketSideLabel


def normalize_binance_trade(payload: dict[str, Any]) -> MarketTick:
    """Normalize a Binance trade-like payload into a MarketTick."""

    return MarketTick(
        source="binance",
        symbol=str(payload["symbol"]),
        price=float(payload["price"]),
        size=float(payload["size"]),
        exchange_ts_ns=payload.get("exchange_ts_ns"),
        sequence=payload.get("sequence"),
    )


def normalize_binance_book_top(payload: dict[str, Any]) -> OrderBookTop:
    """Normalize a Binance top-of-book payload."""

    return OrderBookTop(
        source="binance",
        symbol=str(payload["symbol"]),
        bid_price=float(payload["bid_price"]),
        bid_size=float(payload["bid_size"]),
        ask_price=float(payload["ask_price"]),
        ask_size=float(payload["ask_size"]),
        exchange_ts_ns=payload.get("exchange_ts_ns"),
        sequence=payload.get("sequence"),
    )


def normalize_polymarket_quote(payload: dict[str, Any]) -> PolymarketQuote:
    """Normalize a Polymarket quote payload."""

    best_bid = _optional_float(payload.get("best_bid"))
    best_ask = _optional_float(payload.get("best_ask"))
    spread = None if best_bid is None or best_ask is None else max(0.0, best_ask - best_bid)
    mid_price = None if best_bid is None or best_ask is None else (best_bid + best_ask) / 2.0
    return PolymarketQuote(
        market_id=str(payload["market_id"]),
        condition_id=payload.get("condition_id"),
        token_id=str(payload["token_id"]),
        side_label=cast(PolymarketSideLabel, str(payload.get("side_label", "YES")).upper()),
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid_price,
        spread=spread,
        available_liquidity_at_best=_optional_float(payload.get("available_liquidity_at_best")),
        event_ts=payload.get("event_ts"),
        received_ts=payload.get("received_ts"),
        exchange_event_ts=payload.get("event_ts") or payload.get("exchange_event_ts"),
        local_received_ts=payload.get("received_ts") or payload.get("local_received_ts"),
        exchange_ts_ns=payload.get("exchange_ts_ns"),
        sequence=payload.get("sequence"),
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float | str | bytes):
        return None
    return float(value)
