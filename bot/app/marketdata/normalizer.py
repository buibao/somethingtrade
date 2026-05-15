from typing import Any

from app.core.events import MarketTick, OrderBookTop, PolymarketQuote


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

    return PolymarketQuote(
        market_id=str(payload["market_id"]),
        condition_id=payload.get("condition_id"),
        token_id=str(payload["token_id"]),
        outcome=str(payload["outcome"]),
        bid_probability=float(payload["bid_probability"]),
        ask_probability=float(payload["ask_probability"]),
        bid_size=float(payload["bid_size"]),
        ask_size=float(payload["ask_size"]),
        exchange_ts_ns=payload.get("exchange_ts_ns"),
        sequence=payload.get("sequence"),
    )
