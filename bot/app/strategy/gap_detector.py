from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Literal

from app.core.clock import utc_now_ns
from app.core.events import (
    DepthUpdate,
    GapDirection,
    GapEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
)
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.state.market_state import MarketState, SymbolState

RETURN_KEYS = ("1s", "5s", "15s", "30s")


@dataclass(frozen=True, slots=True)
class BinanceMoveSnapshot:
    return_1s: float | None
    return_5s: float | None
    return_15s: float | None
    return_30s: float | None
    volatility_30s: float | None
    bid_ask_spread: float | None

    @property
    def strongest_return(self) -> float | None:
        values = [
            value
            for value in (
                self.return_1s,
                self.return_5s,
                self.return_15s,
                self.return_30s,
            )
            if value is not None
        ]
        if not values:
            return None
        return max(values, key=abs)


@dataclass(frozen=True, slots=True)
class PendingGap:
    symbol: str
    market: PolymarketMarketMetadata
    direction: GapDirection
    token_id: str
    binance_move_pct: float
    poly_market_price_before: float | None
    detected_ts: int
    estimated_edge: float


@dataclass(frozen=True, slots=True)
class GapMonitorStats:
    detected_gaps: int
    completed_gaps: int
    median_gap_duration_ms: float | None
    p95_gap_duration_ms: float | None
    average_estimated_edge: float | None
    stale_feed_count: int


class GapDetector:
    """Detect Binance-led moves and measure delayed Polymarket repricing."""

    def __init__(
        self,
        markets: tuple[PolymarketMarketMetadata, ...],
        *,
        min_move_pct: float = 0.10,
        reprice_threshold: float = 0.005,
        probability_move_multiplier: float = 1.0,
        stale_feed_ms: float = 5_000.0,
    ) -> None:
        self.markets = markets
        self.min_move_pct = min_move_pct
        self.reprice_threshold = reprice_threshold
        self.probability_move_multiplier = probability_move_multiplier
        self.stale_feed_ms = stale_feed_ms
        self._markets_by_symbol = _markets_by_symbol(markets)
        self._pending: dict[tuple[str, str, GapDirection], PendingGap] = {}
        self._completed: list[GapEvent] = []
        self.detected_count = 0

    def on_market_event(
        self,
        event: MarketTick | OrderBookTop | DepthUpdate | PolymarketQuote,
        state: MarketState,
        *,
        now_ts: int | None = None,
    ) -> tuple[GapEvent, ...]:
        current_ts = now_ts or utc_now_ns()
        if isinstance(event, MarketTick) and event.source == "binance":
            self._detect_binance_move(event.symbol, state, now_ts=current_ts)
            return ()
        if isinstance(event, OrderBookTop) and event.source == "binance":
            self._detect_binance_move(event.symbol, state, now_ts=current_ts)
            return ()
        if isinstance(event, PolymarketQuote):
            return self._detect_polymarket_repricing(event, now_ts=current_ts)
        return ()

    def stats(self, state: MarketState, *, now_ts: int | None = None) -> GapMonitorStats:
        current_ts = now_ts or utc_now_ns()
        durations = [
            event.gap_duration_ms
            for event in self._completed
            if event.gap_duration_ms is not None
        ]
        edges = [event.estimated_edge for event in self._completed]
        return GapMonitorStats(
            detected_gaps=self.detected_count,
            completed_gaps=len(self._completed),
            median_gap_duration_ms=median(durations) if durations else None,
            p95_gap_duration_ms=_percentile(durations, 0.95) if durations else None,
            average_estimated_edge=sum(edges) / len(edges) if edges else None,
            stale_feed_count=self.stale_feed_count(state, now_ts=current_ts),
        )

    def stale_feed_count(self, state: MarketState, *, now_ts: int | None = None) -> int:
        current_ts = now_ts or utc_now_ns()
        stale = 0

        for symbol in self._markets_by_symbol:
            symbol_state = state.symbols.get(symbol)
            if symbol_state is None or _is_stale(
                symbol_state.local_receive_timestamp or symbol_state.last_event_timestamp,
                now_ts=current_ts,
                stale_ms=self.stale_feed_ms,
            ):
                stale += 1

        for market in self.markets:
            for token_id in market.token_ids:
                quote = state.polymarket_quotes.get(token_id)
                quote_ts = None if quote is None else quote.received_ts or quote.local_received_ts
                if quote is None or _is_stale(
                    quote_ts,
                    now_ts=current_ts,
                    stale_ms=self.stale_feed_ms,
                ):
                    stale += 1

        return stale

    def _detect_binance_move(
        self,
        symbol: str,
        state: MarketState,
        *,
        now_ts: int,
    ) -> None:
        markets = self._markets_by_symbol.get(symbol, ())
        if not markets:
            return

        symbol_state = state.symbols.get(symbol)
        if symbol_state is None:
            return

        snapshot = build_move_snapshot(symbol_state)
        strongest_return = snapshot.strongest_return
        if strongest_return is None:
            return

        move_pct = strongest_return * 100.0
        if abs(move_pct) < self.min_move_pct:
            return

        direction: GapDirection = "UP" if move_pct > 0.0 else "DOWN"
        for market in markets:
            token_id = market.yes_token_id if direction == "UP" else market.no_token_id
            pending_key = (symbol, market.market_id, direction)
            if pending_key in self._pending:
                continue

            quote = state.polymarket_quotes.get(token_id)
            if quote is None or state.is_stale_quote(quote, now_ts=now_ts):
                continue

            before_price = quote_price(quote)
            estimated_edge = self._estimated_edge(move_pct=move_pct, observed_poly_move=0.0)
            self._pending[pending_key] = PendingGap(
                symbol=symbol,
                market=market,
                direction=direction,
                token_id=token_id,
                binance_move_pct=move_pct,
                poly_market_price_before=before_price,
                detected_ts=now_ts,
                estimated_edge=estimated_edge,
            )
            self.detected_count += 1

    def _detect_polymarket_repricing(
        self,
        quote: PolymarketQuote,
        *,
        now_ts: int,
    ) -> tuple[GapEvent, ...]:
        closed: list[GapEvent] = []
        for key, pending in list(self._pending.items()):
            if pending.token_id != quote.token_id:
                continue

            after_price = quote_price(quote)
            if not self._has_repriced(pending, after_price):
                continue

            repriced_ts = quote.received_ts or quote.local_received_ts or now_ts
            gap_duration_ms = (repriced_ts - pending.detected_ts) / 1_000_000.0
            event = GapEvent(
                symbol=pending.symbol,
                timeframe=_timeframe_label(pending.market.duration_minutes),
                direction=pending.direction,
                binance_move_pct=pending.binance_move_pct,
                poly_market_price_before=pending.poly_market_price_before,
                poly_market_price_after=after_price,
                detected_ts=pending.detected_ts,
                repriced_ts=repriced_ts,
                gap_duration_ms=gap_duration_ms,
                estimated_edge=pending.estimated_edge,
            )
            closed.append(event)
            self._completed.append(event)
            del self._pending[key]

        return tuple(closed)

    def _has_repriced(self, pending: PendingGap, after_price: float | None) -> bool:
        before_price = pending.poly_market_price_before
        if before_price is None or after_price is None:
            return False
        threshold = max(self.reprice_threshold, pending.market.tick_size)
        return after_price >= before_price + threshold

    def _estimated_edge(self, *, move_pct: float, observed_poly_move: float) -> float:
        expected_probability_move = abs(move_pct) / 100.0 * self.probability_move_multiplier
        return max(0.0, expected_probability_move - abs(observed_poly_move))


def build_move_snapshot(symbol_state: SymbolState) -> BinanceMoveSnapshot:
    return BinanceMoveSnapshot(
        return_1s=symbol_state.rolling_returns.get("1s"),
        return_5s=symbol_state.rolling_returns.get("5s"),
        return_15s=symbol_state.rolling_returns.get("15s"),
        return_30s=symbol_state.rolling_returns.get("30s"),
        volatility_30s=symbol_state.volatility_30s,
        bid_ask_spread=symbol_state.bid_ask_spread,
    )


def quote_price(quote: PolymarketQuote) -> float | None:
    return quote.mid_price if quote.mid_price is not None else quote.best_ask


def _markets_by_symbol(
    markets: tuple[PolymarketMarketMetadata, ...],
) -> dict[str, tuple[PolymarketMarketMetadata, ...]]:
    grouped: dict[str, list[PolymarketMarketMetadata]] = {}
    for market in markets:
        symbol = _symbol_for_market(market)
        if symbol is None:
            continue
        grouped.setdefault(symbol, []).append(market)
    return {symbol: tuple(items) for symbol, items in grouped.items()}


def _symbol_for_market(market: PolymarketMarketMetadata) -> str | None:
    if market.base_asset == "BTC":
        return "BTCUSDT"
    if market.base_asset == "ETH":
        return "ETHUSDT"
    text = f"{market.market_slug} {market.question}".lower()
    if "btc" in text or "bitcoin" in text:
        return "BTCUSDT"
    if "eth" in text or "ethereum" in text:
        return "ETHUSDT"
    return None


def _timeframe_label(duration_minutes: int | None) -> Literal["5m", "15m"]:
    return "5m" if duration_minutes == 5 else "15m"


def _is_stale(timestamp: int | None, *, now_ts: int, stale_ms: float) -> bool:
    if timestamp is None:
        return True
    return (now_ts - timestamp) / 1_000_000.0 > stale_ms


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, round((len(sorted_values) - 1) * percentile))
    return sorted_values[index]
