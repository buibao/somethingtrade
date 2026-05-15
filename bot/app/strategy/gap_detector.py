from __future__ import annotations

from dataclasses import dataclass
from statistics import median

from app.core.clock import utc_now_ns
from app.core.events import (
    DepthUpdate,
    GapDirection,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    TradableGapObservation,
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


@dataclass(slots=True)
class PendingTradableGap:
    symbol: str
    market: PolymarketMarketMetadata
    direction: GapDirection
    token_id: str
    binance_move_pct: float
    first_detected_ts_ns: int
    binance_event_ts_ns: int | None
    before_quote_ts_ns: int | None
    before_best_bid: float | None
    before_best_ask: float | None
    before_best_bid_size: float | None
    before_best_ask_size: float | None
    before_mid: float | None
    spread_before: float | None
    hypothetical_entry_price: float | None
    quote_was_fillable: bool
    pre_reject_reason: str | None
    last_tradable_ts_ns: int | None
    first_non_tradable_ts_ns: int | None
    repriced_ts_ns: int | None = None
    current_best_bid: float | None = None
    current_best_ask: float | None = None
    current_best_bid_size: float | None = None
    current_best_ask_size: float | None = None
    current_mid: float | None = None
    current_spread: float | None = None
    edge_after_spread_latest: float | None = None
    invalid_reason: str | None = None


@dataclass(frozen=True, slots=True)
class GapMonitorStats:
    detected_gaps: int
    completed_gaps: int
    median_gap_duration_ms: float | None
    p95_gap_duration_ms: float | None
    average_estimated_edge: float | None
    stale_feed_count: int


class GapDetector:
    """Measure Binance-led Polymarket repricing delays and true tradable windows."""

    def __init__(
        self,
        markets: tuple[PolymarketMarketMetadata, ...],
        *,
        min_move_pct: float = 0.10,
        reprice_threshold: float = 0.005,
        max_entry_spread: float = 0.05,
        max_entry_price_move: float = 0.02,
        binance_stale_ms: float = 500.0,
        polymarket_stale_ms: float = 1_000.0,
        measurement_stale_ms: float = 5_000.0,
    ) -> None:
        self.markets = markets
        self.min_move_pct = min_move_pct
        self.reprice_threshold = reprice_threshold
        self.max_entry_spread = max_entry_spread
        self.max_entry_price_move = max_entry_price_move
        self.binance_stale_ms = binance_stale_ms
        self.polymarket_stale_ms = polymarket_stale_ms
        self.measurement_stale_ms = measurement_stale_ms
        self._markets_by_symbol = _markets_by_symbol(markets)
        self._pending: dict[tuple[str, str, GapDirection], PendingTradableGap] = {}
        self._completed: list[TradableGapObservation] = []
        self._invalid_markets: set[str] = set()
        self.detected_count = 0

    def on_market_event(
        self,
        event: MarketTick | OrderBookTop | DepthUpdate | PolymarketQuote | MarketLifecycleEvent,
        state: MarketState,
        *,
        now_ts: int | None = None,
    ) -> tuple[TradableGapObservation, ...]:
        current_ts = now_ts or utc_now_ns()
        if isinstance(event, MarketTick) and event.source == "binance":
            self._detect_binance_move(
                event.symbol,
                state,
                now_ts=current_ts,
                binance_event_ts=event.exchange_event_ts or event.local_received_ts,
            )
            return ()
        if isinstance(event, OrderBookTop) and event.source == "binance":
            self._detect_binance_move(
                event.symbol,
                state,
                now_ts=current_ts,
                binance_event_ts=event.exchange_event_ts or event.local_received_ts,
            )
            return ()
        if isinstance(event, PolymarketQuote):
            return self._handle_polymarket_quote(event, now_ts=current_ts)
        if isinstance(event, MarketLifecycleEvent):
            self._handle_lifecycle(event)
            return ()
        return ()

    def stats(self, state: MarketState, *, now_ts: int | None = None) -> GapMonitorStats:
        current_ts = now_ts or utc_now_ns()
        durations = [
            event.gap_duration_ms
            for event in self._completed
            if event.gap_duration_ms is not None
        ]
        executable_edges = [
            event.estimated_edge_after_spread
            for event in self._completed
            if event.reject_reason is None and event.estimated_edge_after_spread is not None
        ]
        return GapMonitorStats(
            detected_gaps=self.detected_count,
            completed_gaps=len(self._completed),
            median_gap_duration_ms=median(durations) if durations else None,
            p95_gap_duration_ms=_percentile(durations, 0.95) if durations else None,
            average_estimated_edge=(
                sum(executable_edges) / len(executable_edges) if executable_edges else None
            ),
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
                stale_ms=self.measurement_stale_ms,
            ):
                stale += 1

        for market in self.markets:
            for token_id in market.token_ids:
                quote = state.polymarket_quotes.get(token_id)
                quote_ts = None if quote is None else quote.received_ts or quote.local_received_ts
                if quote is None or _is_stale(
                    quote_ts,
                    now_ts=current_ts,
                    stale_ms=self.measurement_stale_ms,
                ):
                    stale += 1

        return stale

    def _detect_binance_move(
        self,
        symbol: str,
        state: MarketState,
        *,
        now_ts: int,
        binance_event_ts: int | None,
    ) -> None:
        markets = self._markets_by_symbol.get(symbol, ())
        if not markets:
            return

        symbol_state = state.symbols.get(symbol)
        if symbol_state is None or _is_stale(
            symbol_state.local_receive_timestamp or symbol_state.last_event_timestamp,
            now_ts=now_ts,
            stale_ms=self.binance_stale_ms,
        ):
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
            if market.market_id in self._invalid_markets:
                continue
            token_id = market.token_for_direction(direction)
            if token_id is None:
                continue

            pending_key = (symbol, market.market_id, direction)
            if pending_key in self._pending:
                continue

            quote = state.polymarket_quotes.get(token_id)
            if quote is None or _is_stale(
                quote.received_ts or quote.local_received_ts or quote.event_ts,
                now_ts=now_ts,
                stale_ms=self.polymarket_stale_ms,
            ):
                continue

            fillable, reject_reason = self._fillable_before_repricing(market, quote)
            self._pending[pending_key] = PendingTradableGap(
                symbol=symbol,
                market=market,
                direction=direction,
                token_id=token_id,
                binance_move_pct=move_pct,
                first_detected_ts_ns=now_ts,
                binance_event_ts_ns=binance_event_ts,
                before_quote_ts_ns=quote.event_ts or quote.received_ts,
                before_best_bid=quote.best_bid,
                before_best_ask=quote.best_ask,
                before_best_bid_size=quote.best_bid_size,
                before_best_ask_size=quote.best_ask_size,
                before_mid=quote.mid_price,
                spread_before=quote.spread,
                hypothetical_entry_price=quote.best_ask,
                quote_was_fillable=fillable,
                pre_reject_reason=reject_reason,
                last_tradable_ts_ns=now_ts if fillable else None,
                first_non_tradable_ts_ns=None if fillable else now_ts,
                current_best_bid=quote.best_bid,
                current_best_ask=quote.best_ask,
                current_best_bid_size=quote.best_bid_size,
                current_best_ask_size=quote.best_ask_size,
                current_mid=quote.mid_price,
                current_spread=quote.spread,
            )
            self.detected_count += 1

    def _handle_polymarket_quote(
        self,
        quote: PolymarketQuote,
        *,
        now_ts: int,
    ) -> tuple[TradableGapObservation, ...]:
        closed: list[TradableGapObservation] = []
        for key, pending in list(self._pending.items()):
            if pending.token_id != quote.token_id:
                continue

            quote_ts = quote.received_ts or quote.local_received_ts or now_ts
            repriced = self._has_repriced_in_expected_direction(pending, quote)
            self._update_pending_window(
                pending,
                quote,
                now_ts=now_ts,
                quote_ts=quote_ts,
                is_repricing_quote=repriced,
            )

            if not repriced:
                continue

            pending.repriced_ts_ns = quote_ts
            observation = self._observation_from_pending(pending, quote)
            closed.append(observation)
            self._completed.append(observation)
            del self._pending[key]

        return tuple(closed)

    def _update_pending_window(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
        *,
        now_ts: int,
        quote_ts: int,
        is_repricing_quote: bool,
    ) -> None:
        pending.current_best_bid = quote.best_bid
        pending.current_best_ask = quote.best_ask
        pending.current_best_bid_size = quote.best_bid_size
        pending.current_best_ask_size = quote.best_ask_size
        pending.current_mid = quote.mid_price
        pending.current_spread = quote.spread
        pending.edge_after_spread_latest = _diff(quote.best_bid, pending.hypothetical_entry_price)

        if pending.first_non_tradable_ts_ns is not None:
            return

        end_reason = self._window_end_reason(pending, quote, now_ts=now_ts)
        if end_reason is None:
            pending.last_tradable_ts_ns = quote_ts
            return

        pending.first_non_tradable_ts_ns = quote_ts
        if not is_repricing_quote:
            pending.invalid_reason = end_reason

    def _handle_lifecycle(self, event: MarketLifecycleEvent) -> None:
        if event.lifecycle_type == "new_market":
            return

        self._invalid_markets.add(event.market_id)
        for key, pending in list(self._pending.items()):
            if pending.market.market_id == event.market_id:
                del self._pending[key]

    def _has_repriced_in_expected_direction(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
    ) -> bool:
        if pending.before_mid is None or quote.mid_price is None:
            return False
        threshold = max(self.reprice_threshold, pending.market.tick_size)
        return quote.mid_price >= pending.before_mid + threshold

    def _window_end_reason(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
        *,
        now_ts: int,
    ) -> str | None:
        quote_ts = quote.received_ts or quote.local_received_ts or quote.event_ts
        if quote.book_stale or _is_stale(
            quote_ts,
            now_ts=now_ts,
            stale_ms=self.polymarket_stale_ms,
        ):
            return "quote_stale"
        if pending.market.market_id in self._invalid_markets:
            return "market_invalidated"
        if quote.best_ask is None:
            return "missing_best_ask"
        if quote.best_ask_size is None:
            return "missing_best_ask_size"
        if quote.best_ask_size < pending.market.min_order_size:
            return "insufficient_best_ask_size"
        if quote.spread is None:
            return "missing_spread"
        if quote.spread > self.max_entry_spread:
            return "spread_too_wide"
        if (
            pending.hypothetical_entry_price is not None
            and quote.best_ask > pending.hypothetical_entry_price + self.max_entry_price_move
        ):
            return "entry_price_moved"
        if pending.edge_after_spread_latest is not None and pending.edge_after_spread_latest <= 0.0:
            return "edge_not_positive_after_spread"
        return None

    def _fillable_before_repricing(
        self,
        market: PolymarketMarketMetadata,
        quote: PolymarketQuote,
    ) -> tuple[bool, str | None]:
        if not quote.book_complete:
            return False, "book_incomplete"
        if quote.book_stale:
            return False, "book_stale"
        if quote.best_ask is None:
            return False, "missing_best_ask"
        if quote.best_ask_size is None:
            return False, "missing_best_ask_size"
        if quote.best_ask_size < market.min_order_size:
            return False, "insufficient_best_ask_size"
        if quote.spread is None:
            return False, "missing_spread"
        if quote.spread > self.max_entry_spread:
            return False, "spread_too_wide"
        return True, None

    def _observation_from_pending(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
    ) -> TradableGapObservation:
        repriced_ts = pending.repriced_ts_ns or quote.received_ts or quote.local_received_ts
        gap_duration_ms = (
            None
            if repriced_ts is None
            else (repriced_ts - pending.first_detected_ts_ns) / 1_000_000.0
        )
        raw_edge = _diff(quote.mid_price, pending.before_mid)
        after_spread_edge = _diff(quote.best_bid, pending.hypothetical_entry_price)
        reject_reason = self._final_reject_reason(
            pending,
            after_spread_edge=after_spread_edge,
            after_best_bid=quote.best_bid,
        )
        tradable_window_ms = self._tradable_window_ms(pending, repriced_ts=repriced_ts)

        return TradableGapObservation(
            symbol=pending.symbol,
            market_id=pending.market.market_id,
            token_id=pending.token_id,
            direction=pending.direction,
            binance_move_pct=pending.binance_move_pct,
            detected_ts_ns=pending.first_detected_ts_ns,
            binance_event_ts_ns=pending.binance_event_ts_ns,
            poly_quote_ts_ns=quote.event_ts or quote.received_ts,
            before_best_bid=pending.before_best_bid,
            before_best_ask=pending.before_best_ask,
            before_best_bid_size=pending.before_best_bid_size,
            before_best_ask_size=pending.before_best_ask_size,
            before_mid=pending.before_mid,
            after_best_bid=quote.best_bid,
            after_best_ask=quote.best_ask,
            after_mid=quote.mid_price,
            spread_before=pending.spread_before,
            spread_after=quote.spread,
            gap_duration_ms=gap_duration_ms,
            tradable_window_ms=tradable_window_ms if reject_reason is None else tradable_window_ms,
            hypothetical_entry_price=pending.hypothetical_entry_price,
            hypothetical_exit_price=quote.best_bid,
            quote_was_fillable=pending.quote_was_fillable,
            estimated_edge_raw=raw_edge,
            estimated_edge_after_spread=after_spread_edge if reject_reason is None else None,
            reject_reason=reject_reason,
        )

    def _tradable_window_ms(
        self,
        pending: PendingTradableGap,
        *,
        repriced_ts: int | None,
    ) -> float:
        if not pending.quote_was_fillable:
            return 0.0
        end_ts = pending.first_non_tradable_ts_ns or repriced_ts or pending.last_tradable_ts_ns
        if end_ts is None:
            return 0.0
        return max(0.0, (end_ts - pending.first_detected_ts_ns) / 1_000_000.0)

    def _final_reject_reason(
        self,
        pending: PendingTradableGap,
        *,
        after_spread_edge: float | None,
        after_best_bid: float | None,
    ) -> str | None:
        if pending.pre_reject_reason is not None:
            return pending.pre_reject_reason
        if pending.invalid_reason is not None:
            return pending.invalid_reason
        if after_best_bid is None:
            return "missing_exit_bid"
        if after_spread_edge is None or after_spread_edge <= 0.0:
            return "edge_not_positive_after_spread"
        return None


def build_move_snapshot(symbol_state: SymbolState) -> BinanceMoveSnapshot:
    return BinanceMoveSnapshot(
        return_1s=symbol_state.rolling_returns.get("1s"),
        return_5s=symbol_state.rolling_returns.get("5s"),
        return_15s=symbol_state.rolling_returns.get("15s"),
        return_30s=symbol_state.rolling_returns.get("30s"),
        volatility_30s=symbol_state.volatility_30s,
        bid_ask_spread=symbol_state.bid_ask_spread,
    )


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


def _is_stale(timestamp: int | None, *, now_ts: int, stale_ms: float) -> bool:
    if timestamp is None:
        return True
    return (now_ts - timestamp) / 1_000_000.0 > stale_ms


def _diff(after: float | None, before: float | None) -> float | None:
    if after is None or before is None:
        return None
    return after - before


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, round((len(sorted_values) - 1) * percentile))
    return sorted_values[index]
