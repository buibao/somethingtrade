from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Literal

from app.core.clock import utc_now_ns
from app.core.events import (
    DepthUpdate,
    GapDirection,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    RejectStage,
    TradableGapObservation,
)
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.state.market_state import MarketState, SymbolState

RETURN_KEYS = ("1s", "5s", "15s", "30s")
QuoteCloseKind = Literal["none", "window", "exit", "timeout"]


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
    first_detected_monotonic_ns: int
    binance_event_ts_ns: int | None
    poly_quote_ts_ns: int | None
    before_best_bid: float | None
    before_best_ask: float
    before_best_bid_size: float | None
    before_best_ask_size: float
    before_mid: float | None
    spread_before: float | None
    entry_ask: float
    entry_ask_size: float
    last_tradable_ts_ns: int
    last_tradable_monotonic_ns: int
    first_non_tradable_ts_ns: int | None = None
    first_non_tradable_monotonic_ns: int | None = None
    first_mid_repriced_ts_ns: int | None = None
    first_mid_repriced_monotonic_ns: int | None = None
    first_executable_repriced_ts_ns: int | None = None
    first_executable_repriced_monotonic_ns: int | None = None
    executable_exit_bid: float | None = None
    current_best_bid: float | None = None
    current_best_ask: float | None = None
    current_best_bid_size: float | None = None
    current_best_ask_size: float | None = None
    current_mid: float | None = None
    current_spread: float | None = None
    current_quote_ts_ns: int | None = None
    current_quote_monotonic_ns: int | None = None
    window_end_reason: str | None = None
    exit_reject_reason: str | None = None
    close_reason: str | None = None
    reject_stage: RejectStage = "none"


@dataclass(frozen=True, slots=True)
class GapMonitorStats:
    detected_gaps: int
    completed_gaps: int
    fillable_at_detection_count: int
    non_fillable_at_detection_count: int
    median_mid_repricing_delay_ms: float | None
    p95_mid_repricing_delay_ms: float | None
    median_executable_repricing_delay_ms: float | None
    p95_executable_repricing_delay_ms: float | None
    median_tradable_window_ms: float | None
    p95_tradable_window_ms: float | None
    average_estimated_edge: float | None
    reject_count_by_reason: dict[str, int]
    reject_count_by_stage: dict[str, int]
    stale_feed_count: int

    @property
    def detected_count(self) -> int:
        return self.detected_gaps

    @property
    def completed_count(self) -> int:
        return self.completed_gaps

    @property
    def median_repricing_delay_ms(self) -> float | None:
        return self.median_executable_repricing_delay_ms

    @property
    def p95_repricing_delay_ms(self) -> float | None:
        return self.p95_executable_repricing_delay_ms

    @property
    def median_gap_duration_ms(self) -> float | None:
        return self.median_executable_repricing_delay_ms

    @property
    def p95_gap_duration_ms(self) -> float | None:
        return self.p95_executable_repricing_delay_ms

class GapDetector:
    """Measure Binance-led repricing delays and executable stale-quote windows."""

    def __init__(
        self,
        markets: tuple[PolymarketMarketMetadata, ...],
        *,
        min_move_pct: float = 0.10,
        reprice_threshold: float = 0.005,
        min_exit_edge: float = 0.0,
        max_entry_spread: float = 0.05,
        max_entry_price_move: float = 0.02,
        max_pending_gap_ms: float = 5_000.0,
        binance_stale_ms: float = 500.0,
        polymarket_stale_ms: float = 1_000.0,
        measurement_stale_ms: float = 5_000.0,
    ) -> None:
        self.markets = markets
        self.min_move_pct = min_move_pct
        self.reprice_threshold = reprice_threshold
        self.min_exit_edge = min_exit_edge
        self.max_entry_spread = max_entry_spread
        self.max_entry_price_move = max_entry_price_move
        self.max_pending_gap_ms = max_pending_gap_ms
        self.binance_stale_ms = binance_stale_ms
        self.polymarket_stale_ms = polymarket_stale_ms
        self.measurement_stale_ms = measurement_stale_ms
        self._markets_by_symbol = _markets_by_symbol(markets)
        self._pending: dict[tuple[str, str, GapDirection], PendingTradableGap] = {}
        self._completed: list[TradableGapObservation] = []
        self._invalid_markets: set[str] = set()
        self._reject_count_by_reason: dict[str, int] = {}
        self._reject_count_by_stage: dict[str, int] = {}
        self.detected_gaps = 0
        self.fillable_at_detection_count = 0
        self.non_fillable_at_detection_count = 0

    def on_market_event(
        self,
        event: MarketTick | OrderBookTop | DepthUpdate | PolymarketQuote | MarketLifecycleEvent,
        state: MarketState,
        *,
        now_ts: int | None = None,
    ) -> tuple[TradableGapObservation, ...]:
        current_ts = now_ts or utc_now_ns()
        current_mono = _event_monotonic(event) or current_ts
        closed: list[TradableGapObservation] = []

        if isinstance(event, MarketTick) and event.source == "binance":
            self._detect_binance_move(
                event.symbol,
                state,
                now_ts=current_ts,
                now_monotonic_ns=current_mono,
                binance_event_ts=event.exchange_event_ts or event.local_received_ts,
            )
        elif isinstance(event, OrderBookTop) and event.source == "binance":
            self._detect_binance_move(
                event.symbol,
                state,
                now_ts=current_ts,
                now_monotonic_ns=current_mono,
                binance_event_ts=event.exchange_event_ts or event.local_received_ts,
            )
        elif isinstance(event, PolymarketQuote):
            closed.extend(
                self._handle_polymarket_quote(
                    event,
                    now_ts=current_ts,
                    now_monotonic_ns=current_mono,
                )
            )
        elif isinstance(event, MarketLifecycleEvent):
            return self._handle_lifecycle(
                event,
                state,
                now_ts=current_ts,
                now_monotonic_ns=current_mono,
            )

        closed.extend(
            self._close_expired_pending(
                state,
                now_ts=current_ts,
                now_monotonic_ns=current_mono,
            )
        )
        return tuple(closed)

    def stats(self, state: MarketState, *, now_ts: int | None = None) -> GapMonitorStats:
        current_ts = now_ts or utc_now_ns()
        mid_delays = [
            event.mid_repricing_delay_ms
            for event in self._completed
            if event.mid_repricing_delay_ms is not None
        ]
        executable_delays = [
            event.executable_repricing_delay_ms
            for event in self._completed
            if event.executable_repricing_delay_ms is not None
        ]
        tradable_windows = [
            event.tradable_window_ms
            for event in self._completed
            if event.tradable_window_ms is not None
        ]
        executable_edges = [
            event.estimated_edge_after_spread
            for event in self._completed
            if event.reject_stage == "none" and event.estimated_edge_after_spread is not None
        ]
        return GapMonitorStats(
            detected_gaps=self.detected_gaps,
            completed_gaps=len(self._completed),
            fillable_at_detection_count=self.fillable_at_detection_count,
            non_fillable_at_detection_count=self.non_fillable_at_detection_count,
            median_mid_repricing_delay_ms=median(mid_delays) if mid_delays else None,
            p95_mid_repricing_delay_ms=_percentile(mid_delays, 0.95) if mid_delays else None,
            median_executable_repricing_delay_ms=(
                median(executable_delays) if executable_delays else None
            ),
            p95_executable_repricing_delay_ms=(
                _percentile(executable_delays, 0.95) if executable_delays else None
            ),
            median_tradable_window_ms=median(tradable_windows) if tradable_windows else None,
            p95_tradable_window_ms=(
                _percentile(tradable_windows, 0.95) if tradable_windows else None
            ),
            average_estimated_edge=(
                sum(executable_edges) / len(executable_edges) if executable_edges else None
            ),
            reject_count_by_reason=dict(self._reject_count_by_reason),
            reject_count_by_stage=dict(self._reject_count_by_stage),
            stale_feed_count=self.stale_feed_count(state, now_ts=current_ts),
        )

    def stale_feed_count(self, state: MarketState, *, now_ts: int | None = None) -> int:
        current_ts = now_ts or utc_now_ns()
        stale = 0

        for symbol in self._markets_by_symbol:
            symbol_state = state.symbols.get(symbol)
            if symbol_state is None or _is_stale_wall(
                symbol_state.local_receive_timestamp or symbol_state.last_event_timestamp,
                now_ts=current_ts,
                stale_ms=self.measurement_stale_ms,
            ):
                stale += 1

        for market in self.markets:
            for token_id in market.token_ids:
                quote = state.polymarket_quotes.get(token_id)
                if quote is None or _is_stale_quote(
                    quote,
                    now_ts=current_ts,
                    now_monotonic_ns=None,
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
        now_monotonic_ns: int,
        binance_event_ts: int | None,
    ) -> None:
        markets = self._markets_by_symbol.get(symbol, ())
        if not markets:
            return

        symbol_state = state.symbols.get(symbol)
        if symbol_state is None or _is_stale_wall(
            symbol_state.local_receive_timestamp or symbol_state.last_event_timestamp,
            now_ts=now_ts,
            stale_ms=self.binance_stale_ms,
        ):
            self._record_reject("binance_stale", "pre_entry")
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
            self.detected_gaps += 1

            if market.market_id in self._invalid_markets or state.is_market_invalid(
                market.market_id
            ):
                self._reject_pre_entry("market_invalidated")
                continue

            token_id = market.token_for_direction(direction)
            if token_id is None:
                self._reject_pre_entry("direction_token_unmapped")
                continue

            pending_key = (symbol, market.market_id, direction)
            if pending_key in self._pending:
                continue

            quote = state.polymarket_quotes.get(token_id)
            if quote is None:
                self._reject_pre_entry("missing_quote")
                continue
            if _is_stale_quote(
                quote,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
                stale_ms=self.polymarket_stale_ms,
            ):
                self._reject_pre_entry("quote_stale")
                continue

            fillable, reject_reason = self._fillable_before_repricing(market, quote)
            if not fillable:
                self._reject_pre_entry(reject_reason or "quote_not_fillable")
                continue
            assert quote.best_ask is not None
            assert quote.best_ask_size is not None

            quote_ts = _quote_timestamp(quote)
            quote_mono = _quote_monotonic(quote) or now_monotonic_ns
            self._pending[pending_key] = PendingTradableGap(
                symbol=symbol,
                market=market,
                direction=direction,
                token_id=token_id,
                binance_move_pct=move_pct,
                first_detected_ts_ns=now_ts,
                first_detected_monotonic_ns=now_monotonic_ns,
                binance_event_ts_ns=binance_event_ts,
                poly_quote_ts_ns=quote_ts,
                before_best_bid=quote.best_bid,
                before_best_ask=quote.best_ask,
                before_best_bid_size=quote.best_bid_size,
                before_best_ask_size=quote.best_ask_size,
                before_mid=quote.mid_price,
                spread_before=quote.spread,
                entry_ask=quote.best_ask,
                entry_ask_size=quote.best_ask_size,
                last_tradable_ts_ns=now_ts,
                last_tradable_monotonic_ns=now_monotonic_ns,
                current_best_bid=quote.best_bid,
                current_best_ask=quote.best_ask,
                current_best_bid_size=quote.best_bid_size,
                current_best_ask_size=quote.best_ask_size,
                current_mid=quote.mid_price,
                current_spread=quote.spread,
                current_quote_ts_ns=quote_ts,
                current_quote_monotonic_ns=quote_mono,
            )
            self.fillable_at_detection_count += 1

    def _handle_polymarket_quote(
        self,
        quote: PolymarketQuote,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> tuple[TradableGapObservation, ...]:
        closed: list[TradableGapObservation] = []
        for key, pending in list(self._pending.items()):
            if pending.token_id != quote.token_id:
                continue

            quote_ts = _quote_timestamp(quote) or now_ts
            quote_mono = _quote_monotonic(quote) or now_monotonic_ns
            self._copy_current_quote(pending, quote, quote_ts=quote_ts, quote_mono=quote_mono)
            self._mark_mid_repricing(pending, quote, quote_ts=quote_ts, quote_mono=quote_mono)

            timeout_reason = self._timeout_reason(pending, now_monotonic_ns=quote_mono)
            if timeout_reason is not None:
                pending.close_reason = timeout_reason
                pending.reject_stage = "timeout"
                observation = self._close_pending(key, pending)
                closed.append(observation)
                continue

            structural_end_reason = self._structural_window_end_reason(
                pending,
                quote,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
            )
            if structural_end_reason is not None:
                pending.first_non_tradable_ts_ns = pending.first_non_tradable_ts_ns or quote_ts
                pending.first_non_tradable_monotonic_ns = (
                    pending.first_non_tradable_monotonic_ns or quote_mono
                )
                pending.window_end_reason = structural_end_reason
                pending.close_reason = structural_end_reason
                pending.reject_stage = "window"
                observation = self._close_pending(key, pending)
                closed.append(observation)
                continue

            if self._has_executable_repriced(pending, quote):
                pending.first_executable_repriced_ts_ns = quote_ts
                pending.first_executable_repriced_monotonic_ns = quote_mono
                pending.executable_exit_bid = quote.best_bid
                pending.reject_stage = "none"
                observation = self._close_pending(key, pending)
                closed.append(observation)
                continue

            entry_window_end_reason = self._entry_window_end_reason(pending, quote)
            if entry_window_end_reason is not None:
                pending.first_non_tradable_ts_ns = pending.first_non_tradable_ts_ns or quote_ts
                pending.first_non_tradable_monotonic_ns = (
                    pending.first_non_tradable_monotonic_ns or quote_mono
                )
                pending.window_end_reason = entry_window_end_reason
                pending.close_reason = entry_window_end_reason
                pending.reject_stage = "window"
                observation = self._close_pending(key, pending)
                closed.append(observation)
                continue

            pending.last_tradable_ts_ns = quote_ts
            pending.last_tradable_monotonic_ns = quote_mono

        return tuple(closed)

    def _handle_lifecycle(
        self,
        event: MarketLifecycleEvent,
        state: MarketState,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> tuple[TradableGapObservation, ...]:
        if event.lifecycle_type == "new_market":
            return ()

        self._invalid_markets.add(event.market_id)
        close_ts = event.received_ts or event.local_received_ts or now_ts
        close_mono = event.recv_monotonic_ns or now_monotonic_ns
        closed: list[TradableGapObservation] = []
        for key, pending in list(self._pending.items()):
            if pending.market.market_id != event.market_id:
                continue
            quote = state.polymarket_quotes.get(pending.token_id)
            if quote is not None:
                self._copy_current_quote(
                    pending,
                    quote,
                    quote_ts=_quote_timestamp(quote) or close_ts,
                    quote_mono=_quote_monotonic(quote) or close_mono,
                )
            pending.first_non_tradable_ts_ns = pending.first_non_tradable_ts_ns or close_ts
            pending.first_non_tradable_monotonic_ns = (
                pending.first_non_tradable_monotonic_ns or close_mono
            )
            pending.close_reason = event.lifecycle_type
            pending.reject_stage = "lifecycle"
            closed.append(self._close_pending(key, pending))
        return tuple(closed)

    def _close_expired_pending(
        self,
        state: MarketState,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> tuple[TradableGapObservation, ...]:
        closed: list[TradableGapObservation] = []
        for key, pending in list(self._pending.items()):
            timeout_reason = self._timeout_reason(pending, now_monotonic_ns=now_monotonic_ns)
            if timeout_reason is None:
                continue
            quote = state.polymarket_quotes.get(pending.token_id)
            if quote is not None:
                self._copy_current_quote(
                    pending,
                    quote,
                    quote_ts=_quote_timestamp(quote) or now_ts,
                    quote_mono=_quote_monotonic(quote) or now_monotonic_ns,
                )
            pending.close_reason = timeout_reason
            pending.reject_stage = "timeout"
            closed.append(self._close_pending(key, pending))
        return tuple(closed)

    def _close_pending(
        self,
        key: tuple[str, str, GapDirection],
        pending: PendingTradableGap,
    ) -> TradableGapObservation:
        observation = self._observation_from_pending(pending)
        if observation.reject_reason is not None:
            self._record_reject(observation.reject_reason, observation.reject_stage)
        self._completed.append(observation)
        del self._pending[key]
        return observation

    def _copy_current_quote(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
        *,
        quote_ts: int,
        quote_mono: int,
    ) -> None:
        pending.current_best_bid = quote.best_bid
        pending.current_best_ask = quote.best_ask
        pending.current_best_bid_size = quote.best_bid_size
        pending.current_best_ask_size = quote.best_ask_size
        pending.current_mid = quote.mid_price
        pending.current_spread = quote.spread
        pending.current_quote_ts_ns = quote_ts
        pending.current_quote_monotonic_ns = quote_mono

    def _mark_mid_repricing(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
        *,
        quote_ts: int,
        quote_mono: int,
    ) -> None:
        if pending.first_mid_repriced_ts_ns is not None:
            return
        if pending.before_mid is None or quote.mid_price is None:
            return
        threshold = max(self.reprice_threshold, pending.market.tick_size)
        if quote.mid_price >= pending.before_mid + threshold:
            pending.first_mid_repriced_ts_ns = quote_ts
            pending.first_mid_repriced_monotonic_ns = quote_mono

    def _has_executable_repriced(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
    ) -> bool:
        return (
            quote.best_bid is not None
            and quote.best_bid > pending.entry_ask + self.min_exit_edge
        )

    def _structural_window_end_reason(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> str | None:
        if quote.book_stale or _is_stale_quote(
            quote,
            now_ts=now_ts,
            now_monotonic_ns=now_monotonic_ns,
            stale_ms=self.polymarket_stale_ms,
        ):
            return "quote_stale"
        if pending.market.market_id in self._invalid_markets:
            return "market_invalidated"
        if not quote.book_complete:
            return "book_incomplete"
        return None

    def _entry_window_end_reason(
        self,
        pending: PendingTradableGap,
        quote: PolymarketQuote,
    ) -> str | None:
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
        if quote.best_ask > pending.entry_ask + self.max_entry_price_move:
            return "entry_price_moved"
        return None

    def _timeout_reason(
        self,
        pending: PendingTradableGap,
        *,
        now_monotonic_ns: int,
    ) -> str | None:
        elapsed_ms = _duration_ms(pending.first_detected_monotonic_ns, now_monotonic_ns)
        if elapsed_ms is not None and elapsed_ms > self.max_pending_gap_ms:
            return "max_observation_lifetime_reached"
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
    ) -> TradableGapObservation:
        mid_delay_ms = _duration_ms(
            pending.first_detected_monotonic_ns,
            pending.first_mid_repriced_monotonic_ns,
        )
        executable_delay_ms = _duration_ms(
            pending.first_detected_monotonic_ns,
            pending.first_executable_repriced_monotonic_ns,
        )
        exit_edge = _diff(pending.executable_exit_bid, pending.entry_ask)
        raw_edge = _diff(pending.current_mid, pending.before_mid)
        reject_stage = pending.reject_stage
        exit_reject_reason = self._exit_reject_reason(pending)
        reject_reason = self._final_reject_reason(
            pending,
            exit_reject_reason=exit_reject_reason,
        )

        return TradableGapObservation(
            symbol=pending.symbol,
            market_id=pending.market.market_id,
            token_id=pending.token_id,
            direction=pending.direction,
            binance_move_pct=pending.binance_move_pct,
            detected_ts_ns=pending.first_detected_ts_ns,
            binance_event_ts_ns=pending.binance_event_ts_ns,
            poly_quote_ts_ns=pending.current_quote_ts_ns or pending.poly_quote_ts_ns,
            before_best_bid=pending.before_best_bid,
            before_best_ask=pending.before_best_ask,
            before_best_bid_size=pending.before_best_bid_size,
            before_best_ask_size=pending.before_best_ask_size,
            before_mid=pending.before_mid,
            after_best_bid=pending.current_best_bid,
            after_best_ask=pending.current_best_ask,
            after_mid=pending.current_mid,
            spread_before=pending.spread_before,
            spread_after=pending.current_spread,
            mid_repricing_delay_ms=mid_delay_ms,
            executable_repricing_delay_ms=executable_delay_ms,
            first_mid_repriced_ts_ns=pending.first_mid_repriced_ts_ns,
            first_executable_repriced_ts_ns=pending.first_executable_repriced_ts_ns,
            executable_exit_bid=pending.executable_exit_bid,
            entry_ask=pending.entry_ask,
            entry_ask_size=pending.entry_ask_size,
            exit_edge_after_spread=exit_edge,
            repricing_delay_ms=executable_delay_ms,
            tradable_window_ms=self._tradable_window_ms(pending),
            hypothetical_entry_price=pending.entry_ask,
            hypothetical_exit_price=pending.executable_exit_bid or pending.current_best_bid,
            quote_was_fillable=True,
            estimated_edge_raw=raw_edge,
            estimated_edge_after_spread=exit_edge if reject_stage == "none" else None,
            pre_entry_reject_reason=None,
            window_end_reason=pending.window_end_reason,
            exit_reject_reason=exit_reject_reason,
            reject_stage=reject_stage,
            reject_reason=reject_reason,
        )

    def _tradable_window_ms(self, pending: PendingTradableGap) -> float:
        end_mono = (
            pending.first_non_tradable_monotonic_ns
            or pending.first_executable_repriced_monotonic_ns
            or pending.current_quote_monotonic_ns
            or pending.last_tradable_monotonic_ns
        )
        return _duration_ms(pending.first_detected_monotonic_ns, end_mono) or 0.0

    def _exit_reject_reason(self, pending: PendingTradableGap) -> str | None:
        if pending.reject_stage in {"none", "lifecycle", "timeout", "window"}:
            if pending.reject_stage == "none":
                return None
            if pending.current_best_bid is not None and pending.current_best_bid <= pending.entry_ask:
                return "edge_not_positive_after_spread"
            return None
        return None

    def _final_reject_reason(
        self,
        pending: PendingTradableGap,
        *,
        exit_reject_reason: str | None,
    ) -> str | None:
        if pending.reject_stage == "none":
            return None
        if pending.reject_stage == "window":
            return pending.window_end_reason
        if pending.reject_stage == "lifecycle":
            return pending.close_reason
        if pending.reject_stage == "timeout":
            return pending.close_reason
        if pending.reject_stage == "exit":
            return exit_reject_reason
        return pending.close_reason

    def _reject_pre_entry(self, reason: str) -> None:
        self.non_fillable_at_detection_count += 1
        self._record_reject(reason, "pre_entry")

    def _record_reject(self, reason: str, stage: RejectStage) -> None:
        self._reject_count_by_reason[reason] = self._reject_count_by_reason.get(reason, 0) + 1
        self._reject_count_by_stage[stage] = self._reject_count_by_stage.get(stage, 0) + 1


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


def _event_monotonic(
    event: MarketTick | OrderBookTop | DepthUpdate | PolymarketQuote | MarketLifecycleEvent,
) -> int | None:
    if isinstance(event, PolymarketQuote | MarketLifecycleEvent):
        return event.recv_monotonic_ns or event.received_ts or event.local_received_ts or event.ts_ns
    return event.recv_monotonic_ns or event.local_received_ts or event.ts_ns


def _is_stale_quote(
    quote: PolymarketQuote,
    *,
    now_ts: int,
    now_monotonic_ns: int | None,
    stale_ms: float,
) -> bool:
    if quote.recv_monotonic_ns is not None and now_monotonic_ns is not None:
        return _is_stale_monotonic(
            quote.recv_monotonic_ns,
            now_monotonic_ns=now_monotonic_ns,
            stale_ms=stale_ms,
        )
    return _is_stale_wall(_quote_timestamp(quote), now_ts=now_ts, stale_ms=stale_ms)


def _quote_timestamp(quote: PolymarketQuote) -> int | None:
    return quote.received_ts or quote.local_received_ts or quote.event_ts or quote.exchange_event_ts


def _quote_monotonic(quote: PolymarketQuote) -> int | None:
    return quote.recv_monotonic_ns


def _is_stale_wall(timestamp: int | None, *, now_ts: int, stale_ms: float) -> bool:
    if timestamp is None:
        return True
    return (now_ts - timestamp) / 1_000_000.0 > stale_ms


def _is_stale_monotonic(
    timestamp: int | None,
    *,
    now_monotonic_ns: int,
    stale_ms: float,
) -> bool:
    if timestamp is None:
        return True
    return (now_monotonic_ns - timestamp) / 1_000_000.0 > stale_ms


def _duration_ms(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    return max(0.0, (end_ns - start_ns) / 1_000_000.0)


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
