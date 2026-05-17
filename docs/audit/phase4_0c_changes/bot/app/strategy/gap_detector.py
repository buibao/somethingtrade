from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from statistics import median
from typing import Literal

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import (
    DataQualityTier,
    DepthUpdate,
    GapDirection,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    RejectStage,
    StaleSource,
    TradableGapObservation,
    ValidationMode,
)
from app.core.tick_math import diff_to_ticks, price_to_ticks
from app.marketdata.polymarket_discovery import (
    PolymarketMarketMetadata,
    classify_market_window,
    is_runtime_tradable_market,
)
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

    @property
    def strongest_return_key(self) -> str | None:
        keyed = [
            (key, value)
            for key, value in (
                ("1s", self.return_1s),
                ("5s", self.return_5s),
                ("15s", self.return_15s),
                ("30s", self.return_30s),
            )
            if value is not None
        ]
        if not keyed:
            return None
        return max(keyed, key=lambda item: abs(item[1]))[0]


@dataclass(frozen=True, slots=True)
class BookReadinessGate:
    suppress: bool
    warmup_ms: float | None
    warmup_timeout: bool
    book_complete: bool | None
    book_has_snapshot: bool | None
    book_structurally_complete: bool | None
    reported_best_validation_ok: bool | None
    validation_error: str | None
    market_classification: str | None
    signal_enabled: bool
    market_mismatch_rate: float | None = None
    token_mismatch_rate: float | None = None
    market_quote_complete_rate: float | None = None
    token_quote_complete_rate: float | None = None


@dataclass(frozen=True, slots=True)
class StaleDiagnostics:
    stale_source: StaleSource | None = None
    binance_quote_age_ms: float | None = None
    polymarket_quote_age_ms: float | None = None
    now_monotonic_ns: int | None = None
    last_binance_update_monotonic_ns: int | None = None
    last_polymarket_update_monotonic_ns: int | None = None
    binance_local_received_ts_ns: int | None = None
    polymarket_event_ts_ns: int | None = None
    polymarket_local_received_ts_ns: int | None = None
    state_updated_monotonic_ns: int | None = None
    detector_processed_monotonic_ns: int | None = None


@dataclass(frozen=True, slots=True)
class MoveEpisode:
    move_episode_id: str
    source_move_window_ms: float
    source_move_start_ts_ns: int
    source_move_end_ts_ns: int
    window_bucket: int


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
    tick_size_at_detection: float | None
    spread_at_detection: float | None
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
    market_classification_at_detection: str | None = None
    signal_enabled_at_detection: bool | None = None
    book_complete_at_detection: bool | None = None
    book_has_snapshot_at_detection: bool | None = None
    book_structurally_complete_at_detection: bool | None = None
    reported_best_validation_ok_at_detection: bool | None = None
    book_validation_error_at_detection: str | None = None
    book_warmup_ms_at_detection: float | None = None
    book_warmup_timeout: bool = False
    validation_mode: ValidationMode | None = None
    validation_tolerance_ticks: int | None = None
    market_mismatch_rate_at_detection: float | None = None
    token_mismatch_rate_at_detection: float | None = None
    market_quote_complete_rate_at_detection: float | None = None
    token_quote_complete_rate_at_detection: float | None = None
    stale_diagnostics: StaleDiagnostics = field(default_factory=StaleDiagnostics)
    pre_entry_reject_reason: str | None = None
    window_end_reason: str | None = None
    exit_reject_reason: str | None = None
    lifecycle_reason: str | None = None
    timeout_reason: str | None = None
    close_reason: str | None = None
    reject_stage: RejectStage = "none"
    move_episode_id: str | None = None
    source_move_window_ms: float | None = None
    source_move_start_ts_ns: int | None = None
    source_move_end_ts_ns: int | None = None


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
    pre_entry_observations_written: int = 0
    pre_entry_observations_suppressed: int = 0
    warmup_quotes_received: int = 0
    signal_enabled_markets: int = 0
    warmup_only_markets: int = 0
    book_warmup_suppressed: int = 0
    binance_moves_detected_by_symbol: dict[str, int] = field(default_factory=dict)
    candidates_created_by_symbol: dict[str, int] = field(default_factory=dict)
    pre_entry_rejects_by_symbol: dict[str, int] = field(default_factory=dict)
    window_rejects_by_symbol: dict[str, int] = field(default_factory=dict)
    timeout_rejects_by_symbol: dict[str, int] = field(default_factory=dict)
    suppressed_candidates_by_symbol: dict[str, int] = field(default_factory=dict)
    non_fillable_by_symbol: dict[str, int] = field(default_factory=dict)
    top_reject_reasons_by_symbol: dict[str, dict[str, int]] = field(default_factory=dict)
    pending_observation_count: int = 0
    pending_max_age_ms: float | None = None
    max_pending_gap_ms: float = 0.0
    candidate_duplicate_suppressed_count: int = 0
    candidates_per_symbol_direction_per_minute: dict[str, int] = field(default_factory=dict)
    candidates_per_market_window: dict[str, int] = field(default_factory=dict)

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
        pre_entry_log_cooldown_ms: float = 500.0,
        require_book_ready: bool = True,
        book_warmup_max_ms: float = 3_000.0,
        validation_mode: ValidationMode = "tolerant",
        validation_tolerance_ticks: int = 1,
        close_pending_on_tick_size_change: bool = False,
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
        self.pre_entry_log_cooldown_ms = pre_entry_log_cooldown_ms
        self.require_book_ready = require_book_ready
        self.book_warmup_max_ms = book_warmup_max_ms
        self.validation_mode: ValidationMode = validation_mode
        self.validation_tolerance_ticks = validation_tolerance_ticks
        self.close_pending_on_tick_size_change = close_pending_on_tick_size_change
        self._detector_started_ns = utc_now_ns()
        self._markets_by_symbol = _markets_by_symbol(markets)
        self._markets_by_token = _markets_by_token(markets)
        self._tick_size_by_market = {
            market.market_id: market.tick_size
            for market in markets
            if market.tick_size > 0.0
        }
        self._pending: dict[tuple[str, str, GapDirection], PendingTradableGap] = {}
        self._completed: list[TradableGapObservation] = []
        self._invalid_markets: set[str] = set()
        self._last_pre_entry_log_ns: dict[tuple[str, str, GapDirection, str], int] = {}
        self._reject_count_by_reason: dict[str, int] = {}
        self._reject_count_by_stage: dict[str, int] = {}
        self._binance_moves_detected_by_symbol: dict[str, int] = {}
        self._candidates_created_by_symbol: dict[str, int] = {}
        self._pre_entry_rejects_by_symbol: dict[str, int] = {}
        self._window_rejects_by_symbol: dict[str, int] = {}
        self._timeout_rejects_by_symbol: dict[str, int] = {}
        self._suppressed_candidates_by_symbol: dict[str, int] = {}
        self._non_fillable_by_symbol: dict[str, int] = {}
        self._reject_reasons_by_symbol: dict[str, dict[str, int]] = {}
        self._market_update_observations: list[TradableGapObservation] = []
        self._candidate_duplicate_suppressed_count = 0
        self._candidates_per_symbol_direction_per_minute: dict[str, int] = {}
        self._candidates_per_market_window: dict[str, int] = {}
        self.detected_gaps = 0
        self.fillable_at_detection_count = 0
        self.non_fillable_at_detection_count = 0
        self.pre_entry_observations_written = 0
        self.pre_entry_observations_suppressed = 0
        self.warmup_quotes_received = 0
        self.book_warmup_suppressed = 0

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
            closed.extend(
                self._detect_binance_move(
                    event.symbol,
                    state,
                    now_ts=current_ts,
                    now_monotonic_ns=current_mono,
                    binance_event_ts=event.exchange_event_ts or event.local_received_ts,
                )
            )
        elif isinstance(event, OrderBookTop) and event.source == "binance":
            closed.extend(
                self._detect_binance_move(
                    event.symbol,
                    state,
                    now_ts=current_ts,
                    now_monotonic_ns=current_mono,
                    binance_event_ts=event.exchange_event_ts or event.local_received_ts,
                )
            )
        elif isinstance(event, PolymarketQuote):
            self._record_warmup_quote(event, now_ts=current_ts)
            closed.extend(
                self._handle_polymarket_quote(
                    event,
                    state,
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
        reject_count_by_reason = _count_strings(
            event.reject_reason
            for event in self._completed
            if event.reject_reason is not None
        )
        reject_count_by_stage = _count_strings(
            event.reject_stage
            for event in self._completed
            if event.reject_stage != "none"
        )
        signal_enabled_markets = sum(
            1 for market in self.markets if self._market_signal_enabled(market, current_ts)
        )
        warmup_only_markets = sum(
            1
            for market in self.markets
            if market.selected_for_runtime
            and not self._market_signal_enabled(market, current_ts)
            and classify_market_window(
                market,
                now_ts=current_ts // 1_000_000_000,
            )
            == "next"
        )
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
            reject_count_by_reason=reject_count_by_reason,
            reject_count_by_stage=reject_count_by_stage,
            stale_feed_count=self.stale_feed_count(state, now_ts=current_ts),
            pre_entry_observations_written=self.pre_entry_observations_written,
            pre_entry_observations_suppressed=self.pre_entry_observations_suppressed,
            warmup_quotes_received=self.warmup_quotes_received,
            signal_enabled_markets=signal_enabled_markets,
            warmup_only_markets=warmup_only_markets,
            book_warmup_suppressed=self.book_warmup_suppressed,
            binance_moves_detected_by_symbol=dict(self._binance_moves_detected_by_symbol),
            candidates_created_by_symbol=dict(self._candidates_created_by_symbol),
            pre_entry_rejects_by_symbol=dict(self._pre_entry_rejects_by_symbol),
            window_rejects_by_symbol=dict(self._window_rejects_by_symbol),
            timeout_rejects_by_symbol=dict(self._timeout_rejects_by_symbol),
            suppressed_candidates_by_symbol=dict(self._suppressed_candidates_by_symbol),
            non_fillable_by_symbol=dict(self._non_fillable_by_symbol),
            top_reject_reasons_by_symbol={
                symbol: dict(
                    sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:5]
                )
                for symbol, reasons in sorted(self._reject_reasons_by_symbol.items())
            },
            pending_observation_count=len(self._pending),
            pending_max_age_ms=self.pending_max_age_ms(now_monotonic_ns=monotonic_now_ns()),
            max_pending_gap_ms=self.max_pending_gap_ms,
            candidate_duplicate_suppressed_count=self._candidate_duplicate_suppressed_count,
            candidates_per_symbol_direction_per_minute=dict(
                self._candidates_per_symbol_direction_per_minute
            ),
            candidates_per_market_window=dict(self._candidates_per_market_window),
        )

    def update_markets(self, markets: tuple[PolymarketMarketMetadata, ...]) -> None:
        """Replace runtime markets while preserving still-active pending observations."""

        now_ts = utc_now_ns()
        now_mono = monotonic_now_ns()
        active_by_id = {market.market_id: market for market in markets}
        for key, pending in list(self._pending.items()):
            replacement = active_by_id.get(pending.market.market_id)
            if replacement is not None:
                pending.market = replacement
                continue
            pending.first_non_tradable_ts_ns = pending.first_non_tradable_ts_ns or now_ts
            pending.first_non_tradable_monotonic_ns = (
                pending.first_non_tradable_monotonic_ns or now_mono
            )
            pending.window_end_reason = "market_expired"
            pending.lifecycle_reason = "market_expired"
            pending.close_reason = "market_expired"
            pending.reject_stage = "lifecycle"
            pending.stale_diagnostics = StaleDiagnostics(
                stale_source="unknown",
                now_monotonic_ns=now_mono,
                detector_processed_monotonic_ns=now_mono,
            )
            self._market_update_observations.append(self._close_pending(key, pending))

        self.markets = markets
        self._markets_by_symbol = _markets_by_symbol(markets)
        self._markets_by_token = _markets_by_token(markets)
        self._tick_size_by_market = {
            market.market_id: market.tick_size
            for market in markets
            if market.tick_size > 0.0
        }
        active_ids = set(active_by_id)
        self._invalid_markets = {market_id for market_id in self._invalid_markets if market_id in active_ids}

    def drain_market_update_observations(self) -> tuple[TradableGapObservation, ...]:
        observations = tuple(self._market_update_observations)
        self._market_update_observations.clear()
        return observations

    def pending_max_age_ms(self, *, now_monotonic_ns: int) -> float | None:
        ages = [
            _duration_ms(pending.first_detected_monotonic_ns, now_monotonic_ns)
            for pending in self._pending.values()
        ]
        known = [age for age in ages if age is not None]
        return max(known) if known else None

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
    ) -> tuple[TradableGapObservation, ...]:
        observations: list[TradableGapObservation] = []
        symbol_state = state.symbols.get(symbol)
        if symbol_state is None or _is_stale_wall(
            symbol_state.local_receive_timestamp or symbol_state.last_event_timestamp,
            now_ts=now_ts,
            stale_ms=self.binance_stale_ms,
        ):
            self._record_reject("binance_stale", "pre_entry")
            return ()

        snapshot = build_move_snapshot(symbol_state)
        strongest_return = snapshot.strongest_return
        if strongest_return is None:
            return ()

        move_pct = strongest_return * 100.0
        if abs(move_pct) < self.min_move_pct:
            return ()

        self._increment(self._binance_moves_detected_by_symbol, symbol)
        markets = self._markets_by_symbol.get(symbol, ())
        if not markets:
            return ()

        direction: GapDirection = "UP" if move_pct > 0.0 else "DOWN"
        episode = _move_episode(
            symbol=symbol,
            direction=direction,
            window_key=snapshot.strongest_return_key,
            event_ts_ns=binance_event_ts or now_ts,
        )
        for market in markets:
            if not self._market_signal_enabled(market, now_ts):
                continue

            book_gate = self._book_readiness_gate(market, state, now_ts=now_ts)
            if book_gate.suppress:
                self.book_warmup_suppressed += 1
                continue

            self.detected_gaps += 1
            self._increment(self._candidates_created_by_symbol, symbol)
            token_id = market.token_for_direction(direction)
            self._increment(
                self._candidates_per_market_window,
                f"{market.market_id}:{episode.source_move_window_ms:g}:{episode.window_bucket}",
            )

            if market.market_id in self._invalid_markets or state.is_market_invalid(
                market.market_id
            ):
                self._append_pre_entry_observation(
                    observations,
                    self._pre_entry_observation(
                        symbol=symbol,
                        market=market,
                        direction=direction,
                        token_id=token_id or "",
                        move_pct=move_pct,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                        state=state,
                        binance_event_ts=binance_event_ts,
                        quote=None,
                        reason="market_invalidated",
                        book_gate=book_gate,
                        episode=episode,
                    )
                )
                continue

            if token_id is None:
                self._append_pre_entry_observation(
                    observations,
                    self._pre_entry_observation(
                        symbol=symbol,
                        market=market,
                        direction=direction,
                        token_id="",
                        move_pct=move_pct,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                        state=state,
                        binance_event_ts=binance_event_ts,
                        quote=None,
                        reason="direction_token_unmapped",
                        book_gate=book_gate,
                        episode=episode,
                    )
                )
                continue

            pending_key = (symbol, market.market_id, direction)
            if pending_key in self._pending:
                self._candidate_duplicate_suppressed_count += 1
                self._increment(self._suppressed_candidates_by_symbol, symbol)
                continue

            quote = state.polymarket_quotes.get(token_id)
            if quote is None:
                self._append_pre_entry_observation(
                    observations,
                    self._pre_entry_observation(
                        symbol=symbol,
                        market=market,
                        direction=direction,
                        token_id=token_id,
                        move_pct=move_pct,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                        state=state,
                        binance_event_ts=binance_event_ts,
                        quote=None,
                        reason="missing_quote",
                        book_gate=book_gate,
                        episode=episode,
                    )
                )
                continue
            if _is_stale_quote(
                quote,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
                stale_ms=self.polymarket_stale_ms,
            ):
                stale_diagnostics = self._stale_diagnostics(
                    symbol,
                    quote,
                    state,
                    now_ts=now_ts,
                    now_monotonic_ns=now_monotonic_ns,
                )
                self._append_pre_entry_observation(
                    observations,
                    self._pre_entry_observation(
                        symbol=symbol,
                        market=market,
                        direction=direction,
                        token_id=token_id,
                        move_pct=move_pct,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                        state=state,
                        binance_event_ts=binance_event_ts,
                        quote=quote,
                        reason="quote_stale",
                        book_gate=book_gate,
                        stale_diagnostics=stale_diagnostics,
                        episode=episode,
                    )
                )
                continue

            fillable, reject_reason = self._fillable_before_repricing(market, quote)
            if not fillable:
                self._append_pre_entry_observation(
                    observations,
                    self._pre_entry_observation(
                        symbol=symbol,
                        market=market,
                        direction=direction,
                        token_id=token_id,
                        move_pct=move_pct,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                        state=state,
                        binance_event_ts=binance_event_ts,
                        quote=quote,
                        reason=reject_reason or "quote_not_fillable",
                        book_gate=book_gate,
                        episode=episode,
                    )
                )
                continue
            assert quote.best_ask is not None
            assert quote.best_ask_size is not None

            quote_ts = _quote_timestamp(quote)
            quote_mono = _quote_monotonic(quote) or now_monotonic_ns
            stale_diagnostics = self._stale_diagnostics(
                symbol,
                quote,
                state,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
            )
            tick_size_at_detection = self._tick_size_for_market(market)
            spread_at_detection = _spread(quote.best_bid, quote.best_ask)
            self._increment(
                self._candidates_per_symbol_direction_per_minute,
                f"{symbol}:{direction}:{now_ts // 60_000_000_000}",
            )
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
                tick_size_at_detection=tick_size_at_detection,
                spread_at_detection=spread_at_detection,
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
                market_classification_at_detection=book_gate.market_classification,
                signal_enabled_at_detection=book_gate.signal_enabled,
                book_complete_at_detection=quote.book_complete,
                book_has_snapshot_at_detection=quote.book_has_snapshot,
                book_structurally_complete_at_detection=quote.book_structurally_complete,
                reported_best_validation_ok_at_detection=quote.reported_best_validation_ok,
                book_validation_error_at_detection=quote.validation_error,
                book_warmup_ms_at_detection=book_gate.warmup_ms,
                book_warmup_timeout=book_gate.warmup_timeout,
                validation_mode=self.validation_mode,
                validation_tolerance_ticks=self.validation_tolerance_ticks,
                market_mismatch_rate_at_detection=(
                    quote.market_mismatch_rate or book_gate.market_mismatch_rate
                ),
                token_mismatch_rate_at_detection=(
                    quote.token_mismatch_rate or book_gate.token_mismatch_rate
                ),
                market_quote_complete_rate_at_detection=(
                    quote.market_quote_complete_rate
                    or book_gate.market_quote_complete_rate
                ),
                token_quote_complete_rate_at_detection=(
                    quote.token_quote_complete_rate
                    or book_gate.token_quote_complete_rate
                ),
                stale_diagnostics=stale_diagnostics,
                move_episode_id=episode.move_episode_id,
                source_move_window_ms=episode.source_move_window_ms,
                source_move_start_ts_ns=episode.source_move_start_ts_ns,
                source_move_end_ts_ns=episode.source_move_end_ts_ns,
            )
            self.fillable_at_detection_count += 1

        return tuple(observations)

    def _handle_polymarket_quote(
        self,
        quote: PolymarketQuote,
        state: MarketState,
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
            pending.stale_diagnostics = self._stale_diagnostics(
                pending.symbol,
                quote,
                state,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
            )
            self._mark_mid_repricing(pending, quote, quote_ts=quote_ts, quote_mono=quote_mono)

            timeout_reason = self._timeout_reason(pending, now_monotonic_ns=quote_mono)
            if timeout_reason is not None:
                pending.timeout_reason = timeout_reason
                pending.close_reason = timeout_reason
                pending.reject_stage = "timeout"
                observation = self._close_pending(key, pending)
                closed.append(observation)
                continue

            structural_end_reason = self._structural_window_end_reason(
                pending,
                quote,
                state,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
            )
            if structural_end_reason is not None:
                if structural_end_reason == "quote_stale":
                    pending.stale_diagnostics = self._stale_diagnostics(
                        pending.symbol,
                        quote,
                        state,
                        now_ts=now_ts,
                        now_monotonic_ns=now_monotonic_ns,
                    )
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

        if event.lifecycle_type == "tick_size_change" and event.new_tick_size:
            self._tick_size_by_market[event.market_id] = event.new_tick_size
            if not self.close_pending_on_tick_size_change:
                return ()
        else:
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
                pending.stale_diagnostics = self._stale_diagnostics(
                    pending.symbol,
                    quote,
                    state,
                    now_ts=now_ts,
                    now_monotonic_ns=now_monotonic_ns,
                )
            pending.first_non_tradable_ts_ns = pending.first_non_tradable_ts_ns or close_ts
            pending.first_non_tradable_monotonic_ns = (
                pending.first_non_tradable_monotonic_ns or close_mono
            )
            pending.window_end_reason = event.lifecycle_type
            pending.lifecycle_reason = event.lifecycle_type
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
            pending.stale_diagnostics = self._stale_diagnostics(
                pending.symbol,
                quote,
                state,
                now_ts=now_ts,
                now_monotonic_ns=now_monotonic_ns,
            )
            pending.timeout_reason = timeout_reason
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
        self._record_observation_diagnostics(observation)
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
        threshold = self._effective_reprice_threshold(pending.market)
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
        state: MarketState,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> str | None:
        stale_diagnostics = self._stale_diagnostics(
            pending.symbol,
            quote,
            state,
            now_ts=now_ts,
            now_monotonic_ns=now_monotonic_ns,
        )
        if quote.book_stale or _is_stale_quote(
            quote,
            now_ts=now_ts,
            now_monotonic_ns=now_monotonic_ns,
            stale_ms=self.polymarket_stale_ms,
        ) or stale_diagnostics.stale_source in {"binance", "both"}:
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
        tick_size = pending.tick_size_at_detection
        spread_at_detection = pending.spread_at_detection or pending.spread_before
        effective_threshold = self._effective_reprice_threshold(pending.market)
        data_quality_tier, data_quality_reason = self._data_quality(
            market=pending.market,
            quote_was_fillable=True,
            book_has_snapshot=pending.book_has_snapshot_at_detection,
            book_structurally_complete=pending.book_structurally_complete_at_detection,
            reported_best_validation_ok=pending.reported_best_validation_ok_at_detection,
            validation_error=pending.book_validation_error_at_detection,
            validation_mode=pending.validation_mode,
            market_quote_complete_rate=pending.market_quote_complete_rate_at_detection,
            best_ask_size=pending.before_best_ask_size,
            best_bid_size=pending.current_best_bid_size,
            tick_size=tick_size,
        )

        return TradableGapObservation(
            symbol=pending.symbol,
            market_id=pending.market.market_id,
            market_slug=pending.market.market_slug,
            base_asset=pending.market.base_asset,
            duration_minutes=pending.market.duration_minutes,
            token_id=pending.token_id,
            direction=pending.direction,
            binance_move_pct=pending.binance_move_pct,
            move_episode_id=pending.move_episode_id,
            source_move_window_ms=pending.source_move_window_ms,
            source_move_start_ts_ns=pending.source_move_start_ts_ns,
            source_move_end_ts_ns=pending.source_move_end_ts_ns,
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
            tick_size_at_detection=tick_size,
            spread_at_detection=spread_at_detection,
            spread_ticks_at_detection=_ticks(spread_at_detection, tick_size),
            entry_ask_ticks=_price_ticks(pending.entry_ask, tick_size),
            exit_edge_ticks=_ticks(exit_edge, tick_size),
            estimated_edge_ticks=_ticks(raw_edge, tick_size),
            reprice_threshold_ticks=_ticks(self.reprice_threshold, tick_size),
            effective_reprice_threshold=effective_threshold,
            effective_reprice_threshold_ticks=_ticks(effective_threshold, tick_size),
            repricing_delay_ms=executable_delay_ms,
            tradable_window_ms=self._tradable_window_ms(pending),
            hypothetical_entry_price=pending.entry_ask,
            hypothetical_exit_price=pending.executable_exit_bid or pending.current_best_bid,
            quote_was_fillable=True,
            estimated_edge_raw=raw_edge,
            estimated_edge_after_spread=exit_edge if reject_stage == "none" else None,
            market_classification_at_detection=pending.market_classification_at_detection,
            signal_enabled_at_detection=pending.signal_enabled_at_detection,
            book_complete_at_detection=pending.book_complete_at_detection,
            book_has_snapshot_at_detection=pending.book_has_snapshot_at_detection,
            book_structurally_complete_at_detection=(
                pending.book_structurally_complete_at_detection
            ),
            reported_best_validation_ok_at_detection=(
                pending.reported_best_validation_ok_at_detection
            ),
            book_validation_error_at_detection=pending.book_validation_error_at_detection,
            book_warmup_ms_at_detection=pending.book_warmup_ms_at_detection,
            book_warmup_timeout=pending.book_warmup_timeout,
            stale_source=pending.stale_diagnostics.stale_source,
            binance_quote_age_ms=pending.stale_diagnostics.binance_quote_age_ms,
            polymarket_quote_age_ms=pending.stale_diagnostics.polymarket_quote_age_ms,
            now_monotonic_ns=pending.stale_diagnostics.now_monotonic_ns,
            last_binance_update_monotonic_ns=(
                pending.stale_diagnostics.last_binance_update_monotonic_ns
            ),
            last_polymarket_update_monotonic_ns=(
                pending.stale_diagnostics.last_polymarket_update_monotonic_ns
            ),
            binance_local_received_ts_ns=(
                pending.stale_diagnostics.binance_local_received_ts_ns
            ),
            polymarket_event_ts_ns=pending.stale_diagnostics.polymarket_event_ts_ns,
            polymarket_local_received_ts_ns=(
                pending.stale_diagnostics.polymarket_local_received_ts_ns
            ),
            state_updated_monotonic_ns=(
                pending.stale_diagnostics.state_updated_monotonic_ns
            ),
            detector_processed_monotonic_ns=(
                pending.stale_diagnostics.detector_processed_monotonic_ns
            ),
            validation_mode=pending.validation_mode,
            validation_tolerance_ticks=pending.validation_tolerance_ticks,
            market_mismatch_rate_at_detection=pending.market_mismatch_rate_at_detection,
            token_mismatch_rate_at_detection=pending.token_mismatch_rate_at_detection,
            market_quote_complete_rate_at_detection=(
                pending.market_quote_complete_rate_at_detection
            ),
            token_quote_complete_rate_at_detection=(
                pending.token_quote_complete_rate_at_detection
            ),
            data_quality_tier=data_quality_tier,
            data_quality_reason=data_quality_reason,
            pre_entry_reject_reason=pending.pre_entry_reject_reason,
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
        if pending.reject_stage == "none":
            return None
        if pending.reject_stage == "timeout":
            if pending.first_mid_repriced_ts_ns is None:
                return "no_mid_repricing_before_timeout"
            if pending.first_executable_repriced_ts_ns is None:
                return "no_executable_repricing_before_timeout"
            return None
        if pending.reject_stage == "exit":
            return pending.exit_reject_reason
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

    def _pre_entry_observation(
        self,
        *,
        symbol: str,
        market: PolymarketMarketMetadata,
        direction: GapDirection,
        token_id: str,
        move_pct: float,
        now_ts: int,
        now_monotonic_ns: int,
        state: MarketState,
        binance_event_ts: int | None,
        quote: PolymarketQuote | None,
        reason: str,
        book_gate: BookReadinessGate | None = None,
        stale_diagnostics: StaleDiagnostics | None = None,
        episode: MoveEpisode | None = None,
    ) -> TradableGapObservation:
        quote_ts = _quote_timestamp(quote) if quote is not None else None
        entry_ask = quote.best_ask if quote is not None else None
        entry_ask_size = quote.best_ask_size if quote is not None else None
        tick_size = self._tick_size_for_market(market)
        spread_at_detection = (
            _spread(quote.best_bid, quote.best_ask) if quote is not None else None
        )
        raw_edge = None
        effective_threshold = self._effective_reprice_threshold(market)
        book_complete = (
            quote.book_complete
            if quote is not None
            else (None if book_gate is None else book_gate.book_complete)
        )
        validation_error = (
            quote.validation_error
            if quote is not None
            else (None if book_gate is None else book_gate.validation_error)
        )
        book_has_snapshot = (
            quote.book_has_snapshot
            if quote is not None
            else (None if book_gate is None else book_gate.book_has_snapshot)
        )
        structurally_complete = (
            quote.book_structurally_complete
            if quote is not None
            else (None if book_gate is None else book_gate.book_structurally_complete)
        )
        reported_best_ok = (
            quote.reported_best_validation_ok
            if quote is not None
            else (None if book_gate is None else book_gate.reported_best_validation_ok)
        )
        market_mismatch_rate = _coalesce(
            None if quote is None else quote.market_mismatch_rate,
            None if book_gate is None else book_gate.market_mismatch_rate,
        )
        token_mismatch_rate = _coalesce(
            None if quote is None else quote.token_mismatch_rate,
            None if book_gate is None else book_gate.token_mismatch_rate,
        )
        market_complete_rate = _coalesce(
            None if quote is None else quote.market_quote_complete_rate,
            None if book_gate is None else book_gate.market_quote_complete_rate,
        )
        token_complete_rate = _coalesce(
            None if quote is None else quote.token_quote_complete_rate,
            None if book_gate is None else book_gate.token_quote_complete_rate,
        )
        data_quality_tier, data_quality_reason = self._data_quality(
            market=market,
            quote_was_fillable=False,
            book_has_snapshot=book_has_snapshot,
            book_structurally_complete=structurally_complete,
            reported_best_validation_ok=reported_best_ok,
            validation_error=validation_error or reason,
            validation_mode=self.validation_mode,
            market_quote_complete_rate=market_complete_rate,
            best_ask_size=entry_ask_size,
            best_bid_size=quote.best_bid_size if quote is not None else None,
            tick_size=tick_size,
        )
        stale = stale_diagnostics or self._stale_diagnostics(
            symbol,
            quote,
            state,
            now_ts=now_ts,
            now_monotonic_ns=now_monotonic_ns,
        )
        return TradableGapObservation(
            symbol=symbol,
            market_id=market.market_id,
            market_slug=market.market_slug,
            base_asset=market.base_asset,
            duration_minutes=market.duration_minutes,
            token_id=token_id,
            direction=direction,
            binance_move_pct=move_pct,
            move_episode_id=None if episode is None else episode.move_episode_id,
            source_move_window_ms=None if episode is None else episode.source_move_window_ms,
            source_move_start_ts_ns=None if episode is None else episode.source_move_start_ts_ns,
            source_move_end_ts_ns=None if episode is None else episode.source_move_end_ts_ns,
            detected_ts_ns=now_ts,
            binance_event_ts_ns=binance_event_ts,
            poly_quote_ts_ns=quote_ts,
            before_best_bid=quote.best_bid if quote is not None else None,
            before_best_ask=quote.best_ask if quote is not None else None,
            before_best_bid_size=quote.best_bid_size if quote is not None else None,
            before_best_ask_size=quote.best_ask_size if quote is not None else None,
            before_mid=quote.mid_price if quote is not None else None,
            after_best_bid=quote.best_bid if quote is not None else None,
            after_best_ask=quote.best_ask if quote is not None else None,
            after_mid=quote.mid_price if quote is not None else None,
            spread_before=quote.spread if quote is not None else None,
            spread_after=quote.spread if quote is not None else None,
            repricing_delay_ms=None,
            tradable_window_ms=0.0,
            hypothetical_entry_price=entry_ask,
            hypothetical_exit_price=None,
            quote_was_fillable=False,
            entry_ask=entry_ask,
            entry_ask_size=entry_ask_size,
            tick_size_at_detection=tick_size,
            spread_at_detection=spread_at_detection,
            spread_ticks_at_detection=_ticks(spread_at_detection, tick_size),
            entry_ask_ticks=_price_ticks(entry_ask, tick_size),
            estimated_edge_ticks=_ticks(raw_edge, tick_size),
            reprice_threshold_ticks=_ticks(self.reprice_threshold, tick_size),
            effective_reprice_threshold=effective_threshold,
            effective_reprice_threshold_ticks=_ticks(effective_threshold, tick_size),
            market_classification_at_detection=(
                None if book_gate is None else book_gate.market_classification
            ),
            signal_enabled_at_detection=None if book_gate is None else book_gate.signal_enabled,
            book_complete_at_detection=book_complete,
            book_has_snapshot_at_detection=book_has_snapshot,
            book_structurally_complete_at_detection=structurally_complete,
            reported_best_validation_ok_at_detection=reported_best_ok,
            book_validation_error_at_detection=validation_error,
            book_warmup_ms_at_detection=None if book_gate is None else book_gate.warmup_ms,
            book_warmup_timeout=False if book_gate is None else book_gate.warmup_timeout,
            stale_source=stale.stale_source,
            binance_quote_age_ms=stale.binance_quote_age_ms,
            polymarket_quote_age_ms=stale.polymarket_quote_age_ms,
            now_monotonic_ns=stale.now_monotonic_ns,
            last_binance_update_monotonic_ns=stale.last_binance_update_monotonic_ns,
            last_polymarket_update_monotonic_ns=stale.last_polymarket_update_monotonic_ns,
            binance_local_received_ts_ns=stale.binance_local_received_ts_ns,
            polymarket_event_ts_ns=stale.polymarket_event_ts_ns,
            polymarket_local_received_ts_ns=stale.polymarket_local_received_ts_ns,
            state_updated_monotonic_ns=stale.state_updated_monotonic_ns,
            detector_processed_monotonic_ns=stale.detector_processed_monotonic_ns,
            validation_mode=self.validation_mode,
            validation_tolerance_ticks=self.validation_tolerance_ticks,
            market_mismatch_rate_at_detection=market_mismatch_rate,
            token_mismatch_rate_at_detection=token_mismatch_rate,
            market_quote_complete_rate_at_detection=market_complete_rate,
            token_quote_complete_rate_at_detection=token_complete_rate,
            data_quality_tier=data_quality_tier,
            data_quality_reason=data_quality_reason,
            pre_entry_reject_reason=reason,
            reject_stage="pre_entry",
            reject_reason=reason,
        )

    def _append_pre_entry_observation(
        self,
        observations: list[TradableGapObservation],
        observation: TradableGapObservation,
    ) -> None:
        recorded = self._record_pre_entry_observation(observation)
        if recorded is not None:
            observations.append(recorded)

    def _record_pre_entry_observation(
        self,
        observation: TradableGapObservation,
    ) -> TradableGapObservation | None:
        self.non_fillable_at_detection_count += 1
        self._increment(self._non_fillable_by_symbol, observation.symbol)
        cooldown_key = (
            observation.symbol,
            observation.market_id,
            observation.direction,
            observation.pre_entry_reject_reason or observation.reject_reason or "",
        )
        last_logged_ns = self._last_pre_entry_log_ns.get(cooldown_key)
        if last_logged_ns is not None:
            elapsed_ms = _duration_ms(last_logged_ns, observation.detected_ts_ns)
            if elapsed_ms is not None and elapsed_ms < self.pre_entry_log_cooldown_ms:
                self.pre_entry_observations_suppressed += 1
                self._increment(self._suppressed_candidates_by_symbol, observation.symbol)
                return None

        self._last_pre_entry_log_ns[cooldown_key] = observation.detected_ts_ns
        self.pre_entry_observations_written += 1
        if observation.reject_reason is not None:
            self._record_reject(observation.reject_reason, "pre_entry")
        self._record_observation_diagnostics(observation)
        self._completed.append(observation)
        return observation

    def _record_warmup_quote(self, quote: PolymarketQuote, *, now_ts: int) -> None:
        market = self._markets_by_token.get(quote.token_id)
        if market is None:
            return
        if (
            market.selected_for_runtime
            and not self._market_signal_enabled(market, now_ts)
            and classify_market_window(market, now_ts=now_ts // 1_000_000_000) == "next"
        ):
            self.warmup_quotes_received += 1

    def _market_signal_enabled(
        self,
        market: PolymarketMarketMetadata,
        now_ts: int,
    ) -> bool:
        return (
            is_runtime_tradable_market(market, now_ts=now_ts // 1_000_000_000)
            and classify_market_window(market, now_ts=now_ts // 1_000_000_000) == "current"
        )

    def _book_readiness_gate(
        self,
        market: PolymarketMarketMetadata,
        state: MarketState,
        *,
        now_ts: int,
    ) -> BookReadinessGate:
        classification = classify_market_window(market, now_ts=now_ts // 1_000_000_000)
        signal_enabled = self._market_signal_enabled(market, now_ts)
        quotes = [
            state.polymarket_quotes.get(token_id)
            for token_id in (market.up_token_id, market.down_token_id)
            if token_id is not None
        ]
        known_quotes = [quote for quote in quotes if quote is not None]
        explicit_ready = len(quotes) >= 2 and all(
            quote is not None and _quote_has_initial_snapshot(quote)
            for quote in quotes
        )
        legacy_synthetic_ready = bool(known_quotes) and all(
            quote.book_update_type is None and quote.book_complete
            for quote in known_quotes
        )
        both_ready = explicit_ready or legacy_synthetic_ready
        warmup_start = _min_quote_timestamp(known_quotes) or self._detector_started_ns
        warmup_ms = _duration_ms(warmup_start, now_ts)
        warmup_timeout = (
            not both_ready
            and warmup_ms is not None
            and warmup_ms >= self.book_warmup_max_ms
        )
        suppress = self.require_book_ready and not both_ready and not warmup_timeout
        return BookReadinessGate(
            suppress=suppress,
            warmup_ms=warmup_ms,
            warmup_timeout=warmup_timeout,
            book_complete=both_ready,
            book_has_snapshot=(
                len(quotes) >= 2
                and all(quote is not None and _quote_has_initial_snapshot(quote) for quote in quotes)
            ),
            book_structurally_complete=(
                len(quotes) >= 2
                and all(
                    quote is not None and quote.book_structurally_complete
                    for quote in quotes
                )
            ),
            reported_best_validation_ok=(
                bool(known_quotes)
                and all(quote.reported_best_validation_ok for quote in known_quotes)
            ),
            validation_error=_first_validation_error(known_quotes),
            market_classification=classification,
            signal_enabled=signal_enabled,
            market_mismatch_rate=_first_rate(known_quotes, "market_mismatch_rate"),
            token_mismatch_rate=_first_rate(known_quotes, "token_mismatch_rate"),
            market_quote_complete_rate=_first_rate(
                known_quotes,
                "market_quote_complete_rate",
            ),
            token_quote_complete_rate=_first_rate(known_quotes, "token_quote_complete_rate"),
        )

    def _tick_size_for_market(self, market: PolymarketMarketMetadata) -> float | None:
        tick_size = self._tick_size_by_market.get(market.market_id, market.tick_size)
        return tick_size if tick_size > 0.0 else None

    def _effective_reprice_threshold(self, market: PolymarketMarketMetadata) -> float:
        tick_size = self._tick_size_for_market(market)
        if tick_size is None:
            return self.reprice_threshold
        return max(self.reprice_threshold, tick_size)

    def _data_quality(
        self,
        *,
        market: PolymarketMarketMetadata,
        quote_was_fillable: bool,
        book_has_snapshot: bool | None,
        book_structurally_complete: bool | None,
        reported_best_validation_ok: bool | None,
        validation_error: str | None,
        validation_mode: ValidationMode | None,
        market_quote_complete_rate: float | None,
        best_ask_size: float | None,
        best_bid_size: float | None,
        tick_size: float | None,
    ) -> tuple[DataQualityTier, str]:
        if tick_size is None:
            return "D", "missing_tick_size"
        if validation_error in {"quote_stale", "book_stale"}:
            return "D", "quote_stale"
        if validation_error in {"missing_best_ask_size", "best_ask_size_unknown", "best_bid_size_unknown"}:
            return "D", "size_unknown"
        if quote_was_fillable and best_ask_size is None:
            return "D", "size_unknown"
        if not book_has_snapshot:
            return "D", "missing_snapshot"
        if not book_structurally_complete:
            return "D", "structurally_incomplete"
        if validation_mode == "diagnostic":
            if validation_error and validation_error.startswith("reported_best_"):
                return "C", "diagnostic_mode_only"
            return "C", "diagnostic_mode_only"
        if market_quote_complete_rate is not None and market_quote_complete_rate < 0.85:
            return "C", "low_quote_complete_rate"
        if validation_mode == "tolerant" and validation_error and validation_error.startswith(
            "reported_best_"
        ):
            return "B", "tolerated_one_tick_mismatch"
        if reported_best_validation_ok is True and (
            market_quote_complete_rate is None or market_quote_complete_rate >= 0.95
        ):
            if best_bid_size is None and validation_error in {None, ""}:
                return "B", "size_unknown"
            return "A", "clean_validated"
        if reported_best_validation_ok is True:
            return "B", "clean_validated"
        return "C", validation_error or "weaker_validation_quality"

    def _stale_diagnostics(
        self,
        symbol: str,
        quote: PolymarketQuote | None,
        state: MarketState,
        *,
        now_ts: int,
        now_monotonic_ns: int,
    ) -> StaleDiagnostics:
        symbol_state = state.symbols.get(symbol)
        last_binance_mono = None if symbol_state is None else (
            symbol_state.recv_monotonic_ns
            or symbol_state.parse_done_monotonic_ns
        )
        last_poly_mono = None if quote is None else (
            quote.recv_monotonic_ns
            or quote.parse_done_monotonic_ns
        )
        has_monotonic_now = now_monotonic_ns != now_ts
        binance_age_ms = (
            _duration_ms(last_binance_mono, now_monotonic_ns)
            if has_monotonic_now
            else None
        )
        poly_age_ms = (
            _duration_ms(last_poly_mono, now_monotonic_ns)
            if has_monotonic_now
            else None
        )
        has_quote_monotonic = quote is not None and (
            quote.recv_monotonic_ns is not None
            or quote.parse_done_monotonic_ns is not None
        )
        has_binance_monotonic = last_binance_mono is not None
        if (
            quote is not None
            and quote.book_stale
            and not has_quote_monotonic
            and not has_binance_monotonic
        ):
            source = "unknown"
        elif binance_age_ms is None or poly_age_ms is None:
            source: StaleSource = "unknown"
        else:
            binance_stale = binance_age_ms > self.binance_stale_ms
            poly_stale = poly_age_ms > self.polymarket_stale_ms or (
                quote.book_stale if quote is not None else False
            )
            if binance_stale and poly_stale:
                source = "both"
            elif binance_stale:
                source = "binance"
            elif poly_stale:
                source = "polymarket"
            else:
                source = "unknown"
        return StaleDiagnostics(
            stale_source=source,
            binance_quote_age_ms=binance_age_ms,
            polymarket_quote_age_ms=poly_age_ms,
            now_monotonic_ns=now_monotonic_ns,
            last_binance_update_monotonic_ns=last_binance_mono,
            last_polymarket_update_monotonic_ns=last_poly_mono,
            binance_local_received_ts_ns=(
                None if symbol_state is None else symbol_state.local_receive_timestamp
            ),
            polymarket_event_ts_ns=None if quote is None else quote.exchange_event_ts or quote.event_ts,
            polymarket_local_received_ts_ns=None if quote is None else quote.local_received_ts,
            state_updated_monotonic_ns=(
                None
                if quote is None
                else quote.state_updated_monotonic_ns
                or quote.parse_done_monotonic_ns
                or quote.recv_monotonic_ns
            ),
            detector_processed_monotonic_ns=now_monotonic_ns,
        )

    def _record_reject(self, reason: str, stage: RejectStage) -> None:
        self._reject_count_by_reason[reason] = self._reject_count_by_reason.get(reason, 0) + 1
        self._reject_count_by_stage[stage] = self._reject_count_by_stage.get(stage, 0) + 1

    def _record_observation_diagnostics(self, observation: TradableGapObservation) -> None:
        if observation.reject_stage == "pre_entry":
            self._increment(self._pre_entry_rejects_by_symbol, observation.symbol)
        elif observation.reject_stage == "window":
            self._increment(self._window_rejects_by_symbol, observation.symbol)
        elif observation.reject_stage == "timeout":
            self._increment(self._timeout_rejects_by_symbol, observation.symbol)
        if observation.reject_reason is not None:
            reasons = self._reject_reasons_by_symbol.setdefault(observation.symbol, {})
            self._increment(reasons, observation.reject_reason)

    @staticmethod
    def _increment(counter: dict[str, int], key: str) -> None:
        counter[key] = counter.get(key, 0) + 1


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


def _markets_by_token(
    markets: tuple[PolymarketMarketMetadata, ...],
) -> dict[str, PolymarketMarketMetadata]:
    mapping: dict[str, PolymarketMarketMetadata] = {}
    for market in markets:
        for token_id in market.token_ids:
            mapping[token_id] = market
    return mapping


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


def _quote_has_initial_snapshot(quote: PolymarketQuote) -> bool:
    if quote.book_has_snapshot:
        return True
    return quote.book_update_type is None and quote.book_complete


def _min_quote_timestamp(quotes: Iterable[PolymarketQuote]) -> int | None:
    timestamps = [
        timestamp
        for quote in quotes
        if (timestamp := _quote_timestamp(quote)) is not None
    ]
    return min(timestamps) if timestamps else None


def _first_validation_error(quotes: Iterable[PolymarketQuote]) -> str | None:
    for quote in quotes:
        if quote.validation_error is not None:
            return quote.validation_error
    return None


def _first_rate(quotes: Iterable[PolymarketQuote], attr: str) -> float | None:
    for quote in quotes:
        value = getattr(quote, attr)
        if isinstance(value, int | float):
            return float(value)
    return None


def _coalesce(left: float | None, right: float | None) -> float | None:
    return left if left is not None else right


def _spread(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return max(0.0, best_ask - best_bid)


def _ticks(value: float | None, tick_size: float | None) -> float | None:
    if value is None or tick_size is None:
        return None
    return diff_to_ticks(value, tick_size)


def _price_ticks(value: float | None, tick_size: float | None) -> float | None:
    if value is None or tick_size is None:
        return None
    return price_to_ticks(value, tick_size)


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


def _count_strings(values: Iterable[str | None]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if not isinstance(value, str):
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _move_episode(
    *,
    symbol: str,
    direction: GapDirection,
    window_key: str | None,
    event_ts_ns: int,
) -> MoveEpisode:
    window_ms = _window_key_ms(window_key)
    window_ns = int(window_ms * 1_000_000)
    bucket = event_ts_ns // max(1, window_ns)
    start_ts = bucket * window_ns
    end_ts = start_ts + window_ns
    return MoveEpisode(
        move_episode_id=f"{symbol}:{direction}:{int(window_ms)}:{bucket}",
        source_move_window_ms=window_ms,
        source_move_start_ts_ns=start_ts,
        source_move_end_ts_ns=end_ts,
        window_bucket=bucket,
    )


def _window_key_ms(window_key: str | None) -> float:
    if window_key == "1s":
        return 1_000.0
    if window_key == "5s":
        return 5_000.0
    if window_key == "15s":
        return 15_000.0
    if window_key == "30s":
        return 30_000.0
    return 0.0
