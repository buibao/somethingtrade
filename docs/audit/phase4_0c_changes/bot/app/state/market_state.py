from collections import deque
from dataclasses import asdict, dataclass, field
from math import sqrt

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import (
    BinanceMarketEvent,
    DepthUpdate,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    RealtimeMarketEvent,
)

RETURN_WINDOWS_SEC = (1, 5, 15, 30)
MAX_RETURN_WINDOW_NS = max(RETURN_WINDOWS_SEC) * 1_000_000_000


@dataclass(slots=True)
class SymbolState:
    symbol: str
    latest_price: float | None = None
    best_bid: float | None = None
    best_bid_size: float | None = None
    best_ask: float | None = None
    best_ask_size: float | None = None
    rolling_returns: dict[str, float | None] = field(
        default_factory=lambda: {f"{window}s": None for window in RETURN_WINDOWS_SEC}
    )
    volatility_30s: float | None = None
    bid_ask_spread: float | None = None
    last_event_timestamp: int | None = None
    local_receive_timestamp: int | None = None
    parse_done_timestamp: int | None = None
    state_updated_timestamp: int | None = None
    recv_monotonic_ns: int | None = None
    parse_done_monotonic_ns: int | None = None
    state_updated_monotonic_ns: int | None = None
    latency_ms: float | None = None
    last_depth_update_id: int | None = None
    price_history: deque[tuple[int, float]] = field(default_factory=deque)

    def snapshot(self) -> dict[str, object]:
        data = asdict(self)
        data.pop("price_history", None)
        return data


class MarketState:
    """In-memory realtime market state.

    This object is intentionally database-free. It is safe to keep on the hot
    path because updates are simple dictionary/deque operations.
    """

    def __init__(self, *, max_polymarket_quote_age_ms: float = 5_000.0) -> None:
        self.ticks: dict[str, MarketTick] = {}
        self.book_tops: dict[str, OrderBookTop] = {}
        self.depth_updates: dict[str, DepthUpdate] = {}
        self.polymarket_quotes: dict[str, PolymarketQuote] = {}
        self.market_lifecycle_events: list[MarketLifecycleEvent] = []
        self.lifecycle_events_by_market: dict[str, MarketLifecycleEvent] = {}
        self.market_invalid: set[str] = set()
        self.invalid_polymarket_markets: set[str] = self.market_invalid
        self.symbols: dict[str, SymbolState] = {}
        self.max_polymarket_quote_age_ms = max_polymarket_quote_age_ms

    def apply(
        self,
        event: BinanceMarketEvent | PolymarketQuote | MarketLifecycleEvent,
    ) -> BinanceMarketEvent | PolymarketQuote | MarketLifecycleEvent | None:
        state_updated_ts = utc_now_ns()
        state_updated_monotonic_ns = monotonic_now_ns()
        if isinstance(event, PolymarketQuote) and event.market_id in self.market_invalid:
            event = event.model_copy(
                update={
                    "book_complete": False,
                    "validation_error": event.validation_error or "market_invalidated",
                }
            )
        if isinstance(event, PolymarketQuote) and self.is_stale_quote(
            event,
            now_ts=state_updated_ts,
            now_monotonic_ns=state_updated_monotonic_ns,
        ):
            event = event.model_copy(
                update={
                    "book_complete": False,
                    "book_stale": True,
                    "validation_error": event.validation_error or "quote_stale",
                }
            )

        updated_event = self._with_state_latency(
            event,
            state_updated_ts=state_updated_ts,
            state_updated_monotonic_ns=state_updated_monotonic_ns,
        )

        if isinstance(updated_event, MarketTick):
            self.ticks[updated_event.symbol] = updated_event
            symbol_state = self._symbol_state(updated_event.symbol)
            symbol_state.latest_price = updated_event.price
            self._append_price(
                symbol_state,
                timestamp=_event_timestamp(updated_event),
                price=updated_event.price,
            )
            self._update_common_timestamps(symbol_state, updated_event, state_updated_ts)

        elif isinstance(updated_event, OrderBookTop):
            self.book_tops[updated_event.symbol] = updated_event
            symbol_state = self._symbol_state(updated_event.symbol)
            symbol_state.best_bid = updated_event.bid_price
            symbol_state.best_bid_size = updated_event.bid_size
            symbol_state.best_ask = updated_event.ask_price
            symbol_state.best_ask_size = updated_event.ask_size
            symbol_state.bid_ask_spread = max(
                0.0,
                updated_event.ask_price - updated_event.bid_price,
            )
            self._update_common_timestamps(symbol_state, updated_event, state_updated_ts)

        elif isinstance(updated_event, DepthUpdate):
            self.depth_updates[updated_event.symbol] = updated_event
            symbol_state = self._symbol_state(updated_event.symbol)
            symbol_state.last_depth_update_id = updated_event.final_update_id
            self._update_common_timestamps(symbol_state, updated_event, state_updated_ts)

        elif isinstance(updated_event, PolymarketQuote):
            self.polymarket_quotes[updated_event.token_id] = updated_event
        elif isinstance(updated_event, MarketLifecycleEvent):
            self.market_lifecycle_events.append(updated_event)
            self.lifecycle_events_by_market[updated_event.market_id] = updated_event
            if updated_event.lifecycle_type in {
                "market_resolved",
                "closed",
                "expired",
            }:
                self.market_invalid.add(updated_event.market_id)
                self._invalidate_polymarket_quotes(updated_event.market_id)

        return updated_event

    def is_stale_quote(
        self,
        quote: PolymarketQuote,
        *,
        now_ts: int | None = None,
        now_monotonic_ns: int | None = None,
    ) -> bool:
        if quote.recv_monotonic_ns is not None and now_monotonic_ns is not None:
            age_ms = (now_monotonic_ns - quote.recv_monotonic_ns) / 1_000_000.0
            return age_ms > self.max_polymarket_quote_age_ms

        reference_ts = quote.event_ts or quote.received_ts or quote.exchange_event_ts
        if reference_ts is None:
            return False
        current_ts = now_ts or utc_now_ns()
        age_ms = (current_ts - reference_ts) / 1_000_000.0
        return age_ms > self.max_polymarket_quote_age_ms

    def is_market_invalid(self, market_id: str) -> bool:
        return market_id in self.market_invalid

    def snapshot(self) -> dict[str, dict[str, object]]:
        return {symbol: state.snapshot() for symbol, state in self.symbols.items()}

    def compact_lines(self) -> list[str]:
        lines: list[str] = []
        for symbol in sorted(self.symbols):
            state = self.symbols[symbol]
            lines.append(
                " ".join(
                    [
                        symbol,
                        f"px={_fmt_float(state.latest_price)}",
                        f"bid={_fmt_float(state.best_bid)}",
                        f"ask={_fmt_float(state.best_ask)}",
                        f"r1s={_fmt_bps(state.rolling_returns['1s'])}",
                        f"r5s={_fmt_bps(state.rolling_returns['5s'])}",
                        f"r15s={_fmt_bps(state.rolling_returns['15s'])}",
                        f"r30s={_fmt_bps(state.rolling_returns['30s'])}",
                        f"lat={_fmt_latency(state.latency_ms)}",
                    ]
                )
            )
        return lines

    def polymarket_compact_lines(self) -> list[str]:
        lines: list[str] = []
        for token_id, quote in sorted(
            self.polymarket_quotes.items(),
            key=lambda item: (item[1].market_id, item[1].side_label),
        ):
            token = token_id[:8]
            lines.append(
                " ".join(
                    [
                        quote.market_id[:10],
                        quote.side_label,
                        f"token={token}",
                        f"bid={_fmt_prob(quote.best_bid)}",
                        f"ask={_fmt_prob(quote.best_ask)}",
                        f"mid={_fmt_prob(quote.mid_price)}",
                        f"spr={_fmt_prob(quote.spread)}",
                        f"bidSz={_fmt_float(quote.best_bid_size)}",
                        f"askSz={_fmt_float(quote.best_ask_size)}",
                        f"lat={_fmt_latency(quote.latency_ms)}",
                    ]
                )
            )
        return lines

    def _symbol_state(self, symbol: str) -> SymbolState:
        if symbol not in self.symbols:
            self.symbols[symbol] = SymbolState(symbol=symbol)
        return self.symbols[symbol]

    def _invalidate_polymarket_quotes(self, market_id: str) -> None:
        for token_id, quote in list(self.polymarket_quotes.items()):
            if quote.market_id != market_id:
                continue
            self.polymarket_quotes[token_id] = quote.model_copy(
                update={
                    "book_complete": False,
                    "validation_error": quote.validation_error or "market_invalidated",
                }
            )

    def _append_price(self, state: SymbolState, *, timestamp: int, price: float) -> None:
        state.price_history.append((timestamp, price))
        cutoff = timestamp - MAX_RETURN_WINDOW_NS
        while state.price_history and state.price_history[0][0] < cutoff:
            state.price_history.popleft()
        self._recompute_returns(state, now_ts=timestamp, current_price=price)
        self._recompute_volatility_30s(state)

    def _recompute_returns(
        self,
        state: SymbolState,
        *,
        now_ts: int,
        current_price: float,
    ) -> None:
        for window_sec in RETURN_WINDOWS_SEC:
            key = f"{window_sec}s"
            baseline = _price_at_or_before(
                state.price_history,
                target_ts=now_ts - window_sec * 1_000_000_000,
            )
            if baseline is None or baseline == 0.0:
                state.rolling_returns[key] = None
            else:
                state.rolling_returns[key] = current_price / baseline - 1.0

    def _recompute_volatility_30s(self, state: SymbolState) -> None:
        if len(state.price_history) < 3:
            state.volatility_30s = None
            return

        returns: list[float] = []
        previous_price = state.price_history[0][1]
        for _, price in list(state.price_history)[1:]:
            if previous_price > 0.0:
                returns.append(price / previous_price - 1.0)
            previous_price = price

        if len(returns) < 2:
            state.volatility_30s = None
            return

        mean = sum(returns) / len(returns)
        variance = sum((value - mean) ** 2 for value in returns) / len(returns)
        state.volatility_30s = sqrt(variance)

    def _update_common_timestamps(
        self,
        state: SymbolState,
        event: RealtimeMarketEvent,
        state_updated_ts: int,
    ) -> None:
        state.last_event_timestamp = event.exchange_event_ts or event.ts_ns
        state.local_receive_timestamp = event.local_received_ts
        state.parse_done_timestamp = event.parse_done_ts
        state.state_updated_timestamp = state_updated_ts
        state.recv_monotonic_ns = event.recv_monotonic_ns
        state.parse_done_monotonic_ns = event.parse_done_monotonic_ns
        state.state_updated_monotonic_ns = event.state_updated_monotonic_ns
        state.latency_ms = event.latency_ms

    def _with_state_latency(
        self,
        event: BinanceMarketEvent | PolymarketQuote | MarketLifecycleEvent,
        *,
        state_updated_ts: int,
        state_updated_monotonic_ns: int,
    ) -> BinanceMarketEvent | PolymarketQuote | MarketLifecycleEvent:
        if not isinstance(event, RealtimeMarketEvent):
            return event

        latency_ms = (
            None
            if event.recv_monotonic_ns is None
            else (state_updated_monotonic_ns - event.recv_monotonic_ns) / 1_000_000.0
        )
        return event.model_copy(
            update={
                "state_updated_ts": state_updated_ts,
                "state_updated_monotonic_ns": state_updated_monotonic_ns,
                "latency_ms": latency_ms,
            }
        )


def _event_timestamp(event: RealtimeMarketEvent) -> int:
    return event.exchange_event_ts or event.local_received_ts or event.ts_ns


def _price_at_or_before(history: deque[tuple[int, float]], *, target_ts: int) -> float | None:
    price: float | None = None
    for ts, candidate in history:
        if ts <= target_ts:
            price = candidate
        else:
            break
    return price


def _fmt_float(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def _fmt_bps(value: float | None) -> str:
    return "-" if value is None else f"{value * 10_000:+.1f}bp"


def _fmt_latency(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}ms"


def _fmt_prob(value: float | None) -> str:
    return "-" if value is None else f"{value:.3f}"
