from enum import StrEnum
from typing import Any, Literal, TypeAlias
from uuid import uuid4

import orjson
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
)

from app.core.clock import utc_now_ns


class EventModel(BaseModel):
    """Base class for JSON-serializable cross-module events."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        use_enum_values=True,
    )

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    ts_ns: int = Field(default_factory=utc_now_ns)

    def to_json_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump(mode="json"))

    def to_json_str(self) -> str:
        return self.to_json_bytes().decode("utf-8")


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(StrEnum):
    IOC = "ioc"
    GTC = "gtc"
    FOK = "fok"


class ExecutionStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELED = "canceled"


type PolymarketSideLabel = Literal[
    "YES",
    "NO",
    "UP",
    "DOWN",
    "ABOVE",
    "BELOW",
    "HIGHER",
    "LOWER",
    "UNKNOWN",
]
type GapDirection = Literal["UP", "DOWN"]
type MarketLifecycleType = Literal["tick_size_change", "market_resolved", "new_market"]


class BookLevel(BaseModel):
    """Single price level for order book deltas."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    price: float
    size: float


class RealtimeMarketEvent(EventModel):
    """Market data event with exchange-to-state latency telemetry."""

    exchange_event_ts: int | None = None
    local_received_ts: int | None = None
    parse_done_ts: int | None = None
    state_updated_ts: int | None = None
    recv_monotonic_ns: int | None = None
    parse_done_monotonic_ns: int | None = None
    state_updated_monotonic_ns: int | None = None
    latency_ms: float | None = None
    exchange_ts_ns: int | None = None
    sequence: int | None = None


class MarketTick(RealtimeMarketEvent):
    event_type: Literal["market_tick"] = "market_tick"
    source: Literal["binance", "polymarket", "replay"]
    symbol: str
    price: float
    size: float


class OrderBookTop(RealtimeMarketEvent):
    event_type: Literal["order_book_top"] = "order_book_top"
    source: Literal["binance", "polymarket", "replay"]
    symbol: str
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float


class DepthUpdate(RealtimeMarketEvent):
    event_type: Literal["depth_update"] = "depth_update"
    source: Literal["binance", "replay"] = "binance"
    symbol: str
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None = None
    bids: list[BookLevel] = Field(default_factory=list)
    asks: list[BookLevel] = Field(default_factory=list)


class PolymarketQuote(RealtimeMarketEvent):
    event_type: Literal["polymarket_quote"] = "polymarket_quote"
    market_id: str
    condition_id: str | None = None
    token_id: str
    side_label: PolymarketSideLabel
    best_bid: float | None = None
    best_bid_size: float | None = None
    best_ask: float | None = None
    best_ask_size: float | None = None
    mid_price: float | None = None
    spread: float | None = None
    event_ts: int | None = None
    received_ts: int | None = None
    book_complete: bool = False
    book_stale: bool = False
    book_hash: str | None = None
    validation_error: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def available_liquidity_at_best(self) -> float | None:
        known_sizes = [
            value
            for value in (self.best_bid_size, self.best_ask_size)
            if value is not None
        ]
        return sum(known_sizes) if known_sizes else None

    @property
    def outcome(self) -> str:
        return self.side_label

    @property
    def bid_probability(self) -> float:
        return self.best_bid if self.best_bid is not None else 0.0

    @property
    def ask_probability(self) -> float:
        return self.best_ask if self.best_ask is not None else 1.0

    @property
    def bid_size(self) -> float:
        return self.best_bid_size or 0.0

    @property
    def ask_size(self) -> float:
        return self.best_ask_size or 0.0


class SignalEvent(EventModel):
    event_type: Literal["signal"] = "signal"
    strategy_id: str
    market_id: str
    token_id: str
    side: Side
    fair_probability: float
    quoted_probability: float
    edge_bps: float
    confidence: float = Field(ge=0.0, le=1.0)
    features: dict[str, float | int | str | bool | None] = Field(default_factory=dict)


class OrderIntent(EventModel):
    event_type: Literal["order_intent"] = "order_intent"
    strategy_id: str
    market_id: str
    token_id: str
    side: Side
    order_type: OrderType = OrderType.LIMIT
    limit_price: float | None = None
    size: float
    time_in_force: TimeInForce = TimeInForce.IOC
    reason: str
    client_order_id: str = Field(default_factory=lambda: uuid4().hex)


class ExecutionReport(EventModel):
    event_type: Literal["execution_report"] = "execution_report"
    client_order_id: str
    venue_order_id: str | None = None
    status: ExecutionStatus
    filled_size: float = 0.0
    avg_fill_price: float | None = None
    reject_reason: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_serializer("raw")
    def serialize_raw(self, raw: dict[str, Any]) -> dict[str, Any]:
        return raw


class LatencyTrace(EventModel):
    event_type: Literal["latency_trace"] = "latency_trace"
    trace_id: str
    source_event_id: str | None = None
    recv_ns: int
    normalized_ns: int | None = None
    strategy_ns: int | None = None
    risk_ns: int | None = None
    execution_ns: int | None = None

    @property
    def total_ns(self) -> int | None:
        endpoints = [
            self.execution_ns,
            self.risk_ns,
            self.strategy_ns,
            self.normalized_ns,
        ]
        last = next((value for value in endpoints if value is not None), None)
        if last is None:
            return None
        return last - self.recv_ns


class TradableGapObservation(EventModel):
    event_type: Literal["tradable_gap_observation"] = "tradable_gap_observation"
    symbol: str
    market_id: str
    token_id: str
    direction: GapDirection
    binance_move_pct: float
    detected_ts_ns: int
    binance_event_ts_ns: int | None = None
    poly_quote_ts_ns: int | None = None
    before_best_bid: float | None = None
    before_best_ask: float | None = None
    before_best_bid_size: float | None = None
    before_best_ask_size: float | None = None
    before_mid: float | None = None
    after_best_bid: float | None = None
    after_best_ask: float | None = None
    after_mid: float | None = None
    spread_before: float | None = None
    spread_after: float | None = None
    repricing_delay_ms: float | None = Field(
        None,
        validation_alias=AliasChoices("repricing_delay_ms", "gap_duration_ms"),
    )
    tradable_window_ms: float | None = None
    hypothetical_entry_price: float | None = None
    hypothetical_exit_price: float | None = None
    quote_was_fillable: bool
    estimated_edge_raw: float | None = None
    estimated_edge_after_spread: float | None = None
    reject_reason: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gap_duration_ms(self) -> float | None:
        """Backward-compatible alias for older Phase 3 JSONL readers."""

        return self.repricing_delay_ms


class MarketLifecycleEvent(RealtimeMarketEvent):
    event_type: Literal["market_lifecycle"] = "market_lifecycle"
    market_id: str
    token_id: str | None = None
    lifecycle_type: MarketLifecycleType
    old_tick_size: float | None = None
    new_tick_size: float | None = None
    raw_metadata: dict[str, Any] = Field(default_factory=dict)
    event_ts: int | None = None
    received_ts: int | None = None


BinanceMarketEvent: TypeAlias = MarketTick | OrderBookTop | DepthUpdate

Event: TypeAlias = (
    MarketTick
    | OrderBookTop
    | DepthUpdate
    | PolymarketQuote
    | SignalEvent
    | OrderIntent
    | ExecutionReport
    | LatencyTrace
    | TradableGapObservation
    | MarketLifecycleEvent
)
