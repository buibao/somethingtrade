from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_serializer

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


class MarketTick(EventModel):
    event_type: Literal["market_tick"] = "market_tick"
    source: Literal["binance", "polymarket", "replay"]
    symbol: str
    price: float
    size: float
    exchange_ts_ns: int | None = None
    sequence: int | None = None


class OrderBookTop(EventModel):
    event_type: Literal["order_book_top"] = "order_book_top"
    source: Literal["binance", "polymarket", "replay"]
    symbol: str
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    exchange_ts_ns: int | None = None
    sequence: int | None = None


class PolymarketQuote(EventModel):
    event_type: Literal["polymarket_quote"] = "polymarket_quote"
    market_id: str
    condition_id: str | None = None
    token_id: str
    outcome: str
    bid_probability: float
    ask_probability: float
    bid_size: float
    ask_size: float
    exchange_ts_ns: int | None = None
    sequence: int | None = None


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


Event = (
    MarketTick
    | OrderBookTop
    | PolymarketQuote
    | SignalEvent
    | OrderIntent
    | ExecutionReport
    | LatencyTrace
)
