from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from app.marketdata.orderbook_state import OrderbookState


LIFECYCLE_COUNTER_FIELDS = (
    "connect_count",
    "disconnect_count",
    "reconnect_count",
    "resubscribe_count",
    "snapshot_loaded_count",
    "snapshot_refresh_count",
    "snapshot_failed_count",
    "delta_before_snapshot_count",
    "messages_before_ready_count",
    "state_reset_count",
    "sequence_gap_count",
    "duplicate_messages_detected",
    "duplicate_messages_skipped",
    "queue_backpressure_events",
    "queue_dropped_messages",
    "sample_blocked_by_ready_guard",
    "ready_to_emit_false_warning_count",
)


@dataclass(slots=True)
class WSLifecycleTracker:
    counters: Counter[str] = field(default_factory=Counter)
    ready_to_emit_false_duration_ms_max: float = 0.0
    symbol_ready_false_warnings: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in LIFECYCLE_COUNTER_FIELDS:
            self.counters[field_name] += 0

    def on_connect(self) -> None:
        self.counters["connect_count"] += 1

    def on_disconnect(self) -> None:
        self.counters["disconnect_count"] += 1

    def on_reconnect(self, state: OrderbookState | None = None) -> None:
        self.counters["reconnect_count"] += 1
        if state is not None:
            state.mark_not_ready("reconnect")
            self.counters["state_reset_count"] += 1

    def on_resubscribe(self, state: OrderbookState | None = None) -> None:
        self.counters["resubscribe_count"] += 1
        if state is not None:
            state.mark_not_ready("resubscribe")
            self.counters["state_reset_count"] += 1

    def on_snapshot_loaded(self) -> None:
        self.counters["snapshot_loaded_count"] += 1

    def on_snapshot_refresh(self) -> None:
        self.counters["snapshot_refresh_count"] += 1

    def on_snapshot_failed(self) -> None:
        self.counters["snapshot_failed_count"] += 1

    def on_delta_before_snapshot(self) -> None:
        self.counters["delta_before_snapshot_count"] += 1
        self.counters["messages_before_ready_count"] += 1

    def on_message_before_ready(self) -> None:
        self.counters["messages_before_ready_count"] += 1

    def on_sequence_gap(self) -> None:
        self.counters["sequence_gap_count"] += 1
        self.counters["state_reset_count"] += 1

    def on_duplicate(self) -> None:
        self.counters["duplicate_messages_detected"] += 1
        self.counters["duplicate_messages_skipped"] += 1

    def on_queue_backpressure(self, count: int = 1) -> None:
        self.counters["queue_backpressure_events"] += count

    def on_queue_dropped(self, count: int = 1) -> None:
        self.counters["queue_dropped_messages"] += count

    def on_sample_blocked_by_ready_guard(self) -> None:
        self.counters["sample_blocked_by_ready_guard"] += 1

    def observe_ready_false(self, state: OrderbookState) -> None:
        if state.ready_to_emit_false_duration_ms_max > self.ready_to_emit_false_duration_ms_max:
            self.ready_to_emit_false_duration_ms_max = (
                state.ready_to_emit_false_duration_ms_max
            )
        previous = self.symbol_ready_false_warnings.get(state.symbol, 0)
        if state.ready_to_emit_false_warning_count > previous:
            delta = state.ready_to_emit_false_warning_count - previous
            self.counters["ready_to_emit_false_warning_count"] += delta
            self.symbol_ready_false_warnings[state.symbol] = (
                state.ready_to_emit_false_warning_count
            )

    def report(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            field_name: int(self.counters[field_name])
            for field_name in LIFECYCLE_COUNTER_FIELDS
        }
        data.update(
            {
                "ready_to_emit_false_duration_ms_max": self.ready_to_emit_false_duration_ms_max,
                "market_status_known": False,
                "market_status_mode": "not_applicable_for_binance_spot_orderbook",
                "market_paused_count": 0,
                "market_unpaused_count": 0,
                "market_resolved_count": 0,
            }
        )
        return data
