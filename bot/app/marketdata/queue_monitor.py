from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class QueueEnvelope:
    payload: Any
    enqueue_monotonic_ns: int
    queue_size_at_enqueue: int


class QueueBackpressureMonitor:
    def __init__(
        self,
        *,
        capacity: int | None = None,
        backpressure_size_ratio: float = 0.80,
        severe_lag_ms: float = 250.0,
        processing_lag_severe_ms: float = 50.0,
        snapshot_blocking_lag_ms: float = 250.0,
        sample_capacity: int = 4096,
    ) -> None:
        self.capacity = capacity
        self.backpressure_size_ratio = backpressure_size_ratio
        self.severe_lag_ms = severe_lag_ms
        self.processing_lag_severe_ms = processing_lag_severe_ms
        self.snapshot_blocking_lag_ms = snapshot_blocking_lag_ms
        self.sample_capacity = sample_capacity
        self.queue_current_size = 0
        self.queue_max_size = 0
        self.queue_dropped_messages = 0
        self.queue_backpressure_events = 0
        self.queue_size_backpressure_events = 0
        self.queue_lag_backpressure_events = 0
        self.processing_lag_backpressure_events = 0
        self.snapshot_blocking_lag_events = 0
        self.enqueue_to_dequeue_lag_ms_max = 0.0
        self.max_processing_lag_ms = 0.0
        self._lag_samples: deque[float] = deque(maxlen=sample_capacity)
        self._processing_lag_samples: deque[float] = deque(maxlen=sample_capacity)
        self._snapshot_request_duration_samples: deque[float] = deque(maxlen=sample_capacity)
        self._snapshot_apply_duration_samples: deque[float] = deque(maxlen=sample_capacity)

    def record_enqueue(
        self,
        payload: Any,
        *,
        enqueue_monotonic_ns: int,
        queue_size: int,
    ) -> QueueEnvelope:
        self.queue_current_size = queue_size
        self.queue_max_size = max(self.queue_max_size, queue_size)
        if self._is_backpressured(queue_size):
            self.queue_size_backpressure_events += 1
            self._refresh_backpressure_total()
        return QueueEnvelope(
            payload=payload,
            enqueue_monotonic_ns=enqueue_monotonic_ns,
            queue_size_at_enqueue=queue_size,
        )

    def record_dequeue(
        self,
        envelope: QueueEnvelope,
        *,
        dequeue_monotonic_ns: int,
        processing_done_monotonic_ns: int | None = None,
        queue_size: int | None = None,
    ) -> float:
        if queue_size is not None:
            self.queue_current_size = queue_size
        lag_ms = (
            dequeue_monotonic_ns - envelope.enqueue_monotonic_ns
        ) / 1_000_000.0
        self.enqueue_to_dequeue_lag_ms_max = max(
            self.enqueue_to_dequeue_lag_ms_max,
            lag_ms,
        )
        self._lag_samples.append(lag_ms)
        if processing_done_monotonic_ns is not None:
            self.record_processing_done(
                dequeue_monotonic_ns=dequeue_monotonic_ns,
                processing_done_monotonic_ns=processing_done_monotonic_ns,
            )
        if lag_ms > self.severe_lag_ms:
            self.queue_lag_backpressure_events += 1
            self._refresh_backpressure_total()
        return lag_ms

    def record_processing_done(
        self,
        *,
        dequeue_monotonic_ns: int,
        processing_done_monotonic_ns: int,
    ) -> float:
        processing_lag_ms = (
            processing_done_monotonic_ns - dequeue_monotonic_ns
        ) / 1_000_000.0
        self.max_processing_lag_ms = max(
            self.max_processing_lag_ms,
            processing_lag_ms,
        )
        self._processing_lag_samples.append(processing_lag_ms)
        if processing_lag_ms > self.processing_lag_severe_ms:
            self.processing_lag_backpressure_events += 1
            self._refresh_backpressure_total()
        return processing_lag_ms

    def record_snapshot_request_duration(self, duration_ms: float) -> None:
        self._snapshot_request_duration_samples.append(duration_ms)
        if duration_ms > self.snapshot_blocking_lag_ms:
            self.snapshot_blocking_lag_events += 1
            self._refresh_backpressure_total()

    def record_snapshot_apply_duration(self, duration_ms: float) -> None:
        self._snapshot_apply_duration_samples.append(duration_ms)

    def record_snapshot_blocking_lag(self, count: int = 1) -> None:
        self.snapshot_blocking_lag_events += count
        self._refresh_backpressure_total()

    def record_drop(self, count: int = 1) -> None:
        self.queue_dropped_messages += count
        self.queue_size_backpressure_events += count
        self._refresh_backpressure_total()

    def report(self) -> dict[str, Any]:
        samples = sorted(self._lag_samples)
        processing_samples = sorted(self._processing_lag_samples)
        snapshot_request_samples = sorted(self._snapshot_request_duration_samples)
        snapshot_apply_samples = sorted(self._snapshot_apply_duration_samples)
        self._refresh_backpressure_total()
        return {
            "queue_current_size": self.queue_current_size,
            "queue_max_size": self.queue_max_size,
            "queue_capacity": self.capacity,
            "queue_dropped_messages": self.queue_dropped_messages,
            "queue_backpressure_events": self.queue_backpressure_events,
            "queue_size_backpressure_events": self.queue_size_backpressure_events,
            "queue_lag_backpressure_events": self.queue_lag_backpressure_events,
            "processing_lag_backpressure_events": self.processing_lag_backpressure_events,
            "snapshot_blocking_lag_events": self.snapshot_blocking_lag_events,
            "enqueue_to_dequeue_lag_ms_max": self.enqueue_to_dequeue_lag_ms_max,
            "enqueue_to_dequeue_lag_ms_p50": _percentile(samples, 0.50),
            "enqueue_to_dequeue_lag_ms_p95": _percentile(samples, 0.95),
            "enqueue_to_dequeue_lag_ms_p99": _percentile(samples, 0.99),
            "enqueue_to_dequeue_lag_p50_ms": _percentile(samples, 0.50),
            "enqueue_to_dequeue_lag_p95_ms": _percentile(samples, 0.95),
            "enqueue_to_dequeue_lag_p99_ms": _percentile(samples, 0.99),
            "max_processing_lag_ms": self.max_processing_lag_ms,
            "processing_lag_p50_ms": _percentile(processing_samples, 0.50),
            "processing_lag_p95_ms": _percentile(processing_samples, 0.95),
            "processing_lag_p99_ms": _percentile(processing_samples, 0.99),
            "snapshot_request_duration_p50_ms": _percentile(snapshot_request_samples, 0.50),
            "snapshot_request_duration_p95_ms": _percentile(snapshot_request_samples, 0.95),
            "snapshot_request_duration_p99_ms": _percentile(snapshot_request_samples, 0.99),
            "snapshot_apply_duration_p50_ms": _percentile(snapshot_apply_samples, 0.50),
            "snapshot_apply_duration_p95_ms": _percentile(snapshot_apply_samples, 0.95),
            "snapshot_apply_duration_p99_ms": _percentile(snapshot_apply_samples, 0.99),
        }

    def _is_backpressured(self, queue_size: int) -> bool:
        if self.capacity is None or self.capacity <= 0:
            return False
        return queue_size >= int(self.capacity * self.backpressure_size_ratio)

    def _refresh_backpressure_total(self) -> None:
        self.queue_backpressure_events = (
            self.queue_size_backpressure_events
            + self.queue_lag_backpressure_events
            + self.processing_lag_backpressure_events
            + self.snapshot_blocking_lag_events
        )


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    index = min(len(values) - 1, max(0, round((len(values) - 1) * pct)))
    return values[index]
