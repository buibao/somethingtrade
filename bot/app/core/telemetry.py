from contextlib import asynccontextmanager
from uuid import uuid4

from app.core.clock import monotonic_now_ns
from app.core.events import LatencyTrace


@asynccontextmanager
async def latency_trace(source_event_id: str | None = None):
    """Measure an async block and return a LatencyTrace-compatible timing pair."""

    trace_id = uuid4().hex
    recv_ns = monotonic_now_ns()
    try:
        yield trace_id, recv_ns
    finally:
        pass


def build_latency_trace(
    *,
    trace_id: str,
    recv_ns: int,
    source_event_id: str | None = None,
    execution_ns: int | None = None,
) -> LatencyTrace:
    return LatencyTrace(
        trace_id=trace_id,
        source_event_id=source_event_id,
        recv_ns=recv_ns,
        execution_ns=execution_ns,
    )
