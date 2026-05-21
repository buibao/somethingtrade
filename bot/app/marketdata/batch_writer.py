from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import json
from pathlib import Path
import queue
import threading
import time
from typing import Any


@dataclass(frozen=True, slots=True)
class WriterEnqueueResult:
    writer_enqueue_start_monotonic_ns: int
    writer_enqueue_end_monotonic_ns: int
    enqueued: bool
    dropped: bool
    file_write_start_monotonic_ns: int | None = None
    file_write_end_monotonic_ns: int | None = None


@dataclass(frozen=True, slots=True)
class _QueuedJsonlRecord:
    path: Path
    row: dict[str, Any]
    writer_enqueue_monotonic_ns: int


class JsonlBatchWriter:
    """Threaded JSONL writer that keeps filesystem writes off receive paths."""

    def __init__(
        self,
        *,
        batch_size: int = 512,
        flush_interval_ms: float = 100.0,
        queue_max_size: int = 65_536,
        sample_capacity: int = 4096,
    ) -> None:
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_ms = max(1.0, float(flush_interval_ms))
        self.queue_max_size = max(1, int(queue_max_size))
        self._queue: queue.Queue[_QueuedJsonlRecord] = queue.Queue(maxsize=self.queue_max_size)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="jsonl-batch-writer", daemon=True)
        self._started = False
        self._closed = False
        self._shutdown_flush_completed = False
        self._dropped_records = 0
        self._error_count = 0
        self._records_enqueued = 0
        self._records_written = 0
        self._flush_count = 0
        self._flush_durations_ms: deque[float] = deque(maxlen=sample_capacity)
        self._queue_depth_samples: deque[int] = deque(maxlen=sample_capacity)
        self._last_error: str | None = None

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()

    def enqueue_jsonl(self, path: str | Path, row: dict[str, Any]) -> WriterEnqueueResult:
        if not self._started:
            self.start()
        enqueue_start_ns = time.monotonic_ns()
        record = _QueuedJsonlRecord(
            path=Path(path),
            row=row,
            writer_enqueue_monotonic_ns=enqueue_start_ns,
        )
        self._annotate_writer_enqueue(row, enqueue_start_ns)
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            enqueue_end_ns = time.monotonic_ns()
            with self._lock:
                self._dropped_records += 1
                self._queue_depth_samples.append(self.queue_max_size)
            return WriterEnqueueResult(
                writer_enqueue_start_monotonic_ns=enqueue_start_ns,
                writer_enqueue_end_monotonic_ns=enqueue_end_ns,
                enqueued=False,
                dropped=True,
            )
        enqueue_end_ns = time.monotonic_ns()
        with self._lock:
            self._records_enqueued += 1
            self._queue_depth_samples.append(self._queue.qsize())
        return WriterEnqueueResult(
            writer_enqueue_start_monotonic_ns=enqueue_start_ns,
            writer_enqueue_end_monotonic_ns=enqueue_end_ns,
            enqueued=True,
            dropped=False,
        )

    def close(self, *, timeout_sec: float = 30.0) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._started:
            self._shutdown_flush_completed = True
            return
        self._stop.set()
        self._thread.join(timeout=max(0.1, timeout_sec))
        with self._lock:
            self._shutdown_flush_completed = (not self._thread.is_alive()) and self._queue.empty()

    def report(self) -> dict[str, Any]:
        with self._lock:
            flush_durations = list(self._flush_durations_ms)
            queue_depths = [float(value) for value in self._queue_depth_samples]
            return {
                "writer_mode": "threaded_jsonl_batch_writer",
                "writer_batch_size": self.batch_size,
                "writer_flush_interval_ms": self.flush_interval_ms,
                "writer_queue_max_size": self.queue_max_size,
                "writer_thread_or_task_count": 1 if self._started else 0,
                "writer_shutdown_flush_completed": self._shutdown_flush_completed,
                "writer_dropped_records": self._dropped_records,
                "writer_error_count": self._error_count,
                "writer_records_enqueued": self._records_enqueued,
                "writer_records_written": self._records_written,
                "writer_flush_count": self._flush_count,
                "writer_flush_p50_ms": _percentile(flush_durations, 0.50),
                "writer_flush_p95_ms": _percentile(flush_durations, 0.95),
                "writer_flush_p99_ms": _percentile(flush_durations, 0.99),
                "writer_flush_max_ms": max(flush_durations) if flush_durations else 0.0,
                "writer_queue_depth_p50": _percentile(queue_depths, 0.50),
                "writer_queue_depth_p95": _percentile(queue_depths, 0.95),
                "writer_queue_depth_p99": _percentile(queue_depths, 0.99),
                "writer_queue_depth_max": max(queue_depths) if queue_depths else 0.0,
                "writer_last_error": self._last_error,
            }

    def _run(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            batch = self._next_batch()
            if not batch:
                continue
            self._flush_batch(batch)

    def _next_batch(self) -> list[_QueuedJsonlRecord]:
        timeout = self.flush_interval_ms / 1000.0
        try:
            first = self._queue.get(timeout=timeout)
        except queue.Empty:
            return []
        batch = [first]
        while len(batch) < self.batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _flush_batch(self, batch: list[_QueuedJsonlRecord]) -> None:
        flush_start_ns = time.monotonic_ns()
        written = 0
        try:
            handles: dict[Path, Any] = {}
            try:
                for record in batch:
                    handle = handles.get(record.path)
                    if handle is None:
                        record.path.parent.mkdir(parents=True, exist_ok=True)
                        handle = record.path.open("a", encoding="utf-8", newline="\n")
                        handles[record.path] = handle
                    file_write_start_ns = time.monotonic_ns()
                    prepared = _with_file_write_timestamps(
                        record.row,
                        file_write_start_ns=file_write_start_ns,
                        file_write_end_ns=file_write_start_ns,
                    )
                    handle.write(_json_dumps(prepared) + "\n")
                    written += 1
                for handle in handles.values():
                    handle.flush()
            finally:
                for handle in handles.values():
                    handle.close()
        except Exception as exc:  # pragma: no cover - defensive runtime accounting
            with self._lock:
                self._error_count += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
        finally:
            for _record in batch:
                self._queue.task_done()
            flush_end_ns = time.monotonic_ns()
            with self._lock:
                self._records_written += written
                self._flush_count += 1
                self._flush_durations_ms.append((flush_end_ns - flush_start_ns) / 1_000_000.0)

    @staticmethod
    def _annotate_writer_enqueue(row: dict[str, Any], enqueue_ns: int) -> None:
        stages = row.get("stages")
        if isinstance(stages, dict):
            stages["writer_enqueue_monotonic_ns"] = enqueue_ns


def write_jsonl_sync(path: str | Path, row: dict[str, Any]) -> WriterEnqueueResult:
    start_ns = time.monotonic_ns()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    prepared = _with_file_write_timestamps(
        row,
        file_write_start_ns=start_ns,
        file_write_end_ns=start_ns,
    )
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(_json_dumps(prepared) + "\n")
    end_ns = time.monotonic_ns()
    return WriterEnqueueResult(
        writer_enqueue_start_monotonic_ns=start_ns,
        writer_enqueue_end_monotonic_ns=start_ns,
        enqueued=True,
        dropped=False,
        file_write_start_monotonic_ns=start_ns,
        file_write_end_monotonic_ns=end_ns,
    )


def _with_file_write_timestamps(
    row: dict[str, Any],
    *,
    file_write_start_ns: int,
    file_write_end_ns: int,
) -> dict[str, Any]:
    stages = row.get("stages")
    metrics = row.get("metrics")
    if isinstance(stages, dict):
        stages["file_write_start_monotonic_ns"] = file_write_start_ns
        stages["file_write_end_monotonic_ns"] = file_write_end_ns
    if isinstance(metrics, dict):
        metrics["file_write_duration_ms"] = (file_write_end_ns - file_write_start_ns) / 1_000_000.0
        writer_enqueue = stages.get("writer_enqueue_monotonic_ns") if isinstance(stages, dict) else None
        if isinstance(writer_enqueue, int) and not isinstance(writer_enqueue, bool):
            metrics["writer_wait_ms"] = (file_write_start_ns - writer_enqueue) / 1_000_000.0
    return row


def _json_dumps(payload: Any) -> str:
    return json.dumps(
        payload,
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _percentile(values: list[float], pct: float) -> float:
    clean = sorted(value for value in values if isinstance(value, (int, float)) and not isinstance(value, bool))
    if not clean:
        return 0.0
    if len(clean) == 1:
        return float(clean[0])
    index = min(len(clean) - 1, max(0, round((len(clean) - 1) * pct)))
    return float(clean[index])
