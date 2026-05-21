from __future__ import annotations

import asyncio
import json
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable

import aiohttp

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import DepthUpdate
from app.marketdata.batch_writer import JsonlBatchWriter, WriterEnqueueResult, write_jsonl_sync
from app.marketdata.binance_ws import BinanceWSClient
from app.marketdata.orderbook_quality import (
    OrderbookQualityResult,
    OrderbookQualityValidator,
)
from app.marketdata.orderbook_state import (
    OrderbookApplyResult,
    OrderbookSnapshot,
    OrderbookState,
)
from app.marketdata.queue_monitor import QueueBackpressureMonitor, QueueEnvelope
from app.marketdata.ws_lifecycle import WSLifecycleTracker

DEFAULT_ORDERBOOK_QUALITY_REPORT = Path("data/debug/orderbook_quality_report.json")
DEFAULT_ORDERBOOK_QUALITY_SAMPLES = Path("data/debug/orderbook_quality_samples.jsonl")
DEFAULT_ORDERBOOK_MISMATCH_CASES = Path("data/debug/orderbook_mismatch_cases.jsonl")
DEFAULT_BOOK_INCOMPLETE_CASES = Path("data/debug/book_incomplete_cases.jsonl")
DEFAULT_SEQUENCE_GAP_CASES = Path("data/debug/sequence_gap_cases.jsonl")
DEFAULT_DUPLICATE_UPDATE_CASES = Path("data/debug/duplicate_update_cases.jsonl")
DEFAULT_INVALID_DELTA_CASES = Path("data/debug/invalid_delta_cases.jsonl")
DEFAULT_STALE_PERIOD_CASES = Path("data/debug/stale_period_cases.jsonl")
DEFAULT_SEQUENCE_RECOVERY_TRACE = Path("data/debug/sequence_recovery_trace.jsonl")
DEFAULT_CLEAN_SAMPLE_SCHEMA_VIOLATION_CASES = Path("data/debug/clean_sample_schema_violation_cases.jsonl")
DEFAULT_WS_LIFECYCLE_REPORT = Path("data/debug/ws_lifecycle_report.json")
DEFAULT_ORDERBOOK_CLEAN_SAMPLES = Path("data/dataset/orderbook_clean_samples.jsonl")
DEFAULT_LATENCY_PROFILE_SAMPLES = Path("data/dataset/phase_4_2fg_latency_profile_samples.jsonl")
DEFAULT_ORDERBOOK_MARKDOWN_REPORT = Path("docs/reports/phase_4_1_orderbook_quality_report.md")
DEFAULT_PHASE411_REPORT_JSON = Path("data/reports/phase_4_1_orderbook_quality_report.json")
DEFAULT_PHASE411_REPORT_MD = Path("data/reports/phase_4_1_orderbook_quality_report.md")

PHASE_4_1_SCHEMA_VERSION = "phase_4_1_clean_orderbook_v1"
SNAPSHOT_COPY_BUDGET_US = 200.0
REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class OrderbookPhase41Paths:
    quality_report: Path = REPO_ROOT / DEFAULT_ORDERBOOK_QUALITY_REPORT
    quality_samples: Path = REPO_ROOT / DEFAULT_ORDERBOOK_QUALITY_SAMPLES
    mismatch_cases: Path = REPO_ROOT / DEFAULT_ORDERBOOK_MISMATCH_CASES
    book_incomplete_cases: Path = REPO_ROOT / DEFAULT_BOOK_INCOMPLETE_CASES
    sequence_gap_cases: Path = REPO_ROOT / DEFAULT_SEQUENCE_GAP_CASES
    duplicate_update_cases: Path = REPO_ROOT / DEFAULT_DUPLICATE_UPDATE_CASES
    invalid_delta_cases: Path = REPO_ROOT / DEFAULT_INVALID_DELTA_CASES
    stale_period_cases: Path = REPO_ROOT / DEFAULT_STALE_PERIOD_CASES
    sequence_recovery_trace: Path = REPO_ROOT / DEFAULT_SEQUENCE_RECOVERY_TRACE
    clean_sample_schema_violation_cases: Path = REPO_ROOT / DEFAULT_CLEAN_SAMPLE_SCHEMA_VIOLATION_CASES
    lifecycle_report: Path = REPO_ROOT / DEFAULT_WS_LIFECYCLE_REPORT
    clean_samples: Path = REPO_ROOT / DEFAULT_ORDERBOOK_CLEAN_SAMPLES
    latency_profile_samples: Path = REPO_ROOT / DEFAULT_LATENCY_PROFILE_SAMPLES
    markdown_report: Path = REPO_ROOT / DEFAULT_ORDERBOOK_MARKDOWN_REPORT


@dataclass(frozen=True, slots=True)
class PostSnapshotQueuePurgeResult:
    symbol: str
    snapshot_last_update_id: int
    queue_size_before: int
    old_events_dropped: int
    bridge_candidate_found: bool
    bridge_first_update_id: int | None
    bridge_final_update_id: int | None
    events_preserved_count: int
    queue_size_after: int
    bridge_missing_after_snapshot: bool = False
    future_events_dropped: int = 0

    def to_trace_event(
        self,
        *,
        generation: int,
        monotonic_ts_ns: int,
    ) -> dict[str, Any]:
        return {
            "event": "post_snapshot_queue_purge",
            "symbol": self.symbol,
            "snapshot_last_update_id": self.snapshot_last_update_id,
            "queue_size_before": self.queue_size_before,
            "old_events_dropped": self.old_events_dropped,
            "bridge_candidate_found": self.bridge_candidate_found,
            "bridge_first_update_id": self.bridge_first_update_id,
            "bridge_final_update_id": self.bridge_final_update_id,
            "events_preserved_count": self.events_preserved_count,
            "queue_size_after": self.queue_size_after,
            "bridge_missing_after_snapshot": self.bridge_missing_after_snapshot,
            "future_events_dropped": self.future_events_dropped,
            "generation": generation,
            "monotonic_ts_ns": monotonic_ts_ns,
        }


@dataclass(frozen=True, slots=True)
class QueuePurgeOutput:
    result: PostSnapshotQueuePurgeResult
    preserved: tuple[QueueEnvelope, ...]


class OrderbookDebugRecorder:
    def __init__(
        self,
        *,
        paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
        max_cases: int = 256,
        reset_files: bool = True,
        writer: JsonlBatchWriter | None = None,
        hot_path_decoupled: bool = False,
    ) -> None:
        self.paths = paths
        self.max_cases = max_cases
        self.writer = writer
        self.hot_path_decoupled = hot_path_decoupled
        self.quality_samples: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.mismatch_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.book_incomplete_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.sequence_gap_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.duplicate_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.invalid_delta_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.stale_period_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.sequence_recovery_trace: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.clean_sample_schema_violation_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        for path in (
            self.paths.quality_samples,
            self.paths.mismatch_cases,
            self.paths.book_incomplete_cases,
            self.paths.sequence_gap_cases,
            self.paths.duplicate_update_cases,
            self.paths.invalid_delta_cases,
            self.paths.stale_period_cases,
            self.paths.sequence_recovery_trace,
            self.paths.clean_sample_schema_violation_cases,
            self.paths.clean_samples,
            self.paths.latency_profile_samples,
        ):
            _ensure_file(path, reset=reset_files)

    def record_quality_sample(
        self,
        snapshot: OrderbookSnapshot,
        quality: OrderbookQualityResult,
    ) -> None:
        row = {
            "case_type": "quality_sample",
            "symbol": snapshot.symbol,
            "state_version": snapshot.state_version,
            "snapshot_version": snapshot.snapshot_version,
            "last_update_id": snapshot.last_update_id,
            "bid_count": snapshot.bid_count,
            "ask_count": snapshot.ask_count,
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "book_age_ms": quality.book_age_ms,
            "errors": quality.errors,
            "warnings": quality.warnings,
            "is_valid": quality.is_valid,
        }
        self.quality_samples.append(row)
        self._record_jsonl(self.paths.quality_samples, row)

    def record_mismatch_case(
        self,
        snapshot: OrderbookSnapshot,
        quality: OrderbookQualityResult,
        *,
        first_update_id: int | None = None,
        final_update_id: int | None = None,
        exchange_event_ts: int | None = None,
        raw_message_excerpt: str | None = None,
        queue_lag_ms: float | None = None,
    ) -> None:
        strict = quality.strict_mismatch_details
        tolerant = quality.tolerant_mismatch_details
        case_type = (
            "reported_best_bid_mismatch"
            if strict.get("bid_mismatch")
            else "reported_best_ask_mismatch"
        )
        row = {
            "case_type": case_type,
            "symbol": snapshot.symbol,
            "source": "binance_ws",
            "event_type": "depthUpdate",
            "update_type": "best_bid_ask",
            "local_recv_monotonic_ns": snapshot.local_recv_monotonic_ns,
            "local_recv_wall_ts": snapshot.local_recv_wall_ts,
            "exchange_event_ts": exchange_event_ts,
            "state_version": snapshot.state_version,
            "snapshot_version": snapshot.snapshot_version,
            "last_update_id": snapshot.last_update_id,
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "computed_best_bid": strict.get("computed_best_bid"),
            "computed_best_ask": strict.get("computed_best_ask"),
            "reported_best_bid": strict.get("reported_best_bid"),
            "reported_best_ask": strict.get("reported_best_ask"),
            "bid_diff": strict.get("bid_diff"),
            "ask_diff": strict.get("ask_diff"),
            "strict_mismatch": strict.get("strict_mismatch"),
            "tolerant_mismatch": tolerant.get("tolerant_mismatch"),
            "top_bids": snapshot.bids_top_n,
            "top_asks": snapshot.asks_top_n,
            "bid_count": snapshot.bid_count,
            "ask_count": snapshot.ask_count,
            "book_age_ms": quality.book_age_ms,
            "queue_lag_ms": queue_lag_ms,
            "lifecycle_flags": quality.lifecycle_flags,
            "raw_message_excerpt": raw_message_excerpt,
            "raw_message_hash": None,
        }
        self.mismatch_cases.append(row)
        self._record_jsonl(self.paths.mismatch_cases, row)

    def record_book_incomplete_case(
        self,
        snapshot: OrderbookSnapshot,
        quality: OrderbookQualityResult,
        *,
        reason: str,
    ) -> None:
        row = {
            "case_type": reason,
            "symbol": snapshot.symbol,
            "bid_count": snapshot.bid_count,
            "ask_count": snapshot.ask_count,
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "snapshot_ready": quality.lifecycle_flags.get("snapshot_ready"),
            "ready_to_emit": False,
            "book_age_ms": quality.book_age_ms,
            "state_version": snapshot.state_version,
            "snapshot_version": snapshot.snapshot_version,
            "last_update_id": snapshot.last_update_id,
            "reason": reason,
            "action_taken": "sample_blocked",
        }
        self.book_incomplete_cases.append(row)
        self._record_jsonl(self.paths.book_incomplete_cases, row)

    def record_sequence_gap_case(
        self,
        result: OrderbookApplyResult,
        *,
        symbol: str,
    ) -> None:
        row = {
            "case_type": "sequence_gap_or_reset",
            "symbol": symbol,
            "previous_last_update_id": result.previous_last_update_id,
            "expected_next_update_id": result.expected_next_update_id,
            "received_first_update_id": result.first_update_id,
            "received_final_update_id": result.final_update_id,
            "action_taken": "delta_dropped_snapshot_ready_false_fresh_snapshot_required",
            "snapshot_ready_before": result.snapshot_ready_before,
            "snapshot_ready_after": result.snapshot_ready_after,
            "ready_to_emit_after": result.ready_to_emit_after,
            "state_version_before": result.state_version_before,
            "state_version_after": result.state_version_after,
        }
        self.sequence_gap_cases.append(row)
        self._record_jsonl(self.paths.sequence_gap_cases, row)

    def record_duplicate_case(
        self,
        result: OrderbookApplyResult,
        *,
        symbol: str,
    ) -> None:
        row = {
            "case_type": "duplicate_update",
            "symbol": symbol,
            "previous_last_update_id": result.previous_last_update_id,
            "received_first_update_id": result.first_update_id,
            "received_final_update_id": result.final_update_id,
            "action_taken": "duplicate_old_update_skipped",
            "state_version_before": result.state_version_before,
            "state_version_after": result.state_version_after,
        }
        self.duplicate_cases.append(row)
        self._record_jsonl(self.paths.duplicate_update_cases, row)

    def record_invalid_delta_case(
        self,
        result: OrderbookApplyResult,
        *,
        symbol: str,
        snapshot: OrderbookSnapshot,
        local_recv_monotonic_ns: int,
        raw_message_excerpt: str | None = None,
    ) -> None:
        row = {
            "case_type": "invalid_delta_fail_closed",
            "symbol": symbol,
            "local_recv_monotonic_ns": local_recv_monotonic_ns,
            "first_update_id": result.first_update_id,
            "final_update_id": result.final_update_id,
            "previous_last_update_id": result.previous_last_update_id,
            "snapshot_ready_before": result.snapshot_ready_before,
            "snapshot_ready_after": result.snapshot_ready_after,
            "ready_to_emit_after": result.ready_to_emit_after,
            "rejection_reason": result.errors,
            "raw_message_excerpt": raw_message_excerpt,
            "raw_message_hash": None,
            "top_bids_before_rejection": snapshot.bids_top_n[:5],
            "top_asks_before_rejection": snapshot.asks_top_n[:5],
            "state_version_before": result.state_version_before,
            "state_version_after": result.state_version_after,
            "generation_id": result.generation_after,
        }
        self.invalid_delta_cases.append(row)
        self._record_jsonl(self.paths.invalid_delta_cases, row)

    def record_stale_period_case(self, period: dict[str, Any]) -> None:
        self.stale_period_cases.append(period)
        self._record_jsonl(self.paths.stale_period_cases, period)

    def record_sequence_trace_event(self, event: dict[str, Any]) -> None:
        self.sequence_recovery_trace.append(event)
        self._record_jsonl(self.paths.sequence_recovery_trace, event)

    def record_clean_sample(
        self,
        sample: dict[str, Any],
    ) -> WriterEnqueueResult:
        return self._record_jsonl(self.paths.clean_samples, sample)

    def record_latency_profile_sample(self, row: dict[str, Any]) -> None:
        self._record_jsonl(self.paths.latency_profile_samples, row)

    def record_clean_sample_schema_violation_case(
        self,
        row: dict[str, Any],
    ) -> None:
        self.clean_sample_schema_violation_cases.append(row)
        self._record_jsonl(self.paths.clean_sample_schema_violation_cases, row)

    def _record_jsonl(self, path: Path, row: dict[str, Any]) -> WriterEnqueueResult:
        if self.writer is not None:
            return self.writer.enqueue_jsonl(path, row)
        return write_jsonl_sync(path, row)


class OrderbookPhase41Processor:
    def __init__(
        self,
        *,
        symbols: Iterable[str],
        paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
        depth_n: int = 20,
        stale_after_ms: float = 1_000.0,
        feed_receive_stale_after_ms: float | None = None,
        queue_lag_warning_ms: float = 50.0,
        queue_lag_severe_ms: float = 250.0,
        debug_max_cases: int = 256,
        monotonic_clock: Callable[[], int] = monotonic_now_ns,
        batch_writer_enabled: bool = False,
        writer_batch_size: int = 512,
        writer_flush_interval_ms: float = 100.0,
        writer_queue_max_size: int = 65_536,
    ) -> None:
        self.states = {symbol.upper(): OrderbookState(symbol) for symbol in symbols}
        self.paths = paths
        self.depth_n = depth_n
        self.monotonic_clock = monotonic_clock
        self.stale_threshold_ms = stale_after_ms
        self.feed_receive_stale_threshold_ms = (
            stale_after_ms
            if feed_receive_stale_after_ms is None
            else feed_receive_stale_after_ms
        )
        self.validator = OrderbookQualityValidator(
            stale_after_ms=stale_after_ms,
            queue_lag_warning_ms=queue_lag_warning_ms,
            queue_lag_severe_ms=queue_lag_severe_ms,
        )
        self.lifecycle = WSLifecycleTracker()
        self.queue_monitor = QueueBackpressureMonitor(
            capacity=1024,
            severe_lag_ms=queue_lag_severe_ms,
        )
        self.batch_writer_enabled = batch_writer_enabled
        self.writer = (
            JsonlBatchWriter(
                batch_size=writer_batch_size,
                flush_interval_ms=writer_flush_interval_ms,
                queue_max_size=writer_queue_max_size,
            )
            if batch_writer_enabled
            else None
        )
        if self.writer is not None:
            self.writer.start()
        self.debug = OrderbookDebugRecorder(
            paths=paths,
            max_cases=debug_max_cases,
            writer=self.writer,
            hot_path_decoupled=batch_writer_enabled,
        )
        self.counters: Counter[str] = Counter()
        self.blocked_by_quality_error: Counter[str] = Counter()
        self.snapshot_copy_us_samples: deque[float] = deque(maxlen=4096)
        self.stale_periods: deque[dict[str, Any]] = deque(maxlen=256)
        self.processor_apply_stale_warnings: deque[dict[str, Any]] = deque(maxlen=256)
        self.post_capture_age_warnings: deque[dict[str, Any]] = deque(maxlen=256)
        self._open_stale_periods: dict[str, dict[str, Any]] = {}
        self._post_capture_warning_symbols: set[str] = set()
        self._post_snapshot_trace_counts: Counter[str] = Counter()
        self._updates_processed_since_snapshot: Counter[str] = Counter()
        self._last_snapshot_last_update_id: dict[str, int] = {}
        self._last_ws_message_recv_monotonic_ns: dict[str, int] = {}
        self._last_delta_dequeued_monotonic_ns: dict[str, int] = {}
        self._open_feed_receive_stale_symbols: set[str] = set()
        self._open_processor_apply_stale_symbols: set[str] = set()
        self._processor_apply_stale_consecutive_checks: Counter[str] = Counter()
        self._snapshot_recovery_active_symbols: set[str] = set()
        self.last_snapshot_load_start_monotonic_ns: dict[str, int] = {}
        self.last_snapshot_load_end_monotonic_ns: dict[str, int] = {}
        self.capture_active = True
        self.shutdown_started = False
        self.max_book_age_ms = 0.0
        self.last_book_update_age_ms_at_report: float | None = None

    def state_for(self, symbol: str) -> OrderbookState:
        key = symbol.upper()
        if key not in self.states:
            self.states[key] = OrderbookState(key)
        return self.states[key]

    def load_snapshot(
        self,
        symbol: str,
        *,
        bids: Iterable[Any],
        asks: Iterable[Any],
        last_update_id: int,
        local_recv_monotonic_ns: int | None = None,
        generation: int | None = None,
        snapshot_request_start_monotonic_ns: int | None = None,
        snapshot_response_monotonic_ns: int | None = None,
        snapshot_apply_monotonic_ns: int | None = None,
        queue_size_before_snapshot: int = 0,
        queue_size_after_snapshot: int = 0,
        recovery: bool = False,
    ) -> OrderbookApplyResult:
        state = self.state_for(symbol)
        apply_ns = (
            snapshot_apply_monotonic_ns
            if snapshot_apply_monotonic_ns is not None
            else local_recv_monotonic_ns
            if local_recv_monotonic_ns is not None
            else self.monotonic_clock()
        )
        result = state.apply_snapshot(
            bids=bids,
            asks=asks,
            last_update_id=last_update_id,
            local_recv_monotonic_ns=apply_ns,
            local_recv_wall_ts=_utc_iso_now(),
            generation=generation,
        )
        if result.accepted:
            self.lifecycle.on_snapshot_loaded()
            self._close_stale_period(symbol.upper(), now_monotonic_ns=state.last_book_update_monotonic_ns)
            self._post_snapshot_trace_counts[symbol.upper()] = 0
            self._updates_processed_since_snapshot[symbol.upper()] = 0
            self._last_snapshot_last_update_id[symbol.upper()] = int(last_update_id)
            self.debug.record_sequence_trace_event(
                {
                    "event": "recovery_snapshot_loaded" if recovery else "snapshot_loaded",
                    "symbol": symbol.upper(),
                    "generation_id": state.generation,
                    "snapshot_last_update_id": int(last_update_id),
                    "snapshot_request_start_monotonic_ns": snapshot_request_start_monotonic_ns,
                    "snapshot_response_monotonic_ns": snapshot_response_monotonic_ns,
                    "snapshot_apply_monotonic_ns": apply_ns,
                    "queue_size_before_snapshot": queue_size_before_snapshot,
                    "queue_size_after_snapshot": queue_size_after_snapshot,
                    "ready_to_emit_after": state.ready_to_emit,
                    "snapshot_ready_after": state.snapshot_ready,
                }
            )
        else:
            self.lifecycle.on_snapshot_failed()
        self.lifecycle.observe_ready_false(state)
        return result

    def process_depth_update(
        self,
        event: DepthUpdate,
        *,
        raw_message_excerpt: str | None = None,
        queue_lag_ms: float | None = None,
        queue_put_monotonic_ns: int | None = None,
        queue_put_end_monotonic_ns: int | None = None,
        queue_dequeue_monotonic_ns: int | None = None,
        queue_size_at_enqueue: int | None = None,
    ) -> OrderbookApplyResult:
        self.counters["messages_received"] += 1
        self.counters["messages_parsed"] += 1
        state = self.state_for(event.symbol)
        recv_monotonic_ns = event.recv_monotonic_ns or self.monotonic_clock()
        self.record_ws_message_recv(event.symbol, recv_monotonic_ns)
        state.record_message_recv(recv_monotonic_ns)
        self.check_stale_periods(
            now_monotonic_ns=recv_monotonic_ns,
            symbols=(event.symbol,),
            feed_active=True,
        )
        symbol = event.symbol.upper()
        ready_before = state.ready_to_emit
        snapshot_ready_before = state.snapshot_ready
        generation_before = state.generation
        awaiting_bridge_before = state.awaiting_first_delta_after_snapshot
        updates_since_snapshot_before = int(
            self._updates_processed_since_snapshot[symbol]
        )
        queue_size_at_process = (
            self.queue_monitor.queue_current_size
            if queue_lag_ms is not None
            else 0
        )
        book_apply_start_ns = self.monotonic_clock()
        result = state.apply_delta(
            first_update_id=event.first_update_id,
            final_update_id=event.final_update_id,
            previous_final_update_id=event.previous_final_update_id,
            bids=[(level.price, level.size) for level in event.bids],
            asks=[(level.price, level.size) for level in event.asks],
            local_recv_monotonic_ns=recv_monotonic_ns,
        )
        book_apply_end_ns = self.monotonic_clock()
        if result.accepted:
            self.counters["deltas_accepted"] += 1
            self._close_stale_period(event.symbol, now_monotonic_ns=recv_monotonic_ns)
            snapshot = self.copy_snapshot(state)
            validation_now_ns = max(self.monotonic_clock(), recv_monotonic_ns)
            quality = self.validator.validate(
                snapshot,
                state=state,
                now_monotonic_ns=validation_now_ns,
                queue_lag_ms=queue_lag_ms,
            )
            self.debug.record_quality_sample(snapshot, quality)
            self._maybe_emit_or_block_sample(
                snapshot,
                quality,
                exchange_event_ts=event.exchange_event_ts,
                event=event,
                book_apply_start_monotonic_ns=book_apply_start_ns,
                book_apply_end_monotonic_ns=book_apply_end_ns,
                queue_put_monotonic_ns=queue_put_monotonic_ns,
                queue_put_end_monotonic_ns=queue_put_end_monotonic_ns,
                queue_dequeue_monotonic_ns=queue_dequeue_monotonic_ns,
                queue_wait_ms=queue_lag_ms,
                queue_size_at_enqueue=queue_size_at_enqueue,
            )
        else:
            self.counters["deltas_rejected"] += 1
            if result.status == "delta_before_snapshot":
                self.lifecycle.on_delta_before_snapshot()
            elif result.status == "duplicate_update":
                self.lifecycle.on_duplicate()
                self.debug.record_duplicate_case(result, symbol=event.symbol)
            elif result.status in {"sequence_gap_or_reset", "sequence_bridge_failed"}:
                if result.status == "sequence_bridge_failed":
                    self.counters["first_delta_bridge_failed_count"] += 1
                self.lifecycle.on_sequence_gap()
                self.debug.record_sequence_gap_case(result, symbol=event.symbol)
            elif result.status == "previous_final_update_id_mismatch":
                self.counters["previous_final_update_id_mismatch_count"] += 1
                self.counters["ready_to_emit_disabled_count"] += 1
                self.debug.record_sequence_gap_case(result, symbol=event.symbol)
            elif result.status == "invalid_delta_levels":
                self.counters["invalid_delta_count"] += 1
                self.counters["ready_to_emit_disabled_count"] += 1
                snapshot = self.copy_snapshot(state)
                self.debug.record_invalid_delta_case(
                    result,
                    symbol=event.symbol,
                    snapshot=snapshot,
                    local_recv_monotonic_ns=recv_monotonic_ns,
                    raw_message_excerpt=raw_message_excerpt,
                )
            else:
                self.lifecycle.on_message_before_ready()
        classification = _sequence_trace_classification(
            result,
            awaiting_bridge_before=awaiting_bridge_before,
        )
        self._record_post_snapshot_update_range(
            event,
            result=result,
            generation_id=generation_before,
            classification=classification,
            ready_to_emit_before=ready_before,
            snapshot_ready_before=snapshot_ready_before,
            queue_size_at_process=queue_size_at_process,
            local_recv_monotonic_ns=recv_monotonic_ns,
            apply_monotonic_ns=self.monotonic_clock(),
        )
        if result.status in {
            "sequence_gap_or_reset",
            "sequence_bridge_failed",
            "previous_final_update_id_mismatch",
        }:
            self._record_sequence_gap_trace(
                event,
                result=result,
                generation_id=generation_before,
                ready_to_emit_before=ready_before,
                snapshot_ready_before=snapshot_ready_before,
                queue_size_at_gap=queue_size_at_process,
                updates_processed_since_snapshot=updates_since_snapshot_before,
                local_recv_monotonic_ns=recv_monotonic_ns,
            )
        if result.accepted and (not ready_before) and state.ready_to_emit:
            self.debug.record_sequence_trace_event(
                {
                    "event": "recovery_ready_restored",
                    "symbol": symbol,
                    "generation_id": state.generation,
                    "last_update_id": state.last_update_id,
                    "ready_to_emit_after": state.ready_to_emit,
                    "snapshot_ready_after": state.snapshot_ready,
                    "updates_processed_since_snapshot": int(
                        self._updates_processed_since_snapshot[symbol]
                    ),
                    "local_recv_monotonic_ns": recv_monotonic_ns,
                }
            )
        self._updates_processed_since_snapshot[symbol] += 1
        if not state.ready_to_emit:
            self.lifecycle.observe_ready_false(state)
        return result

    def validate_reported_best(
        self,
        symbol: str,
        *,
        reported_best_bid: Decimal | str | float | None,
        reported_best_ask: Decimal | str | float | None,
        first_update_id: int | None = None,
        final_update_id: int | None = None,
        exchange_event_ts: int | None = None,
        raw_message_excerpt: str | None = None,
    ) -> OrderbookQualityResult:
        state = self.state_for(symbol)
        snapshot = self.copy_snapshot(state)
        quality = self.validator.validate(
            snapshot,
            state=state,
            now_monotonic_ns=self.monotonic_clock(),
            reported_best_bid=reported_best_bid,
            reported_best_ask=reported_best_ask,
        )
        if quality.strict_mismatch_details.get("strict_mismatch"):
            self.debug.record_mismatch_case(
                snapshot,
                quality,
                first_update_id=first_update_id,
                final_update_id=final_update_id,
                exchange_event_ts=exchange_event_ts,
                raw_message_excerpt=raw_message_excerpt,
            )
        return quality

    def copy_snapshot(self, state: OrderbookState) -> OrderbookSnapshot:
        start = time.perf_counter_ns()
        now_ns = self.monotonic_clock()
        if state.last_local_recv_monotonic_ns is not None:
            now_ns = max(now_ns, state.last_local_recv_monotonic_ns)
        snapshot = state.copy_snapshot(
            top_n=self.depth_n,
            local_recv_monotonic_ns=now_ns,
            local_recv_wall_ts=_utc_iso_now(),
        )
        elapsed_us = (time.perf_counter_ns() - start) / 1_000.0
        self.snapshot_copy_us_samples.append(elapsed_us)
        return snapshot

    def cleanup_symbol(self, symbol: str) -> None:
        state = self.states.pop(symbol.upper(), None)
        if state is not None:
            state.cleanup()
        self._open_stale_periods.pop(symbol.upper(), None)

    def record_ws_message_recv(self, symbol: str, monotonic_ns: int) -> None:
        self._last_ws_message_recv_monotonic_ns[symbol.upper()] = monotonic_ns

    def record_delta_dequeued(self, symbol: str, monotonic_ns: int) -> None:
        self._last_delta_dequeued_monotonic_ns[symbol.upper()] = monotonic_ns

    def mark_snapshot_recovery_active(
        self,
        symbol: str,
        *,
        active: bool,
        monotonic_ns: int,
    ) -> None:
        key = symbol.upper()
        if active:
            self._snapshot_recovery_active_symbols.add(key)
            self.last_snapshot_load_start_monotonic_ns[key] = monotonic_ns
        else:
            self._snapshot_recovery_active_symbols.discard(key)
            self.last_snapshot_load_end_monotonic_ns[key] = monotonic_ns

    def set_capture_active(self, active: bool) -> None:
        self.capture_active = active
        if not active:
            self.shutdown_started = True

    def _record_post_snapshot_update_range(
        self,
        event: DepthUpdate,
        *,
        result: OrderbookApplyResult,
        generation_id: int,
        classification: str,
        ready_to_emit_before: bool,
        snapshot_ready_before: bool,
        queue_size_at_process: int,
        local_recv_monotonic_ns: int,
        apply_monotonic_ns: int,
    ) -> None:
        symbol = event.symbol.upper()
        index = int(self._post_snapshot_trace_counts[symbol]) + 1
        if index > 20:
            return
        state = self.state_for(symbol)
        self._post_snapshot_trace_counts[symbol] = index
        self.debug.record_sequence_trace_event(
            {
                "event": "post_snapshot_update_range",
                "symbol": symbol,
                "generation_id": generation_id,
                "index_after_snapshot": index,
                "U": event.first_update_id,
                "u": event.final_update_id,
                "previous_last_update_id": result.previous_last_update_id,
                "classification": classification,
                "ready_to_emit_before": ready_to_emit_before,
                "ready_to_emit_after": state.ready_to_emit,
                "snapshot_ready_before": snapshot_ready_before,
                "snapshot_ready_after": state.snapshot_ready,
                "queue_size_at_process": queue_size_at_process,
                "local_recv_monotonic_ns": local_recv_monotonic_ns,
                "apply_monotonic_ns": apply_monotonic_ns,
            }
        )

    def _record_sequence_gap_trace(
        self,
        event: DepthUpdate,
        *,
        result: OrderbookApplyResult,
        generation_id: int,
        ready_to_emit_before: bool,
        snapshot_ready_before: bool,
        queue_size_at_gap: int,
        updates_processed_since_snapshot: int,
        local_recv_monotonic_ns: int,
    ) -> None:
        state = self.state_for(event.symbol)
        expected = result.expected_next_update_id
        gap_size = (
            None
            if expected is None
            else max(0, event.first_update_id - expected)
        )
        self.debug.record_sequence_trace_event(
            {
                "event": "sequence_gap_detected",
                "symbol": event.symbol.upper(),
                "generation_id": generation_id,
                "previous_last_update_id": result.previous_last_update_id,
                "expected_next_update_id": expected,
                "received_first_update_id": event.first_update_id,
                "received_final_update_id": event.final_update_id,
                "gap_size": gap_size,
                "queue_size_at_gap": queue_size_at_gap,
                "ready_to_emit_before": ready_to_emit_before,
                "ready_to_emit_after": state.ready_to_emit,
                "snapshot_ready_before": snapshot_ready_before,
                "snapshot_ready_after": state.snapshot_ready,
                "last_snapshot_last_update_id": self._last_snapshot_last_update_id.get(event.symbol.upper()),
                "updates_processed_since_snapshot": updates_processed_since_snapshot,
                "local_recv_monotonic_ns": local_recv_monotonic_ns,
            }
        )

    def check_stale_periods(
        self,
        *,
        now_monotonic_ns: int | None = None,
        symbols: Iterable[str] | None = None,
        feed_active: bool | None = None,
        queue_size: int = 0,
    ) -> list[dict[str, Any]]:
        now_ns = now_monotonic_ns if now_monotonic_ns is not None else self.monotonic_clock()
        stale_periods: list[dict[str, Any]] = []
        active = self.capture_active if feed_active is None else feed_active
        selected_symbols = tuple(symbol.upper() for symbol in symbols) if symbols else tuple(self.states)
        for symbol in selected_symbols:
            state = self.states.get(symbol)
            if state is None:
                continue
            apply_age_ms = state.book_age_ms(now_ns)
            feed_age_ms = self._feed_receive_age_ms(symbol, now_ns)
            if apply_age_ms is not None:
                self.max_book_age_ms = max(self.max_book_age_ms, apply_age_ms)
                self.last_book_update_age_ms_at_report = apply_age_ms
            if not active:
                if (
                    apply_age_ms is not None
                    and apply_age_ms > self.stale_threshold_ms
                    and symbol not in self._open_stale_periods
                ):
                    self._record_post_capture_age_warning(
                        symbol=symbol,
                        state=state,
                        now_monotonic_ns=now_ns,
                        age_ms=apply_age_ms,
                    )
                continue

            if symbol in self._snapshot_recovery_active_symbols:
                if queue_size > 0:
                    self.queue_monitor.record_snapshot_blocking_lag()
                continue

            feed_stale = (
                feed_age_ms is not None
                and feed_age_ms > self.feed_receive_stale_threshold_ms
                and queue_size == 0
            )
            if feed_stale:
                assert feed_age_ms is not None
                period = self._record_feed_receive_stale(
                    symbol=symbol,
                    state=state,
                    now_monotonic_ns=now_ns,
                    feed_age_ms=feed_age_ms,
                )
                stale_periods.append(period)
                continue
            self._close_stale_period(symbol, now_monotonic_ns=now_ns)

            processor_apply_stale = (
                apply_age_ms is not None
                and apply_age_ms > self.stale_threshold_ms
                and (
                    queue_size > 0
                    or (
                        feed_age_ms is not None
                        and feed_age_ms <= self.stale_threshold_ms
                    )
                )
            )
            if processor_apply_stale:
                assert apply_age_ms is not None
                self._record_processor_apply_stale(
                    symbol=symbol,
                    state=state,
                    now_monotonic_ns=now_ns,
                    apply_age_ms=apply_age_ms,
                    feed_age_ms=feed_age_ms,
                    queue_size=queue_size,
                )
            else:
                self._open_processor_apply_stale_symbols.discard(symbol)
                self._processor_apply_stale_consecutive_checks[symbol] = 0
        return stale_periods

    def _feed_receive_age_ms(self, symbol: str, now_monotonic_ns: int) -> float | None:
        last_recv = self._last_ws_message_recv_monotonic_ns.get(symbol.upper())
        if last_recv is None:
            return None
        return (now_monotonic_ns - last_recv) / 1_000_000.0

    def _record_feed_receive_stale(
        self,
        *,
        symbol: str,
        state: OrderbookState,
        now_monotonic_ns: int,
        feed_age_ms: float,
    ) -> dict[str, Any]:
        period = self._open_stale_periods.get(symbol)
        if period is None:
            started_ns = int(
                (self._last_ws_message_recv_monotonic_ns.get(symbol) or now_monotonic_ns)
                + self.feed_receive_stale_threshold_ms * 1_000_000
            )
            period = {
                "case_type": "feed_receive_stale",
                "symbol": symbol,
                "stale_threshold_ms": self.feed_receive_stale_threshold_ms,
                "feed_receive_stale_threshold_ms": self.feed_receive_stale_threshold_ms,
                "started_monotonic_ns": started_ns,
                "ended_monotonic_ns": None,
                "max_age_ms": feed_age_ms,
                "reason": "no_websocket_message_received",
                "snapshot_ready": state.snapshot_ready,
                "ready_to_emit": state.ready_to_emit,
                "last_book_update_monotonic_ns": state.last_book_update_monotonic_ns,
                "last_message_recv_monotonic_ns": state.last_message_recv_monotonic_ns,
                "last_ws_message_recv_monotonic_ns": self._last_ws_message_recv_monotonic_ns.get(symbol),
                "generation_id": state.generation,
            }
            self._open_stale_periods[symbol] = period
            self.stale_periods.append(period)
            self.counters["feed_receive_stale_count"] += 1
            self.counters["active_feed_stale_count"] += 1
            self.counters["stale_book_count"] += 1
            self.counters["stale_reset_count"] += 1
            state.mark_not_ready(
                "feed_receive_stale",
                local_recv_monotonic_ns=now_monotonic_ns,
                advance_generation=True,
            )
            period["snapshot_ready"] = state.snapshot_ready
            period["ready_to_emit"] = state.ready_to_emit
            period["generation_id"] = state.generation
            self.lifecycle.observe_ready_false(state)
            self.debug.record_stale_period_case(period)
        else:
            period["max_age_ms"] = max(float(period["max_age_ms"]), feed_age_ms)
            period["snapshot_ready"] = state.snapshot_ready
            period["ready_to_emit"] = state.ready_to_emit
            period["last_message_recv_monotonic_ns"] = state.last_message_recv_monotonic_ns
            period["last_ws_message_recv_monotonic_ns"] = self._last_ws_message_recv_monotonic_ns.get(symbol)
            period["generation_id"] = state.generation
        return period

    def _record_processor_apply_stale(
        self,
        *,
        symbol: str,
        state: OrderbookState,
        now_monotonic_ns: int,
        apply_age_ms: float,
        feed_age_ms: float | None,
        queue_size: int,
    ) -> None:
        self._processor_apply_stale_consecutive_checks[symbol] += 1
        if symbol in self._open_processor_apply_stale_symbols:
            if self.processor_apply_stale_warnings:
                self.processor_apply_stale_warnings[-1]["max_apply_age_ms"] = max(
                    float(self.processor_apply_stale_warnings[-1]["max_apply_age_ms"]),
                    apply_age_ms,
                )
            return
        self._open_processor_apply_stale_symbols.add(symbol)
        self.counters["processor_apply_stale_count"] += 1
        warning = {
            "case_type": "processor_apply_stale",
            "symbol": symbol,
            "stale_threshold_ms": self.stale_threshold_ms,
            "observed_monotonic_ns": now_monotonic_ns,
            "apply_age_ms": apply_age_ms,
            "max_apply_age_ms": apply_age_ms,
            "feed_receive_age_ms": feed_age_ms,
            "queue_size": queue_size,
            "consecutive_checks": int(self._processor_apply_stale_consecutive_checks[symbol]),
            "reason": "websocket_messages_arriving_but_no_successful_apply",
            "snapshot_ready": state.snapshot_ready,
            "ready_to_emit": state.ready_to_emit,
            "generation_id": state.generation,
        }
        self.processor_apply_stale_warnings.append(warning)

    def _record_post_capture_age_warning(
        self,
        *,
        symbol: str,
        state: OrderbookState,
        now_monotonic_ns: int,
        age_ms: float,
    ) -> None:
        if symbol in self._post_capture_warning_symbols:
            for warning in self.post_capture_age_warnings:
                if warning.get("symbol") == symbol:
                    warning["age_ms"] = age_ms
                    warning["observed_monotonic_ns"] = now_monotonic_ns
            return
        warning = {
            "event": "post_capture_age_warning",
            "symbol": symbol,
            "stale_threshold_ms": self.stale_threshold_ms,
            "observed_monotonic_ns": now_monotonic_ns,
            "age_ms": age_ms,
            "reason": "book_age_exceeded_threshold_after_capture_end",
            "snapshot_ready": state.snapshot_ready,
            "ready_to_emit": state.ready_to_emit,
            "last_book_update_monotonic_ns": state.last_book_update_monotonic_ns,
            "last_message_recv_monotonic_ns": state.last_message_recv_monotonic_ns,
            "generation_id": state.generation,
        }
        self._post_capture_warning_symbols.add(symbol)
        self.post_capture_age_warnings.append(warning)

    def _close_stale_period(
        self,
        symbol: str,
        *,
        now_monotonic_ns: int | None,
    ) -> None:
        period = self._open_stale_periods.pop(symbol.upper(), None)
        if period is None or now_monotonic_ns is None:
            return
        period["ended_monotonic_ns"] = now_monotonic_ns
        self.debug.record_stale_period_case(period)

    def summary(self, *, duration_sec: float | None = None) -> dict[str, Any]:
        self.check_stale_periods(
            now_monotonic_ns=self.monotonic_clock(),
            feed_active=self.capture_active,
        )
        snapshot_copy_p99_us = _percentile(list(self.snapshot_copy_us_samples), 0.99)
        lifecycle_report = self.lifecycle.report()
        queue_report = self.queue_monitor.report()
        lifecycle_report.update(
            {
                "feed_receive_stale_count": int(self.counters["feed_receive_stale_count"]),
                "processor_apply_stale_count": int(self.counters["processor_apply_stale_count"]),
                "post_capture_age_warning_count": len(self.post_capture_age_warnings),
                "stale_reset_count": int(self.counters["stale_reset_count"]),
            }
        )
        summary = {
            "phase": "4.1.1",
            "duration_sec": duration_sec,
            "symbol": ",".join(sorted(self.states)),
            "messages_received": int(self.counters["messages_received"]),
            "messages_parsed": int(self.counters["messages_parsed"]),
            "deltas_accepted": int(self.counters["deltas_accepted"]),
            "deltas_rejected": int(self.counters["deltas_rejected"]),
            "sequence_gap_count": int(lifecycle_report["sequence_gap_count"]),
            "sequence_gap_or_reset_count": int(lifecycle_report["sequence_gap_count"]),
            "first_delta_bridge_failed_count": int(self.counters["first_delta_bridge_failed_count"]),
            "bridge_missing_after_snapshot_count": int(self.counters["bridge_missing_after_snapshot_count"]),
            "duplicates_skipped": int(lifecycle_report["duplicate_messages_skipped"]),
            "samples_emitted": int(self.counters["samples_emitted"]),
            "samples_blocked": int(self.counters["samples_blocked"]),
            "sample_before_ready_count": int(lifecycle_report["delta_before_snapshot_count"]),
            "invalid_delta_count": int(self.counters["invalid_delta_count"]),
            "previous_final_update_id_mismatch_count": int(
                self.counters["previous_final_update_id_mismatch_count"]
            ),
            "ready_to_emit_disabled_count": int(self.counters["ready_to_emit_disabled_count"]),
            "ready_to_emit_violation_count": int(self.counters["ready_to_emit_violation_count"]),
            "clean_sample_schema_violation_count": int(self.counters["clean_sample_schema_violation_count"]),
            "strict_mismatch_count": int(self.counters["strict_mismatch_count"]),
            "tolerant_mismatch_count": int(self.counters["tolerant_mismatch_count"]),
            "crossed_book_count": int(self.counters["crossed_book_count"]),
            "book_empty_count": int(self.blocked_by_quality_error.get("book_empty", 0)),
            "one_side_missing_count": int(self.blocked_by_quality_error.get("one_side_missing", 0)),
            "stale_book_count": int(self.counters["stale_book_count"]),
            "active_feed_stale_count": int(self.counters["active_feed_stale_count"]),
            "feed_receive_stale_count": int(self.counters["feed_receive_stale_count"]),
            "processor_apply_stale_count": int(self.counters["processor_apply_stale_count"]),
            "stale_reset_count": int(self.counters["stale_reset_count"]),
            "post_capture_age_warning_count": len(self.post_capture_age_warnings),
            "stale_periods": list(self.stale_periods),
            "processor_apply_stale_warnings": list(self.processor_apply_stale_warnings),
            "post_capture_age_warnings": list(self.post_capture_age_warnings),
            "max_book_age_ms": self.max_book_age_ms,
            "last_book_update_age_ms_at_report": self.last_book_update_age_ms_at_report,
            "stale_threshold_ms": self.stale_threshold_ms,
            "feed_receive_stale_threshold_ms": self.feed_receive_stale_threshold_ms,
            "queue_backpressure_events": int(queue_report["queue_backpressure_events"]),
            "max_queue_lag_ms": queue_report["enqueue_to_dequeue_lag_ms_max"],
            "queue_lag_p50_ms": queue_report["enqueue_to_dequeue_lag_p50_ms"],
            "queue_lag_p95_ms": queue_report["enqueue_to_dequeue_lag_p95_ms"],
            "queue_lag_p99_ms": queue_report["enqueue_to_dequeue_lag_p99_ms"],
            "queue_depth_p50": queue_report["queue_depth_p50"],
            "queue_depth_p95": queue_report["queue_depth_p95"],
            "queue_depth_p99": queue_report["queue_depth_p99"],
            "queue_put_block_count": queue_report["queue_put_block_count"],
            "queue_put_block_p95_ms": queue_report["queue_put_block_p95_ms"],
            "processing_lag_p50_ms": queue_report["processing_lag_p50_ms"],
            "processing_lag_p95_ms": queue_report["processing_lag_p95_ms"],
            "processing_lag_p99_ms": queue_report["processing_lag_p99_ms"],
            "queue_size_backpressure_events": int(queue_report["queue_size_backpressure_events"]),
            "queue_lag_backpressure_events": int(queue_report["queue_lag_backpressure_events"]),
            "processing_lag_backpressure_events": int(queue_report["processing_lag_backpressure_events"]),
            "snapshot_blocking_lag_events": int(queue_report["snapshot_blocking_lag_events"]),
            "snapshot_copy_p99_us": snapshot_copy_p99_us,
            "snapshot_copy_budget_us": SNAPSHOT_COPY_BUDGET_US,
            "snapshot_copy_budget_met": snapshot_copy_p99_us <= SNAPSHOT_COPY_BUDGET_US,
            "blocked_by_quality_error": dict(sorted(self.blocked_by_quality_error.items())),
            "queue": queue_report,
            "writer_batch_report": self.writer_report(),
            "disk_write_on_hot_path": not self.batch_writer_enabled,
            "debug_logging_on_hot_path": not self.batch_writer_enabled,
            "batch_writer_enabled": self.batch_writer_enabled,
            "lifecycle": lifecycle_report,
            "clean_sample_schema_version": PHASE_4_1_SCHEMA_VERSION,
            "reported_best_validation_enabled": False,
            "market_status_known": False,
            "market_status_mode": "not_applicable_for_binance_spot_orderbook",
        }
        phase_pass, failure_reasons = evaluate_phase_4_1_pass(summary)
        summary["phase_4_1_pass"] = phase_pass
        summary["phase_4_1_status"] = "pass" if phase_pass else "fail"
        summary["phase_4_1_failure_reasons"] = failure_reasons
        summary["status"] = summary["phase_4_1_status"]
        summary["hard_fail_reasons"] = list(failure_reasons)
        summary["warning_reasons"] = _phase411_warning_reasons(summary)
        return summary

    def write_reports(self, *, duration_sec: float | None = None) -> dict[str, Any]:
        summary = self.summary(duration_sec=duration_sec)
        _write_json(self.paths.quality_report, summary)
        _write_json(self.paths.lifecycle_report, summary["lifecycle"])
        self.paths.markdown_report.parent.mkdir(parents=True, exist_ok=True)
        self.paths.markdown_report.write_text(
            render_phase41_markdown_report(summary),
            encoding="utf-8",
        )
        return summary

    def close_writer(self) -> None:
        if self.writer is not None:
            self.writer.close()

    def writer_report(self) -> dict[str, Any]:
        if self.writer is None:
            return {
                "writer_mode": "synchronous_jsonl_writer",
                "writer_batch_size": 1,
                "writer_flush_interval_ms": 0.0,
                "writer_queue_max_size": 0,
                "writer_thread_or_task_count": 0,
                "writer_shutdown_flush_completed": True,
                "writer_dropped_records": 0,
                "writer_error_count": 0,
                "writer_records_enqueued": 0,
                "writer_records_written": 0,
                "writer_flush_count": 0,
                "writer_flush_p50_ms": 0.0,
                "writer_flush_p95_ms": 0.0,
                "writer_flush_p99_ms": 0.0,
                "writer_flush_max_ms": 0.0,
            }
        return self.writer.report()

    def _maybe_emit_or_block_sample(
        self,
        snapshot: OrderbookSnapshot,
        quality: OrderbookQualityResult,
        *,
        exchange_event_ts: int | None,
        event: DepthUpdate,
        book_apply_start_monotonic_ns: int,
        book_apply_end_monotonic_ns: int,
        queue_put_monotonic_ns: int | None,
        queue_put_end_monotonic_ns: int | None,
        queue_dequeue_monotonic_ns: int | None,
        queue_wait_ms: float | None,
        queue_size_at_enqueue: int | None,
    ) -> None:
        strict = quality.strict_mismatch_details
        tolerant = quality.tolerant_mismatch_details
        if strict.get("strict_mismatch"):
            self.counters["strict_mismatch_count"] += 1
        if tolerant.get("tolerant_mismatch"):
            self.counters["tolerant_mismatch_count"] += 1
        if "crossed_book" in quality.errors:
            self.counters["crossed_book_count"] += 1
        if "stale_book" in quality.errors:
            self.counters["stale_book_count"] += 1

        if not quality.is_valid:
            self.counters["samples_blocked"] += 1
            self.lifecycle.on_sample_blocked_by_ready_guard()
            for error in quality.errors:
                self.blocked_by_quality_error[error] += 1
            if "one_side_missing" in quality.errors:
                reason = (
                    "bid_side_missing_after_delta"
                    if snapshot.bid_count == 0
                    else "ask_side_missing_after_delta"
                )
                self.debug.record_book_incomplete_case(
                    snapshot,
                    quality,
                    reason=reason,
                )
            return

        sample_build_start_ns = self.monotonic_clock()
        sample = clean_sample_from_snapshot(
            snapshot,
            quality,
            depth_n=self.depth_n,
            exchange_event_ts=exchange_event_ts,
        )
        sample_build_end_ns = self.monotonic_clock()
        violations = validate_clean_sample_schema(sample)
        if violations:
            self.counters["samples_blocked"] += 1
            self.counters["clean_sample_schema_violation_count"] += 1
            self.debug.record_clean_sample_schema_violation_case(
                {
                    "event": "clean_sample_schema_violation",
                    "symbol": sample.get("symbol"),
                    "generation_id": sample.get("generation_id"),
                    "last_update_id": sample.get("last_update_id"),
                    "violations": violations,
                    "sample_preview": {
                        "schema_version": sample.get("schema_version"),
                        "symbol": sample.get("symbol"),
                        "last_update_id": sample.get("last_update_id"),
                    },
                    "monotonic_ts_ns": sample.get("local_recv_monotonic_ns"),
                }
            )
            return
        sample_emit_ns = self.monotonic_clock()
        clean_write = self.debug.record_clean_sample(sample)
        self.debug.record_latency_profile_sample(
            build_latency_profile_sample(
                event=event,
                snapshot=snapshot,
                book_apply_start_monotonic_ns=book_apply_start_monotonic_ns,
                book_apply_end_monotonic_ns=book_apply_end_monotonic_ns,
                sample_build_start_monotonic_ns=sample_build_start_ns,
                sample_build_end_monotonic_ns=sample_build_end_ns,
                sample_emit_monotonic_ns=sample_emit_ns,
                queue_put_monotonic_ns=queue_put_monotonic_ns,
                queue_put_end_monotonic_ns=queue_put_end_monotonic_ns,
                queue_dequeue_monotonic_ns=queue_dequeue_monotonic_ns,
                queue_wait_ms=queue_wait_ms,
                queue_size_at_enqueue=queue_size_at_enqueue,
                writer_enqueue_monotonic_ns=clean_write.writer_enqueue_start_monotonic_ns,
                file_write_start_monotonic_ns=clean_write.file_write_start_monotonic_ns,
                file_write_end_monotonic_ns=clean_write.file_write_end_monotonic_ns,
                disk_write_on_hot_path=not self.batch_writer_enabled,
                debug_logging_on_hot_path=not self.batch_writer_enabled,
                batch_writer_enabled=self.batch_writer_enabled,
            )
        )
        self.counters["samples_emitted"] += 1


def evaluate_phase_4_1_pass(report: dict[str, Any]) -> tuple[bool, list[str]]:
    failure_reasons: list[str] = []
    if _numeric_value(report.get("sequence_gap_count")) > 0:
        failure_reasons.append("sequence_gap_count > 0")
    elif _numeric_value(report.get("sequence_gap_or_reset_count")) > 0:
        failure_reasons.append("sequence_gap_or_reset_count > 0")
    blocker_fields = (
        "crossed_book_count",
        "book_empty_count",
        "one_side_missing_count",
        "sample_before_ready_count",
        "invalid_delta_count",
        "previous_final_update_id_mismatch_count",
        "ready_to_emit_violation_count",
        "clean_sample_schema_violation_count",
    )
    for field in blocker_fields:
        if _numeric_value(report.get(field)) > 0:
            failure_reasons.append(f"{field} > 0")

    active_stale = report.get("active_feed_stale_count")
    feed_receive_stale = report.get("feed_receive_stale_count")
    if feed_receive_stale is not None:
        active_stale = feed_receive_stale
        stale_reason = "feed_receive_stale_count > 0"
    elif active_stale is None:
        active_stale = report.get("stale_book_count")
        stale_reason = "stale_book_count > 0"
    else:
        stale_reason = "active_feed_stale_count > 0"
    if _numeric_value(active_stale) > 0:
        failure_reasons.append(stale_reason)

    for field in (
        "bridge_missing_after_snapshot_count",
        "first_delta_bridge_failed_count",
    ):
        if _numeric_value(report.get(field)) > 0:
            failure_reasons.append(f"{field} > 0")

    queue = report.get("queue")
    if isinstance(queue, dict):
        if _numeric_value(queue.get("queue_dropped_messages")) > 0:
            failure_reasons.append("queue_dropped_messages > 0")

    if not bool(report.get("snapshot_copy_budget_met", True)):
        failure_reasons.append("snapshot_copy_p99_us > snapshot_copy_budget_us")

    return not failure_reasons, failure_reasons


def _phase411_warning_reasons(report: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if _numeric_value(report.get("post_capture_age_warning_count")) > 0:
        warnings.append("post_capture_age_warning_count > 0")
    if str(report.get("market_status_mode")) == "not_applicable_for_binance_spot_orderbook":
        warnings.append("market_status_not_applicable_for_binance_spot_orderbook")
    if _numeric_value(report.get("processor_apply_stale_count")) > 0:
        warnings.append("processor_apply_stale_count > 0")
    return warnings


def _sequence_trace_classification(
    result: OrderbookApplyResult,
    *,
    awaiting_bridge_before: bool,
) -> str:
    if result.accepted:
        return "bridge_accepted" if awaiting_bridge_before else "expected_accepted"
    if result.status == "duplicate_update":
        if (
            result.previous_last_update_id is not None
            and result.final_update_id is not None
            and result.final_update_id < result.previous_last_update_id
        ):
            return "old_dropped"
        return "duplicate_skipped"
    if result.status in {
        "sequence_gap_or_reset",
        "sequence_bridge_failed",
        "previous_final_update_id_mismatch",
    }:
        return "gap_detected"
    if result.status == "invalid_delta_levels":
        return "invalid_fail_closed"
    return result.status


def clean_sample_from_snapshot(
    snapshot: OrderbookSnapshot,
    quality: OrderbookQualityResult,
    *,
    depth_n: int,
    exchange_event_ts: int | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": PHASE_4_1_SCHEMA_VERSION,
        "symbol": snapshot.symbol,
        "source": "binance_ws",
        "generation_id": snapshot.generation_id,
        "local_recv_monotonic_ns": snapshot.local_recv_monotonic_ns,
        "local_recv_wall_ts": snapshot.local_recv_wall_ts,
        "exchange_event_ts": exchange_event_ts,
        "state_version": snapshot.state_version,
        "snapshot_version": snapshot.snapshot_version,
        "last_update_id": snapshot.last_update_id,
        "depth_n": depth_n,
        "bids": _levels_to_json(snapshot.bids_top_n),
        "asks": _levels_to_json(snapshot.asks_top_n),
        "best_bid": _decimal_to_str(snapshot.best_bid),
        "best_ask": _decimal_to_str(snapshot.best_ask),
        "spread": _decimal_to_str(snapshot.spread),
        "mid": _decimal_to_str(snapshot.mid),
        "bid_count": snapshot.bid_count,
        "ask_count": snapshot.ask_count,
        "book_age_ms": quality.book_age_ms,
        "quality": {
            "is_valid": quality.is_valid,
            "errors": list(quality.errors),
            "warnings": list(quality.warnings),
        },
        "lifecycle": quality.lifecycle_flags,
    }


def build_latency_profile_sample(
    *,
    event: DepthUpdate,
    snapshot: OrderbookSnapshot,
    book_apply_start_monotonic_ns: int,
    book_apply_end_monotonic_ns: int,
    sample_build_start_monotonic_ns: int,
    sample_build_end_monotonic_ns: int,
    sample_emit_monotonic_ns: int,
    queue_put_monotonic_ns: int | None,
    queue_put_end_monotonic_ns: int | None = None,
    queue_dequeue_monotonic_ns: int | None = None,
    queue_wait_ms: float | None = None,
    queue_size_at_enqueue: int | None = None,
    writer_enqueue_monotonic_ns: int | None = None,
    file_write_start_monotonic_ns: int | None = None,
    file_write_end_monotonic_ns: int | None = None,
    disk_write_on_hot_path: bool = True,
    debug_logging_on_hot_path: bool = True,
    batch_writer_enabled: bool = False,
) -> dict[str, Any]:
    ws_message_received_ns = event.ws_message_received_monotonic_ns or event.recv_monotonic_ns
    parse_start_ns = event.parse_start_monotonic_ns
    parse_end_ns = event.parse_end_monotonic_ns or event.parse_done_monotonic_ns
    raw_callback_ns = event.raw_ws_callback_monotonic_ns or ws_message_received_ns
    dispatch_start_ns = event.message_dispatch_start_monotonic_ns or parse_start_ns
    earliest_receive_ns = event.socket_recv_monotonic_ns or raw_callback_ns or ws_message_received_ns
    hot_path_end_ns = sample_emit_monotonic_ns if batch_writer_enabled else file_write_end_monotonic_ns
    stages: dict[str, int | None] = {
        "socket_recv_monotonic_ns": event.socket_recv_monotonic_ns,
        "raw_ws_callback_monotonic_ns": raw_callback_ns,
        "ws_message_received_monotonic_ns": ws_message_received_ns,
        "message_dispatch_start_monotonic_ns": dispatch_start_ns,
        "parse_start_monotonic_ns": parse_start_ns,
        "parse_end_monotonic_ns": parse_end_ns,
        "book_apply_start_monotonic_ns": book_apply_start_monotonic_ns,
        "book_apply_end_monotonic_ns": book_apply_end_monotonic_ns,
        "sample_build_start_monotonic_ns": sample_build_start_monotonic_ns,
        "sample_build_end_monotonic_ns": sample_build_end_monotonic_ns,
        "sample_emit_monotonic_ns": sample_emit_monotonic_ns,
        "queue_put_monotonic_ns": queue_put_monotonic_ns,
        "queue_put_start_monotonic_ns": queue_put_monotonic_ns,
        "queue_put_end_monotonic_ns": queue_put_end_monotonic_ns,
        "writer_enqueue_monotonic_ns": writer_enqueue_monotonic_ns,
        "file_write_start_monotonic_ns": file_write_start_monotonic_ns,
        "file_write_end_monotonic_ns": file_write_end_monotonic_ns,
    }
    metrics = {
        "socket_to_parse_start_ms": _duration_ms(event.socket_recv_monotonic_ns, parse_start_ns),
        "callback_to_dispatch_ms": _duration_ms(raw_callback_ns, dispatch_start_ns),
        "dispatch_to_parse_start_ms": _duration_ms(dispatch_start_ns, parse_start_ns),
        "parse_duration_ms": _duration_ms(parse_start_ns, parse_end_ns),
        "parse_to_apply_start_ms": _duration_ms(parse_end_ns, book_apply_start_monotonic_ns),
        "book_apply_duration_ms": _duration_ms(book_apply_start_monotonic_ns, book_apply_end_monotonic_ns),
        "apply_to_sample_emit_ms": _duration_ms(book_apply_end_monotonic_ns, sample_emit_monotonic_ns),
        "apply_to_sample_build_ms": _duration_ms(book_apply_end_monotonic_ns, sample_build_start_monotonic_ns),
        "sample_build_duration_ms": _duration_ms(sample_build_start_monotonic_ns, sample_build_end_monotonic_ns),
        "sample_emit_to_queue_put_start_ms": _duration_ms(sample_emit_monotonic_ns, queue_put_monotonic_ns),
        "sample_emit_to_queue_put_ms": _duration_ms(sample_emit_monotonic_ns, queue_put_monotonic_ns),
        "queue_put_duration_ms": _duration_ms(queue_put_monotonic_ns, queue_put_end_monotonic_ns),
        "queue_wait_ms": queue_wait_ms
        if queue_wait_ms is not None
        else _duration_ms(queue_put_monotonic_ns, queue_dequeue_monotonic_ns),
        "writer_wait_ms": _duration_ms(writer_enqueue_monotonic_ns, file_write_start_monotonic_ns),
        "file_write_duration_ms": _duration_ms(file_write_start_monotonic_ns, file_write_end_monotonic_ns),
        "end_to_end_local_hot_path_ms": _duration_ms(earliest_receive_ns, hot_path_end_ns),
    }
    stage_not_available = [
        name for name, value in stages.items() if value is None
    ]
    earliest_available_receive_stage = _earliest_available_stage(
        stages,
        (
            "socket_recv_monotonic_ns",
            "raw_ws_callback_monotonic_ns",
            "ws_message_received_monotonic_ns",
            "message_dispatch_start_monotonic_ns",
        ),
    )
    return {
        "schema_version": "phase_4_2h_latency_profile_sample_v1" if batch_writer_enabled else "phase_4_2fg_latency_profile_sample_v1",
        "symbol": snapshot.symbol,
        "source": "binance_ws",
        "event_type": "depthUpdate",
        "generation_id": snapshot.generation_id,
        "state_version": snapshot.state_version,
        "snapshot_version": snapshot.snapshot_version,
        "last_update_id": snapshot.last_update_id,
        "first_update_id": event.first_update_id,
        "final_update_id": event.final_update_id,
        "exchange_event_ts": event.exchange_event_ts,
        "local_recv_monotonic_ns": snapshot.local_recv_monotonic_ns,
        "stages": stages,
        "metrics": metrics,
        "stage_not_available": stage_not_available,
        "stage_status": {
            name: ("stage_not_available" if name in stage_not_available else "available")
            for name in stages
        },
        "socket_recv_monotonic_ns": event.socket_recv_monotonic_ns
        if event.socket_recv_monotonic_ns is not None
        else "stage_not_available",
        "earliest_available_receive_stage": earliest_available_receive_stage,
        "queue_size_at_enqueue": queue_size_at_enqueue,
        "queue_dequeue_monotonic_ns": queue_dequeue_monotonic_ns,
        "disk_write_on_hot_path": disk_write_on_hot_path,
        "debug_logging_on_hot_path": debug_logging_on_hot_path,
        "batch_writer_enabled": batch_writer_enabled,
    }


def validate_clean_sample_schema(sample: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    for field in (
        "schema_version",
        "symbol",
        "source",
        "state_version",
        "snapshot_version",
        "last_update_id",
        "local_recv_monotonic_ns",
        "best_bid",
        "best_ask",
        "book_age_ms",
        "quality",
        "lifecycle",
    ):
        if field not in sample:
            violations.append(f"{field}_missing")
        elif sample.get(field) is None:
            violations.append(f"{field}_null")

    if "generation_id" not in sample:
        violations.append("generation_id_missing")
    elif sample.get("generation_id") is None:
        violations.append("generation_id_null")
    elif isinstance(sample.get("generation_id"), bool) or not isinstance(sample.get("generation_id"), int):
        violations.append("generation_id_not_int")

    wall_timestamp = sample.get("local_recv_wall_ts") or sample.get("local_recv_wall_iso") or sample.get("local_recv_wall_ts_ns")
    if wall_timestamp is None:
        violations.append("wall_timestamp_missing")

    bids = sample.get("bids")
    asks = sample.get("asks")
    if not isinstance(bids, list) or not bids:
        violations.append("bids_empty")
        bid_prices: list[Decimal] = []
    else:
        bid_prices = _validate_sample_levels(bids, side="bid", violations=violations)

    if not isinstance(asks, list) or not asks:
        violations.append("asks_empty")
        ask_prices: list[Decimal] = []
    else:
        ask_prices = _validate_sample_levels(asks, side="ask", violations=violations)

    if bid_prices and bid_prices != sorted(bid_prices, reverse=True):
        violations.append("bids_unsorted")
    if ask_prices and ask_prices != sorted(ask_prices):
        violations.append("asks_unsorted")

    best_bid = _sample_decimal(sample.get("best_bid"), "best_bid", violations)
    best_ask = _sample_decimal(sample.get("best_ask"), "best_ask", violations)
    if best_bid is not None and best_ask is not None and best_bid >= best_ask:
        violations.append("crossed_book")

    book_age = _sample_decimal(sample.get("book_age_ms"), "book_age_ms", violations)
    if book_age is not None and book_age < 0:
        violations.append("book_age_ms_negative")

    quality = sample.get("quality")
    if not isinstance(quality, dict):
        violations.append("quality_missing")
    else:
        errors = quality.get("errors")
        if errors:
            violations.append("quality_errors_present")

    lifecycle = sample.get("lifecycle")
    if not isinstance(lifecycle, dict):
        violations.append("lifecycle_missing")
    else:
        if lifecycle.get("snapshot_ready") is not True:
            violations.append("snapshot_not_ready")
        if lifecycle.get("ready_to_emit") is not True:
            violations.append("not_ready_to_emit")
        if lifecycle.get("sequence_continuous") is not True:
            violations.append("sequence_not_continuous")

    return sorted(set(violations))


def purge_queued_depth_updates_after_snapshot(
    envelopes: Iterable[QueueEnvelope],
    *,
    snapshot_last_update_id: int,
    symbol: str,
) -> QueuePurgeOutput:
    symbol = symbol.upper()
    drained = tuple(envelopes)
    preserved: list[QueueEnvelope] = []
    old_events_dropped = 0
    future_events_dropped = 0
    bridge_candidate_found = False
    bridge_first_update_id: int | None = None
    bridge_final_update_id: int | None = None
    target_update_id = int(snapshot_last_update_id) + 1

    for envelope in drained:
        payload = envelope.payload
        if not isinstance(payload, DepthUpdate) or payload.symbol.upper() != symbol:
            preserved.append(envelope)
            continue

        if payload.final_update_id <= snapshot_last_update_id:
            old_events_dropped += 1
            continue

        if not bridge_candidate_found:
            if payload.first_update_id <= target_update_id <= payload.final_update_id:
                bridge_candidate_found = True
                bridge_first_update_id = payload.first_update_id
                bridge_final_update_id = payload.final_update_id
                preserved.append(envelope)
            else:
                future_events_dropped += 1
            continue

        preserved.append(envelope)

    bridge_missing = bool(future_events_dropped and not bridge_candidate_found)
    result = PostSnapshotQueuePurgeResult(
        symbol=symbol,
        snapshot_last_update_id=int(snapshot_last_update_id),
        queue_size_before=len(drained),
        old_events_dropped=old_events_dropped,
        bridge_candidate_found=bridge_candidate_found,
        bridge_first_update_id=bridge_first_update_id,
        bridge_final_update_id=bridge_final_update_id,
        events_preserved_count=len(preserved),
        queue_size_after=len(preserved),
        bridge_missing_after_snapshot=bridge_missing,
        future_events_dropped=future_events_dropped,
    )
    return QueuePurgeOutput(result=result, preserved=tuple(preserved))


def purge_queue_after_snapshot(
    *,
    queue: asyncio.Queue[Any],
    processor: OrderbookPhase41Processor,
    symbol: str,
    snapshot_last_update_id: int,
) -> PostSnapshotQueuePurgeResult:
    drained: list[QueueEnvelope] = []
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        drained.append(item)

    output = purge_queued_depth_updates_after_snapshot(
        drained,
        snapshot_last_update_id=snapshot_last_update_id,
        symbol=symbol,
    )
    dropped_on_requeue = 0
    for envelope in output.preserved:
        try:
            queue.put_nowait(envelope)
        except asyncio.QueueFull:
            dropped_on_requeue += 1
            processor.queue_monitor.record_drop()
            processor.lifecycle.on_queue_dropped()

    result = output.result
    if dropped_on_requeue:
        result = PostSnapshotQueuePurgeResult(
            symbol=result.symbol,
            snapshot_last_update_id=result.snapshot_last_update_id,
            queue_size_before=result.queue_size_before,
            old_events_dropped=result.old_events_dropped,
            bridge_candidate_found=result.bridge_candidate_found,
            bridge_first_update_id=result.bridge_first_update_id,
            bridge_final_update_id=result.bridge_final_update_id,
            events_preserved_count=max(0, result.events_preserved_count - dropped_on_requeue),
            queue_size_after=max(0, result.queue_size_after - dropped_on_requeue),
            bridge_missing_after_snapshot=result.bridge_missing_after_snapshot,
            future_events_dropped=result.future_events_dropped,
        )

    processor.debug.record_sequence_trace_event(
        result.to_trace_event(
            generation=processor.state_for(symbol).generation,
            monotonic_ts_ns=processor.monotonic_clock(),
        )
    )
    if result.bridge_missing_after_snapshot:
        processor.counters["bridge_missing_after_snapshot_count"] += 1
        state = processor.state_for(symbol)
        state.mark_not_ready(
            "bridge_missing_after_snapshot",
            local_recv_monotonic_ns=processor.monotonic_clock(),
            advance_generation=True,
        )
        processor.lifecycle.observe_ready_false(state)
    return result


async def run_orderbook_phase41_capture(
    *,
    symbol: str = "BTCUSDT",
    duration_sec: float = 900.0,
    depth_n: int = 20,
    ws_url: str = "wss://stream.binance.com:9443/ws",
    rest_url: str = "https://api.binance.com",
    paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
    batch_writer_enabled: bool = False,
    writer_batch_size: int = 512,
    writer_flush_interval_ms: float = 100.0,
    writer_queue_max_size: int = 65_536,
) -> dict[str, Any]:
    symbol = symbol.upper()
    processor = OrderbookPhase41Processor(
        symbols=(symbol,),
        paths=paths,
        depth_n=depth_n,
        feed_receive_stale_after_ms=60_000.0,
        batch_writer_enabled=batch_writer_enabled,
        writer_batch_size=writer_batch_size,
        writer_flush_interval_ms=writer_flush_interval_ms,
        writer_queue_max_size=writer_queue_max_size,
    )
    processor.lifecycle.on_connect()
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=4096)
    client = BinanceWSClient(
        url=ws_url,
        symbols=(symbol,),
        streams=("depth@100ms",),
        max_queue=1024,
    )
    stop = asyncio.Event()

    async def _reader() -> None:
        async for event in client.stream():
            if stop.is_set():
                return
            if not isinstance(event, DepthUpdate):
                continue
            queue_put_ns = monotonic_now_ns()
            envelope = processor.queue_monitor.record_enqueue(
                event,
                enqueue_monotonic_ns=queue_put_ns,
                queue_size=queue.qsize() + 1,
            )
            try:
                queue.put_nowait(envelope)
                queue_put_end_ns = monotonic_now_ns()
                envelope.queue_put_end_monotonic_ns = queue_put_end_ns
                processor.queue_monitor.record_queue_put_duration(
                    (queue_put_end_ns - queue_put_ns) / 1_000_000.0,
                    blocked=False,
                )
            except asyncio.QueueFull:
                processor.queue_monitor.record_drop()
                processor.lifecycle.on_queue_dropped()

    reader_task = asyncio.create_task(_reader())
    start_mono = monotonic_now_ns()
    try:
        async with aiohttp.ClientSession() as session:
            await _wait_for_initial_depth_buffer(queue, timeout_sec=5.0)
            await _load_fresh_snapshot_with_bridge_retry(
                processor,
                session=session,
                symbol=symbol,
                rest_url=rest_url,
                queue=queue,
                recovery=False,
            )
            deadline = time.monotonic() + duration_sec
            while time.monotonic() < deadline:
                timeout = max(0.01, min(1.0, deadline - time.monotonic()))
                try:
                    envelope = await asyncio.wait_for(queue.get(), timeout=timeout)
                except TimeoutError:
                    processor.check_stale_periods(
                        now_monotonic_ns=monotonic_now_ns(),
                        queue_size=queue.qsize(),
                    )
                    continue
                dequeue_ns = monotonic_now_ns()
                processor.record_delta_dequeued(symbol, dequeue_ns)
                backpressure_before = processor.queue_monitor.queue_backpressure_events
                queue_lag_ms = processor.queue_monitor.record_dequeue(
                    envelope,
                    dequeue_monotonic_ns=dequeue_ns,
                    queue_size=queue.qsize(),
                )
                result = processor.process_depth_update(
                    envelope.payload,
                    queue_lag_ms=queue_lag_ms,
                    queue_put_monotonic_ns=envelope.enqueue_monotonic_ns,
                    queue_put_end_monotonic_ns=envelope.queue_put_end_monotonic_ns,
                    queue_dequeue_monotonic_ns=dequeue_ns,
                    queue_size_at_enqueue=envelope.queue_size_at_enqueue,
                )
                processing_done_ns = monotonic_now_ns()
                processor.queue_monitor.record_processing_done(
                    dequeue_monotonic_ns=dequeue_ns,
                    processing_done_monotonic_ns=processing_done_ns,
                )
                backpressure_delta = (
                    processor.queue_monitor.queue_backpressure_events
                    - backpressure_before
                )
                if backpressure_delta > 0:
                    processor.lifecycle.on_queue_backpressure(backpressure_delta)
                if result.status in {
                    "sequence_gap_or_reset",
                    "sequence_bridge_failed",
                    "previous_final_update_id_mismatch",
                    "invalid_delta_levels",
                    "delta_before_snapshot",
                }:
                    await _load_fresh_snapshot_with_bridge_retry(
                        processor,
                        session=session,
                        symbol=symbol,
                        rest_url=rest_url,
                        queue=queue,
                        recovery=True,
                    )
    finally:
        stop.set()
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        processor.lifecycle.on_disconnect()
        processor.set_capture_active(False)
        processor.close_writer()
    duration = (monotonic_now_ns() - start_mono) / 1_000_000_000.0
    summary = processor.write_reports(duration_sec=duration)
    _write_json(REPO_ROOT / DEFAULT_PHASE411_REPORT_JSON, summary)
    report_md = REPO_ROOT / DEFAULT_PHASE411_REPORT_MD
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text(render_phase41_markdown_report(summary), encoding="utf-8")
    return summary


async def _wait_for_initial_depth_buffer(
    queue: asyncio.Queue[Any],
    *,
    timeout_sec: float,
) -> None:
    deadline = time.monotonic() + timeout_sec
    while queue.qsize() == 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)


async def _load_fresh_snapshot_with_bridge_retry(
    processor: OrderbookPhase41Processor,
    *,
    session: aiohttp.ClientSession,
    symbol: str,
    rest_url: str,
    queue: asyncio.Queue[Any] | None = None,
    recovery: bool = False,
    max_attempts: int = 3,
) -> PostSnapshotQueuePurgeResult | None:
    result: PostSnapshotQueuePurgeResult | None = None
    for attempt in range(max(1, max_attempts)):
        result = await _load_fresh_snapshot(
            processor,
            session=session,
            symbol=symbol,
            rest_url=rest_url,
            queue=queue,
            recovery=recovery or attempt > 0,
        )
        if result is None or not result.bridge_missing_after_snapshot:
            return result
        await asyncio.sleep(0.05)
    return result


async def _load_fresh_snapshot(
    processor: OrderbookPhase41Processor,
    *,
    session: aiohttp.ClientSession,
    symbol: str,
    rest_url: str,
    queue: asyncio.Queue[Any] | None = None,
    recovery: bool = False,
) -> PostSnapshotQueuePurgeResult | None:
    processor.lifecycle.on_snapshot_refresh()
    queue_size_before = 0 if queue is None else queue.qsize()
    request_start_ns = processor.monotonic_clock()
    processor.mark_snapshot_recovery_active(symbol, active=True, monotonic_ns=request_start_ns)
    processor.debug.record_sequence_trace_event(
        {
            "event": "recovery_snapshot_requested" if recovery else "snapshot_requested",
            "symbol": symbol.upper(),
            "generation_id": processor.state_for(symbol).generation,
            "snapshot_request_start_monotonic_ns": request_start_ns,
            "queue_size_before_snapshot": queue_size_before,
        }
    )
    snapshot = await fetch_binance_depth_snapshot(
        session,
        symbol=symbol,
        rest_url=rest_url,
        limit=1000,
    )
    response_ns = processor.monotonic_clock()
    processor.queue_monitor.record_snapshot_request_duration(
        (response_ns - request_start_ns) / 1_000_000.0
    )
    queue_size_after = 0 if queue is None else queue.qsize()
    apply_start_ns = processor.monotonic_clock()
    processor.load_snapshot(
        symbol,
        bids=snapshot["bids"],
        asks=snapshot["asks"],
        last_update_id=int(snapshot["lastUpdateId"]),
        local_recv_monotonic_ns=apply_start_ns,
        snapshot_request_start_monotonic_ns=request_start_ns,
        snapshot_response_monotonic_ns=response_ns,
        snapshot_apply_monotonic_ns=apply_start_ns,
        queue_size_before_snapshot=queue_size_before,
        queue_size_after_snapshot=queue_size_after,
        recovery=recovery,
    )
    apply_end_ns = processor.monotonic_clock()
    processor.queue_monitor.record_snapshot_apply_duration(
        (apply_end_ns - apply_start_ns) / 1_000_000.0
    )
    purge_result: PostSnapshotQueuePurgeResult | None = None
    if queue is not None:
        purge_result = purge_queue_after_snapshot(
            queue=queue,
            processor=processor,
            symbol=symbol,
            snapshot_last_update_id=int(snapshot["lastUpdateId"]),
        )
    processor.mark_snapshot_recovery_active(symbol, active=False, monotonic_ns=processor.monotonic_clock())
    return purge_result


async def fetch_binance_depth_snapshot(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    rest_url: str,
    limit: int,
) -> dict[str, Any]:
    url = f"{rest_url.rstrip('/')}/api/v3/depth"
    async with session.get(url, params={"symbol": symbol.upper(), "limit": limit}) as response:
        response.raise_for_status()
        payload = await response.json()
    if not isinstance(payload, dict) or "lastUpdateId" not in payload:
        raise RuntimeError(f"invalid Binance depth snapshot payload: {payload!r}")
    return payload


def render_phase41_markdown_report(summary: dict[str, Any]) -> str:
    lifecycle = summary["lifecycle"]
    queue = summary["queue"]
    lines = [
        "# Phase 4.1 Orderbook Quality Report",
        "",
        f"- Phase 4.1 pass: `{summary['phase_4_1_pass']}`",
        f"- Phase 4.1 status: `{summary.get('phase_4_1_status', '-')}`",
        f"- Failure reasons: `{json.dumps(summary.get('phase_4_1_failure_reasons', []), sort_keys=True)}`",
        f"- Messages received: {summary['messages_received']}",
        f"- Messages parsed successfully: {summary['messages_parsed']}",
        f"- Deltas accepted: {summary['deltas_accepted']}",
        f"- Deltas rejected: {summary['deltas_rejected']}",
        f"- Sequence gaps: {summary['sequence_gap_count']}",
        f"- Sequence gap/reset count: {summary.get('sequence_gap_or_reset_count', 0)}",
        f"- Invalid delta count: {summary.get('invalid_delta_count', 0)}",
        f"- Duplicate/old updates skipped: {summary['duplicates_skipped']}",
        f"- Samples emitted: {summary['samples_emitted']}",
        f"- Samples blocked by ready_to_emit: {lifecycle['sample_blocked_by_ready_guard']}",
        f"- Sample-before-ready count: {summary.get('sample_before_ready_count', 0)}",
        f"- ready_to_emit disabled count: {summary.get('ready_to_emit_disabled_count', 0)}",
        f"- Samples blocked by quality error: `{json.dumps(summary['blocked_by_quality_error'], sort_keys=True)}`",
        f"- Strict mismatch count: {summary['strict_mismatch_count']}",
        f"- Tolerant mismatch count: {summary['tolerant_mismatch_count']}",
        (
            "- Tolerant mode materially reduced mismatch: "
            f"{summary['tolerant_mismatch_count'] < summary['strict_mismatch_count']}"
        ),
        "- Top mismatch root causes: see `data/debug/orderbook_mismatch_cases.jsonl`",
        f"- Crossed book count: {summary['crossed_book_count']}",
        f"- Stale book count: {summary['stale_book_count']}",
        f"- Active-feed stale count: {summary.get('active_feed_stale_count', 0)}",
        f"- Feed receive stale count: {summary.get('feed_receive_stale_count', 0)}",
        f"- Processor apply stale count: {summary.get('processor_apply_stale_count', 0)}",
        f"- Stale reset count: {summary.get('stale_reset_count', 0)}",
        f"- Post-capture age warning count: {summary.get('post_capture_age_warning_count', 0)}",
        f"- Stale threshold ms: {summary.get('stale_threshold_ms')}",
        f"- Feed receive stale threshold ms: {summary.get('feed_receive_stale_threshold_ms')}",
        f"- Max book age ms: {summary.get('max_book_age_ms')}",
        f"- Last book update age ms at report: {summary.get('last_book_update_age_ms_at_report')}",
        f"- Stale periods: `{json.dumps(summary.get('stale_periods', []), sort_keys=True)}`",
        f"- Processor apply stale warnings: `{json.dumps(summary.get('processor_apply_stale_warnings', []), sort_keys=True)}`",
        f"- Post-capture age warnings: `{json.dumps(summary.get('post_capture_age_warnings', []), sort_keys=True)}`",
        f"- Queue backpressure events: {summary['queue_backpressure_events']}",
        f"- Queue size backpressure events: {summary.get('queue_size_backpressure_events', 0)}",
        f"- Queue lag backpressure events: {summary.get('queue_lag_backpressure_events', 0)}",
        f"- Processing lag backpressure events: {summary.get('processing_lag_backpressure_events', 0)}",
        f"- Snapshot blocking lag events: {summary.get('snapshot_blocking_lag_events', 0)}",
        f"- Max queue lag ms: {summary['max_queue_lag_ms']}",
        f"- Queue lag p95 ms: {summary.get('queue_lag_p95_ms')}",
        f"- Queue lag p99 ms: {summary.get('queue_lag_p99_ms')}",
        f"- Processing lag p99 ms: {summary.get('processing_lag_p99_ms')}",
        (
            "- Snapshot copy p99 us: "
            f"{summary['snapshot_copy_p99_us']} "
            f"(budget {summary['snapshot_copy_budget_us']}, met={summary['snapshot_copy_budget_met']})"
        ),
        (
            "- Binance spot market status mode: "
            f"`{summary.get('market_status_mode', lifecycle.get('market_status_mode'))}`"
        ),
        f"- Clean sample schema: `{summary['clean_sample_schema_version']}`",
        (
            "- Dataset clean enough for Phase 4.2: "
            f"{summary['phase_4_1_pass']}"
        ),
        "",
        "## Lifecycle",
        "",
        f"`{json.dumps(lifecycle, sort_keys=True)}`",
        "",
        "## Queue",
        "",
        f"`{json.dumps(queue, sort_keys=True)}`",
    ]
    return "\n".join(lines) + "\n"


def _ensure_file(path: Path, *, reset: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if reset or not path.exists():
        path.write_text("", encoding="utf-8")


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(_json_dumps(row) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_dumps(payload, indent=2) + "\n", encoding="utf-8")


def _json_dumps(payload: Any, *, indent: int | None = None) -> str:
    return json.dumps(
        payload,
        default=_json_default,
        indent=indent,
        sort_keys=True,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _levels_to_json(
    levels: tuple[tuple[Decimal, Decimal], ...],
) -> list[list[str]]:
    return [[format(price, "f"), format(size, "f")] for price, size in levels]


def _decimal_to_str(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _sample_decimal(value: Any, field: str, violations: list[str]) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        if field == "price":
            violations.append("invalid_price_level")
        elif field == "size":
            violations.append("invalid_size_level")
        else:
            violations.append(f"{field}_invalid")
        return None
    if not parsed.is_finite():
        if field == "price":
            violations.append("non_finite_price")
        elif field == "size":
            violations.append("non_finite_size")
        else:
            violations.append(f"{field}_non_finite")
        return None
    return parsed


def _validate_sample_levels(
    levels: list[Any],
    *,
    side: str,
    violations: list[str],
) -> list[Decimal]:
    prices: list[Decimal] = []
    for row in levels:
        if not isinstance(row, list | tuple) or len(row) < 2:
            violations.append(f"{side}_level_malformed")
            continue
        price = _sample_decimal(row[0], "price", violations)
        size = _sample_decimal(row[1], "size", violations)
        if price is None or size is None:
            continue
        if price <= 0:
            violations.append("negative_price")
        if size < 0:
            violations.append("negative_size")
        if size == 0:
            violations.append("zero_size_level")
        prices.append(price)
    return prices


def _utc_iso_now() -> str:
    return datetime.fromtimestamp(utc_now_ns() / 1_000_000_000, tz=UTC).isoformat()


def _numeric_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _duration_ms(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    return (end_ns - start_ns) / 1_000_000.0


def _earliest_available_stage(
    stages: dict[str, int | None],
    stage_order: tuple[str, ...],
) -> str | None:
    for stage in stage_order:
        value = stages.get(stage)
        if isinstance(value, int) and not isinstance(value, bool):
            return stage
    return None


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * pct)))
    return ordered[index]
