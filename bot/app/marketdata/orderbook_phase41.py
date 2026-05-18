from __future__ import annotations

import asyncio
import json
import time
from collections import Counter, deque
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

import aiohttp

from app.core.clock import monotonic_now_ns, utc_now_ns
from app.core.events import DepthUpdate
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
from app.marketdata.queue_monitor import QueueBackpressureMonitor
from app.marketdata.ws_lifecycle import WSLifecycleTracker

DEFAULT_ORDERBOOK_QUALITY_REPORT = Path("data/debug/orderbook_quality_report.json")
DEFAULT_ORDERBOOK_QUALITY_SAMPLES = Path("data/debug/orderbook_quality_samples.jsonl")
DEFAULT_ORDERBOOK_MISMATCH_CASES = Path("data/debug/orderbook_mismatch_cases.jsonl")
DEFAULT_BOOK_INCOMPLETE_CASES = Path("data/debug/book_incomplete_cases.jsonl")
DEFAULT_SEQUENCE_GAP_CASES = Path("data/debug/sequence_gap_cases.jsonl")
DEFAULT_DUPLICATE_UPDATE_CASES = Path("data/debug/duplicate_update_cases.jsonl")
DEFAULT_INVALID_DELTA_CASES = Path("data/debug/invalid_delta_cases.jsonl")
DEFAULT_STALE_PERIOD_CASES = Path("data/debug/stale_period_cases.jsonl")
DEFAULT_WS_LIFECYCLE_REPORT = Path("data/debug/ws_lifecycle_report.json")
DEFAULT_ORDERBOOK_CLEAN_SAMPLES = Path("data/dataset/orderbook_clean_samples.jsonl")
DEFAULT_ORDERBOOK_MARKDOWN_REPORT = Path("docs/reports/phase_4_1_orderbook_quality_report.md")

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
    lifecycle_report: Path = REPO_ROOT / DEFAULT_WS_LIFECYCLE_REPORT
    clean_samples: Path = REPO_ROOT / DEFAULT_ORDERBOOK_CLEAN_SAMPLES
    markdown_report: Path = REPO_ROOT / DEFAULT_ORDERBOOK_MARKDOWN_REPORT


class OrderbookDebugRecorder:
    def __init__(
        self,
        *,
        paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
        max_cases: int = 256,
        reset_files: bool = True,
    ) -> None:
        self.paths = paths
        self.max_cases = max_cases
        self.quality_samples: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.mismatch_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.book_incomplete_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.sequence_gap_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.duplicate_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.invalid_delta_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        self.stale_period_cases: deque[dict[str, Any]] = deque(maxlen=max_cases)
        for path in (
            self.paths.quality_samples,
            self.paths.mismatch_cases,
            self.paths.book_incomplete_cases,
            self.paths.sequence_gap_cases,
            self.paths.duplicate_update_cases,
            self.paths.invalid_delta_cases,
            self.paths.stale_period_cases,
            self.paths.clean_samples,
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
        _append_jsonl(self.paths.quality_samples, row)

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
        _append_jsonl(self.paths.mismatch_cases, row)

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
        _append_jsonl(self.paths.book_incomplete_cases, row)

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
        _append_jsonl(self.paths.sequence_gap_cases, row)

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
        _append_jsonl(self.paths.duplicate_update_cases, row)

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
        _append_jsonl(self.paths.invalid_delta_cases, row)

    def record_stale_period_case(self, period: dict[str, Any]) -> None:
        self.stale_period_cases.append(period)
        _append_jsonl(self.paths.stale_period_cases, period)

    def record_clean_sample(
        self,
        sample: dict[str, Any],
    ) -> None:
        _append_jsonl(self.paths.clean_samples, sample)


class OrderbookPhase41Processor:
    def __init__(
        self,
        *,
        symbols: Iterable[str],
        paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
        depth_n: int = 20,
        stale_after_ms: float = 1_000.0,
        queue_lag_warning_ms: float = 50.0,
        queue_lag_severe_ms: float = 250.0,
        debug_max_cases: int = 256,
        monotonic_clock: Callable[[], int] = monotonic_now_ns,
    ) -> None:
        self.states = {symbol.upper(): OrderbookState(symbol) for symbol in symbols}
        self.paths = paths
        self.depth_n = depth_n
        self.monotonic_clock = monotonic_clock
        self.stale_threshold_ms = stale_after_ms
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
        self.debug = OrderbookDebugRecorder(paths=paths, max_cases=debug_max_cases)
        self.counters: Counter[str] = Counter()
        self.blocked_by_quality_error: Counter[str] = Counter()
        self.snapshot_copy_us_samples: deque[float] = deque(maxlen=4096)
        self.stale_periods: deque[dict[str, Any]] = deque(maxlen=256)
        self._open_stale_periods: dict[str, dict[str, Any]] = {}
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
    ) -> OrderbookApplyResult:
        state = self.state_for(symbol)
        result = state.apply_snapshot(
            bids=bids,
            asks=asks,
            last_update_id=last_update_id,
            local_recv_monotonic_ns=local_recv_monotonic_ns or self.monotonic_clock(),
            generation=generation,
        )
        if result.accepted:
            self.lifecycle.on_snapshot_loaded()
            self._close_stale_period(symbol.upper(), now_monotonic_ns=state.last_book_update_monotonic_ns)
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
    ) -> OrderbookApplyResult:
        self.counters["messages_received"] += 1
        self.counters["messages_parsed"] += 1
        state = self.state_for(event.symbol)
        recv_monotonic_ns = event.recv_monotonic_ns or self.monotonic_clock()
        state.record_message_recv(recv_monotonic_ns)
        self.check_stale_periods(
            now_monotonic_ns=recv_monotonic_ns,
            symbols=(event.symbol,),
        )
        result = state.apply_delta(
            first_update_id=event.first_update_id,
            final_update_id=event.final_update_id,
            previous_final_update_id=event.previous_final_update_id,
            bids=[(level.price, level.size) for level in event.bids],
            asks=[(level.price, level.size) for level in event.asks],
            local_recv_monotonic_ns=recv_monotonic_ns,
        )
        if result.accepted:
            self.counters["deltas_accepted"] += 1
            self._close_stale_period(event.symbol, now_monotonic_ns=recv_monotonic_ns)
            if (
                queue_lag_ms is not None
                and queue_lag_ms > self.validator.queue_lag_severe_ms
            ):
                state.mark_not_ready(
                    "queue_lag_exceeded",
                    local_recv_monotonic_ns=recv_monotonic_ns,
                )
            snapshot = self.copy_snapshot(state)
            quality = self.validator.validate(
                snapshot,
                state=state,
                now_monotonic_ns=self.monotonic_clock(),
                queue_lag_ms=queue_lag_ms,
            )
            self.debug.record_quality_sample(snapshot, quality)
            self._maybe_emit_or_block_sample(
                snapshot,
                quality,
                exchange_event_ts=event.exchange_event_ts,
            )
        else:
            self.counters["deltas_rejected"] += 1
            if result.status == "delta_before_snapshot":
                self.lifecycle.on_delta_before_snapshot()
            elif result.status == "duplicate_update":
                self.lifecycle.on_duplicate()
                self.debug.record_duplicate_case(result, symbol=event.symbol)
            elif result.status in {"sequence_gap_or_reset", "sequence_bridge_failed"}:
                self.lifecycle.on_sequence_gap()
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
        snapshot = state.copy_snapshot(top_n=self.depth_n)
        elapsed_us = (time.perf_counter_ns() - start) / 1_000.0
        self.snapshot_copy_us_samples.append(elapsed_us)
        return snapshot

    def cleanup_symbol(self, symbol: str) -> None:
        state = self.states.pop(symbol.upper(), None)
        if state is not None:
            state.cleanup()
        self._open_stale_periods.pop(symbol.upper(), None)

    def check_stale_periods(
        self,
        *,
        now_monotonic_ns: int | None = None,
        symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        now_ns = now_monotonic_ns if now_monotonic_ns is not None else self.monotonic_clock()
        stale_periods: list[dict[str, Any]] = []
        selected_symbols = tuple(symbol.upper() for symbol in symbols) if symbols else tuple(self.states)
        for symbol in selected_symbols:
            state = self.states.get(symbol)
            if state is None:
                continue
            age_ms = state.book_age_ms(now_ns)
            if age_ms is None:
                continue
            self.max_book_age_ms = max(self.max_book_age_ms, age_ms)
            self.last_book_update_age_ms_at_report = age_ms
            if age_ms <= self.stale_threshold_ms:
                self._close_stale_period(symbol, now_monotonic_ns=now_ns)
                continue
            period = self._open_stale_periods.get(symbol)
            if period is None:
                started_ns = int(
                    (state.last_book_update_monotonic_ns or now_ns)
                    + self.stale_threshold_ms * 1_000_000
                )
                period = {
                    "case_type": "stale_period",
                    "symbol": symbol,
                    "stale_threshold_ms": self.stale_threshold_ms,
                    "started_monotonic_ns": started_ns,
                    "ended_monotonic_ns": None,
                    "max_age_ms": age_ms,
                    "reason": "no_successful_book_update",
                    "snapshot_ready": state.snapshot_ready,
                    "ready_to_emit": state.ready_to_emit,
                    "last_book_update_monotonic_ns": state.last_book_update_monotonic_ns,
                    "last_message_recv_monotonic_ns": state.last_message_recv_monotonic_ns,
                    "generation_id": state.generation,
                }
                self._open_stale_periods[symbol] = period
                self.stale_periods.append(period)
                self.counters["stale_book_count"] += 1
                state.mark_not_ready(
                    "stale_book",
                    local_recv_monotonic_ns=now_ns,
                    advance_generation=True,
                )
                period["snapshot_ready"] = state.snapshot_ready
                period["ready_to_emit"] = state.ready_to_emit
                period["generation_id"] = state.generation
                self.lifecycle.observe_ready_false(state)
                self.debug.record_stale_period_case(period)
            else:
                period["max_age_ms"] = max(float(period["max_age_ms"]), age_ms)
                period["snapshot_ready"] = state.snapshot_ready
                period["ready_to_emit"] = state.ready_to_emit
                period["last_message_recv_monotonic_ns"] = state.last_message_recv_monotonic_ns
                period["generation_id"] = state.generation
            stale_periods.append(period)
        return stale_periods

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
        self.check_stale_periods(now_monotonic_ns=self.monotonic_clock())
        snapshot_copy_p99_us = _percentile(list(self.snapshot_copy_us_samples), 0.99)
        lifecycle_report = self.lifecycle.report()
        queue_report = self.queue_monitor.report()
        summary = {
            "duration_sec": duration_sec,
            "messages_received": int(self.counters["messages_received"]),
            "messages_parsed": int(self.counters["messages_parsed"]),
            "deltas_accepted": int(self.counters["deltas_accepted"]),
            "deltas_rejected": int(self.counters["deltas_rejected"]),
            "sequence_gap_count": int(lifecycle_report["sequence_gap_count"]),
            "sequence_gap_or_reset_count": int(lifecycle_report["sequence_gap_count"]),
            "duplicates_skipped": int(lifecycle_report["duplicate_messages_skipped"]),
            "samples_emitted": int(self.counters["samples_emitted"]),
            "samples_blocked": int(self.counters["samples_blocked"]),
            "sample_before_ready_count": int(lifecycle_report["delta_before_snapshot_count"]),
            "invalid_delta_count": int(self.counters["invalid_delta_count"]),
            "ready_to_emit_disabled_count": int(self.counters["ready_to_emit_disabled_count"]),
            "ready_to_emit_violation_count": int(self.counters["ready_to_emit_violation_count"]),
            "clean_sample_schema_violation_count": int(self.counters["clean_sample_schema_violation_count"]),
            "strict_mismatch_count": int(self.counters["strict_mismatch_count"]),
            "tolerant_mismatch_count": int(self.counters["tolerant_mismatch_count"]),
            "crossed_book_count": int(self.counters["crossed_book_count"]),
            "book_empty_count": int(self.blocked_by_quality_error.get("book_empty", 0)),
            "one_side_missing_count": int(self.blocked_by_quality_error.get("one_side_missing", 0)),
            "stale_book_count": int(self.counters["stale_book_count"]),
            "stale_periods": list(self.stale_periods),
            "max_book_age_ms": self.max_book_age_ms,
            "last_book_update_age_ms_at_report": self.last_book_update_age_ms_at_report,
            "stale_threshold_ms": self.stale_threshold_ms,
            "queue_backpressure_events": int(queue_report["queue_backpressure_events"]),
            "max_queue_lag_ms": queue_report["enqueue_to_dequeue_lag_ms_max"],
            "snapshot_copy_p99_us": snapshot_copy_p99_us,
            "snapshot_copy_budget_us": SNAPSHOT_COPY_BUDGET_US,
            "snapshot_copy_budget_met": snapshot_copy_p99_us <= SNAPSHOT_COPY_BUDGET_US,
            "blocked_by_quality_error": dict(sorted(self.blocked_by_quality_error.items())),
            "queue": queue_report,
            "lifecycle": lifecycle_report,
            "clean_sample_schema_version": PHASE_4_1_SCHEMA_VERSION,
            "reported_best_validation_enabled": False,
            "market_status_known": False,
        }
        phase_pass, failure_reasons = evaluate_phase_4_1_pass(summary)
        summary["phase_4_1_pass"] = phase_pass
        summary["phase_4_1_status"] = "pass" if phase_pass else "fail"
        summary["phase_4_1_failure_reasons"] = failure_reasons
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

    def _maybe_emit_or_block_sample(
        self,
        snapshot: OrderbookSnapshot,
        quality: OrderbookQualityResult,
        *,
        exchange_event_ts: int | None,
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

        sample = clean_sample_from_snapshot(
            snapshot,
            quality,
            depth_n=self.depth_n,
            exchange_event_ts=exchange_event_ts,
        )
        self.debug.record_clean_sample(sample)
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
        "stale_book_count",
        "ready_to_emit_violation_count",
        "clean_sample_schema_violation_count",
    )
    for field in blocker_fields:
        if _numeric_value(report.get(field)) > 0:
            failure_reasons.append(f"{field} > 0")

    queue = report.get("queue")
    if isinstance(queue, dict):
        if _numeric_value(queue.get("queue_dropped_messages")) > 0:
            failure_reasons.append("queue_dropped_messages > 0")
        if _numeric_value(queue.get("queue_backpressure_events")) > 0:
            failure_reasons.append("queue_backpressure_events > 0")

    if not bool(report.get("snapshot_copy_budget_met", True)):
        failure_reasons.append("snapshot_copy_p99_us > snapshot_copy_budget_us")

    return not failure_reasons, failure_reasons


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


async def run_orderbook_phase41_capture(
    *,
    symbol: str = "BTCUSDT",
    duration_sec: float = 900.0,
    depth_n: int = 20,
    ws_url: str = "wss://stream.binance.com:9443/ws",
    rest_url: str = "https://api.binance.com",
    paths: OrderbookPhase41Paths = OrderbookPhase41Paths(),
) -> dict[str, Any]:
    symbol = symbol.upper()
    processor = OrderbookPhase41Processor(
        symbols=(symbol,),
        paths=paths,
        depth_n=depth_n,
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
            envelope = processor.queue_monitor.record_enqueue(
                event,
                enqueue_monotonic_ns=event.recv_monotonic_ns or monotonic_now_ns(),
                queue_size=queue.qsize() + 1,
            )
            try:
                queue.put_nowait(envelope)
            except asyncio.QueueFull:
                processor.queue_monitor.record_drop()
                processor.lifecycle.on_queue_dropped()

    reader_task = asyncio.create_task(_reader())
    start_mono = monotonic_now_ns()
    try:
        async with aiohttp.ClientSession() as session:
            await _load_fresh_snapshot(
                processor,
                session=session,
                symbol=symbol,
                rest_url=rest_url,
            )
            deadline = time.monotonic() + duration_sec
            while time.monotonic() < deadline:
                timeout = max(0.01, min(1.0, deadline - time.monotonic()))
                try:
                    envelope = await asyncio.wait_for(queue.get(), timeout=timeout)
                except TimeoutError:
                    stale_periods = processor.check_stale_periods(
                        now_monotonic_ns=monotonic_now_ns()
                    )
                    if stale_periods:
                        await _load_fresh_snapshot(
                            processor,
                            session=session,
                            symbol=symbol,
                            rest_url=rest_url,
                        )
                    continue
                dequeue_ns = monotonic_now_ns()
                backpressure_before = processor.queue_monitor.queue_backpressure_events
                queue_lag_ms = processor.queue_monitor.record_dequeue(
                    envelope,
                    dequeue_monotonic_ns=dequeue_ns,
                    queue_size=queue.qsize(),
                )
                result = processor.process_depth_update(
                    envelope.payload,
                    queue_lag_ms=queue_lag_ms,
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
                    "invalid_delta_levels",
                    "delta_before_snapshot",
                }:
                    await _load_fresh_snapshot(
                        processor,
                        session=session,
                        symbol=symbol,
                        rest_url=rest_url,
                    )
    finally:
        stop.set()
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        processor.lifecycle.on_disconnect()
    duration = (monotonic_now_ns() - start_mono) / 1_000_000_000.0
    return processor.write_reports(duration_sec=duration)


async def _load_fresh_snapshot(
    processor: OrderbookPhase41Processor,
    *,
    session: aiohttp.ClientSession,
    symbol: str,
    rest_url: str,
) -> None:
    processor.lifecycle.on_snapshot_refresh()
    snapshot = await fetch_binance_depth_snapshot(
        session,
        symbol=symbol,
        rest_url=rest_url,
        limit=1000,
    )
    processor.load_snapshot(
        symbol,
        bids=snapshot["bids"],
        asks=snapshot["asks"],
        last_update_id=int(snapshot["lastUpdateId"]),
        local_recv_monotonic_ns=processor.monotonic_clock(),
    )


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
        f"- Stale threshold ms: {summary.get('stale_threshold_ms')}",
        f"- Max book age ms: {summary.get('max_book_age_ms')}",
        f"- Last book update age ms at report: {summary.get('last_book_update_age_ms_at_report')}",
        f"- Stale periods: `{json.dumps(summary.get('stale_periods', []), sort_keys=True)}`",
        f"- Queue backpressure events: {summary['queue_backpressure_events']}",
        f"- Max queue lag ms: {summary['max_queue_lag_ms']}",
        (
            "- Snapshot copy p99 us: "
            f"{summary['snapshot_copy_p99_us']} "
            f"(budget {summary['snapshot_copy_budget_us']}, met={summary['snapshot_copy_budget_met']})"
        ),
        (
            "- Binance lifecycle placeholders unknown: "
            f"{not lifecycle['market_status_known'] and lifecycle['market_paused_count'] == 0 and lifecycle['market_resolved_count'] == 0}"
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


def _numeric_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * pct)))
    return ordered[index]
