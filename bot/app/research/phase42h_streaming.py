from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
import json
import math
import sqlite3
import tempfile
from typing import Any, Iterator

from app.research.clock_sync_receive_lag import (
    build_corrected_hybrid_label,
    corrected_receive_lag_ms,
)
from app.research.orderbook_labeled_dataset import NS_PER_MS, _clean_sample_errors
from app.research.reference_feed_benchmark import (
    AGGTRADE_REFERENCE_EVENTS,
    BENCHMARK_LABELS,
    BOOKTICKER_REFERENCE_QUOTES,
    REFERENCE_SOURCES,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    SEMANTIC_DESCRIPTIONS,
    SEMANTIC_TYPES,
    TRADE_REFERENCE_EVENTS,
    build_reference_label,
    depth_reference_event,
    reference_event_id,
    reference_price,
    sample_mid_price,
    sample_spread_bps,
    validate_reference_event_schema,
)
from app.research.time_protocol_benchmark import (
    ALLOWED_CLOCK_SKEW_MS,
    HYBRID_BUDGETS_MS,
    HORIZON_MS,
    HORIZON_NAME,
    TIME_PROTOCOL_LABELS,
    build_hybrid_label,
    feature_exchange_ts_ms,
    receive_lag_ms,
    source_exchange_ts_ms,
)


JSONL_CHUNK_SIZE = 1024 * 1024
MAX_ERROR_SAMPLES = 10


@dataclass
class BoundedSamples:
    max_samples: int = MAX_ERROR_SAMPLES
    count: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)

    def add(self, sample: dict[str, Any]) -> None:
        self.count += 1
        if len(self.samples) < self.max_samples:
            self.samples.append(sample)


@dataclass
class JsonlStreamReport:
    path: str
    file_exists: bool = False
    nonblank_line_count: int = 0
    object_count: int = 0
    malformed_line_count: int = 0
    malformed_samples: list[dict[str, Any]] = field(default_factory=list)
    max_malformed_samples: int = MAX_ERROR_SAMPLES

    def record_malformed(self, sample: dict[str, Any]) -> None:
        self.malformed_line_count += 1
        if len(self.malformed_samples) < self.max_malformed_samples:
            self.malformed_samples.append(sample)


@dataclass
class SourceIngestStats:
    source: str
    file_exists: bool = True
    reference_event_count: int = 0
    valid_reference_event_count: int = 0
    invalid_reference_event_count: int = 0
    timestamp_monotonic_violations: int = 0
    rows_with_exchange_ts: int = 0
    field_counts: Counter[str] = field(default_factory=Counter)
    first_local_ts: int | None = None
    last_local_ts: int | None = None
    previous_local_ts: int | None = None
    gap_over_100ms_count: int = 0
    gap_over_100ms_total_duration_ms: float = 0.0
    invalid_samples: BoundedSamples = field(default_factory=BoundedSamples)


@dataclass
class CleanIngestStats:
    file_exists: bool
    nonblank_line_count: int = 0
    valid_clean_sample_count: int = 0
    invalid_clean_sample_count: int = 0
    feature_rows_with_exchange_ts: int = 0
    failure_classification: str | None = None
    invalid_samples: BoundedSamples = field(default_factory=BoundedSamples)


@dataclass
class Phase42HStreamingResult:
    clean_sample_count: int
    labeled_sample_count: int
    timestamp_schema: dict[str, Any]
    sources: dict[str, dict[str, Any]]
    leakage_result: dict[str, Any]
    clean_validation: dict[str, Any]
    stream_reports: dict[str, Any]


def stream_jsonl_records(
    path: str | Path,
    *,
    report: JsonlStreamReport | None = None,
) -> Iterator[tuple[int, dict[str, Any]]]:
    target = Path(path)
    if report is not None:
        report.path = _display_path(target)
        report.file_exists = target.exists()
    if not target.exists():
        return
    with target.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            if report is not None:
                report.nonblank_line_count += 1
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                if report is not None:
                    report.record_malformed({"line": line_number, "reason": f"INVALID_JSON:{exc}"})
                continue
            if not isinstance(value, dict):
                if report is not None:
                    report.record_malformed({"line": line_number, "reason": "ROW_NOT_OBJECT"})
                continue
            if report is not None:
                report.object_count += 1
            yield line_number, value


def stream_jsonl_filter(
    source_path: str | Path,
    target_path: str | Path,
    *,
    predicate: Any,
    transform: Any | None = None,
    max_error_samples: int = MAX_ERROR_SAMPLES,
) -> dict[str, Any]:
    report = JsonlStreamReport(path=_display_path(source_path), max_malformed_samples=max_error_samples)
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with target.open("w", encoding="utf-8") as output:
        for _line_number, row in stream_jsonl_records(source_path, report=report):
            if not predicate(row):
                continue
            output_row = transform(row) if transform is not None else row
            output.write(_compact_json(output_row) + "\n")
            written += 1
    return {
        "input": report.__dict__,
        "output_path": _display_path(target),
        "written_count": written,
    }


def summarize_jsonl_stream(
    path: str | Path,
    *,
    max_examples: int = MAX_ERROR_SAMPLES,
) -> dict[str, Any]:
    report = JsonlStreamReport(path=_display_path(path), max_malformed_samples=max_examples)
    examples: list[dict[str, Any]] = []
    for line_number, row in stream_jsonl_records(path, report=report):
        if len(examples) < max_examples:
            examples.append({"line": line_number, "keys": sorted(str(key) for key in row)[:20]})
    return {
        "path": _display_path(path),
        "file_exists": report.file_exists,
        "nonblank_line_count": report.nonblank_line_count,
        "object_count": report.object_count,
        "malformed_line_count": report.malformed_line_count,
        "malformed_samples": report.malformed_samples,
        "examples": examples,
        "max_examples": max_examples,
    }


class SQLiteSeriesStore:
    def __init__(self, directory: Path | None = None) -> None:
        self._tempdir = tempfile.TemporaryDirectory(dir=str(directory) if directory is not None else None)
        self.path = Path(self._tempdir.name) / "phase42h_series.sqlite"
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("CREATE TABLE metric_values (metric TEXT NOT NULL, value REAL NOT NULL)")
        self.connection.execute("CREATE INDEX idx_metric_values_metric_value ON metric_values(metric, value)")
        self._buffer: list[tuple[str, float]] = []

    def add(self, metric: str, value: Any) -> None:
        numeric = _float_or_none(value)
        if numeric is None:
            return
        self._buffer.append((metric, numeric))
        if len(self._buffer) >= 4096:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self.connection.executemany("INSERT INTO metric_values(metric, value) VALUES (?, ?)", self._buffer)
        self.connection.commit()
        self._buffer.clear()

    def summary(self, metric: str) -> dict[str, Any]:
        self.flush()
        count = int(
            self.connection.execute("SELECT COUNT(*) FROM metric_values WHERE metric = ?", (metric,)).fetchone()[0]
        )
        if count <= 0:
            return {"count": 0, "p50": None, "p90": None, "p95": None, "p99": None, "max": None}
        return {
            "count": count,
            "p50": self.percentile(metric, 0.50, count=count),
            "p90": self.percentile(metric, 0.90, count=count),
            "p95": self.percentile(metric, 0.95, count=count),
            "p99": self.percentile(metric, 0.99, count=count),
            "max": self.connection.execute("SELECT MAX(value) FROM metric_values WHERE metric = ?", (metric,)).fetchone()[0],
        }

    def percentile(self, metric: str, percentile: float, *, count: int | None = None) -> float | None:
        self.flush()
        total = count
        if total is None:
            total = int(self.connection.execute("SELECT COUNT(*) FROM metric_values WHERE metric = ?", (metric,)).fetchone()[0])
        if total <= 0:
            return None
        offset = int(round((total - 1) * percentile))
        row = self.connection.execute(
            "SELECT value FROM metric_values WHERE metric = ? ORDER BY value LIMIT 1 OFFSET ?",
            (metric, max(0, min(offset, total - 1))),
        ).fetchone()
        return float(row[0]) if row else None

    def close(self) -> None:
        self.flush()
        self.connection.close()
        self._tempdir.cleanup()


class EventStore:
    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self._tempdir = tempfile.TemporaryDirectory(dir=str(directory))
        self.path = Path(self._tempdir.name) / "phase42h_events.sqlite"
        self.connection = sqlite3.connect(self.path)
        self.connection.execute(
            "CREATE TABLE events (source TEXT NOT NULL, local_ts INTEGER, exchange_ts REAL, row_json TEXT NOT NULL)"
        )
        self.connection.execute("CREATE INDEX idx_events_source_local ON events(source, local_ts)")
        self.connection.execute("CREATE INDEX idx_events_source_exchange ON events(source, exchange_ts)")
        self._buffer: list[tuple[str, int | None, float | None, str]] = []

    def insert(self, source: str, row: dict[str, Any], *, local_ts: int | None, exchange_ts: float | None) -> None:
        self._buffer.append((source, local_ts, exchange_ts, _compact_json(row)))
        if len(self._buffer) >= 4096:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self.connection.executemany(
            "INSERT INTO events(source, local_ts, exchange_ts, row_json) VALUES (?, ?, ?, ?)",
            self._buffer,
        )
        self.connection.commit()
        self._buffer.clear()

    def first_by_local_ts(self, source: str, target_ts: int) -> tuple[dict[str, Any], int] | None:
        self.flush()
        row = self.connection.execute(
            """
            SELECT row_json, local_ts
            FROM events
            WHERE source = ? AND local_ts IS NOT NULL AND local_ts >= ?
            ORDER BY local_ts
            LIMIT 1
            """,
            (source, target_ts),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0]), int(row[1])

    def first_by_exchange_ts(self, source: str, target_ts_ms: float) -> tuple[dict[str, Any], float] | None:
        self.flush()
        row = self.connection.execute(
            """
            SELECT row_json, exchange_ts
            FROM events
            WHERE source = ? AND exchange_ts IS NOT NULL AND exchange_ts >= ?
            ORDER BY exchange_ts, local_ts
            LIMIT 1
            """,
            (source, target_ts_ms),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0]), float(row[1])

    def local_bisect_left(self, source: str, target_ts: int) -> int:
        self.flush()
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM events WHERE source = ? AND local_ts IS NOT NULL AND local_ts < ?",
                (source, target_ts),
            ).fetchone()[0]
        )

    def local_bisect_right(self, source: str, target_ts: int) -> int:
        self.flush()
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM events WHERE source = ? AND local_ts IS NOT NULL AND local_ts <= ?",
                (source, target_ts),
            ).fetchone()[0]
        )

    def exchange_bisect_left(self, source: str, target_ts_ms: float) -> int:
        self.flush()
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM events WHERE source = ? AND exchange_ts IS NOT NULL AND exchange_ts < ?",
                (source, target_ts_ms),
            ).fetchone()[0]
        )

    def close(self) -> None:
        self.flush()
        self.connection.close()
        self._tempdir.cleanup()


class ProtocolMetricAccumulator:
    def __init__(self, series: SQLiteSeriesStore, metric_prefix: str, *, gap_field: str) -> None:
        self.series = series
        self.metric_prefix = metric_prefix
        self.gap_field = gap_field
        self.total = 0
        self.eligible = 0
        self.valid = 0
        self.reasons: Counter[str] = Counter()

    def observe(self, label: dict[str, Any]) -> None:
        self.total += 1
        if label.get("eligible") is True:
            self.eligible += 1
        if label.get("valid") is True:
            self.valid += 1
        else:
            self.reasons[str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")] += 1
        self.series.add(f"{self.metric_prefix}:gap", label.get(self.gap_field))

    def result(self, *, selection_time_basis: str) -> dict[str, Any]:
        reasons = Counter(self.reasons)
        reasons.pop("None", None)
        gaps = self.series.summary(f"{self.metric_prefix}:gap")
        return {
            "horizon_ms": HORIZON_MS,
            "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
            "selection_time_basis": selection_time_basis,
            "eligible_count": self.eligible,
            "valid_count": self.valid,
            "invalid_count": self.total - self.valid,
            "valid_rate_all_rows": self.valid / self.total if self.total else 0.0,
            "valid_rate_eligible_rows": self.valid / self.eligible if self.eligible else 0.0,
            "invalid_reason_counts": dict(sorted(reasons.items())),
            "future_gap_p50_ms": gaps["p50"],
            "future_gap_p90_ms": gaps["p90"],
            "future_gap_p95_ms": gaps["p95"],
            "future_gap_p99_ms": gaps["p99"],
            "future_gap_max_ms": gaps["max"],
        }


class HybridMetricAccumulator:
    def __init__(self, series: SQLiteSeriesStore, metric_prefix: str, *, budget_ms: int, corrected: bool) -> None:
        self.series = series
        self.metric_prefix = metric_prefix
        self.budget_ms = budget_ms
        self.corrected = corrected
        self.total = 0
        self.eligible = 0
        self.valid = 0
        self.reasons: Counter[str] = Counter()
        self.future_over_budget_count = 0

    def observe(self, label: dict[str, Any]) -> None:
        self.total += 1
        if label.get("eligible") is True:
            self.eligible += 1
        if label.get("valid") is True:
            self.valid += 1
        else:
            self.reasons[str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")] += 1
        feature_key = "corrected_feature_receive_lag_ms" if self.corrected else "feature_receive_lag_ms"
        future_key = "corrected_future_receive_lag_ms" if self.corrected else "future_receive_lag_ms"
        self.series.add(f"{self.metric_prefix}:feature_lag", label.get(feature_key))
        future_value = _float_or_none(label.get(future_key))
        if future_value is not None:
            self.series.add(f"{self.metric_prefix}:future_lag", future_value)
            if future_value > self.budget_ms:
                self.future_over_budget_count += 1

    def result(self) -> dict[str, Any]:
        reasons = Counter(self.reasons)
        reasons.pop("None", None)
        feature = self.series.summary(f"{self.metric_prefix}:feature_lag")
        future = self.series.summary(f"{self.metric_prefix}:future_lag")
        if self.corrected:
            return {
                "horizon_ms": HORIZON_MS,
                "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
                "feature_lag_budget_ms": self.budget_ms,
                "future_receive_lag_hard_gate_used": False,
                "future_receive_lag_is_telemetry_only": True,
                "eligible_count": self.eligible,
                "valid_count": self.valid,
                "invalid_count": self.total - self.valid,
                "valid_rate_all_rows": self.valid / self.total if self.total else 0.0,
                "valid_rate_eligible_rows": self.valid / self.eligible if self.eligible else 0.0,
                "invalid_reason_counts": dict(sorted(reasons.items())),
                "corrected_feature_receive_lag_p50_ms": feature["p50"],
                "corrected_feature_receive_lag_p95_ms": feature["p95"],
                "corrected_feature_receive_lag_p99_ms": feature["p99"],
                "corrected_future_receive_lag_p50_ms": future["p50"],
                "corrected_future_receive_lag_p95_ms": future["p95"],
                "corrected_future_receive_lag_p99_ms": future["p99"],
                "cross_stream_receive_reorder_count": reasons.get("CROSS_STREAM_RECEIVE_REORDER", 0),
                "clock_sanity_violation_count": reasons.get("CORRECTED_LAG_CLOCK_SANITY_FAILURE", 0),
            }
        return {
            "horizon_ms": HORIZON_MS,
            "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
            "feature_lag_budget_ms": self.budget_ms,
            "future_receive_lag_hard_gate_used": False,
            "future_receive_lag_is_telemetry_only": True,
            "eligible_count": self.eligible,
            "valid_count": self.valid,
            "invalid_count": self.total - self.valid,
            "valid_rate_all_rows": self.valid / self.total if self.total else 0.0,
            "valid_rate_eligible_rows": self.valid / self.eligible if self.eligible else 0.0,
            "invalid_reason_counts": dict(sorted(reasons.items())),
            "feature_receive_lag_p50_ms": feature["p50"],
            "feature_receive_lag_p95_ms": feature["p95"],
            "feature_receive_lag_p99_ms": feature["p99"],
            "future_receive_lag_p50_ms": future["p50"],
            "future_receive_lag_p95_ms": future["p95"],
            "future_receive_lag_p99_ms": future["p99"],
            "future_receive_lag_over_budget_count": self.future_over_budget_count,
            "cross_stream_receive_reorder_count": reasons.get("CROSS_STREAM_RECEIVE_REORDER", 0),
            "clock_sanity_violation_count": reasons.get("CLOCK_SANITY_VIOLATION", 0),
        }


class LagAccumulator:
    def __init__(self, series: SQLiteSeriesStore, metric_prefix: str, *, prefix: str) -> None:
        self.series = series
        self.metric_prefix = metric_prefix
        self.prefix = prefix
        self.feature_count = 0
        self.future_count = 0
        self.feature_below_negative_skew_count = 0

    def observe(self, *, feature: Any, future: Any) -> None:
        feature_value = _float_or_none(feature)
        future_value = _float_or_none(future)
        if feature_value is not None:
            self.feature_count += 1
            if feature_value < -ALLOWED_CLOCK_SKEW_MS:
                self.feature_below_negative_skew_count += 1
            self.series.add(f"{self.metric_prefix}:feature", feature_value)
        if future_value is not None:
            self.future_count += 1
            self.series.add(f"{self.metric_prefix}:future", future_value)

    def result(self) -> dict[str, Any]:
        feature = self.series.summary(f"{self.metric_prefix}:feature")
        future = self.series.summary(f"{self.metric_prefix}:future")
        prefix = self.prefix
        return {
            f"feature_{prefix}_receive_lag_count": self.feature_count,
            f"feature_{prefix}_receive_lag_p50_ms": feature["p50"],
            f"feature_{prefix}_receive_lag_p95_ms": feature["p95"],
            f"feature_{prefix}_receive_lag_p99_ms": feature["p99"],
            f"future_{prefix}_receive_lag_count": self.future_count,
            f"future_{prefix}_receive_lag_p50_ms": future["p50"],
            f"future_{prefix}_receive_lag_p95_ms": future["p95"],
            f"future_{prefix}_receive_lag_p99_ms": future["p99"],
            f"feature_{prefix}_receive_lag_below_negative_skew_count": self.feature_below_negative_skew_count,
        }


class SourceLabelAccumulators:
    def __init__(self, series: SQLiteSeriesStore, source: str) -> None:
        self.receive = ProtocolMetricAccumulator(series, f"{source}:receive", gap_field="receive_future_gap_ms")
        self.exchange = ProtocolMetricAccumulator(series, f"{source}:exchange", gap_field="exchange_future_gap_ms")
        self.hybrid = {
            budget: HybridMetricAccumulator(series, f"{source}:hybrid:{budget}", budget_ms=budget, corrected=False)
            for budget in HYBRID_BUDGETS_MS
        }
        self.corrected_hybrid = {
            budget: HybridMetricAccumulator(series, f"{source}:corrected_hybrid:{budget}", budget_ms=budget, corrected=True)
            for budget in HYBRID_BUDGETS_MS
        }
        self.raw_lag = LagAccumulator(series, f"{source}:raw_lag", prefix="raw")
        self.corrected_lag = LagAccumulator(series, f"{source}:corrected_lag", prefix="corrected")


def run_phase42h_streaming_finalization(
    *,
    root: str | Path,
    clean_samples_path: str | Path,
    bookticker_path: str | Path = BOOKTICKER_REFERENCE_QUOTES,
    trade_path: str | Path = TRADE_REFERENCE_EVENTS,
    aggtrade_path: str | Path = AGGTRADE_REFERENCE_EVENTS,
    receive_labels_path: str | Path = BENCHMARK_LABELS,
    time_protocol_labels_path: str | Path = TIME_PROTOCOL_LABELS,
    corrected_labels_path: str | Path,
    leakage_output_path: str | Path,
    estimated_clock_offset_ms: float | None,
    clock_offset_drift_valid: bool,
) -> Phase42HStreamingResult:
    root_path = Path(root)
    cache_dir = root_path / "data/cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    event_store = EventStore(cache_dir)
    series = SQLiteSeriesStore(cache_dir)
    try:
        clean_path = _resolve(root_path, clean_samples_path)
        clean_stats, depth_stats, clean_report = _ingest_clean_samples(clean_path, event_store=event_store, series=series)
        reference_stats: dict[str, SourceIngestStats] = {"depth_mid": depth_stats}
        stream_reports: dict[str, Any] = {"clean_samples": clean_report.__dict__}
        for source, relative in (
            ("bookTicker_mid", bookticker_path),
            ("trade_price", trade_path),
            ("aggTrade_price", aggtrade_path),
        ):
            stats, report = _ingest_reference_file(
                _resolve(root_path, relative),
                source=source,
                event_store=event_store,
                series=series,
            )
            reference_stats[source] = stats
            stream_reports[source] = report.__dict__
        event_store.flush()
        timestamp_schema = _build_streaming_timestamp_schema(clean_stats, reference_stats)
        leakage = StreamingLeakageAccumulator()
        label_accumulators = {source: SourceLabelAccumulators(series, source) for source in REFERENCE_SOURCES}
        labeled_count = _write_streaming_labels(
            clean_path=clean_path,
            event_store=event_store,
            timestamp_schema=timestamp_schema,
            receive_labels_path=_resolve(root_path, receive_labels_path),
            time_protocol_labels_path=_resolve(root_path, time_protocol_labels_path),
            corrected_labels_path=_resolve(root_path, corrected_labels_path),
            estimated_clock_offset_ms=estimated_clock_offset_ms,
            clock_offset_drift_valid=clock_offset_drift_valid,
            label_accumulators=label_accumulators,
            leakage=leakage,
        )
        leakage_result = leakage.result(checked_samples=labeled_count)
        _write_json(root_path / leakage_output_path, leakage_result)
        sources = _build_streaming_source_reports(
            reference_stats=reference_stats,
            timestamp_schema=timestamp_schema,
            accumulators=label_accumulators,
        )
        clean_validation = {
            "valid": clean_stats.failure_classification is None,
            "file_exists": clean_stats.file_exists,
            "valid_clean_sample_count": clean_stats.valid_clean_sample_count,
            "invalid_clean_sample_count": clean_stats.invalid_clean_sample_count,
            "failure_classification": clean_stats.failure_classification,
            "violations_sample_count": clean_stats.invalid_samples.count,
            "violations_sample": clean_stats.invalid_samples.samples,
        }
        return Phase42HStreamingResult(
            clean_sample_count=clean_stats.valid_clean_sample_count,
            labeled_sample_count=labeled_count,
            timestamp_schema=timestamp_schema,
            sources=sources,
            leakage_result=leakage_result,
            clean_validation=clean_validation,
            stream_reports=stream_reports,
        )
    finally:
        event_store.close()
        series.close()


def build_latency_stage_profile_streaming(
    samples_path: str | Path,
    *,
    required_stage_names: tuple[str, ...],
    latency_metric_names: tuple[str, ...],
) -> dict[str, Any]:
    path = Path(samples_path)
    report = JsonlStreamReport(path=_display_path(path))
    series = SQLiteSeriesStore(path.parent if path.parent.exists() else None)
    stage_counts: dict[str, dict[str, int]] = {}
    disk_hot_path = False
    debug_hot_path = False
    batch_writer_enabled = False
    earliest_stage_counts: dict[str, int] = {}
    sample_count = 0
    try:
        for _line_number, row in stream_jsonl_records(path, report=report):
            sample_count += 1
            stages = _dict(row.get("stages"))
            metrics = _dict(row.get("metrics"))
            for stage in required_stage_names:
                value = stages.get(stage)
                stats = stage_counts.setdefault(stage, {"available_count": 0, "stage_not_available_count": 0})
                if isinstance(value, bool) or not isinstance(value, int):
                    stats["stage_not_available_count"] += 1
                else:
                    stats["available_count"] += 1
            earliest = str(row.get("earliest_available_receive_stage") or "")
            if earliest:
                earliest_stage_counts[earliest] = earliest_stage_counts.get(earliest, 0) + 1
            for metric in latency_metric_names:
                series.add(f"latency:{metric}", metrics.get(metric))
            series.add("latency:queue_depth", row.get("queue_size_at_enqueue"))
            disk_hot_path = disk_hot_path or row.get("disk_write_on_hot_path") is True
            debug_hot_path = debug_hot_path or row.get("debug_logging_on_hot_path") is True
            batch_writer_enabled = batch_writer_enabled or row.get("batch_writer_enabled") is True
        unavailable = {
            stage: "stage_not_available"
            for stage, stats in stage_counts.items()
            if int(stats["available_count"]) == 0
        }
        earliest_available = _mode(earliest_stage_counts) or (
            "raw_ws_callback_monotonic_ns"
            if "raw_ws_callback_monotonic_ns" not in unavailable
            else "stage_not_available"
        )
        metrics = {metric: _latency_summary(series, f"latency:{metric}") for metric in latency_metric_names}
        return {
            "performed": sample_count > 0,
            "sample_count": sample_count,
            "stage_availability": stage_counts,
            "unavailable_stages": unavailable,
            "socket_recv_monotonic_ns": "stage_not_available"
            if "socket_recv_monotonic_ns" in unavailable
            else "available",
            "earliest_available_receive_stage": earliest_available,
            "metrics": metrics,
            "missing_metrics": sorted(metric for metric, values in metrics.items() if int(values.get("count", 0)) <= 0),
            "queue_depth_from_latency_samples": _latency_summary(series, "latency:queue_depth"),
            "disk_write_on_hot_path": disk_hot_path,
            "debug_logging_on_hot_path": debug_hot_path,
            "batch_writer_enabled": batch_writer_enabled,
            "queue_backpressure_detected": False,
            "stage_profile_path": _display_path(samples_path),
            "streaming_summary": {
                "jsonl": report.__dict__,
                "bounded_memory": True,
            },
        }
    finally:
        series.close()


class StreamingLeakageAccumulator:
    def __init__(self) -> None:
        self.feature_count = 0
        self.label_count = 0
        self.label_by_source = {source: 0 for source in REFERENCE_SOURCES}
        self.violations = BoundedSamples()

    def observe_time_row(self, sample_index: int, row: dict[str, Any]) -> None:
        feature_ts = row.get("local_recv_monotonic_ns")
        quality = _dict(row.get("quality"))
        feature_sources = quality.get("feature_source_indices", {})
        if isinstance(feature_sources, dict):
            for feature_name, source_index in feature_sources.items():
                if isinstance(source_index, int) and source_index > sample_index:
                    self.feature_count += 1
                    self.violations.add(
                        {
                            "type": "feature",
                            "sample_index": sample_index,
                            "feature": feature_name,
                            "source_index": source_index,
                            "reason": "past_feature_uses_future_sample",
                        }
                    )
        labels = row.get("protocol_labels")
        if not isinstance(labels, dict) or not isinstance(feature_ts, int):
            return
        for source in REFERENCE_SOURCES:
            source_labels = labels.get(source)
            if not isinstance(source_labels, dict):
                continue
            for protocol in ("receive_time", "exchange_time"):
                label = source_labels.get(protocol)
                if not isinstance(label, dict):
                    continue
                reason = _streaming_label_leakage_reason(label, protocol_name=protocol)
                if reason is None:
                    continue
                self.label_count += 1
                self.label_by_source[source] += 1
                self.violations.add(
                    {
                        "type": "label",
                        "reference_source": source,
                        "sample_index": sample_index,
                        "protocol": protocol,
                        "reason": reason,
                    }
                )

    def result(self, *, checked_samples: int) -> dict[str, Any]:
        return {
            "performed": True,
            "passed": self.feature_count == 0 and self.label_count == 0,
            "feature_leakage_violations": self.feature_count,
            "label_leakage_violations": self.label_count,
            "label_leakage_violations_by_source": self.label_by_source,
            "checked_samples": checked_samples,
            "checked_sources": list(REFERENCE_SOURCES),
            "checked_horizons": [HORIZON_NAME],
            "violations": self.violations.samples,
            "violation_sample_count": self.violations.count,
            "bounded_violation_samples": True,
        }


def _ingest_clean_samples(
    path: Path,
    *,
    event_store: EventStore,
    series: SQLiteSeriesStore,
) -> tuple[CleanIngestStats, SourceIngestStats, JsonlStreamReport]:
    report = JsonlStreamReport(path=_display_path(path))
    clean = CleanIngestStats(file_exists=path.exists())
    depth = SourceIngestStats(source="depth_mid", file_exists=path.exists())
    if not path.exists():
        clean.failure_classification = "INPUT_FILE_MISSING"
        return clean, depth, report
    for line_number, row in stream_jsonl_records(path, report=report):
        clean.nonblank_line_count += 1
        errors = _clean_sample_errors(row)
        if errors:
            clean.invalid_clean_sample_count += 1
            clean.invalid_samples.add(
                {
                    "line": line_number,
                    "sample_index": clean.valid_clean_sample_count,
                    "symbol": row.get("symbol"),
                    "generation_id": row.get("generation_id"),
                    "last_update_id": row.get("last_update_id"),
                    "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                    "reason": errors,
                    "classification": "INPUT_SCHEMA_FAILURE",
                }
            )
            continue
        clean.valid_clean_sample_count += 1
        if feature_exchange_ts_ms(row) is not None:
            clean.feature_rows_with_exchange_ts += 1
        event = depth_reference_event(row, reference_index=depth.valid_reference_event_count)
        _record_reference_stats(depth, event, "depth_mid", series=series)
        event_store.insert("depth_mid", event, local_ts=_int_or_none(event.get("local_recv_monotonic_ns")), exchange_ts=feature_exchange_ts_ms(event))
    if report.malformed_line_count:
        clean.invalid_clean_sample_count += report.malformed_line_count
        for sample in report.malformed_samples:
            clean.invalid_samples.add({**sample, "classification": "INPUT_SCHEMA_FAILURE"})
    if clean.nonblank_line_count == 0:
        clean.failure_classification = "INPUT_EMPTY"
    elif clean.invalid_clean_sample_count > 0:
        clean.failure_classification = "INPUT_SCHEMA_FAILURE"
    return clean, depth, report


def _ingest_reference_file(
    path: Path,
    *,
    source: str,
    event_store: EventStore,
    series: SQLiteSeriesStore,
) -> tuple[SourceIngestStats, JsonlStreamReport]:
    report = JsonlStreamReport(path=_display_path(path))
    stats = SourceIngestStats(source=source, file_exists=path.exists())
    if not path.exists():
        return stats, report
    for line_number, row in stream_jsonl_records(path, report=report):
        stats.reference_event_count += 1
        errors = validate_reference_event_schema(row, source)
        if errors:
            stats.invalid_reference_event_count += 1
            stats.invalid_samples.add(
                {
                    "line": line_number,
                    "reference_source": source,
                    "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                    "event_id": reference_event_id(row, source),
                    "reason": errors,
                }
            )
            continue
        _record_reference_stats(stats, row, source, series=series)
        selected = source_exchange_ts_ms(row, source)
        event_store.insert(
            source,
            row,
            local_ts=_int_or_none(row.get("local_recv_monotonic_ns")),
            exchange_ts=selected[1] if selected is not None else None,
        )
    if report.malformed_line_count:
        stats.reference_event_count += report.malformed_line_count
        stats.invalid_reference_event_count += report.malformed_line_count
        for sample in report.malformed_samples:
            stats.invalid_samples.add({**sample, "reference_source": source})
    return stats, report


def _record_reference_stats(
    stats: SourceIngestStats,
    row: dict[str, Any],
    source: str,
    *,
    series: SQLiteSeriesStore,
) -> None:
    local_ts = _int_or_none(row.get("local_recv_monotonic_ns"))
    stats.valid_reference_event_count += 1
    if local_ts is not None:
        if stats.first_local_ts is None:
            stats.first_local_ts = local_ts
        stats.last_local_ts = local_ts if stats.last_local_ts is None else max(stats.last_local_ts, local_ts)
        if stats.previous_local_ts is not None:
            gap_ms = (local_ts - stats.previous_local_ts) / NS_PER_MS
            if gap_ms < 0:
                stats.timestamp_monotonic_violations += 1
            else:
                series.add(f"{source}:reference_gap_ms", gap_ms)
                if gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
                    stats.gap_over_100ms_count += 1
                    stats.gap_over_100ms_total_duration_ms += gap_ms - REQUIRED_100MS_MAX_FUTURE_GAP_MS
        stats.previous_local_ts = local_ts
    selected = source_exchange_ts_ms(row, source)
    if selected is not None:
        field_name, _timestamp_ms = selected
        stats.rows_with_exchange_ts += 1
        stats.field_counts[field_name] += 1


def _build_streaming_timestamp_schema(
    clean_stats: CleanIngestStats,
    reference_stats: dict[str, SourceIngestStats],
) -> dict[str, Any]:
    feature_exchange_time_supported = clean_stats.feature_rows_with_exchange_ts > 0
    sources: dict[str, dict[str, Any]] = {}
    for source in REFERENCE_SOURCES:
        stats = reference_stats[source]
        supported = feature_exchange_time_supported and stats.rows_with_exchange_ts > 0
        unsupported_reason = None
        if not feature_exchange_time_supported:
            unsupported_reason = "missing_feature_exchange_timestamp"
        elif stats.rows_with_exchange_ts <= 0:
            unsupported_reason = "missing_exchange_timestamp"
        sources[source] = {
            "source": source,
            "exchange_time_supported": supported,
            "exchange_timestamp_field_used": _preferred_field_used(stats.field_counts, source),
            "reference_rows_with_exchange_ts": stats.rows_with_exchange_ts,
            "valid_reference_event_count": stats.valid_reference_event_count,
            "unsupported_reason": unsupported_reason,
            "exchange_timestamp_fallback_policy": (
                "T_preferred_E_fallback_allowed"
                if source in {"trade_price", "aggTrade_price"}
                else "no_fallback"
            ),
        }
    supported_sources = [source for source, item in sources.items() if item["exchange_time_supported"] is True]
    return {
        "performed": True,
        "status": "pass" if supported_sources else "fail",
        "feature_exchange_time_supported": feature_exchange_time_supported,
        "feature_rows_with_exchange_ts": clean_stats.feature_rows_with_exchange_ts,
        "feature_row_count": clean_stats.valid_clean_sample_count,
        "sources": sources,
        "supported_sources": supported_sources,
    }


def _write_streaming_labels(
    *,
    clean_path: Path,
    event_store: EventStore,
    timestamp_schema: dict[str, Any],
    receive_labels_path: Path,
    time_protocol_labels_path: Path,
    corrected_labels_path: Path,
    estimated_clock_offset_ms: float | None,
    clock_offset_drift_valid: bool,
    label_accumulators: dict[str, SourceLabelAccumulators],
    leakage: StreamingLeakageAccumulator,
) -> int:
    receive_labels_path.parent.mkdir(parents=True, exist_ok=True)
    time_protocol_labels_path.parent.mkdir(parents=True, exist_ok=True)
    corrected_labels_path.parent.mkdir(parents=True, exist_ok=True)
    labeled_count = 0
    with (
        receive_labels_path.open("w", encoding="utf-8") as receive_output,
        time_protocol_labels_path.open("w", encoding="utf-8") as time_output,
        corrected_labels_path.open("w", encoding="utf-8") as corrected_output,
    ):
        for _line_number, sample in stream_jsonl_records(clean_path):
            if _clean_sample_errors(sample):
                continue
            receive_row, time_row, corrected_row = _build_streaming_label_rows(
                sample=sample,
                sample_index=labeled_count,
                event_store=event_store,
                timestamp_schema=timestamp_schema,
                estimated_clock_offset_ms=estimated_clock_offset_ms,
                clock_offset_drift_valid=clock_offset_drift_valid,
            )
            receive_output.write(_compact_json(receive_row) + "\n")
            time_output.write(_compact_json(time_row) + "\n")
            corrected_output.write(_compact_json(corrected_row) + "\n")
            leakage.observe_time_row(labeled_count, time_row)
            _observe_labels(time_row, corrected_row, label_accumulators)
            labeled_count += 1
    return labeled_count


def _build_streaming_label_rows(
    *,
    sample: dict[str, Any],
    sample_index: int,
    event_store: EventStore,
    timestamp_schema: dict[str, Any],
    estimated_clock_offset_ms: float | None,
    clock_offset_drift_valid: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    feature_mid = sample_mid_price(sample)
    feature_spread = sample_spread_bps(sample)
    receive_reference_labels: dict[str, dict[str, Any]] = {}
    protocol_labels: dict[str, dict[str, Any]] = {}
    corrected_protocol_labels: dict[str, dict[str, Any]] = {}
    schema_sources = _dict(timestamp_schema.get("sources"))
    for source in REFERENCE_SOURCES:
        benchmark_label = _build_streaming_reference_label(
            reference_source=source,
            feature_sample=sample,
            feature_mid_price=feature_mid,
            event_store=event_store,
        )
        receive_reference_labels[source] = {HORIZON_NAME: benchmark_label}
        source_schema = _dict(schema_sources.get(source))
        receive_label = _build_streaming_receive_time_label(
            reference_source=source,
            feature_sample=sample,
            feature_mid_price=feature_mid,
            event_store=event_store,
        )
        exchange_label = _build_streaming_exchange_time_label(
            reference_source=source,
            feature_sample=sample,
            feature_mid_price=feature_mid,
            event_store=event_store,
            exchange_time_supported=source_schema.get("exchange_time_supported") is True,
            unsupported_reason=str(source_schema.get("unsupported_reason") or "missing_exchange_timestamp"),
        )
        labels: dict[str, Any] = {
            "receive_time": receive_label,
            "exchange_time": exchange_label,
        }
        for budget_ms in HYBRID_BUDGETS_MS:
            labels[f"hybrid_{budget_ms}ms"] = build_hybrid_label(
                exchange_label=exchange_label,
                feature_lag_budget_ms=budget_ms,
            )
        protocol_labels[source] = labels
        raw_feature = _float_or_none(exchange_label.get("feature_receive_lag_ms"))
        raw_future = _float_or_none(exchange_label.get("future_receive_lag_ms"))
        corrected_feature = corrected_receive_lag_ms(raw_feature, estimated_clock_offset_ms)
        corrected_future = corrected_receive_lag_ms(raw_future, estimated_clock_offset_ms)
        corrected_source: dict[str, Any] = {
            "receive_time": receive_label,
            "exchange_time": exchange_label,
            "raw_feature_receive_lag_ms": raw_feature,
            "corrected_feature_receive_lag_ms": corrected_feature,
            "raw_future_receive_lag_ms": raw_future,
            "corrected_future_receive_lag_ms": corrected_future,
        }
        for budget_ms in HYBRID_BUDGETS_MS:
            corrected_source[f"corrected_hybrid_{budget_ms}ms"] = build_corrected_hybrid_label(
                exchange_label=exchange_label,
                corrected_feature_receive_lag_ms=corrected_feature,
                corrected_future_receive_lag_ms=corrected_future,
                feature_lag_budget_ms=budget_ms,
                clock_offset_drift_valid=clock_offset_drift_valid,
            )
        corrected_protocol_labels[source] = corrected_source
    receive_row = {
        "schema_version": "orderbook_reference_benchmark_v1",
        "symbol": sample.get("symbol"),
        "source": sample.get("source"),
        "generation_id": sample.get("generation_id"),
        "state_version": sample.get("state_version"),
        "snapshot_version": sample.get("snapshot_version"),
        "last_update_id": sample.get("last_update_id"),
        "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
        "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
        "exchange_event_ts": sample.get("exchange_event_ts"),
        "feature_best_bid": _float_or_none(sample.get("best_bid")),
        "feature_best_ask": _float_or_none(sample.get("best_ask")),
        "feature_mid_price": feature_mid,
        "feature_spread_bps": feature_spread,
        "reference_labels": receive_reference_labels,
        "quality": {
            "input_clean_sample_valid": True,
            "feature_source_indices": {},
            "current_index": sample_index,
            "future_label_policy": "first_reference_event_at_or_after_target_time",
            "max_future_gap_policy_ms": {HORIZON_NAME: REQUIRED_100MS_MAX_FUTURE_GAP_MS},
        },
    }
    time_row = {
        "schema_version": "orderbook_time_protocol_benchmark_v1",
        "symbol": sample.get("symbol"),
        "source": sample.get("source"),
        "generation_id": sample.get("generation_id"),
        "state_version": sample.get("state_version"),
        "snapshot_version": sample.get("snapshot_version"),
        "last_update_id": sample.get("last_update_id"),
        "local_recv_monotonic_ns": sample.get("local_recv_monotonic_ns"),
        "local_recv_wall_ts": sample.get("local_recv_wall_ts"),
        "exchange_event_ts": sample.get("exchange_event_ts"),
        "feature_exchange_ts_ms": feature_exchange_ts_ms(sample),
        "feature_best_bid": _float_or_none(sample.get("best_bid")),
        "feature_best_ask": _float_or_none(sample.get("best_ask")),
        "feature_mid_price": feature_mid,
        "feature_spread_bps": feature_spread,
        "protocol_labels": protocol_labels,
        "quality": {
            "input_clean_sample_valid": True,
            "feature_source_indices": {},
            "current_index": sample_index,
            "future_label_policy": "first_reference_event_at_or_after_target_time",
            "exchange_time_selection_basis": "exchange_ts",
            "receive_time_selection_basis": "local_recv_monotonic_ns",
            "future_receive_lag_hard_gate_used": False,
            "max_future_gap_policy_ms": {HORIZON_NAME: REQUIRED_100MS_MAX_FUTURE_GAP_MS},
            "hybrid_feature_lag_budgets_ms": list(HYBRID_BUDGETS_MS),
        },
    }
    corrected_row = {
        **time_row,
        "schema_version": "phase_4_2h_corrected_time_protocol_v1",
        "corrected_protocol_labels": corrected_protocol_labels,
        "clock_offset_estimator": "low_rtt_trimmed_median",
        "estimated_clock_offset_ms": estimated_clock_offset_ms,
        "future_receive_lag_hard_gate_used": False,
    }
    return receive_row, time_row, corrected_row


def _build_streaming_reference_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    event_store: EventStore,
) -> dict[str, Any]:
    feature_ts = feature_sample.get("local_recv_monotonic_ns")
    if not isinstance(feature_ts, int):
        return build_reference_label(
            reference_source=reference_source,
            feature_sample=feature_sample,
            feature_mid_price=feature_mid_price,
            references=[],
            reference_timestamps_ns=[],
        )
    target_ts = feature_ts + HORIZON_MS * NS_PER_MS
    first_after_feature = event_store.local_bisect_right(reference_source, feature_ts)
    future_index = event_store.local_bisect_left(reference_source, target_ts)
    future = event_store.first_by_local_ts(reference_source, target_ts)
    base = _reference_label_base(
        reference_source=reference_source,
        target_ts=target_ts,
        first_after_feature=first_after_feature,
        future_index=None,
    )
    feature_mid_price_value = _float_or_none(feature_mid_price)
    if feature_mid_price_value is None or feature_mid_price_value <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    if future is None:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_reference, future_ts = future
    return _complete_reference_like_label(
        base=base,
        reference_source=reference_source,
        feature_mid_price=feature_mid_price_value,
        future_reference=future_reference,
        future_ts=future_ts,
        target_ts=target_ts,
        future_index=future_index,
        receive_field_name="future_gap_ms",
    )


def _build_streaming_receive_time_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    event_store: EventStore,
) -> dict[str, Any]:
    feature_ts = feature_sample.get("local_recv_monotonic_ns")
    target_ts = feature_ts + HORIZON_MS * NS_PER_MS if isinstance(feature_ts, int) else None
    first_after_feature = event_store.local_bisect_right(reference_source, feature_ts) if isinstance(feature_ts, int) else None
    base = {
        "protocol": "receive_time_label_protocol",
        "reference_source": reference_source,
        "horizon_ms": HORIZON_MS,
        "target_local_recv_monotonic_ns": target_ts,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "first_reference_index_after_feature": first_after_feature,
        "future_reference_index": None,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_event_id": None,
        "future_reference_price": None,
        "future_gap_ms": None,
        "receive_future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "eligible": False,
        "valid": False,
        "invalid_reason": None,
    }
    if not isinstance(feature_ts, int) or target_ts is None:
        return {**base, "invalid_reason": "FEATURE_TIMESTAMP_INVALID"}
    feature_mid_price_value = _float_or_none(feature_mid_price)
    if feature_mid_price_value is None or feature_mid_price_value <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    future_index = event_store.local_bisect_left(reference_source, target_ts)
    future = event_store.first_by_local_ts(reference_source, target_ts)
    if future is None:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_reference, future_ts = future
    return _complete_reference_like_label(
        base=base,
        reference_source=reference_source,
        feature_mid_price=feature_mid_price_value,
        future_reference=future_reference,
        future_ts=future_ts,
        target_ts=target_ts,
        future_index=future_index,
        receive_field_name="receive_future_gap_ms",
        mark_eligible=True,
    )


def _build_streaming_exchange_time_label(
    *,
    reference_source: str,
    feature_sample: dict[str, Any],
    feature_mid_price: float | None,
    event_store: EventStore,
    exchange_time_supported: bool,
    unsupported_reason: str,
) -> dict[str, Any]:
    feature_exchange_ms = feature_exchange_ts_ms(feature_sample)
    feature_recv_ns = feature_sample.get("local_recv_monotonic_ns")
    target_exchange_ms = feature_exchange_ms + HORIZON_MS if feature_exchange_ms is not None else None
    feature_receive_lag = receive_lag_ms(
        local_recv_wall_ts=feature_sample.get("local_recv_wall_ts"),
        exchange_ts_ms=feature_exchange_ms,
    )
    base = {
        "protocol": "exchange_time_label_protocol",
        "reference_source": reference_source,
        "horizon_ms": HORIZON_MS,
        "target_exchange_ts_ms": target_exchange_ms,
        "feature_exchange_ts_ms": feature_exchange_ms,
        "feature_local_recv_monotonic_ns": feature_recv_ns,
        "feature_receive_lag_ms": feature_receive_lag,
        "selection_time_basis": "exchange_ts",
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "future_reference_index": None,
        "future_reference_exchange_ts_ms": None,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_event_id": None,
        "future_reference_price": None,
        "future_receive_lag_ms": None,
        "exchange_future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "eligible": False,
        "valid": False,
        "invalid_reason": None,
    }
    if not exchange_time_supported:
        return {**base, "invalid_reason": unsupported_reason}
    if feature_exchange_ms is None or target_exchange_ms is None:
        return {**base, "invalid_reason": "FEATURE_EXCHANGE_TIMESTAMP_MISSING"}
    feature_mid_price_value = _float_or_none(feature_mid_price)
    if feature_mid_price_value is None or feature_mid_price_value <= 0:
        return {**base, "invalid_reason": "CURRENT_MID_INVALID"}
    future_index = event_store.exchange_bisect_left(reference_source, target_exchange_ms)
    future = event_store.first_by_exchange_ts(reference_source, target_exchange_ms)
    if future is None:
        return {**base, "invalid_reason": "NO_FUTURE_REFERENCE"}
    future_reference, future_exchange_ms = future
    future_gap_ms = future_exchange_ms - target_exchange_ms
    future_price = reference_price(future_reference, reference_source)
    future_recv_ns = future_reference.get("local_recv_monotonic_ns")
    base.update(
        {
            "future_reference_index": future_index,
            "future_reference_exchange_ts_ms": future_exchange_ms,
            "future_reference_local_recv_monotonic_ns": future_recv_ns,
            "future_reference_event_id": reference_event_id(future_reference, reference_source),
            "future_reference_price": future_price,
            "future_receive_lag_ms": receive_lag_ms(
                local_recv_wall_ts=future_reference.get("local_recv_wall_ts"),
                exchange_ts_ms=future_exchange_ms,
            ),
            "exchange_future_gap_ms": future_gap_ms,
            "eligible": True,
        }
    )
    if future_gap_ms < 0:
        return {**base, "invalid_reason": "LABEL_LEAKAGE_FUTURE_BEFORE_TARGET"}
    if future_gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    future_price_value = _float_or_none(future_price)
    if future_price_value is None or future_price_value <= 0:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_PRICE_INVALID"}
    return {
        **base,
        "return_bps": _compute_return_bps(feature_mid_price_value, future_price_value),
        "direction": _direction_label(_compute_return_bps(feature_mid_price_value, future_price_value)),
        "valid": True,
        "invalid_reason": None,
    }


def _reference_label_base(
    *,
    reference_source: str,
    target_ts: int | None,
    first_after_feature: int | None,
    future_index: int | None,
) -> dict[str, Any]:
    return {
        "reference_source": reference_source,
        "horizon_ms": HORIZON_MS,
        "target_local_recv_monotonic_ns": target_ts,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "first_reference_index_after_feature": first_after_feature,
        "future_reference_index": future_index,
        "future_reference_local_recv_monotonic_ns": None,
        "future_reference_event_id": None,
        "future_reference_price": None,
        "future_gap_ms": None,
        "return_bps": None,
        "direction": None,
        "valid": False,
        "invalid_reason": None,
    }


def _complete_reference_like_label(
    *,
    base: dict[str, Any],
    reference_source: str,
    feature_mid_price: float,
    future_reference: dict[str, Any],
    future_ts: int,
    target_ts: int,
    future_index: int,
    receive_field_name: str,
    mark_eligible: bool = False,
) -> dict[str, Any]:
    future_gap_ms = (future_ts - target_ts) / NS_PER_MS
    future_price = reference_price(future_reference, reference_source)
    base.update(
        {
            "future_reference_index": future_index,
            "future_reference_local_recv_monotonic_ns": future_ts,
            "future_reference_event_id": reference_event_id(future_reference, reference_source),
            "future_reference_price": future_price,
            "future_gap_ms": future_gap_ms,
            receive_field_name: future_gap_ms,
        }
    )
    if mark_eligible:
        base["eligible"] = True
    if future_gap_ms < 0:
        return {**base, "invalid_reason": "LABEL_LEAKAGE_FUTURE_BEFORE_TARGET"}
    if future_gap_ms > REQUIRED_100MS_MAX_FUTURE_GAP_MS:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_GAP_TOO_LARGE"}
    future_price_value = _float_or_none(future_price)
    if future_price_value is None or future_price_value <= 0:
        return {**base, "invalid_reason": "FUTURE_REFERENCE_PRICE_INVALID"}
    return_bps = _compute_return_bps(feature_mid_price, future_price_value)
    return {
        **base,
        "return_bps": return_bps,
        "direction": _direction_label(return_bps),
        "valid": True,
        "invalid_reason": None,
    }


def _observe_labels(
    time_row: dict[str, Any],
    corrected_row: dict[str, Any],
    accumulators: dict[str, SourceLabelAccumulators],
) -> None:
    protocol_labels = _dict(time_row.get("protocol_labels"))
    corrected_labels = _dict(corrected_row.get("corrected_protocol_labels"))
    for source in REFERENCE_SOURCES:
        labels = _dict(protocol_labels.get(source))
        corrected = _dict(corrected_labels.get(source))
        accumulator = accumulators[source]
        receive = _dict(labels.get("receive_time"))
        exchange = _dict(labels.get("exchange_time"))
        accumulator.receive.observe(receive)
        accumulator.exchange.observe(exchange)
        for budget_ms in HYBRID_BUDGETS_MS:
            accumulator.hybrid[budget_ms].observe(_dict(labels.get(f"hybrid_{budget_ms}ms")))
            accumulator.corrected_hybrid[budget_ms].observe(_dict(corrected.get(f"corrected_hybrid_{budget_ms}ms")))
        accumulator.raw_lag.observe(
            feature=corrected.get("raw_feature_receive_lag_ms"),
            future=corrected.get("raw_future_receive_lag_ms"),
        )
        accumulator.corrected_lag.observe(
            feature=corrected.get("corrected_feature_receive_lag_ms"),
            future=corrected.get("corrected_future_receive_lag_ms"),
        )


def _build_streaming_source_reports(
    *,
    reference_stats: dict[str, SourceIngestStats],
    timestamp_schema: dict[str, Any],
    accumulators: dict[str, SourceLabelAccumulators],
) -> dict[str, dict[str, Any]]:
    schema_sources = _dict(timestamp_schema.get("sources"))
    reports: dict[str, dict[str, Any]] = {}
    for source in REFERENCE_SOURCES:
        stats = reference_stats[source]
        accumulator = accumulators[source]
        raw_receive_lag = accumulator.raw_lag.result()
        corrected_receive_lag = accumulator.corrected_lag.result()
        reports[source] = {
            "source": source,
            "semantic_type": SEMANTIC_TYPES[source],
            "semantic_description": SEMANTIC_DESCRIPTIONS[source],
            "reference_event_count": stats.reference_event_count,
            "valid_reference_event_count": stats.valid_reference_event_count,
            "invalid_reference_event_count": stats.invalid_reference_event_count,
            "reference_timestamp_monotonic_violations": stats.timestamp_monotonic_violations,
            "reference_sample_rate_per_sec": _reference_quality(stats, accumulator)["reference_sample_rate_per_sec"],
            "exchange_time_supported": _dict(schema_sources.get(source)).get("exchange_time_supported") is True,
            "exchange_timestamp_field_used": _dict(schema_sources.get(source)).get("exchange_timestamp_field_used"),
            "unsupported_reason": _dict(schema_sources.get(source)).get("unsupported_reason"),
            "receive_time": accumulator.receive.result(selection_time_basis="local_recv_monotonic_ns"),
            "exchange_time": {
                **accumulator.exchange.result(selection_time_basis="exchange_ts"),
                "exchange_future_gap_p95_ms": accumulator.exchange.result(selection_time_basis="exchange_ts").get("future_gap_p95_ms"),
                "label_leakage_violations": 0,
            },
            "raw_receive_lag": raw_receive_lag,
            "corrected_receive_lag": corrected_receive_lag,
            "clock_offset_explains_raw_lag": _clock_offset_explains_from_summaries(raw_receive_lag, corrected_receive_lag),
            "corrected_hybrid": {
                f"corrected_hybrid_{budget_ms}ms": accumulator.corrected_hybrid[budget_ms].result()
                for budget_ms in HYBRID_BUDGETS_MS
            },
            "hybrid": {
                f"hybrid_{budget_ms}ms": accumulator.hybrid[budget_ms].result()
                for budget_ms in HYBRID_BUDGETS_MS
            },
            "reference_quality": _reference_quality(stats, accumulator),
            "invalid_reference_event_sample": stats.invalid_samples.samples,
        }
    return reports


def _reference_quality(stats: SourceIngestStats, accumulator: SourceLabelAccumulators) -> dict[str, Any]:
    # The accumulator owns the shared SQLite series store; pull exact gap percentiles from it.
    gap_summary = accumulator.receive.series.summary(f"{stats.source}:reference_gap_ms")
    first_last_duration_ms = 0.0
    if stats.first_local_ts is not None and stats.last_local_ts is not None:
        first_last_duration_ms = max(0.0, (stats.last_local_ts - stats.first_local_ts) / NS_PER_MS)
    # When the stream has more than one row, the gap-derived duration is better represented by sum(gaps).
    gap_count = int(gap_summary.get("count", 0) or 0)
    sample_rate = stats.valid_reference_event_count / (first_last_duration_ms / 1000.0) if first_last_duration_ms > 0 else 0.0
    return {
        "reference_sample_rate_per_sec": sample_rate if gap_count else 0.0,
        "gap_p50_ms": gap_summary.get("p50"),
        "gap_p90_ms": gap_summary.get("p90"),
        "gap_p95_ms": gap_summary.get("p95"),
        "gap_p99_ms": gap_summary.get("p99"),
        "gap_max_ms": gap_summary.get("max"),
        "gap_over_100ms_count": stats.gap_over_100ms_count,
        "gap_over_100ms_total_duration_ms": stats.gap_over_100ms_total_duration_ms,
        "bad_time_coverage_ratio_100ms": stats.gap_over_100ms_total_duration_ms / first_last_duration_ms if first_last_duration_ms > 0 else 0.0,
        "timestamp_monotonic_violations": stats.timestamp_monotonic_violations,
    }


def _clock_offset_explains_from_summaries(raw_lag: dict[str, Any], corrected_lag: dict[str, Any]) -> bool | None:
    raw_p50 = _float_or_none(raw_lag.get("feature_raw_receive_lag_p50_ms"))
    corrected_p50 = _float_or_none(corrected_lag.get("feature_corrected_receive_lag_p50_ms"))
    if raw_p50 is None or corrected_p50 is None:
        return None
    if raw_p50 <= 1000:
        return False
    return abs(corrected_p50) <= 250


def _latency_summary(series: SQLiteSeriesStore, metric: str) -> dict[str, Any]:
    summary = series.summary(metric)
    return {
        "count": summary["count"],
        "p50": summary["p50"],
        "p95": summary["p95"],
        "p99": summary["p99"],
        "max": summary["max"],
    }


def _streaming_label_leakage_reason(label: dict[str, Any], *, protocol_name: str) -> str | None:
    if protocol_name == "receive_time":
        target = label.get("target_local_recv_monotonic_ns")
        future = label.get("future_reference_local_recv_monotonic_ns")
        if isinstance(target, int) and isinstance(future, int) and future < target:
            return "future_reference_timestamp_before_target"
        return None
    target_exchange = _float_or_none(label.get("target_exchange_ts_ms"))
    future_exchange = _float_or_none(label.get("future_reference_exchange_ts_ms"))
    if target_exchange is not None and future_exchange is not None and future_exchange < target_exchange:
        return "future_reference_exchange_timestamp_before_target"
    return None


def _preferred_field_used(field_counts: Counter[str], source: str) -> str | None:
    if source in {"trade_price", "aggTrade_price"} and field_counts.get("T", 0) > 0:
        return "T"
    if field_counts.get("E", 0) > 0:
        return "E"
    if field_counts.get("T", 0) > 0:
        return "T"
    return None


def _mode(counts: dict[str, int]) -> str | None:
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _compact_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _compute_return_bps(current_mid_price: float, future_mid_price: float) -> float:
    if not math.isfinite(current_mid_price) or not math.isfinite(future_mid_price) or current_mid_price <= 0:
        return 0.0
    return ((future_mid_price - current_mid_price) / current_mid_price) * 10_000.0


def _direction_label(return_bps: float) -> int:
    if return_bps > 0:
        return 1
    if return_bps < 0:
        return -1
    return 0
