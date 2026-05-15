from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
import json
from pathlib import Path
from statistics import median
from typing import Any

QUALITY_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3}


def build_dataset_quality_report(
    input_path: str | Path,
    *,
    min_quality_tier: str | None = None,
    print_top: int = 20,
) -> dict[str, Any]:
    rows = list(_read_jsonl(Path(input_path)))
    filtered = _filter_quality(rows, min_quality_tier)
    top_n = max(1, print_top)

    reject_reasons = _counts(_field_values(filtered, "reject_reason"))
    reject_stages = _counts(_field_values(filtered, "reject_stage"))
    quality_tiers = _counts(_field_values(filtered, "data_quality_tier"))
    validation_modes = _counts(_field_values(filtered, "validation_mode"))
    success_rows = [row for row in filtered if row.get("reject_stage") == "none"]

    report: dict[str, Any] = {
        "input_path": str(input_path),
        "min_quality_tier": min_quality_tier,
        "total_rows": len(rows),
        "included_rows": len(filtered),
        "time_range": _time_range(filtered),
        "symbols": _top_counts(_field_values(filtered, "symbol"), top_n),
        "directions": _top_counts(_field_values(filtered, "direction"), top_n),
        "markets": _top_counts(_field_values(filtered, "market_slug"), top_n),
        "validation_mode_distribution": validation_modes,
        "data_quality_tier_distribution": quality_tiers,
        "outcome": {
            "reject_stage_counts": reject_stages,
            "reject_reason_counts": reject_reasons,
            "success_count": len(success_rows),
            "fillable_count": sum(1 for row in filtered if row.get("quote_was_fillable") is True),
            "pre_entry_count": reject_stages.get("pre_entry", 0),
            "window_count": reject_stages.get("window", 0),
            "timeout_count": reject_stages.get("timeout", 0),
            "lifecycle_count": reject_stages.get("lifecycle", 0),
        },
        "timing": {
            "mid_repricing_delay_ms": _summary(_numbers(filtered, "mid_repricing_delay_ms")),
            "executable_repricing_delay_ms": _summary(
                _numbers(filtered, "executable_repricing_delay_ms")
            ),
            "tradable_window_ms": _summary(_numbers(filtered, "tradable_window_ms")),
        },
        "edge": {
            "exit_edge_after_spread": _summary(_numbers(filtered, "exit_edge_after_spread")),
            "exit_edge_ticks": _summary(_numbers(filtered, "exit_edge_ticks")),
            "spread_ticks_at_detection": _summary(
                _numbers(filtered, "spread_ticks_at_detection")
            ),
        },
        "quality": {
            "reported_best_validation_ok_at_detection_counts": _counts(
                _field_values(filtered, "reported_best_validation_ok_at_detection")
            ),
            "book_structurally_complete_at_detection_counts": _counts(
                _field_values(filtered, "book_structurally_complete_at_detection")
            ),
            "book_has_snapshot_at_detection_counts": _counts(
                _field_values(filtered, "book_has_snapshot_at_detection")
            ),
            "book_complete_at_detection_counts": _counts(
                _field_values(filtered, "book_complete_at_detection")
            ),
            "stale_source_counts": _counts(_field_values(filtered, "stale_source")),
            "data_quality_reason_counts": _counts(
                _field_values(filtered, "data_quality_reason")
            ),
            "market_quote_complete_rate": _summary(
                _numbers(filtered, "market_quote_complete_rate_at_detection")
            ),
            "token_quote_complete_rate": _summary(
                _numbers(filtered, "token_quote_complete_rate_at_detection")
            ),
        },
        "by_group": {
            "by_symbol": _group_counts(filtered, "symbol"),
            "by_direction": _group_counts(filtered, "direction"),
            "by_duration_minutes": _group_counts(filtered, "duration_minutes"),
            "by_market_slug": _group_counts(filtered, "market_slug", limit=top_n),
            "by_data_quality_tier": _group_counts(filtered, "data_quality_tier"),
        },
        "success_quality": {
            "success_by_symbol_direction": _success_by_symbol_direction(success_rows),
            "success_by_duration": _group_counts(success_rows, "duration_minutes"),
            "success_by_quality_tier": _group_counts(success_rows, "data_quality_tier"),
            "success_edge_ticks": _summary(_numbers(success_rows, "exit_edge_ticks")),
            "success_delay": _summary(
                _numbers(success_rows, "executable_repricing_delay_ms")
            ),
        },
    }
    report["warnings"] = _warnings(report, filtered)
    return report


def write_dataset_quality_report(report: dict[str, Any], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def default_report_path() -> Path:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    return Path(f"data/reports/dataset_quality_{stamp}.json")


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            decoded = json.loads(stripped)
            if isinstance(decoded, dict):
                yield decoded


def _filter_quality(rows: list[dict[str, Any]], min_quality_tier: str | None) -> list[dict[str, Any]]:
    if min_quality_tier is None:
        return rows
    max_rank = QUALITY_ORDER.get(min_quality_tier.upper())
    if max_rank is None:
        return rows
    return [
        row
        for row in rows
        if QUALITY_ORDER.get(str(row.get("data_quality_tier", "D")), 3) <= max_rank
    ]


def _field_values(rows: Iterable[dict[str, Any]], field: str) -> Iterable[object]:
    for row in rows:
        value = row.get(field)
        if value is not None:
            yield value


def _numbers(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if isinstance(value, int | float):
            values.append(float(value))
    return values


def _counts(values: Iterable[object]) -> dict[str, int]:
    return dict(Counter(str(value) for value in values))


def _top_counts(values: Iterable[object], limit: int) -> dict[str, int]:
    return dict(Counter(str(value) for value in values).most_common(limit))


def _group_counts(
    rows: Iterable[dict[str, Any]],
    field: str,
    *,
    limit: int | None = None,
) -> dict[str, dict[str, int] | int]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        key = str(row.get(field))
        grouped[key]["rows"] += 1
        if row.get("reject_stage") == "none":
            grouped[key]["success"] += 1
    items = sorted(grouped.items(), key=lambda item: (-item[1]["rows"], item[0]))
    if limit is not None:
        items = items[:limit]
    return {key: dict(counter) for key, counter in items}


def _success_by_symbol_direction(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts[f"{row.get('symbol')}:{row.get('direction')}"] += 1
    return dict(counts)


def _summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "median": None, "p95": None, "max": None}
    sorted_values = sorted(values)
    return {
        "min": sorted_values[0],
        "median": median(sorted_values),
        "p95": _percentile(sorted_values, 0.95),
        "max": sorted_values[-1],
    }


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = int(round((len(sorted_values) - 1) * percentile))
    return sorted_values[min(max(index, 0), len(sorted_values) - 1)]


def _time_range(rows: list[dict[str, Any]]) -> dict[str, int | str | None]:
    timestamps = [
        int(value)
        for row in rows
        if isinstance((value := row.get("detected_ts_ns") or row.get("ts_ns")), int)
    ]
    if not timestamps:
        return {"start_ts_ns": None, "end_ts_ns": None, "start_iso": None, "end_iso": None}
    start = min(timestamps)
    end = max(timestamps)
    return {
        "start_ts_ns": start,
        "end_ts_ns": end,
        "start_iso": _ns_to_iso(start),
        "end_iso": _ns_to_iso(end),
    }


def _ns_to_iso(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()


def _warnings(report: dict[str, Any], rows: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    total = int(report["included_rows"])
    if total < 1000:
        warnings.append("total_rows_below_1000")
    outcome = report["outcome"]
    success_count = int(outcome["success_count"])
    if success_count == 0:
        warnings.append("success_count_zero")
    tier_counts = report["data_quality_tier_distribution"]
    d_count = int(tier_counts.get("D", 0))
    if total and d_count / total > 0.20:
        warnings.append("data_quality_tier_d_above_20pct")
    reject_reasons = outcome["reject_reason_counts"]
    book_incomplete = int(reject_reasons.get("book_incomplete", 0))
    if total and book_incomplete / total > 0.30:
        warnings.append("book_incomplete_above_30pct")
    quote_stale = int(reject_reasons.get("quote_stale", 0))
    if total and quote_stale / total > 0.10:
        warnings.append("quote_stale_above_10pct")
    executable_summary = report["timing"]["executable_repricing_delay_ms"]
    if executable_summary["median"] is None:
        warnings.append("median_executable_delay_missing")
    validation_modes = report["validation_mode_distribution"]
    diagnostic_count = int(validation_modes.get("diagnostic", 0))
    if total and diagnostic_count / total > 0.50:
        warnings.append("diagnostic_mode_majority")
    missing_tick = sum(1 for row in rows if row.get("tick_size_at_detection") is None)
    if total and missing_tick / total > 0.05:
        warnings.append("tick_size_missing_above_5pct")
    return warnings
