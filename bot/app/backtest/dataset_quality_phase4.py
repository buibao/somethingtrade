from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import math
from pathlib import Path
from statistics import median
from typing import Any, TypeGuard


REPORT_VERSION = "phase4.0"
CODE_SCOPE = "offline_backtest_report_only"

QUALITY_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3}
QUALITY_TIERS = ("A", "B", "C", "D")
VALIDATION_MODES = ("strict", "tolerant", "diagnostic", "unknown")

MIN_PRIMARY_ROWS = 1000
MAX_TIER_D_RATE = 0.20
MAX_QUOTE_STALE_RATE = 0.10
MAX_BOOK_INCOMPLETE_RATE = 0.30
MIN_SUCCESS_ROWS_FOR_EMPIRICAL = 100
MIN_BUCKET_ROWS = 30

MAX_EXECUTABLE_DELAY_P50_MS = 1000.0
MAX_EXECUTABLE_DELAY_P95_MS = 5000.0
MIN_TRADABLE_WINDOW_P50_MS = 50.0
MIN_TRADABLE_WINDOW_P95_MS = 100.0
STRONG_PRIMARY_ROWS_FOR_BASELINE_MODEL_RESEARCH = 10000
STRONG_SUCCESS_ROWS_FOR_BASELINE_MODEL_RESEARCH = 1000

IMPORTANT_FIELDS = (
    "symbol",
    "market_id",
    "market_slug",
    "token_id",
    "direction",
    "duration_minutes",
    "detected_ts_ns",
    "validation_mode",
    "data_quality_tier",
    "data_quality_reason",
    "reject_stage",
    "reject_reason",
    "quote_was_fillable",
    "before_best_bid",
    "before_best_ask",
    "before_best_bid_size",
    "before_best_ask_size",
    "before_mid",
    "after_best_bid",
    "after_best_ask",
    "after_mid",
    "spread_before",
    "spread_after",
    "entry_ask",
    "entry_ask_size",
    "executable_exit_bid",
    "exit_edge_after_spread",
    "estimated_edge_after_spread",
    "mid_repricing_delay_ms",
    "executable_repricing_delay_ms",
    "tradable_window_ms",
    "tick_size_at_detection",
    "exit_edge_ticks",
    "spread_ticks_at_detection",
    "reported_best_validation_ok_at_detection",
    "book_structurally_complete_at_detection",
    "book_has_snapshot_at_detection",
    "book_complete_at_detection",
    "market_quote_complete_rate_at_detection",
    "token_quote_complete_rate_at_detection",
    "stale_source",
    "binance_quote_age_ms",
    "polymarket_quote_age_ms",
)

CORE_FIELDS = (
    "symbol",
    "direction",
    "detected_ts_ns",
    "validation_mode",
    "data_quality_tier",
    "reject_stage",
)

TIMING_FIELDS = (
    "mid_repricing_delay_ms",
    "executable_repricing_delay_ms",
    "tradable_window_ms",
    "binance_quote_age_ms",
    "polymarket_quote_age_ms",
)

EDGE_FIELDS = (
    "exit_edge_after_spread",
    "estimated_edge_after_spread",
    "exit_edge_ticks",
)

LIQUIDITY_SPREAD_FIELDS = (
    "spread_before",
    "spread_after",
    "spread_ticks_at_detection",
    "before_best_bid_size",
    "before_best_ask_size",
    "entry_ask_size",
)

REJECT_TAXONOMY = {
    "pre_entry_data_unavailable": {
        "missing_quote",
        "direction_token_unmapped",
        "market_invalidated",
    },
    "book_quality": {
        "book_incomplete",
        "book_stale",
        "missing_best_ask",
        "missing_best_ask_size",
        "missing_spread",
        "missing_snapshot",
        "missing_tick_size",
        "structurally_incomplete",
    },
    "liquidity": {
        "insufficient_best_ask_size",
    },
    "spread_and_entry_quality": {
        "spread_too_wide",
        "entry_price_moved",
    },
    "staleness": {
        "quote_stale",
        "binance_stale",
        "polymarket_stale",
        "both_stale",
        "stale_source_unknown",
    },
    "lifecycle": {
        "closed",
        "resolved",
        "lifecycle",
        "market_expired",
        "market_closed",
    },
    "timeout": {
        "max_observation_lifetime_reached",
    },
    "edge_failure": {
        "edge_not_positive_after_spread",
    },
}

REASON_TO_CATEGORY: dict[str, str] = {}
for _category, _reasons in REJECT_TAXONOMY.items():
    for _reason in _reasons:
        REASON_TO_CATEGORY.setdefault(_reason, _category)

READINESS_ORDER = {
    "NOT_READY": 0,
    "NEEDS_MORE_DATA": 1,
    "NEEDS_MORE_CLEANING": 2,
    "READY_FOR_EMPIRICAL_RESEARCH": 3,
    "READY_FOR_BASELINE_MODEL_RESEARCH": 4,
}


@dataclass(frozen=True)
class JsonlReadAudit:
    rows: list[dict[str, Any]]
    file_exists: bool
    file_size_bytes: int
    total_physical_lines: int
    blank_lines: int
    malformed_rows: list[dict[str, Any]]


def build_phase4_dataset_quality_report(
    input_path: str | Path,
    *,
    output_path: str | Path | None = None,
    markdown_output_path: str | Path | None = None,
    csv_dir: str | Path | None = None,
    min_quality_tier: str | None = None,
    primary_min_tier: str = "B",
    include_diagnostic: bool = False,
    print_top: int = 20,
    mismatch_samples_path: str | Path | None = None,
    runtime_summary_jsonl_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build the Phase 4 offline dataset quality and calibration report."""

    input_file = Path(input_path)
    output_file = Path(output_path) if output_path is not None else None
    markdown_file = Path(markdown_output_path) if markdown_output_path is not None else None
    csv_output_dir = Path(csv_dir) if csv_dir is not None else None
    normalized_min_tier = _normalize_tier_or_none(min_quality_tier)
    normalized_primary_min_tier = _normalize_primary_tier(primary_min_tier)

    audit = _read_jsonl_with_audit(input_file)
    rows = audit.rows
    included_rows = _filter_quality(rows, normalized_min_tier)
    primary_rows = _filter_primary_rows(
        included_rows,
        primary_min_tier=normalized_primary_min_tier,
        include_diagnostic=include_diagnostic,
    )
    top_n = max(1, print_top)

    input_audit = _build_input_audit(audit, rows)
    schema_audit = _build_schema_audit(rows)
    dataset_health = _build_dataset_health(rows, included_rows, primary_rows, top_n=top_n)
    quality_tier_analysis = _build_quality_tier_analysis(included_rows, top_n=top_n)
    validation_mode_analysis = _build_validation_mode_analysis(included_rows, top_n=top_n)
    reject_taxonomy = _build_reject_taxonomy(included_rows, top_n=top_n)
    timing_analysis = _build_timing_analysis(included_rows)
    edge_analysis = _build_edge_analysis(included_rows)
    liquidity_and_spread_analysis = _build_liquidity_and_spread_analysis(included_rows)
    stale_feed_analysis = _build_stale_feed_analysis(included_rows)
    tick_calibration_analysis = _build_tick_calibration_analysis(
        included_rows,
        mismatch_samples_path=mismatch_samples_path,
    )
    runtime_coverage_analysis = _build_runtime_coverage_analysis(
        included_rows,
        runtime_summary_jsonl_path=runtime_summary_jsonl_path,
    )
    empirical_bucket_analysis = _build_empirical_bucket_analysis(
        primary_rows,
        primary_min_tier=normalized_primary_min_tier,
    )
    cohort_sensitivity = _build_cohort_sensitivity(
        rows,
        primary_min_tier=normalized_primary_min_tier,
        include_diagnostic=include_diagnostic,
        primary_rows=primary_rows,
    )
    warnings = _build_warnings(
        input_audit=input_audit,
        schema_audit=schema_audit,
        dataset_health=dataset_health,
        timing_analysis=timing_analysis,
        stale_feed_analysis=stale_feed_analysis,
        tick_calibration_analysis=tick_calibration_analysis,
        runtime_coverage_analysis=runtime_coverage_analysis,
        empirical_bucket_analysis=empirical_bucket_analysis,
        cohort_sensitivity=cohort_sensitivity,
    )
    readiness_assessment = _build_readiness_assessment(
        input_audit=input_audit,
        schema_audit=schema_audit,
        dataset_health=dataset_health,
        timing_analysis=timing_analysis,
        edge_analysis=edge_analysis,
        stale_feed_analysis=stale_feed_analysis,
        runtime_coverage_analysis=runtime_coverage_analysis,
        empirical_bucket_analysis=empirical_bucket_analysis,
        cohort_sensitivity=cohort_sensitivity,
        warnings=warnings,
    )

    metadata = {
        "report_version": REPORT_VERSION,
        "generated_at_iso": datetime.now(tz=UTC).isoformat(),
        "input_path": str(input_file),
        "output_path": None if output_file is None else str(output_file),
        "markdown_output_path": None if markdown_file is None else str(markdown_file),
        "csv_dir": None if csv_output_dir is None else str(csv_output_dir),
        "total_rows_loaded": len(rows),
        "total_rows_included": len(included_rows),
        "primary_min_tier": normalized_primary_min_tier,
        "min_quality_tier": normalized_min_tier,
        "include_diagnostic": include_diagnostic,
        "runtime_summary_jsonl_path": (
            None if runtime_summary_jsonl_path is None else str(runtime_summary_jsonl_path)
        ),
        "code_scope": CODE_SCOPE,
        "realtime_path_modified": False,
        "model_prediction_added": False,
        "trading_signal_added": False,
        "live_execution_added": False,
    }

    report = {
        "metadata": metadata,
        "input_audit": input_audit,
        "schema_audit": schema_audit,
        "dataset_health": dataset_health,
        "quality_tier_analysis": quality_tier_analysis,
        "validation_mode_analysis": validation_mode_analysis,
        "reject_taxonomy": reject_taxonomy,
        "timing_analysis": timing_analysis,
        "edge_analysis": edge_analysis,
        "liquidity_and_spread_analysis": liquidity_and_spread_analysis,
        "stale_feed_analysis": stale_feed_analysis,
        "tick_calibration_analysis": tick_calibration_analysis,
        "runtime_coverage_analysis": runtime_coverage_analysis,
        "empirical_bucket_analysis": empirical_bucket_analysis,
        "cohort_sensitivity": cohort_sensitivity,
        "warnings": warnings,
        "readiness_assessment": readiness_assessment,
        "next_phase_recommendation": readiness_assessment["recommended_next_phase"],
    }
    return report


def write_phase4_dataset_quality_report(report: dict[str, Any], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_phase4_markdown_report(report: dict[str, Any], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_phase4_markdown_report(report), encoding="utf-8")


def write_phase4_csv_outputs(report: dict[str, Any], csv_dir: str | Path) -> None:
    directory = Path(csv_dir)
    directory.mkdir(parents=True, exist_ok=True)

    _write_csv(
        directory / "cohort_summary.csv",
        [
            "cohort",
            "row_count",
            "success_count",
            "success_rate",
            "median_executable_repricing_delay_ms",
            "median_tradable_window_ms",
            "median_exit_edge_ticks",
            "warnings",
        ],
        _cohort_csv_rows(report),
    )
    _write_csv(
        directory / "reject_taxonomy.csv",
        [
            "category",
            "reason",
            "count",
            "pct_total_rows",
            "pct_rejected_rows",
            "top_tier",
            "top_validation_mode",
        ],
        _reject_taxonomy_csv_rows(report),
    )
    _write_csv(
        directory / "quality_tier_summary.csv",
        [
            "tier",
            "row_count",
            "row_rate",
            "success_count",
            "success_rate",
            "top_reject_reason",
            "median_delay",
            "median_window",
            "median_edge_ticks",
        ],
        _quality_tier_csv_rows(report),
    )
    _write_csv(
        directory / "validation_mode_summary.csv",
        [
            "validation_mode",
            "row_count",
            "row_rate",
            "success_count",
            "success_rate",
            "tier_distribution",
            "median_delay",
            "median_edge_ticks",
        ],
        _validation_mode_csv_rows(report),
    )
    _write_csv(
        directory / "timing_summary.csv",
        [
            "metric",
            "cohort",
            "count",
            "missing_count",
            "min",
            "mean",
            "median",
            "p75",
            "p90",
            "p95",
            "p99",
            "max",
        ],
        _timing_csv_rows(report),
    )
    _write_csv(
        directory / "edge_summary.csv",
        [
            "metric",
            "cohort",
            "count",
            "missing_count",
            "positive_count",
            "positive_rate",
            "min",
            "mean",
            "median",
            "p75",
            "p90",
            "p95",
            "p99",
            "max",
        ],
        _edge_csv_rows(report),
    )
    _write_csv(
        directory / "empirical_buckets.csv",
        [
            "feature",
            "bucket",
            "row_count",
            "success_count",
            "success_rate",
            "median_executable_repricing_delay_ms",
            "median_tradable_window_ms",
            "median_exit_edge_ticks",
            "insufficient_sample_warning",
        ],
        _empirical_bucket_csv_rows(report),
    )
    _write_csv(
        directory / "readiness_checks.csv",
        [
            "check_name",
            "status",
            "value",
            "threshold",
            "severity",
            "message",
        ],
        _readiness_csv_rows(report),
    )
    malformed = report["input_audit"].get("malformed_rows", [])
    if malformed:
        _write_csv(
            directory / "malformed_rows.csv",
            ["line_number", "error", "line_sample"],
            malformed,
        )


def render_phase4_markdown_report(report: dict[str, Any]) -> str:
    metadata = report["metadata"]
    input_audit = report["input_audit"]
    dataset_health = report["dataset_health"]
    readiness = report["readiness_assessment"]
    validation_conclusion = report["validation_mode_analysis"]["conclusion"]

    lines = [
        "# Phase 4.0 Dataset Quality Report & Empirical Calibration",
        "",
        "## 1. Executive Summary",
        "",
        f"- Readiness classification: `{readiness['classification']}`",
        f"- Parsed rows: {input_audit['parsed_json_rows']}",
        f"- Included rows: {dataset_health['included_rows']}",
        f"- Primary rows ({metadata['primary_min_tier']} or better): {dataset_health['primary_rows']}",
        (
            "- Measured executable repricing success count: "
            f"{dataset_health['success_count']} "
            f"({_format_rate(dataset_health['success_rate'])})"
        ),
        f"- Recommended next phase: {readiness['recommended_next_phase']}",
        "",
        "## 2. Input Audit",
        "",
        f"- Input path: `{metadata['input_path']}`",
        f"- File exists: {input_audit['file_exists']}",
        f"- File size bytes: {input_audit['file_size_bytes']}",
        f"- Physical lines: {input_audit['total_physical_lines']}",
        f"- Blank lines skipped: {input_audit['blank_lines']}",
        f"- Malformed JSON lines: {input_audit['malformed_json_lines']}",
        f"- Time range: {input_audit['first_detected_iso']} to {input_audit['last_detected_iso']}",
        "",
        "## 3. Dataset Health",
        "",
        f"- Rows by quality tier: `{json.dumps(dataset_health['rows_by_data_quality_tier'], sort_keys=True)}`",
        f"- Rows by validation mode: `{json.dumps(dataset_health['rows_by_validation_mode'], sort_keys=True)}`",
        f"- Pre-entry reject rate: {_format_rate(dataset_health['pre_entry_reject_rate'])}",
        f"- Window reject rate: {_format_rate(dataset_health['window_reject_rate'])}",
        f"- Timeout rate: {_format_rate(dataset_health['timeout_rate'])}",
        f"- Lifecycle reject rate: {_format_rate(dataset_health['lifecycle_reject_rate'])}",
        "",
        "## 4. Quality Tier Analysis",
        "",
        _markdown_quality_tier_table(report),
        "",
        "## 5. Validation Mode Analysis",
        "",
        _markdown_validation_mode_table(report),
        "",
        (
            "- Tolerant mode materially changes distribution: "
            f"{validation_conclusion['tolerant_mode_materially_changes_distribution']}"
        ),
        f"- Reason: {validation_conclusion['reason']}",
        "",
        "## 6. Reject Taxonomy",
        "",
        _markdown_reject_taxonomy_table(report),
        "",
        "## 7. Timing Analysis",
        "",
        _markdown_metric_table(report["timing_analysis"]["overall"], TIMING_FIELDS),
        "",
        "## 8. Edge Analysis",
        "",
        "Measured edge is exit bid minus entry ask under measurement assumptions before full fee, slippage, and queue modeling.",
        "",
        _markdown_metric_table(report["edge_analysis"]["overall"], EDGE_FIELDS),
        "",
        "## 9. Liquidity & Spread Analysis",
        "",
        _markdown_metric_table(
            report["liquidity_and_spread_analysis"]["summaries"],
            LIQUIDITY_SPREAD_FIELDS,
        ),
        "",
        "## 10. Stale Feed Analysis",
        "",
        f"- Staleness status: {report['stale_feed_analysis']['staleness_status']}",
        f"- Stale source distribution: `{json.dumps(report['stale_feed_analysis']['stale_source_distribution'], sort_keys=True)}`",
        f"- Quote stale rate: {_format_rate(report['stale_feed_analysis']['quote_stale_rate'])}",
        f"- Binance stale rate: {_format_rate(report['stale_feed_analysis']['binance_stale_rate'])}",
        f"- Polymarket stale rate: {_format_rate(report['stale_feed_analysis']['polymarket_stale_rate'])}",
        f"- Both stale rate: {_format_rate(report['stale_feed_analysis']['both_stale_rate'])}",
        "",
        "## 10.5 Runtime Coverage Analysis",
        "",
        f"- Runtime coverage status: {report['runtime_coverage_analysis']['status']}",
        f"- Runtime summary rows: {report['runtime_coverage_analysis'].get('runtime_summary_rows', 0)}",
        (
            "- Gap-event/runtime coverage ratio: "
            f"{_format_rate(report['runtime_coverage_analysis'].get('gap_event_time_coverage_ratio'))}"
        ),
        f"- Runtime coverage warnings: {', '.join(report['runtime_coverage_analysis'].get('warning_flags', [])) or '-'}",
        "",
        "## 11. Tick Calibration Analysis",
        "",
        f"- Tick size distribution: `{json.dumps(report['tick_calibration_analysis']['tick_size_distribution'], sort_keys=True)}`",
        f"- Tolerated mismatch row count: {report['tick_calibration_analysis']['tolerated_mismatch_row_count']}",
        f"- Mismatch sample status: {report['tick_calibration_analysis']['mismatch_sample_status']}",
        f"- Mismatch sample total: {report['tick_calibration_analysis'].get('mismatch_total', 0)}",
        f"- Mismatch by error type: `{json.dumps(report['tick_calibration_analysis'].get('mismatch_by_error_type', {}), sort_keys=True)}`",
        f"- Top affected markets: `{json.dumps(report['tick_calibration_analysis'].get('top_affected_markets', {}), sort_keys=True)}`",
        f"- Top affected tokens: `{json.dumps(report['tick_calibration_analysis'].get('top_affected_tokens', {}), sort_keys=True)}`",
        f"- Pct within 1 tick: {_format_rate(report['tick_calibration_analysis'].get('pct_within_1_tick'))}",
        f"- Pct above 2 ticks: {_format_rate(report['tick_calibration_analysis'].get('pct_above_2_ticks'))}",
        f"- Warnings: {', '.join(report['tick_calibration_analysis'].get('warning_flags', [])) or '-'}",
        "",
        "## 12. Empirical Bucket Analysis",
        "",
        _primary_bucket_markdown_note(report),
        "These buckets are descriptive historical measurements only; they are not forecasts, model outputs, or execution signals.",
        "",
        _markdown_empirical_bucket_table(report),
        "",
        "## 13. Cohort Sensitivity",
        "",
        _markdown_cohort_table(report),
        f"- Conclusion: {report['cohort_sensitivity']['conclusion']}",
        "",
        "## 14. Readiness Assessment",
        "",
        f"- Classification: `{readiness['classification']}`",
        f"- Blocking issues: `{json.dumps(readiness['blocking_issues'])}`",
        f"- Non-blocking warnings: `{json.dumps(readiness['non_blocking_warnings'])}`",
        "",
        "## 15. Recommended Next Phase",
        "",
        f"{readiness['recommended_next_phase']}",
        "",
        "## 16. Non-Goals Confirmed",
        "",
        "- no model prediction was added",
        "- no ML training was added",
        "- no trading signal was added",
        "- no live execution was added",
        "- no private-key handling was added",
        "- no wallet copy trading or on-chain wallet logic was added",
        "- no LLM was added to realtime path",
        "",
    ]
    return "\n".join(lines)


def should_fail_for_readiness(classification: str, fail_on_readiness: str | None) -> bool:
    if fail_on_readiness is None:
        return False
    threshold_rank = READINESS_ORDER.get(fail_on_readiness)
    classification_rank = READINESS_ORDER.get(classification)
    if threshold_rank is None or classification_rank is None:
        return False
    return classification_rank <= threshold_rank


def _read_jsonl_with_audit(path: Path) -> JsonlReadAudit:
    if not path.exists():
        raise FileNotFoundError(f"input file does not exist: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"input path is not a file: {path}")

    rows: list[dict[str, Any]] = []
    malformed_rows: list[dict[str, Any]] = []
    total_physical_lines = 0
    blank_lines = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            total_physical_lines += 1
            stripped = line.strip()
            if not stripped:
                blank_lines += 1
                continue
            try:
                decoded = json.loads(stripped)
            except json.JSONDecodeError as exc:
                malformed_rows.append(
                    {
                        "line_number": line_number,
                        "error": exc.msg,
                        "line_sample": stripped[:200],
                    }
                )
                continue
            if not isinstance(decoded, dict):
                malformed_rows.append(
                    {
                        "line_number": line_number,
                        "error": "expected JSON object",
                        "line_sample": stripped[:200],
                    }
                )
                continue
            rows.append(decoded)

    return JsonlReadAudit(
        rows=rows,
        file_exists=True,
        file_size_bytes=path.stat().st_size,
        total_physical_lines=total_physical_lines,
        blank_lines=blank_lines,
        malformed_rows=malformed_rows,
    )


def _build_input_audit(
    audit: JsonlReadAudit,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    timestamps = sorted(_int_values(rows, "detected_ts_ns"))
    first_ts = timestamps[0] if timestamps else None
    last_ts = timestamps[-1] if timestamps else None
    duration_minutes = (
        (last_ts - first_ts) / 1_000_000_000 / 60
        if first_ts is not None and last_ts is not None
        else None
    )
    markets = {
        str(row.get("market_id") or row.get("market_slug"))
        for row in rows
        if row.get("market_id") is not None or row.get("market_slug") is not None
    }
    return {
        "file_exists": audit.file_exists,
        "file_size_bytes": audit.file_size_bytes,
        "total_physical_lines": audit.total_physical_lines,
        "parsed_json_rows": len(rows),
        "blank_lines": audit.blank_lines,
        "malformed_json_lines": len(audit.malformed_rows),
        "malformed_json_line_numbers_sample": [
            row["line_number"] for row in audit.malformed_rows[:20]
        ],
        "malformed_rows": audit.malformed_rows,
        "first_detected_ts_ns": first_ts,
        "last_detected_ts_ns": last_ts,
        "first_detected_iso": _ns_to_iso(first_ts) if first_ts is not None else None,
        "last_detected_iso": _ns_to_iso(last_ts) if last_ts is not None else None,
        "duration_minutes": duration_minutes,
        "symbols_count": len(_non_null_unique(rows, "symbol")),
        "markets_count": len(markets),
        "tokens_count": len(_non_null_unique(rows, "token_id")),
    }


def _build_schema_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    fields: dict[str, dict[str, Any]] = {}
    for field in IMPORTANT_FIELDS:
        present_values = [row[field] for row in rows if field in row and row[field] is not None]
        type_counts = Counter(type(value).__name__ for value in present_values)
        fields[field] = {
            "present_count": len(present_values),
            "missing_count": total - len(present_values),
            "present_rate": _rate(len(present_values), total),
            "type_distribution": dict(sorted(type_counts.items())),
        }
    core_missing = [field for field in CORE_FIELDS if fields[field]["present_count"] == 0]
    core_incomplete = [
        field
        for field in CORE_FIELDS
        if total > 0 and fields[field]["present_count"] < total
    ]
    return {
        "total_rows": total,
        "fields": fields,
        "core_fields_missing": core_missing,
        "core_fields_incomplete": core_incomplete,
    }


def _build_dataset_health(
    rows: list[dict[str, Any]],
    included_rows: list[dict[str, Any]],
    primary_rows: list[dict[str, Any]],
    *,
    top_n: int,
) -> dict[str, Any]:
    included_count = len(included_rows)
    reject_stage_counts = _field_counter(included_rows, "reject_stage")
    reject_reason_counts = _field_counter(included_rows, "reject_reason", include_missing=False)
    success_count = _success_count(included_rows)
    fillable_count = sum(1 for row in included_rows if row.get("quote_was_fillable") is True)
    return {
        "total_rows": len(rows),
        "included_rows": included_count,
        "primary_rows": len(primary_rows),
        "primary_row_rate": _rate(len(primary_rows), included_count),
        "rows_by_symbol": _field_counter(included_rows, "symbol", limit=top_n),
        "rows_by_direction": _field_counter(included_rows, "direction", limit=top_n),
        "rows_by_symbol_direction": _symbol_direction_counter(included_rows, limit=top_n),
        "rows_by_duration_minutes": _field_counter(
            included_rows,
            "duration_minutes",
            limit=top_n,
        ),
        "rows_by_market_slug": _field_counter(included_rows, "market_slug", limit=top_n),
        "rows_by_validation_mode": _validation_mode_counter(included_rows),
        "rows_by_data_quality_tier": _tier_counter(included_rows),
        "rows_by_reject_stage": reject_stage_counts,
        "rows_by_reject_reason": reject_reason_counts,
        "success_count": success_count,
        "success_rate": _rate(success_count, included_count),
        "fillable_count": fillable_count,
        "fillable_rate": _rate(fillable_count, included_count),
        "pre_entry_reject_rate": _rate(reject_stage_counts.get("pre_entry", 0), included_count),
        "window_reject_rate": _rate(reject_stage_counts.get("window", 0), included_count),
        "timeout_rate": _rate(reject_stage_counts.get("timeout", 0), included_count),
        "lifecycle_reject_rate": _rate(reject_stage_counts.get("lifecycle", 0), included_count),
        "success_wording": "measured executable repricing success",
    }


def _build_quality_tier_analysis(
    rows: list[dict[str, Any]],
    *,
    top_n: int,
) -> dict[str, Any]:
    total = len(rows)
    tiers: dict[str, dict[str, Any]] = {}
    for tier in QUALITY_TIERS:
        tier_rows = [row for row in rows if _tier_key(row) == tier]
        success_count = _success_count(tier_rows)
        reject_reasons = _field_counter(tier_rows, "reject_reason", include_missing=False)
        warning_flags: list[str] = []
        if not tier_rows:
            warning_flags.append("no_rows")
        if tier == "D" and tier_rows:
            warning_flags.append("diagnostic_only_not_for_clean_empirical_buckets")
        if tier_rows and success_count == 0:
            warning_flags.append("no_measured_executable_repricing_success")
        if tier_rows and _metric_summary(tier_rows, "executable_repricing_delay_ms")["count"] == 0:
            warning_flags.append("executable_repricing_delay_missing")
        tiers[tier] = {
            "row_count": len(tier_rows),
            "row_rate": _rate(len(tier_rows), total),
            "success_count": success_count,
            "success_rate": _rate(success_count, len(tier_rows)),
            "reject_stage_distribution": _field_counter(tier_rows, "reject_stage"),
            "reject_reason_top": dict(Counter(reject_reasons).most_common(top_n)),
            "validation_mode_distribution": _validation_mode_counter(tier_rows),
            "median_executable_repricing_delay_ms": _metric_summary(
                tier_rows,
                "executable_repricing_delay_ms",
            )["median"],
            "p95_executable_repricing_delay_ms": _metric_summary(
                tier_rows,
                "executable_repricing_delay_ms",
            )["p95"],
            "median_tradable_window_ms": _metric_summary(
                tier_rows,
                "tradable_window_ms",
            )["median"],
            "p95_tradable_window_ms": _metric_summary(
                tier_rows,
                "tradable_window_ms",
            )["p95"],
            "median_exit_edge_ticks": _metric_summary(tier_rows, "exit_edge_ticks")["median"],
            "p95_exit_edge_ticks": _metric_summary(tier_rows, "exit_edge_ticks")["p95"],
            "median_spread_ticks_at_detection": _metric_summary(
                tier_rows,
                "spread_ticks_at_detection",
            )["median"],
            "warning_flags": warning_flags,
        }
    return {
        "tier_semantics": {
            "A": "clean validated row",
            "B": "usable research row with tolerated one-tick mismatch or minor caveat",
            "C": "sensitivity-analysis row",
            "D": "reject/diagnostic evidence",
        },
        "primary_default": "A/B",
        "tier_c_usage": "sensitivity_analysis_only",
        "tier_d_usage": "diagnostic_only_not_clean_empirical_bucket_input",
        "tiers": tiers,
    }


def _build_validation_mode_analysis(
    rows: list[dict[str, Any]],
    *,
    top_n: int,
) -> dict[str, Any]:
    total = len(rows)
    modes: dict[str, dict[str, Any]] = {}
    for mode in VALIDATION_MODES:
        mode_rows = [row for row in rows if _validation_mode_key(row) == mode]
        success_count = _success_count(mode_rows)
        modes[mode] = {
            "row_count": len(mode_rows),
            "row_rate": _rate(len(mode_rows), total),
            "quality_tier_distribution": _tier_counter(mode_rows),
            "success_count": success_count,
            "success_rate": _rate(success_count, len(mode_rows)),
            "reject_stage_distribution": _field_counter(mode_rows, "reject_stage"),
            "reject_reason_distribution": _field_counter(
                mode_rows,
                "reject_reason",
                include_missing=False,
                limit=top_n,
            ),
            "median_timing_metrics": {
                field: _metric_summary(mode_rows, field)["median"] for field in TIMING_FIELDS
            },
            "median_edge_metrics": {
                field: _metric_summary(mode_rows, field)["median"] for field in EDGE_FIELDS
            },
            "median_spread_tick_metrics": {
                "spread_ticks_at_detection": _metric_summary(
                    mode_rows,
                    "spread_ticks_at_detection",
                )["median"],
                "tick_size_at_detection": _metric_summary(
                    mode_rows,
                    "tick_size_at_detection",
                )["median"],
            },
        }

    comparisons = {
        "strict_vs_tolerant": _compare_row_sets(
            [row for row in rows if _validation_mode_key(row) == "strict"],
            [row for row in rows if _validation_mode_key(row) == "tolerant"],
        ),
        "A_only_vs_B_only": _compare_row_sets(
            [row for row in rows if _tier_key(row) == "A"],
            [row for row in rows if _tier_key(row) == "B"],
        ),
        "AB_vs_CD": _compare_row_sets(
            [row for row in rows if QUALITY_ORDER[_tier_key(row)] <= QUALITY_ORDER["B"]],
            [row for row in rows if QUALITY_ORDER[_tier_key(row)] >= QUALITY_ORDER["C"]],
        ),
    }
    conclusion = _validation_distribution_conclusion(comparisons["strict_vs_tolerant"])
    return {
        "modes": modes,
        "comparisons": comparisons,
        "conclusion": conclusion,
    }


def _build_reject_taxonomy(
    rows: list[dict[str, Any]],
    *,
    top_n: int,
) -> dict[str, Any]:
    rejected_rows = [
        row
        for row in rows
        if row.get("reject_stage") != "none" or row.get("reject_reason") is not None
    ]
    total_rows = len(rows)
    rejected_count = len(rejected_rows)
    category_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reason_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rejected_rows:
        reason = _reason_key(row)
        category = REASON_TO_CATEGORY.get(reason, "unknown")
        category_rows[category].append(row)
        reason_rows[reason].append(row)

    categories: dict[str, dict[str, Any]] = {}
    for category in tuple(REJECT_TAXONOMY) + ("unknown",):
        category_group = category_rows.get(category, [])
        category_reasons = {
            reason: _reject_group_stats(reason_group, total_rows, rejected_count)
            for reason, reason_group in sorted(reason_rows.items())
            if REASON_TO_CATEGORY.get(reason, "unknown") == category
        }
        categories[category] = {
            **_reject_group_stats(category_group, total_rows, rejected_count),
            "reasons": category_reasons,
        }

    reasons = {
        reason: {
            "category": REASON_TO_CATEGORY.get(reason, "unknown"),
            **_reject_group_stats(reason_group, total_rows, rejected_count),
        }
        for reason, reason_group in sorted(
            reason_rows.items(),
            key=lambda item: (-len(item[1]), item[0]),
        )[:top_n]
    }
    return {
        "total_rows": total_rows,
        "rejected_rows": rejected_count,
        "categories": categories,
        "reasons": reasons,
    }


def _build_timing_analysis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    analysis = {
        "overall": {field: _metric_summary(rows, field) for field in TIMING_FIELDS},
        "by_quality_tier": _metrics_by_group(rows, TIMING_FIELDS, _tier_key, QUALITY_TIERS),
        "by_validation_mode": _metrics_by_group(
            rows,
            TIMING_FIELDS,
            _validation_mode_key,
            VALIDATION_MODES,
        ),
        "by_symbol": _metrics_by_field(rows, TIMING_FIELDS, "symbol"),
        "by_direction": _metrics_by_field(rows, TIMING_FIELDS, "direction"),
        "by_duration_minutes": _metrics_by_field(rows, TIMING_FIELDS, "duration_minutes"),
        "by_reject_stage": _metrics_by_field(rows, TIMING_FIELDS, "reject_stage"),
    }
    overall = analysis["overall"]
    executable = overall["executable_repricing_delay_ms"]
    tradable = overall["tradable_window_ms"]
    analysis["latency_readiness_flags"] = {
        "executable_delay_missing": executable["count"] == 0,
        "executable_delay_p50_too_high": _gt(
            executable["median"],
            MAX_EXECUTABLE_DELAY_P50_MS,
        ),
        "executable_delay_p95_too_high": _gt(
            executable["p95"],
            MAX_EXECUTABLE_DELAY_P95_MS,
        ),
        "tradable_window_p50_too_small": _lt(
            tradable["median"],
            MIN_TRADABLE_WINDOW_P50_MS,
        ),
        "tradable_window_p95_too_small": _lt(
            tradable["p95"],
            MIN_TRADABLE_WINDOW_P95_MS,
        ),
    }
    return analysis


def _build_edge_analysis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "analysis_note": (
            "measured edge before full fee/slippage/queue modeling; "
            "exit bid minus entry ask under measurement assumptions"
        ),
        "overall": {field: _edge_summary(rows, field) for field in EDGE_FIELDS},
        "by_quality_tier": _edge_by_group(rows, EDGE_FIELDS, _tier_key, QUALITY_TIERS),
        "by_validation_mode": _edge_by_group(
            rows,
            EDGE_FIELDS,
            _validation_mode_key,
            VALIDATION_MODES,
        ),
        "by_symbol": _edge_by_field(rows, EDGE_FIELDS, "symbol"),
        "by_direction": _edge_by_field(rows, EDGE_FIELDS, "direction"),
        "by_duration_minutes": _edge_by_field(rows, EDGE_FIELDS, "duration_minutes"),
    }


def _build_liquidity_and_spread_analysis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    summaries = {field: _metric_summary(rows, field) for field in LIQUIDITY_SPREAD_FIELDS}
    missing_rates = {
        field: _rate(summaries[field]["missing_count"], total)
        for field in LIQUIDITY_SPREAD_FIELDS
    }
    zero_rates = {
        field: _rate(_zero_count(rows, field), summaries[field]["count"])
        for field in LIQUIDITY_SPREAD_FIELDS
    }
    bucket_distributions = {
        field: _bucket_counter(rows, field, _spread_liquidity_bucket)
        for field in LIQUIDITY_SPREAD_FIELDS
    }
    missing_size_count = sum(
        1
        for row in rows
        if any(row.get(field) is None for field in ("before_best_bid_size", "before_best_ask_size", "entry_ask_size"))
    )
    low_liquidity_count = sum(
        1
        for row in rows
        if row.get("reject_reason") == "insufficient_best_ask_size"
        or any(
            _is_number(row.get(field)) and float(row[field]) <= 0
            for field in ("before_best_bid_size", "before_best_ask_size", "entry_ask_size")
        )
    )
    high_spread_tick_count = sum(
        1
        for row in rows
        if _is_number(row.get("spread_ticks_at_detection"))
        and float(row["spread_ticks_at_detection"]) > 10
    )
    return {
        "summaries": summaries,
        "missing_rates": missing_rates,
        "zero_rates": zero_rates,
        "bucket_distributions": bucket_distributions,
        "flags": {
            "spread_too_wide_rate": _rate(
                sum(1 for row in rows if row.get("reject_reason") == "spread_too_wide"),
                total,
            ),
            "missing_size_rate": _rate(missing_size_count, total),
            "low_liquidity_rate": _rate(low_liquidity_count, total),
            "high_spread_tick_rate": _rate(high_spread_tick_count, total),
        },
    }


def _build_stale_feed_analysis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    stale_source_distribution = _field_counter(rows, "stale_source")
    quote_stale_count = 0
    binance_stale_count = 0
    polymarket_stale_count = 0
    both_stale_count = 0
    unknown_stale_count = 0
    for row in rows:
        reason = str(row.get("reject_reason") or "")
        source = str(row.get("stale_source") or "").lower()
        stale = reason in {"quote_stale", "binance_stale", "polymarket_stale", "both_stale"} or source not in {"", "none", "false"}
        if stale:
            quote_stale_count += 1
        if reason == "both_stale" or source == "both":
            both_stale_count += 1
        if reason == "binance_stale" or source in {"binance", "both"}:
            binance_stale_count += 1
        if reason == "polymarket_stale" or source in {"polymarket", "both"}:
            polymarket_stale_count += 1
        if stale and not source:
            unknown_stale_count += 1
        if reason == "stale_source_unknown" or source == "unknown":
            unknown_stale_count += 1

    has_quote_age_fields = any(
        _is_number(row.get("binance_quote_age_ms"))
        or _is_number(row.get("polymarket_quote_age_ms"))
        for row in rows
    )
    stale_source_unknown_for_all_rows = total > 0 and all(
        _stale_source_is_unknown(row.get("stale_source")) for row in rows
    )
    quote_age_fields_missing = not has_quote_age_fields
    staleness_status = (
        "unknown_missing_quote_age_fields"
        if stale_source_unknown_for_all_rows and quote_age_fields_missing
        else "measured_from_stale_source_or_reject_reason"
    )
    quote_stale_rate: float | None = (
        None if staleness_status == "unknown_missing_quote_age_fields" else _rate(quote_stale_count, total)
    )
    binance_stale_rate: float | None = (
        None if staleness_status == "unknown_missing_quote_age_fields" else _rate(binance_stale_count, total)
    )
    polymarket_stale_rate: float | None = (
        None if staleness_status == "unknown_missing_quote_age_fields" else _rate(polymarket_stale_count, total)
    )
    both_stale_rate: float | None = (
        None if staleness_status == "unknown_missing_quote_age_fields" else _rate(both_stale_count, total)
    )
    unknown_stale_rate: float | None = (
        None if staleness_status == "unknown_missing_quote_age_fields" else _rate(unknown_stale_count, total)
    )
    return {
        "staleness_status": staleness_status,
        "quote_age_fields_missing": quote_age_fields_missing,
        "stale_source_unknown_for_all_rows": stale_source_unknown_for_all_rows,
        "stale_source_distribution": stale_source_distribution,
        "quote_stale_rate": quote_stale_rate,
        "binance_stale_rate": binance_stale_rate,
        "polymarket_stale_rate": polymarket_stale_rate,
        "both_stale_rate": both_stale_rate,
        "unknown_stale_rate": unknown_stale_rate,
        "quote_stale_count": quote_stale_count,
        "binance_stale_count": binance_stale_count,
        "polymarket_stale_count": polymarket_stale_count,
        "both_stale_count": both_stale_count,
        "unknown_stale_count": unknown_stale_count,
        "quote_age_summary": {
            "binance_quote_age_ms": _metric_summary(rows, "binance_quote_age_ms"),
            "polymarket_quote_age_ms": _metric_summary(rows, "polymarket_quote_age_ms"),
        },
        "timestamp_basis": "quote_age_fields" if has_quote_age_fields else "unknown_missing_quote_age_fields",
    }


def _build_tick_calibration_analysis(
    rows: list[dict[str, Any]],
    *,
    mismatch_samples_path: str | Path | None,
) -> dict[str, Any]:
    total = len(rows)
    tolerated_rows = [row for row in rows if _row_has_tolerated_mismatch(row)]
    analysis = {
        "tick_size_distribution": _field_counter(rows, "tick_size_at_detection"),
        "missing_tick_size_rate": _rate(
            sum(1 for row in rows if not _is_number(row.get("tick_size_at_detection"))),
            total,
        ),
        "spread_tick_distribution": _bucket_counter(
            rows,
            "spread_ticks_at_detection",
            _spread_ticks_bucket,
        ),
        "exit_edge_tick_distribution": _bucket_counter(
            rows,
            "exit_edge_ticks",
            _edge_ticks_bucket,
        ),
        "tolerated_mismatch_row_count": len(tolerated_rows),
        "tolerated_mismatch_row_rate": _rate(len(tolerated_rows), total),
    }
    if mismatch_samples_path is None:
        analysis["mismatch_sample_status"] = "skipped_missing_mismatch_sample_input"
        _add_missing_mismatch_sample_warning(analysis)
        return analysis

    sample_path = Path(mismatch_samples_path)
    if not sample_path.exists():
        analysis["mismatch_sample_status"] = "skipped_missing_mismatch_sample_input"
        analysis["mismatch_sample_path"] = str(sample_path)
        _add_missing_mismatch_sample_warning(analysis)
        return analysis

    analysis.update(_analyze_mismatch_samples(sample_path))
    analysis["warning_flags"] = []
    return analysis


def _build_empirical_bucket_analysis(
    primary_rows: list[dict[str, Any]],
    *,
    primary_min_tier: str,
) -> dict[str, Any]:
    buckets: list[dict[str, Any]] = []
    bucket_specs: list[tuple[str, str, tuple[tuple[str, str, Callable[[float], bool]], ...]]] = [
        (
            "binance_move_pct",
            "binance_move_pct",
            (
                ("<= -0.20", "<= -0.20", lambda value: value <= -0.20),
                ("-0.20 to -0.10", "-0.20 to -0.10", lambda value: -0.20 < value <= -0.10),
                ("-0.10 to -0.05", "-0.10 to -0.05", lambda value: -0.10 < value <= -0.05),
                ("-0.05 to 0", "-0.05 to 0", lambda value: -0.05 < value < 0),
                ("0 to 0.05", "0 to 0.05", lambda value: 0 <= value <= 0.05),
                ("0.05 to 0.10", "0.05 to 0.10", lambda value: 0.05 < value <= 0.10),
                ("0.10 to 0.20", "0.10 to 0.20", lambda value: 0.10 < value <= 0.20),
                ("> 0.20", "> 0.20", lambda value: value > 0.20),
            ),
        ),
        (
            "spread_ticks_at_detection",
            "spread_ticks_at_detection",
            (
                ("0", "0", lambda value: value == 0),
                ("1", "1", lambda value: value == 1),
                ("2", "2", lambda value: value == 2),
                ("3-5", "3-5", lambda value: 3 <= value <= 5),
                ("6-10", "6-10", lambda value: 6 <= value <= 10),
                (">10", ">10", lambda value: value > 10),
            ),
        ),
        (
            "tradable_window_ms",
            "tradable_window_ms",
            _time_bucket_specs(),
        ),
        (
            "executable_repricing_delay_ms",
            "executable_repricing_delay_ms",
            _time_bucket_specs(),
        ),
    ]

    for feature, field, specs in bucket_specs:
        buckets.extend(_numeric_bucket_rows(primary_rows, feature, field, specs))

    for tier in QUALITY_TIERS:
        tier_rows = [row for row in primary_rows if _tier_key(row) == tier]
        buckets.append(_bucket_summary("validation_quality", tier, tier, tier_rows, len(primary_rows)))

    for cohort in ("BTCUSDT:UP", "BTCUSDT:DOWN", "ETHUSDT:UP", "ETHUSDT:DOWN"):
        symbol, direction = cohort.split(":", 1)
        cohort_rows = [
            row
            for row in primary_rows
            if str(row.get("symbol")) == symbol and str(row.get("direction")) == direction
        ]
        buckets.append(
            _bucket_summary("symbol_direction_cohort", cohort, cohort, cohort_rows, len(primary_rows))
        )

    by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for bucket in buckets:
        by_feature[str(bucket["feature"])].append(bucket)

    return {
        "primary_min_tier": primary_min_tier,
        "primary_row_count": len(primary_rows),
        "analysis_note": (
            "descriptive historical measured rates only; not a probability forecast, "
            "prediction model, or execution signal"
        ),
        "is_prediction": False,
        "model_training_added": False,
        "trading_signal_added": False,
        "buckets": buckets,
        "by_feature": dict(sorted(by_feature.items())),
    }


def _build_cohort_sensitivity(
    rows: list[dict[str, Any]],
    *,
    primary_min_tier: str,
    include_diagnostic: bool,
    primary_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    cohorts = {
        "A only": [row for row in rows if _tier_key(row) == "A"],
        "A/B": [row for row in rows if QUALITY_ORDER[_tier_key(row)] <= QUALITY_ORDER["B"]],
        "A/B/C": [row for row in rows if QUALITY_ORDER[_tier_key(row)] <= QUALITY_ORDER["C"]],
        "all rows": list(rows),
    }
    cohort_summaries = {
        name: _cohort_summary(cohort_rows)
        for name, cohort_rows in cohorts.items()
    }
    primary_summary = _cohort_summary(primary_rows)
    all_summary = cohort_summaries["all rows"]
    success_delta = _delta(primary_summary["success_rate"], all_summary["success_rate"])
    delay_delta = _delta(
        primary_summary["median_executable_repricing_delay_ms"],
        all_summary["median_executable_repricing_delay_ms"],
    )
    edge_delta = _delta(
        primary_summary["median_exit_edge_ticks"],
        all_summary["median_exit_edge_ticks"],
    )
    if (
        len(primary_rows) < MIN_PRIMARY_ROWS
        or cohort_summaries["A only"]["row_count"] < MIN_BUCKET_ROWS
        or len(rows) < MIN_BUCKET_ROWS
    ):
        conclusion = "insufficient_data"
    elif success_delta is not None and abs(success_delta) > 0.10:
        conclusion = "unstable"
    elif delay_delta is not None and abs(delay_delta) > 500:
        conclusion = "unstable"
    elif edge_delta is not None and abs(edge_delta) > 5:
        conclusion = "unstable"
    else:
        conclusion = "stable"
    return {
        "primary_min_tier": primary_min_tier,
        "include_diagnostic_in_primary": include_diagnostic,
        "cohorts": cohort_summaries,
        "primary_vs_all_success_rate_delta": success_delta,
        "primary_vs_all_delay_delta": delay_delta,
        "primary_vs_all_edge_delta": edge_delta,
        "conclusion": conclusion,
    }


def _build_runtime_coverage_analysis(
    gap_rows: list[dict[str, Any]],
    *,
    runtime_summary_jsonl_path: str | Path | None,
) -> dict[str, Any]:
    if runtime_summary_jsonl_path is None:
        return {
            "status": "skipped_no_runtime_summary_input",
            "runtime_summary_path": None,
            "runtime_summary_rows": 0,
            "warning_flags": [],
        }

    summary_path = Path(runtime_summary_jsonl_path)
    try:
        audit = _read_jsonl_with_audit(summary_path)
    except FileNotFoundError:
        return {
            "status": "skipped_missing_runtime_summary_input",
            "runtime_summary_path": str(summary_path),
            "runtime_summary_rows": 0,
            "warning_flags": ["runtime_summary_jsonl_missing"],
        }

    runtime_rows = [
        row
        for row in audit.rows
        if row.get("event_type") == "runtime_summary" or row.get("generated_ts_ns") is not None
    ]
    runtime_timestamps = sorted(_int_values(runtime_rows, "generated_ts_ns"))
    gap_timestamps = sorted(_int_values(gap_rows, "detected_ts_ns"))
    runtime_duration_ns = (
        runtime_timestamps[-1] - runtime_timestamps[0]
        if len(runtime_timestamps) >= 2
        else None
    )
    gap_event_duration_ns = (
        gap_timestamps[-1] - gap_timestamps[0] if len(gap_timestamps) >= 2 else 0
    )
    coverage_ratio = (
        gap_event_duration_ns / runtime_duration_ns
        if runtime_duration_ns is not None and runtime_duration_ns > 0
        else None
    )
    warning_flags: list[str] = []
    if runtime_duration_ns is not None and runtime_duration_ns > 0:
        if not gap_timestamps or (coverage_ratio is not None and coverage_ratio < 0.50):
            warning_flags.append("gap_event_coverage_shorter_than_runtime")

    no_event_warning_counts: Counter[str] = Counter()
    for row in runtime_rows:
        warnings = row.get("no_event_warnings")
        if isinstance(warnings, list):
            for warning in warnings:
                no_event_warning_counts[str(warning)] += 1
        single_warning = row.get("no_event_warning")
        if single_warning:
            no_event_warning_counts[str(single_warning)] += 1

    if no_event_warning_counts:
        warning_flags.append("runtime_summary_contains_no_event_warnings")

    return {
        "status": "analyzed",
        "runtime_summary_path": str(summary_path),
        "runtime_summary_rows": len(runtime_rows),
        "runtime_duration_ns": runtime_duration_ns,
        "runtime_duration_minutes": (
            runtime_duration_ns / 1_000_000_000 / 60
            if runtime_duration_ns is not None
            else None
        ),
        "gap_event_rows": len(gap_rows),
        "gap_event_duration_ns": gap_event_duration_ns,
        "gap_event_duration_minutes": gap_event_duration_ns / 1_000_000_000 / 60,
        "gap_event_time_coverage_ratio": coverage_ratio,
        "no_event_warning_counts": dict(sorted(no_event_warning_counts.items())),
        "warning_flags": sorted(set(warning_flags)),
    }


def _build_warnings(
    *,
    input_audit: dict[str, Any],
    schema_audit: dict[str, Any],
    dataset_health: dict[str, Any],
    timing_analysis: dict[str, Any],
    stale_feed_analysis: dict[str, Any],
    tick_calibration_analysis: dict[str, Any],
    runtime_coverage_analysis: dict[str, Any],
    empirical_bucket_analysis: dict[str, Any],
    cohort_sensitivity: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if input_audit["malformed_json_lines"] > 0:
        warnings.append("malformed_json_lines_present")
    if schema_audit["core_fields_missing"]:
        warnings.append("schema_missing_core_fields")
    if schema_audit["core_fields_incomplete"]:
        warnings.append("schema_core_fields_incomplete")
    if dataset_health["primary_rows"] < MIN_PRIMARY_ROWS:
        warnings.append("primary_rows_below_minimum")
    tier_counts = dataset_health["rows_by_data_quality_tier"]
    tier_d_rate = _rate(int(tier_counts.get("D", 0)), dataset_health["included_rows"])
    if tier_d_rate > MAX_TIER_D_RATE:
        warnings.append("tier_d_rate_above_threshold")
    quote_stale_rate = stale_feed_analysis["quote_stale_rate"]
    if _is_number(quote_stale_rate) and quote_stale_rate > MAX_QUOTE_STALE_RATE:
        warnings.append("quote_stale_rate_above_threshold")
    if stale_feed_analysis["quote_age_fields_missing"]:
        warnings.append("quote_age_fields_missing")
    warnings.extend(str(flag) for flag in tick_calibration_analysis.get("warning_flags", []))
    warnings.extend(str(flag) for flag in runtime_coverage_analysis.get("warning_flags", []))
    book_incomplete_rate = _rate(
        int(dataset_health["rows_by_reject_reason"].get("book_incomplete", 0)),
        dataset_health["included_rows"],
    )
    if book_incomplete_rate > MAX_BOOK_INCOMPLETE_RATE:
        warnings.append("book_incomplete_rate_above_threshold")
    for flag, flagged in timing_analysis["latency_readiness_flags"].items():
        if flagged:
            warnings.append(flag)
    sparse_buckets = [
        bucket
        for bucket in empirical_bucket_analysis["buckets"]
        if bucket["insufficient_sample_warning"] and bucket["row_count"] > 0
    ]
    if sparse_buckets:
        warnings.append("empirical_buckets_sparse")
    if cohort_sensitivity["conclusion"] == "unstable":
        warnings.append("cohort_sensitivity_unstable")
    return sorted(set(warnings))


def _build_readiness_assessment(
    *,
    input_audit: dict[str, Any],
    schema_audit: dict[str, Any],
    dataset_health: dict[str, Any],
    timing_analysis: dict[str, Any],
    edge_analysis: dict[str, Any],
    stale_feed_analysis: dict[str, Any],
    runtime_coverage_analysis: dict[str, Any],
    empirical_bucket_analysis: dict[str, Any],
    cohort_sensitivity: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    checks = _readiness_checks(
        input_audit=input_audit,
        schema_audit=schema_audit,
        dataset_health=dataset_health,
        timing_analysis=timing_analysis,
        edge_analysis=edge_analysis,
        stale_feed_analysis=stale_feed_analysis,
        runtime_coverage_analysis=runtime_coverage_analysis,
        empirical_bucket_analysis=empirical_bucket_analysis,
        cohort_sensitivity=cohort_sensitivity,
    )
    blocking_issues = [
        str(check["message"])
        for check in checks
        if check["status"] == "FAIL" and check["severity"] == "blocking"
    ]
    non_blocking_warnings = [
        str(check["message"])
        for check in checks
        if check["status"] in {"FAIL", "WARN"} and check["severity"] != "blocking"
    ]

    parsed_rows = input_audit["parsed_json_rows"]
    primary_rows = dataset_health["primary_rows"]
    tier_d_rate = _rate(
        int(dataset_health["rows_by_data_quality_tier"].get("D", 0)),
        dataset_health["included_rows"],
    )
    book_incomplete_rate = _rate(
        int(dataset_health["rows_by_reject_reason"].get("book_incomplete", 0)),
        dataset_health["included_rows"],
    )
    success_count = dataset_health["success_count"]
    timing_available = (
        timing_analysis["overall"]["executable_repricing_delay_ms"]["count"] > 0
        and timing_analysis["overall"]["tradable_window_ms"]["count"] > 0
    )
    edge_available = edge_analysis["overall"]["exit_edge_ticks"]["count"] > 0
    sparse_primary_bucket_count = sum(
        1
        for bucket in empirical_bucket_analysis["buckets"]
        if bucket["insufficient_sample_warning"] and bucket["row_count"] > 0
    )

    reasons: list[str] = []
    if parsed_rows == 0:
        classification = "NOT_READY"
        reasons.append("parsed rows are zero")
    elif primary_rows == 0:
        classification = "NOT_READY"
        reasons.append("primary rows are zero")
    elif schema_audit["core_fields_missing"]:
        classification = "NOT_READY"
        reasons.append("schema is missing core fields")
    elif primary_rows < MIN_PRIMARY_ROWS:
        classification = "NEEDS_MORE_DATA"
        reasons.append("primary row count is below the Phase 4 threshold")
    elif success_count < MIN_SUCCESS_ROWS_FOR_EMPIRICAL:
        classification = "NEEDS_MORE_DATA"
        reasons.append("measured executable repricing success rows are below threshold")
    elif (
        tier_d_rate > MAX_TIER_D_RATE
        or _rate_exceeds(stale_feed_analysis["quote_stale_rate"], MAX_QUOTE_STALE_RATE)
        or book_incomplete_rate > MAX_BOOK_INCOMPLETE_RATE
        or cohort_sensitivity["conclusion"] == "unstable"
    ):
        classification = "NEEDS_MORE_CLEANING"
        reasons.append("quality, staleness, book completeness, or cohort stability failed checks")
    elif not timing_available or not edge_available:
        classification = "NEEDS_MORE_CLEANING"
        reasons.append("timing or tick-normalized edge fields are not sufficiently available")
    elif (
        primary_rows >= STRONG_PRIMARY_ROWS_FOR_BASELINE_MODEL_RESEARCH
        and success_count >= STRONG_SUCCESS_ROWS_FOR_BASELINE_MODEL_RESEARCH
        and tier_d_rate <= 0.05
        and _rate_at_or_below(stale_feed_analysis["quote_stale_rate"], 0.02)
        and sparse_primary_bucket_count == 0
        and not warnings
    ):
        classification = "READY_FOR_BASELINE_MODEL_RESEARCH"
        reasons.append("dataset passes stronger conservative baseline-model readiness checks")
    else:
        classification = "READY_FOR_EMPIRICAL_RESEARCH"
        reasons.append("dataset is sufficient for descriptive microstructure empirical research")

    recommended_next_phase = _recommended_next_phase(classification)
    if classification != "READY_FOR_BASELINE_MODEL_RESEARCH":
        non_blocking_warnings.append("Do not proceed to model prediction yet")
    return {
        "classification": classification,
        "reasons": reasons,
        "blocking_issues": blocking_issues,
        "non_blocking_warnings": sorted(set(non_blocking_warnings)),
        "recommended_next_phase": recommended_next_phase,
        "checks": checks,
    }


def _readiness_checks(
    *,
    input_audit: dict[str, Any],
    schema_audit: dict[str, Any],
    dataset_health: dict[str, Any],
    timing_analysis: dict[str, Any],
    edge_analysis: dict[str, Any],
    stale_feed_analysis: dict[str, Any],
    runtime_coverage_analysis: dict[str, Any],
    empirical_bucket_analysis: dict[str, Any],
    cohort_sensitivity: dict[str, Any],
) -> list[dict[str, Any]]:
    tier_d_rate = _rate(
        int(dataset_health["rows_by_data_quality_tier"].get("D", 0)),
        dataset_health["included_rows"],
    )
    book_incomplete_rate = _rate(
        int(dataset_health["rows_by_reject_reason"].get("book_incomplete", 0)),
        dataset_health["included_rows"],
    )
    timing_count = timing_analysis["overall"]["executable_repricing_delay_ms"]["count"]
    edge_count = edge_analysis["overall"]["exit_edge_ticks"]["count"]
    bucket_sufficient_count = sum(
        1
        for bucket in empirical_bucket_analysis["buckets"]
        if bucket["row_count"] >= MIN_BUCKET_ROWS
    )
    quote_stale_check = (
        {
            "check_name": "quote_stale_rate",
            "status": "WARN",
            "value": stale_feed_analysis["staleness_status"],
            "threshold": MAX_QUOTE_STALE_RATE,
            "severity": "warning",
            "message": (
                "quote stale rate cannot be confidently assessed because quote-age "
                "fields are missing"
            ),
        }
        if stale_feed_analysis["quote_age_fields_missing"]
        else _check(
            "quote_stale_rate",
            stale_feed_analysis["quote_stale_rate"] <= MAX_QUOTE_STALE_RATE,
            stale_feed_analysis["quote_stale_rate"],
            MAX_QUOTE_STALE_RATE,
            "blocking",
            "quote stale rate should be below threshold",
        )
    )
    checks = [
        _check(
            "parsed_rows_nonzero",
            input_audit["parsed_json_rows"] > 0,
            input_audit["parsed_json_rows"],
            "> 0",
            "blocking",
            "parsed rows must be nonzero",
        ),
        _check(
            "primary_rows_nonzero",
            dataset_health["primary_rows"] > 0,
            dataset_health["primary_rows"],
            "> 0",
            "blocking",
            "primary rows must be nonzero",
        ),
        _check(
            "core_schema_present",
            not schema_audit["core_fields_missing"],
            schema_audit["core_fields_missing"],
            "no missing core fields",
            "blocking",
            "schema must include core fields",
        ),
        _check(
            "primary_rows_minimum",
            dataset_health["primary_rows"] >= MIN_PRIMARY_ROWS,
            dataset_health["primary_rows"],
            MIN_PRIMARY_ROWS,
            "blocking",
            "primary rows should meet the Phase 4 minimum",
        ),
        _check(
            "tier_d_rate",
            tier_d_rate <= MAX_TIER_D_RATE,
            tier_d_rate,
            MAX_TIER_D_RATE,
            "blocking",
            "D-tier diagnostic row share should be below threshold",
        ),
        quote_stale_check,
        _check(
            "quote_age_fields_missing",
            not stale_feed_analysis["quote_age_fields_missing"],
            stale_feed_analysis["quote_age_fields_missing"],
            False,
            "warning",
            "quote_age_fields_missing",
        ),
        _check(
            "book_incomplete_rate",
            book_incomplete_rate <= MAX_BOOK_INCOMPLETE_RATE,
            book_incomplete_rate,
            MAX_BOOK_INCOMPLETE_RATE,
            "blocking",
            "book incomplete reject rate should be below threshold",
        ),
        _check(
            "runtime_gap_event_coverage",
            "gap_event_coverage_shorter_than_runtime"
            not in runtime_coverage_analysis.get("warning_flags", []),
            runtime_coverage_analysis.get("gap_event_time_coverage_ratio"),
            ">= 0.50 when runtime summary JSONL is provided",
            "warning",
            "gap_event_coverage_shorter_than_runtime",
        ),
        _check(
            "success_rows_for_empirical",
            dataset_health["success_count"] >= MIN_SUCCESS_ROWS_FOR_EMPIRICAL,
            dataset_health["success_count"],
            MIN_SUCCESS_ROWS_FOR_EMPIRICAL,
            "warning",
            "measured executable repricing success rows should support empirical summaries",
        ),
        _check(
            "timing_fields_available",
            timing_count > 0,
            timing_count,
            "> 0",
            "blocking",
            "executable repricing delay should be available",
        ),
        _check(
            "edge_tick_fields_available",
            edge_count > 0,
            edge_count,
            "> 0",
            "blocking",
            "tick-normalized edge should be available",
        ),
        _check(
            "cohort_sensitivity_stable",
            cohort_sensitivity["conclusion"] != "unstable",
            cohort_sensitivity["conclusion"],
            "stable or insufficient_data",
            "blocking",
            "quality cohort sensitivity should not be unstable",
        ),
        _check(
            "empirical_bucket_samples",
            bucket_sufficient_count > 0,
            bucket_sufficient_count,
            f">= 1 bucket with {MIN_BUCKET_ROWS}+ rows",
            "warning",
            "at least one empirical bucket should have enough rows",
        ),
    ]
    return checks


def _check(
    name: str,
    passed: bool,
    value: Any,
    threshold: Any,
    severity: str,
    message: str,
) -> dict[str, Any]:
    return {
        "check_name": name,
        "status": "PASS" if passed else ("FAIL" if severity == "blocking" else "WARN"),
        "value": value,
        "threshold": threshold,
        "severity": severity,
        "message": message,
    }


def _recommended_next_phase(classification: str) -> str:
    if classification == "NOT_READY":
        return "Do not proceed to model prediction yet"
    if classification == "NEEDS_MORE_DATA":
        return "Collect more Phase 3 data"
    if classification == "NEEDS_MORE_CLEANING":
        return "Fix order book/data quality issues"
    if classification == "READY_FOR_EMPIRICAL_RESEARCH":
        return "Run Phase 5.0 microstructure empirical signal research"
    return "Run Phase 4.1 cleaning and label refinement"


def _analyze_mismatch_samples(path: Path) -> dict[str, Any]:
    try:
        audit = _read_jsonl_with_audit(path)
    except FileNotFoundError:
        return {
            "mismatch_sample_status": "skipped_missing_mismatch_sample_input",
            "mismatch_sample_path": str(path),
        }
    samples = audit.rows
    tick_diffs: list[float] = []
    abs_price_diffs: list[float] = []
    bid_mismatch_count = 0
    ask_mismatch_count = 0
    error_types: Counter[str] = Counter()
    markets: Counter[str] = Counter()
    tokens: Counter[str] = Counter()
    for sample in samples:
        error_types[str(sample.get("error_type") or sample.get("validation_error") or sample.get("reason") or "unknown")] += 1
        if sample.get("market_id") is not None:
            markets[str(sample.get("market_id"))] += 1
        if sample.get("token_id") is not None:
            tokens[str(sample.get("token_id"))] += 1
        side = str(sample.get("side") or sample.get("book_side") or "").lower()
        if "bid" in side:
            bid_mismatch_count += 1
        if "ask" in side:
            ask_mismatch_count += 1
        abs_diff = _first_number(sample, ("abs_price_diff", "absolute_price_diff", "price_diff_abs"))
        if abs_diff is None:
            local_price = _first_number(sample, ("local_price", "local_best", "book_price"))
            reported_price = _first_number(sample, ("reported_price", "reported_best", "api_price"))
            if local_price is not None and reported_price is not None:
                abs_diff = abs(local_price - reported_price)
        if abs_diff is not None:
            abs_price_diffs.append(abs_diff)
        tick_diff = _first_number(sample, ("tick_diff", "ticks_diff", "abs_tick_diff"))
        if tick_diff is None and abs_diff is not None:
            tick_size = _first_number(sample, ("tick_size", "tick_size_at_detection"))
            if tick_size is not None and tick_size > 0:
                tick_diff = abs_diff / tick_size
        if tick_diff is not None:
            tick_diffs.append(abs(tick_diff))

    within_1 = sum(1 for value in tick_diffs if value <= 1)
    within_2 = sum(1 for value in tick_diffs if value <= 2)
    above_2 = sum(1 for value in tick_diffs if value > 2)
    pct_within_1 = _rate(within_1, len(tick_diffs))
    pct_within_2 = _rate(within_2, len(tick_diffs))
    if tick_diffs and pct_within_1 >= 0.95:
        recommendation: int | str = 1
    elif tick_diffs and pct_within_2 >= 0.95:
        recommendation = 2
    else:
        recommendation = "investigate_before_increasing_tolerance"
    return {
        "mismatch_sample_status": "loaded",
        "mismatch_sample_path": str(path),
        "mismatch_total": len(samples),
        "mismatch_malformed_json_lines": len(audit.malformed_rows),
        "mismatch_by_error_type": dict(sorted(error_types.items())),
        "top_affected_markets": dict(markets.most_common(10)),
        "top_affected_tokens": dict(tokens.most_common(10)),
        "abs_price_diff": _summary_values(abs_price_diffs, len(samples)),
        "tick_diff": _summary_values(tick_diffs, len(samples)),
        "tick_diff_distribution": dict(sorted(Counter(_tick_diff_bucket(value) for value in tick_diffs).items())),
        "pct_within_1_tick": pct_within_1,
        "pct_within_2_ticks": pct_within_2,
        "pct_above_2_ticks": _rate(above_2, len(tick_diffs)),
        "bid_mismatch_count": bid_mismatch_count,
        "ask_mismatch_count": ask_mismatch_count,
        "recommendation_for_tolerance_ticks": recommendation,
    }


def _add_missing_mismatch_sample_warning(analysis: dict[str, Any]) -> None:
    if int(analysis.get("tolerated_mismatch_row_count", 0)) <= 0:
        analysis["warning_flags"] = []
        return
    analysis["warning_flags"] = ["tolerated_mismatch_rows_without_mismatch_samples"]
    analysis["mismatch_sample_recommendation"] = (
        "Rerun with --mismatch-samples when Polymarket orderbook mismatch samples "
        "are available to calibrate tolerated one-tick rows."
    )


def _metric_summary(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    return _summary_values(_numbers(rows, field), len(rows))


def _edge_summary(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = _numbers(rows, field)
    summary = _summary_values(values, len(rows))
    positive_count = sum(1 for value in values if value > 0)
    zero_count = sum(1 for value in values if value == 0)
    negative_count = sum(1 for value in values if value < 0)
    summary.update(
        {
            "positive_count": positive_count,
            "positive_rate": _rate(positive_count, len(values)),
            "zero_count": zero_count,
            "negative_count": negative_count,
        }
    )
    return summary


def _summary_values(values: list[float], total_count: int) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "missing_count": total_count,
            "min": None,
            "mean": None,
            "median": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    sorted_values = sorted(values)
    return {
        "count": len(sorted_values),
        "missing_count": max(0, total_count - len(sorted_values)),
        "min": sorted_values[0],
        "mean": sum(sorted_values) / len(sorted_values),
        "median": median(sorted_values),
        "p75": _percentile(sorted_values, 0.75),
        "p90": _percentile(sorted_values, 0.90),
        "p95": _percentile(sorted_values, 0.95),
        "p99": _percentile(sorted_values, 0.99),
        "max": sorted_values[-1],
    }


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = int(round((len(sorted_values) - 1) * percentile))
    return sorted_values[min(max(index, 0), len(sorted_values) - 1)]


def _metrics_by_group(
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
    key_fn: Callable[[dict[str, Any]], str],
    ordered_keys: Iterable[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    grouped = {key: [row for row in rows if key_fn(row) == key] for key in ordered_keys}
    return {
        key: {field: _metric_summary(group_rows, field) for field in fields}
        for key, group_rows in grouped.items()
    }


def _metrics_by_field(
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
    field_name: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    grouped = _rows_by_field(rows, field_name)
    return {
        key: {field: _metric_summary(group_rows, field) for field in fields}
        for key, group_rows in grouped.items()
    }


def _edge_by_group(
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
    key_fn: Callable[[dict[str, Any]], str],
    ordered_keys: Iterable[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    grouped = {key: [row for row in rows if key_fn(row) == key] for key in ordered_keys}
    return {
        key: {field: _edge_summary(group_rows, field) for field in fields}
        for key, group_rows in grouped.items()
    }


def _edge_by_field(
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
    field_name: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    grouped = _rows_by_field(rows, field_name)
    return {
        key: {field: _edge_summary(group_rows, field) for field in fields}
        for key, group_rows in grouped.items()
    }


def _rows_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_value_key(row.get(field))].append(row)
    return dict(sorted(grouped.items()))


def _filter_quality(
    rows: list[dict[str, Any]],
    min_quality_tier: str | None,
) -> list[dict[str, Any]]:
    if min_quality_tier is None:
        return list(rows)
    max_rank = QUALITY_ORDER[min_quality_tier]
    return [row for row in rows if QUALITY_ORDER[_tier_key(row)] <= max_rank]


def _filter_primary_rows(
    rows: list[dict[str, Any]],
    *,
    primary_min_tier: str,
    include_diagnostic: bool,
) -> list[dict[str, Any]]:
    max_rank = QUALITY_ORDER[primary_min_tier]
    return [
        row
        for row in rows
        if QUALITY_ORDER[_tier_key(row)] <= max_rank
        and (include_diagnostic or _validation_mode_key(row) != "diagnostic")
    ]


def _normalize_tier_or_none(tier: str | None) -> str | None:
    if tier is None:
        return None
    normalized = tier.upper()
    if normalized not in QUALITY_ORDER:
        raise ValueError(f"unknown quality tier: {tier}")
    return normalized


def _normalize_primary_tier(tier: str) -> str:
    normalized = tier.upper()
    if normalized not in {"A", "B"}:
        raise ValueError("primary_min_tier must be A or B")
    return normalized


def _tier_key(row: dict[str, Any]) -> str:
    tier = str(row.get("data_quality_tier") or "D").upper()
    return tier if tier in QUALITY_ORDER else "D"


def _validation_mode_key(row: dict[str, Any]) -> str:
    mode = str(row.get("validation_mode") or "unknown").lower()
    return mode if mode in VALIDATION_MODES else "unknown"


def _reason_key(row: dict[str, Any]) -> str:
    reason = row.get("reject_reason")
    return str(reason) if reason is not None else "unknown"


def _success_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("reject_stage") == "none")


def _field_counter(
    rows: list[dict[str, Any]],
    field: str,
    *,
    include_missing: bool = True,
    limit: int | None = None,
) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = row.get(field)
        if value is None and not include_missing:
            continue
        counter[_value_key(value)] += 1
    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    if limit is not None:
        items = items[:limit]
    return dict(items)


def _tier_counter(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(_tier_key(row) for row in rows)
    return {tier: counter.get(tier, 0) for tier in QUALITY_TIERS}


def _validation_mode_counter(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(_validation_mode_key(row) for row in rows)
    return {mode: counter.get(mode, 0) for mode in VALIDATION_MODES}


def _symbol_direction_counter(
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> dict[str, int]:
    counter = Counter(
        f"{_value_key(row.get('symbol'))}:{_value_key(row.get('direction'))}" for row in rows
    )
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit])


def _non_null_unique(rows: list[dict[str, Any]], field: str) -> set[str]:
    return {str(row[field]) for row in rows if field in row and row[field] is not None}


def _numbers(rows: list[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if _is_number(value):
            values.append(float(value))
    return values


def _int_values(rows: list[dict[str, Any]], field: str) -> list[int]:
    values: list[int] = []
    for row in rows:
        value = row.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            values.append(value)
    return values


def _is_number(value: Any) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _first_number(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    for field in fields:
        value = row.get(field)
        if _is_number(value):
            return float(value)
    return None


def _rate(count: int | float, total: int | float) -> float:
    if total == 0:
        return 0.0
    return float(count) / float(total)


def _delta(left: Any, right: Any) -> float | None:
    if _is_number(left) and _is_number(right):
        return float(left) - float(right)
    return None


def _gt(value: Any, threshold: float) -> bool:
    return _is_number(value) and float(value) > threshold


def _lt(value: Any, threshold: float) -> bool:
    return _is_number(value) and float(value) < threshold


def _rate_exceeds(value: Any, threshold: float) -> bool:
    return _is_number(value) and float(value) > threshold


def _rate_at_or_below(value: Any, threshold: float) -> bool:
    return _is_number(value) and float(value) <= threshold


def _value_key(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _ns_to_iso(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()


def _reject_group_stats(
    rows: list[dict[str, Any]],
    total_rows: int,
    rejected_rows: int,
) -> dict[str, Any]:
    binance_ages = _numbers(rows, "binance_quote_age_ms")
    polymarket_ages = _numbers(rows, "polymarket_quote_age_ms")
    return {
        "count": len(rows),
        "percentage_of_total_rows": _rate(len(rows), total_rows),
        "percentage_of_rejected_rows": _rate(len(rows), rejected_rows),
        "tier_distribution": _tier_counter(rows),
        "validation_mode_distribution": _validation_mode_counter(rows),
        "symbol_distribution": _field_counter(rows, "symbol"),
        "direction_distribution": _field_counter(rows, "direction"),
        "median_binance_quote_age_ms": median(binance_ages) if binance_ages else None,
        "median_polymarket_quote_age_ms": median(polymarket_ages) if polymarket_ages else None,
    }


def _compare_row_sets(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    left_success_rate = _rate(_success_count(left_rows), len(left_rows))
    right_success_rate = _rate(_success_count(right_rows), len(right_rows))
    left_delay = _metric_summary(left_rows, "executable_repricing_delay_ms")["median"]
    right_delay = _metric_summary(right_rows, "executable_repricing_delay_ms")["median"]
    left_edge = _metric_summary(left_rows, "exit_edge_ticks")["median"]
    right_edge = _metric_summary(right_rows, "exit_edge_ticks")["median"]
    return {
        "left_row_count": len(left_rows),
        "right_row_count": len(right_rows),
        "left_success_rate": left_success_rate,
        "right_success_rate": right_success_rate,
        "success_rate_delta": left_success_rate - right_success_rate,
        "left_median_executable_delay_ms": left_delay,
        "right_median_executable_delay_ms": right_delay,
        "median_executable_delay_delta": _delta(left_delay, right_delay),
        "left_median_exit_edge_ticks": left_edge,
        "right_median_exit_edge_ticks": right_edge,
        "median_exit_edge_ticks_delta": _delta(left_edge, right_edge),
    }


def _validation_distribution_conclusion(comparison: dict[str, Any]) -> dict[str, Any]:
    if (
        comparison["left_row_count"] < MIN_BUCKET_ROWS
        or comparison["right_row_count"] < MIN_BUCKET_ROWS
    ):
        return {
            "tolerant_mode_materially_changes_distribution": "unknown",
            "reason": "strict and tolerant cohorts do not both have enough rows",
        }
    success_delta = abs(float(comparison["success_rate_delta"]))
    delay_delta = comparison["median_executable_delay_delta"]
    edge_delta = comparison["median_exit_edge_ticks_delta"]
    material = success_delta > 0.10
    if delay_delta is not None and abs(delay_delta) > 500:
        material = True
    if edge_delta is not None and abs(edge_delta) > 5:
        material = True
    return {
        "tolerant_mode_materially_changes_distribution": material,
        "reason": (
            "strict and tolerant measured distributions diverge beyond conservative thresholds"
            if material
            else "strict and tolerant measured distributions are within conservative thresholds"
        ),
    }


def _bucket_counter(
    rows: list[dict[str, Any]],
    field: str,
    bucket_fn: Callable[[float], str],
) -> dict[str, int]:
    counter: Counter[str] = Counter()
    missing = 0
    for row in rows:
        value = row.get(field)
        if not _is_number(value):
            missing += 1
            continue
        counter[bucket_fn(float(value))] += 1
    if missing:
        counter["missing"] += missing
    return dict(sorted(counter.items()))


def _spread_liquidity_bucket(value: float) -> str:
    if value < 0:
        return "<0"
    if value == 0:
        return "0"
    if value <= 1:
        return "0-1"
    if value <= 5:
        return "1-5"
    if value <= 10:
        return "5-10"
    return ">10"


def _spread_ticks_bucket(value: float) -> str:
    if value == 0:
        return "0"
    if value == 1:
        return "1"
    if value == 2:
        return "2"
    if 3 <= value <= 5:
        return "3-5"
    if 6 <= value <= 10:
        return "6-10"
    if value > 10:
        return ">10"
    return "other"


def _edge_ticks_bucket(value: float) -> str:
    if value < 0:
        return "<0"
    if value == 0:
        return "0"
    if value <= 1:
        return "0-1"
    if value <= 2:
        return "1-2"
    if value <= 5:
        return "2-5"
    return ">5"


def _tick_diff_bucket(value: float) -> str:
    if value <= 1:
        return "<=1"
    if value <= 2:
        return "<=2"
    return ">2"


def _zero_count(rows: list[dict[str, Any]], field: str) -> int:
    return sum(1 for row in rows if _is_number(row.get(field)) and float(row[field]) == 0)


def _row_has_tolerated_mismatch(row: dict[str, Any]) -> bool:
    reason = str(row.get("data_quality_reason") or "").lower()
    if "tolerat" in reason and "mismatch" in reason:
        return True
    if "one_tick" in reason or "1_tick" in reason or "one-tick" in reason:
        return True
    return _validation_mode_key(row) == "tolerant" and _tier_key(row) == "B"


def _stale_source_is_unknown(value: Any) -> bool:
    return value is None or str(value).strip().lower() in {"", "unknown"}


def _time_bucket_specs() -> tuple[tuple[str, str, Callable[[float], bool]], ...]:
    return (
        ("0-50ms", "0-50ms", lambda value: 0 <= value <= 50),
        ("50-100ms", "50-100ms", lambda value: 50 < value <= 100),
        ("100-250ms", "100-250ms", lambda value: 100 < value <= 250),
        ("250-500ms", "250-500ms", lambda value: 250 < value <= 500),
        ("500-1000ms", "500-1000ms", lambda value: 500 < value <= 1000),
        (">1000ms", ">1000ms", lambda value: value > 1000),
    )


def _numeric_bucket_rows(
    rows: list[dict[str, Any]],
    feature: str,
    field: str,
    specs: tuple[tuple[str, str, Callable[[float], bool]], ...],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for bucket_name, bucket_range, predicate in specs:
        bucket_rows = [
            row
            for row in rows
            if _is_number(row.get(field)) and predicate(float(row[field]))
        ]
        output.append(_bucket_summary(feature, bucket_name, bucket_range, bucket_rows, len(rows)))
    missing_rows = [row for row in rows if not _is_number(row.get(field))]
    if missing_rows:
        output.append(_bucket_summary(feature, "missing", "missing", missing_rows, len(rows)))
    return output


def _bucket_summary(
    feature: str,
    bucket_name: str,
    bucket_range: str,
    rows: list[dict[str, Any]],
    denominator: int,
) -> dict[str, Any]:
    success_count = _success_count(rows)
    return {
        "feature": feature,
        "bucket_name": bucket_name,
        "bucket_range": bucket_range,
        "row_count": len(rows),
        "row_rate": _rate(len(rows), denominator),
        "success_count": success_count,
        "success_rate": _rate(success_count, len(rows)),
        "median_executable_repricing_delay_ms": _metric_summary(
            rows,
            "executable_repricing_delay_ms",
        )["median"],
        "median_tradable_window_ms": _metric_summary(rows, "tradable_window_ms")["median"],
        "median_exit_edge_ticks": _metric_summary(rows, "exit_edge_ticks")["median"],
        "p95_exit_edge_ticks": _metric_summary(rows, "exit_edge_ticks")["p95"],
        "insufficient_sample_warning": len(rows) < MIN_BUCKET_ROWS,
        "rate_wording": "historical measured rate in this dataset",
    }


def _cohort_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    success_count = _success_count(rows)
    warnings: list[str] = []
    if len(rows) < MIN_BUCKET_ROWS:
        warnings.append("row_count_below_bucket_minimum")
    if _metric_summary(rows, "executable_repricing_delay_ms")["count"] == 0:
        warnings.append("executable_delay_missing")
    if any(_tier_key(row) == "D" for row in rows):
        warnings.append("contains_tier_D_diagnostic_rows")
    return {
        "row_count": len(rows),
        "success_count": success_count,
        "success_rate": _rate(success_count, len(rows)),
        "top_reject_reasons": _field_counter(rows, "reject_reason", include_missing=False, limit=5),
        "median_executable_repricing_delay_ms": _metric_summary(
            rows,
            "executable_repricing_delay_ms",
        )["median"],
        "median_tradable_window_ms": _metric_summary(rows, "tradable_window_ms")["median"],
        "median_exit_edge_ticks": _metric_summary(rows, "exit_edge_ticks")["median"],
        "warning_flags": warnings,
    }


def _write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field)) for field in fieldnames})


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _cohort_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for cohort, summary in report["cohort_sensitivity"]["cohorts"].items():
        rows.append(
            {
                "cohort": cohort,
                "row_count": summary["row_count"],
                "success_count": summary["success_count"],
                "success_rate": summary["success_rate"],
                "median_executable_repricing_delay_ms": summary["median_executable_repricing_delay_ms"],
                "median_tradable_window_ms": summary["median_tradable_window_ms"],
                "median_exit_edge_ticks": summary["median_exit_edge_ticks"],
                "warnings": summary["warning_flags"],
            }
        )
    return rows


def _reject_taxonomy_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for reason, summary in report["reject_taxonomy"]["reasons"].items():
        rows.append(
            {
                "category": summary["category"],
                "reason": reason,
                "count": summary["count"],
                "pct_total_rows": summary["percentage_of_total_rows"],
                "pct_rejected_rows": summary["percentage_of_rejected_rows"],
                "top_tier": _top_key(summary["tier_distribution"]),
                "top_validation_mode": _top_key(summary["validation_mode_distribution"]),
            }
        )
    return rows


def _quality_tier_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for tier, summary in report["quality_tier_analysis"]["tiers"].items():
        rows.append(
            {
                "tier": tier,
                "row_count": summary["row_count"],
                "row_rate": summary["row_rate"],
                "success_count": summary["success_count"],
                "success_rate": summary["success_rate"],
                "top_reject_reason": _top_key(summary["reject_reason_top"]),
                "median_delay": summary["median_executable_repricing_delay_ms"],
                "median_window": summary["median_tradable_window_ms"],
                "median_edge_ticks": summary["median_exit_edge_ticks"],
            }
        )
    return rows


def _validation_mode_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for mode, summary in report["validation_mode_analysis"]["modes"].items():
        rows.append(
            {
                "validation_mode": mode,
                "row_count": summary["row_count"],
                "row_rate": summary["row_rate"],
                "success_count": summary["success_count"],
                "success_rate": summary["success_rate"],
                "tier_distribution": summary["quality_tier_distribution"],
                "median_delay": summary["median_timing_metrics"]["executable_repricing_delay_ms"],
                "median_edge_ticks": summary["median_edge_metrics"]["exit_edge_ticks"],
            }
        )
    return rows


def _timing_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return _flatten_metric_csv_rows(report["timing_analysis"], TIMING_FIELDS, edge=False)


def _edge_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return _flatten_metric_csv_rows(report["edge_analysis"], EDGE_FIELDS, edge=True)


def _flatten_metric_csv_rows(
    analysis: dict[str, Any],
    fields: tuple[str, ...],
    *,
    edge: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in fields:
        rows.append(_metric_csv_row(field, "overall", analysis["overall"][field], edge=edge))
    for group_name in (
        "by_quality_tier",
        "by_validation_mode",
        "by_symbol",
        "by_direction",
        "by_duration_minutes",
        "by_reject_stage",
    ):
        if group_name not in analysis:
            continue
        for cohort, metrics in analysis[group_name].items():
            for field in fields:
                if field in metrics:
                    rows.append(
                        _metric_csv_row(
                            field,
                            f"{group_name}:{cohort}",
                            metrics[field],
                            edge=edge,
                        )
                    )
    return rows


def _metric_csv_row(
    field: str,
    cohort: str,
    summary: dict[str, Any],
    *,
    edge: bool,
) -> dict[str, Any]:
    row = {
        "metric": field,
        "cohort": cohort,
        "count": summary["count"],
        "missing_count": summary["missing_count"],
        "min": summary["min"],
        "mean": summary["mean"],
        "median": summary["median"],
        "p75": summary["p75"],
        "p90": summary["p90"],
        "p95": summary["p95"],
        "p99": summary["p99"],
        "max": summary["max"],
    }
    if edge:
        row["positive_count"] = summary["positive_count"]
        row["positive_rate"] = summary["positive_rate"]
    return row


def _empirical_bucket_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for bucket in report["empirical_bucket_analysis"]["buckets"]:
        rows.append(
            {
                "feature": bucket["feature"],
                "bucket": bucket["bucket_name"],
                "row_count": bucket["row_count"],
                "success_count": bucket["success_count"],
                "success_rate": bucket["success_rate"],
                "median_executable_repricing_delay_ms": bucket["median_executable_repricing_delay_ms"],
                "median_tradable_window_ms": bucket["median_tradable_window_ms"],
                "median_exit_edge_ticks": bucket["median_exit_edge_ticks"],
                "insufficient_sample_warning": bucket["insufficient_sample_warning"],
            }
        )
    return rows


def _readiness_csv_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return list(report["readiness_assessment"]["checks"])


def _top_key(counts: dict[str, int]) -> str:
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _markdown_quality_tier_table(report: dict[str, Any]) -> str:
    lines = ["| Tier | Rows | Success Rate | Median Exec Delay | Median Edge Ticks | Warnings |", "| --- | ---: | ---: | ---: | ---: | --- |"]
    for tier, summary in report["quality_tier_analysis"]["tiers"].items():
        lines.append(
            "| "
            + " | ".join(
                [
                    tier,
                    str(summary["row_count"]),
                    _format_rate(summary["success_rate"]),
                    _format_number(summary["median_executable_repricing_delay_ms"]),
                    _format_number(summary["median_exit_edge_ticks"]),
                    ", ".join(summary["warning_flags"]) or "-",
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _markdown_validation_mode_table(report: dict[str, Any]) -> str:
    lines = ["| Mode | Rows | Success Rate | Median Exec Delay | Median Edge Ticks |", "| --- | ---: | ---: | ---: | ---: |"]
    for mode, summary in report["validation_mode_analysis"]["modes"].items():
        lines.append(
            "| "
            + " | ".join(
                [
                    mode,
                    str(summary["row_count"]),
                    _format_rate(summary["success_rate"]),
                    _format_number(summary["median_timing_metrics"]["executable_repricing_delay_ms"]),
                    _format_number(summary["median_edge_metrics"]["exit_edge_ticks"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _markdown_reject_taxonomy_table(report: dict[str, Any]) -> str:
    lines = ["| Category | Count | Pct Total | Pct Rejected |", "| --- | ---: | ---: | ---: |"]
    for category, summary in report["reject_taxonomy"]["categories"].items():
        lines.append(
            "| "
            + " | ".join(
                [
                    category,
                    str(summary["count"]),
                    _format_rate(summary["percentage_of_total_rows"]),
                    _format_rate(summary["percentage_of_rejected_rows"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _markdown_metric_table(
    metrics: dict[str, dict[str, Any]],
    fields: tuple[str, ...],
) -> str:
    lines = ["| Metric | Count | Missing | Median | P95 | Max |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for field in fields:
        summary = metrics[field]
        lines.append(
            "| "
            + " | ".join(
                [
                    field,
                    str(summary["count"]),
                    str(summary["missing_count"]),
                    _format_number(summary["median"]),
                    _format_number(summary["p95"]),
                    _format_number(summary["max"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _markdown_empirical_bucket_table(report: dict[str, Any]) -> str:
    lines = ["| Feature | Bucket | Rows | Success Rate | Sparse |", "| --- | --- | ---: | ---: | --- |"]
    for bucket in report["empirical_bucket_analysis"]["buckets"][:20]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(bucket["feature"]),
                    str(bucket["bucket_name"]),
                    str(bucket["row_count"]),
                    _format_rate(bucket["success_rate"]),
                    str(bucket["insufficient_sample_warning"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _primary_bucket_markdown_note(report: dict[str, Any]) -> str:
    primary_min_tier = str(report["metadata"]["primary_min_tier"])
    primary_description = "A/B" if primary_min_tier == "B" else "A only"
    return (
        "By default, empirical buckets are computed on primary rows only. "
        f"For `--primary-min-tier {primary_min_tier}`, primary rows are {primary_description}."
    )


def _markdown_cohort_table(report: dict[str, Any]) -> str:
    lines = ["| Cohort | Rows | Success Rate | Median Delay | Median Edge Ticks |", "| --- | ---: | ---: | ---: | ---: |"]
    for cohort, summary in report["cohort_sensitivity"]["cohorts"].items():
        lines.append(
            "| "
            + " | ".join(
                [
                    cohort,
                    str(summary["row_count"]),
                    _format_rate(summary["success_rate"]),
                    _format_number(summary["median_executable_repricing_delay_ms"]),
                    _format_number(summary["median_exit_edge_ticks"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _format_rate(value: Any) -> str:
    if not _is_number(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def _format_number(value: Any) -> str:
    if not _is_number(value):
        return "-"
    return f"{float(value):.4g}"
