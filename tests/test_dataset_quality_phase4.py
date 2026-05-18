from __future__ import annotations

import csv
import json
from pathlib import Path

from app.backtest.dataset_quality_phase4 import (
    build_phase4_dataset_quality_report,
    render_phase4_markdown_report,
    write_phase4_csv_outputs,
)
from app.main import parse_args, run_dataset_quality_report


def _base_row(
    index: int,
    *,
    tier: str = "A",
    mode: str = "strict",
    reject_stage: str = "none",
    reject_reason: str | None = None,
    symbol: str = "BTCUSDT",
    direction: str = "UP",
    exec_delay: float = 20.0,
    window_ms: float = 100.0,
    edge_ticks: float = 2.0,
    spread_ticks: float = 1.0,
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "market_id": f"market-{index}",
        "market_slug": f"{symbol.lower()}-{direction.lower()}-{index}",
        "token_id": f"token-{index}",
        "direction": direction,
        "duration_minutes": 5,
        "detected_ts_ns": 1_700_000_000_000_000_000 + index * 1_000_000,
        "validation_mode": mode,
        "data_quality_tier": tier,
        "data_quality_reason": "clean" if tier == "A" else "tolerated_one_tick_mismatch",
        "reject_stage": reject_stage,
        "quote_was_fillable": reject_stage == "none",
        "before_best_bid": 0.49,
        "before_best_ask": 0.50,
        "before_best_bid_size": 100.0,
        "before_best_ask_size": 100.0,
        "before_mid": 0.495,
        "after_best_bid": 0.52,
        "after_best_ask": 0.53,
        "after_mid": 0.525,
        "spread_before": 0.01,
        "spread_after": 0.01,
        "entry_ask": 0.50,
        "entry_ask_size": 100.0,
        "executable_exit_bid": 0.52,
        "exit_edge_after_spread": 0.02,
        "estimated_edge_after_spread": 0.02,
        "mid_repricing_delay_ms": 10.0,
        "executable_repricing_delay_ms": exec_delay,
        "tradable_window_ms": window_ms,
        "tick_size_at_detection": 0.01,
        "exit_edge_ticks": edge_ticks,
        "spread_ticks_at_detection": spread_ticks,
        "reported_best_validation_ok_at_detection": True,
        "book_structurally_complete_at_detection": True,
        "book_has_snapshot_at_detection": True,
        "book_complete_at_detection": True,
        "market_quote_complete_rate_at_detection": 1.0,
        "token_quote_complete_rate_at_detection": 1.0,
        "stale_source": "none",
        "binance_quote_age_ms": 5.0,
        "polymarket_quote_age_ms": 8.0,
        "binance_move_pct": 0.08,
    }
    if reject_reason is not None:
        row["reject_reason"] = reject_reason
    return row


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_quote_stale_runtime_consistency_inputs(
    tmp_path: Path,
) -> tuple[Path, Path]:
    events_path = tmp_path / "events.jsonl"
    runtime_path = tmp_path / "runtime_summary.jsonl"
    total = 2_457
    gap_start_ns = 1_700_000_000_000_000_000
    gap_duration_ns = 1_068_000_000_000
    runtime_duration_ns = 1_000_000_000_000
    rows: list[dict[str, object]] = []
    for index in range(total):
        row = _base_row(index)
        row["detected_ts_ns"] = gap_start_ns + round(
            index * gap_duration_ns / (total - 1)
        )
        if index < 25:
            row["stale_source"] = "binance"
        elif index < 169:
            row["stale_source"] = "polymarket"
        else:
            row["stale_source"] = "unknown"
        if index < 166:
            row["reject_stage"] = "pre_entry"
            row["reject_reason"] = "quote_stale"
            row["quote_was_fillable"] = False
        rows.append(row)
    runtime_rows: list[dict[str, object]] = []
    for index in range(25):
        runtime_rows.append(
            {
                "event_type": "runtime_summary",
                "generated_ts_ns": gap_start_ns
                + round(index * runtime_duration_ns / 24),
                "no_event_warnings": [],
            }
        )
    _write_rows(events_path, rows)
    _write_rows(runtime_path, runtime_rows)
    return events_path, runtime_path


def test_phase4_reads_jsonl_with_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [_base_row(1), _base_row(2)]
    path.write_text(json.dumps(rows[0]) + "\n\n" + json.dumps(rows[1]) + "\n", encoding="utf-8")

    report = build_phase4_dataset_quality_report(path)

    assert report["input_audit"]["blank_lines"] == 1
    assert report["input_audit"]["parsed_json_rows"] == 2


def test_phase4_reports_malformed_json_lines(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps(_base_row(1)) + "\n{bad json\n", encoding="utf-8")

    report = build_phase4_dataset_quality_report(path)

    assert report["input_audit"]["parsed_json_rows"] == 1
    assert report["input_audit"]["malformed_json_lines"] == 1
    assert report["input_audit"]["malformed_json_line_numbers_sample"] == [2]


def test_phase4_schema_audit_detects_missing_fields(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    row = _base_row(1)
    row.pop("token_id")
    _write_rows(path, [row])

    report = build_phase4_dataset_quality_report(path)

    token_audit = report["schema_audit"]["fields"]["token_id"]
    assert token_audit["present_count"] == 0
    assert token_audit["missing_count"] == 1


def test_phase4_quality_tier_analysis_counts_A_B_C_D(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1, tier=tier) for tier in ("A", "B", "C", "D")])

    report = build_phase4_dataset_quality_report(path, include_diagnostic=True)
    tiers = report["quality_tier_analysis"]["tiers"]

    assert {tier: tiers[tier]["row_count"] for tier in ("A", "B", "C", "D")} == {
        "A": 1,
        "B": 1,
        "C": 1,
        "D": 1,
    }


def test_phase4_primary_rows_default_to_A_B(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1, tier=tier) for tier in ("A", "B", "C", "D")])

    report = build_phase4_dataset_quality_report(path)

    assert report["dataset_health"]["primary_rows"] == 2
    assert report["empirical_bucket_analysis"]["primary_row_count"] == 2


def test_phase4_reject_taxonomy_groups_known_reasons(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [
        _base_row(1, tier="D", reject_stage="pre_entry", reject_reason="book_incomplete"),
        _base_row(2, tier="D", reject_stage="pre_entry", reject_reason="quote_stale"),
        _base_row(3, tier="C", reject_stage="window", reject_reason="insufficient_best_ask_size"),
    ]
    _write_rows(path, rows)

    report = build_phase4_dataset_quality_report(path)
    categories = report["reject_taxonomy"]["categories"]

    assert categories["book_quality"]["count"] == 1
    assert categories["staleness"]["count"] == 1
    assert categories["liquidity"]["count"] == 1


def test_phase4_validation_mode_analysis_compares_strict_tolerant(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(
        path,
        [
            _base_row(1, mode="strict", tier="A"),
            _base_row(2, mode="tolerant", tier="B"),
        ],
    )

    report = build_phase4_dataset_quality_report(path)
    analysis = report["validation_mode_analysis"]

    assert analysis["modes"]["strict"]["row_count"] == 1
    assert analysis["modes"]["tolerant"]["row_count"] == 1
    assert "strict_vs_tolerant" in analysis["comparisons"]


def test_phase4_timing_summary_percentiles(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(index, exec_delay=float(index)) for index in range(1, 6)])

    report = build_phase4_dataset_quality_report(path)
    summary = report["timing_analysis"]["overall"]["executable_repricing_delay_ms"]

    assert summary["count"] == 5
    assert summary["median"] == 3.0
    assert summary["p95"] == 5.0


def test_phase4_edge_summary_positive_rate(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [
        _base_row(1, edge_ticks=1.0),
        _base_row(2, edge_ticks=0.0),
        _base_row(3, edge_ticks=-1.0),
    ]
    rows[0]["exit_edge_after_spread"] = 0.01
    rows[1]["exit_edge_after_spread"] = 0.0
    rows[2]["exit_edge_after_spread"] = -0.01
    _write_rows(path, rows)

    report = build_phase4_dataset_quality_report(path)
    summary = report["edge_analysis"]["overall"]["exit_edge_after_spread"]

    assert summary["positive_count"] == 1
    assert summary["positive_rate"] == 1 / 3


def test_phase4_empirical_buckets_do_not_predict(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1, tier="A")])

    report = build_phase4_dataset_quality_report(path)
    buckets = report["empirical_bucket_analysis"]

    assert buckets["is_prediction"] is False
    assert buckets["model_training_added"] is False
    assert buckets["trading_signal_added"] is False


def test_phase4_cohort_sensitivity_A_vs_AB_vs_all(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(
        path,
        [
            _base_row(1, tier="A"),
            _base_row(2, tier="B"),
            _base_row(3, tier="D", reject_stage="pre_entry", reject_reason="book_incomplete"),
        ],
    )

    report = build_phase4_dataset_quality_report(path)
    cohorts = report["cohort_sensitivity"]["cohorts"]

    assert cohorts["A only"]["row_count"] == 1
    assert cohorts["A/B"]["row_count"] == 2
    assert cohorts["all rows"]["row_count"] == 3
    assert "primary_vs_all_success_rate_delta" in report["cohort_sensitivity"]


def test_phase4_readiness_not_ready_on_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")

    report = build_phase4_dataset_quality_report(path)

    assert report["readiness_assessment"]["classification"] == "NOT_READY"


def test_phase4_readiness_needs_more_data_when_low_primary_rows(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1)])

    report = build_phase4_dataset_quality_report(path)

    assert report["readiness_assessment"]["classification"] == "NEEDS_MORE_DATA"


def test_phase4_readiness_needs_cleaning_when_tier_D_high(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [_base_row(index) for index in range(1000)]
    rows.extend(
        _base_row(
            10_000 + index,
            tier="D",
            mode="diagnostic",
            reject_stage="pre_entry",
            reject_reason="book_incomplete",
        )
        for index in range(300)
    )
    _write_rows(path, rows)

    report = build_phase4_dataset_quality_report(path)

    assert report["dataset_health"]["primary_rows"] == 1000
    assert report["readiness_assessment"]["classification"] == "NEEDS_MORE_CLEANING"


def test_phase4_markdown_contains_non_goals(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1)])

    report = build_phase4_dataset_quality_report(path)
    markdown = render_phase4_markdown_report(report)

    assert "# Phase 4.0 Dataset Quality Report & Empirical Calibration" in markdown
    assert "## 16. Non-Goals Confirmed" in markdown
    assert "no model prediction was added" in markdown
    assert "no wallet copy trading or on-chain wallet logic was added" in markdown


def test_phase4_staleness_unknown_when_quote_age_fields_missing(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    row = _base_row(1)
    row.pop("stale_source")
    row.pop("binance_quote_age_ms")
    row.pop("polymarket_quote_age_ms")
    _write_rows(path, [row])

    report = build_phase4_dataset_quality_report(path)
    stale = report["stale_feed_analysis"]

    assert stale["staleness_status"] == "unknown_missing_quote_age_fields"
    assert stale["quote_stale_rate"] is None
    assert "quote_age_fields_missing" in report["warnings"]


def test_phase4_readiness_warns_when_quote_age_fields_missing(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    row = _base_row(1)
    row.pop("stale_source")
    row.pop("binance_quote_age_ms")
    row.pop("polymarket_quote_age_ms")
    _write_rows(path, [row])

    report = build_phase4_dataset_quality_report(path)
    checks = {
        check["check_name"]: check
        for check in report["readiness_assessment"]["checks"]
    }

    assert checks["quote_stale_rate"]["status"] == "WARN"
    assert checks["quote_age_fields_missing"]["status"] == "WARN"
    assert "quote_age_fields_missing" in report["readiness_assessment"]["non_blocking_warnings"]


def test_phase4_quote_stale_rate_does_not_count_unknown_stale_source_as_stale(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    rows: list[dict[str, object]] = []
    total = 2_457
    for index in range(total):
        row = _base_row(index)
        if index < 25:
            row["stale_source"] = "binance"
        elif index < 169:
            row["stale_source"] = "polymarket"
        else:
            row["stale_source"] = "unknown"
        if index < 166:
            row["reject_stage"] = "window"
            row["reject_reason"] = "quote_stale"
            row["quote_was_fillable"] = False
        rows.append(row)
    _write_rows(path, rows)

    report = build_phase4_dataset_quality_report(path)
    stale = report["stale_feed_analysis"]
    checks = {check["check_name"]: check for check in report["readiness_assessment"]["checks"]}
    markdown = render_phase4_markdown_report(report)

    assert stale["stale_source_distribution"] == {
        "binance": 25,
        "polymarket": 144,
        "unknown": 2_288,
    }
    assert stale["quote_stale_rate_basis"] == "reject_reason_quote_stale"
    assert stale["quote_stale_count"] == 166
    assert abs(stale["quote_stale_rate"] - 166 / total) < 1e-12
    assert stale["quote_stale_rate"] != 1.0
    assert abs(stale["binance_stale_rate"] - 25 / total) < 1e-12
    assert abs(stale["polymarket_stale_rate"] - 144 / total) < 1e-12
    assert stale["both_stale_rate"] == 0.0
    assert stale["unknown_quote_age_or_not_stale_source_count"] == 2_288
    assert checks["quote_stale_rate"]["value"] == stale["quote_stale_rate"]
    assert checks["quote_stale_rate"]["status"] == "PASS"
    assert "`unknown` stale_source is reported separately and is not counted as stale" in markdown


def test_dataset_quality_report_outputs_corrected_quote_stale_and_runtime_coverage(
    tmp_path: Path,
) -> None:
    events_path, runtime_path = _write_quote_stale_runtime_consistency_inputs(tmp_path)
    output_path = tmp_path / "dataset_quality_latest.json"
    markdown_path = tmp_path / "dataset_quality_latest.md"
    csv_dir = tmp_path / "dataset_quality_latest_csv"

    args = parse_args(
        [
            "dataset-quality-report",
            "--input",
            str(events_path),
            "--output",
            str(output_path),
            "--markdown-output",
            str(markdown_path),
            "--csv-dir",
            str(csv_dir),
            "--runtime-summary-jsonl",
            str(runtime_path),
        ]
    )
    run_dataset_quality_report(args)

    report = json.loads(output_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    readiness_rows = {
        row["check_name"]: row for row in _read_csv_dicts(csv_dir / "readiness_checks.csv")
    }
    runtime_coverage_row = _read_csv_dicts(csv_dir / "runtime_coverage.csv")[0]
    expected_quote_stale_rate = 166 / 2_457
    expected_runtime_coverage_ratio = 1.068

    stale = report["stale_feed_analysis"]
    assert abs(stale["quote_stale_rate"] - expected_quote_stale_rate) < 1e-12
    assert stale["quote_stale_rate_basis"] == "reject_reason_quote_stale"
    assert stale["stale_source_distribution"] == {
        "binance": 25,
        "polymarket": 144,
        "unknown": 2_288,
    }
    assert stale["unknown_quote_age_or_not_stale_source_count"] == 2_288
    assert "Quote stale rate: 6.76%" in markdown
    assert "Quote stale rate: 100.00%" not in markdown
    assert "basis: reject_reason_quote_stale" in markdown
    assert "Unknown/not-stale-source count: 2288" in markdown

    quote_stale_check = readiness_rows["quote_stale_rate"]
    assert quote_stale_check["status"] == "PASS"
    assert abs(float(quote_stale_check["value"]) - expected_quote_stale_rate) < 1e-12

    coverage = report["runtime_coverage_analysis"]
    assert coverage["status"] == "analyzed"
    assert coverage["runtime_summary_rows"] == 25
    assert abs(coverage["gap_event_time_coverage_ratio"] - expected_runtime_coverage_ratio) < 1e-12
    assert "Runtime coverage status: analyzed" in markdown
    assert "Runtime summary rows: 25" in markdown
    assert "Gap-event/runtime coverage ratio: 106.80%" in markdown
    assert runtime_coverage_row["status"] == "analyzed"
    assert runtime_coverage_row["runtime_summary_rows"] == "25"
    assert abs(
        float(runtime_coverage_row["gap_event_time_coverage_ratio"])
        - expected_runtime_coverage_ratio
    ) < 1e-12

    runtime_check = readiness_rows["runtime_gap_event_coverage"]
    assert runtime_check["status"] == "PASS"
    assert abs(float(runtime_check["value"]) - expected_runtime_coverage_ratio) < 1e-12


def test_dataset_quality_json_markdown_and_csv_consistency(
    tmp_path: Path,
) -> None:
    events_path, runtime_path = _write_quote_stale_runtime_consistency_inputs(tmp_path)
    output_path = tmp_path / "report.json"
    markdown_path = tmp_path / "report.md"
    csv_dir = tmp_path / "csv"

    args = parse_args(
        [
            "dataset-quality-report",
            "--input",
            str(events_path),
            "--output",
            str(output_path),
            "--markdown-output",
            str(markdown_path),
            "--csv-dir",
            str(csv_dir),
            "--runtime-summary-jsonl",
            str(runtime_path),
        ]
    )
    run_dataset_quality_report(args)

    report = json.loads(output_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    readiness_rows = {
        row["check_name"]: row for row in _read_csv_dicts(csv_dir / "readiness_checks.csv")
    }
    runtime_coverage_row = _read_csv_dicts(csv_dir / "runtime_coverage.csv")[0]
    report_checks = {
        check["check_name"]: check for check in report["readiness_assessment"]["checks"]
    }

    quote_stale_rate = report["stale_feed_analysis"]["quote_stale_rate"]
    assert quote_stale_rate is not None
    assert readiness_rows["quote_stale_rate"]["status"] == report_checks["quote_stale_rate"]["status"]
    assert abs(float(readiness_rows["quote_stale_rate"]["value"]) - quote_stale_rate) < 1e-12
    assert f"Quote stale rate: {quote_stale_rate * 100:.2f}%" in markdown

    coverage = report["runtime_coverage_analysis"]
    coverage_ratio = coverage["gap_event_time_coverage_ratio"]
    assert coverage_ratio is not None
    assert readiness_rows["runtime_gap_event_coverage"]["status"] == (
        report_checks["runtime_gap_event_coverage"]["status"]
    )
    assert abs(
        float(readiness_rows["runtime_gap_event_coverage"]["value"]) - coverage_ratio
    ) < 1e-12
    assert runtime_coverage_row["status"] == coverage["status"]
    assert int(runtime_coverage_row["runtime_summary_rows"]) == coverage["runtime_summary_rows"]
    assert abs(
        float(runtime_coverage_row["gap_event_time_coverage_ratio"]) - coverage_ratio
    ) < 1e-12
    assert f"Runtime summary rows: {coverage['runtime_summary_rows']}" in markdown
    assert f"Gap-event/runtime coverage ratio: {coverage_ratio * 100:.2f}%" in markdown


def test_phase4_markdown_says_empirical_buckets_use_primary_rows(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1, tier="A"), _base_row(2, tier="B"), _base_row(3, tier="C")])

    report = build_phase4_dataset_quality_report(path, primary_min_tier="B")
    markdown = render_phase4_markdown_report(report)

    assert "empirical buckets are computed on primary rows only" in markdown
    assert "For `--primary-min-tier B`, primary rows are A/B" in markdown


def test_phase4_warns_when_tolerated_rows_need_mismatch_samples(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1, tier="B", mode="tolerant")])

    report = build_phase4_dataset_quality_report(path)
    tick = report["tick_calibration_analysis"]

    assert tick["tolerated_mismatch_row_count"] == 1
    assert tick["mismatch_sample_status"] == "skipped_missing_mismatch_sample_input"
    assert "tolerated_mismatch_rows_without_mismatch_samples" in tick["warning_flags"]
    assert "tolerated_mismatch_rows_without_mismatch_samples" in report["warnings"]


def test_report_warns_when_gap_event_coverage_shorter_than_runtime(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    runtime_path = tmp_path / "runtime_summary.jsonl"
    rows = [_base_row(1), _base_row(2)]
    rows[0]["detected_ts_ns"] = 1_000_000_000
    rows[1]["detected_ts_ns"] = 61_000_000_000
    _write_rows(events_path, rows)
    _write_rows(
        runtime_path,
        [
            {
                "event_type": "runtime_summary",
                "generated_ts_ns": 0,
                "no_event_warnings": [],
            },
            {
                "event_type": "runtime_summary",
                "generated_ts_ns": 7_200_000_000_000,
                "no_event_warnings": ["no_signal_enabled_markets_while_binance_moves_continue"],
            },
        ],
    )

    report = build_phase4_dataset_quality_report(
        events_path,
        runtime_summary_jsonl_path=runtime_path,
    )

    coverage = report["runtime_coverage_analysis"]
    assert coverage["status"] == "analyzed"
    assert coverage["gap_event_time_coverage_ratio"] < 0.50
    assert "gap_event_coverage_shorter_than_runtime" in coverage["warning_flags"]
    assert "gap_event_coverage_shorter_than_runtime" in report["warnings"]
    checks = {check["check_name"]: check for check in report["readiness_assessment"]["checks"]}
    assert checks["runtime_gap_event_coverage"]["status"] == "WARN"


def test_phase4_csv_outputs_are_written(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    csv_dir = tmp_path / "csv"
    _write_rows(path, [_base_row(1)])

    report = build_phase4_dataset_quality_report(path, csv_dir=csv_dir)
    write_phase4_csv_outputs(report, csv_dir)

    expected = {
        "cohort_summary.csv",
        "reject_taxonomy.csv",
        "quality_tier_summary.csv",
        "validation_mode_summary.csv",
        "timing_summary.csv",
        "edge_summary.csv",
        "empirical_buckets.csv",
        "readiness_checks.csv",
        "runtime_coverage.csv",
    }
    assert expected <= {path.name for path in csv_dir.iterdir()}
    for filename in expected:
        assert (csv_dir / filename).read_text(encoding="utf-8").strip()


def test_dataset_quality_report_cli_writes_json_markdown_csv(tmp_path: Path) -> None:
    input_path = tmp_path / "events.jsonl"
    output_path = tmp_path / "report.json"
    markdown_path = tmp_path / "report.md"
    csv_dir = tmp_path / "csv"
    _write_rows(input_path, [_base_row(1)])

    args = parse_args(
        [
            "dataset-quality-report",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--markdown-output",
            str(markdown_path),
            "--csv-dir",
            str(csv_dir),
            "--primary-min-tier",
            "B",
        ]
    )
    run_dataset_quality_report(args)

    decoded = json.loads(output_path.read_text(encoding="utf-8"))
    assert "metadata" in decoded
    assert "readiness_assessment" in decoded
    assert markdown_path.exists()
    assert (csv_dir / "cohort_summary.csv").exists()


def test_no_realtime_path_llm_or_execution_added(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    _write_rows(path, [_base_row(1)])

    report = build_phase4_dataset_quality_report(path)

    assert report["metadata"]["realtime_path_modified"] is False
    assert report["metadata"]["model_prediction_added"] is False
    assert report["metadata"]["trading_signal_added"] is False
    assert report["metadata"]["live_execution_added"] is False
