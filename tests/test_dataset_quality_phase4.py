from __future__ import annotations

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
