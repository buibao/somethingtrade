import json

from app.backtest.dataset_quality import (
    build_dataset_quality_report,
    write_dataset_quality_report,
)


def _write_jsonl(path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def test_dataset_quality_report_parses_jsonl_and_counts(tmp_path) -> None:
    input_path = tmp_path / "gap_events.jsonl"
    _write_jsonl(
        input_path,
        [
            {
                "detected_ts_ns": 100,
                "symbol": "BTCUSDT",
                "direction": "UP",
                "market_slug": "btc-updown-15m-1",
                "duration_minutes": 15,
                "validation_mode": "tolerant",
                "data_quality_tier": "A",
                "reject_stage": "none",
                "quote_was_fillable": True,
                "mid_repricing_delay_ms": 10.0,
                "executable_repricing_delay_ms": 20.0,
                "tradable_window_ms": 30.0,
                "exit_edge_after_spread": 0.02,
                "exit_edge_ticks": 2.0,
                "spread_ticks_at_detection": 2.0,
                "reported_best_validation_ok_at_detection": True,
                "book_structurally_complete_at_detection": True,
                "book_has_snapshot_at_detection": True,
                "book_complete_at_detection": True,
            },
            {
                "detected_ts_ns": 200,
                "symbol": "ETHUSDT",
                "direction": "DOWN",
                "market_slug": "eth-updown-5m-1",
                "duration_minutes": 5,
                "validation_mode": "diagnostic",
                "data_quality_tier": "D",
                "data_quality_reason": "book_incomplete",
                "reject_stage": "pre_entry",
                "reject_reason": "book_incomplete",
                "quote_was_fillable": False,
                "tick_size_at_detection": 0.01,
                "reported_best_validation_ok_at_detection": False,
                "book_structurally_complete_at_detection": False,
                "book_has_snapshot_at_detection": False,
                "book_complete_at_detection": False,
            },
        ],
    )

    report = build_dataset_quality_report(input_path)

    assert report["total_rows"] == 2
    assert report["included_rows"] == 2
    assert report["symbols"] == {"BTCUSDT": 1, "ETHUSDT": 1}
    assert report["validation_mode_distribution"] == {"tolerant": 1, "diagnostic": 1}
    assert report["data_quality_tier_distribution"] == {"A": 1, "D": 1}
    assert report["outcome"]["success_count"] == 1
    assert report["outcome"]["pre_entry_count"] == 1


def test_dataset_quality_report_computes_median_and_p95(tmp_path) -> None:
    input_path = tmp_path / "gap_events.jsonl"
    _write_jsonl(
        input_path,
        [
            {
                "detected_ts_ns": index,
                "symbol": "BTCUSDT",
                "direction": "UP",
                "validation_mode": "tolerant",
                "data_quality_tier": "A",
                "reject_stage": "none",
                "quote_was_fillable": True,
                "executable_repricing_delay_ms": float(index),
            }
            for index in (1, 2, 3)
        ],
    )

    report = build_dataset_quality_report(input_path)

    summary = report["timing"]["executable_repricing_delay_ms"]
    assert summary["min"] == 1.0
    assert summary["median"] == 2.0
    assert summary["p95"] == 3.0
    assert summary["max"] == 3.0


def test_dataset_quality_report_warns_when_success_count_zero(tmp_path) -> None:
    input_path = tmp_path / "gap_events.jsonl"
    _write_jsonl(
        input_path,
        [
            {
                "detected_ts_ns": 1,
                "symbol": "BTCUSDT",
                "direction": "UP",
                "validation_mode": "tolerant",
                "data_quality_tier": "B",
                "reject_stage": "timeout",
                "reject_reason": "max_observation_lifetime_reached",
                "quote_was_fillable": True,
            }
        ],
    )

    report = build_dataset_quality_report(input_path)

    assert "success_count_zero" in report["warnings"]


def test_dataset_quality_report_warns_when_diagnostic_mode_dominates(tmp_path) -> None:
    input_path = tmp_path / "gap_events.jsonl"
    _write_jsonl(
        input_path,
        [
            {
                "detected_ts_ns": index,
                "symbol": "BTCUSDT",
                "direction": "UP",
                "validation_mode": "diagnostic",
                "data_quality_tier": "C",
                "reject_stage": "pre_entry",
                "quote_was_fillable": False,
            }
            for index in range(3)
        ],
    )

    report = build_dataset_quality_report(input_path)

    assert "diagnostic_mode_majority" in report["warnings"]


def test_dataset_quality_report_writes_json_output(tmp_path) -> None:
    input_path = tmp_path / "gap_events.jsonl"
    output_path = tmp_path / "report.json"
    _write_jsonl(
        input_path,
        [
            {
                "detected_ts_ns": 1,
                "symbol": "BTCUSDT",
                "direction": "UP",
                "validation_mode": "tolerant",
                "data_quality_tier": "A",
                "reject_stage": "none",
                "quote_was_fillable": True,
            }
        ],
    )

    report = build_dataset_quality_report(input_path)
    write_dataset_quality_report(report, output_path)
    decoded = json.loads(output_path.read_text(encoding="utf-8"))

    assert decoded["total_rows"] == 1
