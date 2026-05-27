from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from typing import Any
import zipfile

import pytest

from app.research.reference_feed_benchmark import (
    REFERENCE_SOURCES,
    ReferenceValidationResult,
    analyze_reference_gap_distribution,
    reference_timestamp_monotonic_violations,
    validate_reference_event_schema,
)
from app.research.time_protocol_benchmark import (
    HYBRID_BUDGETS_MS,
    PHASE42D_BUNDLE,
    PHASE42D_REQUIRED_BUNDLE_FILES,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    build_clock_sanity_report,
    build_exchange_time_label,
    build_hybrid_label,
    build_phase42d_report,
    build_receive_time_label,
    cleanup_phase42d_artifacts,
    create_phase42d_bundle,
    generate_time_protocol_rows,
    phase42d_bundle_missing_files,
    receive_lag_ms,
    run_phase42d_analysis,
    run_phase42d_leakage_check,
    source_exchange_ts_ms,
    validate_gitignore_rules,
    validate_phase42d_report_schema,
    validate_timestamp_schema,
    write_phase42d_artifacts,
)


ROOT = Path(__file__).resolve().parents[1]


def _wall(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample(
    local_ms: int,
    exchange_ms: int,
    *,
    lag_ms: int = 5,
    last_update_id: int = 100,
) -> dict[str, Any]:
    best_bid = 100.0 + last_update_id / 10_000.0
    best_ask = best_bid + 1.0
    return {
        "schema_version": "phase_4_1_clean_orderbook_v1",
        "symbol": "BTCUSDT",
        "source": "binance_ws",
        "generation_id": 42,
        "state_version": last_update_id,
        "snapshot_version": last_update_id,
        "last_update_id": last_update_id,
        "local_recv_monotonic_ns": local_ms * 1_000_000,
        "local_recv_wall_ts": _wall(exchange_ms + lag_ms),
        "exchange_event_ts": exchange_ms,
        "best_bid": f"{best_bid:.8f}",
        "best_ask": f"{best_ask:.8f}",
        "bids": [_level(best_bid - index, 10.0) for index in range(20)],
        "asks": [_level(best_ask + index, 5.0) for index in range(20)],
        "quality": {"is_valid": True, "errors": [], "warnings": []},
        "lifecycle": {
            "snapshot_ready": True,
            "ready_to_emit": True,
            "sequence_continuous": True,
        },
    }


def _ref(
    source: str,
    local_ms: int,
    exchange_ms: int | None,
    *,
    event_id: int = 1,
    price: float = 101.0,
    lag_ms: int = 5,
    include_t: bool = True,
    include_e: bool = True,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": "BTCUSDT",
        "local_recv_monotonic_ns": local_ms * 1_000_000,
        "local_recv_wall_ts": _wall((exchange_ms or 0) + lag_ms),
        "quality": {"valid": True, "errors": []},
    }
    if include_e:
        base["exchange_event_ts"] = exchange_ms
    if source == "bookTicker_mid":
        return {
            **base,
            "schema_version": "bookticker_reference_v1",
            "source": "binance_ws_bookTicker",
            "update_id": event_id,
            "best_bid": price - 0.5,
            "best_bid_qty": 1.0,
            "best_ask": price + 0.5,
            "best_ask_qty": 1.0,
            "mid_price": price,
            "spread": 1.0,
            "spread_bps": 1.0 / price * 10_000,
        }
    if source == "trade_price":
        row = {
            **base,
            "schema_version": "trade_reference_v1",
            "source": "binance_ws_trade",
            "trade_id": event_id,
            "price": price,
            "quantity": 0.01,
            "is_buyer_market_maker": False,
        }
        if include_t:
            row["trade_time"] = exchange_ms
        return row
    if source == "aggTrade_price":
        row = {
            **base,
            "schema_version": "aggtrade_reference_v1",
            "source": "binance_ws_aggTrade",
            "aggregate_trade_id": event_id,
            "first_trade_id": event_id,
            "last_trade_id": event_id + 1,
            "price": price,
            "quantity": 0.02,
            "is_buyer_market_maker": False,
        }
        if include_t:
            row["trade_time"] = exchange_ms
        return row
    raise AssertionError(source)


def _validation(source: str, rows: list[dict[str, Any]], *, file_exists: bool = True) -> ReferenceValidationResult:
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for row in rows:
        errors = validate_reference_event_schema(row, source)
        if errors:
            invalid.append({"reason": errors})
        else:
            valid.append(row)
    return ReferenceValidationResult(
        reference_source=source,
        file_exists=file_exists,
        reference_event_count=len(rows),
        valid_reference_event_count=len(valid),
        invalid_reference_event_count=len(invalid),
        valid_events=valid,
        invalid_events=invalid,
        timestamp_monotonic_violations=reference_timestamp_monotonic_violations(valid),
        quality=analyze_reference_gap_distribution(valid),
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_required_gitignore(root: Path) -> None:
    root.joinpath(".gitignore").write_text(
        "\n".join(
            [
                "*.jsonl",
                "data/dataset/",
                "data/debug/",
                "data/cache/",
                "data/logs/",
                "data/reports/",
                "logs/",
                "reports/",
                "debug/",
                "cache/",
                "*.zip",
                "*.log",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _analysis_fixture(tmp_path: Path, *, feature_lag_ms: int = 5) -> dict[str, Any]:
    _write_required_gitignore(tmp_path)
    samples = [
        _sample(index * 100, index * 100, lag_ms=feature_lag_ms, last_update_id=100 + index)
        for index in range(6)
    ]
    refs = [
        _ref("trade_price", index * 100 + 10, index * 100, event_id=200 + index, lag_ms=5)
        for index in range(8)
    ]
    _write_jsonl(tmp_path / "data/dataset/orderbook_clean_samples.jsonl", samples)
    _write_jsonl(tmp_path / "data/dataset/trade_reference_events.jsonl", refs)
    _write_jsonl(tmp_path / "data/dataset/aggtrade_reference_events.jsonl", [])
    _write_jsonl(tmp_path / "data/dataset/bookticker_reference_quotes.jsonl", [])
    return {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "skip_capture": True,
        "cleanup_performed": True,
        "duration_sec": 1800.0,
        "requested_streams": ["btcusdt@depth@100ms", "btcusdt@bookTicker", "btcusdt@trade", "btcusdt@aggTrade"],
        "reference_event_counts": {"trade_price": len(refs)},
        "depth_clean_sample_count": len(samples),
    }


def test_phase42d_cleanup_deletes_generated_artifacts_only(tmp_path: Path) -> None:
    generated = tmp_path / "data/dataset/old.jsonl"
    generated.parent.mkdir(parents=True)
    generated.write_text("old\n", encoding="utf-8")
    old_zip = tmp_path / "phase_4_2c_reference_feed_benchmark_bundle.zip"
    old_zip.write_text("zip\n", encoding="utf-8")
    source = tmp_path / "scripts/keep.py"
    source.parent.mkdir()
    source.write_text("print('keep')\n", encoding="utf-8")

    report = cleanup_phase42d_artifacts(tmp_path)

    assert report["cleanup_performed"] is True
    assert not generated.exists()
    assert not old_zip.exists()
    assert source.exists()
    assert (tmp_path / "data/debug/phase_4_2d_artifact_cleanup.json").exists()


def test_phase42d_gitignore_requires_jsonl_and_heavy_artifacts(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    assert validate_gitignore_rules(tmp_path)["passed"] is True

    tmp_path.joinpath(".gitignore").write_text("*.pyc\n", encoding="utf-8")
    result = validate_gitignore_rules(tmp_path)
    assert result["passed"] is False
    assert "*.jsonl" in result["missing_patterns"]
    assert "data/dataset/" in result["missing_patterns"]


def test_phase42d_final_run_requires_fresh_30_min_capture(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42d_time_protocol_benchmark.py"),
            "--skip-pytest",
            "--duration-sec",
            "1",
            "--root",
            str(tmp_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode != 0
    self_check = json.loads((tmp_path / "data/reports/phase42d_self_check.json").read_text())
    assert self_check["passed"] is False
    assert (tmp_path / "data/debug/phase_4_2d_typecheck_report.txt").exists()


def test_phase42d_typecheck_report_blocks_pass_on_failure(tmp_path: Path) -> None:
    capture = _analysis_fixture(tmp_path)
    report = run_phase42d_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        typecheck_passed=False,
        typecheck_summary="failed with pyright",
        fresh_capture_required=False,
    )
    write_phase42d_artifacts(report, root=tmp_path, pytest_output="pytest skipped\n")

    self_check = json.loads((tmp_path / "data/reports/phase42d_self_check.json").read_text())
    assert self_check["passed"] is False
    assert "TYPECHECK_FAILURE" in report["failure_classifications"]


def test_timestamp_schema_source_support_and_preferred_fields() -> None:
    samples = [_sample(0, 0)]
    schema = validate_timestamp_schema(
        samples,
        {
            "depth_mid": [{"exchange_event_ts": 100, "local_recv_monotonic_ns": 1}],
            "bookTicker_mid": [_ref("bookTicker_mid", 100, None, include_e=False)],
            "trade_price": [_ref("trade_price", 100, 100, include_t=True, include_e=True)],
            "aggTrade_price": [_ref("aggTrade_price", 100, 100, include_t=True, include_e=True)],
        },
    )

    sources = schema["sources"]
    assert sources["depth_mid"]["exchange_time_supported"] is True
    assert sources["bookTicker_mid"]["exchange_time_supported"] is False
    assert sources["bookTicker_mid"]["unsupported_reason"] == "missing_exchange_timestamp"
    assert sources["trade_price"]["exchange_timestamp_field_used"] == "T"
    assert sources["aggTrade_price"]["exchange_timestamp_field_used"] == "T"


def test_trade_falls_back_to_e_only_when_policy_allows() -> None:
    row = _ref("trade_price", 100, 100, include_t=False, include_e=True)
    assert source_exchange_ts_ms(row, "trade_price", allow_event_time_fallback=True) == ("E", 100.0)
    assert source_exchange_ts_ms(row, "trade_price", allow_event_time_fallback=False) is None


def test_depth_exchange_time_unsupported_when_feature_e_missing() -> None:
    sample = _sample(0, 0)
    sample.pop("exchange_event_ts")
    schema = validate_timestamp_schema([sample], {"depth_mid": [{"exchange_event_ts": 100}]})
    assert schema["sources"]["depth_mid"]["exchange_time_supported"] is False
    assert schema["sources"]["depth_mid"]["unsupported_reason"] == "missing_feature_exchange_timestamp"


def test_receive_and_exchange_future_selection_are_separate() -> None:
    sample = _sample(0, 0)
    refs = [
        _ref("trade_price", 500, 90, event_id=1, price=101),
        _ref("trade_price", 1000, 100, event_id=2, price=102),
        _ref("trade_price", 100, 500, event_id=3, price=103),
    ]
    receive = build_receive_time_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=sorted(refs, key=lambda row: int(row["local_recv_monotonic_ns"])),
        reference_timestamps_ns=sorted(int(row["local_recv_monotonic_ns"]) for row in refs),
    )
    exchange_sorted = sorted(refs, key=lambda row: float(row["trade_time"]))
    exchange = build_exchange_time_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=exchange_sorted,
        reference_exchange_timestamps_ms=[float(row["trade_time"]) for row in exchange_sorted],
        exchange_time_supported=True,
        unsupported_reason="",
    )

    assert receive["future_reference_event_id"] == 3
    assert exchange["future_reference_event_id"] == 2
    assert exchange["selection_time_basis"] == "exchange_ts"


def test_100ms_gap_boundary_remains_hard() -> None:
    sample = _sample(0, 0)
    equal = build_exchange_time_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=[_ref("trade_price", 200, 200)],
        reference_exchange_timestamps_ms=[200.0],
        exchange_time_supported=True,
        unsupported_reason="",
    )
    above = build_exchange_time_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=[_ref("trade_price", 201, 201)],
        reference_exchange_timestamps_ms=[201.0],
        exchange_time_supported=True,
        unsupported_reason="",
    )
    assert REQUIRED_100MS_MAX_FUTURE_GAP_MS == 100
    assert equal["valid"] is True
    assert equal["exchange_future_gap_ms"] == 100
    assert above["valid"] is False
    assert above["invalid_reason"] == "FUTURE_REFERENCE_GAP_TOO_LARGE"


def test_hybrid_lag_reorder_and_future_lag_telemetry_rules() -> None:
    good_exchange = {
        "reference_source": "trade_price",
        "eligible": True,
        "valid": True,
        "feature_receive_lag_ms": 10.0,
        "future_receive_lag_ms": 10_000.0,
        "feature_local_recv_monotonic_ns": 100,
        "future_reference_local_recv_monotonic_ns": 200,
    }
    assert build_hybrid_label(exchange_label=good_exchange, feature_lag_budget_ms=25)["valid"] is True

    high_feature_lag = {**good_exchange, "feature_receive_lag_ms": 51.0}
    failed = build_hybrid_label(exchange_label=high_feature_lag, feature_lag_budget_ms=50)
    assert failed["valid"] is False
    assert failed["invalid_reason"] == "FEATURE_RECEIVE_LAG_TOO_HIGH"

    reordered = {**good_exchange, "future_reference_local_recv_monotonic_ns": 100}
    failed = build_hybrid_label(exchange_label=reordered, feature_lag_budget_ms=25)
    assert failed["invalid_reason"] == "CROSS_STREAM_RECEIVE_REORDER"

    bad_clock = {**good_exchange, "feature_receive_lag_ms": -6.0}
    failed = build_hybrid_label(exchange_label=bad_clock, feature_lag_budget_ms=25)
    assert failed["invalid_reason"] == "CLOCK_SANITY_VIOLATION"


def test_future_receive_lag_is_telemetry_only_in_metrics() -> None:
    samples = [_sample(0, 0, lag_ms=5)]
    refs = [_ref("trade_price", 110, 100, lag_ms=10_000)]
    schema = validate_timestamp_schema(samples, {"trade_price": refs})
    rows = generate_time_protocol_rows(samples, {"trade_price": refs}, schema)
    hybrid = rows[0]["protocol_labels"]["trade_price"]["hybrid_25ms"]
    assert hybrid["valid"] is True
    assert hybrid["future_receive_lag_ms"] == pytest.approx(10_000)
    assert hybrid["future_receive_lag_hard_gate_used"] is False

    report = build_phase42d_report(
        symbol="BTCUSDT",
        clean_samples=samples,
        validations={source: _validation(source, refs if source == "trade_price" else []) for source in REFERENCE_SOURCES},
        rows=rows,
        timestamp_schema=schema,
        leakage_result=run_phase42d_leakage_check(rows),
        clock_sanity_report=build_clock_sanity_report(rows),
        capture={"duration_sec": 1800, "fresh_capture_performed": True, "cleanup_performed": True},
        cleanup_report={"cleanup_performed": True},
        gitignore_validation={"passed": True},
    )
    metrics = report["sources"]["trade_price"]["hybrid"]["hybrid_25ms"]
    assert metrics["future_receive_lag_p95_ms"] == pytest.approx(10_000)
    assert metrics["valid_rate_eligible_rows"] == pytest.approx(1.0)


def test_report_decision_requires_hybrid_not_exchange_alone(tmp_path: Path) -> None:
    capture = _analysis_fixture(tmp_path, feature_lag_ms=500)
    report = run_phase42d_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )

    assert report["exchange_time_coverage_status"] == "pass"
    assert report["hybrid_low_latency_status"] == "fail"
    assert report["low_latency_ready"] is False
    assert report["selected_protocol_candidate"] is None
    assert "exchange_time_market_coverage_passed_but_hybrid_live_observability_failed" in report["warning_reasons"]
    assert "phase5_ready" not in report


def test_low_latency_ready_requires_exchange_hybrid_leakage_and_clock(tmp_path: Path) -> None:
    capture = _analysis_fixture(tmp_path, feature_lag_ms=5)
    report = run_phase42d_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )

    assert report["exchange_time_coverage_status"] == "pass"
    assert report["hybrid_low_latency_status"] == "pass"
    assert report["low_latency_ready"] is True
    assert report["selected_protocol_candidate"]["protocol"] == "hybrid_low_latency_protocol"
    assert report["selected_protocol_candidate"]["budget_ms"] in HYBRID_BUDGETS_MS


def test_report_schema_artifacts_and_failure_investigation(tmp_path: Path) -> None:
    capture = _analysis_fixture(tmp_path, feature_lag_ms=500)
    report = run_phase42d_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    write_phase42d_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")

    assert not validate_phase42d_report_schema(report)
    assert (tmp_path / "data/reports/phase_4_2d_time_protocol_benchmark_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2d_time_protocol_benchmark_report.md").exists()
    assert (tmp_path / "data/reports/phase42d_self_check.json").exists()
    assert (tmp_path / "data/debug/phase_4_2d_protocol_summary.json").exists()
    assert (tmp_path / "data/debug/phase_4_2d_receive_lag_distribution.json").exists()
    assert (tmp_path / "data/debug/phase_4_2d_clock_sanity_report.json").exists()
    assert (tmp_path / "data/debug/phase42d_failure_investigation.md").exists()
    assert not (tmp_path / PHASE42D_BUNDLE).exists()


def test_phase42d_bundle_only_when_definition_of_done_passes(tmp_path: Path) -> None:
    capture = _analysis_fixture(tmp_path, feature_lag_ms=5)
    report = run_phase42d_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    write_phase42d_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")
    for relative in PHASE42D_REQUIRED_BUNDLE_FILES:
        if relative.endswith("/"):
            continue
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("x\n", encoding="utf-8")

    bundle = create_phase42d_bundle(root=tmp_path, source_root=ROOT)
    assert bundle.exists()
    assert phase42d_bundle_missing_files(bundle) == []
    with zipfile.ZipFile(bundle) as archive:
        assert "data/dataset/orderbook_time_protocol_benchmark_labels.jsonl" in set(archive.namelist())


def test_receive_lag_calculation_and_clock_report() -> None:
    assert receive_lag_ms(local_recv_wall_ts=_wall(105), exchange_ts_ms=100) == pytest.approx(5)
    samples = [_sample(0, 100, lag_ms=-6)]
    refs = [_ref("trade_price", 110, 200, lag_ms=5)]
    schema = validate_timestamp_schema(samples, {"trade_price": refs})
    rows = generate_time_protocol_rows(samples, {"trade_price": refs}, schema)
    clock = build_clock_sanity_report(rows)
    assert clock["clock_sanity_blocker_by_source"]["trade_price"] is True

