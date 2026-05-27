from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
import zipfile

import pytest

from app.research.clock_sync_receive_lag import (
    CORRECTED_TIME_PROTOCOL_LABELS,
    PHASE42E_FAIL_AUDIT_BUNDLE,
    build_clock_sanity_report,
    build_corrected_hybrid_label,
    build_phase42e_report,
    build_receive_lag_raw_vs_corrected,
    build_server_time_sample,
    cleanup_phase42e_artifacts,
    compute_clock_offset_summary,
    corrected_receive_lag_ms,
    create_phase42e_bundle,
    evaluate_phase42e_report,
    generate_corrected_time_protocol_rows,
    phase42e_bundle_missing_files,
    raw_receive_lag_ms,
    run_phase42e_analysis,
    validate_phase42e_report_schema,
    write_phase42e_artifacts,
)
from app.research.reference_feed_benchmark import (
    REFERENCE_SOURCES,
    ReferenceValidationResult,
    analyze_reference_gap_distribution,
    reference_timestamp_monotonic_violations,
    validate_reference_event_schema,
)
from app.research.time_protocol_benchmark import (
    HYBRID_BUDGETS_MS,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    build_exchange_time_label,
    source_exchange_ts_ms,
    validate_gitignore_rules,
    validate_timestamp_schema,
)


ROOT = Path(__file__).resolve().parents[1]


def _wall(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample(local_ms: int, exchange_ms: int, *, lag_ms: float = 37_500.0, last_update_id: int = 100) -> dict[str, Any]:
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
        "lifecycle": {"snapshot_ready": True, "ready_to_emit": True, "sequence_continuous": True},
    }


def _ref(
    source: str,
    local_ms: int,
    exchange_ms: int | None,
    *,
    event_id: int = 1,
    price: float = 101.0,
    lag_ms: float = 37_510.0,
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


def _clock_samples(offset: float = 37_480.0, *, drift: float = 0.0, rtt: float = 10.0) -> list[dict[str, Any]]:
    return [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=offset,
            local_wall_after_response_ms=offset + rtt,
            binance_server_time_ms=rtt / 2.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=offset + drift + 1000.0,
            local_wall_after_response_ms=offset + drift + 1000.0 + rtt,
            binance_server_time_ms=1000.0 + rtt / 2.0,
        ),
    ]


def _clock_sample_from_offset(
    *,
    sample_id: int,
    phase: str,
    offset_ms: float,
    rtt_ms: float,
    server_time_ms: float,
) -> dict[str, Any]:
    return build_server_time_sample(
        sample_id=sample_id,
        phase=phase,
        local_wall_before_request_ms=server_time_ms + offset_ms - rtt_ms / 2.0,
        local_wall_after_response_ms=server_time_ms + offset_ms + rtt_ms / 2.0,
        binance_server_time_ms=server_time_ms,
    )


def _robust_clock_outlier_samples() -> list[dict[str, Any]]:
    return [
        _clock_sample_from_offset(sample_id=1, phase="before_capture", offset_ms=-74.0117, rtt_ms=220.7639, server_time_ms=1_000.0),
        _clock_sample_from_offset(sample_id=2, phase="during_capture", offset_ms=-3.6, rtt_ms=83.6, server_time_ms=2_000.0),
        _clock_sample_from_offset(sample_id=3, phase="during_capture", offset_ms=-4.1, rtt_ms=82.0, server_time_ms=3_000.0),
        _clock_sample_from_offset(sample_id=4, phase="during_capture", offset_ms=-5.0, rtt_ms=84.0, server_time_ms=4_000.0),
        _clock_sample_from_offset(sample_id=5, phase="during_capture", offset_ms=-5.4, rtt_ms=81.5, server_time_ms=5_000.0),
        _clock_sample_from_offset(sample_id=6, phase="during_capture", offset_ms=-6.0, rtt_ms=85.0, server_time_ms=6_000.0),
        _clock_sample_from_offset(sample_id=7, phase="during_capture", offset_ms=-6.5, rtt_ms=83.0, server_time_ms=7_000.0),
        _clock_sample_from_offset(sample_id=8, phase="after_capture", offset_ms=-4.9, rtt_ms=82.5, server_time_ms=8_000.0),
    ]


def _analysis_fixture(tmp_path: Path, *, feature_lag_ms: float = 37_500.0, offset: float = 37_480.0) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _write_required_gitignore(tmp_path)
    samples = [
        _sample(index * 100, index * 100, lag_ms=feature_lag_ms, last_update_id=100 + index)
        for index in range(6)
    ]
    refs = [
        _ref("trade_price", index * 100 + 10, index * 100, event_id=200 + index, lag_ms=feature_lag_ms + 5)
        for index in range(8)
    ]
    _write_jsonl(tmp_path / "data/dataset/orderbook_clean_samples.jsonl", samples)
    _write_jsonl(tmp_path / "data/dataset/trade_reference_events.jsonl", refs)
    _write_jsonl(tmp_path / "data/dataset/aggtrade_reference_events.jsonl", [])
    _write_jsonl(tmp_path / "data/dataset/bookticker_reference_quotes.jsonl", [])
    capture = {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "skip_capture": True,
        "cleanup_performed": True,
        "duration_sec": 1800.0,
        "requested_streams": ["btcusdt@depth@100ms", "btcusdt@bookTicker", "btcusdt@trade", "btcusdt@aggTrade"],
        "reference_event_counts": {"trade_price": len(refs)},
        "depth_clean_sample_count": len(samples),
    }
    return capture, _clock_samples(offset)


def test_server_time_sample_schema_and_midpoint_offset() -> None:
    sample = build_server_time_sample(
        sample_id=7,
        phase="before_capture",
        local_wall_before_request_ms=1000.0,
        local_wall_after_response_ms=1020.0,
        binance_server_time_ms=900.0,
    )
    assert sample["local_wall_midpoint_ms"] == pytest.approx(1010.0)
    assert sample["round_trip_ms"] == pytest.approx(20.0)
    assert sample["estimated_clock_offset_ms"] == pytest.approx(110.0)
    for field in (
        "local_wall_before_request_ms",
        "local_wall_after_response_ms",
        "local_wall_midpoint_ms",
        "binance_server_time_ms",
        "round_trip_ms",
        "estimated_clock_offset_ms",
    ):
        assert field in sample


def test_clock_offset_summary_requires_before_after_and_drift() -> None:
    summary = compute_clock_offset_summary(_clock_samples(offset=100.0, drift=12.0))
    assert summary["before_after_samples_present"] is True
    assert summary["offset_drift_ms"] == pytest.approx(12.0)
    missing = compute_clock_offset_summary([_clock_samples()[0]])
    assert missing["before_after_samples_present"] is False
    report = evaluate_phase42e_report(
        {
            "phase": "4.2E",
            "status": "pass",
            "implementation_status": "pass",
            "fresh_capture_status": "pass",
            "clock_sync_status": "pass",
            "exchange_time_coverage_status": "pass",
            "corrected_hybrid_status": "pass",
            "protocol_decision_status": "pass",
            "low_latency_ready": True,
            "primary_failure": None,
            "failure_classifications": [],
            "symbol": "BTCUSDT",
            "duration_sec": 1800.0,
            "fresh_capture_required": False,
            "max_future_gap_ms": 100,
            "clock_offset_summary": missing,
            "clock_sanity_report": {"performed": True, "clock_sanity_valid": True, "corrected_lag_sanity_valid": True},
            "leakage_check": {"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
            "sources": {},
            "selected_protocol_candidate": {"source": "trade_price"},
            "hard_fail_reasons": [],
            "warning_reasons": [],
        }
    )
    assert "CLOCK_SYNC_FAILURE" in report["failure_classifications"]


def test_clock_offset_robust_estimator_discards_high_rtt_outlier() -> None:
    samples = _robust_clock_outlier_samples()
    summary = compute_clock_offset_summary(samples)

    assert summary["clock_offset_estimator_strategy"] == "low_rtt_trimmed_median"
    assert summary["raw_clock_offset_drift_ms"] > 65.0
    assert summary["robust_offset_drift_ms"] == pytest.approx(2.9)
    assert summary["robust_clock_offset_drift_valid"] is True
    assert summary["clock_offset_drift_valid"] is True
    assert summary["discarded_clock_sample_count"] == 1
    assert summary["discarded_clock_sample_reasons"][0]["reason"] == "high_rtt_outlier"
    assert samples[0]["server_time_rtt_ms"] == pytest.approx(220.7639)
    assert samples[0]["accepted_for_clock_offset"] is False
    assert samples[0]["rejection_reason"] == "high_rtt_outlier"
    assert all(sample["accepted_for_clock_offset"] is True for sample in samples[1:])


def test_clock_offset_real_drift_across_low_rtt_samples_still_fails() -> None:
    samples = [
        _clock_sample_from_offset(sample_id=1, phase="before_capture", offset_ms=-1.0, rtt_ms=20.0, server_time_ms=1_000.0),
        _clock_sample_from_offset(sample_id=2, phase="during_capture", offset_ms=25.0, rtt_ms=21.0, server_time_ms=2_000.0),
        _clock_sample_from_offset(sample_id=3, phase="after_capture", offset_ms=62.0, rtt_ms=19.0, server_time_ms=3_000.0),
    ]
    summary = compute_clock_offset_summary(samples)
    assert summary["clock_offset_sample_quality_valid"] is True
    assert summary["robust_offset_drift_ms"] == pytest.approx(63.0)
    assert summary["clock_offset_drift_valid"] is False

    report = evaluate_phase42e_report(
        {
            "phase": "4.2E",
            "status": "pass",
            "implementation_status": "pass",
            "fresh_capture_status": "pass",
            "clock_sync_status": "pass",
            "exchange_time_coverage_status": "pass",
            "corrected_hybrid_status": "pass",
            "protocol_decision_status": "pass",
            "low_latency_ready": False,
            "primary_failure": None,
            "failure_classifications": [],
            "symbol": "BTCUSDT",
            "duration_sec": 1800.0,
            "fresh_capture_required": False,
            "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
            "clock_offset_summary": summary,
            "clock_sanity_report": build_clock_sanity_report(clock_offset_summary=summary, sources={}),
            "leakage_check": {"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
            "sources": {},
            "selected_protocol_candidate": None,
            "hard_fail_reasons": [],
            "warning_reasons": [],
        }
    )
    assert "CLOCK_OFFSET_DRIFT_FAILURE" in report["failure_classifications"]


def test_clock_offset_sample_quality_failure_when_too_few_low_rtt_samples() -> None:
    samples = [
        _clock_sample_from_offset(sample_id=1, phase="before_capture", offset_ms=-74.0, rtt_ms=220.0, server_time_ms=1_000.0),
        _clock_sample_from_offset(sample_id=2, phase="during_capture", offset_ms=-5.0, rtt_ms=221.0, server_time_ms=2_000.0),
        _clock_sample_from_offset(sample_id=3, phase="after_capture", offset_ms=-4.0, rtt_ms=20.0, server_time_ms=3_000.0),
    ]
    summary = compute_clock_offset_summary(samples)
    assert summary["accepted_clock_sample_count"] == 1
    assert summary["clock_offset_sample_quality_valid"] is False
    assert summary["clock_offset_drift_valid"] is False

    report = evaluate_phase42e_report(
        {
            "phase": "4.2E",
            "status": "pass",
            "implementation_status": "pass",
            "fresh_capture_status": "pass",
            "clock_sync_status": "pass",
            "exchange_time_coverage_status": "pass",
            "corrected_hybrid_status": "pass",
            "protocol_decision_status": "pass",
            "low_latency_ready": False,
            "primary_failure": None,
            "failure_classifications": [],
            "symbol": "BTCUSDT",
            "duration_sec": 1800.0,
            "fresh_capture_required": False,
            "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
            "clock_offset_summary": summary,
            "clock_sanity_report": build_clock_sanity_report(clock_offset_summary=summary, sources={}),
            "leakage_check": {"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
            "sources": {},
            "selected_protocol_candidate": None,
            "hard_fail_reasons": [],
            "warning_reasons": [],
        }
    )
    assert "CLOCK_OFFSET_SAMPLE_QUALITY_FAILURE" in report["failure_classifications"]
    assert "CLOCK_OFFSET_DRIFT_FAILURE" not in report["failure_classifications"]


def test_clock_offset_drift_and_rtt_gates() -> None:
    report = build_phase42e_report(
        symbol="BTCUSDT",
        clean_samples=[],
        validations={source: _validation(source, []) for source in REFERENCE_SOURCES},
        time_rows=[],
        corrected_rows=[],
        timestamp_schema={"performed": True, "status": "pass", "sources": {}},
        leakage_result={"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
        clock_offset_samples=_clock_samples(drift=60.0),
        clock_offset_summary=compute_clock_offset_summary(_clock_samples(drift=60.0)),
        capture={"duration_sec": 1800},
        cleanup_report={"cleanup_performed": True},
        gitignore_validation={"passed": True},
        fresh_capture_required=False,
    )
    assert "CLOCK_OFFSET_DRIFT_FAILURE" in report["failure_classifications"]

    report = build_phase42e_report(
        symbol="BTCUSDT",
        clean_samples=[],
        validations={source: _validation(source, []) for source in REFERENCE_SOURCES},
        time_rows=[],
        corrected_rows=[],
        timestamp_schema={"performed": True, "status": "pass", "sources": {}},
        leakage_result={"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
        clock_offset_samples=_clock_samples(rtt=1200.0),
        clock_offset_summary=compute_clock_offset_summary(_clock_samples(rtt=1200.0)),
        capture={"duration_sec": 1800},
        cleanup_report={"cleanup_performed": True},
        gitignore_validation={"passed": True},
        fresh_capture_required=False,
    )
    assert "SERVER_TIME_RTT_FAILURE" in report["failure_classifications"]


def test_raw_and_corrected_receive_lag_calculation() -> None:
    raw = raw_receive_lag_ms(local_recv_wall_ts=_wall(37_600), exchange_ts_ms=100.0)
    assert raw == pytest.approx(37_500.0)
    assert corrected_receive_lag_ms(raw, 37_480.0) == pytest.approx(20.0)


def test_large_raw_lag_explained_by_stable_clock_offset(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path, feature_lag_ms=37_500.0, offset=37_480.0)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    lag = report["sources"]["trade_price"]
    assert lag["raw_receive_lag"]["feature_raw_receive_lag_p50_ms"] == pytest.approx(37_500.0)
    assert lag["corrected_receive_lag"]["feature_corrected_receive_lag_p50_ms"] == pytest.approx(20.0)
    assert lag["clock_offset_explains_raw_lag"] is True


def test_large_raw_lag_not_explained_if_corrected_still_large(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path, feature_lag_ms=37_500.0, offset=1_000.0)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    assert report["sources"]["trade_price"]["clock_offset_explains_raw_lag"] is False


def test_corrected_hybrid_gates_and_future_lag_telemetry_only() -> None:
    exchange_label = {
        "reference_source": "trade_price",
        "eligible": True,
        "valid": True,
        "feature_receive_lag_ms": 37_500.0,
        "future_receive_lag_ms": 99_999.0,
        "feature_local_recv_monotonic_ns": 100,
        "future_reference_local_recv_monotonic_ns": 200,
    }
    valid = build_corrected_hybrid_label(
        exchange_label=exchange_label,
        corrected_feature_receive_lag_ms=20.0,
        corrected_future_receive_lag_ms=60_000.0,
        feature_lag_budget_ms=25,
        clock_offset_drift_valid=True,
    )
    assert valid["valid"] is True
    assert valid["future_receive_lag_hard_gate_used"] is False

    too_high = build_corrected_hybrid_label(
        exchange_label=exchange_label,
        corrected_feature_receive_lag_ms=51.0,
        corrected_future_receive_lag_ms=60_000.0,
        feature_lag_budget_ms=50,
        clock_offset_drift_valid=True,
    )
    assert too_high["invalid_reason"] == "CORRECTED_FEATURE_RECEIVE_LAG_TOO_HIGH"

    reordered = build_corrected_hybrid_label(
        exchange_label={**exchange_label, "future_reference_local_recv_monotonic_ns": 100},
        corrected_feature_receive_lag_ms=20.0,
        corrected_future_receive_lag_ms=60_000.0,
        feature_lag_budget_ms=25,
        clock_offset_drift_valid=True,
    )
    assert reordered["invalid_reason"] == "CROSS_STREAM_RECEIVE_REORDER"

    drift = build_corrected_hybrid_label(
        exchange_label=exchange_label,
        corrected_feature_receive_lag_ms=20.0,
        corrected_future_receive_lag_ms=60_000.0,
        feature_lag_budget_ms=25,
        clock_offset_drift_valid=False,
    )
    assert drift["invalid_reason"] == "CLOCK_OFFSET_DRIFT_INVALID"


def test_corrected_hybrid_computed_for_all_budgets(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    assert set(report["sources"]["trade_price"]["corrected_hybrid"]) == {
        f"corrected_hybrid_{budget}ms" for budget in HYBRID_BUDGETS_MS
    }


def test_exchange_time_safety_rules_still_hold() -> None:
    sample = _sample(0, 0, lag_ms=5)
    refs = [
        _ref("trade_price", 500, 90, event_id=1, price=101),
        _ref("trade_price", 1000, 100, event_id=2, price=102),
        _ref("trade_price", 100, 500, event_id=3, price=103),
    ]
    exchange_sorted = sorted(refs, key=lambda row: float(row["trade_time"]))
    label = build_exchange_time_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=exchange_sorted,
        reference_exchange_timestamps_ms=[float(row["trade_time"]) for row in exchange_sorted],
        exchange_time_supported=True,
        unsupported_reason="",
    )
    assert label["future_reference_event_id"] == 2
    assert label["selection_time_basis"] == "exchange_ts"
    trade_ts = source_exchange_ts_ms(_ref("trade_price", 100, 100, include_t=True), "trade_price")
    aggtrade_ts = source_exchange_ts_ms(_ref("aggTrade_price", 100, 100, include_t=True), "aggTrade_price")
    assert trade_ts is not None
    assert aggtrade_ts is not None
    assert trade_ts[0] == "T"
    assert aggtrade_ts[0] == "T"
    schema = validate_timestamp_schema([sample], {"bookTicker_mid": [_ref("bookTicker_mid", 100, None, include_e=False)]})
    assert schema["sources"]["bookTicker_mid"]["exchange_time_supported"] is False
    assert REQUIRED_100MS_MAX_FUTURE_GAP_MS == 100


def test_low_latency_ready_requires_corrected_hybrid_and_no_leakage(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path, feature_lag_ms=37_500.0, offset=37_480.0)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    assert report["low_latency_ready"] is True
    assert report["selected_protocol_candidate"]["protocol"] == "corrected_hybrid_low_latency_protocol"

    broken = dict(report)
    broken["leakage_check"] = {"performed": True, "feature_leakage_violations": 1, "label_leakage_violations": 0}
    assert evaluate_phase42e_report(broken)["low_latency_ready"] is True
    assert "FEATURE_LEAKAGE_FAILURE" in evaluate_phase42e_report(broken)["failure_classifications"]


def test_exchange_pass_corrected_hybrid_fail_not_low_latency_ready(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path, feature_lag_ms=37_500.0, offset=1_000.0)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    assert report["exchange_time_coverage_status"] == "pass"
    assert report["corrected_hybrid_status"] == "fail"
    assert report["low_latency_ready"] is False
    assert "exchange_time_market_coverage_passed_but_corrected_hybrid_live_observability_failed" in report["warning_reasons"]


def test_phase42e_artifacts_and_fail_audit_bundle(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path, feature_lag_ms=37_500.0, offset=1_000.0)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    write_phase42e_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")
    assert not validate_phase42e_report_schema(report)
    assert (tmp_path / "data/reports/phase_4_2e_clock_sync_receive_lag_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2e_clock_sync_receive_lag_report.md").exists()
    assert (tmp_path / "data/reports/phase42e_self_check.json").exists()
    assert (tmp_path / "data/debug/phase_4_2e_clock_offset_samples.json").exists()
    clock_payload = json.loads((tmp_path / "data/debug/phase_4_2e_clock_offset_samples.json").read_text(encoding="utf-8"))
    assert clock_payload["summary"]["clock_offset_estimator_strategy"] == "low_rtt_trimmed_median"
    assert "raw_estimated_clock_offset_ms_values" in clock_payload["summary"]
    assert "raw_server_time_rtt_ms_values" in clock_payload["summary"]
    assert "discarded_clock_sample_reasons" in clock_payload["summary"]
    assert "server_time_rtt_ms" in clock_payload["samples"][0]
    assert "accepted_for_clock_offset" in clock_payload["samples"][0]
    assert "rejection_reason" in clock_payload["samples"][0]
    assert (tmp_path / "data/debug/phase_4_2e_receive_lag_raw_vs_corrected.json").exists()
    assert (tmp_path / "data/debug/phase_4_2e_corrected_hybrid_summary.json").exists()
    assert (tmp_path / "data/debug/phase_4_2e_clock_sanity_report.json").exists()
    assert (tmp_path / "data/debug/phase42e_failure_investigation.md").exists()
    (tmp_path / "data/debug/phase_4_2e_artifact_cleanup.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "data/debug/phase_4_2e_multifeed_capture_diagnostics.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "data/debug/phase_4_2e_typecheck_report.txt").write_text("ok\n", encoding="utf-8")
    bundle = create_phase42e_bundle(root=tmp_path, pass_bundle=False)
    assert bundle.name == PHASE42E_FAIL_AUDIT_BUNDLE.name
    assert phase42e_bundle_missing_files(bundle, pass_bundle=False) == []


def test_phase42e_pass_bundle_created_only_on_pass(tmp_path: Path) -> None:
    capture, samples = _analysis_fixture(tmp_path)
    report = run_phase42e_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=samples,
        capture=capture,
        cleanup_report={"cleanup_performed": True},
        gitignore_validation=validate_gitignore_rules(tmp_path),
        fresh_capture_required=False,
    )
    write_phase42e_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")
    (tmp_path / "data/debug/phase_4_2e_artifact_cleanup.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "data/debug/phase_4_2e_multifeed_capture_diagnostics.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "data/debug/phase_4_2e_typecheck_report.txt").write_text("ok\n", encoding="utf-8")
    bundle = create_phase42e_bundle(root=tmp_path, pass_bundle=True)
    assert bundle.exists()
    assert phase42e_bundle_missing_files(bundle, pass_bundle=True) == []
    with zipfile.ZipFile(bundle) as archive:
        assert "data/reports/phase42e_self_check.json" in set(archive.namelist())


def test_cleanup_report_schema(tmp_path: Path) -> None:
    generated = tmp_path / "data/dataset/old.jsonl"
    generated.parent.mkdir(parents=True)
    generated.write_text("old\n", encoding="utf-8")
    source = tmp_path / "scripts/keep.py"
    source.parent.mkdir()
    source.write_text("print('keep')\n", encoding="utf-8")
    report = cleanup_phase42e_artifacts(tmp_path)
    assert set(report) == {"cleanup_performed", "deleted_files", "missing_files_skipped", "errors"}
    assert report["cleanup_performed"] is True
    assert not generated.exists()
    assert source.exists()


def test_corrected_lag_negative_beyond_skew_fails() -> None:
    exchange_label = {
        "reference_source": "trade_price",
        "eligible": True,
        "valid": True,
        "feature_receive_lag_ms": 10.0,
        "future_receive_lag_ms": 10.0,
        "feature_local_recv_monotonic_ns": 100,
        "future_reference_local_recv_monotonic_ns": 200,
    }
    within = build_corrected_hybrid_label(
        exchange_label=exchange_label,
        corrected_feature_receive_lag_ms=-5.0,
        corrected_future_receive_lag_ms=1000.0,
        feature_lag_budget_ms=25,
        clock_offset_drift_valid=True,
    )
    beyond = build_corrected_hybrid_label(
        exchange_label=exchange_label,
        corrected_feature_receive_lag_ms=-5.1,
        corrected_future_receive_lag_ms=1000.0,
        feature_lag_budget_ms=25,
        clock_offset_drift_valid=True,
    )
    assert within["valid"] is True
    assert beyond["invalid_reason"] == "CORRECTED_LAG_CLOCK_SANITY_FAILURE"

