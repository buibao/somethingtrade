from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from app.marketdata.binance_aggtrade_source import parse_aggtrade_payload
from app.marketdata.binance_trade_source import parse_trade_payload
from app.research.bookticker_reference import parse_bookticker_payload
from app.research.reference_feed_benchmark import (
    PHASE42C_CAPTURE_DIAGNOSTICS,
    PHASE42C_CLEANUP_REPORT,
    PHASE42C_REQUIRED_BUNDLE_FILES,
    PHASE42C_TYPECHECK_REPORT,
    REFERENCE_SOURCES,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    ReferenceValidationResult,
    analyze_reference_gap_distribution,
    build_phase42c_report,
    build_reference_label,
    classify_phase42c_failure,
    cleanup_phase42c_artifacts,
    compute_source_metrics,
    create_phase42c_bundle,
    evaluate_phase42c_report,
    generate_benchmark_rows,
    phase42c_bundle_missing_files,
    rank_reference_sources,
    reference_timestamp_monotonic_violations,
    required_streams,
    run_phase42c_leakage_check,
    stream_name_for_source,
    validate_depth_reference_events,
    validate_capture_diagnostics,
    validate_phase42c_report_schema,
    validate_reference_event_schema,
    validate_reference_events,
    write_phase42c_artifacts,
)


ROOT = Path(__file__).resolve().parents[1]


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample_ms(ts_ms: int, *, last_update_id: int = 100) -> dict[str, object]:
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
        "local_recv_monotonic_ns": ts_ms * 1_000_000,
        "local_recv_wall_ts": "2026-05-20T00:00:00.000000+00:00",
        "exchange_event_ts": 1_779_213_534_814_000_000 + ts_ms,
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


def _ref_ns(source: str, ts_ns: int, *, event_id: int = 1, price: float = 101.0) -> dict[str, object]:
    base: dict[str, object] = {
        "symbol": "BTCUSDT",
        "local_recv_monotonic_ns": ts_ns,
        "local_recv_wall_ts": "2026-05-20T00:00:00.000000+00:00",
        "exchange_event_ts": 1_779_213_534_814,
        "quality": {"valid": True, "errors": []},
    }
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
        return {
            **base,
            "schema_version": "trade_reference_v1",
            "source": "binance_ws_trade",
            "trade_time": 1_779_213_534_814,
            "trade_id": event_id,
            "price": price,
            "quantity": 0.01,
            "is_buyer_market_maker": False,
        }
    if source == "aggTrade_price":
        return {
            **base,
            "schema_version": "aggtrade_reference_v1",
            "source": "binance_ws_aggTrade",
            "trade_time": 1_779_213_534_814,
            "aggregate_trade_id": event_id,
            "first_trade_id": event_id,
            "last_trade_id": event_id + 1,
            "price": price,
            "quantity": 0.02,
            "is_buyer_market_maker": False,
        }
    raise AssertionError(source)


def _ref_ms(source: str, ts_ms: float, *, event_id: int = 1, price: float = 101.0) -> dict[str, object]:
    return _ref_ns(source, int(ts_ms * 1_000_000), event_id=event_id, price=price)


def _validation(source: str, rows: list[dict[str, object]], *, file_exists: bool = True) -> ReferenceValidationResult:
    valid: list[dict[str, object]] = []
    invalid: list[dict[str, object]] = []
    for row in rows:
        errors = validate_reference_event_schema(row, source)
        if errors:
            invalid.append({"reason": errors})
        else:
            valid.append(row)
    quality = analyze_reference_gap_distribution(valid)
    return ReferenceValidationResult(
        reference_source=source,
        file_exists=file_exists,
        reference_event_count=len(rows),
        valid_reference_event_count=len(valid),
        invalid_reference_event_count=len(invalid),
        valid_events=valid,
        invalid_events=invalid,
        timestamp_monotonic_violations=reference_timestamp_monotonic_violations(valid),
        quality=quality,
    )


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _runtime_quality(**overrides: object) -> dict[str, object]:
    quality: dict[str, object] = {
        "sample_before_ready_count": 0,
        "feed_receive_stale_count": 0,
        "queue_dropped_messages": 0,
        "sequence_gap_count": 0,
        "invalid_delta_count": 0,
        "crossed_book_count": 0,
        "book_empty_count": 0,
        "one_side_missing_count": 0,
        "clean_sample_schema_violation_count": 0,
        "snapshot_copy_p99_us": 0.0,
    }
    quality.update(overrides)
    return quality


def _capture(**overrides: object) -> dict[str, object]:
    capture: dict[str, object] = {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "duration_sec": 10.0,
        "depth_stream": "btcusdt@depth@100ms",
        "reference_streams": ["btcusdt@bookTicker", "btcusdt@trade", "btcusdt@aggTrade"],
        "downsampling_enabled": False,
    }
    capture.update(overrides)
    return capture


def _diagnostics(
    *,
    symbol: str = "BTCUSDT",
    missing_stream: str | None = None,
    message_overrides: dict[str, int] | None = None,
    parsed_overrides: dict[str, int] | None = None,
) -> dict[str, object]:
    streams = required_streams(symbol)
    if missing_stream is not None:
        streams = [stream for stream in streams if stream != missing_stream]
    messages = {stream: 10 for stream in required_streams(symbol)}
    if message_overrides:
        messages.update(message_overrides)
    parsed = {source: 10 for source in REFERENCE_SOURCES}
    if parsed_overrides:
        parsed.update(parsed_overrides)
    return {
        "fresh_capture_performed": True,
        "fixture_mode": False,
        "skip_capture": False,
        "duration_sec": 1800.0,
        "symbol": symbol,
        "websocket_url": "wss://stream.binance.com:9443/stream?streams=x",
        "requested_streams": streams,
        "connected": True,
        "connect_count": 1,
        "disconnect_count": 1,
        "reconnect_count": 0,
        "message_count_by_stream": messages,
        "parsed_count_by_source": parsed,
        "parse_error_count_by_source": {source: 0 for source in REFERENCE_SOURCES},
        "unknown_stream_count": 0,
        "first_message_wall_ts_by_stream": {},
        "last_message_wall_ts_by_stream": {},
        "output_file_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker": "data/dataset/bookticker_reference_quotes.jsonl",
            "trade": "data/dataset/trade_reference_events.jsonl",
            "aggtrade": "data/dataset/aggtrade_reference_events.jsonl",
        },
        "output_file_sizes_bytes": {
            "clean_samples": 1,
            "bookticker": 1,
            "trade": 1,
            "aggtrade": 1,
        },
    }


def _metrics(source: str, rate: float, *, leakage: int = 0, monotonic: int = 0) -> dict[str, object]:
    passes = rate >= 0.95 and leakage == 0 and monotonic == 0
    return {
        "reference_source": source,
        "semantic_type": "quote_mid" if source in {"depth_mid", "bookTicker_mid"} else "transaction_price",
        "semantic_description": source,
        "file_exists": True,
        "max_future_gap_ms": 100,
        "reference_event_count": 100,
        "valid_reference_event_count": 100,
        "invalid_reference_event_count": 0,
        "reference_timestamp_monotonic_violations": monotonic,
        "reference_sample_rate_per_sec": 10.0,
        "gap_p50_ms": 10.0,
        "gap_p90_ms": 20.0,
        "gap_p95_ms": 30.0,
        "gap_p99_ms": 40.0,
        "gap_max_ms": 50.0,
        "gap_over_100ms_count": 0,
        "gap_over_100ms_total_duration_ms": 0.0,
        "bad_time_coverage_ratio_100ms": 0.0,
        "eligible_count": 100,
        "valid_count": int(rate * 100),
        "invalid_count": 100 - int(rate * 100),
        "valid_rate_all_rows": rate,
        "valid_rate_eligible_rows": rate,
        "invalid_reason_counts": {},
        "future_gap_p50_ms": 0.0,
        "future_gap_p90_ms": 1.0,
        "future_gap_p95_ms": 2.0,
        "future_gap_p99_ms": 3.0,
        "future_gap_max_ms": 4.0,
        "label_leakage_violations": leakage,
        "passes_100ms_gate": passes,
        "source_status": "measured_pass" if passes else "measured_coverage_failed",
    }


def _report_from_rates(rates: dict[str, float]) -> dict[str, object]:
    metrics = {source: _metrics(source, rates[source]) for source in REFERENCE_SOURCES}
    benchmark_rows = [{"local_recv_monotonic_ns": 0, "reference_labels": {}}]
    return build_phase42c_report(
        symbol="BTCUSDT",
        clean_samples=[_sample_ms(index * 100, last_update_id=100 + index) for index in range(20)],
        source_metrics=metrics,
        benchmark_rows=benchmark_rows,
        leakage_result={
            "passed": True,
            "feature_leakage_violations": 0,
            "label_leakage_violations": 0,
            "label_leakage_violations_by_source": {source: 0 for source in REFERENCE_SOURCES},
        },
        depth_runtime_quality=_runtime_quality(),
        capture=_capture(),
        fresh_capture_required=False,
    )


def test_trade_parse_valid_payload() -> None:
    row = parse_trade_payload(
        {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 123, "p": "104000.10", "q": "0.001", "T": 2, "m": True},
        local_recv_monotonic_ns=99,
        local_recv_wall_ts="2026-05-20T00:00:00+00:00",
    )
    assert row["schema_version"] == "trade_reference_v1"
    assert row["trade_id"] == 123
    assert row["price"] == pytest.approx(104000.10)
    assert row["quantity"] == pytest.approx(0.001)
    assert row["quality"]["valid"] is True


def test_trade_parser_rejects_bad_payloads() -> None:
    base = {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 123, "p": "100", "q": "0.001", "T": 2, "m": True}
    assert "MISSING_PRICE" in parse_trade_payload({**base, "p": None}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "INVALID_PRICE" in parse_trade_payload({**base, "p": "bad"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "NEGATIVE_QUANTITY" in parse_trade_payload({**base, "q": "-1"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "MISSING_TRADE_ID" in parse_trade_payload({**base, "t": None}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]


def test_aggtrade_parse_valid_payload() -> None:
    row = parse_aggtrade_payload(
        {"e": "aggTrade", "E": 1, "s": "BTCUSDT", "a": 123, "p": "104000.10", "q": "0.010", "f": 100, "l": 105, "T": 2, "m": True},
        local_recv_monotonic_ns=99,
        local_recv_wall_ts="2026-05-20T00:00:00+00:00",
    )
    assert row["schema_version"] == "aggtrade_reference_v1"
    assert row["aggregate_trade_id"] == 123
    assert row["first_trade_id"] == 100
    assert row["last_trade_id"] == 105
    assert row["price"] == pytest.approx(104000.10)
    assert row["quantity"] == pytest.approx(0.010)
    assert row["quality"]["valid"] is True


def test_aggtrade_parser_rejects_bad_payloads() -> None:
    base = {"e": "aggTrade", "E": 1, "s": "BTCUSDT", "a": 123, "p": "100", "q": "0.001", "f": 1, "l": 2, "T": 2, "m": True}
    assert "MISSING_AGGREGATE_TRADE_ID" in parse_aggtrade_payload({**base, "a": None}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "INVALID_PRICE" in parse_aggtrade_payload({**base, "p": "bad"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "NEGATIVE_QUANTITY" in parse_aggtrade_payload({**base, "q": "-1"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]


def test_bookticker_parser_regression() -> None:
    row = parse_bookticker_payload(
        {"u": 400900217, "s": "BTCUSDT", "b": "100.0", "B": "1.0", "a": "101.0", "A": "2.0"},
        local_recv_monotonic_ns=123,
        local_recv_wall_ts="2026-05-20T00:00:00+00:00",
    )
    assert row["mid_price"] == pytest.approx(100.5)
    assert row["quality"]["valid"] is True


def test_reference_schema_and_timestamp_validation(tmp_path: Path) -> None:
    valid = _ref_ms("trade_price", 0)
    missing_ts = dict(valid)
    missing_ts.pop("local_recv_monotonic_ns")
    missing_wall = dict(valid)
    missing_wall.pop("local_recv_wall_ts")
    non_positive = dict(valid)
    non_positive["price"] = 0
    assert "MISSING_LOCAL_RECV_MONOTONIC_NS" in validate_reference_event_schema(missing_ts, "trade_price")
    assert "MISSING_LOCAL_RECV_WALL_TS" in validate_reference_event_schema(missing_wall, "trade_price")
    assert "NON_POSITIVE_REFERENCE_PRICE" in validate_reference_event_schema(non_positive, "trade_price")
    assert reference_timestamp_monotonic_violations([_ref_ms("trade_price", 0), _ref_ms("trade_price", 100, event_id=2)]) == 0
    assert reference_timestamp_monotonic_violations([_ref_ms("trade_price", 100), _ref_ms("trade_price", 90, event_id=2)]) == 1

    path = tmp_path / "trade.jsonl"
    _write_jsonl(path, [valid, missing_wall])
    result = validate_reference_events(path, reference_source="trade_price")
    assert result.reference_event_count == 2
    assert result.valid_reference_event_count == 1
    assert result.invalid_reference_event_count == 1


@pytest.mark.parametrize("source", REFERENCE_SOURCES)
def test_reference_future_first_at_or_after_target_all_sources(source: str) -> None:
    sample = _sample_ms(0)
    refs = [_ref_ms(source, 99, event_id=1), _ref_ms(source, 100, event_id=2), _ref_ms(source, 101, event_id=3)] if source != "depth_mid" else []
    if source == "depth_mid":
        refs = validate_depth_reference_events([_sample_ms(99, last_update_id=1), _sample_ms(100, last_update_id=2), _sample_ms(101, last_update_id=3)]).valid_events
    timestamps = [int(row["local_recv_monotonic_ns"]) for row in refs]
    label = build_reference_label(
        reference_source=source,
        feature_sample=sample,
        feature_mid_price=100.5,
        references=refs,
        reference_timestamps_ns=timestamps,
    )
    assert label["future_reference_index"] == 1
    assert label["future_reference_local_recv_monotonic_ns"] == 100_000_000


@pytest.mark.parametrize("source", REFERENCE_SOURCES)
def test_reference_future_skips_before_target_all_sources(source: str) -> None:
    sample = _sample_ms(0)
    refs = [_ref_ms(source, 99, event_id=1), _ref_ms(source, 101, event_id=2)] if source != "depth_mid" else []
    if source == "depth_mid":
        refs = validate_depth_reference_events([_sample_ms(99, last_update_id=1), _sample_ms(101, last_update_id=2)]).valid_events
    label = build_reference_label(
        reference_source=source,
        feature_sample=sample,
        feature_mid_price=100.5,
        references=refs,
        reference_timestamps_ns=[int(row["local_recv_monotonic_ns"]) for row in refs],
    )
    assert label["future_reference_index"] == 1
    assert label["future_reference_local_recv_monotonic_ns"] == 101_000_000


def test_reference_future_gap_boundaries_and_no_future() -> None:
    sample = _sample_ms(0)
    equal = build_reference_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=[_ref_ms("trade_price", 200)],
        reference_timestamps_ns=[200_000_000],
    )
    assert equal["valid"] is True
    assert equal["future_gap_ms"] == 100.0

    above = build_reference_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=[_ref_ns("trade_price", 200_001_000)],
        reference_timestamps_ns=[200_001_000],
    )
    assert above["valid"] is False
    assert above["invalid_reason"] == "FUTURE_REFERENCE_GAP_TOO_LARGE"

    no_future = build_reference_label(
        reference_source="trade_price",
        feature_sample=sample,
        feature_mid_price=100.5,
        references=[_ref_ms("trade_price", 99)],
        reference_timestamps_ns=[99_000_000],
    )
    assert no_future["invalid_reason"] == "NO_FUTURE_REFERENCE"


def test_future_reference_never_uses_current_or_past_event() -> None:
    rows = generate_benchmark_rows(
        [_sample_ms(0)],
        {
            "depth_mid": validate_depth_reference_events([_sample_ms(0), _sample_ms(100, last_update_id=2)]).valid_events,
            "bookTicker_mid": [_ref_ms("bookTicker_mid", 0), _ref_ms("bookTicker_mid", 100, event_id=2)],
            "trade_price": [_ref_ms("trade_price", 0), _ref_ms("trade_price", 100, event_id=2)],
            "aggTrade_price": [_ref_ms("aggTrade_price", 0), _ref_ms("aggTrade_price", 100, event_id=2)],
        },
    )
    assert run_phase42c_leakage_check(rows)["label_leakage_violations"] == 0
    for source in REFERENCE_SOURCES:
        label = rows[0]["reference_labels"][source]["horizon_100ms"]
        assert label["future_reference_local_recv_monotonic_ns"] == 100_000_000


def test_valid_rate_eligible_rows_tail_and_middle_gap() -> None:
    samples = [_sample_ms(0), _sample_ms(100, last_update_id=2), _sample_ms(200, last_update_id=3)]
    tail_refs = [_ref_ms("trade_price", 100)]
    rows = generate_benchmark_rows(samples, {"trade_price": tail_refs})
    leakage = run_phase42c_leakage_check(rows)
    metrics = compute_source_metrics(
        reference_source="trade_price",
        validation=_validation("trade_price", tail_refs),
        clean_samples=samples,
        benchmark_rows=rows,
        label_leakage_violations=leakage["label_leakage_violations_by_source"]["trade_price"],
    )
    assert metrics["eligible_count"] == 1
    assert metrics["valid_count"] == 1
    assert metrics["valid_rate_eligible_rows"] == pytest.approx(1.0)

    gap_refs = [_ref_ms("trade_price", 100), _ref_ms("trade_price", 400, event_id=2)]
    rows = generate_benchmark_rows(samples, {"trade_price": gap_refs})
    metrics = compute_source_metrics(
        reference_source="trade_price",
        validation=_validation("trade_price", gap_refs),
        clean_samples=samples,
        benchmark_rows=rows,
        label_leakage_violations=0,
    )
    assert metrics["eligible_count"] == 3
    assert metrics["valid_count"] == 2
    assert metrics["valid_rate_eligible_rows"] == pytest.approx(2 / 3)


def test_phase_pass_fail_boundaries_and_selection() -> None:
    fail_report = _report_from_rates({source: 0.94 for source in REFERENCE_SOURCES})
    assert fail_report["definition_of_done_status"] == "fail"
    assert fail_report["selected_reference_source"] is None
    assert classify_phase42c_failure(fail_report) == "NO_REFERENCE_SOURCE_PASSED_100MS"

    equal_report = _report_from_rates({
        "depth_mid": 0.94,
        "bookTicker_mid": 0.95,
        "trade_price": 0.94,
        "aggTrade_price": 0.94,
    })
    assert equal_report["definition_of_done_status"] == "pass"
    assert equal_report["selected_reference_source"] == "bookTicker_mid"

    high_report = _report_from_rates({
        "depth_mid": 0.96,
        "bookTicker_mid": 0.97,
        "trade_price": 0.99,
        "aggTrade_price": 0.98,
    })
    assert high_report["selected_reference_source"] == "trade_price"
    assert high_report["semantic_warning"]

    tied = rank_reference_sources({
        "depth_mid": _metrics("depth_mid", 0.98),
        "bookTicker_mid": _metrics("bookTicker_mid", 0.98),
        "trade_price": _metrics("trade_price", 0.98),
        "aggTrade_price": _metrics("aggTrade_price", 0.98),
    })
    assert [item["reference_source"] for item in tied] == list(REFERENCE_SOURCES)


def test_bad_time_coverage_ratio_calculation() -> None:
    dense = analyze_reference_gap_distribution([_ref_ms("trade_price", 0), _ref_ms("trade_price", 50, event_id=2), _ref_ms("trade_price", 100, event_id=3)])
    assert dense["bad_time_coverage_ratio_100ms"] == 0

    sparse = analyze_reference_gap_distribution([_ref_ms("trade_price", 0), _ref_ms("trade_price", 100, event_id=2), _ref_ms("trade_price", 300, event_id=3)])
    assert sparse["gap_over_100ms_count"] == 1
    assert sparse["gap_over_100ms_total_duration_ms"] == 100.0
    assert sparse["bad_time_coverage_ratio_100ms"] == pytest.approx(100 / 300)


def test_leakage_detected_per_source_and_globally() -> None:
    rows = generate_benchmark_rows([_sample_ms(0)], {"trade_price": [_ref_ms("trade_price", 100)]})
    rows[0]["reference_labels"]["trade_price"]["horizon_100ms"]["future_reference_local_recv_monotonic_ns"] = 99_000_000
    leakage = run_phase42c_leakage_check(rows)
    assert leakage["label_leakage_violations_by_source"]["trade_price"] == 1

    rows = generate_benchmark_rows([_sample_ms(0)], {"trade_price": [_ref_ms("trade_price", 100)]})
    rows[0]["quality"]["feature_source_indices"]["past_mid_return_100ms_bps"] = 1
    leakage = run_phase42c_leakage_check(rows)
    assert leakage["feature_leakage_violations"] == 1

    rows = generate_benchmark_rows([_sample_ms(0)], {"trade_price": [_ref_ms("trade_price", 100)]})
    assert run_phase42c_leakage_check(rows)["passed"] is True


def test_phase42c_report_schema_and_content() -> None:
    report = _report_from_rates({source: 0.96 for source in REFERENCE_SOURCES})
    assert not validate_phase42c_report_schema(report)
    assert set(report["reference_sources"]) == set(REFERENCE_SOURCES)
    assert report["ranking"]
    assert report["selected_reference_source"] == "depth_mid"

    broken = dict(report)
    broken.pop("reference_sources")
    assert "missing required field: reference_sources" in validate_phase42c_report_schema(broken)

    fail_report = _report_from_rates({source: 0.94 for source in REFERENCE_SOURCES})
    assert fail_report["selected_reference_source"] is None

    trade_report = _report_from_rates({
        "depth_mid": 0.94,
        "bookTicker_mid": 0.94,
        "trade_price": 0.96,
        "aggTrade_price": 0.94,
    })
    assert trade_report["selected_reference_source"] == "trade_price"
    assert "transaction-price-based" in trade_report["semantic_warning"]


def test_policy_relaxation_and_leakage_failures_are_hard() -> None:
    assert REQUIRED_100MS_MAX_FUTURE_GAP_MS == 100
    report = _report_from_rates({source: 0.96 for source in REFERENCE_SOURCES})
    report["reference_sources"]["depth_mid"]["max_future_gap_ms"] = 120
    assert classify_phase42c_failure(evaluate_phase42c_report(report)) == "HORIZON_100MS_POLICY_RELAXED"

    report = _report_from_rates({source: 0.96 for source in REFERENCE_SOURCES})
    report["leakage_check"]["feature_leakage_violations"] = 1
    assert classify_phase42c_failure(evaluate_phase42c_report(report)) == "FEATURE_LEAKAGE_FAILURE"

    metrics = {source: _metrics(source, 0.94) for source in REFERENCE_SOURCES}
    metrics["trade_price"] = _metrics("trade_price", 0.96, leakage=1)
    report = build_phase42c_report(
        symbol="BTCUSDT",
        clean_samples=[_sample_ms(0)],
        source_metrics=metrics,
        benchmark_rows=[{"local_recv_monotonic_ns": 0, "reference_labels": {}}],
        leakage_result={"passed": False, "feature_leakage_violations": 0, "label_leakage_violations": 1, "label_leakage_violations_by_source": {"trade_price": 1}},
        depth_runtime_quality=_runtime_quality(),
        capture=_capture(),
        fresh_capture_required=False,
    )
    assert report["selected_reference_source"] is None
    assert classify_phase42c_failure(report) == "NO_REFERENCE_SOURCE_PASSED_100MS"


def test_phase42c_fail_artifacts_and_no_bundle(tmp_path: Path) -> None:
    report = _report_from_rates({source: 0.94 for source in REFERENCE_SOURCES})
    write_phase42c_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")

    assert (tmp_path / "data/reports/phase_4_2c_reference_feed_benchmark_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2c_reference_feed_benchmark_report.md").exists()
    self_check = json.loads((tmp_path / "data/reports/phase42c_self_check.json").read_text())
    assert self_check["passed"] is False
    assert (tmp_path / "data/debug/phase42c_failure_investigation.md").exists()
    assert not (tmp_path / "phase_4_2c_reference_feed_benchmark_bundle.zip").exists()


def test_phase42c_cleanup_deletes_stale_artifacts_and_skips_missing(tmp_path: Path) -> None:
    stale = tmp_path / "data/debug/phase_4_2c_old.json"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("stale\n", encoding="utf-8")
    explicit = tmp_path / "data/dataset/trade_reference_events.jsonl"
    explicit.parent.mkdir(parents=True, exist_ok=True)
    explicit.write_text("stale\n", encoding="utf-8")

    report = cleanup_phase42c_artifacts(tmp_path)

    assert report["cleanup_performed"] is True
    assert "data/debug/phase_4_2c_old.json" in report["deleted_files"]
    assert "data/dataset/trade_reference_events.jsonl" in report["deleted_files"]
    assert report["errors"] == []
    assert not stale.exists()
    assert not explicit.exists()
    assert (tmp_path / PHASE42C_CLEANUP_REPORT).exists()
    assert report["missing_files_skipped"]


def test_phase42c_cleanup_failure_blocks_self_check() -> None:
    report = _report_from_rates({source: 0.96 for source in REFERENCE_SOURCES})
    report["cleanup_failed"] = True
    evaluated = evaluate_phase42c_report(report)
    assert classify_phase42c_failure(evaluated) == "ARTIFACT_CLEANUP_FAILURE"
    assert evaluated["definition_of_done_status"] == "fail"


def test_phase42c_final_run_fails_if_skip_capture_without_fixture_flag(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42c_reference_feed_benchmark.py"),
            "--skip-capture",
            "--skip-pytest",
            "--root",
            str(tmp_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode != 0
    self_check = json.loads((tmp_path / "data/reports/phase42c_self_check.json").read_text())
    assert self_check["failure_classification"] == "FRESH_CAPTURE_NOT_PERFORMED"
    assert not (tmp_path / "phase_4_2c_reference_feed_benchmark_bundle.zip").exists()


def test_phase42c_final_run_requires_duration_at_least_1800(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42c_reference_feed_benchmark.py"),
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
    self_check = json.loads((tmp_path / "data/reports/phase42c_self_check.json").read_text())
    assert self_check["failure_classification"] == "MULTI_FEED_CAPTURE_INCOMPLETE"


def test_phase42c_diagnostics_required_streams_and_counts() -> None:
    diagnostics = _diagnostics()
    assert validate_capture_diagnostics(diagnostics, symbol="BTCUSDT") == []
    assert set(diagnostics["message_count_by_stream"]) == set(required_streams("BTCUSDT"))
    assert set(diagnostics["parse_error_count_by_source"]) == set(REFERENCE_SOURCES)

    missing = _diagnostics(missing_stream="btcusdt@trade")
    assert "missing requested stream: btcusdt@trade" in validate_capture_diagnostics(missing, symbol="BTCUSDT")


def test_phase42c_source_statuses_from_diagnostics() -> None:
    trade_stream = stream_name_for_source(symbol="BTCUSDT", reference_source="trade_price")
    empty_diag = _diagnostics(message_overrides={trade_stream: 0}, parsed_overrides={"trade_price": 0})
    empty_metrics = compute_source_metrics(
        reference_source="trade_price",
        validation=_validation("trade_price", []),
        clean_samples=[_sample_ms(0)],
        benchmark_rows=[],
        label_leakage_violations=0,
        capture_diagnostics=empty_diag,
    )
    assert empty_metrics["source_status"] == "captured_but_empty"

    parser_diag = _diagnostics(message_overrides={trade_stream: 5}, parsed_overrides={"trade_price": 5})
    invalid_trade = _ref_ms("trade_price", 100)
    invalid_trade["price"] = None
    parser_metrics = compute_source_metrics(
        reference_source="trade_price",
        validation=_validation("trade_price", [invalid_trade]),
        clean_samples=[_sample_ms(0)],
        benchmark_rows=[],
        label_leakage_violations=0,
        capture_diagnostics=parser_diag,
    )
    assert parser_metrics["source_status"] == "parser_failed_or_all_invalid"


def test_phase42c_bundle_contains_required_files(tmp_path: Path) -> None:
    for relative in PHASE42C_REQUIRED_BUNDLE_FILES:
        if relative.endswith("/"):
            continue
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    bundle = create_phase42c_bundle(root=tmp_path, source_root=ROOT)
    assert bundle.exists()
    assert phase42c_bundle_missing_files(bundle) == []
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
    for required in PHASE42C_REQUIRED_BUNDLE_FILES:
        assert required in names


def test_phase42c_self_check_skip_capture_fixture_passes_and_creates_bundle(tmp_path: Path) -> None:
    clean_path = tmp_path / "data/dataset/orderbook_clean_samples.jsonl"
    book_path = tmp_path / "data/dataset/bookticker_reference_quotes.jsonl"
    trade_path = tmp_path / "data/dataset/trade_reference_events.jsonl"
    agg_path = tmp_path / "data/dataset/aggtrade_reference_events.jsonl"
    samples = [_sample_ms(index * 100, last_update_id=100 + index) for index in range(30)]
    _write_jsonl(clean_path, samples)
    _write_jsonl(book_path, [_ref_ms("bookTicker_mid", index * 100, event_id=1000 + index) for index in range(32)])
    _write_jsonl(trade_path, [_ref_ms("trade_price", index * 100, event_id=2000 + index) for index in range(32)])
    _write_jsonl(agg_path, [_ref_ms("aggTrade_price", index * 100, event_id=3000 + index) for index in range(32)])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42c_reference_feed_benchmark.py"),
            "--skip-capture",
            "--allow-fixture-mode",
            "--skip-pytest",
            "--root",
            str(tmp_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    self_check = json.loads((tmp_path / "data/reports/phase42c_self_check.json").read_text())
    assert self_check["passed"] is True
    assert self_check["bundle_created"] is True
    assert (tmp_path / "phase_4_2c_reference_feed_benchmark_bundle.zip").exists()
