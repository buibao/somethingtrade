from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from app.research.bookticker_reference import (
    PHASE42B_REQUIRED_BUNDLE_FILES,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    analyze_reference_feed_quality,
    build_phase42b_report,
    classify_phase42b_failure,
    create_phase42b_bundle,
    evaluate_phase42b_report,
    generate_labeled_rows_with_bookticker,
    parse_bookticker_payload,
    run_bookticker_leakage_check,
    select_future_reference_index,
    validate_phase42b_report_schema,
    validate_reference_quotes,
    write_phase42b_artifacts,
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
        "generation_id": 77,
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


def _ref_ms(
    ts_ms: float,
    *,
    update_id: int = 1000,
    bid: float = 100.5,
    ask: float = 101.5,
    valid: bool = True,
) -> dict[str, object]:
    mid = (bid + ask) / 2
    spread = ask - bid
    return {
        "schema_version": "bookticker_reference_v1",
        "symbol": "BTCUSDT",
        "source": "binance_ws_bookTicker",
        "local_recv_monotonic_ns": int(ts_ms * 1_000_000),
        "local_recv_wall_ts": "2026-05-20T00:00:00.000000+00:00",
        "exchange_event_ts": None,
        "update_id": update_id,
        "best_bid": bid,
        "best_bid_qty": 1.0,
        "best_ask": ask,
        "best_ask_qty": 1.0,
        "mid_price": mid,
        "spread": spread,
        "spread_bps": spread / mid * 10_000,
        "quality": {"valid": valid, "errors": [], "warnings": []},
    }


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
    }
    quality.update(overrides)
    return quality


def _capture(**overrides: object) -> dict[str, object]:
    capture: dict[str, object] = {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "duration_sec": 10.0,
        "depth_stream": "btcusdt@depth@100ms",
        "reference_stream": "btcusdt@bookTicker",
        "downsampling_enabled": False,
    }
    capture.update(overrides)
    return capture


def _passing_report() -> dict[str, object]:
    samples = [_sample_ms(index * 100, last_update_id=100 + index) for index in range(30)]
    references = [_ref_ms(index * 100, update_id=1000 + index, bid=100.5 + index / 100, ask=101.5 + index / 100) for index in range(32)]
    labeled = generate_labeled_rows_with_bookticker(samples, references)
    leakage = run_bookticker_leakage_check(labeled)
    return build_phase42b_report(
        symbol="BTCUSDT",
        clean_samples=samples,
        reference_quotes=references,
        labeled_rows=labeled,
        leakage_result=leakage,
        depth_runtime_quality=_runtime_quality(),
        capture=_capture(),
        fresh_capture_required=False,
    )


def test_bookticker_parse_valid_payload() -> None:
    row = parse_bookticker_payload(
        {"u": 400900217, "s": "BNBUSDT", "b": "25.35190000", "B": "31.21000000", "a": "25.36520000", "A": "40.66000000"},
        local_recv_monotonic_ns=123,
        local_recv_wall_ts="2026-05-20T00:00:00+00:00",
    )

    assert row["update_id"] == 400900217
    assert row["symbol"] == "BNBUSDT"
    assert row["best_bid"] == 25.3519
    assert row["best_ask"] == 25.3652
    assert row["best_bid_qty"] == 31.21
    assert row["best_ask_qty"] == 40.66
    assert row["mid_price"] == pytest.approx((25.3519 + 25.3652) / 2)
    assert row["quality"]["valid"] is True


def test_bookticker_parse_invalid_payloads_and_zero_qty_policy() -> None:
    base = {"u": 1, "s": "BTCUSDT", "b": "100", "B": "1", "a": "101", "A": "1"}
    assert "MISSING_BID" in parse_bookticker_payload({**base, "b": None}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "MISSING_ASK" in parse_bookticker_payload({**base, "a": None}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "INVALID_BID" in parse_bookticker_payload({**base, "b": "bad"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "NEGATIVE_QTY" in parse_bookticker_payload({**base, "B": "-1"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    assert "CROSSED_QUOTE" in parse_bookticker_payload({**base, "b": "102"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")["quality"]["errors"]
    zero = parse_bookticker_payload({**base, "B": "0"}, local_recv_monotonic_ns=1, local_recv_wall_ts="w")
    assert zero["quality"]["valid"] is True
    assert "ZERO_QTY" in zero["quality"]["warnings"]


def test_reference_quote_schema_and_quality(tmp_path: Path) -> None:
    path = tmp_path / "refs.jsonl"
    valid = _ref_ms(0)
    missing_ts = dict(valid)
    missing_ts.pop("local_recv_monotonic_ns")
    missing_wall = dict(valid)
    missing_wall.pop("local_recv_wall_ts")
    missing_update = dict(valid)
    missing_update.pop("update_id")
    _write_jsonl(path, [valid, missing_ts, missing_wall, missing_update])

    result = validate_reference_quotes(path, invalid_output_path=tmp_path / "invalid.jsonl")

    assert result.reference_quote_count == 4
    assert result.valid_reference_quote_count == 1
    assert result.invalid_reference_quote_count == 3
    assert result.valid_quotes[0]["mid_price"] == pytest.approx(101.0)
    assert result.valid_quotes[0]["spread_bps"] == pytest.approx((1 / 101.0) * 10_000)
    assert (tmp_path / "invalid.jsonl").read_text(encoding="utf-8").strip()


def test_reference_timestamp_quality_and_gap_gate() -> None:
    quality = analyze_reference_feed_quality([_ref_ms(0, update_id=1), _ref_ms(10, update_id=1), _ref_ms(50, update_id=3)])
    assert quality["duplicate_update_id_count"] == 1
    assert quality["non_monotonic_reference_timestamp_count"] == 0
    assert {"reference_gap_p50_ms", "reference_gap_p90_ms", "reference_gap_p95_ms", "reference_gap_p99_ms", "reference_gap_max_ms"} <= set(quality)

    bad = analyze_reference_feed_quality([_ref_ms(100), _ref_ms(90, update_id=2)])
    assert bad["non_monotonic_reference_timestamp_count"] == 1

    report = _passing_report()
    report["reference_feed_quality"]["reference_gap_p95_ms"] = 101.0  # type: ignore[index]
    assert classify_phase42b_failure(evaluate_phase42b_report(report)) == "REFERENCE_FEED_GAP_FAILURE"


def test_bookticker_future_reference_selection_rules() -> None:
    refs = [_ref_ms(1099), _ref_ms(1100, update_id=2), _ref_ms(1101, update_id=3)]
    assert select_future_reference_index(refs, feature_timestamp_ns=1_000_000_000, horizon_ms=100) == 1

    refs = [_ref_ms(1099), _ref_ms(1120, update_id=2)]
    assert select_future_reference_index(refs, feature_timestamp_ns=1_000_000_000, horizon_ms=100) == 1

    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [_ref_ms(1099)])
    assert labeled[0]["labels"]["horizon_100ms"]["invalid_reason"] == "NO_FUTURE_REFERENCE"

    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [_ref_ms(1200.001)])
    assert labeled[0]["labels"]["horizon_100ms"]["invalid_reason"] == "FUTURE_REFERENCE_GAP_TOO_LARGE"

    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [_ref_ms(1200)])
    assert labeled[0]["labels"]["horizon_100ms"]["valid"] is True
    assert labeled[0]["labels"]["horizon_100ms"]["future_gap_ms"] == 100.0


def test_bookticker_reference_label_calculation_and_alignment() -> None:
    sample = _sample_ms(1000, last_update_id=10)
    future = _ref_ms(1100, update_id=42, bid=102, ask=104)
    labeled = generate_labeled_rows_with_bookticker([sample], [future])
    row = labeled[0]
    label = row["labels"]["horizon_100ms"]

    assert row["generation_id"] == sample["generation_id"]
    assert row["last_update_id"] == sample["last_update_id"]
    assert row["local_recv_monotonic_ns"] == sample["local_recv_monotonic_ns"]
    assert row["best_bid"] == pytest.approx(100.001)
    assert row["features"]["best_bid"] == pytest.approx(100.001)
    assert label["reference_source"] == "bookTicker"
    assert label["future_reference_update_id"] == 42
    assert label["return_bps"] == pytest.approx(((103.0 - row["mid_price"]) / row["mid_price"]) * 10_000)
    assert label["direction"] == 1
    assert label["spread_adjusted_direction"] == 1


def test_bookticker_reference_invalid_current_and_future_mid() -> None:
    sample = _sample_ms(1000)
    sample["best_bid"] = "0"
    labeled = generate_labeled_rows_with_bookticker([sample], [_ref_ms(1100)])
    assert labeled[0]["labels"]["horizon_100ms"]["invalid_reason"] == "CURRENT_MID_INVALID"

    ref = _ref_ms(1100)
    ref["mid_price"] = None
    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [ref])
    assert labeled[0]["labels"]["horizon_100ms"]["invalid_reason"] == "FUTURE_REFERENCE_MID_INVALID"


def test_bookticker_leakage_checks() -> None:
    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [_ref_ms(1100)])
    assert run_bookticker_leakage_check(labeled)["passed"] is True

    labeled[0]["labels"]["horizon_100ms"]["future_reference_local_recv_monotonic_ns"] = 1_099_000_000
    assert classify_phase42b_failure(build_phase42b_report(
        symbol="BTCUSDT",
        clean_samples=[_sample_ms(1000)],
        reference_quotes=[_ref_ms(1100)],
        labeled_rows=labeled,
        leakage_result=run_bookticker_leakage_check(labeled),
        depth_runtime_quality=_runtime_quality(),
        capture=_capture(),
        fresh_capture_required=False,
    )) == "LABEL_LEAKAGE_FAILURE"

    labeled = generate_labeled_rows_with_bookticker([_sample_ms(1000)], [_ref_ms(1100)])
    labeled[0]["quality"]["feature_source_indices"]["past_mid_return_100ms_bps"] = 1
    assert run_bookticker_leakage_check(labeled)["feature_leakage_violations"] == 1


def test_phase42b_hard_policy_and_status_separation() -> None:
    assert REQUIRED_100MS_MAX_FUTURE_GAP_MS == 100
    report = _passing_report()
    assert report["definition_of_done_status"] == "pass"

    report["horizon_100ms"]["max_future_gap_ms"] = 120  # type: ignore[index]
    assert classify_phase42b_failure(evaluate_phase42b_report(report)) == "HORIZON_100MS_POLICY_RELAXED"

    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.94  # type: ignore[index]
    evaluated = evaluate_phase42b_report(report)
    assert evaluated["implementation_status"] == "pass"
    assert evaluated["dataset_coverage_status"] == "fail"
    assert evaluated["definition_of_done_status"] == "fail"
    assert classify_phase42b_failure(evaluated) == "LABEL_VALID_RATE_FAILURE"

    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.95  # type: ignore[index]
    assert evaluate_phase42b_report(report)["definition_of_done_status"] == "pass"


def test_phase42b_report_schema_and_reference_failure() -> None:
    report = _passing_report()
    assert not validate_phase42b_report_schema(report)
    assert {"implementation_status", "runtime_status", "reference_feed_status", "dataset_coverage_status", "definition_of_done_status", "primary_failure"} <= set(report)

    broken = dict(report)
    broken.pop("reference_feed_quality")
    assert "missing required field: reference_feed_quality" in validate_phase42b_report_schema(broken)

    report = _passing_report()
    report["reference_feed_quality"]["valid_reference_quote_count"] = 0  # type: ignore[index]
    evaluated = evaluate_phase42b_report(report)
    assert evaluated["reference_feed_status"] == "fail"
    assert evaluated["definition_of_done_status"] == "fail"


def test_phase42b_fail_artifacts_and_bundle_rules(tmp_path: Path) -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.94  # type: ignore[index]
    report = evaluate_phase42b_report(report)

    write_phase42b_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")

    assert (tmp_path / "data/reports/phase_4_2b_bookticker_reference_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2b_bookticker_reference_report.md").exists()
    self_check = json.loads((tmp_path / "data/reports/phase42b_self_check.json").read_text())
    assert self_check["passed"] is False
    assert (tmp_path / "data/debug/phase42b_failure_investigation.md").exists()
    assert not (tmp_path / "phase_4_2b_bookticker_reference_pass_bundle.zip").exists()

    for relative in PHASE42B_REQUIRED_BUNDLE_FILES:
        if relative.endswith("/"):
            continue
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    bundle = create_phase42b_bundle(root=tmp_path, source_root=ROOT)
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
    for required in PHASE42B_REQUIRED_BUNDLE_FILES:
        assert required in names


def test_phase42b_self_check_skip_capture_fixture(tmp_path: Path) -> None:
    clean_path = tmp_path / "data/dataset/orderbook_clean_samples.jsonl"
    ref_path = tmp_path / "data/dataset/bookticker_reference_quotes.jsonl"
    _write_jsonl(clean_path, [_sample_ms(index * 100, last_update_id=100 + index) for index in range(30)])
    _write_jsonl(ref_path, [_ref_ms(index * 100, update_id=1000 + index) for index in range(32)])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42b_bookticker_reference.py"),
            "--skip-capture",
            "--skip-pytest",
            "--input-clean-samples",
            str(clean_path),
            "--input-reference-quotes",
            str(ref_path),
            "--root",
            str(tmp_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    self_check = json.loads((tmp_path / "data/reports/phase42b_self_check.json").read_text())
    assert self_check["passed"] is True
    assert (tmp_path / "phase_4_2b_bookticker_reference_pass_bundle.zip").exists()

