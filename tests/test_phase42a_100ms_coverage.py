from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from app.research.orderbook_100ms_coverage import (
    PHASE42A_REQUIRED_BUNDLE_FILES,
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    analyze_sample_gap_distribution,
    build_phase42a_report,
    classify_phase42a_failure,
    create_phase42a_bundle,
    evaluate_phase42a_report,
    extract_horizon_100ms_coverage,
    validate_phase42a_report_schema,
    write_phase42a_artifacts,
)
from app.research.orderbook_labeled_dataset import (
    MAX_FUTURE_GAP_MS,
    generate_labeled_rows,
    run_leakage_check,
)


ROOT = Path(__file__).resolve().parents[1]


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample_ns(ts_ns: int, *, last_update_id: int = 100) -> dict[str, object]:
    best_bid = 100.0 + (last_update_id / 10_000.0)
    best_ask = best_bid + 1.0
    return {
        "schema_version": "phase_4_1_clean_orderbook_v1",
        "symbol": "BTCUSDT",
        "source": "binance_ws",
        "generation_id": 99,
        "state_version": last_update_id,
        "snapshot_version": last_update_id,
        "last_update_id": last_update_id,
        "local_recv_monotonic_ns": ts_ns,
        "local_recv_wall_ts": "2026-05-20T00:00:00.000000+00:00",
        "exchange_event_ts": 1_779_213_534_814_000_000 + ts_ns,
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


def _sample_ms(ts_ms: int, *, last_update_id: int = 100) -> dict[str, object]:
    return _sample_ns(ts_ms * 1_000_000, last_update_id=last_update_id)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _passing_report() -> dict[str, object]:
    samples = [_sample_ms(index * 100, last_update_id=100 + index) for index in range(80)]
    labeled = generate_labeled_rows(samples)
    return build_phase42a_report(
        symbol="BTCUSDT",
        clean_samples=samples,
        labeled_rows=labeled,
        leakage_result=run_leakage_check(labeled),
        runtime_quality=_runtime_quality(),
        capture=_capture(sample_count=len(samples)),
        fresh_capture_required=False,
    )


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
        "snapshot_copy_p99_us": 10.0,
    }
    quality.update(overrides)
    return quality


def _capture(**overrides: object) -> dict[str, object]:
    capture: dict[str, object] = {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "duration_sec": 7.9,
        "sample_count": 80,
        "sample_rate_per_sec": 10.0,
        "downsampling_enabled": False,
        "emits_every_accepted_delta": True,
        "sample_source": "accepted_depth_delta",
    }
    capture.update(overrides)
    return capture


def test_phase42a_100ms_max_future_gap_is_hard_100ms() -> None:
    assert MAX_FUTURE_GAP_MS["horizon_100ms"] == 100
    assert REQUIRED_100MS_MAX_FUTURE_GAP_MS == 100


def test_phase42a_fails_if_100ms_max_future_gap_relaxed() -> None:
    report = _passing_report()
    report["horizon_100ms"]["max_future_gap_ms"] = 120  # type: ignore[index]

    evaluated = evaluate_phase42a_report(report)

    assert evaluated["status"] == "fail"
    assert classify_phase42a_failure(evaluated) == "HORIZON_100MS_POLICY_RELAXED"


def test_phase42a_does_not_allow_100ms_diagnostic_only() -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.94  # type: ignore[index]

    evaluated = evaluate_phase42a_report(report)

    assert evaluated["definition_of_done_status"] == "fail"
    assert classify_phase42a_failure(evaluated) == "LABEL_VALID_RATE_FAILURE"


def test_phase42a_valid_rate_boundaries() -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.95  # type: ignore[index]
    assert evaluate_phase42a_report(report)["definition_of_done_status"] == "pass"

    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.951  # type: ignore[index]
    assert evaluate_phase42a_report(report)["definition_of_done_status"] == "pass"

    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.949  # type: ignore[index]
    assert evaluate_phase42a_report(report)["definition_of_done_status"] == "fail"


def test_100ms_valid_rate_uses_eligible_rows_denominator() -> None:
    timestamps_ms = [index * 100 for index in range(90)] + list(range(8910, 9010, 10))
    samples = [
        _sample_ms(timestamp, last_update_id=100 + index)
        for index, timestamp in enumerate(timestamps_ms)
    ]
    labeled = generate_labeled_rows(samples)
    for index in range(86, 90):
        labeled[index]["labels"]["horizon_100ms"]["valid"] = False
        labeled[index]["labels"]["horizon_100ms"]["invalid_reason"] = "FUTURE_GAP_TOO_LARGE"
    coverage = extract_horizon_100ms_coverage(samples, labeled)

    assert coverage["eligible_count"] == 90
    assert coverage["valid_count"] == 86
    assert coverage["tail_no_future_count"] == 10
    assert coverage["valid_rate_eligible_rows"] == pytest.approx(86 / 90)
    assert coverage["valid_rate_all_rows"] == pytest.approx(86 / 100)


def test_100ms_future_gap_equal_100ms_is_valid() -> None:
    labeled = generate_labeled_rows([_sample_ms(0), _sample_ms(200, last_update_id=101)])

    label = labeled[0]["labels"]["horizon_100ms"]
    assert label["valid"] is True
    assert label["future_gap_ms"] == 100.0


def test_100ms_future_gap_above_100ms_is_invalid() -> None:
    labeled = generate_labeled_rows([_sample_ms(0), _sample_ns(200_001_000, last_update_id=101)])

    label = labeled[0]["labels"]["horizon_100ms"]
    assert label["valid"] is False
    assert label["invalid_reason"] == "FUTURE_GAP_TOO_LARGE"


def test_100ms_future_gap_distribution_percentiles_and_invalid_cases(tmp_path: Path) -> None:
    samples = [_sample_ms(0), _sample_ms(200), _sample_ns(400_001_000)]
    labeled = generate_labeled_rows(samples)
    coverage = extract_horizon_100ms_coverage(
        samples,
        labeled,
        invalid_cases_path=tmp_path / "invalid.jsonl",
    )

    assert {"future_gap_ms_p50", "future_gap_ms_p90", "future_gap_ms_p95", "future_gap_ms_p99", "future_gap_ms_max"} <= set(coverage)
    assert (tmp_path / "invalid.jsonl").read_text(encoding="utf-8").strip()


def test_sample_gap_distribution_computed_and_boundaries() -> None:
    samples = [_sample_ms(value) for value in (0, 100, 200, 400)]

    gaps = analyze_sample_gap_distribution(samples)

    assert gaps["gap_count"] == 3
    assert gaps["gap_p50_ms"] == 100.0
    assert gaps["gap_p95_ms"] == 200.0
    assert gaps["gap_p99_ms"] == 200.0
    assert gaps["gap_max_ms"] == 200.0


def test_phase42a_gap_and_timestamp_failures_are_classified() -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 1.0  # type: ignore[index]
    report["timestamp_quality"]["gap_p95_ms"] = 101.0  # type: ignore[index]
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "SAMPLE_GAP_P95_FAILURE"

    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 1.0  # type: ignore[index]
    report["timestamp_quality"]["gap_p99_ms"] = 201.0  # type: ignore[index]
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "SAMPLE_GAP_P99_FAILURE"

    report = _passing_report()
    report["timestamp_quality"]["duplicate_timestamp_count"] = 1  # type: ignore[index]
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "DUPLICATE_TIMESTAMP_FAILURE"

    report = _passing_report()
    report["timestamp_quality"]["timestamp_monotonic_violations"] = 1  # type: ignore[index]
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "TIMESTAMP_MONOTONIC_FAILURE"


def test_phase42a_capture_protocol_and_runtime_invariant_failures() -> None:
    report = _passing_report()
    report["capture"]["downsampling_enabled"] = True  # type: ignore[index]
    assert evaluate_phase42a_report(report)["definition_of_done_status"] == "fail"

    report = _passing_report()
    report["capture"]["emits_every_accepted_delta"] = False  # type: ignore[index]
    assert evaluate_phase42a_report(report)["definition_of_done_status"] == "fail"

    report = build_phase42a_report(
        symbol="BTCUSDT",
        clean_samples=[_sample_ms(index * 100) for index in range(20)],
        labeled_rows=generate_labeled_rows([_sample_ms(index * 100) for index in range(20)]),
        leakage_result={"passed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0},
        runtime_quality=_runtime_quality(sample_before_ready_count=1),
        capture=_capture(sample_count=20),
        fresh_capture_required=False,
    )
    assert report["runtime_status"] == "fail"
    assert report["definition_of_done_status"] == "fail"
    assert classify_phase42a_failure(report) == "RUNTIME_QUALITY_FAILURE"


def test_phase42a_status_separation_primary_failure_and_schema() -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.94  # type: ignore[index]
    evaluated = evaluate_phase42a_report(report)

    assert evaluated["implementation_status"] == "pass"
    assert evaluated["dataset_coverage_status"] == "fail"
    assert evaluated["definition_of_done_status"] == "fail"
    assert evaluated["primary_failure"] == "horizon_100ms_valid_rate_below_threshold"
    assert not validate_phase42a_report_schema(evaluated)

    broken = dict(evaluated)
    broken.pop("horizon_100ms")
    assert "missing required field: horizon_100ms" in validate_phase42a_report_schema(broken)


def test_phase42a_fails_on_leakage() -> None:
    report = _passing_report()
    report["leakage_check"] = {"passed": False, "feature_leakage_violations": 1, "label_leakage_violations": 0}
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "FEATURE_LEAKAGE_FAILURE"

    report = _passing_report()
    report["leakage_check"] = {"passed": False, "feature_leakage_violations": 0, "label_leakage_violations": 1}
    assert classify_phase42a_failure(evaluate_phase42a_report(report)) == "LABEL_LEAKAGE_FAILURE"


def test_phase42a_writes_fail_artifacts_and_no_bundle(tmp_path: Path) -> None:
    report = _passing_report()
    report["horizon_100ms"]["valid_rate_eligible_rows"] = 0.94  # type: ignore[index]
    report = evaluate_phase42a_report(report)

    write_phase42a_artifacts(report, root=tmp_path, pytest_output="pytest ok\n")

    assert (tmp_path / "data/reports/phase_4_2a_100ms_coverage_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2a_100ms_coverage_report.md").exists()
    self_check = json.loads((tmp_path / "data/reports/phase42a_self_check.json").read_text())
    assert self_check["passed"] is False
    assert (tmp_path / "data/debug/phase42a_failure_investigation.md").exists()
    assert not (tmp_path / "phase_4_2a_100ms_coverage_pass_bundle.zip").exists()


def test_phase42a_creates_bundle_only_on_pass(tmp_path: Path) -> None:
    for relative in PHASE42A_REQUIRED_BUNDLE_FILES:
        if relative.endswith("/"):
            continue
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")

    report = _passing_report()
    report = evaluate_phase42a_report(report)
    bundle = create_phase42a_bundle(root=tmp_path, source_root=ROOT)

    assert report["definition_of_done_status"] == "pass"
    assert bundle.exists()
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
    for required in PHASE42A_REQUIRED_BUNDLE_FILES:
        assert required in names


def test_phase42a_self_check_skip_capture_failure_writes_artifacts(tmp_path: Path) -> None:
    input_path = tmp_path / "data/dataset/orderbook_clean_samples.jsonl"
    _write_jsonl(input_path, [_sample_ms(index * 250, last_update_id=100 + index) for index in range(30)])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42a_100ms_recapture.py"),
            "--skip-capture",
            "--input-clean-samples",
            str(input_path),
            "--root",
            str(tmp_path),
            "--skip-pytest",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode != 0
    assert (tmp_path / "data/reports/phase_4_2a_100ms_coverage_report.json").exists()
    self_check = json.loads((tmp_path / "data/reports/phase42a_self_check.json").read_text())
    assert self_check["passed"] is False
    assert self_check["failure_classification"] == "LABEL_VALID_RATE_FAILURE"
    assert not (tmp_path / "phase_4_2a_100ms_coverage_pass_bundle.zip").exists()
