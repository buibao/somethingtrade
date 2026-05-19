from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from app.marketdata.orderbook_phase41 import (
    PHASE_4_1_SCHEMA_VERSION,
    clean_sample_from_snapshot,
    validate_clean_sample_schema,
)
from app.marketdata.orderbook_quality import OrderbookQualityValidator
from orderbook_phase41_test_utils import FakeMonotonicClock, make_depth_update, make_processor, make_state


def _checker_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "check_phase41_report.py"
    spec = importlib.util.spec_from_file_location("check_phase41_report", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _self_check_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_phase41_self_check.py"
    spec = importlib.util.spec_from_file_location("run_phase41_self_check", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _valid_report() -> dict:
    return {
        "phase": "4.1.1",
        "symbol": "BTCUSDT",
        "duration_sec": 120,
        "phase_4_1_status": "pass",
        "status": "pass",
        "sample_before_ready_count": 0,
        "feed_receive_stale_count": 0,
        "sequence_gap_count": 0,
        "invalid_delta_count": 0,
        "previous_final_update_id_mismatch_count": 0,
        "crossed_book_count": 0,
        "book_empty_count": 0,
        "one_side_missing_count": 0,
        "clean_sample_schema_violation_count": 0,
        "snapshot_copy_p99_us": 50,
        "post_capture_age_warning_count": 0,
        "market_status_mode": "not_applicable_for_binance_spot_orderbook",
        "queue": {
            "queue_dropped_messages": 0,
            "enqueue_to_dequeue_lag_p99_ms": 20,
            "queue_size_backpressure_events": 0,
            "queue_lag_backpressure_events": 0,
            "processing_lag_backpressure_events": 0,
            "snapshot_blocking_lag_events": 0,
            "processing_lag_p99_ms": 5,
        },
        "lifecycle": {
            "snapshot_loaded_count": 1,
            "snapshot_refresh_count": 1,
            "feed_receive_stale_count": 0,
            "processor_apply_stale_count": 0,
            "post_capture_age_warning_count": 0,
            "stale_reset_count": 0,
        },
    }


def _valid_sample(tmp_path) -> dict:
    make_processor(tmp_path)
    rows = (tmp_path / "orderbook_clean_samples.jsonl").read_text(encoding="utf-8").splitlines()
    return json.loads(rows[-1])


def test_clean_sample_has_non_null_generation_id_and_wall_timestamp(tmp_path) -> None:
    sample = _valid_sample(tmp_path)

    assert sample["schema_version"] == PHASE_4_1_SCHEMA_VERSION
    assert isinstance(sample["generation_id"], int)
    assert sample["generation_id"] is not None
    assert sample["local_recv_wall_ts"]
    assert validate_clean_sample_schema(sample) == []


def test_clean_sample_generation_id_matches_state_and_changes_after_refresh(tmp_path) -> None:
    processor = make_processor(tmp_path)
    state = processor.state_for("BTCUSDT")
    first = json.loads((tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()[-1])
    assert first["generation_id"] == state.generation

    state.mark_not_ready("test_snapshot_refresh", local_recv_monotonic_ns=1_010_000_000)
    processor.load_snapshot(
        "BTCUSDT",
        bids=[("100.00", "1.0")],
        asks=[("101.00", "1.0")],
        last_update_id=200,
        local_recv_monotonic_ns=1_011_000_000,
        recovery=True,
    )
    processor.process_depth_update(
        make_depth_update(first_update_id=201, final_update_id=201, recv_monotonic_ns=1_012_000_000)
    )
    second = json.loads((tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()[-1])

    assert second["generation_id"] == state.generation
    assert second["generation_id"] > first["generation_id"]


def test_apply_snapshot_preserves_wall_timestamp_for_debug() -> None:
    state = make_state()
    state.apply_snapshot(
        bids=[("100", "1")],
        asks=[("101", "1")],
        last_update_id=200,
        local_recv_monotonic_ns=1_010_000_000,
        local_recv_wall_ts="2026-05-20T00:00:00+00:00",
    )

    snapshot = state.copy_snapshot(top_n=20)

    assert snapshot.local_recv_wall_ts == "2026-05-20T00:00:00+00:00"


def test_wall_timestamp_not_used_for_stale_math(tmp_path) -> None:
    clock = FakeMonotonicClock(1_002_000_000)
    processor = make_processor(tmp_path, clock=clock, stale_after_ms=1_000)
    state = processor.state_for("BTCUSDT")
    state.last_local_recv_wall_ts = "1970-01-01T00:00:00+00:00"

    summary = processor.summary(duration_sec=1)

    assert summary["stale_book_count"] == 0


def test_validate_clean_sample_schema_rejects_required_hotfix_violations(tmp_path) -> None:
    sample = _valid_sample(tmp_path)

    cases = [
        ("generation_id", None, "generation_id_null"),
        ("local_recv_wall_ts", None, "wall_timestamp_missing"),
        ("local_recv_monotonic_ns", None, "local_recv_monotonic_ns_null"),
        ("bids", [], "bids_empty"),
        ("asks", [], "asks_empty"),
    ]
    for field, value, expected in cases:
        mutated = dict(sample)
        mutated[field] = value
        assert expected in validate_clean_sample_schema(mutated)


def test_validate_clean_sample_schema_rejects_orderbook_invariants(tmp_path) -> None:
    sample = _valid_sample(tmp_path)

    unsorted_bids = dict(sample, bids=[["99", "1"], ["100", "1"]])
    unsorted_asks = dict(sample, asks=[["102", "1"], ["101", "1"]])
    crossed = dict(sample, best_bid="101", best_ask="100")
    negative_size = dict(sample, bids=[["100", "-1"]])
    non_finite = dict(sample, bids=[["NaN", "1"]])

    assert "bids_unsorted" in validate_clean_sample_schema(unsorted_bids)
    assert "asks_unsorted" in validate_clean_sample_schema(unsorted_asks)
    assert "crossed_book" in validate_clean_sample_schema(crossed)
    assert "negative_size" in validate_clean_sample_schema(negative_size)
    assert "non_finite_price" in validate_clean_sample_schema(non_finite)


def test_validate_clean_sample_schema_rejects_quality_errors_and_not_ready_lifecycle(tmp_path) -> None:
    sample = _valid_sample(tmp_path)

    quality_error = dict(sample, quality={"is_valid": False, "errors": ["stale_book"], "warnings": []})
    not_ready = dict(
        sample,
        lifecycle={
            "snapshot_ready": True,
            "ready_to_emit": False,
            "sequence_continuous": True,
        },
    )

    assert "quality_errors_present" in validate_clean_sample_schema(quality_error)
    assert "not_ready_to_emit" in validate_clean_sample_schema(not_ready)


def test_schema_violation_blocks_emit_increments_counter_and_writes_debug(tmp_path, monkeypatch) -> None:
    import app.marketdata.orderbook_phase41 as phase41

    processor = make_processor(tmp_path)
    before_rows = (tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()
    original = phase41.clean_sample_from_snapshot

    def invalid_sample(*args, **kwargs):
        sample = original(*args, **kwargs)
        sample.pop("generation_id", None)
        return sample

    monkeypatch.setattr(phase41, "clean_sample_from_snapshot", invalid_sample)
    processor.process_depth_update(make_depth_update(first_update_id=102, final_update_id=102))
    summary = processor.summary(duration_sec=1)
    after_rows = (tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()
    violation_rows = (tmp_path / "clean_sample_schema_violation_cases.jsonl").read_text().splitlines()

    assert len(after_rows) == len(before_rows)
    assert summary["clean_sample_schema_violation_count"] == 1
    assert summary["phase_4_1_pass"] is False
    assert violation_rows
    assert "generation_id_missing" in json.loads(violation_rows[-1])["violations"]


def test_clean_sample_from_snapshot_carries_snapshot_generation_id() -> None:
    state = make_state()
    snapshot = state.copy_snapshot(top_n=20, local_recv_wall_ts="2026-05-20T00:00:00+00:00")
    quality = OrderbookQualityValidator().validate(snapshot, state=state, now_monotonic_ns=1_002_000_000)

    sample = clean_sample_from_snapshot(snapshot, quality, depth_n=20)

    assert sample["generation_id"] == snapshot.generation_id == state.generation
    assert validate_clean_sample_schema(sample) == []


def test_checker_main_exit_codes_and_schema_validation(tmp_path) -> None:
    checker = _checker_module()
    valid = tmp_path / "valid.json"
    valid.write_text(json.dumps(_valid_report()), encoding="utf-8")
    failed = tmp_path / "failed.json"
    failed_payload = _valid_report()
    failed_payload["sequence_gap_count"] = 1
    failed.write_text(json.dumps(failed_payload), encoding="utf-8")
    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text("{", encoding="utf-8")
    missing_fields = tmp_path / "missing.json"
    missing_fields.write_text(json.dumps({"phase": "4.1.1"}), encoding="utf-8")

    assert checker.main(["--gate", "2m", "--report", str(valid), "--output", str(tmp_path / "out.json")]) == 0
    assert checker.main(["--gate", "2m", "--report", str(failed), "--output", str(tmp_path / "fail.json")]) == 1
    assert checker.main(["--gate", "2m", "--report", str(tmp_path / "missing_file.json")]) == 2
    assert checker.main(["--gate", "2m", "--report", str(invalid_json)]) == 2
    assert checker.main(["--gate", "2m", "--report", str(missing_fields)]) == 2
    assert checker.main(["--gate", "9m", "--report", str(valid)]) == 3


def test_checker_requires_queue_lifecycle_and_decomposed_metrics() -> None:
    checker = _checker_module()
    missing_queue = _valid_report()
    missing_queue.pop("queue")
    legacy_queue_only = _valid_report()
    legacy_queue_only["queue"] = {
        "queue_dropped_messages": 0,
        "queue_backpressure_events": 0,
        "enqueue_to_dequeue_lag_p99_ms": 1,
    }
    missing_lifecycle = _valid_report()
    missing_lifecycle.pop("lifecycle")

    assert any("queue" in error for error in checker.validate_report_schema(missing_queue))
    assert any("queue_size_backpressure_events" in error for error in checker.validate_report_schema(legacy_queue_only))
    assert any("lifecycle" in error for error in checker.validate_report_schema(missing_lifecycle))


def test_console_log_encoding_check_passes_utf8_and_fails_nul_or_invalid_utf8(tmp_path) -> None:
    self_check = _self_check_module()
    valid = tmp_path / "valid.log"
    scratch = Path("data/debug")
    scratch.mkdir(parents=True, exist_ok=True)
    nul = scratch / "phase411_hotfix_nul_test.log"
    invalid = scratch / "phase411_hotfix_invalid_utf8_test.log"
    valid.write_text("ok\n", encoding="utf-8")
    try:
        nul.write_bytes(bytearray([111, 107, 0, 98, 97, 100]))
        invalid.write_bytes(bytearray([255, 254]))

        assert self_check.check_console_log_encoding(valid)[0] is True
        assert self_check.check_console_log_encoding(nul)[0] is False
        assert "LOG_ENCODING_FAILURE" in self_check.check_console_log_encoding(nul)[1]
        assert self_check.check_console_log_encoding(invalid)[0] is False
    finally:
        nul.unlink(missing_ok=True)
        invalid.unlink(missing_ok=True)


def test_self_check_retry_policy_only_retries_transient_failures() -> None:
    self_check = _self_check_module()

    assert self_check._should_retry(["NETWORK_UNAVAILABLE"], attempt=1, max_attempts=2) is True
    assert self_check._should_retry(["LOG_ENCODING_FAILURE"], attempt=1, max_attempts=2) is True
    assert self_check._should_retry(["REPORT_SCHEMA_FAILURE"], attempt=1, max_attempts=2) is False


def test_previous_final_update_id_validation_paths() -> None:
    state = make_state()
    accepted = state.apply_delta(
        first_update_id=102,
        final_update_id=105,
        previous_final_update_id=101,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=1_003_000_000,
    )
    assert accepted.accepted

    state = make_state()
    mismatch = state.apply_delta(
        first_update_id=102,
        final_update_id=105,
        previous_final_update_id=99,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=1_003_000_000,
    )
    assert mismatch.status == "previous_final_update_id_mismatch"
    assert state.ready_to_emit is False
    assert state.snapshot_ready is False

    state = make_state()
    spot = state.apply_delta(
        first_update_id=102,
        final_update_id=105,
        previous_final_update_id=None,
        bids=[],
        asks=[],
        local_recv_monotonic_ns=1_003_000_000,
    )
    assert spot.accepted


def test_previous_final_update_id_mismatch_is_report_hard_fail(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(
            first_update_id=102,
            final_update_id=105,
            previous_final_update_id=99,
        )
    )
    summary = processor.summary(duration_sec=1)

    assert summary["previous_final_update_id_mismatch_count"] == 1
    assert summary["phase_4_1_pass"] is False
    assert "previous_final_update_id_mismatch_count > 0" in summary["phase_4_1_failure_reasons"]
