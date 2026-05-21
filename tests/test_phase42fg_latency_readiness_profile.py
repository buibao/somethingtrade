from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import zipfile

import pytest

from app.core.events import BookLevel, DepthUpdate
from app.marketdata.orderbook_phase41 import build_latency_profile_sample
from app.marketdata.orderbook_state import OrderbookSnapshot
from app.research.clock_sync_receive_lag import build_server_time_sample, compute_clock_offset_summary
from app.research.latency_readiness_profile import (
    LATENCY_PROFILE_DATASETS_ZIP,
    PHASE42FG_FAIL_AUDIT_BUNDLE,
    PHASE42FG_PASS_BUNDLE,
    build_latency_stage_profile,
    build_phase42fg_report,
    build_queue_backpressure_report,
    cleanup_phase42fg_artifacts,
    compute_readiness_semantics,
    create_phase42fg_bundle,
    create_phase42fg_dataset_zip,
    evaluate_phase42fg_report,
    phase42fg_bundle_missing_files,
    validate_phase42fg_report_schema,
    write_phase42fg_artifacts,
)
from app.research.reference_feed_benchmark import REFERENCE_SOURCES
from app.research.time_protocol_benchmark import REQUIRED_100MS_MAX_FUTURE_GAP_MS


def _hybrid_metrics(rate: float, *, p95: float = 80.0, p99: float = 120.0, budget: int = 100) -> dict[str, object]:
    return {
        "horizon_ms": 100,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "feature_lag_budget_ms": budget,
        "future_receive_lag_hard_gate_used": False,
        "future_receive_lag_is_telemetry_only": True,
        "eligible_count": 100,
        "valid_count": int(rate * 100),
        "valid_rate_eligible_rows": rate,
        "corrected_feature_receive_lag_p95_ms": p95,
        "corrected_feature_receive_lag_p99_ms": p99,
        "cross_stream_receive_reorder_count": 0,
        "clock_sanity_violation_count": 0,
    }


def _sources(*, h100: float = 0.0, h250: float = 0.96, p95: float = 146.0, p99: float = 326.0) -> dict[str, dict[str, object]]:
    sources: dict[str, dict[str, object]] = {}
    for source in REFERENCE_SOURCES:
        supported = source == "depth_mid"
        sources[source] = {
            "source": source,
            "exchange_time_supported": supported,
            "exchange_timestamp_field_used": "E" if supported else None,
            "receive_time": {"valid_rate_eligible_rows": 0.80},
            "exchange_time": {
                "valid_rate_eligible_rows": 0.97 if supported else 0.0,
                "selection_time_basis": "exchange_ts",
            },
            "raw_receive_lag": {"feature_raw_receive_lag_p50_ms": 37_500.0},
            "corrected_receive_lag": {
                "feature_corrected_receive_lag_p50_ms": 146.0,
                "feature_corrected_receive_lag_p95_ms": p95,
                "feature_corrected_receive_lag_p99_ms": p99,
            },
            "corrected_hybrid": {
                "corrected_hybrid_25ms": _hybrid_metrics(0.0, budget=25, p95=p95, p99=p99),
                "corrected_hybrid_50ms": _hybrid_metrics(0.0, budget=50, p95=p95, p99=p99),
                "corrected_hybrid_100ms": _hybrid_metrics(h100, budget=100, p95=p95, p99=p99),
                "corrected_hybrid_250ms": _hybrid_metrics(h250 if supported else 0.0, budget=250, p95=p95, p99=p99),
            },
        }
    return sources


def _clock_summary() -> dict[str, object]:
    samples = [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=37_500.0,
            local_wall_after_response_ms=37_510.0,
            binance_server_time_ms=5.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=38_500.0,
            local_wall_after_response_ms=38_510.0,
            binance_server_time_ms=1005.0,
        ),
    ]
    return compute_clock_offset_summary(samples)


def _clock_sanity() -> dict[str, object]:
    return {
        "performed": True,
        "clock_sanity_valid": True,
        "clock_offset_drift_valid": True,
        "server_time_rtt_valid": True,
        "corrected_lag_sanity_valid": True,
    }


def _leakage() -> dict[str, object]:
    return {"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0}


def _queue_report(*, drops: int = 0) -> dict[str, object]:
    return {
        "performed": True,
        "queue_max_size": 3,
        "queue_capacity": 4096,
        "queue_depth_p50": 1.0,
        "queue_depth_p95": 2.0,
        "queue_depth_p99": 3.0,
        "queue_dropped_messages": drops,
        "queue_backpressure_events": 0,
        "queue_backpressure_detected": False,
        "queue_depth_near_capacity": False,
        "queue_put_block_count": 0,
        "queue_put_block_p95_ms": 0.0,
        "writer_flush_count": 0,
        "writer_flush_p95_ms": 0.0,
        "disk_write_on_hot_path": True,
        "debug_logging_on_hot_path": True,
        "batch_writer_enabled": False,
        "warnings": ["disk_write_on_hot_path_detected", "debug_logging_on_hot_path_detected"],
    }


def _latency_profile() -> dict[str, object]:
    return {
        "performed": True,
        "sample_count": 1,
        "metrics": {
            "parse_duration_ms": {"count": 1, "p95": 0.01, "p99": 0.01},
            "book_apply_duration_ms": {"count": 1, "p95": 0.02, "p99": 0.02},
            "file_write_duration_ms": {"count": 1, "p95": 0.03, "p99": 0.03},
            "end_to_end_local_hot_path_ms": {"count": 1, "p95": 0.10, "p99": 0.10},
        },
        "disk_write_on_hot_path": True,
        "debug_logging_on_hot_path": True,
        "batch_writer_enabled": False,
    }


def _phase41(*, sequence_gap_count: int = 0, drops: int = 0) -> dict[str, object]:
    return {
        "sequence_gap_count": sequence_gap_count,
        "queue": {
            "queue_max_size": 3,
            "queue_capacity": 4096,
            "queue_depth_p50": 1.0,
            "queue_depth_p95": 2.0,
            "queue_depth_p99": 3.0,
            "queue_dropped_messages": drops,
            "queue_backpressure_events": 0,
            "queue_put_block_count": 0,
            "queue_put_block_p95_ms": 0.0,
        },
    }


def _report(*, h100: float = 0.0, h250: float = 0.96, p95: float = 146.0, p99: float = 326.0, drops: int = 0, gaps: int = 0) -> dict[str, object]:
    return build_phase42fg_report(
        symbol="BTCUSDT",
        clean_samples=[{"ok": True}],
        sources=_sources(h100=h100, h250=h250, p95=p95, p99=p99),
        timestamp_schema={"performed": True, "status": "pass"},
        leakage_result=_leakage(),
        clock_offset_samples=[],
        clock_offset_summary=_clock_summary(),
        clock_sanity=_clock_sanity(),
        latency_profile=_latency_profile(),
        queue_report=_queue_report(drops=drops),
        phase41_report=_phase41(sequence_gap_count=gaps, drops=drops),
        capture={"duration_sec": 1800.0, "fresh_capture_performed": True, "fixture_mode": False, "skip_capture": False},
        cleanup_report={"cleanup_performed": True, "errors": []},
        gitignore_validation={"passed": True},
        pytest_passed=True,
        typecheck_passed=True,
        typecheck_summary="passed",
        fresh_capture_required=False,
        labeled_sample_count=1,
    )


def test_low_latency_ready_false_when_only_250ms_passes() -> None:
    report = _report(h100=0.0, h250=0.96, p95=146.0, p99=326.0)
    assert report["market_time_label_ready"] is True
    assert report["relaxed_250ms_observability_candidate"] is True
    assert report["strict_100ms_observability_ready"] is False
    assert report["low_latency_ready"] is False
    assert report["phase5_ready"] is False
    assert report["selected_operational_budget_ms"] is None
    assert report["status"] == "pass"


def test_corrected_hybrid_100ms_pass_sets_strict_ready_and_low_latency() -> None:
    report = _report(h100=0.97, h250=0.98, p95=80.0, p99=120.0)
    assert report["strict_100ms_observability_ready"] is True
    assert report["low_latency_ready"] is True
    assert report["phase5_ready"] is False
    assert report["selected_operational_budget_ms"] == 100
    assert report["selected_protocol_candidate"]["budget_ms"] == 100


def test_strict_ready_blocked_by_lag_queue_drop_and_sequence_gap() -> None:
    assert _report(h100=0.97, p95=101.0, p99=120.0)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=80.0, p99=201.0)["strict_100ms_observability_ready"] is False
    dropped = _report(h100=0.97, p95=80.0, p99=120.0, drops=1)
    assert dropped["strict_100ms_observability_ready"] is False
    assert "QUEUE_DROPPED_MESSAGES_FAILURE" in dropped["failure_classifications"]
    assert _report(h100=0.97, p95=80.0, p99=120.0, gaps=1)["strict_100ms_observability_ready"] is False


def test_readiness_invariants_are_hard_failures() -> None:
    report = _report(h100=0.0)
    report["low_latency_ready"] = True
    evaluated = evaluate_phase42fg_report(report)
    assert "READINESS_SEMANTICS_FAILURE" in evaluated["failure_classifications"]
    report = _report(h100=0.97, p95=80.0, p99=120.0)
    report["phase5_ready"] = True
    evaluated = evaluate_phase42fg_report(report)
    assert "PHASE5_READY_FORBIDDEN" in evaluated["failure_classifications"]


def test_latency_stage_profile_schema_and_calculations(tmp_path: Path) -> None:
    event = DepthUpdate(
        symbol="BTCUSDT",
        first_update_id=101,
        final_update_id=101,
        bids=[BookLevel(price=100.0, size=1.0)],
        asks=[],
        recv_monotonic_ns=1_000_000,
        ws_message_received_monotonic_ns=1_000_000,
        parse_start_monotonic_ns=1_100_000,
        parse_end_monotonic_ns=1_300_000,
        parse_done_monotonic_ns=1_300_000,
        exchange_event_ts=1_700_000_000_000,
    )
    snapshot = OrderbookSnapshot(
        symbol="BTCUSDT",
        snapshot_version=1,
        state_version=2,
        generation_id=1,
        bids_top_n=((Decimal("100"), Decimal("1")),),
        asks_top_n=((Decimal("101"), Decimal("1")),),
        bid_count=1,
        ask_count=1,
        best_bid=Decimal("100"),
        best_ask=Decimal("101"),
        spread=Decimal("1"),
        mid=Decimal("100.5"),
        last_update_id=101,
        last_book_update_monotonic_ns=2_000_000,
        local_recv_monotonic_ns=2_000_000,
    )
    row = build_latency_profile_sample(
        event=event,
        snapshot=snapshot,
        book_apply_start_monotonic_ns=2_000_000,
        book_apply_end_monotonic_ns=2_500_000,
        sample_build_start_monotonic_ns=2_600_000,
        sample_build_end_monotonic_ns=2_700_000,
        sample_emit_monotonic_ns=2_800_000,
        queue_put_monotonic_ns=1_400_000,
        queue_dequeue_monotonic_ns=1_900_000,
        queue_wait_ms=None,
        queue_size_at_enqueue=2,
        file_write_start_monotonic_ns=2_900_000,
        file_write_end_monotonic_ns=3_200_000,
    )
    assert row["metrics"]["parse_duration_ms"] == pytest.approx(0.2)
    assert row["metrics"]["book_apply_duration_ms"] == pytest.approx(0.5)
    assert row["metrics"]["file_write_duration_ms"] == pytest.approx(0.3)
    assert row["metrics"]["end_to_end_local_hot_path_ms"] == pytest.approx(2.2)
    assert "socket_recv_monotonic_ns" in row["stage_not_available"]
    path = tmp_path / "latency.jsonl"
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    profile = build_latency_stage_profile(path)
    assert profile["performed"] is True
    assert profile["sample_count"] == 1
    assert profile["metrics"]["parse_duration_ms"]["p95"] == pytest.approx(0.2)
    assert profile["unavailable_stages"]["socket_recv_monotonic_ns"] == "stage_not_available"


def test_queue_backpressure_metrics_and_hot_path_risks() -> None:
    profile = {"disk_write_on_hot_path": True, "debug_logging_on_hot_path": True, "batch_writer_enabled": False}
    queue = build_queue_backpressure_report(phase41_report=_phase41(), latency_profile=profile)
    assert queue["queue_depth_p95"] == 2.0
    assert queue["queue_dropped_messages"] == 0
    assert queue["disk_write_on_hot_path"] is True
    assert queue["debug_logging_on_hot_path"] is True
    assert queue["batch_writer_enabled"] is False


def test_exchange_time_safety_and_100ms_policy() -> None:
    report = _report(h100=0.97, p95=80.0, p99=120.0)
    assert report["max_future_gap_ms"] == 100
    assert report["future_receive_lag_hard_gate_used"] is False
    assert report["sources"]["bookTicker_mid"]["exchange_time_supported"] is False
    report["sources"]["depth_mid"]["exchange_time"]["selection_time_basis"] = "local_recv_monotonic_ns"
    evaluated = evaluate_phase42fg_report(report)
    assert "EXCHANGE_TIME_FAKE_TIMESTAMP" in evaluated["failure_classifications"]


def test_report_schema_and_artifacts_written(tmp_path: Path) -> None:
    report = _report(h100=0.0)
    assert validate_phase42fg_report_schema(report) == []
    write_phase42fg_artifacts(report, root=tmp_path, pytest_output="pytest ok", bundle_created=False)
    assert (tmp_path / "data/reports/phase_4_2fg_latency_readiness_profile_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2fg_latency_readiness_profile_report.md").exists()
    assert (tmp_path / "data/reports/phase42fg_self_check.json").exists()
    assert (tmp_path / "data/debug/phase_4_2fg_latency_stage_profile.json").exists()
    assert (tmp_path / "data/debug/phase_4_2fg_queue_backpressure_report.json").exists()


def test_pass_and_fail_audit_bundles(tmp_path: Path) -> None:
    report = _report(h100=0.0)
    write_phase42fg_artifacts(report, root=tmp_path, pytest_output="pytest ok", bundle_created=True)
    (tmp_path / LATENCY_PROFILE_DATASETS_ZIP).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(tmp_path / LATENCY_PROFILE_DATASETS_ZIP, "w") as archive:
        archive.writestr("data/dataset/phase_4_2fg_latency_profile_samples.jsonl", "{}\n")
    pass_bundle = create_phase42fg_bundle(root=tmp_path, pass_bundle=True)
    assert pass_bundle.name == PHASE42FG_PASS_BUNDLE.name
    assert phase42fg_bundle_missing_files(pass_bundle, pass_bundle=True) == []

    failing = _report(h100=0.97, p95=80.0, p99=120.0, drops=1)
    write_phase42fg_artifacts(failing, root=tmp_path, pytest_output="pytest ok", bundle_created=True)
    fail_bundle = create_phase42fg_bundle(root=tmp_path, pass_bundle=False)
    assert fail_bundle.name == PHASE42FG_FAIL_AUDIT_BUNDLE.name
    assert phase42fg_bundle_missing_files(fail_bundle, pass_bundle=False) == []


def test_dataset_zip_and_cleanup(tmp_path: Path) -> None:
    dataset = tmp_path / "data/dataset/phase_4_2fg_latency_profile_samples.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")
    bundle = create_phase42fg_dataset_zip(tmp_path)
    assert bundle.exists()
    cleanup = cleanup_phase42fg_artifacts(tmp_path)
    assert cleanup["cleanup_performed"] is True
    assert cleanup["errors"] == []

