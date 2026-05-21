from __future__ import annotations

import json
from pathlib import Path
import zipfile
from typing import Any

import pytest

from app.research.clock_sync_receive_lag import build_server_time_sample, compute_clock_offset_summary
from app.research.hotpath_environment_latency import (
    LATENCY_PROFILE_DATASETS_ZIP,
    PHASE42H_FAIL_AUDIT_BUNDLE,
    PHASE42H_PASS_BUNDLE,
    build_environment_metadata,
    build_latency_stage_profile,
    build_phase42h_report,
    build_queue_backpressure_report,
    build_writer_batch_report,
    compute_readiness_semantics,
    create_phase42h_bundle,
    create_phase42h_dataset_zip,
    evaluate_phase42h_report,
    phase42h_bundle_missing_files,
    validate_phase42h_report_schema,
    write_phase42h_artifacts,
)
from app.research.reference_feed_benchmark import REFERENCE_SOURCES
from app.research.time_protocol_benchmark import (
    REQUIRED_100MS_MAX_FUTURE_GAP_MS,
    build_exchange_time_label,
    validate_timestamp_schema,
)


def _hybrid_metrics(rate: float, *, p95: float = 80.0, p99: float = 120.0, budget: int = 100) -> dict[str, Any]:
    return {
        "horizon_ms": 100,
        "max_future_gap_ms": REQUIRED_100MS_MAX_FUTURE_GAP_MS,
        "feature_lag_budget_ms": budget,
        "future_receive_lag_hard_gate_used": False,
        "future_receive_lag_is_telemetry_only": True,
        "eligible_count": 100,
        "valid_count": int(rate * 100),
        "valid_rate_eligible_rows": rate,
        "corrected_feature_receive_lag_p50_ms": 75.0,
        "corrected_feature_receive_lag_p95_ms": p95,
        "corrected_feature_receive_lag_p99_ms": p99,
        "cross_stream_receive_reorder_count": 0,
        "clock_sanity_violation_count": 0,
    }


def _sources(*, h100: float = 0.0, h250: float = 0.96, p95: float = 146.0, p99: float = 326.0) -> dict[str, dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
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
                "feature_corrected_receive_lag_p50_ms": 134.0,
                "feature_corrected_receive_lag_p95_ms": p95,
                "feature_corrected_receive_lag_p99_ms": p99,
            },
            "corrected_hybrid": {
                "corrected_hybrid_25ms": _hybrid_metrics(0.0, budget=25, p95=p95, p99=p99),
                "corrected_hybrid_50ms": _hybrid_metrics(0.0, budget=50, p95=p95, p99=p99),
                "corrected_hybrid_100ms": _hybrid_metrics(h100 if supported else 0.0, budget=100, p95=p95, p99=p99),
                "corrected_hybrid_250ms": _hybrid_metrics(h250 if supported else 0.0, budget=250, p95=p95, p99=p99),
            },
        }
    return sources


def _clock_summary() -> dict[str, Any]:
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


def _clock_sanity() -> dict[str, Any]:
    return {
        "performed": True,
        "clock_sanity_valid": True,
        "clock_offset_drift_valid": True,
        "server_time_rtt_valid": True,
        "corrected_lag_sanity_valid": True,
    }


def _leakage() -> dict[str, Any]:
    return {"performed": True, "feature_leakage_violations": 0, "label_leakage_violations": 0}


def _latency_profile() -> dict[str, Any]:
    return {
        "performed": True,
        "sample_count": 1,
        "socket_recv_monotonic_ns": "stage_not_available",
        "earliest_available_receive_stage": "raw_ws_callback_monotonic_ns",
        "metrics": {
            "parse_duration_ms": {"count": 1, "p95": 0.01, "p99": 0.01},
            "book_apply_duration_ms": {"count": 1, "p95": 0.02, "p99": 0.02},
            "queue_put_duration_ms": {"count": 1, "p95": 0.01, "p99": 0.01},
            "file_write_duration_ms": {"count": 1, "p95": 0.03, "p99": 0.03},
            "end_to_end_local_hot_path_ms": {"count": 1, "p95": 0.10, "p99": 0.10},
        },
        "disk_write_on_hot_path": False,
        "debug_logging_on_hot_path": False,
        "batch_writer_enabled": True,
    }


def _writer_report(*, dropped: int = 0, errors: int = 0, flushed: bool = True, flush_p95: float = 1.0) -> dict[str, Any]:
    return {
        "writer_mode": "threaded_jsonl_batch_writer",
        "writer_batch_size": 512,
        "writer_flush_interval_ms": 100.0,
        "writer_queue_max_size": 65536,
        "writer_thread_or_task_count": 2,
        "writer_shutdown_flush_completed": flushed,
        "writer_dropped_records": dropped,
        "writer_error_count": errors,
        "writer_records_enqueued": 10,
        "writer_records_written": 10,
        "writer_flush_count": 2,
        "writer_flush_p50_ms": 0.5,
        "writer_flush_p95_ms": flush_p95,
        "writer_flush_p99_ms": flush_p95,
        "writer_flush_max_ms": flush_p95,
    }


def _queue_report(*, drops: int = 0, near_capacity: bool = False, put_p95: float = 0.0, writer: dict[str, Any] | None = None) -> dict[str, Any]:
    phase41 = {
        "queue": {
            "queue_capacity": 10,
            "queue_max_size": 9 if near_capacity else 3,
            "queue_depth_p50": 1.0,
            "queue_depth_p95": 2.0,
            "queue_depth_p99": 9.0 if near_capacity else 3.0,
            "queue_dropped_messages": drops,
            "queue_backpressure_events": 1 if near_capacity else 0,
            "queue_put_block_count": 1 if put_p95 > 0 else 0,
            "queue_put_block_p50_ms": 0.0,
            "queue_put_block_p95_ms": put_p95,
            "queue_put_block_p99_ms": put_p95,
        }
    }
    return build_queue_backpressure_report(
        phase41_report=phase41,
        latency_profile=_latency_profile(),
        writer_report=writer or _writer_report(),
    )


def _phase41(*, gaps: int = 0, writer: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "sequence_gap_count": gaps,
        "queue": {
            "queue_capacity": 10,
            "queue_max_size": 3,
            "queue_depth_p50": 1.0,
            "queue_depth_p95": 2.0,
            "queue_depth_p99": 3.0,
            "queue_dropped_messages": 0,
            "queue_backpressure_events": 0,
            "queue_put_block_count": 0,
            "queue_put_block_p50_ms": 0.0,
            "queue_put_block_p95_ms": 0.0,
            "queue_put_block_p99_ms": 0.0,
        },
        "writer_batch_report": writer or _writer_report(),
    }


def _report(
    *,
    h100: float = 0.0,
    h250: float = 0.96,
    p95: float = 146.0,
    p99: float = 326.0,
    queue_drops: int = 0,
    writer_drops: int = 0,
    writer_errors: int = 0,
    writer_flushed: bool = True,
    gaps: int = 0,
) -> dict[str, Any]:
    writer = _writer_report(dropped=writer_drops, errors=writer_errors, flushed=writer_flushed)
    return build_phase42h_report(
        symbol="BTCUSDT",
        clean_samples=[{"ok": True}],
        sources=_sources(h100=h100, h250=h250, p95=p95, p99=p99),
        timestamp_schema={"performed": True, "status": "pass"},
        leakage_result=_leakage(),
        clock_offset_samples=[],
        clock_offset_summary=_clock_summary(),
        clock_sanity=_clock_sanity(),
        latency_profile=_latency_profile(),
        queue_report=_queue_report(drops=queue_drops, writer=writer),
        writer_report=writer,
        phase41_report=_phase41(gaps=gaps, writer=writer),
        capture={"duration_sec": 1800.0, "fresh_capture_performed": True, "fixture_mode": False, "skip_capture": False},
        cleanup_report={"cleanup_performed": True, "errors": []},
        gitignore_validation={"passed": True},
        environment=build_environment_metadata(environment_name="local_vn", environment_region="VN-HCMC", machine_profile="test"),
        pytest_passed=True,
        typecheck_passed=True,
        typecheck_summary="passed",
        fresh_capture_required=False,
        labeled_sample_count=1,
    )


def test_hot_path_decoupling_flags_and_batch_writer_enabled() -> None:
    report = _report()
    assert report["hot_path_latency_summary"]["disk_write_on_hot_path"] is False
    assert report["hot_path_latency_summary"]["debug_logging_on_hot_path"] is False
    assert report["hot_path_latency_summary"]["batch_writer_enabled"] is True
    assert report["hot_path_decoupling_status"] == "pass"


def test_writer_shutdown_and_hard_fail_gates() -> None:
    assert _report()["writer_batch_report"]["writer_shutdown_flush_completed"] is True
    assert "WRITER_DROPPED_RECORDS_FAILURE" in _report(writer_drops=1)["failure_classifications"]
    assert "WRITER_ERROR_FAILURE" in _report(writer_errors=1)["failure_classifications"]
    assert "WRITER_SHUTDOWN_FLUSH_FAILURE" in _report(writer_flushed=False)["failure_classifications"]


def test_latency_stage_profile_schema_and_stage_calculations(tmp_path: Path) -> None:
    row = {
        "stages": {
            "socket_recv_monotonic_ns": None,
            "raw_ws_callback_monotonic_ns": 1_000_000,
            "ws_message_received_monotonic_ns": 1_000_000,
            "message_dispatch_start_monotonic_ns": 1_100_000,
            "parse_start_monotonic_ns": 1_200_000,
            "parse_end_monotonic_ns": 1_400_000,
            "book_apply_start_monotonic_ns": 1_500_000,
            "book_apply_end_monotonic_ns": 1_800_000,
            "sample_build_start_monotonic_ns": 1_900_000,
            "sample_emit_monotonic_ns": 2_100_000,
            "queue_put_start_monotonic_ns": 2_200_000,
            "queue_put_end_monotonic_ns": 2_230_000,
            "writer_enqueue_monotonic_ns": 2_240_000,
            "file_write_start_monotonic_ns": 2_500_000,
            "file_write_end_monotonic_ns": 2_800_000,
        },
        "metrics": {
            "callback_to_dispatch_ms": 0.1,
            "dispatch_to_parse_start_ms": 0.1,
            "parse_duration_ms": 0.2,
            "parse_to_apply_start_ms": 0.1,
            "book_apply_duration_ms": 0.3,
            "apply_to_sample_build_ms": 0.1,
            "sample_build_duration_ms": 0.2,
            "sample_emit_to_queue_put_start_ms": 0.1,
            "queue_put_duration_ms": 0.03,
            "queue_wait_ms": 0.2,
            "writer_wait_ms": 0.26,
            "file_write_duration_ms": 0.3,
            "end_to_end_local_hot_path_ms": 1.1,
        },
        "socket_recv_monotonic_ns": "stage_not_available",
        "earliest_available_receive_stage": "raw_ws_callback_monotonic_ns",
        "queue_size_at_enqueue": 2,
        "disk_write_on_hot_path": False,
        "debug_logging_on_hot_path": False,
        "batch_writer_enabled": True,
    }
    path = tmp_path / "latency.jsonl"
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    profile = build_latency_stage_profile(path)
    assert profile["performed"] is True
    assert profile["unavailable_stages"]["socket_recv_monotonic_ns"] == "stage_not_available"
    assert profile["socket_recv_monotonic_ns"] == "stage_not_available"
    assert profile["earliest_available_receive_stage"] == "raw_ws_callback_monotonic_ns"
    assert profile["metrics"]["parse_duration_ms"]["p95"] == pytest.approx(0.2)
    assert profile["metrics"]["book_apply_duration_ms"]["p95"] == pytest.approx(0.3)
    assert profile["metrics"]["queue_put_duration_ms"]["p95"] == pytest.approx(0.03)
    assert profile["metrics"]["file_write_duration_ms"]["p95"] == pytest.approx(0.3)


def test_queue_backpressure_report_schema_and_hard_fail() -> None:
    queue = _queue_report(near_capacity=True, put_p95=6.0, writer=_writer_report(flush_p95=60.0))
    for field in (
        "queue_max_size",
        "queue_depth_p50",
        "queue_depth_p95",
        "queue_depth_p99",
        "queue_depth_max",
        "queue_put_block_count",
        "queue_put_block_p50_ms",
        "queue_put_block_p95_ms",
        "queue_put_block_p99_ms",
        "queue_dropped_messages",
        "queue_backpressure_detected",
        "writer_flush_count",
        "writer_flush_p95_ms",
    ):
        assert field in queue
    assert "queue_depth_near_capacity" in queue["warnings"]
    assert "queue_put_block_p95_gt_5ms" in queue["warnings"]
    assert "writer_flush_p95_gt_50ms" in queue["warnings"]
    assert "QUEUE_DROPPED_MESSAGES_FAILURE" in _report(queue_drops=1)["failure_classifications"]


def test_readiness_semantics_keep_100ms_hard_and_phase5_false() -> None:
    report = _report(h100=0.0, h250=0.96)
    assert report["market_time_label_ready"] is True
    assert report["relaxed_250ms_observability_candidate"] is True
    assert report["strict_100ms_observability_ready"] is False
    assert report["low_latency_ready"] is False
    assert report["phase5_ready"] is False
    assert report["selected_operational_budget_ms"] is None
    assert report["status"] == "pass"


def test_strict_ready_requires_hybrid_100ms_feature_lag_queue_writer_and_sequence() -> None:
    assert _report(h100=0.97, h250=0.98, p95=80.0, p99=120.0)["strict_100ms_observability_ready"] is True
    assert _report(h100=0.94, h250=0.98, p95=80.0, p99=120.0)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=101.0, p99=120.0)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=80.0, p99=201.0)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=80.0, p99=120.0, queue_drops=1)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=80.0, p99=120.0, writer_drops=1)["strict_100ms_observability_ready"] is False
    assert _report(h100=0.97, p95=80.0, p99=120.0, gaps=1)["strict_100ms_observability_ready"] is False


def test_readiness_invariants_are_hard_failures() -> None:
    report = _report(h100=0.0)
    report["low_latency_ready"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "READINESS_SEMANTICS_FAILURE" in evaluated["failure_classifications"]
    report = _report(h100=0.97, p95=80.0, p99=120.0)
    report["phase5_ready"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "PHASE5_READY_FORBIDDEN" in evaluated["failure_classifications"]


def test_corrected_timing_continuity_and_exchange_time_policy() -> None:
    feature = {
        "symbol": "BTCUSDT",
        "exchange_event_ts": 1_000.0,
        "local_recv_monotonic_ns": 10_000,
        "local_recv_wall_ts": "1970-01-01T00:00:37.500+00:00",
        "mid_price": 100.0,
    }
    reference = {
        "exchange_event_ts": 1_100.0,
        "local_recv_monotonic_ns": 20_000,
        "local_recv_wall_ts": "1970-01-01T00:00:37.600+00:00",
        "event_id": 1,
        "price": 101.0,
        "mid_price": 101.0,
    }
    label = build_exchange_time_label(
        reference_source="depth_mid",
        feature_sample=feature,
        feature_mid_price=100.0,
        references=[reference],
        reference_exchange_timestamps_ms=[1_100.0],
        exchange_time_supported=True,
        unsupported_reason="",
    )
    assert label["selection_time_basis"] == "exchange_ts"
    assert label["max_future_gap_ms"] == 100
    assert label["future_receive_lag_ms"] is not None
    assert _report()["future_receive_lag_hard_gate_used"] is False
    schema = validate_timestamp_schema(
        [feature],
        {"depth_mid": [reference], "bookTicker_mid": [{"local_recv_monotonic_ns": 1, "mid_price": 1.0}], "trade_price": [], "aggTrade_price": []},
    )
    assert schema["sources"]["bookTicker_mid"]["exchange_time_supported"] is False


def test_environment_metadata_and_report_artifacts(tmp_path: Path) -> None:
    report = _report()
    assert validate_phase42h_report_schema(report) == []
    assert report["environment"]["name"] == "local_vn"
    assert report["environment"]["region"] == "VN-HCMC"
    write_phase42h_artifacts(report, root=tmp_path, pytest_output="pytest ok", bundle_created=False)
    assert (tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").exists()
    assert (tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.md").exists()
    assert (tmp_path / "data/reports/phase42h_self_check.json").exists()
    assert (tmp_path / "data/debug/phase_4_2h_latency_stage_profile.json").exists()
    assert (tmp_path / "data/debug/phase_4_2h_queue_backpressure_report.json").exists()
    assert (tmp_path / "data/debug/phase_4_2h_writer_batch_report.json").exists()


def test_pass_and_fail_audit_bundles_created_correctly(tmp_path: Path) -> None:
    passing = _report()
    write_phase42h_artifacts(passing, root=tmp_path, pytest_output="pytest ok", bundle_created=True)
    (tmp_path / LATENCY_PROFILE_DATASETS_ZIP).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(tmp_path / LATENCY_PROFILE_DATASETS_ZIP, "w") as archive:
        archive.writestr("data/dataset/phase_4_2h_latency_profile_samples.jsonl", "{}\n")
    pass_bundle = create_phase42h_bundle(root=tmp_path, pass_bundle=True)
    assert pass_bundle.name == PHASE42H_PASS_BUNDLE.name
    assert phase42h_bundle_missing_files(pass_bundle, pass_bundle=True) == []

    failing = _report(writer_drops=1)
    write_phase42h_artifacts(failing, root=tmp_path, pytest_output="pytest ok", bundle_created=True)
    fail_bundle = create_phase42h_bundle(root=tmp_path, pass_bundle=False)
    assert fail_bundle.name == PHASE42H_FAIL_AUDIT_BUNDLE.name
    assert phase42h_bundle_missing_files(fail_bundle, pass_bundle=False) == []


def test_dataset_zip_typecheck_placeholder_and_no_phase5_scope(tmp_path: Path) -> None:
    dataset = tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text("{}\n", encoding="utf-8")
    bundle = create_phase42h_dataset_zip(tmp_path)
    assert bundle.exists()
    write_phase42h_artifacts(_report(), root=tmp_path, pytest_output="pytest ok", bundle_created=False)
    assert (tmp_path / "data/debug/phase_4_2h_typecheck_report.txt").exists()
    module_text = Path("bot/app/research/hotpath_environment_latency.py").read_text(encoding="utf-8")
    script_text = Path("scripts/run_phase42h_hotpath_environment_latency.py").read_text(encoding="utf-8")
    forbidden = ("class ProbabilityModel", "PaperExecutor", "ExecutionReport(", "OrderIntent(")
    assert not any(token in module_text or token in script_text for token in forbidden)


def test_compute_readiness_semantics_direct_future_lag_telemetry_only() -> None:
    semantics = compute_readiness_semantics(
        sources=_sources(h100=0.97, h250=0.98, p95=80.0, p99=120.0),
        leakage_result=_leakage(),
        clock_sanity_report=_clock_sanity(),
        queue_report=_queue_report(),
        writer_report=_writer_report(),
        phase41_report=_phase41(),
    )
    assert semantics["strict_100ms_observability_ready"] is True
    for source_report in _sources().values():
        for metrics in source_report["corrected_hybrid"].values():
            assert metrics["future_receive_lag_hard_gate_used"] is False
            assert metrics["future_receive_lag_is_telemetry_only"] is True
