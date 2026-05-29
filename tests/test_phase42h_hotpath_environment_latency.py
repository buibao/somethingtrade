from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import gc
import hashlib
import json
from pathlib import Path
import time
import zipfile
from typing import Any

import pytest

import app.research.hotpath_environment_latency as hotpath
import app.research.phase42h_streaming as phase42h_streaming
import scripts.run_phase42h_hotpath_environment_latency as phase42h_cli
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
    cleanup_phase42h_artifacts,
    create_phase42h_bundle,
    create_phase42h_dataset_zip,
    evaluate_phase42h_report,
    phase42h_bundle_missing_files,
    phase41_runtime_report_status,
    resolve_phase41_runtime_report,
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
    metrics = {metric: {"count": 1, "p50": 0.01, "p95": 0.01, "p99": 0.01, "max": 0.01} for metric in hotpath.LATENCY_METRIC_NAMES}
    metrics["book_apply_duration_ms"] = {"count": 1, "p50": 0.02, "p95": 0.02, "p99": 0.02, "max": 0.02}
    metrics["file_write_duration_ms"] = {"count": 1, "p50": 0.03, "p95": 0.03, "p99": 0.03, "max": 0.03}
    metrics["end_to_end_local_hot_path_ms"] = {"count": 1, "p50": 0.10, "p95": 0.10, "p99": 0.10, "max": 0.10}
    stage_availability = {
        stage: {
            "available_count": 0 if stage == "socket_recv_monotonic_ns" else 1,
            "stage_not_available_count": 1 if stage == "socket_recv_monotonic_ns" else 0,
        }
        for stage in hotpath.REQUIRED_STAGE_NAMES
    }
    return {
        "schema_version": hotpath.PHASE42H_LATENCY_STAGE_PROFILE_SCHEMA_VERSION,
        "phase": "4.2H",
        "performed": True,
        "sample_count": 1,
        "stage_availability": stage_availability,
        "unavailable_stages": {"socket_recv_monotonic_ns": "stage_not_available"},
        "socket_recv_monotonic_ns": "stage_not_available",
        "earliest_available_receive_stage": "raw_ws_callback_monotonic_ns",
        "metrics": metrics,
        "missing_metrics": [],
        "queue_depth_from_latency_samples": {"count": 1, "p50": 1.0, "p95": 1.0, "p99": 1.0, "max": 1.0},
        "disk_write_on_hot_path": False,
        "debug_logging_on_hot_path": False,
        "batch_writer_enabled": True,
        "stage_profile_path": "data/dataset/phase_4_2h_latency_profile_samples.jsonl",
        "required_latency_profile_samples_path": "data/dataset/phase_4_2h_latency_profile_samples.jsonl",
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
            ]
        )
        + "\n",
        encoding="utf-8",
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
        "phase_4_1_pass": True,
        "phase_4_1_status": "pass",
        "phase_4_1_failure_reasons": [],
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
    phase41_report: dict[str, Any] | None = None,
    fresh_capture_required: bool = False,
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
        phase41_report=phase41_report if phase41_report is not None else _phase41(gaps=gaps, writer=writer),
        capture={"duration_sec": 1800.0, "fresh_capture_performed": True, "fixture_mode": False, "skip_capture": False},
        cleanup_report={"cleanup_performed": True, "errors": []},
        gitignore_validation={"passed": True},
        environment=build_environment_metadata(environment_name="local_vn", environment_region="VN-HCMC", machine_profile="test"),
        pytest_passed=True,
        typecheck_passed=True,
        typecheck_summary="passed",
        fresh_capture_required=fresh_capture_required,
        labeled_sample_count=1,
    )


def _fresh_phase42h_report() -> dict[str, Any]:
    report = _report(h100=0.97, h250=0.98, p95=80.0, p99=120.0, fresh_capture_required=False)
    report["phase41_runtime_report_source"] = {
        "source": "current_capture_summary",
        "path": "capture.capture_diagnostics.phase41_runtime_report",
        "fresh": True,
        "artifact_loaded": False,
    }
    report["phase41_runtime_report_status"] = "pass"
    report["capture"] = {**report["capture"], "fresh_capture_performed": True, "skip_capture": False, "fixture_mode": False}
    report["fresh_capture_performed"] = True
    report["fresh_capture_required"] = True
    report["skip_capture"] = False
    report["fixture_mode"] = False
    report["status"] = "pass"
    report["primary_failure"] = None
    report["failure_classifications"] = []
    report["hard_fail_reasons"] = []
    return report


def test_phase42h_fresh_capture_without_cleanup_fails() -> None:
    report = _fresh_phase42h_report()
    report["cleanup_report"] = {"cleanup_performed": False, "errors": []}
    evaluated = evaluate_phase42h_report(report)
    assert evaluated["status"] == "fail"
    assert evaluated["primary_failure"] == "ARTIFACT_CLEANUP_FAILURE"
    assert "ARTIFACT_CLEANUP_FAILURE" in evaluated["failure_classifications"]
    assert "artifact cleanup was not performed" in evaluated["hard_fail_reasons"]


def test_phase42h_fresh_capture_with_cleanup_passes_when_runtime_gates_pass() -> None:
    report = _fresh_phase42h_report()
    report["cleanup_report"] = {"cleanup_performed": True, "errors": []}
    evaluated = evaluate_phase42h_report(report)
    assert evaluated["status"] == "pass"
    assert evaluated["primary_failure"] is None
    assert evaluated["strict_100ms_observability_ready"] is True
    assert evaluated["phase5_ready"] is False


def test_phase42h_cleanup_error_fails() -> None:
    report = _fresh_phase42h_report()
    report["cleanup_report"] = {"cleanup_performed": True, "errors": ["permission denied"]}
    evaluated = evaluate_phase42h_report(report)
    assert evaluated["status"] == "fail"
    assert evaluated["primary_failure"] == "ARTIFACT_CLEANUP_FAILURE"
    assert "ARTIFACT_CLEANUP_FAILURE" in evaluated["failure_classifications"]


def test_phase42h_cleanup_report_schema_has_targets_and_timestamps(tmp_path: Path) -> None:
    report = cleanup_phase42h_artifacts(tmp_path, source_root=tmp_path)
    assert report["cleanup_performed"] is True
    assert "cleanup_targets" in report
    assert report["cleanup_targets"][0]["role"] == "actual_capture_write_location"
    assert report["cleanup_started_at_utc"]
    assert report["cleanup_finished_at_utc"]


def test_phase42h_cleanup_cleans_actual_source_write_location(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    session_root = tmp_path / "sessions/session_001"
    stale = source_root / "data/dataset/orderbook_clean_samples.jsonl"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("{}\n", encoding="utf-8")
    cleanup_phase42h_artifacts(session_root, source_root=source_root)
    assert not stale.exists()


def test_phase42h_cleanup_cleans_session_root_when_root_differs(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    session_root = tmp_path / "sessions/session_001"
    stale = session_root / "data/debug/phase_4_2h_latency_stage_profile.json"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("{}", encoding="utf-8")
    cleanup_phase42h_artifacts(session_root, source_root=source_root)
    assert not stale.exists()


def test_phase42h_cleanup_does_not_delete_source_code_or_tests(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    session_root = tmp_path / "sessions/session_001"
    source_file = source_root / "bot/app/research/keep.py"
    test_file = source_root / "tests/test_keep.py"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text("print('keep')\n", encoding="utf-8")
    test_file.write_text("def test_keep(): pass\n", encoding="utf-8")
    cleanup_phase42h_artifacts(session_root, source_root=source_root)
    assert source_file.exists()
    assert test_file.exists()


def test_phase42h_cleanup_does_not_delete_archived_failed_runs(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    archive = source_root / "data/phase_5_2_failed_before_cleanup_fix/phase_5_2_session_001_capture_bundle.zip"
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_bytes(b"archive")
    cleanup_phase42h_artifacts(source_root, source_root=source_root)
    assert archive.exists()


def test_preflight_fails_if_generated_jsonl_tracked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)

    def fake_run_git(root: Path, *args: str) -> dict[str, Any]:
        if args == ("rev-parse", "--is-inside-work-tree"):
            return {"returncode": 0, "stdout": "true\n", "stderr": ""}
        if args == ("ls-files",):
            return {"returncode": 0, "stdout": "data/dataset/orderbook_clean_samples.jsonl\n", "stderr": ""}
        if args == ("diff", "--name-only", "--cached"):
            return {"returncode": 0, "stdout": "", "stderr": ""}
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(hotpath, "_run_git", fake_run_git)
    report = hotpath.run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    assert report["passed"] is False
    assert report["checks"]["heavy_generated_artifacts_not_tracked_or_staged"]["passed"] is False
    assert "data/dataset/orderbook_clean_samples.jsonl" in report["checks"]["heavy_generated_artifacts_not_tracked_or_staged"]["tracked"]


def test_preflight_fails_if_generated_zip_staged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)

    def fake_run_git(root: Path, *args: str) -> dict[str, Any]:
        if args == ("rev-parse", "--is-inside-work-tree"):
            return {"returncode": 0, "stdout": "true\n", "stderr": ""}
        if args == ("ls-files",):
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if args == ("diff", "--name-only", "--cached"):
            return {"returncode": 0, "stdout": "phase_4_2h_hotpath_environment_latency_bundle.zip\n", "stderr": ""}
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(hotpath, "_run_git", fake_run_git)
    report = hotpath.run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    assert report["passed"] is False
    assert "phase_4_2h_hotpath_environment_latency_bundle.zip" in report["checks"]["heavy_generated_artifacts_not_tracked_or_staged"]["staged"]


def test_phase42h_preflight_uses_source_root_gitignore_when_root_is_session_dir(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    session_root = source_root / "data/phase_5_2/sessions/session_001_sanity_30m"
    session_root.mkdir(parents=True)
    _write_required_gitignore(source_root)

    report = hotpath.run_phase42h_vps_preflight(session_root, source_root=source_root, required_imports=("json",), check_network=False)

    gitignore = report["checks"]["gitignore_status"]
    assert report["passed"] is True
    assert not (session_root / ".gitignore").exists()
    assert gitignore["present"] is True
    assert gitignore["path"] == str((source_root / ".gitignore").resolve()).replace("\\", "/")
    assert gitignore["source_root"] == str(source_root.resolve()).replace("\\", "/")


def test_phase42h_preflight_still_uses_source_root_gitignore_when_root_is_session_dir(tmp_path: Path) -> None:
    test_phase42h_preflight_uses_source_root_gitignore_when_root_is_session_dir(tmp_path)


def test_phase42h_preflight_fails_when_source_root_gitignore_missing(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    session_root = source_root / "data/phase_5_2/sessions/session_001_sanity_30m"
    session_root.mkdir(parents=True)

    report = hotpath.run_phase42h_vps_preflight(session_root, source_root=source_root, required_imports=("json",), check_network=False)

    gitignore = report["checks"]["gitignore_status"]
    assert report["passed"] is False
    assert gitignore["present"] is False
    assert gitignore["path"] == str((source_root / ".gitignore").resolve()).replace("\\", "/")
    assert ".gitignore missing generated artifact rules" in report["hard_fail_reasons"]


def test_phase42h_preflight_passes_without_session_gitignore_when_source_root_has_policy(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    session_root = source_root / "data/phase_5_2/sessions/session_001_sanity_30m"
    session_root.mkdir(parents=True)
    _write_required_gitignore(source_root)

    report = hotpath.run_phase42h_vps_preflight(session_root, source_root=source_root, required_imports=("json",), check_network=False)

    assert not (session_root / ".gitignore").exists()
    assert report["checks"]["gitignore_status"]["passed"] is True
    assert report["passed"] is True


def test_vps_preflight_reports_swap_status(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    report = hotpath.run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    memory = report["checks"]["memory_and_swap"]
    assert "swap_total_bytes" in memory
    assert "swap_enabled" in memory


def test_vps_preflight_reports_memory_total_available(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    report = hotpath.run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    memory = report["checks"]["memory_and_swap"]
    assert "memory_total_bytes" in memory
    assert "memory_available_bytes" in memory
    assert "total_memory_bytes" in memory
    assert "available_memory_bytes" in memory
    assert "disk_free_bytes" in memory


def test_vps_preflight_warns_low_memory_no_swap_for_long_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hotpath, "_system_memory_status", lambda: {
        "memory_total_bytes": 3 * 1024 * 1024 * 1024,
        "memory_available_bytes": 2 * 1024 * 1024 * 1024,
        "memory_total_mb": 3072,
        "memory_available_mb": 2048,
        "swap_total_bytes": 0,
        "swap_enabled": False,
        "low_memory_no_swap_warning": True,
    })
    status = hotpath._system_memory_status()
    assert status["low_memory_no_swap_warning"] is True


def test_gitignore_contains_required_generated_patterns() -> None:
    patterns = {
        line.strip()
        for line in Path(".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    for pattern in (
        "*.jsonl",
        "*.zip",
        "*.log",
        "data/dataset/",
        "data/debug/",
        "data/cache/",
        "data/cache/phase_5_2_failed_runs/",
        "data/logs/",
        "data/reports/",
        "data/phase_5_2/",
        "logs/",
        "reports/",
        "debug/",
        "cache/",
    ):
        assert pattern in patterns


def test_cleanup_still_cleans_source_root_and_session_root(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    session_root = tmp_path / "sessions/session_001"
    source_file = source_root / "data/dataset/orderbook_clean_samples.jsonl"
    session_file = session_root / "data/debug/phase_4_2h_latency_stage_profile.json"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text("{}\n", encoding="utf-8")
    session_file.write_text("{}", encoding="utf-8")
    cleanup_phase42h_artifacts(session_root, source_root=source_root)
    assert not source_file.exists()
    assert not session_file.exists()


def test_gitignore_required_rules_still_present() -> None:
    test_gitignore_contains_required_generated_patterns()


def test_strict_100ms_gate_not_relaxed() -> None:
    report = _report(h100=0.0, h250=0.96)
    assert report["max_future_gap_ms"] == 100
    assert report["strict_100ms_observability_ready"] is False
    assert report["relaxed_250ms_observability_candidate"] is True
    assert report["phase5_ready"] is False


def test_phase42h_strict_100ms_gates_unchanged() -> None:
    test_strict_100ms_gate_not_relaxed()


def test_runtime_cleanup_does_not_modify_gitignore(tmp_path: Path) -> None:
    gitignore = tmp_path / ".gitignore"
    original = "*.jsonl\n*.zip\n"
    gitignore.write_text(original, encoding="utf-8")
    cleanup_phase42h_artifacts(tmp_path, source_root=tmp_path)
    assert gitignore.read_text(encoding="utf-8") == original


def test_phase42h_cli_clean_flag_sets_cleanup_performed_true(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)
    _patch_phase42h_cli_fixture(monkeypatch, source_root=tmp_path)
    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "1800",
            "--environment-name",
            "test",
            "--environment-region",
            "local",
            "--run-mode",
            "vps_final",
            "--skip-preflight",
            "--skip-pytest",
            "--clean",
            "--no-bundle",
        ]
    )
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert report["cleanup_report"]["cleanup_performed"] is True


def test_phase42h_cli_without_clean_for_fresh_capture_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)
    _patch_phase42h_cli_fixture(monkeypatch, source_root=tmp_path)
    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "1800",
            "--environment-name",
            "test",
            "--environment-region",
            "local",
            "--run-mode",
            "vps_final",
            "--skip-preflight",
            "--skip-pytest",
            "--no-bundle",
        ]
    )
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["status"] == "fail"
    assert report["primary_failure"] == "ARTIFACT_CLEANUP_FAILURE"


def test_phase42h_cli_rejects_evaluate_existing_without_skip_capture(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        phase42h_cli.main(
            [
                "--root",
                str(tmp_path),
                "--duration-sec",
                "7200",
                "--environment-name",
                "test",
                "--environment-region",
                "local",
                "--run-mode",
                "repaired_eval",
                "--evaluate-existing-artifacts",
                "--skip-pytest",
                "--no-bundle",
            ]
        )
    assert exc.value.code == 2


def test_phase42h_cli_rejects_evaluate_existing_with_clean(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        phase42h_cli.main(
            [
                "--root",
                str(tmp_path),
                "--duration-sec",
                "7200",
                "--environment-name",
                "test",
                "--environment-region",
                "local",
                "--run-mode",
                "repaired_eval",
                "--skip-capture",
                "--evaluate-existing-artifacts",
                "--clean",
                "--skip-pytest",
                "--no-bundle",
            ]
        )
    assert exc.value.code == 2


def test_phase42h_cli_skip_capture_without_evaluate_existing_still_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "test",
            "--environment-region",
            "local",
            "--run-mode",
            "vps_final",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["primary_failure"] == "FRESH_CAPTURE_NOT_PERFORMED"
    assert report["fixture_mode"] is True


def test_phase42h_cli_evaluate_existing_artifacts_passes_valid_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=48)
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert report["evaluation_mode"] == "existing_artifacts"
    assert report["fresh_capture_required"] is False
    assert report["fresh_capture_performed"] is False
    assert report["skip_capture"] is True
    assert report["fixture_mode"] is False
    assert report["latency_profile_status"] == "pass"
    assert report["latency_stage_profile_artifact"]["valid"] is True
    assert report["hot_path_latency_summary"]["sample_count"] == 48
    assert report["derived_artifact_mode"] == "reuse_existing"
    assert report["rebuild_derived_artifacts"] is False


def test_phase42h_cli_evaluate_existing_socket_recv_unavailable_is_warning_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=24)
    profile_path = tmp_path / hotpath.PHASE42H_LATENCY_STAGE_PROFILE
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    profile["socket_recv_monotonic_ns"] = "stage_not_available"
    profile["unavailable_stages"] = {"socket_recv_monotonic_ns": "stage_not_available"}
    profile["stage_availability"]["socket_recv_monotonic_ns"] = {
        "available_count": 0,
        "stage_not_available_count": profile["sample_count"],
    }
    profile["earliest_available_receive_stage"] = "raw_ws_callback_monotonic_ns"
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    queue_path = tmp_path / hotpath.PHASE42H_QUEUE_BACKPRESSURE_REPORT
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    for key in ("disk_write_on_hot_path", "debug_logging_on_hot_path", "batch_writer_enabled"):
        queue.pop(key, None)
    queue["queue_backpressure_detected"] = False
    queue_path.write_text(json.dumps(queue), encoding="utf-8")

    exit_code, report = _run_phase42h_existing_eval(tmp_path, monkeypatch)

    assert exit_code == 0
    assert report["status"] == "pass"
    assert report["hot_path_decoupling_status"] == "pass"
    assert report["implementation_status"] == "pass"
    assert report["strict_100ms_observability_ready"] is True
    assert report["low_latency_ready"] is True
    assert "socket_recv_monotonic_ns_unavailable" in report["warning_reasons"]
    assert report["hot_path_latency_summary"]["earliest_available_receive_stage"] == "raw_ws_callback_monotonic_ns"
    assert report["hot_path_latency_summary"]["unavailable_stages"] == {"socket_recv_monotonic_ns": "stage_not_available"}


@pytest.mark.parametrize(
    ("case", "expected_classification"),
    [
        ("missing_metrics", "LATENCY_PROFILE_MISSING"),
        ("disk_write_on_hot_path", "HOT_PATH_DECOUPLING_INCOMPLETE"),
        ("debug_logging_on_hot_path", "HOT_PATH_DECOUPLING_INCOMPLETE"),
        ("batch_writer_disabled", "HOT_PATH_DECOUPLING_INCOMPLETE"),
        ("queue_backpressure_detected", "HOT_PATH_DECOUPLING_INCOMPLETE"),
    ],
)
def test_phase42h_cli_evaluate_existing_still_fails_actual_hotpath_issues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    case: str,
    expected_classification: str,
) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=24)
    if case == "queue_backpressure_detected":
        queue_path = tmp_path / hotpath.PHASE42H_QUEUE_BACKPRESSURE_REPORT
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        queue["queue_backpressure_detected"] = True
        queue_path.write_text(json.dumps(queue), encoding="utf-8")
    else:
        profile_path = tmp_path / hotpath.PHASE42H_LATENCY_STAGE_PROFILE
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        if case == "missing_metrics":
            profile["missing_metrics"] = ["parse_duration_ms"]
        elif case == "disk_write_on_hot_path":
            profile["disk_write_on_hot_path"] = True
        elif case == "debug_logging_on_hot_path":
            profile["debug_logging_on_hot_path"] = True
        elif case == "batch_writer_disabled":
            profile["batch_writer_enabled"] = False
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

    exit_code, report = _run_phase42h_existing_eval(tmp_path, monkeypatch)

    assert exit_code == 1
    assert expected_classification in report["failure_classifications"]
    if expected_classification == "HOT_PATH_DECOUPLING_INCOMPLETE":
        assert report["hot_path_decoupling_status"] == "fail"


def test_phase42h_cli_evaluate_existing_does_not_rewrite_derived_artifacts_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=48)
    derived_paths = [
        tmp_path / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
        tmp_path / "data/dataset/orderbook_time_protocol_benchmark_labels.jsonl",
        tmp_path / "data/dataset/orderbook_reference_benchmark_labels.jsonl",
    ]
    cache_root = tmp_path / "data/cache"
    assert list(cache_root.glob("tmp*/phase42h_*.sqlite")) == []
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in derived_paths}

    def fail_analysis(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("run_phase42h_analysis should not rebuild derived artifacts by default")

    def fail_full_count(_path: str | Path) -> int:
        raise AssertionError("evaluate-existing default should not full-scan JSONL artifacts")

    def fail_streaming_finalization(**_kwargs: Any) -> Any:
        raise AssertionError("evaluate-existing reuse mode should not call streaming finalization")

    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))
    monkeypatch.setattr(phase42h_cli, "run_phase42h_analysis", fail_analysis)
    monkeypatch.setattr(hotpath, "run_phase42h_streaming_finalization", fail_streaming_finalization)
    monkeypatch.setattr(phase42h_cli, "_count_jsonl", fail_full_count)
    monkeypatch.setattr(
        phase42h_cli,
        "create_phase42h_dataset_zip",
        lambda root: (_ for _ in ()).throw(AssertionError("dataset zip should not be rebuilt by default")),
    )

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    assert exit_code == 0
    after = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in derived_paths}
    assert after == before
    assert list(cache_root.glob("tmp*/phase42h_*.sqlite")) == []
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert report["streaming_finalization"]["skipped"] is True
    assert report["existing_artifact_validation"]["valid"] is True
    assert report["derived_artifact_mode"] == "reuse_existing"


@pytest.mark.parametrize(
    ("relative_path", "role"),
    [
        ("data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", "corrected_time_protocol_labels"),
        ("data/dataset/orderbook_time_protocol_benchmark_labels.jsonl", "time_protocol_labels"),
        ("data/dataset/orderbook_reference_benchmark_labels.jsonl", "receive_time_reference_labels"),
    ],
)
@pytest.mark.parametrize("missing", [False, True])
def test_phase42h_cli_evaluate_existing_fails_missing_or_empty_derived_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    relative_path: str,
    role: str,
    missing: bool,
) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=8)
    path = tmp_path / relative_path
    if missing:
        path.unlink()
    else:
        path.write_text("", encoding="utf-8")
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["primary_failure"] == hotpath.DERIVED_ARTIFACT_MISSING_CLASSIFICATION
    assert hotpath.DERIVED_ARTIFACT_MISSING_CLASSIFICATION in report["failure_classifications"]
    invalid = [item for item in report["existing_artifact_validation"]["files"] if item["role"] == role]
    assert invalid and invalid[0]["valid"] is False


def test_phase42h_cli_evaluate_existing_fails_missing_required_debug_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=8)
    (tmp_path / hotpath.PHASE42H_QUEUE_BACKPRESSURE_REPORT).unlink()
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert "QUEUE_BACKPRESSURE_REPORT_MISSING" in report["failure_classifications"]
    invalid = [item for item in report["existing_artifact_validation"]["files"] if item["role"] == "queue_backpressure_report"]
    assert invalid and invalid[0]["valid"] is False


def test_phase42h_cli_evaluate_existing_rebuild_flag_allows_derived_rebuild(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=24)
    calls: list[dict[str, Any]] = []

    def fake_analysis(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        report = _fresh_phase42h_report()
        report["fresh_capture_required"] = False
        report["fresh_capture_performed"] = False
        report["skip_capture"] = True
        report["fixture_mode"] = False
        report["capture"] = {
            **report["capture"],
            **kwargs["capture"],
            "fresh_capture_required": False,
            "fresh_capture_performed": False,
            "skip_capture": True,
            "fixture_mode": False,
            "evaluation_mode": "existing_artifacts",
        }
        return evaluate_phase42h_report(report)

    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))
    monkeypatch.setattr(phase42h_cli, "run_phase42h_analysis", fake_analysis)
    monkeypatch.setattr(phase42h_cli, "create_phase42h_dataset_zip", lambda root: root / LATENCY_PROFILE_DATASETS_ZIP)

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--rebuild-derived-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert len(calls) == 1
    assert report["derived_artifact_mode"] == "rebuilt"
    assert report["rebuild_derived_artifacts"] is True


def test_phase42h_cli_evaluate_existing_fails_empty_42h_latency_samples(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=24)
    (tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl").write_text("", encoding="utf-8")
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["evaluation_mode"] == "existing_artifacts"
    assert report["fixture_mode"] is False
    assert report["primary_failure"] == "LATENCY_PROFILE_MISSING"
    assert report["latency_profile_status"] == "fail"


def test_phase42h_cli_evaluate_existing_ignores_42fg_latency_samples(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_existing_phase42h_artifacts(tmp_path, line_count=24)
    phase42h = tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    phase42fg = tmp_path / "data/dataset/phase_4_2fg_latency_profile_samples.jsonl"
    phase42fg.write_bytes(phase42h.read_bytes())
    phase42h.unlink()
    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 1
    assert "LATENCY_PROFILE_MISSING" in report["failure_classifications"]
    assert report["hot_path_latency_summary"]["stage_profile_path"].endswith("phase_4_2h_latency_profile_samples.jsonl")


def test_phase42h_capture_duration_stops_near_requested_duration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def slow_capture(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        await asyncio.sleep(1.0)
        return 0, {}

    async def fake_sample(*, sample_id: int, phase: str) -> dict[str, Any]:
        return build_server_time_sample(
            sample_id=sample_id,
            phase=phase,
            local_wall_before_request_ms=37_500.0 + sample_id,
            local_wall_after_response_ms=37_505.0 + sample_id,
            binance_server_time_ms=10.0 + sample_id,
        )

    monkeypatch.setattr(phase42h_cli, "_run_phase42h_multi_feed_capture", slow_capture)
    monkeypatch.setattr(phase42h_cli, "_sample_binance_server_time", fake_sample)

    started = time.monotonic()
    _samples, capture_code, diagnostics = asyncio.run(
        phase42h_cli._run_capture_with_clock_samples(
            root=tmp_path,
            symbol="BTCUSDT",
            duration_sec=0.01,
            depth_n=20,
            writer_batch_size=512,
            writer_flush_interval_ms=100.0,
            writer_queue_max_size=65_536,
            capture_guard_grace_sec=0.01,
        )
    )
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert capture_code == 1
    assert diagnostics["duration_guard_failure"] is True
    assert diagnostics["capture_duration_sec"] <= 0.5


def test_phase42h_duration_guard_fails_large_capture_overrun() -> None:
    report = _fresh_phase42h_report()
    report["duration_sec"] = 7200.0
    report["capture_duration_sec"] = 48_360.0
    report["total_child_duration_sec"] = 48_360.0
    report["finalization_duration_sec"] = 0.0
    report["bundle_duration_sec"] = 0.0
    report["capture"] = {
        **report["capture"],
        "capture_duration_sec": 48_360.0,
        "capture_duration_guard_limit_sec": 7320.0,
        "capture_duration_within_guard": False,
    }

    evaluated = evaluate_phase42h_report(report)

    assert evaluated["status"] == "fail"
    assert "CAPTURE_DURATION_EXCEEDED" in evaluated["failure_classifications"]


def test_phase42h_capture_duration_guard_limit_for_10800_is_10920() -> None:
    assert hotpath.phase42h_capture_duration_guard_limit_sec(10_800) == 10_920.0


def test_phase42h_capture_duration_guard_limit_for_14400_is_14520() -> None:
    assert hotpath.phase42h_capture_duration_guard_limit_sec(14_400) == 14_520.0


def test_phase42h_capture_duration_guard_applies_to_3h_session() -> None:
    _assert_duration_guard_applies_to_requested_duration(10_800)


def test_phase42h_capture_duration_guard_applies_to_4h_session() -> None:
    _assert_duration_guard_applies_to_requested_duration(14_400)


def _assert_duration_guard_applies_to_requested_duration(requested_duration_sec: float) -> None:
    guard_limit = hotpath.phase42h_capture_duration_guard_limit_sec(requested_duration_sec)
    assert guard_limit is not None
    capture_duration = guard_limit + 1.0
    report = _fresh_phase42h_report()
    report["duration_sec"] = float(requested_duration_sec)
    report["capture_duration_sec"] = capture_duration
    report["total_child_duration_sec"] = capture_duration
    report["finalization_duration_sec"] = 0.0
    report["bundle_duration_sec"] = 0.0
    report["capture"] = {
        **report["capture"],
        "capture_duration_sec": capture_duration,
        "capture_duration_guard_limit_sec": guard_limit,
        "capture_duration_within_guard": False,
    }

    evaluated = evaluate_phase42h_report(report)

    assert evaluated["status"] == "fail"
    assert "CAPTURE_DURATION_EXCEEDED" in evaluated["failure_classifications"]


def test_phase42h_report_includes_stage_timestamps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_required_gitignore(tmp_path)
    _patch_phase42h_cli_fixture(monkeypatch, source_root=tmp_path)

    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "1800",
            "--environment-name",
            "test",
            "--environment-region",
            "local",
            "--run-mode",
            "vps_final",
            "--skip-preflight",
            "--skip-pytest",
            "--clean",
            "--no-bundle",
        ]
    )

    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    for field in hotpath.PHASE42H_STAGE_TIMING_FIELDS:
        assert field in report
    assert report["capture_started_at_utc"]
    assert report["capture_ended_at_utc"]
    assert report["finalization_started_at_utc"]
    assert report["finalization_ended_at_utc"]
    assert report["bundle_started_at_utc"]
    assert report["bundle_ended_at_utc"]
    assert report["child_started_at_utc"]
    assert report["child_ended_at_utc"]
    assert report["capture_duration_sec"] is not None
    assert report["finalization_duration_sec"] is not None
    assert report["bundle_duration_sec"] is not None
    assert report["total_child_duration_sec"] is not None


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
            "input_queue_put_start_monotonic_ns": 1_900_000,
            "input_queue_put_end_monotonic_ns": 1_930_000,
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
            "input_queue_put_to_sample_emit_ms": 0.2,
            "input_queue_put_duration_ms": 0.03,
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
    assert profile["metrics"]["input_queue_put_duration_ms"]["p95"] == pytest.approx(0.03)
    assert profile["metrics"]["input_queue_put_to_sample_emit_ms"]["p95"] == pytest.approx(0.2)
    assert "sample_emit_to_queue_put_start_ms" not in profile["metrics"]
    assert profile["metrics"]["file_write_duration_ms"]["p95"] == pytest.approx(0.3)


def test_medium_session_valid_phase42h_latency_stage_profile_passes(tmp_path: Path) -> None:
    session_root = tmp_path / "data/phase_5_2/sessions/session_005_medium_2h"
    report = _fresh_phase42h_report()
    report["duration_sec"] = 7200.0
    report["capture"]["duration_sec"] = 7200.0

    write_phase42h_artifacts(report, root=session_root, pytest_output="pytest ok", bundle_created=False)

    payload = json.loads((session_root / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["latency_profile_status"] == "pass"
    assert payload["latency_stage_profile_artifact"]["valid"] is True
    assert payload["latency_stage_profile_artifact"]["root"] == str(session_root.resolve()).replace("\\", "/")


def test_phase42h_latency_stage_profile_empty_or_invalid_schema_fails(tmp_path: Path) -> None:
    session_root = tmp_path / "data/phase_5_2/sessions/session_005_medium_2h"
    profile_path = session_root / "data/debug/phase_4_2h_latency_stage_profile.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("", encoding="utf-8")

    empty_validation = hotpath.validate_phase42h_latency_stage_profile_artifact(session_root, report=_fresh_phase42h_report())
    assert empty_validation["valid"] is False

    profile_path.write_text(json.dumps({"performed": True, "sample_count": 1}), encoding="utf-8")
    invalid_validation = hotpath.validate_phase42h_latency_stage_profile_artifact(session_root, report=_fresh_phase42h_report())
    report = _fresh_phase42h_report()
    report["latency_stage_profile_artifact"] = invalid_validation
    evaluated = evaluate_phase42h_report(report)
    assert invalid_validation["valid"] is False
    assert evaluated["status"] == "fail"
    assert evaluated["primary_failure"] == "LATENCY_PROFILE_MISSING"
    assert "latency stage profile missing" in evaluated["hard_fail_reasons"]


def test_phase42h_legacy_42fg_latency_samples_do_not_satisfy_requirement(tmp_path: Path) -> None:
    _write_phase42h_streaming_inputs(tmp_path, line_count=20)
    legacy_path = tmp_path / "data/dataset/phase_4_2fg_latency_profile_samples.jsonl"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_text(json.dumps({"performed": True, "sample_count": 1}) + "\n", encoding="utf-8")
    clock_samples = [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=1_700_000_000_000.0,
            local_wall_after_response_ms=1_700_000_000_004.0,
            binance_server_time_ms=1_700_000_000_002.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=1_700_000_002_000.0,
            local_wall_after_response_ms=1_700_000_002_004.0,
            binance_server_time_ms=1_700_000_002_002.0,
        ),
    ]

    report = hotpath.run_phase42h_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=clock_samples,
        environment={"environment_name": "test"},
        capture={
            "duration_sec": 1800.0,
            "fresh_capture_performed": True,
            "fixture_mode": False,
            "skip_capture": False,
            "phase41_runtime_report": _phase41(),
            "capture_diagnostics": {"reference_writer_batch_report": _writer_report()},
        },
        cleanup_report={"cleanup_performed": True, "errors": []},
        gitignore_validation={"passed": True},
        fresh_capture_required=False,
    )

    assert (tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl").exists() is False
    assert "LATENCY_PROFILE_MISSING" in report["failure_classifications"]
    assert report["latency_profile_status"] == "fail"


def test_phase42h_latency_stage_profile_validation_uses_session_root_not_repo_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    session_root = repo_root / "data/phase_5_2/sessions/session_005_medium_2h"
    repo_profile = repo_root / "data/debug/phase_4_2h_latency_stage_profile.json"
    repo_profile.parent.mkdir(parents=True, exist_ok=True)
    repo_profile.write_text(json.dumps(_latency_profile()), encoding="utf-8")

    validation = hotpath.validate_phase42h_latency_stage_profile_artifact(session_root, report=_fresh_phase42h_report())

    assert validation["valid"] is False
    assert validation["root"] == str(session_root.resolve()).replace("\\", "/")
    assert validation["absolute_path"] == str((session_root / "data/debug/phase_4_2h_latency_stage_profile.json").resolve()).replace("\\", "/")
    assert "latency stage profile artifact missing" in validation["errors"]


def test_phase42h_capture_writes_latency_samples_to_42h_path_not_42fg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, Path] = {}

    async def fake_depth_capture(**kwargs: Any) -> dict[str, Any]:
        paths = kwargs["paths"]
        observed["latency_profile_samples"] = paths.latency_profile_samples
        paths.latency_profile_samples.parent.mkdir(parents=True, exist_ok=True)
        paths.latency_profile_samples.write_text("{}\n", encoding="utf-8")
        return _phase41()

    async def fake_references(**_kwargs: Any) -> dict[str, Any]:
        return {
            "connected": True,
            "message_count_by_stream": {},
            "parsed_count_by_source": {},
            "parse_error_count_by_source": {},
            "reference_writer_batch_report": _writer_report(),
        }

    monkeypatch.setattr(phase42h_cli, "run_orderbook_phase41_capture", fake_depth_capture)
    monkeypatch.setattr(phase42h_cli, "_capture_references", fake_references)

    code, _diagnostics = asyncio.run(
        phase42h_cli._run_phase42h_multi_feed_capture(
            root=tmp_path,
            symbol="BTCUSDT",
            duration_sec=1.0,
            depth_n=20,
            writer_batch_size=512,
            writer_flush_interval_ms=100.0,
            writer_queue_max_size=1024,
        )
    )

    assert code == 0
    assert observed["latency_profile_samples"] == tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    assert (tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl").exists()
    assert not (tmp_path / "data/dataset/phase_4_2fg_latency_profile_samples.jsonl").exists()


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


def test_phase41_runtime_fail_blocks_low_latency_even_without_sequence_gaps() -> None:
    phase41 = {
        **_phase41(gaps=0),
        "phase_4_1_pass": False,
        "phase_4_1_status": "fail",
        "phase_4_1_failure_reasons": ["snapshot_copy_p99_us > snapshot_copy_budget_us"],
    }
    report = _report(h100=0.97, h250=0.98, p95=80.0, p99=120.0, phase41_report=phase41)
    assert report["phase41_runtime_report_status"] == "fail"
    assert report["phase41_runtime_ready"] is False
    assert report["strict_100ms_observability_ready"] is False
    assert report["low_latency_ready"] is False
    assert report["readiness_decision_reason"] == "phase41_runtime_report_failed"
    assert report["status"] == "fail"
    assert "PHASE41_RUNTIME_FAILURE" in report["failure_classifications"]


def test_phase41_runtime_missing_blocks_fresh_capture_strict_readiness() -> None:
    report = _report(
        h100=0.97,
        h250=0.98,
        p95=80.0,
        p99=120.0,
        phase41_report={},
        fresh_capture_required=True,
    )
    assert report["phase41_runtime_report_status"] == "missing"
    assert report["phase41_runtime_ready"] is False
    assert report["strict_100ms_observability_ready"] is False
    assert report["low_latency_ready"] is False
    assert report["readiness_decision_reason"] == "phase41_runtime_report_missing"
    assert "PHASE41_RUNTIME_REPORT_MISSING" in report["failure_classifications"]


def test_readiness_invariants_are_hard_failures() -> None:
    report = _report(h100=0.0)
    report["low_latency_ready"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "READINESS_SEMANTICS_FAILURE" in evaluated["failure_classifications"]
    report = _report(h100=0.97, p95=80.0, p99=120.0)
    report["phase5_ready"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "PHASE5_READY_FORBIDDEN" in evaluated["failure_classifications"]


def test_embedded_phase41_runtime_failure_hard_fails_phase42h() -> None:
    report = _report()
    report["phase41_runtime_report"]["phase_4_1_pass"] = False
    report["phase41_runtime_report"]["phase_4_1_status"] = "fail"
    report["phase41_runtime_report"]["phase_4_1_failure_reasons"] = ["snapshot_copy_p99_us > snapshot_copy_budget_us"]
    report["phase41_runtime_report_status"] = phase41_runtime_report_status(report["phase41_runtime_report"])
    evaluated = evaluate_phase42h_report(report)
    assert evaluated["status"] == "fail"
    assert "PHASE41_RUNTIME_FAILURE" in evaluated["failure_classifications"]
    assert any("snapshot_copy_p99_us" in reason for reason in evaluated["hard_fail_reasons"])


def test_phase41_runtime_report_prefers_current_capture_over_stale_artifact(tmp_path: Path) -> None:
    stale_path = tmp_path / "data/reports/phase_4_1_orderbook_quality_report.json"
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text(
        json.dumps(
            {
                "phase_4_1_pass": False,
                "phase_4_1_status": "fail",
                "phase_4_1_failure_reasons": ["stale artifact should not be loaded"],
            }
        ),
        encoding="utf-8",
    )
    current = {
        "phase_4_1_pass": True,
        "phase_4_1_status": "pass",
        "phase_4_1_failure_reasons": [],
    }
    report, source = resolve_phase41_runtime_report(
        tmp_path,
        capture={
            "fresh_capture_performed": True,
            "skip_capture": False,
            "capture_diagnostics": {"phase41_runtime_report": current},
        },
    )
    assert report == current
    assert source["source"] == "current_capture_summary"
    assert source["fresh"] is True


def test_phase41_runtime_report_does_not_fallback_to_artifact_for_fresh_capture(tmp_path: Path) -> None:
    stale_path = tmp_path / "data/reports/phase_4_1_orderbook_quality_report.json"
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text(
        json.dumps(
            {
                "phase_4_1_pass": False,
                "phase_4_1_status": "fail",
                "phase_4_1_failure_reasons": ["stale artifact"],
            }
        ),
        encoding="utf-8",
    )
    report, source = resolve_phase41_runtime_report(
        tmp_path,
        capture={"fresh_capture_performed": True, "skip_capture": False, "capture_diagnostics": {}},
    )
    assert report == {}
    assert source["source"] == "missing_current_capture_summary"
    evaluated = evaluate_phase42h_report(
        {
            **_report(),
            "fresh_capture_required": True,
            "fresh_capture_performed": True,
            "skip_capture": False,
            "phase41_runtime_report": report,
            "phase41_runtime_report_source": source,
            "phase41_runtime_report_status": "missing",
        }
    )
    assert "PHASE41_RUNTIME_STALE_RISK" in evaluated["failure_classifications"]
    assert "PHASE41_RUNTIME_REPORT_MISSING" in evaluated["failure_classifications"]


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


def test_phase42h_large_file_paths_do_not_use_read_text() -> None:
    streaming_text = Path("bot/app/research/phase42h_streaming.py").read_text(encoding="utf-8")
    assert ".read_text(" not in streaming_text


def test_phase42h_large_file_paths_do_not_use_readlines() -> None:
    relevant = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "bot/app/research/phase42h_streaming.py",
            "bot/app/research/hotpath_environment_latency.py",
            "scripts/run_phase42h_hotpath_environment_latency.py",
        )
    )
    assert ".readlines(" not in relevant


def test_phase42h_large_file_paths_do_not_use_json_load_on_jsonl() -> None:
    streaming_text = Path("bot/app/research/phase42h_streaming.py").read_text(encoding="utf-8")
    assert "json.load(" not in streaming_text


def test_phase42h_bundle_does_not_read_file_bytes_into_memory() -> None:
    bundle_text = Path("bot/app/research/hotpath_environment_latency.py").read_text(encoding="utf-8")
    runner_text = Path("scripts/run_phase42h_hotpath_environment_latency.py").read_text(encoding="utf-8")
    assert ".read_bytes(" not in bundle_text
    assert ".read_bytes(" not in runner_text


def test_phase42h_streaming_helpers_are_used_for_large_jsonl() -> None:
    module_text = Path("bot/app/research/hotpath_environment_latency.py").read_text(encoding="utf-8")
    assert "run_phase42h_streaming_finalization(" in module_text
    assert "build_latency_stage_profile_streaming(" in module_text


def test_phase42h_streaming_finalization_still_avoids_read_text_read_bytes_on_large_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_phase42h_streaming_inputs(tmp_path, line_count=25)

    def blocked_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        raise AssertionError(f"read_text should not be used while streaming {self}")

    def blocked_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"read_bytes should not be used while streaming {self}")

    monkeypatch.setattr(Path, "read_text", blocked_read_text)
    monkeypatch.setattr(Path, "read_bytes", blocked_read_bytes)
    result = phase42h_streaming.run_phase42h_streaming_finalization(
        root=tmp_path,
        clean_samples_path="data/dataset/orderbook_clean_samples.jsonl",
        corrected_labels_path="data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
        leakage_output_path="data/debug/phase_4_2h_leakage_check.json",
        estimated_clock_offset_ms=0.0,
        clock_offset_drift_valid=True,
    )
    assert result.labeled_sample_count == 25


def test_stream_jsonl_counts_large_file_without_memory_growth(tmp_path: Path) -> None:
    path = tmp_path / "large.jsonl"
    _write_simple_jsonl(path, line_count=120_000)
    before = _rss_bytes()
    count = sum(1 for _line, _row in phase42h_streaming.stream_jsonl_records(path))
    gc.collect()
    after = _rss_bytes()
    assert count == 120_000
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)


def test_stream_jsonl_filters_large_file_without_accumulating_records(tmp_path: Path) -> None:
    source = tmp_path / "large.jsonl"
    target = tmp_path / "filtered.jsonl"
    _write_simple_jsonl(source, line_count=80_000)
    before = _rss_bytes()
    summary = phase42h_streaming.stream_jsonl_filter(
        source,
        target,
        predicate=lambda row: row["i"] % 10 == 0,
        transform=lambda row: {"i": row["i"], "bucket": row["i"] // 10},
    )
    after = _rss_bytes()
    assert summary["written_count"] == 8_000
    assert sum(1 for _line, _row in phase42h_streaming.stream_jsonl_records(target)) == 8_000
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)


def test_stream_jsonl_handles_malformed_lines_with_bounded_error_sample(tmp_path: Path) -> None:
    path = tmp_path / "malformed.jsonl"
    path.write_text('{"ok": true}\nnot-json\n[]\n{bad}\n{"ok": true}\n', encoding="utf-8")
    report = phase42h_streaming.JsonlStreamReport(path=str(path), max_malformed_samples=2)
    rows = list(phase42h_streaming.stream_jsonl_records(path, report=report))
    assert len(rows) == 2
    assert report.malformed_line_count == 3
    assert len(report.malformed_samples) == 2


def test_stream_jsonl_summary_keeps_only_bounded_examples(tmp_path: Path) -> None:
    path = tmp_path / "summary.jsonl"
    _write_simple_jsonl(path, line_count=50)
    summary = phase42h_streaming.summarize_jsonl_stream(path, max_examples=3)
    assert summary["object_count"] == 50
    assert len(summary["examples"]) == 3


def test_large_label_generation_streams_output(tmp_path: Path) -> None:
    _write_phase42h_streaming_inputs(tmp_path, line_count=2_500)
    before = _rss_bytes()
    result = phase42h_streaming.run_phase42h_streaming_finalization(
        root=tmp_path,
        clean_samples_path="data/dataset/orderbook_clean_samples.jsonl",
        corrected_labels_path="data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
        leakage_output_path="data/debug/phase_4_2h_leakage_check.json",
        estimated_clock_offset_ms=0.0,
        clock_offset_drift_valid=True,
    )
    after = _rss_bytes()
    assert result.labeled_sample_count == 2_500
    assert _count_jsonl(tmp_path / "data/dataset/orderbook_reference_benchmark_labels.jsonl") == 2_500
    assert _count_jsonl(tmp_path / "data/dataset/orderbook_time_protocol_benchmark_labels.jsonl") == 2_500
    assert _count_jsonl(tmp_path / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl") == 2_500
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)


def test_phase52_simulated_1h_large_outputs_no_oom(tmp_path: Path) -> None:
    result = _assert_streaming_finalization_memory_ceiling(tmp_path, line_count=3_600)
    assert result.labeled_sample_count == 3_600


def test_corrected_time_protocol_label_generation_memory_ceiling(tmp_path: Path) -> None:
    _assert_streaming_finalization_memory_ceiling(tmp_path, line_count=3_000)


def test_orderbook_time_protocol_benchmark_generation_memory_ceiling(tmp_path: Path) -> None:
    result = _assert_streaming_finalization_memory_ceiling(tmp_path, line_count=2_000)
    assert result.sources["depth_mid"]["receive_time"]["max_future_gap_ms"] == 100


def test_orderbook_reference_benchmark_generation_memory_ceiling(tmp_path: Path) -> None:
    result = _assert_streaming_finalization_memory_ceiling(tmp_path, line_count=2_000)
    assert result.sources["bookTicker_mid"]["valid_reference_event_count"] == 2_000


def test_latency_profile_summary_streaming_memory_ceiling(tmp_path: Path) -> None:
    path = tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "stages": {stage: index + 1 for index, stage in enumerate(hotpath.REQUIRED_STAGE_NAMES)},
        "metrics": {metric: 1.0 for metric in hotpath.LATENCY_METRIC_NAMES},
        "earliest_available_receive_stage": "raw_ws_callback_monotonic_ns",
        "queue_size_at_enqueue": 1,
        "disk_write_on_hot_path": False,
        "debug_logging_on_hot_path": False,
        "batch_writer_enabled": True,
    }
    with path.open("w", encoding="utf-8") as handle:
        for _ in range(20_000):
            handle.write(json.dumps(row) + "\n")
    before = _rss_bytes()
    profile = hotpath.build_latency_stage_profile(path)
    after = _rss_bytes()
    assert profile["sample_count"] == 20_000
    assert profile["metrics"]["end_to_end_local_hot_path_ms"]["p99"] == pytest.approx(1.0)
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)


def test_multisource_protocol_reports_streaming_memory_ceiling(tmp_path: Path) -> None:
    result = _assert_streaming_finalization_memory_ceiling(tmp_path, line_count=2_000)
    assert set(result.sources) == set(REFERENCE_SOURCES)
    assert result.sources["trade_price"]["corrected_hybrid"]["corrected_hybrid_100ms"]["max_future_gap_ms"] == 100


def test_phase42h_analysis_uses_streamed_clean_count_without_materialized_rows(tmp_path: Path) -> None:
    line_count = 120
    _write_phase42h_streaming_inputs(tmp_path, line_count=line_count)
    _write_phase42h_latency_profile_samples(tmp_path, line_count=24)
    clock_samples = [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=1_700_000_000_000.0,
            local_wall_after_response_ms=1_700_000_000_004.0,
            binance_server_time_ms=1_700_000_000_002.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=1_700_000_012_000.0,
            local_wall_after_response_ms=1_700_000_012_004.0,
            binance_server_time_ms=1_700_000_012_002.0,
        ),
    ]
    report = hotpath.run_phase42h_analysis(
        root=tmp_path,
        symbol="BTCUSDT",
        clock_offset_samples=clock_samples,
        environment={"environment_name": "test"},
        capture={
            "duration_sec": 60.0,
            "fresh_capture_performed": True,
            "fixture_mode": False,
            "skip_capture": False,
            "phase41_runtime_report": _phase41(),
            "capture_diagnostics": {"reference_writer_batch_report": _writer_report()},
        },
        cleanup_report={"performed": True},
        gitignore_validation={"passed": True},
        memory_telemetry=hotpath.new_memory_telemetry(),
    )
    assert report["clean_sample_count"] == line_count
    assert report["labeled_sample_count"] == line_count
    assert "streaming_finalization" in report
    assert _count_jsonl(tmp_path / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl") == line_count


def test_capture_bundle_uses_zipfile_write_not_read_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_phase42h_artifacts(_report(), root=tmp_path, pytest_output="ok", bundle_created=True)
    (tmp_path / LATENCY_PROFILE_DATASETS_ZIP).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(tmp_path / LATENCY_PROFILE_DATASETS_ZIP, "w") as archive:
        archive.writestr("data/dataset/phase_4_2h_latency_profile_samples.jsonl", "{}\n")

    def fail_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"read_bytes called for {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)
    assert create_phase42h_bundle(root=tmp_path, pass_bundle=True).exists()


def test_large_artifact_copy_uses_streaming_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.jsonl"
    target = tmp_path / "target.jsonl"
    source.write_bytes(b"x" * (2 * 1024 * 1024))

    def fail_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"read_bytes called for {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)
    phase42h_cli._copy_if_exists(source, target)
    assert target.stat().st_size == source.stat().st_size


def test_bundle_large_files_memory_ceiling(tmp_path: Path) -> None:
    report = _report()
    write_phase42h_artifacts(report, root=tmp_path, pytest_output="ok", bundle_created=True)
    dataset = tmp_path / LATENCY_PROFILE_DATASETS_ZIP
    dataset.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dataset, "w") as archive:
        archive.writestr("large.bin", b"x" * (5 * 1024 * 1024))
    before = _rss_bytes()
    bundle = create_phase42h_bundle(root=tmp_path, pass_bundle=True)
    after = _rss_bytes()
    assert bundle.exists()
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)


def test_bundle_skips_missing_files_without_loading_existing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_phase42h_artifacts(_report(writer_drops=1), root=tmp_path, pytest_output="ok", bundle_created=True)

    def fail_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        raise AssertionError(f"read_text called for {self}")

    monkeypatch.setattr(Path, "read_text", fail_read_text)
    assert create_phase42h_bundle(root=tmp_path, pass_bundle=False).exists()


def test_bundle_records_file_sizes_and_sha256_streaming(tmp_path: Path) -> None:
    write_phase42h_artifacts(_report(), root=tmp_path, pytest_output="ok", bundle_created=True)
    telemetry = hotpath.new_memory_telemetry()
    large = tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    large.parent.mkdir(parents=True, exist_ok=True)
    large.write_bytes(b"x" * (1024 * 1024))
    hotpath.refresh_generated_file_sizes(tmp_path, telemetry)
    assert telemetry["generated_file_sizes_bytes"]["data/dataset/phase_4_2h_latency_profile_samples.jsonl"] == 1024 * 1024
    bundle = create_phase42h_bundle(root=tmp_path, pass_bundle=True)
    with zipfile.ZipFile(bundle) as archive:
        manifest = json.loads(archive.read("data/debug/phase_4_2h_bundle_file_manifest.json"))
    assert any(item["path"] == "data/debug/phase_4_2h_latency_stage_profile.json" for item in manifest["files"])


def test_write_and_bundle_creates_phase42h_bundle_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Path] = []

    def fake_bundle(*, root: str | Path, pass_bundle: bool, bundle_path: str | Path | None = None) -> Path:
        target = Path(bundle_path) if bundle_path is not None else Path(root) / PHASE42H_PASS_BUNDLE
        calls.append(target)
        target.write_bytes(b"bundle")
        return target

    monkeypatch.setattr(phase42h_cli, "create_phase42h_bundle", fake_bundle)
    phase42h_cli._write_and_bundle(_report(), root=tmp_path, pytest_output="ok", no_bundle=False, memory_telemetry=hotpath.new_memory_telemetry())
    assert len(calls) == 1


def test_write_and_bundle_preserves_bundle_memory_stages(tmp_path: Path) -> None:
    phase42h_cli._write_and_bundle(_report(), root=tmp_path, pytest_output="ok", no_bundle=True, memory_telemetry=hotpath.new_memory_telemetry())
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    samples = report["memory_telemetry"]["samples"]
    assert "bundle_start" in samples
    assert "bundle_end" in samples


def test_write_and_bundle_final_report_has_memory_telemetry(tmp_path: Path) -> None:
    phase42h_cli._write_and_bundle(_report(), root=tmp_path, pytest_output="ok", no_bundle=True, memory_telemetry=hotpath.new_memory_telemetry())
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    assert report["memory_telemetry"]["schema_version"] == "phase_4_2h_memory_telemetry_v1"


def test_write_and_bundle_does_not_use_read_bytes_for_large_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report()
    large = tmp_path / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    large.parent.mkdir(parents=True, exist_ok=True)
    large.write_bytes(b"x" * 1024 * 1024)

    def blocked_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"read_bytes should not be used while bundling {self}")

    monkeypatch.setattr(Path, "read_bytes", blocked_read_bytes)
    phase42h_cli._write_and_bundle(report, root=tmp_path, pytest_output="ok", no_bundle=False, memory_telemetry=hotpath.new_memory_telemetry())
    assert (tmp_path / PHASE42H_PASS_BUNDLE).exists()


def test_phase42h_bundle_sha256_still_valid_after_single_bundle_creation(tmp_path: Path) -> None:
    phase42h_cli._write_and_bundle(_report(), root=tmp_path, pytest_output="ok", no_bundle=False, memory_telemetry=hotpath.new_memory_telemetry())
    bundle = tmp_path / PHASE42H_PASS_BUNDLE
    first = _sha256(bundle)
    assert first == _sha256(bundle)


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


def test_memory_telemetry_schema_present(tmp_path: Path) -> None:
    telemetry = hotpath.new_memory_telemetry()
    for stage in hotpath.MEMORY_TELEMETRY_STAGES:
        hotpath.record_memory_stage(telemetry, stage)
    finalized = hotpath.finalize_memory_telemetry(tmp_path, telemetry)
    report = _report()
    report["memory_telemetry"] = finalized
    assert report["memory_telemetry"]["schema_version"] == "phase_4_2h_memory_telemetry_v1"


def test_memory_telemetry_has_stage_samples(tmp_path: Path) -> None:
    telemetry = hotpath.new_memory_telemetry()
    for stage in hotpath.MEMORY_TELEMETRY_STAGES:
        hotpath.record_memory_stage(telemetry, stage)
    finalized = hotpath.finalize_memory_telemetry(tmp_path, telemetry)
    assert set(hotpath.MEMORY_TELEMETRY_STAGES) <= set(finalized["samples"])


def test_memory_telemetry_handles_missing_psutil(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    original_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "psutil":
            raise ImportError("psutil hidden for test")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    telemetry = hotpath.new_memory_telemetry()
    hotpath.record_memory_stage(telemetry, "process_start")
    finalized = hotpath.finalize_memory_telemetry(tmp_path, telemetry)
    assert "process_start" in finalized["samples"]
    assert finalized["samples"]["process_start"]["peak_rss_bytes"] >= 0


def test_memory_telemetry_peak_rss_non_negative(tmp_path: Path) -> None:
    telemetry = hotpath.new_memory_telemetry()
    hotpath.record_memory_stage(telemetry, "process_start")
    finalized = hotpath.finalize_memory_telemetry(tmp_path, telemetry)
    assert finalized["peak_rss_bytes"] >= 0


def test_memory_telemetry_included_in_failure_metadata() -> None:
    report = _report(writer_drops=1)
    evaluated = evaluate_phase42h_report(report)
    assert "memory_telemetry" in evaluated


def _run_phase42h_existing_eval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[int, dict[str, Any]]:
    def fail_analysis(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("evaluate-existing reuse mode should not rebuild derived artifacts")

    def fail_streaming_finalization(**_kwargs: Any) -> Any:
        raise AssertionError("evaluate-existing reuse mode should not call streaming finalization")

    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", tmp_path)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))
    monkeypatch.setattr(phase42h_cli, "run_phase42h_analysis", fail_analysis)
    monkeypatch.setattr(hotpath, "run_phase42h_streaming_finalization", fail_streaming_finalization)
    exit_code = phase42h_cli.main(
        [
            "--root",
            str(tmp_path),
            "--duration-sec",
            "7200",
            "--environment-name",
            "phase52_vps_repaired_eval",
            "--environment-region",
            "unknown",
            "--run-mode",
            "repaired_eval",
            "--skip-preflight",
            "--skip-pytest",
            "--skip-capture",
            "--evaluate-existing-artifacts",
            "--no-bundle",
        ]
    )
    report = json.loads((tmp_path / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").read_text(encoding="utf-8"))
    return exit_code, report


def _patch_phase42h_cli_fixture(monkeypatch: pytest.MonkeyPatch, *, source_root: Path) -> None:
    async def fake_capture(**kwargs: Any) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
        return (
            [
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
                    local_wall_before_request_ms=37_520.0,
                    local_wall_after_response_ms=37_530.0,
                    binance_server_time_ms=25.0,
                ),
            ],
            0,
            {"phase41_runtime_report": _phase41(), "reference_writer_batch_report": _writer_report()},
        )

    def fake_analysis(**kwargs: Any) -> dict[str, Any]:
        report = _fresh_phase42h_report()
        report["cleanup_report"] = kwargs["cleanup_report"]
        report["fresh_capture_required"] = kwargs["fresh_capture_required"]
        report["fresh_capture_performed"] = True
        report["skip_capture"] = False
        report["fixture_mode"] = False
        report["capture"] = {**report["capture"], **kwargs["capture"], "fresh_capture_performed": True, "skip_capture": False, "fixture_mode": False}
        return evaluate_phase42h_report(report)

    monkeypatch.setattr(phase42h_cli, "SOURCE_ROOT", source_root)
    monkeypatch.setattr(phase42h_cli, "_run_typecheck", lambda output_path: (0, "typecheck/compileall passed with test fixture"))
    monkeypatch.setattr(phase42h_cli, "_run_capture_with_clock_samples", fake_capture)
    monkeypatch.setattr(phase42h_cli, "run_phase42h_analysis", fake_analysis)
    monkeypatch.setattr(phase42h_cli, "create_phase42h_dataset_zip", lambda root: root / LATENCY_PROFILE_DATASETS_ZIP)


def _seed_existing_phase42h_artifacts(root: Path, *, line_count: int) -> None:
    _write_required_gitignore(root)
    _write_phase42h_streaming_inputs(root, line_count=line_count)
    _write_phase42h_latency_profile_samples(root, line_count=line_count)
    clock_samples = [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=1_700_000_000_000.0,
            local_wall_after_response_ms=1_700_000_000_004.0,
            binance_server_time_ms=1_700_000_000_002.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=1_700_000_012_000.0,
            local_wall_after_response_ms=1_700_000_012_004.0,
            binance_server_time_ms=1_700_000_012_002.0,
        ),
    ]
    clock_summary = compute_clock_offset_summary(clock_samples)
    streaming = phase42h_streaming.run_phase42h_streaming_finalization(
        root=root,
        clean_samples_path="data/dataset/orderbook_clean_samples.jsonl",
        corrected_labels_path="data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
        leakage_output_path="data/debug/phase_4_2h_leakage_check.json",
        estimated_clock_offset_ms=clock_summary.get("estimated_clock_offset_ms"),
        clock_offset_drift_valid=clock_summary.get("clock_offset_drift_valid") is True,
    )
    latency_profile = build_latency_stage_profile(root / "data/dataset/phase_4_2h_latency_profile_samples.jsonl")
    latency_profile_path = root / hotpath.PHASE42H_LATENCY_STAGE_PROFILE
    latency_profile_path.parent.mkdir(parents=True, exist_ok=True)
    latency_profile_path.write_text(json.dumps(latency_profile), encoding="utf-8")
    clock_path = root / hotpath.PHASE42H_CLOCK_OFFSET_SAMPLES
    clock_path.parent.mkdir(parents=True, exist_ok=True)
    clock_path.write_text(
        json.dumps({"samples": clock_samples, "summary": clock_summary}),
        encoding="utf-8",
    )
    output_paths = {
        "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
        "latency_profile_samples": "data/dataset/phase_4_2h_latency_profile_samples.jsonl",
        "bookticker": "data/dataset/bookticker_reference_quotes.jsonl",
        "trade": "data/dataset/trade_reference_events.jsonl",
        "aggtrade": "data/dataset/aggtrade_reference_events.jsonl",
    }
    diagnostics = {
        "fresh_capture_performed": False,
        "fixture_mode": False,
        "skip_capture": True,
        "evaluation_mode": "existing_artifacts",
        "duration_sec": 7200.0,
        "symbol": "BTCUSDT",
        "requested_streams": phase42h_cli.required_streams("BTCUSDT"),
        "message_count_by_stream": {stream: line_count for stream in phase42h_cli.required_streams("BTCUSDT")},
        "parsed_count_by_source": {
            "depth_mid": line_count,
            "bookTicker_mid": line_count,
            "trade_price": line_count,
            "aggTrade_price": line_count,
        },
        "parse_error_count_by_source": {
            "depth_mid": 0,
            "bookTicker_mid": 0,
            "trade_price": 0,
            "aggTrade_price": 0,
        },
        "output_file_paths": output_paths,
        "output_file_sizes_bytes": {key: (root / relative).stat().st_size for key, relative in output_paths.items()},
        "phase41_runtime_report": _phase41(),
        "reference_writer_batch_report": _writer_report(),
    }
    diagnostics_path = root / hotpath.PHASE42H_CAPTURE_DIAGNOSTICS
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_path.write_text(json.dumps(diagnostics), encoding="utf-8")
    writer = _writer_report()
    phase41 = _phase41(writer=writer)
    queue = build_queue_backpressure_report(
        phase41_report=phase41,
        latency_profile=latency_profile,
        writer_report=writer,
    )
    clock_sanity = _clock_sanity()
    debug_artifacts = {
        hotpath.PHASE42H_QUEUE_BACKPRESSURE_REPORT: queue,
        hotpath.PHASE42H_WRITER_BATCH_REPORT: writer,
        hotpath.PHASE42H_CLOCK_SANITY_REPORT: clock_sanity,
        hotpath.PHASE42H_LEAKAGE_CHECK: streaming.leakage_result,
    }
    for relative, payload in debug_artifacts.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_phase42h_report(
        symbol="BTCUSDT",
        clean_samples=[],
        sources=streaming.sources,
        timestamp_schema=streaming.timestamp_schema,
        leakage_result=streaming.leakage_result,
        clock_offset_samples=clock_samples,
        clock_offset_summary=clock_summary,
        clock_sanity=clock_sanity,
        latency_profile=latency_profile,
        queue_report=queue,
        writer_report=writer,
        phase41_report=phase41,
        capture={
            "duration_sec": 7200.0,
            "fresh_capture_performed": False,
            "fresh_capture_required": False,
            "fixture_mode": False,
            "skip_capture": True,
            "evaluation_mode": "existing_artifacts",
            "capture_diagnostics": diagnostics,
        },
        cleanup_report={"cleanup_performed": False, "errors": []},
        gitignore_validation={"passed": True},
        environment=build_environment_metadata(
            environment_name="phase52_vps_repaired_eval",
            environment_region="unknown",
            machine_profile="test",
            run_mode="repaired_eval",
        ),
        pytest_passed=True,
        typecheck_passed=True,
        typecheck_summary="typecheck/compileall passed with test fixture",
        fresh_capture_required=False,
        labeled_sample_count=streaming.labeled_sample_count,
        clean_sample_count=streaming.clean_sample_count,
    )
    report_path = root / hotpath.PHASE42H_REPORT_JSON
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(evaluate_phase42h_report(report)), encoding="utf-8")


def _write_simple_jsonl(path: Path, *, line_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for index in range(line_count):
            handle.write(json.dumps({"i": index, "value": f"row-{index}"}) + "\n")


def _assert_streaming_finalization_memory_ceiling(tmp_path: Path, *, line_count: int) -> phase42h_streaming.Phase42HStreamingResult:
    _write_phase42h_streaming_inputs(tmp_path, line_count=line_count)
    before = _rss_bytes()
    result = phase42h_streaming.run_phase42h_streaming_finalization(
        root=tmp_path,
        clean_samples_path="data/dataset/orderbook_clean_samples.jsonl",
        corrected_labels_path="data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
        leakage_output_path="data/debug/phase_4_2h_leakage_check.json",
        estimated_clock_offset_ms=0.0,
        clock_offset_drift_valid=True,
    )
    after = _rss_bytes()
    assert result.labeled_sample_count == line_count
    _assert_memory_delta_below(before, after, 100 * 1024 * 1024)
    return result


def _write_phase42h_streaming_inputs(root: Path, *, line_count: int) -> None:
    dataset = root / "data/dataset"
    dataset.mkdir(parents=True, exist_ok=True)
    paths = {
        "clean": dataset / "orderbook_clean_samples.jsonl",
        "book": dataset / "bookticker_reference_quotes.jsonl",
        "trade": dataset / "trade_reference_events.jsonl",
        "agg": dataset / "aggtrade_reference_events.jsonl",
    }
    handles = {key: path.open("w", encoding="utf-8") for key, path in paths.items()}
    try:
        for index in range(line_count):
            local_ts = index * 100_000_000
            exchange_ms = 1_700_000_000_000.0 + index * 100.0
            wall_ts = _iso_ms(exchange_ms + 20.0)
            bid = 100.0 + index * 0.01
            ask = bid + 1.0
            mid = (bid + ask) / 2.0
            clean = {
                "schema_version": "orderbook_clean_v1",
                "symbol": "BTCUSDT",
                "source": "binance_depth",
                "generation_id": index,
                "state_version": index,
                "snapshot_version": 1,
                "last_update_id": index,
                "local_recv_monotonic_ns": local_ts,
                "local_recv_wall_ts": wall_ts,
                "exchange_event_ts": exchange_ms,
                "best_bid": bid,
                "best_ask": ask,
                "mid_price": mid,
                "bids": [[bid, 1.0], [bid - 1.0, 2.0]],
                "asks": [[ask, 1.0], [ask + 1.0, 2.0]],
                "quality": {"errors": [], "is_valid": True},
                "lifecycle": {"snapshot_ready": True, "ready_to_emit": True, "sequence_continuous": True},
            }
            book = {
                "schema_version": "bookticker_reference_v1",
                "symbol": "BTCUSDT",
                "source": "bookTicker_mid",
                "update_id": index,
                "event_id": index,
                "local_recv_monotonic_ns": local_ts,
                "local_recv_wall_ts": wall_ts,
                "exchange_event_ts": exchange_ms,
                "best_bid": bid,
                "best_ask": ask,
                "mid_price": mid,
                "price": mid,
                "quality": {"valid": True, "errors": []},
            }
            trade = {
                "schema_version": "trade_reference_v1",
                "symbol": "BTCUSDT",
                "source": "trade_price",
                "trade_id": index,
                "event_id": index,
                "local_recv_monotonic_ns": local_ts,
                "local_recv_wall_ts": wall_ts,
                "exchange_event_ts": exchange_ms,
                "trade_time": exchange_ms,
                "price": mid,
                "quality": {"valid": True, "errors": []},
            }
            agg = {
                "schema_version": "aggtrade_reference_v1",
                "symbol": "BTCUSDT",
                "source": "aggTrade_price",
                "aggregate_trade_id": index,
                "event_id": index,
                "local_recv_monotonic_ns": local_ts,
                "local_recv_wall_ts": wall_ts,
                "exchange_event_ts": exchange_ms,
                "trade_time": exchange_ms,
                "price": mid,
                "quality": {"valid": True, "errors": []},
            }
            handles["clean"].write(json.dumps(clean) + "\n")
            handles["book"].write(json.dumps(book) + "\n")
            handles["trade"].write(json.dumps(trade) + "\n")
            handles["agg"].write(json.dumps(agg) + "\n")
    finally:
        for handle in handles.values():
            handle.close()


def _write_phase42h_latency_profile_samples(root: Path, *, line_count: int) -> None:
    path = root / "data/dataset/phase_4_2h_latency_profile_samples.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for index in range(line_count):
            base = index * 1_000_000
            row = {
                "stages": {stage: base + offset for offset, stage in enumerate(hotpath.REQUIRED_STAGE_NAMES, start=1)},
                "metrics": {metric: 1.0 for metric in hotpath.LATENCY_METRIC_NAMES},
                "earliest_available_receive_stage": "raw_ws_callback_monotonic_ns",
                "queue_size_at_enqueue": 1,
                "disk_write_on_hot_path": False,
                "debug_logging_on_hot_path": False,
                "batch_writer_enabled": True,
            }
            handle.write(json.dumps(row) + "\n")


def _iso_ms(epoch_ms: float) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat()


def _count_jsonl(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rss_bytes() -> int:
    sample = hotpath._sample_process_memory()
    return int(sample.get("rss_bytes") or 0)


def _assert_memory_delta_below(before: int, after: int, threshold: int) -> None:
    if before <= 0 or after <= 0:
        return
    assert after - before < threshold
