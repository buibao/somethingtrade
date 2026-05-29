from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.research.phase52_dataset_quality_analysis import build_phase52_dataset_quality_analysis
import scripts.run_phase52_dataset_quality_analysis as cli


DEFAULT = object()


def test_includes_only_eligible_passed_research_sessions(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_001_sanity_30m")
    _write_session(sessions_root, "session_002_failed_1h", quality_status="fail", hotpath_status="fail", primary_failure="SYNTHETIC_FAILURE")

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert [session["session_id"] for session in report["sessions"]] == ["session_001_sanity_30m"]
    assert report["eligible_session_count"] == 1
    assert report["excluded_session_count"] == 1


def test_excludes_failed_or_non_research_sessions_with_reasons(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_failed", quality_status="fail", metadata_runtime_status="fail", hotpath_status="fail")
    _write_session(sessions_root, "session_non_research", quality_research_eligible=False, metadata_research_eligible=False)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    excluded = {session["session_id"]: session["reason"] for session in report["excluded_sessions"]}
    assert "quality_status_not_pass" in excluded["session_failed"]
    assert "metadata_runtime_status_not_pass" in excluded["session_failed"]
    assert "hotpath_status_not_pass" in excluded["session_failed"]
    assert "quality_research_eligible_not_true" in excluded["session_non_research"]
    assert "metadata_research_eligible_not_true" in excluded["session_non_research"]
    assert report["failed_session_count"] == 1


def test_aggregate_totals_are_correct(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_001", requested_duration_sec=1800, capture_duration_sec=1800, labeled=10, clean=12)
    _write_session(sessions_root, "session_002", requested_duration_sec=3600, capture_duration_sec=3600, labeled=20, clean=22)

    report = build_phase52_dataset_quality_analysis(sessions_root)
    aggregate = report["aggregate"]

    assert report["total_requested_duration_sec"] == 5400.0
    assert report["total_capture_duration_sec"] == 5400.0
    assert report["total_labeled_sample_count"] == 30
    assert report["total_clean_sample_count"] == 34
    assert aggregate["capture_duration_sec"] == {"min": 1800.0, "median": 2700.0, "max": 3600.0}


def test_flags_duration_anomaly_when_capture_duration_exceeds_requested_plus_grace(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_overrun", requested_duration_sec=3600, capture_duration_sec=3721)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["aggregate"]["sessions_with_duration_anomalies"] == ["session_overrun"]
    assert "phase_5_analysis_duration_anomalies_present" in report["blockers"]


def test_flags_missing_capture_duration_sec_as_duration_anomaly(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_missing_capture", capture_duration_sec=None)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["sessions"][0]["capture_duration_sec"] is None
    assert report["aggregate"]["sessions_with_duration_anomalies"] == ["session_missing_capture"]


def test_flags_memory_anomaly_when_memory_finalization_delta_bytes_is_missing(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_missing_memory_delta", memory_delta=None)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["sessions"][0]["memory_finalization_delta_bytes"] is None
    assert report["aggregate"]["sessions_with_memory_anomalies"] == ["session_missing_memory_delta"]
    assert "phase_5_analysis_memory_anomalies_present" in report["blockers"]


def test_flags_queue_writer_anomaly_when_drops_errors_or_sequence_gaps_exist(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_queue_writer", queue_drops=1, writer_drops=2, writer_errors=3, sequence_gaps=4)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["aggregate"]["sessions_with_queue_or_writer_anomalies"] == ["session_queue_writer"]
    assert "phase_5_analysis_queue_or_writer_anomalies_present" in report["blockers"]


def test_ready_for_phase_5_analysis_is_true_for_four_clean_eligible_sessions_with_enough_capture(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    for index in range(4):
        _write_session(sessions_root, f"session_{index + 1:03d}", requested_duration_sec=3600, capture_duration_sec=3600)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["status"] == "pass"
    assert report["ready_for_phase_5_analysis"] is True
    assert report["ready_for_long_collection"] is True
    assert report["blockers"] == []


def test_ready_for_phase_5_analysis_is_false_with_clear_blockers_if_eligible_sessions_lt_4(tmp_path: Path) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_001")

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["status"] == "fail"
    assert report["ready_for_phase_5_analysis"] is False
    assert "phase_5_analysis_eligible_session_count_lt_4" in report["blockers"]
    assert "long_collection_eligible_session_count_lt_4" in report["blockers"]


def test_generated_report_does_not_use_read_text_or_read_bytes_on_jsonl_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sessions_root = tmp_path / "data/phase_5_2/sessions"
    _write_session(sessions_root, "session_001")
    jsonl_path = sessions_root / "session_001/data/dataset/orderbook_clean_samples.jsonl"
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path.write_text("{}\n", encoding="utf-8")
    original_read_text = Path.read_text
    original_read_bytes = Path.read_bytes

    def guarded_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        if self.suffix == ".jsonl":
            raise AssertionError(f"read_text called on JSONL file: {self}")
        return original_read_text(self, *args, **kwargs)

    def guarded_read_bytes(self: Path) -> bytes:
        if self.suffix == ".jsonl":
            raise AssertionError(f"read_bytes called on JSONL file: {self}")
        return original_read_bytes(self)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    report = build_phase52_dataset_quality_analysis(sessions_root)

    assert report["eligible_session_count"] == 1


def test_cli_writes_both_json_and_md_outputs(tmp_path: Path) -> None:
    sessions_root = tmp_path / "sessions"
    for index in range(4):
        _write_session(sessions_root, f"session_{index + 1:03d}", requested_duration_sec=3600, capture_duration_sec=3600)
    output_json = tmp_path / "reports/quality.json"
    output_md = tmp_path / "reports/quality.md"

    exit_code = cli.main(["--sessions-root", str(sessions_root), "--output-json", str(output_json), "--output-md", str(output_md)])

    assert exit_code == 0
    assert output_json.exists()
    assert output_md.exists()
    assert json.loads(output_json.read_text(encoding="utf-8"))["status"] == "pass"
    assert "Phase 5.2 Dataset Quality Analysis" in output_md.read_text(encoding="utf-8")


def test_cli_exits_zero_for_pass_and_one_for_fail(tmp_path: Path) -> None:
    pass_root = tmp_path / "pass_sessions"
    for index in range(4):
        _write_session(pass_root, f"session_{index + 1:03d}", requested_duration_sec=3600, capture_duration_sec=3600)
    fail_root = tmp_path / "fail_sessions"
    _write_session(fail_root, "session_001")

    pass_code = cli.main(["--sessions-root", str(pass_root), "--output-json", str(tmp_path / "pass.json"), "--output-md", str(tmp_path / "pass.md")])
    fail_code = cli.main(["--sessions-root", str(fail_root), "--output-json", str(tmp_path / "fail.json"), "--output-md", str(tmp_path / "fail.md")])

    assert pass_code == 0
    assert fail_code == 1


def _write_session(
    sessions_root: Path,
    session_id: str,
    *,
    requested_duration_sec: float = 3600.0,
    actual_duration_sec: float | None = None,
    capture_duration_sec: float | None | object = DEFAULT,
    finalization_duration_sec: float = 10.0,
    bundle_duration_sec: float = 5.0,
    labeled: int = 100,
    clean: int = 120,
    quality_status: str = "pass",
    quality_research_eligible: bool = True,
    bundle_sha256_valid: bool = True,
    metadata_runtime_status: str = "pass",
    metadata_research_eligible: bool = True,
    hotpath_status: str = "pass",
    strict_ready: bool = True,
    low_latency_ready: bool = True,
    clock_sync_status: str = "pass",
    primary_failure: str | None = None,
    memory_delta: int | None = 0,
    memory_available: bool = True,
    queue_drops: int = 0,
    writer_drops: int = 0,
    writer_errors: int = 0,
    sequence_gaps: int = 0,
    warning_reasons: list[str] | None = None,
) -> None:
    capture_value = requested_duration_sec if capture_duration_sec is DEFAULT else capture_duration_sec
    actual_value = requested_duration_sec if actual_duration_sec is None else actual_duration_sec
    total_child = None if capture_value is None else float(capture_value) + finalization_duration_sec + bundle_duration_sec
    session_dir = sessions_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    memory = {
        "available": memory_available,
        "finalization_memory_delta_bytes": memory_delta,
        "peak_rss_bytes": 123_456_789,
        "generated_file_sizes_bytes": {
            "data/dataset/phase_4_2h_latency_profile_samples.jsonl": 2048,
            "data/reports/phase_4_2h_hotpath_environment_latency_report.json": 1024,
        },
    }
    quality = {
        "status": quality_status,
        "research_eligible": quality_research_eligible,
        "bundle_sha256_valid": bundle_sha256_valid,
        "warning_reasons": warning_reasons or [],
    }
    metadata = {
        "session_id": session_id,
        "requested_duration_sec": requested_duration_sec,
        "actual_duration_sec": actual_value,
        "capture_duration_sec": capture_value,
        "finalization_duration_sec": finalization_duration_sec,
        "bundle_duration_sec": bundle_duration_sec,
        "total_child_duration_sec": total_child,
        "runtime_status": metadata_runtime_status,
        "primary_failure": primary_failure,
        "research_eligible": metadata_research_eligible,
        "memory_telemetry": memory,
    }
    hotpath = {
        "status": hotpath_status,
        "primary_failure": primary_failure,
        "duration_sec": requested_duration_sec,
        "capture_duration_sec": capture_value,
        "finalization_duration_sec": finalization_duration_sec,
        "bundle_duration_sec": bundle_duration_sec,
        "total_child_duration_sec": total_child,
        "strict_100ms_observability_ready": strict_ready,
        "low_latency_ready": low_latency_ready,
        "clock_sync_status": clock_sync_status,
        "labeled_sample_count": labeled,
        "clean_sample_count": clean,
        "warning_reasons": warning_reasons or [],
        "hot_path_latency_summary": {
            "metrics": {
                "end_to_end_local_hot_path_ms": {
                    "p50": 1.0,
                    "p95": 2.0,
                    "p99": 3.0,
                }
            }
        },
        "queue_backpressure_summary": {"queue_dropped_messages": queue_drops},
        "writer_batch_report": {"writer_dropped_records": writer_drops, "writer_error_count": writer_errors},
        "phase41_runtime_report": {"sequence_gap_count": sequence_gaps},
        "memory_telemetry": memory,
        "sources": {
            "depth_mid": {
                "corrected_hybrid": {
                    "corrected_hybrid_100ms": {
                        "valid_rate_eligible_rows": 0.97,
                    }
                }
            },
            "trade_price": {
                "corrected_hybrid": {
                    "corrected_hybrid_100ms": {
                        "valid_rate_eligible_rows": 0.96,
                    }
                }
            },
        },
    }
    _write_json(session_dir / f"phase_5_2_{session_id}_quality_report.json", quality)
    _write_json(session_dir / f"phase_5_2_{session_id}_metadata.json", metadata)
    _write_json(session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json", hotpath)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
