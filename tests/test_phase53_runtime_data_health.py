from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import RuntimeDataHealthAuditor, SessionLineageBuilder
from tests.phase53_test_utils import phase53_config, write_good_session, write_json, write_jsonl


def _audit_first(tmp_path: Path) -> dict:
    config = phase53_config(tmp_path)
    lineage = SessionLineageBuilder(config).build()["sessions"][0]
    return RuntimeDataHealthAuditor(config).audit_session(lineage)


def test_duplicate_update_cases_counted(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/debug/duplicate_update_cases.jsonl", [{"case": 1}, {"case": 2}])

    assert _audit_first(tmp_path)["duplicate_update_cases"] == 2


def test_sequence_gap_cases_counted(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/debug/sequence_gap_cases.jsonl", [{"case": 1}])

    result = _audit_first(tmp_path)

    assert result["sequence_gap_cases"] == 1
    assert result["runtime_health_status"] == "fail"


def test_stale_period_cases_counted(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/debug/stale_period_cases.jsonl", [{"case": 1}])

    assert _audit_first(tmp_path)["stale_period_cases"] == 1


def test_queue_backpressure_report_status_extracted(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_json(session / "data/debug/phase_4_2h_queue_backpressure_report.json", {"queue_backpressure_detected": True, "queue_dropped_messages": 0})

    result = _audit_first(tmp_path)

    assert result["queue_backpressure_status"] == "fail"


def test_ws_lifecycle_report_status_extracted(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_json(session / "data/debug/ws_lifecycle_report.json", {"sequence_gap_count": 1, "queue_dropped_messages": 0})

    result = _audit_first(tmp_path)

    assert result["ws_lifecycle_status"] == "fail"


def test_stopped_early_marker_handled(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_json(
        session / "data/reports/phase_4_2h_hotpath_environment_latency_report.json",
        {"status": "fail", "primary_failure": "REPORT_MISSING", "strict_100ms_observability_ready": True, "low_latency_ready": True},
    )

    result = _audit_first(tmp_path)

    assert result["stopped_early_status"] == "fail"
    assert "session_primary_failure_present" in result["hard_fail_reasons"]
