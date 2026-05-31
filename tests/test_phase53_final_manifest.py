from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import ResearchReadinessManifestBuilder
from tests.phase53_test_utils import phase53_config


def _manifest(tmp_path: Path, *, allowed: list[str], excluded: list[dict] | None = None, hard: list[str] | None = None) -> dict:
    config = phase53_config(tmp_path)
    eligibility = {
        "allowed_phase54_sessions": allowed,
        "excluded_sessions": excluded or [],
        "sessions": [
            {"session_id": session, "canonical_candidate": True, "allowed_for_phase54": True, "audit_warnings": []}
            for session in allowed
        ]
        + [
            {"session_id": item["session_id"], "canonical_candidate": True, "allowed_for_phase54": False, "audit_warnings": []}
            for item in (excluded or [])
        ],
    }
    return ResearchReadinessManifestBuilder(config).build(
        inventory_report={},
        artifact_report={"integrity_status": "pass", "hard_fail_reasons": hard or [], "warnings": []},
        lineage_report={"sessions": eligibility["sessions"], "session_count": len(eligibility["sessions"])},
        schema_report={"file_count": 1, "dataset_schema_status": "pass", "hard_fail_reasons": []},
        timestamp_report={"timestamp_status": "pass", "future_timestamp_leak_count": 0, "negative_latency_count": 0, "horizon_100ms_timestamp_verifiable": True, "hard_fail_reasons": []},
        coverage_report={"coverage_status": "pass", "coverage_ratio": 1.0, "coverage_threshold": 0.95, "coverage_threshold_source": "test", "future_leak_count": 0, "hard_fail_reasons": []},
        orderbook_report={"reference_consistency_status": "pass", "bid_ask_crossed_count": 0, "spread_negative_count": 0, "mismatch_counter": {}, "hard_fail_reasons": []},
        runtime_report={"runtime_health_status": "pass", "queue_backpressure_status": "pass", "writer_batch_status": "pass", "ws_lifecycle_status": "pass", "hard_fail_reasons": []},
        eligibility_report=eligibility,
        report_paths={"report.json": "data/phase_5_3/reports/report.json"},
    )


def test_final_manifest_schema(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, allowed=["session_001_sanity_30m"])

    assert manifest["phase"] == "5.3"
    assert manifest["name"] == "Dataset Integrity & Research Readiness"
    assert manifest["final_status"] == "phase_5_3_pass"
    assert manifest["research_ready"] is True
    assert "report_paths" in manifest


def test_final_status_partial_when_subset_excluded(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        allowed=["session_001_sanity_30m"],
        excluded=[{"session_id": "session_005_medium_2h", "reasons": ["hotpath_status_not_pass"]}],
    )

    assert manifest["final_status"] == "phase_5_3_partial"
    assert manifest["research_ready"] == "partial"


def test_final_status_fail_when_no_allowed_sessions(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, allowed=[], excluded=[{"session_id": "session_001", "reasons": ["no_labels"]}])

    assert manifest["final_status"] == "phase_5_3_fail"
    assert manifest["research_ready"] is False


def test_report_paths_allowed_excluded_and_hard_fail_reasons_included(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        allowed=["session_001_sanity_30m"],
        excluded=[{"session_id": "session_005_medium_2h", "reasons": ["future_timestamp_leak_detected"]}],
        hard=["artifact_integrity_failure"],
    )

    assert manifest["allowed_phase54_sessions"] == ["session_001_sanity_30m"]
    assert manifest["excluded_sessions"][0]["session_id"] == "session_005_medium_2h"
    assert "artifact_integrity_failure" in manifest["hard_fail_reasons"]
    assert manifest["report_paths"]["report.json"] == "data/phase_5_3/reports/report.json"
