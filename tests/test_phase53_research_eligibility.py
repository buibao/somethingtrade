from __future__ import annotations

from app.research.phase53_dataset_integrity import (
    FAILED_RUN_LINEAGE,
    PREFLIGHT_SESSION,
    PRIMARY_PHASE52_SESSION,
    REPAIRED_EVAL_SESSION,
    ResearchEligibilityClassifier,
)


def _lineage(session_id: str = "session_001_sanity_30m", cls: str = PRIMARY_PHASE52_SESSION) -> dict:
    return {
        "session_id": session_id,
        "session_class": cls,
        "lineage_status": "complete",
        "lineage_hard_fail_reasons": [],
        "hotpath_status": "pass",
        "strict_100ms_observability_ready": True,
        "low_latency_ready": True,
        "status_from_quality_report": "pass",
        "research_eligible_from_phase52": True,
        "evaluation_mode": "existing_artifacts" if cls == REPAIRED_EVAL_SESSION else None,
        "derived_artifact_mode": "reuse_existing" if cls == REPAIRED_EVAL_SESSION else None,
        "rebuild_derived_artifacts": False if cls == REPAIRED_EVAL_SESSION else None,
        "paired_original_session_id": "session_005_medium_2h" if cls == REPAIRED_EVAL_SESSION else None,
    }


def _classify(lineages: list[dict], *, timestamp_status: str = "pass", coverage_status: str = "pass") -> dict:
    return ResearchEligibilityClassifier().classify(
        lineages=lineages,
        artifact_report={"integrity_status": "pass"},
        schema_report={"files": [{"session_id": item["session_id"], "schema_status": "pass"} for item in lineages]},
        timestamp_report={"sessions": [{"session_id": item["session_id"], "timestamp_status": timestamp_status, "hard_fail_reasons": ["future_timestamp_leak_detected"] if timestamp_status == "fail" else []} for item in lineages]},
        coverage_report={
            "sessions": [
                {
                    "session_id": item["session_id"],
                    "coverage_status": coverage_status,
                    "hard_fail_reasons": ["missing_explicit_100ms_coverage_threshold"] if coverage_status == "partial" else [],
                }
                for item in lineages
            ]
        },
        orderbook_report={"sessions": [{"session_id": item["session_id"], "reference_consistency_status": "pass"} for item in lineages]},
        runtime_report={"sessions": [{"session_id": item["session_id"], "runtime_health_status": "pass"} for item in lineages]},
    )


def test_preflight_not_eligible() -> None:
    report = _classify([_lineage("preflight_check_60s", PREFLIGHT_SESSION)])

    assert report["sessions"][0]["research_eligible"] is False
    assert "preflight_only" in report["sessions"][0]["blocking_reasons"]


def test_failed_run_not_eligible() -> None:
    report = _classify([_lineage("session_001", FAILED_RUN_LINEAGE)])

    assert report["sessions"][0]["research_eligible"] is False
    assert "failed_run_lineage_only" in report["sessions"][0]["blocking_reasons"]


def test_primary_with_all_gates_pass_allowed_for_phase54() -> None:
    report = _classify([_lineage()])

    row = report["sessions"][0]
    assert row["research_eligible"] is True
    assert row["allowed_for_phase54"] is True


def test_primary_with_timestamp_leak_not_eligible() -> None:
    report = _classify([_lineage()], timestamp_status="fail")

    row = report["sessions"][0]
    assert row["research_eligible"] is False
    assert "future_timestamp_leak_detected" in row["blocking_reasons"]


def test_primary_with_missing_coverage_threshold_is_partial() -> None:
    report = _classify([_lineage()], coverage_status="partial")

    row = report["sessions"][0]
    assert row["research_eligible"] == "partial"
    assert row["allowed_for_phase54"] is False


def test_repaired_eval_can_supersede_original_only_with_explicit_lineage_reason() -> None:
    report = _classify([_lineage("session_005_medium_2h_repaired_eval", REPAIRED_EVAL_SESSION)])

    row = report["sessions"][0]
    assert row["allowed_for_phase54"] is True
    assert row["reasons"] == ["repaired_eval_explicit_lineage_from_session_005_medium_2h"]
