from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_evidence_integrity_uses_passed_phase42h_bundle() -> None:
    report = load_json("data/debug/phase_5_0_evidence_integrity_report.json")
    assert report["status"] == "pass"
    assert report["bundle_filename"] == "phase_4_2h_hotpath_environment_latency_bundle.zip"
    assert report["bundle_sha256_valid"] is True
    assert report["bundle_extractable"] is True
    assert report["runtime_status"] == "pass"
    assert report["primary_failure"] is None
    assert report["phase41_status"] == "pass"
    assert report["clock_sync_status"] == "pass"
    assert report["clock_offset_drift_valid"] is True
    assert report["clock_offset_sample_quality_valid"] is True
    assert report["snapshot_copy_budget_met"] is True
    assert report["strict_100ms_observability_ready"] is True
    assert report["low_latency_ready"] is True
    assert report["phase5_ready"] is False
    assert report["phase5_ready_false_interpretation"] == "acceptable_before_phase5_implementation"
