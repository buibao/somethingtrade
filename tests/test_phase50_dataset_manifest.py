from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_dataset_manifest_records_source_bundle_and_100ms_horizon() -> None:
    manifest = load_json("data/debug/phase_5_0_dataset_manifest.json")
    assert manifest["status"] == "pass"
    assert manifest["source_repo_commit"]
    assert manifest["bundle_filename"] == "phase_4_2h_hotpath_environment_latency_bundle.zip"
    assert manifest["bundle_sha256"]
    assert manifest["primary_label_horizon_ms"] == 100
    assert manifest["diagnostic_horizon_ms"] == 250
    evidence = manifest["phase42h_pass_evidence"]
    assert evidence["status"] == "pass"
    assert evidence["primary_failure"] is None
    assert evidence["strict_100ms_observability_ready"] is True
    assert evidence["low_latency_ready"] is True
    assert manifest["dataset_summary"]["valid_100ms_label_count"] > 0
