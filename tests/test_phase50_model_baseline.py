from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_model_baseline_is_simple_and_reports_required_metrics() -> None:
    report = load_json("data/debug/phase_5_0_model_baseline_report.json")
    assert report["status"] in {"pass", "skipped"}
    assert report["allowed_model_type"] == "l2_logistic_regression"
    assert report["primary_label_horizon_ms"] == 100
    assert "deep_learning" in report.get("forbidden_model_families_excluded", ["deep_learning"])
    if report["status"] == "pass":
        assert {"train", "validation", "test"} == set(report["metrics"])
        for metrics in report["metrics"].values():
            assert "auc" in metrics
            assert "precision_at_top_k" in metrics
            assert "calibration" in metrics
            assert "expected_return_bps_by_prediction_bucket" in metrics
        assert "validation_to_test_degradation" in report
        assert isinstance(report["edge_claim_allowed"], bool)
