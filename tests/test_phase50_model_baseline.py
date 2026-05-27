from __future__ import annotations

from app.research.microstructure_signal_research import build_final_report, model_edge_claim_supported
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


def test_phase50_model_edge_requires_validation_and_test_after_cost_support() -> None:
    validation_only = {
        "status": "pass",
        "edge_claim_allowed": True,
        "metrics": {
            "validation": {"auc": 0.7, "expected_return_bps_by_prediction_bucket": [{"edge_after_cost_bps": 2.0}]},
            "test": {"auc": 0.7, "expected_return_bps_by_prediction_bucket": [{"edge_after_cost_bps": -1.0}]},
        },
    }
    assert model_edge_claim_supported(validation_only) is False
    final = build_final_report(
        source_gate={"status": "pass"},
        evidence={"status": "pass"},
        manifest={"status": "pass"},
        split_report=_passing_split_report(),
        feature_schema={"status": "pass"},
        label_report={"status": "pass", "primary_horizon_ms": 100},
        leakage={"status": "pass"},
        bucket_edge=_supported_bucket_report(),
        model_baseline=validation_only,
    )
    assert final["edge_conclusion"] != "EDGE_PROVEN"


def test_phase50_model_edge_claim_is_supported_only_when_validation_and_test_pass() -> None:
    supported = {
        "status": "pass",
        "edge_claim_allowed": True,
        "metrics": {
            "validation": {"auc": 0.7, "expected_return_bps_by_prediction_bucket": [{"edge_after_cost_bps": 2.0}]},
            "test": {"auc": 0.7, "expected_return_bps_by_prediction_bucket": [{"edge_after_cost_bps": 1.0}]},
        },
    }
    assert model_edge_claim_supported(supported) is True


def _passing_split_report() -> dict:
    return {
        "status": "pass",
        "split_method": "deterministic_chronological_time_based",
        "random_split_used": False,
        "random_split_rejected": True,
        "sample_count": 3,
        "duplicate_sample_ids": [],
        "overlap_pairs": [],
        "time_overlap_violations": [],
        "splits": {
            "train": {"sample_count": 1, "sample_ids": ["a"], "time_range_ns": {"min": 1, "max": 1}},
            "validation": {"sample_count": 1, "sample_ids": ["b"], "time_range_ns": {"min": 2, "max": 2}},
            "test": {"sample_count": 1, "sample_ids": ["c"], "time_range_ns": {"min": 3, "max": 3}},
        },
    }


def _supported_bucket_report() -> dict:
    return {
        "status": "pass",
        "edge_claim_allowed": True,
        "stable_edge_bucket_count": 1,
        "buckets": [
            {
                "low_sample_bucket": False,
                "split_stability": {"validation_and_test_support": True},
                "splits": {
                    "validation": {"edge_after_cost_bps": 1.0},
                    "test": {"edge_after_cost_bps": 1.0},
                },
            }
        ],
    }
