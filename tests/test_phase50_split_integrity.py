from __future__ import annotations

from app.research.microstructure_signal_research import build_final_report, build_split_report, validate_split_integrity_report
from phase50_test_utils import load_json


def test_phase50_split_is_deterministic_chronological_and_non_overlapping() -> None:
    report = load_json("data/debug/phase_5_0_split_report.json")
    assert report["status"] == "pass"
    assert report["split_method"] == "deterministic_chronological_time_based"
    assert report["random_split_used"] is False
    assert report["random_split_rejected"] is True
    assert report["duplicate_sample_ids"] == []
    assert report["overlap_pairs"] == []
    assert report["time_overlap_violations"] == []
    assert report["splits"]["train"]["time_range_ns"]["max"] < report["splits"]["validation"]["time_range_ns"]["min"]
    assert report["splits"]["validation"]["time_range_ns"]["max"] < report["splits"]["test"]["time_range_ns"]["min"]
    assert all(report["splits"][split]["sample_count"] > 0 for split in ("train", "validation", "test"))


def test_phase50_split_rejects_random_split() -> None:
    report = build_split_report(_samples(9))
    report["random_split_used"] = True
    validation = validate_split_integrity_report(report)
    assert validation["status"] == "fail"
    assert "random_split_used" in validation["failure_reasons"]


def test_phase50_split_rejects_overlapping_time_ranges() -> None:
    report = build_split_report(_samples(9))
    report["splits"]["train"]["time_range_ns"]["max"] = report["splits"]["validation"]["time_range_ns"]["min"]
    validation = validate_split_integrity_report(report)
    assert validation["status"] == "fail"
    assert "train_validation_time_overlap_or_out_of_order" in validation["failure_reasons"]


def test_phase50_split_rejects_duplicate_sample_ids_across_splits() -> None:
    report = build_split_report(_samples(9))
    duplicate_id = report["splits"]["train"]["sample_ids"][0]
    report["splits"]["validation"]["sample_ids"][0] = duplicate_id
    validation = validate_split_integrity_report(report)
    assert validation["status"] == "fail"
    assert "computed_duplicate_sample_ids_across_splits" in validation["failure_reasons"]


def test_phase50_split_rejects_out_of_order_chronological_ranges() -> None:
    report = build_split_report(_samples(9))
    report["splits"]["train"]["time_range_ns"]["min"] = report["splits"]["test"]["time_range_ns"]["min"] + 1
    validation = validate_split_integrity_report(report)
    assert validation["status"] == "fail"
    assert "train_validation_chronology_out_of_order" in validation["failure_reasons"]


def test_phase50_small_dataset_cannot_produce_edge_proven() -> None:
    split_report = build_split_report(_samples(2))
    final = build_final_report(
        source_gate={"status": "pass"},
        evidence={"status": "pass"},
        manifest={"status": "pass"},
        split_report=split_report,
        feature_schema={"status": "pass"},
        label_report={"status": "pass", "primary_horizon_ms": 100},
        leakage={"status": "pass"},
        bucket_edge={"status": "pass", "edge_claim_allowed": True, "stable_edge_bucket_count": 1, "buckets": [_supported_bucket()]},
        model_baseline={"status": "pass", "edge_claim_allowed": True, "metrics": _supported_model_metrics()},
    )
    assert final["edge_conclusion"] != "EDGE_PROVEN"
    assert final["edge_conclusion"] == "EDGE_FAILED"
    assert "time_based_split_gate" in final["blockers"]


def _samples(count: int) -> list[dict]:
    return [
        {
            "sample_id": f"s{i}",
            "feature_ts_ns": i * 1_000_000,
            "valid_100ms_label": True,
        }
        for i in range(count)
    ]


def _supported_bucket() -> dict:
    return {
        "low_sample_bucket": False,
        "split_stability": {"validation_and_test_support": True},
        "splits": {
            "validation": {"edge_after_cost_bps": 1.0},
            "test": {"edge_after_cost_bps": 1.0},
        },
    }


def _supported_model_metrics() -> dict:
    bucket = {"edge_after_cost_bps": 1.0}
    return {
        "validation": {"auc": 0.6, "expected_return_bps_by_prediction_bucket": [bucket]},
        "test": {"auc": 0.6, "expected_return_bps_by_prediction_bucket": [bucket]},
    }
