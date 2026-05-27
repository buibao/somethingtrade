from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_leakage_checks_pass_explicitly() -> None:
    report = load_json("data/debug/phase_5_0_leakage_check.json")
    assert report["status"] == "pass"
    assert report["primary_label_horizon_ms"] == 100
    assert report["feature_ts_lte_label_start_ts"] is True
    assert report["feature_ts_after_label_start_count"] == 0
    assert report["label_future_ts_gt_feature_ts"] is True
    assert report["label_future_ts_violation_count"] == 0
    assert report["feature_source_ts_lte_feature_ts"] is True
    assert report["feature_source_future_violation_count"] == 0
    assert report["future_price_or_orderbook_fields_in_features"] == []
    assert report["label_derived_features"] == []
    assert report["split_overlap_pairs"] == []
    assert report["duplicate_sample_ids_across_splits"] == []
    assert report["total_violation_count"] == 0
