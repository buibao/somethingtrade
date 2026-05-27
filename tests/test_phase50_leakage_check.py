from __future__ import annotations

from app.research.microstructure_signal_research import build_leakage_report
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


def test_phase50_leakage_fails_when_feature_ts_after_label_start() -> None:
    sample = _sample()
    sample["feature_ts_ns"] = 200
    sample["label_start_ts_ns"] = 100
    report = build_leakage_report([sample], _split_report())
    assert report["status"] == "fail"
    assert report["feature_ts_after_label_start_count"] == 1


def test_phase50_leakage_fails_when_label_future_not_after_feature() -> None:
    sample = _sample()
    sample["label_future_ts_ns"] = sample["feature_ts_ns"]
    report = build_leakage_report([sample], _split_report())
    assert report["status"] == "fail"
    assert report["label_future_ts_violation_count"] == 1


def test_phase50_leakage_fails_when_feature_source_ts_after_feature_ts() -> None:
    sample = _sample()
    sample["feature_source_ts_ns"] = {"reference_future_quote": sample["feature_ts_ns"] + 1}
    sample.pop("feature_source_max_ts_ns")
    report = build_leakage_report([sample], _split_report())
    assert report["status"] == "fail"
    assert report["feature_source_future_violation_count"] == 1


def test_phase50_leakage_detects_future_price_and_orderbook_feature_names() -> None:
    sample = _sample()
    sample["features"] = {"future_mid_price": 1.0, "next_orderbook_bid": 1.0}
    report = build_leakage_report([sample], _split_report())
    assert report["status"] == "fail"
    assert "future_mid_price" in report["future_price_or_orderbook_fields_in_features"]
    assert "next_orderbook_bid" in report["future_price_or_orderbook_fields_in_features"]


def test_phase50_leakage_detects_label_derived_feature_names() -> None:
    sample = _sample()
    sample["features"] = {"future_return_100ms_bps": 1.0, "label_direction": 1}
    report = build_leakage_report([sample], _split_report())
    assert report["status"] == "fail"
    assert "future_return_100ms_bps" in report["label_derived_features"]
    assert "label_direction" in report["label_derived_features"]


def _sample() -> dict:
    return {
        "sample_id": "s1",
        "feature_ts_ns": 100,
        "label_start_ts_ns": 100,
        "label_future_ts_ns": 200,
        "valid_100ms_label": True,
        "feature_source_max_ts_ns": 100,
        "features": {},
    }


def _split_report() -> dict:
    return {"duplicate_sample_ids": [], "overlap_pairs": []}
