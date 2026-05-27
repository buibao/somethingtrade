from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_feature_schema_v0_is_future_free_and_reports_null_rates() -> None:
    report = load_json("data/debug/phase_5_0_feature_schema.json")
    assert report["status"] == "pass"
    assert report["schema_version"] == "phase_5_0_feature_schema_v0"
    assert report["primary_label_horizon_ms"] == 100
    assert report["feature_count"] >= 10
    assert set(report["null_rates"]) == {feature["name"] for feature in report["features"]}
    groups = {feature["group"] for feature in report["features"]}
    assert "reference_market" in groups
    assert "polymarket_target_market" in groups
    assert "cross_market_latency" in groups
    for feature in report["features"]:
        assert {"name", "dtype", "group", "timestamp_rule", "uses_future_data", "nullable", "description"} <= set(feature)
        assert feature["uses_future_data"] is False
    assert report["features_marked_as_using_future_data"] == []
