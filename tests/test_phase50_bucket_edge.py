from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_bucket_edge_report_contains_required_buckets_and_split_stability() -> None:
    report = load_json("data/debug/phase_5_0_bucket_edge_report.json")
    assert report["status"] == "pass"
    assert report["primary_label_horizon_ms"] == 100
    assert set(report["bucket_features"]) == {
        "repricing_gap_bps",
        "book_imbalance",
        "spread",
        "quote_age",
        "latency_quality",
        "trade_pressure",
    }
    assert report["conservative_cost_assumptions"]["total_cost_bps"] > 0
    assert report["buckets"]
    for bucket in report["buckets"]:
        assert {"train", "validation", "test"} == set(bucket["splits"])
        assert "split_stability" in bucket
        for stats in bucket["splits"].values():
            assert {
                "sample_count",
                "hit_rate",
                "avg_future_return_bps",
                "median_future_return_bps",
                "p25_future_return_bps",
                "p75_future_return_bps",
                "edge_after_cost_bps",
                "low_sample_bucket",
            } <= set(stats)
    assert isinstance(report["edge_claim_allowed"], bool)
