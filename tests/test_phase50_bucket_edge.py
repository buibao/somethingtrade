from __future__ import annotations

from app.research.microstructure_signal_research import build_bucket_edge_report, build_final_report, bucket_edge_claim_supported
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


def test_phase50_train_only_bucket_edge_does_not_produce_edge_proven() -> None:
    samples, split_report = _edge_samples(train_return=10.0, validation_return=-10.0, test_return=-10.0, per_split=40)
    bucket_report = build_bucket_edge_report(samples, split_report)
    assert bucket_report["edge_claim_allowed"] is False
    final = _final_with(bucket_report=bucket_report, model_report=_supported_model_report())
    assert final["edge_conclusion"] != "EDGE_PROVEN"


def test_phase50_validation_only_bucket_edge_does_not_produce_edge_proven() -> None:
    samples, split_report = _edge_samples(train_return=10.0, validation_return=10.0, test_return=-10.0, per_split=40)
    bucket_report = build_bucket_edge_report(samples, split_report)
    assert bucket_edge_claim_supported(bucket_report) is False
    final = _final_with(bucket_report=bucket_report, model_report=_supported_model_report())
    assert final["edge_conclusion"] != "EDGE_PROVEN"


def test_phase50_low_sample_positive_bucket_does_not_produce_edge_proven() -> None:
    samples, split_report = _edge_samples(train_return=10.0, validation_return=10.0, test_return=10.0, per_split=5)
    bucket_report = build_bucket_edge_report(samples, split_report)
    assert bucket_report["stable_edge_bucket_count"] == 0
    assert bucket_report["edge_claim_allowed"] is False
    final = _final_with(bucket_report=bucket_report, model_report=_supported_model_report())
    assert final["edge_conclusion"] != "EDGE_PROVEN"


def _edge_samples(*, train_return: float, validation_return: float, test_return: float, per_split: int) -> tuple[list[dict], dict]:
    samples: list[dict] = []
    splits = {"train": [], "validation": [], "test": []}
    returns = {"train": train_return, "validation": validation_return, "test": test_return}
    for split, value in returns.items():
        for index in range(per_split):
            sample_id = f"{split}-{index}"
            splits[split].append(sample_id)
            samples.append(
                {
                    "sample_id": sample_id,
                    "valid_100ms_label": True,
                    "future_return_100ms_bps": value,
                    "features": {
                        "repricing_gap_bps": 1.0,
                        "target_book_imbalance_5": 1.0,
                        "target_spread_bps": 0.1,
                        "reference_bookticker_age_ms": 1.0,
                        "latency_quality_score": 1.0,
                        "reference_signed_trade_qty_1s": 1.0,
                    },
                }
            )
    split_report = {
        "splits": {
            split: {"sample_ids": ids, "sample_count": len(ids), "time_range_ns": {"min": i * 1000, "max": i * 1000 + len(ids)}}
            for i, (split, ids) in enumerate(splits.items())
        }
    }
    return samples, split_report


def _final_with(*, bucket_report: dict, model_report: dict) -> dict:
    return build_final_report(
        source_gate={"status": "pass"},
        evidence={"status": "pass"},
        manifest={"status": "pass"},
        split_report=_passing_split_report(),
        feature_schema={"status": "pass"},
        label_report={"status": "pass", "primary_horizon_ms": 100},
        leakage={"status": "pass"},
        bucket_edge=bucket_report,
        model_baseline=model_report,
    )


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


def _supported_model_report() -> dict:
    bucket = {"edge_after_cost_bps": 1.0}
    return {
        "status": "pass",
        "edge_claim_allowed": True,
        "metrics": {
            "validation": {"auc": 0.6, "expected_return_bps_by_prediction_bucket": [bucket]},
            "test": {"auc": 0.6, "expected_return_bps_by_prediction_bucket": [bucket]},
        },
    }
