from __future__ import annotations

from app.research.edge_robustness_research import build_opportunity_filter_report
from phase51_test_utils import load_json


def test_opportunity_threshold_sweep_runs() -> None:
    report = load_json("data/debug/phase_5_1_opportunity_filter_report.json")
    assert report["threshold_sweep_completed"] is True
    assert report["filters"]


def test_selection_rate_reported() -> None:
    report = load_json("data/debug/phase_5_1_opportunity_filter_report.json")
    first = report["filters"][0]["splits"]["validation"]
    assert "selection_rate" in first


def test_low_sample_filter_flagged() -> None:
    report = build_opportunity_filter_report([_sample("train", 0), _sample("validation", 1), _sample("test", 2)])
    assert any(item["low_sample_filter"] for item in report["filters"])


def test_filter_requires_validation_and_test_support() -> None:
    report = load_json("data/debug/phase_5_1_opportunity_filter_report.json")
    assert report["validation_test_support_required"] is True
    assert all("edge_claim_allowed" in item for item in report["filters"])


def test_filter_cannot_claim_edge_from_train_only() -> None:
    samples = [_sample("train", i, ret=10.0) for i in range(40)]
    samples += [_sample("validation", i + 100, ret=-10.0) for i in range(40)]
    samples += [_sample("test", i + 200, ret=-10.0) for i in range(40)]
    report = build_opportunity_filter_report(samples)
    assert report["stable_filter_count"] == 0


def _sample(split: str, index: int, *, ret: float = 1.0) -> dict:
    return {
        "sample_id": f"{split}-{index}",
        "split": split,
        "valid_100ms_label": True,
        "future_return_100ms_bps": ret,
        "features": {
            "repricing_gap_bps": 20.0,
            "target_spread_bps": 1.0,
            "reference_bookticker_age_ms": 1.0,
            "latency_quality_score": 1.0,
            "reference_trade_count_1s": 20.0,
        },
    }

