from __future__ import annotations

from app.research.edge_robustness_research import build_topk_edge_report
from phase51_test_utils import load_json


def test_multiple_topk_slices_evaluated() -> None:
    report = load_json("data/debug/phase_5_1_topk_edge_report.json")
    assert set(report["topk_slices"]) == {0.1, 0.5, 1.0, 2.0, 5.0, 10.0}


def test_validation_and_test_topk_reported() -> None:
    report = load_json("data/debug/phase_5_1_topk_edge_report.json")
    splits = {row["split"] for row in report["rows"]}
    assert {"validation", "test"} <= splits
    assert report["validation_and_test_reported"] is True


def test_low_sample_topk_flagged() -> None:
    report = load_json("data/debug/phase_5_1_topk_edge_report.json")
    assert report["low_sample_topk_flagged"] is True


def test_topk_edge_after_cost_reported() -> None:
    report = load_json("data/debug/phase_5_1_topk_edge_report.json")
    assert report["topk_edge_after_cost_reported"] is True
    assert "edge_after_cost_bps" in report["rows"][0]


def test_topk_train_only_edge_not_robust() -> None:
    samples = [_sample("train", i, 10.0) for i in range(100)]
    samples += [_sample("validation", i, -10.0) for i in range(100)]
    samples += [_sample("test", i, -10.0) for i in range(100)]
    report = build_topk_edge_report(samples)
    assert report["train_only_edge_not_robust"] is True
    assert report["robust_topk_edge_claim_allowed"] is False


def _sample(split: str, index: int, ret: float) -> dict:
    return {
        "sample_id": f"{split}-{index}",
        "split": split,
        "valid_100ms_label": True,
        "future_return_100ms_bps": ret,
        "features": {
            "repricing_gap_bps": 10.0,
            "latency_quality_score": 1.0,
            "reference_trade_count_1s": 10.0,
        },
    }

