from __future__ import annotations

from app.research.edge_robustness_research import build_label_distribution_report
from phase51_test_utils import load_json


def test_flat_ratio_reported() -> None:
    report = load_json("data/debug/phase_5_1_label_distribution_report.json")
    assert "flat_ratio" in report
    assert report["flat_ratio"] > 0.9


def test_nonflat_ratio_reported() -> None:
    report = load_json("data/debug/phase_5_1_label_distribution_report.json")
    assert "nonflat_ratio" in report
    assert report["nonflat_ratio"] < 0.1


def test_tradable_move_rate_reported() -> None:
    report = load_json("data/debug/phase_5_1_label_distribution_report.json")
    assert "tradable_move_rate" in report
    assert "tradable_move_count" in report


def test_high_flat_ratio_sets_high_flat_label_risk() -> None:
    report = build_label_distribution_report([_sample(i, 0, 0.0) for i in range(95)] + [_sample(i + 100, 1, 3.0) for i in range(5)])
    assert report["flat_label_risk"] == "high"
    assert report["auc_may_be_inflated_by_flat_label_dominance"] is True


def test_nonflat_only_evaluation_required() -> None:
    report = load_json("data/debug/phase_5_1_label_distribution_report.json")
    assert "nonflat_only_diagnostics" in report
    assert "tradable_move_only_diagnostics" in report


def _sample(index: int, direction: int, ret: float) -> dict:
    return {
        "sample_id": f"s{index}",
        "valid_100ms_label": True,
        "direction_100ms": direction,
        "future_return_100ms_bps": ret,
    }

