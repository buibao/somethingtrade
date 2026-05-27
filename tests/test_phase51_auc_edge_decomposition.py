from __future__ import annotations

from phase51_test_utils import load_json


def test_nonflat_auc_reported() -> None:
    report = load_json("data/debug/phase_5_1_auc_edge_decomposition_report.json")
    assert "nonflat_only_auc" in report


def test_tradable_move_auc_reported() -> None:
    report = load_json("data/debug/phase_5_1_auc_edge_decomposition_report.json")
    assert "tradable_move_only_auc" in report


def test_prediction_decile_edge_reported() -> None:
    report = load_json("data/debug/phase_5_1_auc_edge_decomposition_report.json")
    assert report["prediction_deciles"]
    assert "edge_after_cost_bps" in report["prediction_deciles"][0]


def test_high_auc_negative_edge_is_explained() -> None:
    report = load_json("data/debug/phase_5_1_auc_edge_decomposition_report.json")
    assert report["auc_edge_mismatch_explanation"]
    if report["all_sample_auc"] and report["all_sample_auc"] > 0.7:
        assert "AUC" in report["auc_edge_mismatch_explanation"] or "ranking" in report["auc_edge_mismatch_explanation"]


def test_flat_label_auc_inflation_risk_reported() -> None:
    report = load_json("data/debug/phase_5_1_auc_edge_decomposition_report.json")
    assert report["flat_label_auc_inflation_risk"] in {"low", "medium", "high"}

