from __future__ import annotations

from app.research.edge_robustness_research import build_dataset_expansion_report, build_decision_gate_report
from phase51_test_utils import load_json


def test_phase51_compares_phase50_and_phase51_sample_counts() -> None:
    report = load_json("data/debug/phase_5_1_dataset_expansion_report.json")
    assert report["phase50_valid_label_count"] > 0
    assert report["phase51_valid_label_count"] == report["phase50_valid_label_count"]
    assert "increase_ratio" in report


def test_analysis_only_mode_documents_no_new_data() -> None:
    report = load_json("data/debug/phase_5_1_dataset_expansion_report.json")
    assert report["dataset_expansion_mode"] == "analysis_only_no_new_data"
    assert report["expansion_limitation_documented"] is True


def test_multi_session_count_reported() -> None:
    report = build_dataset_expansion_report(
        {"phase50_valid_label_count": 10, "phase50_edge_conclusion": "EDGE_INCONCLUSIVE"},
        {"input_mode": "multi_bundle", "bundle_count": 2, "bundles": [{"capture_duration_sec": 10}, {"capture_duration_sec": 20}]},
        [_sample(i) for i in range(25)],
    )
    assert report["session_count"] == 2
    assert report["total_capture_duration_sec"] == 30


def test_insufficient_expansion_blocks_robust_edge_claim() -> None:
    dataset = build_dataset_expansion_report(
        {"phase50_valid_label_count": 100, "phase50_edge_conclusion": "EDGE_INCONCLUSIVE"},
        {"input_mode": "phase50_existing_dataset", "bundle_count": 1, "bundles": [{"capture_duration_sec": 10}]},
        [_sample(i) for i in range(100)],
    )
    decision = build_decision_gate_report(
        baseline_lock={"status": "pass", "phase50_primary_horizon_ms": 100},
        input_manifest={"status": "pass"},
        dataset_expansion=dataset,
        label_distribution={"flat_label_risk": "low"},
        opportunity_filter={"stable_filter_count": 1},
        cost_sensitivity={"positive_edge_after_base_cost": True, "positive_edge_before_cost": True},
        auc_edge={},
        regime={"robust_across_regimes": True, "one_regime_only_dependency": False},
        topk={"robust_topk_edge_claim_allowed": True, "validation_test_support": True},
    )
    assert decision["edge_robustness_conclusion"] != "EDGE_ROBUST_ENOUGH_FOR_STRATEGY_SIMULATION"


def _sample(index: int) -> dict:
    return {"sample_id": f"s{index}", "feature_ts_ns": index, "valid_100ms_label": True}

