from __future__ import annotations

from app.research.edge_robustness_research import ALLOWED_DECISIONS, build_decision_gate_report
from phase51_test_utils import load_json


def test_decision_gate_report_created() -> None:
    report = load_json("data/debug/phase_5_1_decision_gate_report.json")
    assert report["status"] == "pass"
    assert report["decision_backed_by_reports"] is True


def test_final_conclusion_is_allowed_value() -> None:
    report = load_json("data/debug/phase_5_1_decision_gate_report.json")
    assert report["edge_robustness_conclusion"] in ALLOWED_DECISIONS
    assert report["final_conclusion_is_allowed_value"] is True


def test_robust_strategy_simulation_requires_validation_and_test_edge() -> None:
    decision = build_decision_gate_report(
        baseline_lock=_baseline(),
        input_manifest={"status": "pass"},
        dataset_expansion={"sufficient_expansion_for_edge_claim": True},
        label_distribution={"flat_label_risk": "low"},
        opportunity_filter={"stable_filter_count": 0},
        cost_sensitivity={"positive_edge_after_base_cost": True, "positive_edge_before_cost": True},
        auc_edge={},
        regime={"robust_across_regimes": True, "one_regime_only_dependency": False},
        topk={"robust_topk_edge_claim_allowed": False, "validation_test_support": False},
    )
    assert decision["edge_robustness_conclusion"] != "EDGE_ROBUST_ENOUGH_FOR_STRATEGY_SIMULATION"


def test_weak_edge_maps_to_more_data_recommendation() -> None:
    decision = build_decision_gate_report(
        baseline_lock=_baseline(),
        input_manifest={"status": "pass"},
        dataset_expansion={"sufficient_expansion_for_edge_claim": True},
        label_distribution={"flat_label_risk": "low"},
        opportunity_filter={"stable_filter_count": 0},
        cost_sensitivity={"positive_edge_after_base_cost": False, "positive_edge_before_cost": True},
        auc_edge={},
        regime={"robust_across_regimes": False, "one_regime_only_dependency": False},
        topk={"robust_topk_edge_claim_allowed": False, "validation_test_support": False},
    )
    assert decision["edge_robustness_conclusion"] == "EDGE_WEAK_BUT_WORTH_MORE_DATA"
    assert "data" in decision["next_phase_recommendation"]


def test_no_raw_edge_maps_to_stop_signal_branch() -> None:
    decision = build_decision_gate_report(
        baseline_lock=_baseline(),
        input_manifest={"status": "pass"},
        dataset_expansion={"sufficient_expansion_for_edge_claim": True},
        label_distribution={"flat_label_risk": "low"},
        opportunity_filter={"stable_filter_count": 0},
        cost_sensitivity={"positive_edge_after_base_cost": False, "positive_edge_before_cost": False},
        auc_edge={},
        regime={"robust_across_regimes": False, "one_regime_only_dependency": False},
        topk={"robust_topk_edge_claim_allowed": False, "validation_test_support": False},
    )
    assert decision["edge_robustness_conclusion"] == "EDGE_FAILED_STOP_SIGNAL_BRANCH"


def test_no_live_trading_execution_wallet_flags() -> None:
    final = load_json("data/reports/phase_5_1_edge_robustness_report.json")
    assert final["no_live_trading"] is True
    assert final["no_execution"] is True
    assert final["no_wallet_logic"] is True


def _baseline() -> dict:
    return {"status": "pass", "phase50_primary_horizon_ms": 100, "phase50_edge_conclusion": "EDGE_INCONCLUSIVE"}

