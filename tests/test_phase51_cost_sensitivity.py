from __future__ import annotations

from phase51_test_utils import load_json


def test_zero_optimistic_base_conservative_stress_costs_exist() -> None:
    report = load_json("data/debug/phase_5_1_cost_sensitivity_report.json")
    assert set(report["cost_scenarios"]) == {"zero_cost", "optimistic_cost", "base_cost", "conservative_cost", "stress_cost"}


def test_break_even_cost_reported() -> None:
    report = load_json("data/debug/phase_5_1_cost_sensitivity_report.json")
    assert "break_even_cost_bps" in report
    assert report["break_even_cost_bps"] is not None


def test_edge_disappears_after_cost_is_documented() -> None:
    report = load_json("data/debug/phase_5_1_cost_sensitivity_report.json")
    assert "edge_disappears_only_after_cost" in report
    assert "raw_edge_assessment" in report


def test_realistic_cost_assessment_required() -> None:
    report = load_json("data/debug/phase_5_1_cost_sensitivity_report.json")
    assert "realistic_cost_assessment" in report
    assert isinstance(report["break_even_cost_realistic"], bool)

