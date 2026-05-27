from __future__ import annotations

from pathlib import Path

from app.research.edge_robustness_research import BASELINE_REQUIRED_ARTIFACTS, build_phase50_baseline_lock
from phase51_test_utils import ROOT, load_json, write_json


def test_phase51_requires_phase50_final_report(tmp_path: Path) -> None:
    report = build_phase50_baseline_lock(root_path=tmp_path, phase50_report_path=tmp_path / "missing.json", primary_horizon_ms=100)
    assert report["status"] == "fail"
    assert report["phase51_allowed_to_start"] is False


def test_phase51_requires_phase50_primary_horizon_100ms() -> None:
    report = build_phase50_baseline_lock(root_path=ROOT, phase50_report_path=ROOT / "data/reports/phase_5_0_empirical_signal_report.json", primary_horizon_ms=250)
    assert report["status"] == "fail"
    assert report["requested_primary_horizon_ms"] == 250


def test_phase51_rejects_phase50_leakage_failure(tmp_path: Path) -> None:
    _write_minimal_phase50_tree(tmp_path, leakage_status="fail")
    report = build_phase50_baseline_lock(root_path=tmp_path, phase50_report_path=tmp_path / "data/reports/phase_5_0_empirical_signal_report.json", primary_horizon_ms=100)
    assert report["status"] == "fail"
    assert report["phase50_leakage_check_pass"] is False


def test_phase51_accepts_phase50_edge_inconclusive() -> None:
    final_report = load_json("data/reports/phase_5_1_edge_robustness_report.json")
    baseline = build_phase50_baseline_lock(root_path=ROOT, phase50_report_path=ROOT / "data/reports/phase_5_0_empirical_signal_report.json", primary_horizon_ms=100)
    assert baseline["status"] == "pass"
    assert baseline["phase50_edge_conclusion"] == "EDGE_INCONCLUSIVE"
    assert baseline["phase50_edge_inconclusive_accepted"] is True
    assert final_report["phase50_baseline_valid"] is True


def _write_minimal_phase50_tree(root: Path, *, leakage_status: str) -> None:
    for artifact in BASELINE_REQUIRED_ARTIFACTS:
        payload = {"status": "pass"}
        if artifact.endswith("phase_5_0_empirical_signal_report.json"):
            payload = {
                "primary_label_horizon_ms": 100,
                "edge_conclusion": "EDGE_INCONCLUSIVE",
                "research_scope_confirmation": {
                    "live_trading": False,
                    "order_execution": False,
                    "private_key_or_wallet_logic": False,
                },
            }
        if artifact.endswith("phase_5_0_label_validation_report.json"):
            payload = {"status": "pass", "primary_horizon_ms": 100, "valid_100ms_label_count": 10}
        if artifact.endswith("phase_5_0_leakage_check.json"):
            payload = {"status": leakage_status}
        write_json(root / artifact, payload)

