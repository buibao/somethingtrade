from __future__ import annotations

import zipfile

from app.research.microstructure_signal_research import build_final_report
from phase50_test_utils import ROOT, ensure_phase50_outputs, load_json


def test_phase50_final_reports_and_bundle_exist_with_exact_edge_conclusion() -> None:
    root = ensure_phase50_outputs()
    report = load_json("data/reports/phase_5_0_empirical_signal_report.json")
    assert report["edge_conclusion"] in {"EDGE_PROVEN", "EDGE_INCONCLUSIVE", "EDGE_FAILED"}
    assert set(report["allowed_edge_conclusions"]) == {"EDGE_PROVEN", "EDGE_INCONCLUSIVE", "EDGE_FAILED"}
    assert report["edge_conclusion"] == "EDGE_INCONCLUSIVE"
    assert report["primary_label_horizon_ms"] == 100
    assert not report["blockers"]
    assert report["warnings"]
    assert all(report["gates"].values())
    scope = report["research_scope_confirmation"]
    assert scope["live_trading"] is False
    assert scope["order_execution"] is False
    assert scope["private_key_or_wallet_logic"] is False
    assert scope["copy_trading"] is False
    assert scope["production_strategy_execution"] is False

    md_path = root / "data/reports/phase_5_0_empirical_signal_report.md"
    bundle_path = root / "phase_5_0_empirical_signal_research_bundle.zip"
    assert md_path.exists()
    assert bundle_path.exists()
    assert "Edge conclusion: EDGE_INCONCLUSIVE" in md_path.read_text(encoding="utf-8")
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    assert "data/reports/phase_5_0_empirical_signal_report.json" in names
    assert "data/reports/phase_5_0_empirical_signal_report.md" in names
    assert "data/debug/phase_5_0_evidence_integrity_report.json" in names
    assert "bot/app/research/microstructure_signal_research.py" in names


def test_phase50_files_do_not_add_execution_order_or_strategy_runtime_tokens() -> None:
    combined = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in (
            "bot/app/research/microstructure_signal_research.py",
            "scripts/run_phase50_microstructure_signal_research.py",
        )
    )
    forbidden = ("OrderIntent(", "PaperExecutor(", "ExecutionReport(", "PolymarketExecutor(", "place_order(")
    assert not any(token in combined for token in forbidden)


def test_phase50_edge_inconclusive_includes_warnings() -> None:
    report = build_final_report(
        source_gate={"status": "pass"},
        evidence={"status": "pass"},
        manifest={"status": "pass"},
        split_report=_passing_split_report(),
        feature_schema={"status": "pass"},
        label_report={"status": "pass", "primary_horizon_ms": 100},
        leakage={"status": "pass"},
        bucket_edge={"status": "pass", "edge_claim_allowed": False, "stable_edge_bucket_count": 0, "buckets": []},
        model_baseline={"status": "pass", "edge_claim_allowed": False, "metrics": {}},
    )
    assert report["edge_conclusion"] == "EDGE_INCONCLUSIVE"
    assert report["warnings"]


def test_phase50_edge_failed_includes_blockers() -> None:
    report = build_final_report(
        source_gate={"status": "fail"},
        evidence={"status": "pass"},
        manifest={"status": "pass"},
        split_report=_passing_split_report(),
        feature_schema={"status": "pass"},
        label_report={"status": "pass", "primary_horizon_ms": 100},
        leakage={"status": "pass"},
        bucket_edge=_supported_bucket_report(),
        model_baseline=_supported_model_report(),
    )
    assert report["edge_conclusion"] == "EDGE_FAILED"
    assert report["blockers"]
    assert "source_reproducibility_gate" in report["blockers"]


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


def _supported_bucket_report() -> dict:
    return {
        "status": "pass",
        "edge_claim_allowed": True,
        "stable_edge_bucket_count": 1,
        "buckets": [
            {
                "low_sample_bucket": False,
                "split_stability": {"validation_and_test_support": True},
                "splits": {
                    "validation": {"edge_after_cost_bps": 1.0},
                    "test": {"edge_after_cost_bps": 1.0},
                },
            }
        ],
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
