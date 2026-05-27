from __future__ import annotations

import zipfile

from phase50_test_utils import ROOT, ensure_phase50_outputs, load_json


def test_phase50_final_reports_and_bundle_exist_with_exact_edge_conclusion() -> None:
    root = ensure_phase50_outputs()
    report = load_json("data/reports/phase_5_0_empirical_signal_report.json")
    assert report["edge_conclusion"] in {"EDGE_PROVEN", "EDGE_INCONCLUSIVE", "EDGE_FAILED"}
    assert report["edge_conclusion"] == "EDGE_INCONCLUSIVE"
    assert not report["blockers"]
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
