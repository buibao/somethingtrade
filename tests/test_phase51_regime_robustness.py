from __future__ import annotations

from app.research.edge_robustness_research import build_regime_robustness_report
from phase51_test_utils import load_json


def test_regime_report_created() -> None:
    report = load_json("data/debug/phase_5_1_regime_robustness_report.json")
    assert report["status"] == "pass"
    assert report["groupings"]


def test_latency_quality_grouping_included() -> None:
    report = load_json("data/debug/phase_5_1_regime_robustness_report.json")
    assert report["latency_quality_grouping_included"] is True
    assert any(row["regime_name"] == "latency_quality_bucket" for row in report["groupings"])


def test_time_or_session_grouping_included() -> None:
    report = load_json("data/debug/phase_5_1_regime_robustness_report.json")
    assert report["at_least_one_time_or_session_grouping"] is True
    assert any(row["regime_name"] in {"session_id", "time_bucket"} for row in report["groupings"])


def test_unstable_regime_flagged() -> None:
    report = load_json("data/debug/phase_5_1_regime_robustness_report.json")
    assert report["unstable_regimes_flagged"] is True


def test_one_regime_only_edge_not_robust() -> None:
    samples = [_sample(i, session="only") for i in range(40)]
    report = build_regime_robustness_report(samples)
    assert report["one_regime_only_dependency"] is True
    assert report["robust_across_regimes"] is False


def _sample(index: int, *, session: str) -> dict:
    return {
        "sample_id": f"s{index}",
        "session_id": session,
        "feature_ts_ns": index,
        "valid_100ms_label": True,
        "direction_100ms": 1,
        "future_return_100ms_bps": 5.0,
        "features": {
            "repricing_gap_bps": 1.0,
            "target_spread_bps": 0.01,
            "latency_quality_score": 1.0,
            "reference_bookticker_age_ms": 1.0,
            "reference_trade_count_1s": 10.0,
        },
    }

