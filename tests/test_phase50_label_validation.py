from __future__ import annotations

import pytest

from app.research.microstructure_signal_research import validate_primary_horizon_ms
from phase50_test_utils import load_json


def test_phase50_label_validation_keeps_strict_100ms_primary() -> None:
    report = load_json("data/debug/phase_5_0_label_validation_report.json")
    assert report["status"] == "pass"
    assert report["primary_horizon_ms"] == 100
    assert report["primary_horizon_relaxed_to_250ms"] is False
    assert report["max_future_gap_ms"] <= 100
    assert report["valid_100ms_label_count"] > 0
    assert set(report["generated_fields"]) == {
        "future_return_100ms_bps",
        "direction_100ms",
        "spread_adjusted_direction_100ms",
        "valid_100ms_label",
    }
    assert report["diagnostic_horizon_ms"] == 250
    assert report["diagnostic_is_primary"] is False


def test_phase50_primary_horizon_cannot_be_relaxed_to_250ms() -> None:
    with pytest.raises(ValueError, match="100ms"):
        validate_primary_horizon_ms(250)
