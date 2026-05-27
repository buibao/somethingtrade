from __future__ import annotations

from phase50_test_utils import load_json


def test_phase50_split_is_deterministic_chronological_and_non_overlapping() -> None:
    report = load_json("data/debug/phase_5_0_split_report.json")
    assert report["status"] == "pass"
    assert report["split_method"] == "deterministic_chronological_time_based"
    assert report["random_split_used"] is False
    assert report["random_split_rejected"] is True
    assert report["duplicate_sample_ids"] == []
    assert report["overlap_pairs"] == []
    assert report["time_overlap_violations"] == []
    assert report["splits"]["train"]["time_range_ns"]["max"] < report["splits"]["validation"]["time_range_ns"]["min"]
    assert report["splits"]["validation"]["time_range_ns"]["max"] < report["splits"]["test"]["time_range_ns"]["min"]
    assert all(report["splits"][split]["sample_count"] > 0 for split in ("train", "validation", "test"))
