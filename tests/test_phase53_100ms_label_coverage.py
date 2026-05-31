from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import LabelCoverageAuditor, SessionLineageBuilder
from tests.phase53_test_utils import label_row, phase53_config, write_good_session, write_jsonl


def _lineage(tmp_path: Path) -> tuple:
    config = phase53_config(tmp_path)
    return config, SessionLineageBuilder(config).build()["sessions"][0]


def test_coverage_ratio_computation(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/orderbook_clean_samples.jsonl", [{}, {}])
    write_jsonl(session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", [label_row(valid=True), label_row(valid=False, invalid_reason="gap")])
    config, lineage = _lineage(tmp_path)

    result = LabelCoverageAuditor(config, coverage_threshold=0.5).audit_session(lineage)

    assert result["coverage_ratio"] == 0.5
    assert result["labeled_100ms_observations"] == 1


def test_missing_label_reason_counter(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", [label_row(valid=False, invalid_reason="FUTURE_REFERENCE_GAP_TOO_LARGE")])
    config, lineage = _lineage(tmp_path)

    result = LabelCoverageAuditor(config, coverage_threshold=0.1).audit_session(lineage)

    assert result["missing_reason_counter"]["FUTURE_REFERENCE_GAP_TOO_LARGE"] == 1


def test_future_leak_blocks_eligibility(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", [label_row(future_leak=True)])
    config, lineage = _lineage(tmp_path)

    result = LabelCoverageAuditor(config).audit_session(lineage)

    assert result["future_leak_count"] > 0
    assert result["research_eligible_100ms"] is False


def test_missing_threshold_produces_partial(tmp_path: Path) -> None:
    write_good_session(tmp_path)
    config, lineage = _lineage(tmp_path)

    result = LabelCoverageAuditor(config, coverage_threshold=None).audit_session(lineage)

    assert result["coverage_status"] == "partial"
    assert result["research_eligible_100ms"] == "partial"
    assert "missing_explicit_100ms_coverage_threshold" in result["hard_fail_reasons"]


def test_strict_100ms_observability_ready_false_blocks_eligibility(tmp_path: Path) -> None:
    write_good_session(tmp_path)
    config, lineage = _lineage(tmp_path)
    lineage["strict_100ms_observability_ready"] = False

    result = LabelCoverageAuditor(config).audit_session(lineage)

    assert result["coverage_status"] == "fail"
    assert "strict_100ms_observability_ready_not_true" in result["hard_fail_reasons"]
