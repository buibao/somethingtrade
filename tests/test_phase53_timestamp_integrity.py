from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import SessionLineageBuilder, TimestampIntegrityAuditor
from tests.phase53_test_utils import clean_row, label_row, phase53_config, write_good_session, write_jsonl


def _audit_first(tmp_path: Path) -> dict:
    config = phase53_config(tmp_path)
    lineage = SessionLineageBuilder(config).build()["sessions"][0]
    return TimestampIntegrityAuditor(config).audit_session(lineage)


def test_monotonic_receive_timestamps_pass(tmp_path: Path) -> None:
    write_good_session(tmp_path)

    result = _audit_first(tmp_path)

    assert result["monotonic_violation_count"] == 0
    assert result["timestamp_status"] == "pass"


def test_monotonic_violation_detected(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/orderbook_clean_samples.jsonl", [clean_row(receive_ns=2_000_000_000), clean_row(receive_ns=1_000_000_000)])

    result = _audit_first(tmp_path)

    assert result["monotonic_violation_count"] == 1
    assert result["timestamp_status"] == "fail"


def test_negative_latency_detected(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(
        session / "data/dataset/orderbook_clean_samples.jsonl",
        [clean_row(wall="2026-05-30T00:00:00+00:00", exchange_ns=1_798_000_000_000_000_000)],
    )

    result = _audit_first(tmp_path)

    assert result["negative_latency_count"] == 1
    assert result["timestamp_status"] == "fail"


def test_future_label_leakage_detected(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", [label_row(future_leak=True)])

    result = _audit_first(tmp_path)

    assert result["future_timestamp_leak_count"] > 0
    assert result["timestamp_status"] == "fail"


def test_100ms_horizon_verifiable_when_fields_present(tmp_path: Path) -> None:
    write_good_session(tmp_path)

    result = _audit_first(tmp_path)

    assert result["horizon_100ms_timestamp_verifiable"] is True


def test_ambiguous_timestamp_aliases_lead_to_partial_not_false_pass(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl", [{"symbol": "BTCUSDT"}])

    result = _audit_first(tmp_path)

    assert result["timestamp_status"] == "partial"
    assert result["horizon_100ms_timestamp_verifiable"] is False
