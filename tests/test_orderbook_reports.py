from __future__ import annotations

import json

from orderbook_phase41_test_utils import make_depth_update, make_processor


def test_tc_69_quality_report_json_schema(tmp_path) -> None:
    processor = make_processor(tmp_path)
    summary = processor.write_reports(duration_sec=1.0)
    payload = json.loads((tmp_path / "orderbook_quality_report.json").read_text())
    required = {
        "messages_received",
        "messages_parsed",
        "deltas_accepted",
        "deltas_rejected",
        "sequence_gap_count",
        "samples_emitted",
        "phase_4_1_pass",
    }
    assert required <= set(payload)
    assert payload["messages_received"] == summary["messages_received"]


def test_tc_70_lifecycle_report_json_schema(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.write_reports(duration_sec=1.0)
    payload = json.loads((tmp_path / "ws_lifecycle_report.json").read_text())
    required = {
        "connect_count",
        "disconnect_count",
        "snapshot_loaded_count",
        "sequence_gap_count",
        "duplicate_messages_skipped",
        "market_status_known",
    }
    assert required <= set(payload)
    assert payload["market_status_known"] is False


def test_tc_71_mismatch_jsonl_schema(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.validate_reported_best(
        "BTCUSDT",
        reported_best_bid="99.50",
        reported_best_ask="101.00",
        first_update_id=101,
        final_update_id=101,
        raw_message_excerpt="depth",
    )
    row = json.loads((tmp_path / "orderbook_mismatch_cases.jsonl").read_text().splitlines()[-1])
    required = {
        "case_type",
        "symbol",
        "state_version",
        "snapshot_version",
        "computed_best_bid",
        "reported_best_bid",
        "strict_mismatch",
        "tolerant_mismatch",
        "raw_message_hash",
    }
    assert required <= set(row)
    assert row["raw_message_hash"] is None


def test_tc_72_sequence_gap_jsonl_schema(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.process_depth_update(
        make_depth_update(first_update_id=105, final_update_id=110)
    )
    row = json.loads((tmp_path / "sequence_gap_cases.jsonl").read_text().splitlines()[-1])
    required = {
        "case_type",
        "previous_last_update_id",
        "expected_next_update_id",
        "received_first_update_id",
        "received_final_update_id",
        "action_taken",
    }
    assert required <= set(row)


def test_tc_73_markdown_report_generated_and_summarizes_counters(tmp_path) -> None:
    processor = make_processor(tmp_path)
    processor.write_reports(duration_sec=1.0)
    text = (tmp_path / "phase_4_1_orderbook_quality_report.md").read_text()
    assert "Messages received" in text
    assert "Strict mismatch count" in text
    assert "Dataset clean enough for Phase 4.2" in text


def test_tc_74_clean_sample_schema_generated(tmp_path) -> None:
    make_processor(tmp_path)
    row = json.loads((tmp_path / "orderbook_clean_samples.jsonl").read_text().splitlines()[-1])
    required = {
        "schema_version",
        "symbol",
        "source",
        "state_version",
        "snapshot_version",
        "last_update_id",
        "depth_n",
        "bids",
        "asks",
        "best_bid",
        "best_ask",
        "quality",
        "lifecycle",
    }
    assert required <= set(row)
    assert row["schema_version"] == "phase_4_1_clean_orderbook_v1"
    assert row["quality"]["is_valid"] is True


def test_tc_75_runtime_summary_includes_pass_fail_conclusion(tmp_path) -> None:
    processor = make_processor(tmp_path)
    summary = processor.write_reports(duration_sec=1.0)
    assert "phase_4_1_pass" in summary
    assert isinstance(summary["phase_4_1_pass"], bool)
