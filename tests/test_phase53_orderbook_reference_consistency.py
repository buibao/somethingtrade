from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import OrderbookReferenceAuditor, SessionLineageBuilder
from tests.phase53_test_utils import clean_row, phase53_config, reference_row, write_good_session, write_jsonl


def _audit_first(tmp_path: Path) -> dict:
    config = phase53_config(tmp_path)
    lineage = SessionLineageBuilder(config).build()["sessions"][0]
    return OrderbookReferenceAuditor(config).audit_session(lineage)


def test_bid_lt_ask_pass(tmp_path: Path) -> None:
    write_good_session(tmp_path)

    result = _audit_first(tmp_path)

    assert result["bid_ask_crossed_count"] == 0
    assert result["reference_consistency_status"] == "pass"


def test_bid_gte_ask_fails(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    row = clean_row()
    row["best_bid"] = 102.0
    row["best_ask"] = 101.0
    write_jsonl(session / "data/dataset/orderbook_clean_samples.jsonl", [row])

    result = _audit_first(tmp_path)

    assert result["bid_ask_crossed_count"] == 1
    assert result["reference_consistency_status"] == "fail"


def test_negative_spread_fails(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    row = clean_row()
    row["spread"] = -1.0
    write_jsonl(session / "data/dataset/orderbook_clean_samples.jsonl", [row])

    result = _audit_first(tmp_path)

    assert result["spread_negative_count"] == 1
    assert result["reference_consistency_status"] == "fail"


def test_stale_incomplete_mismatch_counters_loaded_from_debug_artifacts(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/debug/stale_period_cases.jsonl", [{"case": 1}])
    write_jsonl(session / "data/debug/book_incomplete_cases.jsonl", [{"case": 1}])
    write_jsonl(session / "data/debug/orderbook_mismatch_cases.jsonl", [{"case": 1}])

    result = _audit_first(tmp_path)

    assert result["stale_book_counter"] == 1
    assert result["incomplete_book_counter"] == 1
    assert result["mismatch_counter"]["orderbook_mismatch_cases"] == 1
    assert result["reference_consistency_status"] == "fail"


def test_reference_rows_counted_separately_by_feed(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    write_jsonl(session / "data/dataset/bookticker_reference_quotes.jsonl", [reference_row(), reference_row()])
    write_jsonl(session / "data/dataset/trade_reference_events.jsonl", [reference_row()])
    write_jsonl(session / "data/dataset/aggtrade_reference_events.jsonl", [reference_row(), reference_row(), reference_row()])

    result = _audit_first(tmp_path)

    assert result["bookticker_reference_rows"] == 2
    assert result["trade_reference_rows"] == 1
    assert result["aggtrade_reference_rows"] == 3
