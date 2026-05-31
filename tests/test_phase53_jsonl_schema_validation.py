from __future__ import annotations

import json
from pathlib import Path

from app.research.phase53_dataset_integrity import DatasetSchemaValidator
from tests.phase53_test_utils import clean_row, phase53_config, write_jsonl


def test_valid_jsonl_file_count(tmp_path: Path) -> None:
    validator = DatasetSchemaValidator(phase53_config(tmp_path))
    path = tmp_path / "orderbook_clean_samples.jsonl"
    write_jsonl(path, [clean_row(), clean_row(receive_ns=2_000_000_000)])

    result = validator.validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["row_count"] == 2
    assert result["schema_status"] == "pass"


def test_invalid_jsonl_row_count(tmp_path: Path) -> None:
    validator = DatasetSchemaValidator(phase53_config(tmp_path))
    path = tmp_path / "orderbook_clean_samples.jsonl"
    path.write_text(json.dumps(clean_row()) + "\n{bad\n", encoding="utf-8")

    result = validator.validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["row_count"] == 1
    assert result["parse_error_count"] == 1
    assert result["schema_status"] == "fail"


def test_empty_file_handling(tmp_path: Path) -> None:
    path = tmp_path / "orderbook_clean_samples.jsonl"
    path.write_text("", encoding="utf-8")

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["row_count"] == 0
    assert result["schema_status"] == "fail"


def test_required_field_missing_handling(tmp_path: Path) -> None:
    path = tmp_path / "orderbook_clean_samples.jsonl"
    write_jsonl(path, [{"symbol": "BTCUSDT", "best_bid": 100.0, "local_recv_monotonic_ns": 1}])

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["required_field_failures"] >= 1
    assert result["schema_status"] == "partial"


def test_numeric_invalid_handling(tmp_path: Path) -> None:
    path = tmp_path / "bookticker_reference_quotes.jsonl"
    write_jsonl(path, [{"symbol": "BTCUSDT", "price": "not-number"}])

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="bookticker_reference_quotes.jsonl")

    assert result["type_failures"] >= 1
    assert result["schema_status"] == "fail"


def test_bid_gte_ask_handling(tmp_path: Path) -> None:
    row = clean_row()
    row["best_bid"] = 101.0
    row["best_ask"] = 100.0
    path = tmp_path / "orderbook_clean_samples.jsonl"
    write_jsonl(path, [row])

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["numeric_sanity_failures"] >= 1
    assert result["schema_status"] == "fail"


def test_negative_size_handling(tmp_path: Path) -> None:
    path = tmp_path / "trade_reference_events.jsonl"
    write_jsonl(path, [{"symbol": "BTCUSDT", "price": 100.0, "quantity": -1.0}])

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="trade_reference_events.jsonl")

    assert result["numeric_sanity_failures"] >= 1
    assert result["schema_status"] == "fail"


def test_bounded_sample_errors(tmp_path: Path) -> None:
    path = tmp_path / "orderbook_clean_samples.jsonl"
    path.write_text("{bad\n" * 50, encoding="utf-8")

    result = DatasetSchemaValidator(phase53_config(tmp_path)).validate_file(path, file_name="orderbook_clean_samples.jsonl")

    assert result["parse_error_count"] == 50
    assert len(result["sample_errors"]) == 20
