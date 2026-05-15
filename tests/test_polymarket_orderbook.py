from pathlib import Path

import orjson
import pytest

from app.marketdata.polymarket_orderbook import PolymarketLocalOrderBook, TokenBookMetadata
from app.marketdata.polymarket_discovery import PolymarketMarketMetadata
from app.marketdata.polymarket_ws import PolymarketWSClient


def _book(
    *,
    best_validation_mode: str = "strict",
    mismatch_sample_path: Path | str | None = None,
    mismatch_sample_per_token_per_min: int = 20,
) -> PolymarketLocalOrderBook:
    return PolymarketLocalOrderBook(
        token_metadata={
            "token-up": TokenBookMetadata(
                condition_id="0xcondition",
                market_id="0xmarket",
                side_label="UP",
                market_slug="bitcoin-up-or-down-15m",
                base_asset="BTC",
                duration_minutes=15,
                token_outcome="Up",
                tick_size=0.01,
            )
        },
        stale_after_ms=60_000.0,
        best_validation_mode=best_validation_mode,  # type: ignore[arg-type]
        mismatch_sample_path=mismatch_sample_path,
        mismatch_sample_per_token_per_min=mismatch_sample_per_token_per_min,
    )


def _snapshot(book: PolymarketLocalOrderBook, *, ts: int = 1_000) -> None:
    book.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-up",
            "market": "0xmarket",
            "bids": [
                {"price": "0.48", "size": "30"},
                {"price": "0.50", "size": "15"},
            ],
            "asks": [
                {"price": "0.52", "size": "25"},
                {"price": "0.53", "size": "60"},
            ],
        },
        received_ts=ts,
        parse_done_ts=ts + 1,
        recv_monotonic_ns=ts,
        parse_done_monotonic_ns=ts + 1,
        event_ts=ts,
        sequence="hash-1",
    )


def test_book_snapshot_creates_correct_best_bid_ask_sizes() -> None:
    orderbook = _book()

    quote = orderbook.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-up",
            "market": "0xmarket",
            "bids": [
                {"price": "0.48", "size": "30"},
                {"price": "0.50", "size": "15"},
            ],
            "asks": [
                {"price": "0.52", "size": "25"},
                {"price": "0.53", "size": "60"},
            ],
        },
        received_ts=1_000,
        parse_done_ts=1_001,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_001,
        event_ts=900,
        sequence="hash-1",
    )

    assert quote.best_bid == pytest.approx(0.50)
    assert quote.best_bid_size == pytest.approx(15.0)
    assert quote.best_ask == pytest.approx(0.52)
    assert quote.best_ask_size == pytest.approx(25.0)
    assert quote.mid_price == pytest.approx(0.51)
    assert quote.spread == pytest.approx(0.02)
    assert quote.book_complete is True
    assert quote.book_hash == "hash-1"


def test_price_change_buy_updates_bid_side() -> None:
    orderbook = _book()
    _snapshot(orderbook)

    quote = orderbook.apply_price_change(
        {"asset_id": "token-up", "side": "BUY", "price": "0.51", "size": "8"},
        parent_payload={"market": "0xmarket"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.best_bid == pytest.approx(0.51)
    assert quote.best_bid_size == pytest.approx(8.0)
    assert quote.best_ask == pytest.approx(0.52)
    assert quote.best_ask_size == pytest.approx(25.0)


def test_price_change_before_snapshot_does_not_mark_book_complete() -> None:
    orderbook = _book()

    quote = orderbook.apply_price_change(
        {"asset_id": "token-up", "side": "BUY", "price": "0.51", "size": "8"},
        parent_payload={"market": "0xmarket"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )
    readiness = orderbook.token_readiness_snapshot()["token-up"]

    assert quote.book_complete is False
    assert quote.book_has_snapshot is False
    assert quote.book_structurally_complete is False
    assert quote.book_update_type == "price_change"
    assert readiness["price_change_before_snapshot_count"] == 1
    assert readiness["delta_count"] == 1
    assert readiness["first_book_snapshot_ts_ns"] is None


def test_book_snapshot_marks_token_complete() -> None:
    orderbook = _book()

    quote = orderbook.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-up",
            "market": "0xmarket",
            "bids": [{"price": "0.50", "size": "15"}],
            "asks": [{"price": "0.52", "size": "25"}],
        },
        received_ts=1_000,
        parse_done_ts=1_001,
        recv_monotonic_ns=1_000,
        parse_done_monotonic_ns=1_001,
        event_ts=900,
        sequence="hash-1",
    )
    readiness = orderbook.token_readiness_snapshot()["token-up"]

    assert quote.book_complete is True
    assert quote.book_has_snapshot is True
    assert quote.book_structurally_complete is True
    assert quote.reported_best_validation_ok is True
    assert quote.book_update_type == "book"
    assert readiness["first_book_snapshot_ts_ns"] == 1_000
    assert readiness["first_complete_quote_ts_ns"] == 1_000
    assert readiness["snapshot_count"] == 1
    assert readiness["book_complete_true_count"] == 1


def test_both_up_down_snapshots_mark_market_book_ready() -> None:
    market = PolymarketMarketMetadata(
        condition_id="0xcondition",
        market_id="0xmarket",
        market_slug="bitcoin-up-or-down-15m",
        question="Bitcoin Up or Down - 15 minute",
        end_time="2099-05-15T12:15:00Z",
        event_start_time="2000-01-01T00:00:00Z",
        tick_size=0.01,
        min_order_size=5.0,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
        selected_for_runtime=True,
        signal_enabled=True,
        up_token_id="token-up",
        down_token_id="token-down",
        token_outcomes={"token-up": "Up", "token-down": "Down"},
        base_asset="BTC",
        duration_minutes=15,
    )
    client = PolymarketWSClient(markets=(market,), mismatch_sample_path=None)
    for token_id in ("token-up", "token-down"):
        client.normalize_message(
            orjson.dumps(
                {
                "event_type": "book",
                "asset_id": token_id,
                "market": "0xmarket",
                "bids": [{"price": "0.50", "size": "15"}],
                "asks": [{"price": "0.52", "size": "25"}],
                "timestamp": "1700000000000",
                }
            )
        )

    readiness = client.book_readiness_snapshot(now_ts=1_700_000_001_000_000_000)
    market_row = readiness["markets"][0]

    assert market_row["up_token_book_complete"] is True
    assert market_row["down_token_book_complete"] is True
    assert market_row["both_tokens_complete"] is True
    assert readiness["summary"]["complete_markets"] == 1


def test_price_change_sell_updates_ask_side() -> None:
    orderbook = _book()
    _snapshot(orderbook)

    quote = orderbook.apply_price_change(
        {"asset_id": "token-up", "side": "SELL", "price": "0.515", "size": "7"},
        parent_payload={"market": "0xmarket"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.best_bid == pytest.approx(0.50)
    assert quote.best_bid_size == pytest.approx(15.0)
    assert quote.best_ask == pytest.approx(0.515)
    assert quote.best_ask_size == pytest.approx(7.0)


def test_size_zero_removes_price_level() -> None:
    orderbook = _book()
    _snapshot(orderbook)

    quote = orderbook.apply_price_change(
        {"asset_id": "token-up", "side": "BUY", "price": "0.50", "size": "0"},
        parent_payload={"market": "0xmarket"},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.best_bid == pytest.approx(0.48)
    assert quote.best_bid_size == pytest.approx(30.0)


def test_best_bid_ask_without_size_does_not_corrupt_known_size() -> None:
    orderbook = _book()
    _snapshot(orderbook)

    compatible = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.50",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    incompatible = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.50",
            "best_ask": "0.51",
        },
        received_ts=3_000,
        parse_done_ts=3_001,
        recv_monotonic_ns=3_000,
        parse_done_monotonic_ns=3_001,
        event_ts=3_000,
        sequence=None,
    )

    restored = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.50",
            "best_ask": "0.52",
        },
        received_ts=4_000,
        parse_done_ts=4_001,
        recv_monotonic_ns=4_000,
        parse_done_monotonic_ns=4_001,
        event_ts=4_000,
        sequence=None,
    )

    assert compatible.best_bid_size == pytest.approx(15.0)
    assert compatible.best_ask_size == pytest.approx(25.0)
    assert compatible.book_complete is True
    assert incompatible.best_ask == pytest.approx(0.51)
    assert incompatible.best_ask_size is None
    assert incompatible.book_complete is False
    assert incompatible.validation_error == "reported_best_ask_mismatch"
    assert restored.best_ask == pytest.approx(0.52)
    assert restored.best_ask_size == pytest.approx(25.0)
    assert restored.book_complete is True


def test_reported_best_mismatch_marks_book_incomplete() -> None:
    orderbook = _book()
    _snapshot(orderbook)

    quote = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.47",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.best_bid == pytest.approx(0.47)
    assert quote.best_bid_size is None
    assert quote.best_ask_size == pytest.approx(25.0)
    assert quote.book_complete is False
    assert quote.book_has_snapshot is True
    assert quote.book_structurally_complete is True
    assert quote.reported_best_validation_ok is False
    assert quote.validation_error == "reported_best_bid_mismatch"
    assert quote.book_hash == "hash-2"


def test_tolerant_mode_allows_reported_best_mismatch_within_tolerance() -> None:
    orderbook = _book(best_validation_mode="tolerant")
    _snapshot(orderbook)

    quote = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.49",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.validation_error == "reported_best_bid_mismatch"
    assert quote.reported_best_validation_ok is True
    assert quote.book_complete is True
    assert quote.best_bid_size == pytest.approx(15.0)


def test_tolerant_mode_rejects_reported_best_mismatch_beyond_tolerance() -> None:
    orderbook = _book(best_validation_mode="tolerant")
    _snapshot(orderbook)

    quote = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.47",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.validation_error == "reported_best_bid_mismatch"
    assert quote.reported_best_validation_ok is False
    assert quote.book_complete is False


def test_diagnostic_mode_records_mismatch_without_marking_incomplete() -> None:
    orderbook = _book(best_validation_mode="diagnostic")
    _snapshot(orderbook)

    quote = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.47",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.validation_error == "reported_best_bid_mismatch"
    assert quote.reported_best_validation_ok is False
    assert quote.book_structurally_complete is True
    assert quote.book_complete is True


def test_diagnostic_mode_still_marks_missing_snapshot_incomplete() -> None:
    orderbook = _book(best_validation_mode="diagnostic")

    quote = orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.47",
            "best_ask": "0.52",
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    assert quote.validation_error == "missing_snapshot"
    assert quote.book_has_snapshot is False
    assert quote.book_complete is False


def test_mismatch_sampler_writes_compact_jsonl_sample(tmp_path) -> None:
    sample_path = tmp_path / "samples.jsonl"
    orderbook = _book(best_validation_mode="diagnostic", mismatch_sample_path=sample_path)
    _snapshot(orderbook)

    orderbook.apply_best_bid_ask(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-up",
            "market": "0xmarket",
            "best_bid": "0.47",
            "best_ask": "0.52",
            "hash": "payload-hash",
            "extra": {"large": "ignored"},
        },
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence="hash-2",
    )

    rows = [orjson.loads(line) for line in sample_path.read_bytes().splitlines()]
    assert len(rows) == 1
    sample = rows[0]
    assert sample["market_id"] == "0xmarket"
    assert sample["market_slug"] == "bitcoin-up-or-down-15m"
    assert sample["base_asset"] == "BTC"
    assert sample["duration_minutes"] == 15
    assert sample["token_id"] == "token-up"
    assert sample["token_outcome"] == "Up"
    assert sample["update_type"] == "best_bid_ask"
    assert sample["validation_error"] == "reported_best_bid_mismatch"
    assert sample["reported_best_bid"] == 0.47
    assert sample["payload_hash"] == "payload-hash"
    assert isinstance(sample["raw_payload_compact"], dict)


def test_mismatch_sampler_limiter_suppresses_excessive_samples(tmp_path) -> None:
    sample_path = tmp_path / "samples.jsonl"
    orderbook = _book(
        best_validation_mode="diagnostic",
        mismatch_sample_path=sample_path,
        mismatch_sample_per_token_per_min=1,
    )
    _snapshot(orderbook)

    for offset in (0, 1):
        orderbook.apply_best_bid_ask(
            {
                "event_type": "best_bid_ask",
                "asset_id": "token-up",
                "market": "0xmarket",
                "best_bid": "0.47",
                "best_ask": "0.52",
            },
            received_ts=2_000 + offset,
            parse_done_ts=2_001 + offset,
            recv_monotonic_ns=2_000 + offset,
            parse_done_monotonic_ns=2_001 + offset,
            event_ts=2_000 + offset,
            sequence=f"hash-{offset}",
        )

    assert len(sample_path.read_bytes().splitlines()) == 1


def test_mismatch_sampler_does_not_crash_without_raw_payload(tmp_path) -> None:
    sample_path = tmp_path / "samples.jsonl"
    orderbook = _book(best_validation_mode="diagnostic", mismatch_sample_path=sample_path)
    _snapshot(orderbook)

    quote = orderbook.apply_best_bid_ask(
        {},
        received_ts=2_000,
        parse_done_ts=2_001,
        recv_monotonic_ns=2_000,
        parse_done_monotonic_ns=2_001,
        event_ts=2_000,
        sequence=None,
    )

    assert quote.validation_error == "missing_snapshot"
    assert sample_path.exists()
