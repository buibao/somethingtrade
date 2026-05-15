import pytest

from app.marketdata.polymarket_orderbook import PolymarketLocalOrderBook, TokenBookMetadata


def _book() -> PolymarketLocalOrderBook:
    return PolymarketLocalOrderBook(
        token_metadata={
            "token-up": TokenBookMetadata(
                condition_id="0xcondition",
                market_id="0xmarket",
                side_label="UP",
            )
        },
        stale_after_ms=60_000.0,
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
    assert quote.validation_error == "reported_best_bid_mismatch"
    assert quote.book_hash == "hash-2"
