from datetime import UTC, datetime

import pytest

from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    classify_market_window,
    floor_to_window,
    generate_crypto_updown_slugs,
    is_runtime_tradable_market,
    parse_market_metadata,
    select_runtime_markets,
)


def _market_payload(
    *,
    slug: str,
    outcomes: list[str] | str = '["Up","Down"]',
    token_ids: list[str] | str = '["up-token","down-token"]',
    question: str | None = None,
    market_id: str = "0xmarket",
    condition_id: str = "0xcondition",
    end_date: str = "2026-05-15T12:15:00Z",
    event_start_time: str | None = "2026-05-15T12:00:00Z",
    active: bool = True,
    closed: bool = False,
    accepting_orders: bool = True,
    enable_order_book: bool = True,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "conditionId": condition_id,
        "market": market_id,
        "slug": slug,
        "question": question or f"{slug} up or down",
        "endDate": end_date,
        "endDateIso": end_date.split("T", maxsplit=1)[0],
        "clobTokenIds": token_ids,
        "outcomes": outcomes,
        "order_price_min_tick_size": "0.01",
        "minimum_order_size": "5",
        "active": active,
        "closed": closed,
        "acceptingOrders": accepting_orders,
        "enableOrderBook": enable_order_book,
    }
    if event_start_time is not None:
        payload["eventStartTime"] = event_start_time
    return payload


def test_floor_to_window_15m() -> None:
    assert floor_to_window(1_778_832_999, 900) == 1_778_832_900


def test_floor_to_window_5m() -> None:
    assert floor_to_window(1_778_832_999, 300) == 1_778_832_900


def test_generate_crypto_updown_slugs_includes_current_previous_next_windows() -> None:
    slugs = generate_crypto_updown_slugs(
        1_778_832_999,
        assets=("btc",),
        horizons=("15m",),
        lookback_windows=1,
        lookahead_windows=1,
    )

    assert slugs == [
        "btc-updown-15m-1778832000",
        "btc-updown-15m-1778832900",
        "btc-updown-15m-1778833800",
    ]


def test_generate_crypto_updown_slugs_order_is_deterministic() -> None:
    slugs = generate_crypto_updown_slugs(
        1_778_832_999,
        assets=("btc", "eth"),
        horizons=("5m", "15m"),
        lookback_windows=0,
        lookahead_windows=0,
    )

    assert slugs == [
        "btc-updown-5m-1778832900",
        "btc-updown-15m-1778832900",
        "eth-updown-5m-1778832900",
        "eth-updown-15m-1778832900",
    ]


def test_parse_btc_rolling_15m_slug_sets_asset_and_duration() -> None:
    metadata = parse_market_metadata(_market_payload(slug="btc-updown-15m-1778832900"))

    assert metadata is not None
    assert metadata.base_asset == "BTC"
    assert metadata.duration_minutes == 15
    assert metadata.market_slug == "btc-updown-15m-1778832900"


def test_parse_eth_rolling_5m_slug_sets_asset_and_duration() -> None:
    metadata = parse_market_metadata(_market_payload(slug="eth-updown-5m-1778832900"))

    assert metadata is not None
    assert metadata.base_asset == "ETH"
    assert metadata.duration_minutes == 5
    assert metadata.market_slug == "eth-updown-5m-1778832900"


def test_full_end_date_is_preserved_over_date_only_end_date_iso() -> None:
    metadata = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778832900",
            end_date="2026-05-15T15:15:00Z",
            event_start_time="2026-05-15T15:00:00Z",
        )
    )

    assert metadata is not None
    assert metadata.end_time == "2026-05-15T15:15:00Z"
    assert metadata.event_start_time == "2026-05-15T15:00:00Z"


@pytest.mark.asyncio
async def test_direct_market_slug_up_down_maps_correctly() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _market_payload(slug=slug)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            raise AssertionError("event endpoint should not be needed")

    client = Client()

    markets = await client.discover_rolling_markets(now_ts=1_778_832_999)

    assert markets
    assert markets[0].up_token_id == "up-token"
    assert markets[0].down_token_id == "down-token"
    assert markets[0].token_for_direction("UP") == "up-token"
    assert markets[0].token_for_direction("DOWN") == "down-token"


@pytest.mark.asyncio
async def test_direct_market_slug_reversed_down_up_maps_correctly() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _market_payload(
                slug=slug,
                outcomes='["Down","Up"]',
                token_ids='["down-token","up-token"]',
            )

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            raise AssertionError("event endpoint should not be needed")

    client = Client()

    markets = await client.discover_rolling_markets(now_ts=1_778_832_999)

    assert markets
    assert markets[0].up_token_id == "up-token"
    assert markets[0].down_token_id == "down-token"


@pytest.mark.asyncio
async def test_nested_event_markets_response_is_parsed() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            if slug != "btc-updown-5m-1778832900":
                return None
            return {
                "slug": slug,
                "title": "Bitcoin Up or Down - 15 minute",
                "endDateIso": "2026-05-15T12:15:00Z",
                "markets": [_market_payload(slug="", question=None)],
            }

    client = Client()

    markets = await client.discover_rolling_markets(now_ts=1_778_832_999)

    assert markets
    assert markets[0].market_slug == "btc-updown-5m-1778832900"
    assert markets[0].up_token_id == "up-token"


def test_ambiguous_yes_no_rolling_market_rejected_unless_text_resolves_direction() -> None:
    reasons: list[str] = []
    ambiguous = parse_market_metadata(
        _market_payload(slug="btc-updown-15m-1778832900", outcomes='["Yes","No"]'),
        reject_logger=reasons.append,
    )
    resolved = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778832900",
            outcomes='["Yes","No"]',
            question="Will Bitcoin be higher than its open price?",
        )
    )

    assert ambiguous is None
    assert reasons == ["no_direction_mapping"]
    assert resolved is not None
    assert resolved.up_token_id == "up-token"
    assert resolved.down_token_id == "down-token"


def test_closed_market_is_not_runtime_tradable() -> None:
    market = parse_market_metadata(
        _market_payload(slug="btc-updown-15m-1778832900", closed=True)
    )

    assert market is not None
    assert is_runtime_tradable_market(market, now_ts=1_778_833_200) is False
    assert classify_market_window(market, now_ts=1_778_833_200) == "closed"


def test_accepting_orders_false_is_not_runtime_tradable() -> None:
    market = parse_market_metadata(
        _market_payload(slug="btc-updown-15m-1778832900", accepting_orders=False)
    )

    assert market is not None
    assert is_runtime_tradable_market(market, now_ts=1_778_833_200) is False
    assert classify_market_window(market, now_ts=1_778_833_200) == "not_accepting"


def test_expired_but_not_closed_market_is_not_runtime_tradable() -> None:
    market = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778832900",
            event_start_time=_iso_from_ts(1_778_832_900),
            end_date=_iso_from_ts(1_778_833_800),
        )
    )

    assert market is not None
    assert is_runtime_tradable_market(market, now_ts=1_778_833_801) is False
    assert classify_market_window(market, now_ts=1_778_833_801) == "expired"


def test_current_and_next_markets_are_selected_but_later_future_is_not() -> None:
    current = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778832900",
            market_id="current",
            event_start_time=_iso_from_ts(1_778_832_900),
            end_date=_iso_from_ts(1_778_833_800),
        )
    )
    next_market = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778833800",
            market_id="next",
            event_start_time=_iso_from_ts(1_778_833_800),
            end_date=_iso_from_ts(1_778_834_700),
        )
    )
    later_future = parse_market_metadata(
        _market_payload(
            slug="btc-updown-15m-1778834700",
            market_id="later",
            event_start_time=_iso_from_ts(1_778_834_700),
            end_date=_iso_from_ts(1_778_835_600),
        )
    )
    assert current is not None
    assert next_market is not None
    assert later_future is not None

    selected = select_runtime_markets(
        (later_future, next_market, current),
        now_ts=1_778_833_500,
    )

    assert [market.market_id for market in selected] == ["current", "next"]
    assert classify_market_window(current, now_ts=1_778_833_500) == "current"
    assert classify_market_window(next_market, now_ts=1_778_833_500) == "next"
    assert classify_market_window(later_future, now_ts=1_778_833_500) == "future"


def test_current_and_next_selection_is_per_asset_and_duration() -> None:
    markets = []
    for asset, duration, base_start in (
        ("btc", "5m", 1_778_832_900),
        ("btc", "15m", 1_778_832_900),
        ("eth", "5m", 1_778_832_900),
        ("eth", "15m", 1_778_832_900),
    ):
        seconds = 300 if duration == "5m" else 900
        for index, start_ts in enumerate((base_start, base_start + seconds, base_start + 2 * seconds)):
            metadata = parse_market_metadata(
                _market_payload(
                    slug=f"{asset}-updown-{duration}-{start_ts}",
                    market_id=f"{asset}-{duration}-{index}",
                    event_start_time=_iso_from_ts(start_ts),
                    end_date=_iso_from_ts(start_ts + seconds),
                )
            )
            assert metadata is not None
            markets.append(metadata)

    selected = select_runtime_markets(tuple(markets), now_ts=1_778_833_000)

    assert [market.market_id for market in selected] == [
        "btc-5m-0",
        "btc-5m-1",
        "btc-15m-0",
        "btc-15m-1",
        "eth-5m-0",
        "eth-5m-1",
        "eth-15m-0",
        "eth-15m-1",
    ]


@pytest.mark.asyncio
async def test_pagination_fallback_stops_on_empty_page() -> None:
    class Client(PolymarketDiscoveryClient):
        def __init__(self) -> None:
            super().__init__(enable_direct_slug_lookup=False, page_limit=100, max_pages=10)
            self.market_offsets: list[int] = []

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

        async def _fetch_page(self, endpoint: str, *, limit: int, offset: int) -> list[dict[str, object]]:
            assert endpoint == "markets"
            self.market_offsets.append(offset)
            if offset == 0:
                return [_market_payload(slug="btc-updown-15m-1778832900")]
            return []

    client = Client()

    markets = await client.discover(write_cache=False)

    assert len(markets) == 1
    assert client.market_offsets == [0, 100]


@pytest.mark.asyncio
async def test_discovery_returns_rolling_markets_before_broad_fallback() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _market_payload(slug=slug, market_id="rolling-market")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_raw_market_payloads(self) -> list[dict[str, object]]:
            raise AssertionError("broad fallback should not run when direct rolling lookup succeeds")

    client = Client()

    markets = await client.discover(write_cache=False, now_ts=1_778_832_999)

    assert markets
    assert markets[0].market_id == "rolling-market"


def _iso_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")
