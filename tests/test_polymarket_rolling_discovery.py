import pytest

from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    floor_to_window,
    generate_crypto_updown_slugs,
    parse_market_metadata,
)


def _market_payload(
    *,
    slug: str,
    outcomes: list[str] | str = '["Up","Down"]',
    token_ids: list[str] | str = '["up-token","down-token"]',
    question: str | None = None,
    market_id: str = "0xmarket",
    condition_id: str = "0xcondition",
) -> dict[str, object]:
    return {
        "conditionId": condition_id,
        "market": market_id,
        "slug": slug,
        "question": question or f"{slug} up or down",
        "endDateIso": "2026-05-15T12:15:00Z",
        "clobTokenIds": token_ids,
        "outcomes": outcomes,
        "order_price_min_tick_size": "0.01",
        "minimum_order_size": "5",
        "active": True,
        "closed": False,
    }


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
