from datetime import UTC, datetime
from pathlib import Path

import orjson
import pytest

from app.core.clock import utc_now_ns
from app.main import _discover_polymarket_markets_for_startup
from app.marketdata.polymarket_discovery import (
    PolymarketMarketCache,
    PolymarketMarketMetadata,
    PolymarketDiscoveryClient,
    RollingDiscoveryResult,
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


def test_order_min_size_is_preferred_over_rewards_min_size() -> None:
    payload = _market_payload(slug="btc-updown-15m-1778832900")
    payload.pop("minimum_order_size")
    payload["orderMinSize"] = "5"
    payload["rewardsMinSize"] = "50"

    metadata = parse_market_metadata(payload)

    assert metadata is not None
    assert metadata.min_order_size == pytest.approx(5.0)
    assert metadata.rewards_min_size == pytest.approx(50.0)


def test_missing_order_min_size_does_not_use_rewards_min_size_for_fillability() -> None:
    payload = _market_payload(slug="btc-updown-15m-1778832900")
    payload.pop("minimum_order_size")
    payload["rewardsMinSize"] = "50"

    metadata = parse_market_metadata(payload)

    assert metadata is not None
    assert metadata.min_order_size != pytest.approx(50.0)
    assert metadata.min_order_size == pytest.approx(0.0)
    assert metadata.rewards_min_size == pytest.approx(50.0)


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
    assert classify_market_window(market, now_ts=1_778_833_200) == "active_but_not_accepting_orders"


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
    assert classify_market_window(current, now_ts=1_778_833_500) == "current_signal"
    assert classify_market_window(next_market, now_ts=1_778_833_500) == "next_warmup"
    assert classify_market_window(later_future, now_ts=1_778_833_500) == "future_tracked"
    assert selected[0].selected_for_runtime is True
    assert selected[0].signal_enabled is True
    assert selected[0].classification == "current_signal"
    assert selected[1].selected_for_runtime is True
    assert selected[1].signal_enabled is False
    assert selected[1].classification == "next_warmup"


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
            return _payload_for_slug(slug, market_id=f"rolling-market-{slug}")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_raw_market_payloads(self) -> list[dict[str, object]]:
            raise AssertionError("broad fallback should not run when direct rolling lookup succeeds")

    client = Client()

    markets = await client.discover(write_cache=False, now_ts=1_778_832_999)

    assert markets
    assert markets[0].market_id.startswith("rolling-market-")


@pytest.mark.asyncio
async def test_discovery_returns_rolling_markets_before_broad_fallback_offline() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"offline-direct-{slug}")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            raise AssertionError("event fallback should not run when direct lookup succeeds")

        async def _fetch_page(self, endpoint: str, *, limit: int, offset: int) -> list[dict[str, object]]:
            raise AssertionError("unit test must not perform broad network pagination")

    markets = await Client().discover(write_cache=False, now_ts=1_778_832_999)

    assert markets
    assert markets[0].market_id.startswith("offline-direct-")


@pytest.mark.asyncio
async def test_polymarket_discovery_no_real_network_in_unit_tests() -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"direct-no-network-{slug}")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            raise AssertionError("unexpected event network call")

        async def _fetch_raw_market_payloads(self) -> list[dict[str, object]]:
            raise AssertionError("unexpected raw market network call")

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            raise AssertionError("unexpected active events network call")

    result = await Client().discover_rolling_markets_robust(now_ts=1_778_832_999)

    assert result.runtime_markets
    assert result.runtime_markets[0].market_id.startswith("direct-no-network-")


def test_cache_all_closed_is_rejected_for_runtime(tmp_path: Path) -> None:
    client = PolymarketDiscoveryClient(cache_path=tmp_path / "markets.json")
    closed = parse_market_metadata(
        _market_payload(
            slug="btc-updown-5m-1778832900",
            market_id="closed",
            event_start_time=_iso_from_ts(1_778_832_900),
            end_date=_iso_from_ts(1_778_833_200),
            closed=True,
        )
    )
    assert closed is not None
    client.write_cache([closed])

    validation = client.validate_cache_for_runtime(now_ts=1_778_832_999, ttl_ms=60_000)

    assert validation.valid is False
    assert validation.rejected_reason == "cache_all_closed"


def test_cache_with_runtime_market_is_accepted(tmp_path: Path) -> None:
    client = PolymarketDiscoveryClient(cache_path=tmp_path / "markets.json")
    market = parse_market_metadata(
        _market_payload(
            slug="btc-updown-5m-1778832900",
            market_id="runtime",
            event_start_time=_iso_from_ts(1_778_832_900),
            end_date=_iso_from_ts(1_778_833_200),
        )
    )
    assert market is not None
    client.write_cache([market])

    validation = client.validate_cache_for_runtime(now_ts=1_778_832_999, ttl_ms=60_000)

    assert validation.valid is True
    assert validation.runtime_markets[0].market_id == "runtime"
    assert validation.runtime_markets[0].discovery_source == "cache"


@pytest.mark.asyncio
async def test_direct_slug_all_closed_triggers_active_events_fallback() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"direct-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {
                    "slug": "btc-active-event",
                    "markets": [
                        _payload_for_slug(
                            "btc-updown-5m-1778832900",
                            market_id="active-current",
                        )
                    ],
                }
            ]

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)

    assert result.fallback_used is True
    assert [market.market_id for market in result.runtime_markets] == ["active-current"]
    assert result.runtime_markets[0].discovery_source == "active_events"


@pytest.mark.asyncio
async def test_direct_slug_found_but_all_closed_triggers_fallback(tmp_path: Path) -> None:
    path = tmp_path / "attempts.jsonl"

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    result = await Client(cache_path=tmp_path / "cache.json").discover_rolling_markets_robust(
        now_ts=1_778_832_999,
        discovery_debug_jsonl=path,
    )
    row = orjson.loads(path.read_bytes().splitlines()[0])

    assert row["direct_found_count"] > 0
    assert row["direct_runtime_count"] == 0
    assert row["fallback_used"] is True
    assert row["failure_reason"] == "direct_slug_found_but_all_closed"
    assert result.runtime_markets == ()


@pytest.mark.asyncio
async def test_all_closed_direct_slug_result_does_not_overwrite_valid_cache(tmp_path: Path) -> None:
    now_ts = 1_778_832_999
    cache_path = tmp_path / "markets.json"
    valid = parse_market_metadata(
        _payload_for_slug("btc-updown-5m-1778832900", market_id="valid-cache")
    )
    assert valid is not None

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    client = Client(cache_path=cache_path)
    client.write_cache([valid])
    before = cache_path.read_bytes()

    result = await client.discover_rolling_markets_robust(now_ts=now_ts, write_cache=True)

    assert result.cache_used is True
    assert result.runtime_markets[0].market_id == "valid-cache"
    assert cache_path.read_bytes() == before


@pytest.mark.asyncio
async def test_runtime_cache_written_only_when_runtime_markets_exist(tmp_path: Path) -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            if slug != "btc-updown-5m-1778832900":
                return None
            return _payload_for_slug(slug, market_id="runtime-direct")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    cache_path = tmp_path / "markets.json"
    result = await Client(cache_path=cache_path).discover_rolling_markets_robust(
        now_ts=now_ts,
        write_cache=True,
    )

    cache = PolymarketMarketCache.model_validate_json(cache_path.read_bytes())
    assert result.runtime_markets
    assert [market.market_id for market in cache.markets] == ["runtime-direct"]


@pytest.mark.asyncio
async def test_cache_not_updated_when_runtime_count_zero(tmp_path: Path) -> None:
    now_ts = 1_778_832_999
    cache_path = tmp_path / "markets.json"

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    result = await Client(cache_path=cache_path).discover_rolling_markets_robust(
        now_ts=now_ts,
        write_cache=True,
    )

    assert result.runtime_markets == ()
    assert cache_path.exists() is False
    assert result.attempt["cache_not_updated_reason"] == "cache_not_updated_runtime_count_zero"


@pytest.mark.asyncio
async def test_direct_all_closed_mandates_active_events_fallback() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)

    assert result.fallback_used is True
    assert result.active_events_result is not None
    assert result.active_events_result["attempted"] is True


@pytest.mark.asyncio
async def test_active_events_fallback_selects_current_signal_markets() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {
                    "markets": [
                        _payload_for_slug(
                            "eth-updown-15m-1778832900",
                            market_id="eth-current",
                        )
                    ]
                }
            ]

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)

    assert result.runtime_markets[0].classification == "current_signal"
    assert result.runtime_markets[0].signal_enabled is True


@pytest.mark.asyncio
async def test_active_events_payload_with_nested_markets_selects_current() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {
                    "eventStartTime": _iso_from_ts(1_778_832_900),
                    "endDate": _iso_from_ts(1_778_833_200),
                    "clobTokenIds": '["event-up","event-down"]',
                    "outcomes": '["Up","Down"]',
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "enableOrderBook": True,
                    "markets": [
                        {
                            "conditionId": "event-condition",
                            "market": "nested-current",
                            "slug": "btc-updown-5m-1778832900",
                            "question": "BTC Up or Down",
                            "order_price_min_tick_size": "0.01",
                            "minimum_order_size": "5",
                        }
                    ],
                }
            ]

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)

    assert [market.market_id for market in result.runtime_markets] == ["nested-current"]
    assert result.runtime_markets[0].up_token_id == "event-up"


@pytest.mark.asyncio
async def test_active_events_reject_reasons_are_reported() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {
                    "markets": [
                        _payload_for_slug(
                            "btc-updown-5m-1778832900",
                            market_id="not-accepting",
                            accepting_orders=False,
                        )
                    ]
                }
            ]

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)
    assert result.active_events_result is not None
    reasons = {item["reason"] for item in result.active_events_result["reject_reasons"]}

    assert "active_but_not_accepting_orders" in reasons


@pytest.mark.asyncio
async def test_active_events_pagination_scans_until_runtime_market_found() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        def __init__(self) -> None:
            super().__init__(limit=1, max_pages=3)
            self.offsets: list[int] = []

        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_page(self, endpoint: str, *, limit: int, offset: int) -> list[dict[str, object]]:
            assert endpoint == "events"
            self.offsets.append(offset)
            if offset == 0:
                return [{"markets": [_market_payload(slug="not-crypto", question="Politics")]}]
            if offset == 1:
                return [{"markets": [_payload_for_slug("eth-updown-5m-1778832900", market_id="paged")]}]
            return []

    client = Client()
    result = await client.discover_rolling_markets_robust(now_ts=now_ts)

    assert [market.market_id for market in result.runtime_markets] == ["paged"]
    assert client.offsets == [0, 1, 2]


@pytest.mark.asyncio
async def test_active_events_missing_current_reports_runtime_zero_not_success() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [{"markets": [_payload_for_slug("btc-updown-5m-1778833200", market_id="next-only")]}]

    result = await Client().discover_rolling_markets_robust(now_ts=now_ts)
    assert result.active_events_result is not None

    assert result.active_events_result["runtime_candidate_count"] == 1
    assert result.active_events_result["runtime_tradable_count"] == 0
    assert result.attempt["active_events_runtime_count"] == 0


@pytest.mark.asyncio
async def test_discovery_deduplicates_markets_found_by_multiple_sources() -> None:
    now_ts = 1_778_832_999

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            if slug != "btc-updown-5m-1778832900":
                return None
            return _payload_for_slug(slug, market_id="same-market")

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [
                {"markets": [_payload_for_slug("btc-updown-5m-1778832900", market_id="same-market")]}
            ]

    result = await Client().discover_rolling_markets_robust(
        now_ts=now_ts,
        force_active_events=True,
    )

    assert [market.market_id for market in result.markets].count("same-market") == 1


@pytest.mark.asyncio
async def test_startup_wait_retries_until_market_available(tmp_path: Path) -> None:
    market = parse_market_metadata(
        _payload_for_slug("btc-updown-5m-1778832900", market_id="later")
    )
    assert market is not None

    class Client(PolymarketDiscoveryClient):
        def __init__(self) -> None:
            super().__init__(cache_path=tmp_path / "cache.json")
            self.calls = 0

        async def discover_rolling_markets_robust(self, **kwargs: object) -> RollingDiscoveryResult:
            self.calls += 1
            if self.calls < 2:
                return _rolling_result((), failure_reason="no_runtime_tradable_markets")
            annotated = select_runtime_markets((market,), now_ts=1_778_832_999)
            return _rolling_result(annotated)

    now = 0.0

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        now += delay

    client = Client()
    markets = await _discover_polymarket_markets_for_startup(
        client,
        wait_for_markets=True,
        retry_ms=1,
        startup_timeout_ms=10,
        sleep=fake_sleep,
        monotonic=lambda: now,
    )

    assert client.calls == 2
    assert markets[0].market_id == "later"


@pytest.mark.asyncio
async def test_startup_timeout_exits_with_clear_error(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        async def discover_rolling_markets_robust(self, **kwargs: object) -> RollingDiscoveryResult:
            return _rolling_result((), failure_reason="no_runtime_tradable_markets")

    now = 0.0

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        now += delay

    with pytest.raises(SystemExit, match="no_active_markets_after_startup_timeout"):
        await _discover_polymarket_markets_for_startup(
            Client(cache_path=tmp_path / "cache.json"),
            wait_for_markets=True,
            retry_ms=1,
            startup_timeout_ms=1,
            sleep=fake_sleep,
            monotonic=lambda: now,
        )


@pytest.mark.asyncio
async def test_no_wait_for_markets_preserves_fail_fast_behavior(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        def __init__(self) -> None:
            super().__init__(cache_path=tmp_path / "cache.json")
            self.calls = 0

        async def discover_rolling_markets_robust(self, **kwargs: object) -> RollingDiscoveryResult:
            self.calls += 1
            return _rolling_result((), failure_reason="no_runtime_tradable_markets")

    client = Client()
    markets = await _discover_polymarket_markets_for_startup(
        client,
        wait_for_markets=False,
        startup_timeout_ms=1,
    )

    assert markets == ()
    assert client.calls == 1


@pytest.mark.asyncio
async def test_discovery_attempt_jsonl_written_on_failure(tmp_path: Path) -> None:
    path = tmp_path / "attempts.jsonl"

    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    result = await Client(cache_path=tmp_path / "cache.json").discover_rolling_markets_robust(
        now_ts=1_778_832_999,
        discovery_debug_jsonl=path,
        write_cache=True,
    )

    assert result.runtime_markets == ()
    row = orjson.loads(path.read_bytes().splitlines()[0])
    assert row["event_type"] == "polymarket_discovery_attempt"
    assert row["failure_reason"] == "direct_slug_found_but_all_closed"
    assert row["cache_not_updated_reason"] == "cache_not_updated_all_closed"


@pytest.mark.asyncio
async def test_debug_command_reports_direct_slug_all_closed(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    debug = await Client(cache_path=tmp_path / "cache.json").debug_rolling_discovery(
        now_ts=1_778_832_999,
    )

    assert debug["attempt"]["failure_reason"] == "direct_slug_found_but_all_closed"


@pytest.mark.asyncio
async def test_debug_command_reports_active_events_fallback_counts(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [{"markets": [_payload_for_slug("btc-updown-5m-1778832900")]}]

    debug = await Client(cache_path=tmp_path / "cache.json").debug_rolling_discovery(
        now_ts=1_778_832_999,
    )

    assert debug["attempt"]["active_events_found_runtime_count"] == 1


@pytest.mark.asyncio
async def test_debug_command_reports_direct_slug_and_active_events_breakdown(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return [{"markets": [_payload_for_slug("btc-updown-5m-1778832900")]}]

    debug = await Client(cache_path=tmp_path / "cache.json").debug_rolling_discovery(
        now_ts=1_778_832_999,
    )
    strategy = debug["strategy_results"]

    assert strategy["direct_slug"]["found_count"] > 0
    assert strategy["direct_slug"]["runtime_tradable_count"] == 0
    assert strategy["active_events"]["attempted"] is True
    assert strategy["active_events"]["runtime_tradable_count"] == 1
    assert "cache" in strategy


@pytest.mark.asyncio
async def test_future_generated_slug_closed_records_diagnostic(tmp_path: Path) -> None:
    class Client(PolymarketDiscoveryClient):
        async def fetch_market_by_slug(self, slug: str) -> dict[str, object] | None:
            return _payload_for_slug(slug, market_id=f"closed-{slug}", closed=True)

        async def fetch_event_by_slug(self, slug: str) -> dict[str, object] | None:
            return None

        async def _fetch_paginated_events(self) -> list[dict[str, object]]:
            return []

    result = await Client(cache_path=tmp_path / "cache.json").discover_rolling_markets_robust(
        now_ts=1_778_832_999,
    )

    assert "direct_slug_returned_closed_for_current_or_future_window" in result.attempt["diagnostics"]


def test_market_cache_ttl_rejects_old_cache(tmp_path: Path) -> None:
    client = PolymarketDiscoveryClient(cache_path=tmp_path / "markets.json")
    market = parse_market_metadata(_payload_for_slug("btc-updown-5m-1778832900"))
    assert market is not None
    cache = PolymarketMarketCache(discovered_at_ts=utc_now_ns() - 10_000_000_000, markets=[market])
    client.cache_path.write_bytes(orjson.dumps(cache.model_dump(mode="json")))

    validation = client.validate_cache_for_runtime(now_ts=1_778_832_999, ttl_ms=1)

    assert validation.valid is False
    assert validation.rejected_reason == "cache_expired"


def test_active_but_not_accepting_orders_not_runtime_tradable() -> None:
    market = parse_market_metadata(
        _payload_for_slug("btc-updown-5m-1778832900", accepting_orders=False)
    )

    assert market is not None
    assert is_runtime_tradable_market(market, now_ts=1_778_832_999) is False
    assert classify_market_window(market, now_ts=1_778_832_999) == "active_but_not_accepting_orders"


def test_enable_orderbook_false_not_runtime_tradable() -> None:
    market = parse_market_metadata(
        _payload_for_slug("btc-updown-5m-1778832900", enable_order_book=False)
    )

    assert market is not None
    assert is_runtime_tradable_market(market, now_ts=1_778_832_999) is False
    assert classify_market_window(market, now_ts=1_778_832_999) == "missing_orderbook"


def test_missing_up_down_tokens_not_runtime_tradable() -> None:
    market = PolymarketMarketMetadata(
        condition_id="missing-condition",
        market_id="missing",
        market_slug="btc-updown-5m-1778832900",
        question="BTC Up or Down",
        end_time=_iso_from_ts(1_778_833_200),
        event_start_time=_iso_from_ts(1_778_832_900),
        tick_size=0.01,
        min_order_size=5.0,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
        up_token_id="up",
        down_token_id=None,
        token_outcomes={"up": "Up"},
        base_asset="BTC",
        duration_minutes=5,
    )

    assert is_runtime_tradable_market(market, now_ts=1_778_832_999) is False
    assert classify_market_window(market, now_ts=1_778_832_999) == "missing_tokens"


def _payload_for_slug(
    slug: str,
    *,
    market_id: str = "0xmarket",
    closed: bool = False,
    accepting_orders: bool = True,
    enable_order_book: bool = True,
) -> dict[str, object]:
    parts = slug.split("-")
    horizon = parts[2]
    start_ts = int(parts[3])
    duration_s = 300 if horizon == "5m" else 900
    return _market_payload(
        slug=slug,
        market_id=market_id,
        condition_id=f"{market_id}-condition",
        event_start_time=_iso_from_ts(start_ts),
        end_date=_iso_from_ts(start_ts + duration_s),
        closed=closed,
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
    )


def _rolling_result(
    markets: tuple[PolymarketMarketMetadata, ...],
    *,
    failure_reason: str | None = None,
) -> RollingDiscoveryResult:
    attempt = {
        "strategy_results": {
            "direct_slug": {"found_count": 0, "runtime_tradable_count": len(markets)},
            "active_events": {"runtime_tradable_count": 0},
            "cache": {"runtime_count": 0, "rejected": False, "rejected_reason": None},
        },
        "runtime_tradable_count": len(markets),
        "current_signal_count": sum(1 for market in markets if market.signal_enabled),
        "next_warmup_count": sum(
            1 for market in markets if market.runtime_selection_reason == "next_warmup"
        ),
        "selected_market_slugs": [market.market_slug for market in markets],
        "current_signal_slugs": [market.market_slug for market in markets if market.signal_enabled],
        "next_warmup_slugs": [
            market.market_slug
            for market in markets
            if market.runtime_selection_reason == "next_warmup"
        ],
        "fallback_used": False,
        "failure_reason": failure_reason,
        "diagnostics": [],
    }
    return RollingDiscoveryResult(markets=markets, runtime_markets=markets, attempt=attempt)


def _iso_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")
