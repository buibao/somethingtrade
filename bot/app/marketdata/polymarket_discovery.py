from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any

import aiohttp
import orjson
from pydantic import BaseModel, ConfigDict, Field

from app.core.clock import utc_now_ns

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_MARKET_CACHE_PATH = Path("data/cache/polymarket_markets.json")


class PolymarketMarketMetadata(BaseModel):
    """Public metadata needed to subscribe to and display a binary CLOB market."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    condition_id: str
    market_id: str
    market_slug: str
    question: str
    end_time: str
    yes_token_id: str
    no_token_id: str
    tick_size: float
    min_order_size: float
    base_asset: str | None = None
    duration_minutes: int | None = None

    @property
    def token_ids(self) -> tuple[str, str]:
        return (self.yes_token_id, self.no_token_id)


class PolymarketMarketCache(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    discovered_at_ts: int | None = None
    markets: list[PolymarketMarketMetadata] = Field(default_factory=list)


FetchMarkets = Callable[[], Sequence[dict[str, Any]]]


class PolymarketDiscoveryClient:
    """Discover active short-duration crypto markets through public Gamma data."""

    def __init__(
        self,
        *,
        gamma_url: str = DEFAULT_GAMMA_URL,
        cache_path: Path | str = DEFAULT_MARKET_CACHE_PATH,
        limit: int = 500,
        fetch_markets: FetchMarkets | None = None,
    ) -> None:
        self.gamma_url = gamma_url.rstrip("/")
        self.cache_path = Path(cache_path)
        self.limit = limit
        self._fetch_markets = fetch_markets

    async def discover(self, *, write_cache: bool = True) -> tuple[PolymarketMarketMetadata, ...]:
        raw_markets = (
            list(self._fetch_markets()) if self._fetch_markets is not None else await self._fetch()
        )
        markets = tuple(
            metadata
            for payload in raw_markets
            if is_short_duration_crypto_market(payload)
            if (metadata := parse_market_metadata(payload)) is not None
        )
        if write_cache:
            self.write_cache(markets)
        return markets

    async def _fetch(self) -> list[dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.gamma_url}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": str(self.limit),
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        if not isinstance(payload, list):
            raise ValueError("Gamma /markets response must be a list")
        return [item for item in payload if isinstance(item, dict)]

    def write_cache(self, markets: Sequence[PolymarketMarketMetadata]) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache = PolymarketMarketCache(discovered_at_ts=utc_now_ns(), markets=list(markets))
        self.cache_path.write_bytes(
            orjson.dumps(
                cache.model_dump(mode="json"),
                option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE,
            )
        )

    def read_cache(self) -> PolymarketMarketCache:
        if not self.cache_path.exists():
            return PolymarketMarketCache()
        return PolymarketMarketCache.model_validate_json(self.cache_path.read_bytes())


def flatten_token_ids(markets: Sequence[PolymarketMarketMetadata]) -> tuple[str, ...]:
    return tuple(token_id for market in markets for token_id in market.token_ids)


def token_side_labels(
    markets: Sequence[PolymarketMarketMetadata],
) -> dict[str, tuple[PolymarketMarketMetadata, str]]:
    mapping: dict[str, tuple[PolymarketMarketMetadata, str]] = {}
    for market in markets:
        mapping[market.yes_token_id] = (market, "YES")
        mapping[market.no_token_id] = (market, "NO")
    return mapping


def is_short_duration_crypto_market(payload: dict[str, Any]) -> bool:
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "slug", "market_slug", "description", "groupItemTitle")
    ).lower()
    if not any(term in text for term in ("btc", "bitcoin", "eth", "ethereum")):
        return False
    if not any(term in text for term in ("up or down", "up/down", "above", "below")):
        return False
    duration = infer_duration_minutes(payload)
    return duration in {5, 15}


def parse_market_metadata(payload: dict[str, Any]) -> PolymarketMarketMetadata | None:
    token_ids = _parse_jsonish_list(payload.get("clobTokenIds") or payload.get("clob_token_ids"))
    outcomes = _parse_jsonish_list(payload.get("outcomes"))
    if len(token_ids) < 2:
        return None

    yes_index = _outcome_index(outcomes, "yes", default=0)
    no_index = _outcome_index(outcomes, "no", default=1)
    if yes_index >= len(token_ids) or no_index >= len(token_ids):
        return None

    condition_id = str(
        payload.get("conditionId")
        or payload.get("condition_id")
        or payload.get("condition")
        or payload.get("market")
        or payload.get("id")
        or ""
    )
    market_id = str(payload.get("market") or payload.get("id") or condition_id)
    market_slug = str(payload.get("slug") or payload.get("market_slug") or "")
    question = str(payload.get("question") or "")
    end_time = str(payload.get("endDateIso") or payload.get("endDate") or payload.get("end_time") or "")

    if not condition_id or not market_id or not market_slug or not question:
        return None

    return PolymarketMarketMetadata(
        condition_id=condition_id,
        market_id=market_id,
        market_slug=market_slug,
        question=question,
        end_time=end_time,
        yes_token_id=str(token_ids[yes_index]),
        no_token_id=str(token_ids[no_index]),
        tick_size=_float_from_any(
            payload.get("order_price_min_tick_size")
            or payload.get("minimum_tick_size")
            or payload.get("tick_size")
            or payload.get("tickSize")
            or 0.01
        ),
        min_order_size=_float_from_any(
            payload.get("minimum_order_size")
            or payload.get("min_order_size")
            or payload.get("rewardsMinSize")
            or 0.0
        ),
        base_asset=_infer_base_asset(payload),
        duration_minutes=infer_duration_minutes(payload),
    )


def infer_duration_minutes(payload: dict[str, Any]) -> int | None:
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "slug", "market_slug", "description", "groupItemTitle")
    ).lower()
    if re.search(r"(?<!\d)15[\s-]?(?:m|min|minute)s?(?!\d)", text):
        return 15
    if re.search(r"(?<!\d)5[\s-]?(?:m|min|minute)s?(?!\d)", text):
        return 5

    start = _parse_datetime(payload.get("startDate") or payload.get("start_time"))
    end = _parse_datetime(payload.get("endDateIso") or payload.get("endDate") or payload.get("end_time"))
    if start is None or end is None:
        return None
    minutes = round((end - start).total_seconds() / 60)
    return minutes if minutes in {5, 15} else None


def _parse_jsonish_list(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = orjson.loads(value)
        except orjson.JSONDecodeError:
            return [part.strip() for part in value.split(",") if part.strip()]
        return decoded if isinstance(decoded, list) else []
    return []


def _outcome_index(outcomes: Sequence[object], target: str, *, default: int) -> int:
    for index, outcome in enumerate(outcomes):
        if str(outcome).strip().lower() == target:
            return index
    return default


def _infer_base_asset(payload: dict[str, Any]) -> str | None:
    text = " ".join(str(payload.get(key, "")) for key in ("question", "slug", "description")).lower()
    if "btc" in text or "bitcoin" in text:
        return "BTC"
    if "eth" in text or "ethereum" in text:
        return "ETH"
    return None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _float_from_any(value: object) -> float:
    if isinstance(value, int | float | str | bytes):
        return float(value)
    return 0.0
