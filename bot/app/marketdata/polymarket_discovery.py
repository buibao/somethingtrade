from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any

import aiohttp
import orjson
from pydantic import BaseModel, ConfigDict, Field
import structlog

from app.core.clock import utc_now_ns
from app.core.events import GapDirection

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_MARKET_CACHE_PATH = Path("data/cache/polymarket_markets.json")

UP_OUTCOMES = {"up", "above", "higher"}
DOWN_OUTCOMES = {"down", "below", "lower"}


class PolymarketMarketMetadata(BaseModel):
    """Public metadata needed to subscribe to and display a binary CLOB market."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    condition_id: str
    market_id: str
    market_slug: str
    question: str
    end_time: str
    tick_size: float
    min_order_size: float
    up_token_id: str | None = None
    down_token_id: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None
    token_outcomes: dict[str, str] = Field(default_factory=dict)
    base_asset: str | None = None
    duration_minutes: int | None = None

    @property
    def token_ids(self) -> tuple[str, ...]:
        if self.token_outcomes:
            return tuple(self.token_outcomes)
        return tuple(
            token_id
            for token_id in (
                self.up_token_id,
                self.down_token_id,
                self.yes_token_id,
                self.no_token_id,
            )
            if token_id is not None
        )

    def token_for_direction(self, direction: GapDirection) -> str | None:
        if direction == "UP":
            return self.up_token_id
        return self.down_token_id


class PolymarketMarketCache(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    discovered_at_ts: int | None = None
    markets: list[PolymarketMarketMetadata] = Field(default_factory=list)


FetchMarkets = Callable[[], Sequence[dict[str, Any]]]
RejectLogger = Callable[[str], None]


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
        self._logger = structlog.get_logger("polymarket_discovery")

    async def discover(self, *, write_cache: bool = True) -> tuple[PolymarketMarketMetadata, ...]:
        raw_markets = (
            list(self._fetch_markets()) if self._fetch_markets is not None else await self._fetch()
        )
        markets: list[PolymarketMarketMetadata] = []
        for payload in raw_markets:
            if not is_short_duration_crypto_market(payload):
                continue
            def log_reject(reason: str) -> None:
                self._log_reject(payload, reason)

            metadata = parse_market_metadata(
                payload,
                reject_logger=log_reject,
            )
            if metadata is not None:
                markets.append(metadata)

        if write_cache:
            self.write_cache(markets)
        return tuple(markets)

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

    def _log_reject(self, payload: dict[str, Any], reason: str) -> None:
        self._logger.warning(
            "polymarket_market_rejected",
            reason=reason,
            slug=payload.get("slug") or payload.get("market_slug"),
            question=payload.get("question"),
            condition_id=payload.get("conditionId") or payload.get("condition_id"),
        )


def flatten_token_ids(markets: Sequence[PolymarketMarketMetadata]) -> tuple[str, ...]:
    return tuple(token_id for market in markets for token_id in market.token_ids)


def token_side_labels(
    markets: Sequence[PolymarketMarketMetadata],
) -> dict[str, tuple[PolymarketMarketMetadata, str]]:
    mapping: dict[str, tuple[PolymarketMarketMetadata, str]] = {}
    for market in markets:
        for token_id, outcome in market.token_outcomes.items():
            mapping[token_id] = (market, outcome)
    return mapping


def is_short_duration_crypto_market(payload: dict[str, Any]) -> bool:
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "slug", "market_slug", "description", "groupItemTitle")
    ).lower()
    if not any(term in text for term in ("btc", "bitcoin", "eth", "ethereum")):
        return False
    if not any(
        term in text
        for term in (
            "up or down",
            "up/down",
            "up",
            "down",
            "above",
            "below",
            "higher",
            "lower",
        )
    ):
        return False
    duration = infer_duration_minutes(payload)
    return duration in {5, 15}


def parse_market_metadata(
    payload: dict[str, Any],
    *,
    reject_logger: RejectLogger | None = None,
) -> PolymarketMarketMetadata | None:
    def reject(reason: str) -> None:
        if reject_logger is not None:
            reject_logger(reason)

    token_ids = _parse_jsonish_list(payload.get("clobTokenIds") or payload.get("clob_token_ids"))
    outcomes = _parse_jsonish_list(payload.get("outcomes"))
    if len(token_ids) < 2:
        reject("missing_clob_token_ids")
        return None
    if len(outcomes) != len(token_ids):
        reject("malformed_outcomes")
        return None

    token_id_strings = [str(token_id) for token_id in token_ids]
    outcome_strings = [str(outcome).strip() for outcome in outcomes]
    token_outcomes = dict(zip(token_id_strings, outcome_strings, strict=True))

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
        reject("missing_required_market_metadata")
        return None

    mapping = _map_direction_tokens(
        token_outcomes,
        question=question,
        slug=market_slug,
    )
    if mapping.up_token_id is None or mapping.down_token_id is None:
        reject(mapping.reject_reason or "direction_mapping_not_confident")
        return None

    return PolymarketMarketMetadata(
        condition_id=condition_id,
        market_id=market_id,
        market_slug=market_slug,
        question=question,
        end_time=end_time,
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
        up_token_id=mapping.up_token_id,
        down_token_id=mapping.down_token_id,
        yes_token_id=mapping.yes_token_id,
        no_token_id=mapping.no_token_id,
        token_outcomes=token_outcomes,
        base_asset=_infer_base_asset(payload),
        duration_minutes=infer_duration_minutes(payload),
    )


class _DirectionMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    up_token_id: str | None = None
    down_token_id: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None
    reject_reason: str | None = None


def _map_direction_tokens(
    token_outcomes: dict[str, str],
    *,
    question: str,
    slug: str,
) -> _DirectionMapping:
    up_token_id: str | None = None
    down_token_id: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None

    for token_id, outcome in token_outcomes.items():
        normalized = _normalize_outcome(outcome)
        if normalized == "yes":
            yes_token_id = token_id
        elif normalized == "no":
            no_token_id = token_id
        elif normalized in UP_OUTCOMES:
            up_token_id = token_id
        elif normalized in DOWN_OUTCOMES:
            down_token_id = token_id
        else:
            return _DirectionMapping(reject_reason=f"unsupported_outcome:{outcome}")

    if up_token_id is not None and down_token_id is not None:
        return _DirectionMapping(
            up_token_id=up_token_id,
            down_token_id=down_token_id,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
        )

    if yes_token_id is not None and no_token_id is not None:
        yes_direction = _infer_yes_direction(question=question, slug=slug)
        if yes_direction == "UP":
            return _DirectionMapping(
                up_token_id=yes_token_id,
                down_token_id=no_token_id,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
            )
        if yes_direction == "DOWN":
            return _DirectionMapping(
                up_token_id=no_token_id,
                down_token_id=yes_token_id,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
            )
        return _DirectionMapping(
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            reject_reason="yes_no_direction_ambiguous",
        )

    return _DirectionMapping(reject_reason="missing_up_down_direction_tokens")


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


def _normalize_outcome(outcome: str) -> str:
    return outcome.strip().lower().replace("_", " ").replace("-", " ")


def _infer_yes_direction(*, question: str, slug: str) -> GapDirection | None:
    text = f"{question} {slug}".lower()
    up_matches = _direction_matches(text, UP_OUTCOMES)
    down_matches = _direction_matches(text, DOWN_OUTCOMES)
    if up_matches and not down_matches:
        return "UP"
    if down_matches and not up_matches:
        return "DOWN"
    return None


def _direction_matches(text: str, terms: set[str]) -> bool:
    return any(re.search(rf"\b{re.escape(term)}\b", text) for term in terms)


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
