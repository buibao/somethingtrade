from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any, Literal
from urllib.parse import quote

import aiohttp
import orjson
from pydantic import BaseModel, ConfigDict, Field
import structlog

from app.core.clock import utc_now_ns
from app.core.events import GapDirection

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_MARKET_CACHE_PATH = Path("data/cache/polymarket_markets.json")
DEFAULT_DISCOVERY_DEBUG_JSONL_PATH = Path("data/debug/polymarket_discovery_attempts.jsonl")
DEFAULT_MARKET_CACHE_TTL_MS = 60_000

UP_OUTCOMES = {"up", "above", "higher"}
DOWN_OUTCOMES = {"down", "below", "lower"}
ROLLING_HORIZON_SECONDS = {"5m": 300, "15m": 900}
ROLLING_UPDOWN_SLUG_RE = re.compile(r"\b(btc|eth)-updown-(5m|15m)-(\d+)\b")
DiscoverySource = Literal["direct_slug", "active_events", "cache"]
MarketWindowClassification = Literal[
    "runtime_tradable",
    "current_signal",
    "next_warmup",
    "future_tracked",
    "expired",
    "closed",
    "active_but_not_accepting_orders",
    "missing_orderbook",
    "missing_tokens",
    "not_found",
    "unknown",
    # Legacy cache/test values are accepted for backward-compatible cache reads.
    "current",
    "next",
    "future",
    "not_accepting",
]


def floor_to_window(ts: int, window_seconds: int) -> int:
    """Floor a Unix timestamp in seconds to a UTC window boundary."""

    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    return (ts // window_seconds) * window_seconds


def generate_crypto_updown_slugs(
    now_ts: int,
    assets: tuple[str, ...] = ("btc", "eth"),
    horizons: tuple[str, ...] = ("5m", "15m"),
    lookback_windows: int = 2,
    lookahead_windows: int = 2,
) -> list[str]:
    """Generate deterministic rolling crypto up/down market slugs around now."""

    slugs: list[str] = []
    seen: set[str] = set()
    for asset in assets:
        normalized_asset = asset.lower()
        for horizon in horizons:
            window_seconds = ROLLING_HORIZON_SECONDS.get(horizon)
            if window_seconds is None:
                raise ValueError(f"unsupported rolling horizon: {horizon}")
            current_window = floor_to_window(now_ts, window_seconds)
            for offset in range(-lookback_windows, lookahead_windows + 1):
                window_start = current_window + offset * window_seconds
                slug = f"{normalized_asset}-updown-{horizon}-{window_start}"
                if slug not in seen:
                    seen.add(slug)
                    slugs.append(slug)
    return slugs


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
    rewards_min_size: float | None = None
    start_time: str | None = None
    event_start_time: str | None = None
    active: bool | None = None
    closed: bool | None = None
    accepting_orders: bool | None = None
    enable_order_book: bool | None = None
    classification: MarketWindowClassification | None = None
    selected_for_runtime: bool = False
    signal_enabled: bool = False
    runtime_selection_reason: str | None = None
    discovery_source: DiscoverySource | None = None
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


@dataclass(frozen=True, slots=True)
class CacheRuntimeValidation:
    exists: bool
    valid: bool
    markets: tuple[PolymarketMarketMetadata, ...] = ()
    runtime_markets: tuple[PolymarketMarketMetadata, ...] = ()
    cache_age_ms: float | None = None
    ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS
    rejected: bool = False
    rejected_reason: str | None = None
    error: str | None = None

    def to_summary(self) -> dict[str, Any]:
        return {
            "exists": self.exists,
            "valid": self.valid,
            "cache_age_ms": self.cache_age_ms,
            "ttl_ms": self.ttl_ms,
            "runtime_count": len(self.runtime_markets),
            "market_count": len(self.markets),
            "rejected": self.rejected,
            "rejected_reason": self.rejected_reason,
            "error": self.error,
            "selected_market_slugs": [
                market.market_slug for market in self.runtime_markets
            ],
        }


@dataclass(frozen=True, slots=True)
class RollingDiscoveryResult:
    markets: tuple[PolymarketMarketMetadata, ...]
    runtime_markets: tuple[PolymarketMarketMetadata, ...]
    attempt: dict[str, Any]
    direct_results: tuple[dict[str, Any], ...] = ()
    active_events_result: dict[str, Any] | None = None
    cache_validation: CacheRuntimeValidation | None = None
    cache_used: bool = False
    fallback_used: bool = False


FetchMarkets = Callable[[], Sequence[dict[str, Any]]]
RejectLogger = Callable[[str], None]


class PolymarketDiscoveryClient:
    """Discover active short-duration crypto markets through public Gamma data."""

    def __init__(
        self,
        *,
        gamma_url: str = DEFAULT_GAMMA_URL,
        cache_path: Path | str = DEFAULT_MARKET_CACHE_PATH,
        limit: int = 100,
        page_limit: int | None = None,
        max_pages: int = 10,
        fetch_markets: FetchMarkets | None = None,
        enable_direct_slug_lookup: bool | None = None,
    ) -> None:
        self.gamma_url = gamma_url.rstrip("/")
        self.cache_path = Path(cache_path)
        self.limit = page_limit or limit
        self.max_pages = max_pages
        self._fetch_markets = fetch_markets
        self.enable_direct_slug_lookup = (
            fetch_markets is None
            if enable_direct_slug_lookup is None
            else enable_direct_slug_lookup
        )
        self._logger = structlog.get_logger("polymarket_discovery")

    async def discover(
        self,
        *,
        write_cache: bool = True,
        now_ts: int | None = None,
        rolling_lookahead_windows: int = 2,
        market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
        discovery_debug_jsonl: Path | str | None = None,
        refresh_reason: str | None = None,
    ) -> tuple[PolymarketMarketMetadata, ...]:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        if self.enable_direct_slug_lookup:
            rolling_result = await self.discover_rolling_markets_robust(
                now_ts=current_ts,
                lookahead_windows=rolling_lookahead_windows,
                market_cache_ttl_ms=market_cache_ttl_ms,
                discovery_debug_jsonl=discovery_debug_jsonl,
                write_cache=write_cache,
                refresh_reason=refresh_reason,
            )
            if rolling_result.markets:
                return rolling_result.markets

        raw_markets = await self._fetch_raw_market_payloads()
        markets = annotate_runtime_market_roles(
            self._parse_market_payloads(raw_markets),
            now_ts=current_ts,
            lookahead_windows=rolling_lookahead_windows,
        )

        if write_cache:
            self.write_runtime_cache_if_usable(markets, now_ts=current_ts)
        return tuple(markets)

    async def discover_rolling_markets(
        self,
        *,
        now_ts: int | None = None,
        lookahead_windows: int = 2,
    ) -> tuple[PolymarketMarketMetadata, ...]:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        markets, _ = await self._discover_rolling_slug_candidates(
            now_ts=current_ts,
            include_raw=False,
            lookahead_windows=lookahead_windows,
        )
        return annotate_runtime_market_roles(
            _with_discovery_source(markets, "direct_slug"),
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )

    async def discover_rolling_markets_robust(
        self,
        *,
        now_ts: int | None = None,
        lookahead_windows: int = 2,
        market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
        discovery_debug_jsonl: Path | str | None = None,
        write_cache: bool = False,
        force_active_events: bool = False,
        use_cache: bool = True,
        include_raw: bool = False,
        refresh_reason: str | None = None,
    ) -> RollingDiscoveryResult:
        """Discover rolling markets from direct slugs, active events, and safe cache."""

        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        direct_markets: tuple[PolymarketMarketMetadata, ...] = ()
        direct_results: list[dict[str, Any]] = []
        if self.enable_direct_slug_lookup:
            raw_direct_markets, direct_results = await self._discover_rolling_slug_candidates(
                now_ts=current_ts,
                include_raw=include_raw,
                lookahead_windows=lookahead_windows,
            )
            direct_markets = annotate_runtime_market_roles(
                _with_discovery_source(raw_direct_markets, "direct_slug"),
                now_ts=current_ts,
                lookahead_windows=lookahead_windows,
            )

        direct_runtime = select_runtime_markets(
            direct_markets,
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        cache_validation = self.validate_cache_for_runtime(
            now_ts=current_ts,
            ttl_ms=market_cache_ttl_ms,
            lookahead_windows=lookahead_windows,
        )
        direct_classifications = [
            classify_market_window(market, now_ts=current_ts) for market in direct_markets
        ]
        fallback_used = (
            force_active_events
            or not direct_runtime
            or not any(market.signal_enabled for market in direct_runtime)
            or (bool(direct_markets) and all(value == "closed" for value in direct_classifications))
            or (bool(direct_markets) and all(market.accepting_orders is False for market in direct_markets))
            or cache_validation.rejected
        )
        active_markets: tuple[PolymarketMarketMetadata, ...] = ()
        active_result: dict[str, Any] = _empty_active_events_result(attempted=False)
        if fallback_used:
            active_markets, active_result = await self.discover_active_event_rolling_markets(
                now_ts=current_ts,
                lookahead_windows=lookahead_windows,
                include_raw=include_raw,
            )

        merged_markets = annotate_runtime_market_roles(
            _dedupe_prefer_runtime(
                tuple(direct_markets) + tuple(active_markets),
                now_ts=current_ts,
            ),
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        runtime_markets = select_runtime_markets(
            merged_markets,
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )

        cache_used = False
        final_markets = merged_markets
        if use_cache and not runtime_markets and cache_validation.valid:
            final_markets = cache_validation.markets
            runtime_markets = cache_validation.runtime_markets
            cache_used = True

        cache_not_updated_reason = None
        if write_cache and not cache_used:
            cache_not_updated_reason = self.write_runtime_cache_if_usable(
                final_markets,
                runtime_markets=runtime_markets,
                now_ts=current_ts,
            )
        attempt = _build_discovery_attempt(
            now_ts=current_ts,
            final_markets=final_markets,
            runtime_markets=runtime_markets,
            direct_markets=direct_markets,
            direct_results=direct_results,
            active_markets=active_markets,
            active_result=active_result,
            cache_validation=cache_validation,
            cache_used=cache_used,
            fallback_used=fallback_used,
            refresh_reason=refresh_reason,
            cache_not_updated_reason=cache_not_updated_reason,
        )
        if discovery_debug_jsonl is not None:
            write_discovery_attempt_jsonl(discovery_debug_jsonl, attempt)

        return RollingDiscoveryResult(
            markets=final_markets,
            runtime_markets=runtime_markets,
            attempt=attempt,
            direct_results=tuple(direct_results),
            active_events_result=active_result,
            cache_validation=cache_validation,
            cache_used=cache_used,
            fallback_used=fallback_used,
        )

    async def debug_rolling_discovery(
        self,
        *,
        now_ts: int | None = None,
        market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
        discovery_debug_jsonl: Path | str | None = None,
    ) -> dict[str, Any]:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        robust = await self.discover_rolling_markets_robust(
            now_ts=current_ts,
            lookahead_windows=2,
            market_cache_ttl_ms=market_cache_ttl_ms,
            discovery_debug_jsonl=discovery_debug_jsonl,
            write_cache=False,
            force_active_events=True,
            include_raw=True,
        )
        markets = list(robust.markets)
        results = list(robust.direct_results)
        selected_keys = _market_keys(robust.runtime_markets)
        slugs = [str(result["slug"]) for result in results]
        found_slugs = [str(result["slug"]) for result in results if result["found"]]
        return {
            "generated_at_ts": current_ts,
            "generated_slugs": slugs,
            "found_slugs": found_slugs,
            "not_found_slugs": [slug for slug in slugs if slug not in set(found_slugs)],
            "strategy_results": robust.attempt["strategy_results"],
            "attempt": robust.attempt,
            "cache_validation": (
                None
                if robust.cache_validation is None
                else robust.cache_validation.to_summary()
            ),
            "active_events": robust.active_events_result,
            "selected_market_slugs": robust.attempt["selected_market_slugs"],
            "current_signal_slugs": robust.attempt["current_signal_slugs"],
            "next_warmup_slugs": robust.attempt["next_warmup_slugs"],
            "results": [
                _enrich_debug_result(result, now_ts=current_ts, selected_keys=selected_keys)
                for result in results
            ],
            "markets": [
                _debug_market_payload(
                    market,
                    now_ts=current_ts,
                    selected_keys=selected_keys,
                )
                for market in markets
            ],
        }

    async def discover_active_event_rolling_markets(
        self,
        *,
        now_ts: int | None = None,
        lookahead_windows: int = 2,
        include_raw: bool = False,
    ) -> tuple[tuple[PolymarketMarketMetadata, ...], dict[str, Any]]:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        events = await self._fetch_paginated_events()
        result = _empty_active_events_result(attempted=True)
        result["event_count"] = len(events)
        markets: list[PolymarketMarketMetadata] = []
        reject_reasons: list[str] = []

        for event in events:
            candidates = _extract_market_payloads(event, fallback_slug=None)
            result["candidate_count"] += len(candidates)
            for candidate in candidates:
                slug = str(candidate.get("slug") or candidate.get("market_slug") or "")
                if include_raw:
                    result.setdefault("raw_candidate_sample", []).append(candidate)
                if not is_short_duration_crypto_market(candidate):
                    reason = "not_btc_eth_5m_15m_updown"
                    reject_reasons.append(reason)
                    result["rejected_candidates"].append({"slug": slug, "reason": reason})
                    continue

                candidate_rejects: list[str] = []
                metadata = parse_market_metadata(
                    candidate,
                    reject_logger=candidate_rejects.append,
                )
                if metadata is None:
                    reason = ",".join(candidate_rejects) or "parse_failed"
                    reject_reasons.extend(candidate_rejects or [reason])
                    result["rejected_candidates"].append({"slug": slug, "reason": reason})
                    continue

                result["parsed_count"] += 1
                metadata = metadata.model_copy(update={"discovery_source": "active_events"})
                classification = classify_market_window(metadata, now_ts=current_ts)
                runtime_ready = is_runtime_tradable_market(metadata, now_ts=current_ts)
                compatible = _is_rolling_window_compatible(
                    metadata,
                    now_ts=current_ts,
                    lookahead_windows=lookahead_windows,
                )
                if not runtime_ready or not compatible:
                    reason = (
                        "incompatible_rolling_window"
                        if runtime_ready and not compatible
                        else classification
                    )
                    reject_reasons.append(reason)
                    result["rejected_candidates"].append(
                        {
                            "slug": metadata.market_slug,
                            "market_id": metadata.market_id,
                            "classification": classification,
                            "reason": reason,
                        }
                    )
                    continue

                markets.append(metadata)

        annotated = annotate_runtime_market_roles(
            _dedupe_prefer_runtime(tuple(markets), now_ts=current_ts),
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        active_runtime = select_runtime_markets(
            annotated,
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        current_signal_count = sum(1 for market in active_runtime if market.signal_enabled)
        result["runtime_candidate_count"] = len(active_runtime)
        result["runtime_tradable_count"] = current_signal_count
        result["current_signal_count"] = current_signal_count
        result["next_warmup_count"] = sum(
            1
            for market in active_runtime
            if market.runtime_selection_reason == "next_warmup"
        )
        result["reject_reasons"] = [
            {"reason": reason, "count": count}
            for reason, count in sorted(Counter(reject_reasons).items())
        ]
        result["markets"] = [market.model_dump(mode="json") for market in annotated]
        return annotated, result

    async def fetch_market_by_slug(self, slug: str) -> dict[str, Any] | None:
        return await self._fetch_slug_object("markets", slug)

    async def fetch_event_by_slug(self, slug: str) -> dict[str, Any] | None:
        return await self._fetch_slug_object("events", slug)

    async def _discover_rolling_slug_candidates(
        self,
        *,
        now_ts: int | None,
        include_raw: bool,
        lookahead_windows: int = 2,
    ) -> tuple[list[PolymarketMarketMetadata], list[dict[str, Any]]]:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        slugs = generate_crypto_updown_slugs(
            current_ts,
            lookahead_windows=max(1, lookahead_windows),
        )
        markets: list[PolymarketMarketMetadata] = []
        results: list[dict[str, Any]] = []
        seen_market_keys: set[tuple[str, tuple[str, ...]]] = set()

        for slug in slugs:
            slug_markets, result = await self._lookup_rolling_slug(
                slug,
                include_raw=include_raw,
            )
            results.append(result)
            for market in slug_markets:
                key = (market.market_id, market.token_ids)
                if key in seen_market_keys:
                    continue
                seen_market_keys.add(key)
                markets.append(market)

        return markets, results

    async def _lookup_rolling_slug(
        self,
        slug: str,
        *,
        include_raw: bool,
    ) -> tuple[list[PolymarketMarketMetadata], dict[str, Any]]:
        result: dict[str, Any] = {
            "slug": slug,
            "found": False,
            "endpoint_used": None,
            "parsed_markets": [],
            "reject_reasons": [],
            "attempts": [],
        }
        all_rejects: list[str] = []

        for endpoint_name, fetcher in (
            ("markets/slug", self.fetch_market_by_slug),
            ("events/slug", self.fetch_event_by_slug),
        ):
            payload = await fetcher(slug)
            if payload is None:
                result["attempts"].append({"endpoint": endpoint_name, "found": False})
                continue

            result["found"] = True
            if include_raw:
                result.setdefault("raw_payloads", {})[endpoint_name] = payload

            candidates = _extract_market_payloads(payload, fallback_slug=slug)
            parsed: list[PolymarketMarketMetadata] = []
            rejects: list[str] = []
            for candidate in candidates:
                metadata = parse_market_metadata(candidate, reject_logger=rejects.append)
                if metadata is not None:
                    parsed.append(metadata)

            all_rejects.extend(rejects)
            result["attempts"].append(
                {
                    "endpoint": endpoint_name,
                    "found": True,
                    "candidate_count": len(candidates),
                    "parsed_count": len(parsed),
                    "reject_reasons": rejects,
                }
            )
            if parsed:
                result["endpoint_used"] = endpoint_name
                result["parsed_markets"] = [
                    market.model_dump(mode="json") for market in parsed
                ]
                result["reject_reasons"] = all_rejects
                return parsed, result

        result["reject_reasons"] = all_rejects
        return [], result

    async def _fetch_slug_object(self, endpoint: str, slug: str) -> dict[str, Any] | None:
        paths = (
            f"{endpoint}/slug/{quote(slug, safe='')}",
            f"{endpoint}?slug={quote(slug, safe='')}",
        )
        for path in paths:
            try:
                payload = await self._request_json(path)
            except (aiohttp.ClientError, TimeoutError, OSError) as exc:
                self._logger.warning(
                    "gamma_slug_lookup_network_error",
                    endpoint=endpoint,
                    slug=slug,
                    path=path,
                    error=str(exc),
                )
                return None
            except orjson.JSONDecodeError as exc:
                self._logger.warning(
                    "gamma_slug_lookup_json_error",
                    endpoint=endpoint,
                    slug=slug,
                    path=path,
                    error=str(exc),
                )
                return None

            if isinstance(payload, dict) and payload:
                return payload
            if isinstance(payload, list):
                return next((item for item in payload if isinstance(item, dict)), None)
        return None

    async def _request_json(self, path: str) -> Any:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(f"{self.gamma_url}/{path}") as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                body = await response.read()
        if not body.strip():
            return None
        return orjson.loads(body)

    async def _fetch_raw_market_payloads(self) -> list[dict[str, Any]]:
        if self._fetch_markets is not None:
            return list(self._fetch_markets())

        search_payloads = await self._fetch_search_market_payloads()
        if search_payloads:
            return search_payloads

        event_payloads = await self._fetch_event_market_payloads()
        if event_payloads:
            return event_payloads

        return await self._fetch_paginated_markets()

    async def _fetch_search_market_payloads(self) -> list[dict[str, Any]]:
        return []

    async def _fetch_event_market_payloads(self) -> list[dict[str, Any]]:
        events = await self._fetch_paginated_events()
        payloads: list[dict[str, Any]] = []
        for event in events:
            payloads.extend(_extract_market_payloads(event, fallback_slug=None))
        return payloads

    async def _fetch_paginated_events(self) -> list[dict[str, Any]]:
        return await self._fetch_paginated_endpoint("events")

    async def _fetch_paginated_markets(self) -> list[dict[str, Any]]:
        return await self._fetch_paginated_endpoint("markets")

    async def _fetch_paginated_endpoint(self, endpoint: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        total_scanned = 0
        for page_index in range(self.max_pages):
            offset = page_index * self.limit
            page = await self._fetch_page(endpoint, limit=self.limit, offset=offset)
            total_scanned += len(page)
            if not page:
                break
            items.extend(page)
        self._logger.info(
            "gamma_pagination_scanned",
            endpoint=endpoint,
            total_scanned=total_scanned,
            limit=self.limit,
            max_pages=self.max_pages,
        )
        return items

    async def _fetch_page(
        self,
        endpoint: str,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.gamma_url}/{endpoint}",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": str(limit),
                    "offset": str(offset),
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Gamma /{endpoint} response must be a list")
        return [item for item in payload if isinstance(item, dict)]

    def _parse_market_payloads(
        self,
        raw_markets: Sequence[dict[str, Any]],
        *,
        discovery_source: DiscoverySource | None = None,
    ) -> list[PolymarketMarketMetadata]:
        markets: list[PolymarketMarketMetadata] = []
        seen_market_keys: set[tuple[str, tuple[str, ...]]] = set()
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
                if discovery_source is not None:
                    metadata = metadata.model_copy(update={"discovery_source": discovery_source})
                key = (metadata.market_id, metadata.token_ids)
                if key in seen_market_keys:
                    continue
                seen_market_keys.add(key)
                markets.append(metadata)
        return markets

    def validate_cache_for_runtime(
        self,
        *,
        now_ts: int | None = None,
        ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
        lookahead_windows: int = 2,
    ) -> CacheRuntimeValidation:
        def reject(
            reason: str,
            *,
            markets: tuple[PolymarketMarketMetadata, ...] = (),
            runtime_markets: tuple[PolymarketMarketMetadata, ...] = (),
            age_ms: float | None = None,
            error: str | None = None,
        ) -> CacheRuntimeValidation:
            self._logger.warning(
                "cache_rejected_for_runtime",
                reason=reason,
                cache_path=str(self.cache_path),
                cache_age_ms=age_ms,
                market_count=len(markets),
                runtime_count=len(runtime_markets),
            )
            return CacheRuntimeValidation(
                exists=True,
                valid=False,
                markets=markets,
                runtime_markets=runtime_markets,
                cache_age_ms=age_ms,
                ttl_ms=ttl_ms,
                rejected=True,
                rejected_reason=reason,
                error=error,
            )

        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        if not self.cache_path.exists():
            return CacheRuntimeValidation(exists=False, valid=False, ttl_ms=ttl_ms)
        try:
            cache = self.read_cache()
        except (OSError, ValueError) as exc:
            return reject(
                "cache_missing_required_fields",
                error=f"{type(exc).__name__}: {exc}",
            )

        age_ms = _cache_age_ms(cache.discovered_at_ts)
        annotated = annotate_runtime_market_roles(
            _with_discovery_source(cache.markets, "cache"),
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        runtime_markets = select_runtime_markets(
            annotated,
            now_ts=current_ts,
            lookahead_windows=lookahead_windows,
        )
        if age_ms is None:
            return reject(
                "cache_missing_required_fields",
                markets=annotated,
                runtime_markets=runtime_markets,
                age_ms=age_ms,
            )
        if age_ms > ttl_ms:
            return reject(
                "cache_expired",
                markets=annotated,
                runtime_markets=runtime_markets,
                age_ms=age_ms,
            )
        if annotated and all(market.closed is True for market in annotated):
            return reject(
                "cache_all_closed",
                markets=annotated,
                runtime_markets=runtime_markets,
                age_ms=age_ms,
            )
        if annotated and all(market.accepting_orders is False for market in annotated):
            return reject(
                "cache_no_runtime_markets",
                markets=annotated,
                runtime_markets=runtime_markets,
                age_ms=age_ms,
            )
        has_current_or_next = any(
            market.classification in {"current_signal", "next_warmup"}
            for market in runtime_markets
        )
        if not runtime_markets or not has_current_or_next:
            return reject(
                "cache_no_runtime_markets",
                markets=annotated,
                runtime_markets=runtime_markets,
                age_ms=age_ms,
            )
        return CacheRuntimeValidation(
            exists=True,
            valid=True,
            markets=annotated,
            runtime_markets=runtime_markets,
            cache_age_ms=age_ms,
            ttl_ms=ttl_ms,
        )

    def write_cache(self, markets: Sequence[PolymarketMarketMetadata]) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache = PolymarketMarketCache(discovered_at_ts=utc_now_ns(), markets=list(markets))
        self.cache_path.write_bytes(
            orjson.dumps(
                cache.model_dump(mode="json"),
                option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE,
            )
        )

    def write_runtime_cache_if_usable(
        self,
        markets: Sequence[PolymarketMarketMetadata],
        *,
        runtime_markets: Sequence[PolymarketMarketMetadata] | None = None,
        now_ts: int | None = None,
    ) -> str | None:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        annotated = annotate_runtime_market_roles(markets, now_ts=current_ts)
        selected = (
            tuple(runtime_markets)
            if runtime_markets is not None
            else select_runtime_markets(annotated, now_ts=current_ts)
        )
        if annotated and all(market.closed is True for market in annotated):
            return "cache_not_updated_all_closed"
        if annotated and all(market.accepting_orders is False for market in annotated):
            return "cache_not_updated_all_accepting_orders_false"
        if not selected:
            return "cache_not_updated_runtime_count_zero"
        if not any(
            market.classification in {"current_signal", "next_warmup"}
            or market.runtime_selection_reason in {"current_signal", "next_warmup"}
            for market in selected
        ):
            return "cache_not_updated_no_current_or_warmup"
        self.write_cache(annotated)
        return None

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


def write_discovery_attempt_jsonl(path: Path | str, attempt: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("ab") as handle:
        handle.write(orjson.dumps(attempt, option=orjson.OPT_APPEND_NEWLINE))


def _empty_active_events_result(*, attempted: bool) -> dict[str, Any]:
    return {
        "attempted": attempted,
        "event_count": 0,
        "candidate_count": 0,
        "parsed_count": 0,
        "runtime_tradable_count": 0,
        "runtime_candidate_count": 0,
        "current_signal_count": 0,
        "next_warmup_count": 0,
        "reject_reasons": [],
        "rejected_candidates": [],
        "markets": [],
    }


def _with_discovery_source(
    markets: Sequence[PolymarketMarketMetadata],
    source: DiscoverySource,
) -> tuple[PolymarketMarketMetadata, ...]:
    return tuple(market.model_copy(update={"discovery_source": source}) for market in markets)


def _dedupe_prefer_runtime(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
) -> tuple[PolymarketMarketMetadata, ...]:
    selected_by_key: dict[tuple[str, tuple[str, ...]], PolymarketMarketMetadata] = {}
    order: list[tuple[str, tuple[str, ...]]] = []
    for market in markets:
        key = _market_key(market)
        if key not in selected_by_key:
            selected_by_key[key] = market
            order.append(key)
            continue
        previous = selected_by_key[key]
        previous_runtime = is_runtime_tradable_market(previous, now_ts=now_ts)
        current_runtime = is_runtime_tradable_market(market, now_ts=now_ts)
        if current_runtime and not previous_runtime:
            selected_by_key[key] = market
    return tuple(selected_by_key[key] for key in order)


def _build_discovery_attempt(
    *,
    now_ts: int,
    final_markets: Sequence[PolymarketMarketMetadata],
    runtime_markets: Sequence[PolymarketMarketMetadata],
    direct_markets: Sequence[PolymarketMarketMetadata],
    direct_results: Sequence[dict[str, Any]],
    active_markets: Sequence[PolymarketMarketMetadata],
    active_result: dict[str, Any],
    cache_validation: CacheRuntimeValidation,
    cache_used: bool,
    fallback_used: bool,
    refresh_reason: str | None = None,
    cache_not_updated_reason: str | None = None,
) -> dict[str, Any]:
    time_diagnostics = _time_sanity_diagnostics(now_ts, direct_markets, direct_results)
    classifications = Counter(
        classify_market_window(market, now_ts=now_ts) for market in final_markets
    )
    selected_market_slugs = [market.market_slug for market in runtime_markets]
    current_signal_slugs = [
        market.market_slug for market in runtime_markets if market.signal_enabled
    ]
    next_warmup_slugs = [
        market.market_slug
        for market in runtime_markets
        if market.runtime_selection_reason == "next_warmup"
    ]
    failure_reason = None
    direct_found_count = sum(1 for result in direct_results if result.get("found"))
    direct_runtime_count = sum(
        1 for market in direct_markets if is_runtime_tradable_market(market, now_ts=now_ts)
    )
    active_events_runtime_count = int(active_result.get("runtime_tradable_count", 0))
    cache_runtime_count = len(cache_validation.runtime_markets)
    if not runtime_markets:
        if direct_found_count and direct_markets and all(
            classify_market_window(market, now_ts=now_ts) == "closed"
            for market in direct_markets
        ):
            failure_reason = "direct_slug_found_but_all_closed"
        else:
            failure_reason = "no_runtime_tradable_markets"

    diagnostics = list(time_diagnostics.pop("diagnostics"))
    if failure_reason == "direct_slug_found_but_all_closed":
        diagnostics.append("direct_slug_found_but_all_closed")

    attempt = {
        "event_type": "polymarket_discovery_attempt",
        "refresh_reason": refresh_reason,
        "timestamp": _iso_from_unix_seconds(now_ts),
        "now_utc": _iso_from_unix_seconds(now_ts),
        "now_utc_iso": _iso_from_unix_seconds(now_ts),
        "local_system_time_iso": datetime.now().astimezone().isoformat(),
        "strategy_results": {
            "direct_slug": {
                "attempted": bool(direct_results),
                "generated_slug_count": len(direct_results),
                "found_count": direct_found_count,
                "market_count": len(direct_markets),
                "runtime_tradable_count": direct_runtime_count,
            },
            "active_events": {
                "attempted": bool(active_result.get("attempted")),
                "event_count": active_result.get("event_count", 0),
                "candidate_count": active_result.get("candidate_count", 0),
                "parsed_count": active_result.get("parsed_count", 0),
                "market_count": len(active_markets),
                "runtime_tradable_count": active_result.get("runtime_tradable_count", 0),
            },
            "cache": cache_validation.to_summary(),
        },
        "generated_slug_count": len(direct_results),
        "direct_found_count": direct_found_count,
        "direct_runtime_count": direct_runtime_count,
        "active_events_found_count": len(active_markets),
        "active_events_runtime_count": active_events_runtime_count,
        "active_events_found_runtime_count": active_events_runtime_count,
        "cache_runtime_count": cache_runtime_count,
        "runtime_tradable_count": len(runtime_markets),
        "current_signal_count": len(current_signal_slugs),
        "next_warmup_count": len(next_warmup_slugs),
        "closed_count": classifications.get("closed", 0),
        "accepting_orders_false_count": sum(
            1 for market in final_markets if market.accepting_orders is False
        ),
        "missing_orderbook_count": classifications.get("missing_orderbook", 0),
        "missing_tokens_count": classifications.get("missing_tokens", 0)
        + _reject_count(direct_results, "missing_clob_token_ids")
        + _active_reject_count(active_result, "missing_clob_token_ids"),
        "cache_used": cache_used,
        "cache_rejected": cache_validation.rejected,
        "cache_rejected_reason": cache_validation.rejected_reason,
        "cache_not_updated": cache_not_updated_reason is not None,
        "cache_not_updated_reason": cache_not_updated_reason,
        "selected_market_slugs": selected_market_slugs,
        "current_signal_slugs": current_signal_slugs,
        "next_warmup_slugs": next_warmup_slugs,
        "fallback_used": fallback_used,
        "failure_reason": failure_reason,
        "diagnostics": diagnostics,
        **time_diagnostics,
    }
    return attempt


def _reject_count(results: Sequence[dict[str, Any]], reason: str) -> int:
    total = 0
    for result in results:
        for reject in result.get("reject_reasons") or []:
            if str(reject) == reason:
                total += 1
    return total


def _active_reject_count(active_result: dict[str, Any], reason: str) -> int:
    total = 0
    for item in active_result.get("reject_reasons") or []:
        if isinstance(item, dict) and item.get("reason") == reason:
            total += int(item.get("count", 0))
    return total


def _time_sanity_diagnostics(
    now_ts: int,
    direct_markets: Sequence[PolymarketMarketMetadata],
    direct_results: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    generated = _generated_window_bounds(
        now_ts,
        [str(result.get("slug")) for result in direct_results if result.get("slug")],
    )
    diagnostics: list[str] = []
    closed_future_count = 0
    accepting_false_count = 0
    for market in direct_markets:
        start_ts = _market_start_sort_key(market)
        end_ts = _unix_seconds(market.end_time)
        current_or_future = (
            end_ts is not None
            and (start_ts <= now_ts < end_ts or start_ts > now_ts)
        )
        if market.closed is True and current_or_future:
            closed_future_count += 1
        if market.active is True and market.accepting_orders is False:
            accepting_false_count += 1
    if closed_future_count:
        diagnostics.append("direct_slug_returned_closed_for_current_or_future_window")
    return {
        **generated,
        "system_clock_skew_warning": None,
        "direct_slug_closed_future_window_count": closed_future_count,
        "direct_slug_active_but_accepting_orders_false_count": accepting_false_count,
        "diagnostics": diagnostics,
    }


def _generated_window_bounds(now_ts: int, slugs: Sequence[str]) -> dict[str, Any]:
    windows: list[tuple[int, int]] = []
    for slug in slugs:
        parts = _rolling_slug_parts(slug)
        if parts is None:
            continue
        _asset, horizon, start_ts = parts
        duration = ROLLING_HORIZON_SECONDS[horizon]
        windows.append((start_ts, start_ts + duration))
    future_starts = [start for start, _end in windows if start > now_ts]
    ended = [end for _start, end in windows if end <= now_ts]
    return {
        "generated_slug_min_start": min((start for start, _end in windows), default=None),
        "generated_slug_max_end": max((end for _start, end in windows), default=None),
        "seconds_until_next_generated_start": (
            min(future_starts) - now_ts if future_starts else None
        ),
        "seconds_since_last_generated_end": (
            now_ts - max(ended) if ended else None
        ),
    }


def is_runtime_tradable_market(
    market: PolymarketMarketMetadata,
    *,
    now_ts: int,
) -> bool:
    """Return whether a discovered market is safe to subscribe for runtime measurement."""

    if market.active is not True:
        return False
    if market.closed is not False:
        return False
    if market.accepting_orders is not True:
        return False
    if market.enable_order_book is not True:
        return False
    if market.up_token_id is None or market.down_token_id is None:
        return False
    if len(market.token_ids) < 2:
        return False

    end_ts = _unix_seconds(market.end_time)
    return end_ts is not None and end_ts > now_ts


def classify_market_window(
    market: PolymarketMarketMetadata,
    *,
    now_ts: int,
) -> MarketWindowClassification:
    """Classify a rolling market relative to now for debug and runtime selection."""

    if market.closed is True:
        return "closed"
    if market.active is True and market.closed is False and market.accepting_orders is not True:
        return "active_but_not_accepting_orders"
    if market.active is True and market.closed is False and market.enable_order_book is not True:
        return "missing_orderbook"
    if market.up_token_id is None or market.down_token_id is None or len(market.token_ids) < 2:
        return "missing_tokens"
    if market.active is not True or market.closed is not False:
        return "unknown"

    end_ts = _unix_seconds(market.end_time)
    if end_ts is None:
        return "runtime_tradable"
    if end_ts <= now_ts:
        return "expired"

    start_ts = _market_start_sort_key(market)
    if start_ts <= now_ts < end_ts:
        return "current_signal"
    if start_ts > now_ts:
        duration_seconds = _market_duration_seconds(market)
        if duration_seconds is not None and start_ts - now_ts <= duration_seconds:
            return "next_warmup"
        return "future_tracked"
    return "expired"


def select_runtime_markets(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> tuple[PolymarketMarketMetadata, ...]:
    """Select current plus first future market per asset/duration for live monitoring."""

    annotated = annotate_runtime_market_roles(
        markets,
        now_ts=now_ts,
        lookahead_windows=lookahead_windows,
    )
    selected_by_key = {_market_key(market): market for market in annotated}
    return tuple(
        selected_by_key[key]
        for key in _select_runtime_market_key_order(
            markets,
            now_ts=now_ts,
            lookahead_windows=lookahead_windows,
        )
        if key in selected_by_key
    )


def annotate_runtime_market_roles(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> tuple[PolymarketMarketMetadata, ...]:
    selected_roles = _select_runtime_market_key_roles(
        markets,
        now_ts=now_ts,
        lookahead_windows=lookahead_windows,
    )
    annotated: list[PolymarketMarketMetadata] = []
    for market in markets:
        classification = classify_market_window(market, now_ts=now_ts)
        selected_role = selected_roles.get(_market_key(market))
        selected = selected_role is not None
        signal_enabled = selected_role == "current_signal"
        reason = _runtime_selection_reason(
            classification=classification,
            selected_role=selected_role,
            signal_enabled=signal_enabled,
        )
        annotated.append(
            market.model_copy(
                update={
                    "classification": classification,
                    "selected_for_runtime": selected,
                    "signal_enabled": signal_enabled,
                    "runtime_selection_reason": reason,
                }
            )
        )
    return tuple(annotated)


def _select_runtime_market_keys(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> set[tuple[str, tuple[str, ...]]]:
    return set(
        _select_runtime_market_key_order(
            markets,
            now_ts=now_ts,
            lookahead_windows=lookahead_windows,
        )
    )


def _select_runtime_market_key_roles(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> dict[tuple[str, tuple[str, ...]], MarketWindowClassification]:
    roles: dict[tuple[str, tuple[str, ...]], MarketWindowClassification] = {}
    for key, role in _select_runtime_market_key_role_order(
        markets,
        now_ts=now_ts,
        lookahead_windows=lookahead_windows,
    ):
        roles[key] = role
    return roles


def _select_runtime_market_key_order(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> list[tuple[str, tuple[str, ...]]]:
    return [
        key
        for key, _role in _select_runtime_market_key_role_order(
            markets,
            now_ts=now_ts,
            lookahead_windows=lookahead_windows,
        )
    ]


def _select_runtime_market_key_role_order(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 1,
) -> list[tuple[tuple[str, tuple[str, ...]], MarketWindowClassification]]:
    groups: dict[tuple[str | None, int | None], list[PolymarketMarketMetadata]] = {}
    group_order: list[tuple[str | None, int | None]] = []
    for market in markets:
        if not is_runtime_tradable_market(market, now_ts=now_ts):
            continue
        key = (market.base_asset, market.duration_minutes)
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append(market)

    selected: list[tuple[PolymarketMarketMetadata, MarketWindowClassification]] = []
    for key in group_order:
        group = groups[key]
        current = sorted(
            (market for market in group if _is_current_window(market, now_ts=now_ts)),
            key=_market_start_sort_key,
        )
        selected.extend((market, "current_signal") for market in current)

        futures = [
            market
            for market in group
            if _market_start_sort_key(market) > now_ts
        ]
        futures.sort(key=_market_start_sort_key)
        for index, market in enumerate(futures[: max(1, lookahead_windows)]):
            role: MarketWindowClassification = (
                "next_warmup" if index == 0 else "future_tracked"
            )
            selected.append((market, role))

    return [(_market_key(market), role) for market, role in selected]


def _runtime_selection_reason(
    *,
    classification: MarketWindowClassification,
    selected_role: MarketWindowClassification | None,
    signal_enabled: bool,
) -> str:
    if signal_enabled:
        return "current_signal"
    if selected_role is not None:
        return selected_role
    return classification


def _market_key(market: PolymarketMarketMetadata) -> tuple[str, tuple[str, ...]]:
    return market.market_id, market.token_ids


def _market_keys(
    markets: Sequence[PolymarketMarketMetadata],
) -> set[tuple[str, tuple[str, ...]]]:
    return {_market_key(market) for market in markets}


def _debug_market_payload(
    market: PolymarketMarketMetadata,
    *,
    now_ts: int,
    selected_keys: set[tuple[str, tuple[str, ...]]],
) -> dict[str, Any]:
    classification = classify_market_window(market, now_ts=now_ts)
    selected = _market_key(market) in selected_keys
    signal_enabled = selected and classification == "current_signal"
    selected_role = classification if selected else None
    payload = market.model_dump(mode="json")
    payload["classification"] = classification
    payload["selected_for_runtime"] = selected
    payload["signal_enabled"] = signal_enabled
    payload["runtime_selection_reason"] = _runtime_selection_reason(
        classification=classification,
        selected_role=selected_role,
        signal_enabled=signal_enabled,
    )
    payload["token_for_up"] = market.token_for_direction("UP")
    payload["token_for_down"] = market.token_for_direction("DOWN")
    return payload


def _enrich_debug_result(
    result: dict[str, Any],
    *,
    now_ts: int,
    selected_keys: set[tuple[str, tuple[str, ...]]],
) -> dict[str, Any]:
    enriched = dict(result)
    enriched["parsed_markets"] = [
        _enrich_debug_market_dict(
            market,
            now_ts=now_ts,
            selected_keys=selected_keys,
        )
        for market in result.get("parsed_markets") or []
        if isinstance(market, dict)
    ]
    return enriched


def _enrich_debug_market_dict(
    market_payload: dict[str, Any],
    *,
    now_ts: int,
    selected_keys: set[tuple[str, tuple[str, ...]]],
) -> dict[str, Any]:
    market = PolymarketMarketMetadata.model_validate(market_payload)
    return _debug_market_payload(market, now_ts=now_ts, selected_keys=selected_keys)


def is_short_duration_crypto_market(payload: dict[str, Any]) -> bool:
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "title", "slug", "market_slug", "description", "groupItemTitle")
    ).lower()
    if _rolling_slug_parts(text) is not None:
        return True
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
    question = str(
        payload.get("question")
        or payload.get("title")
        or payload.get("groupItemTitle")
        or market_slug
        or ""
    )
    description = str(payload.get("description") or payload.get("subtitle") or "")
    end_time = _best_timestamp_text(
        payload,
        ("endDate", "end_time", "end", "endDateIso"),
    )
    start_time = _best_timestamp_text(
        payload,
        ("startDate", "start_time", "startDateIso"),
    )
    event_start_time = _best_timestamp_text(
        payload,
        ("eventStartTime", "event_start_time", "eventStartDate"),
    )
    if not event_start_time:
        event_start_time = _iso_from_unix_seconds(_rolling_slug_start_ts(market_slug)) or ""

    if not condition_id or not market_id or not market_slug or not question:
        reject("missing_required_market_metadata")
        return None

    mapping = _map_direction_tokens(
        token_outcomes,
        question=question,
        slug=market_slug,
        description=description,
    )
    if mapping.up_token_id is None or mapping.down_token_id is None:
        reject(mapping.reject_reason or "no_direction_mapping")
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
            payload.get("orderMinSize")
            or payload.get("order_min_size")
            or payload.get("minimum_order_size")
            or payload.get("min_order_size")
            or 0.0
        ),
        rewards_min_size=(
            _float_from_any(payload.get("rewardsMinSize"))
            if payload.get("rewardsMinSize") is not None
            else None
        ),
        start_time=start_time or None,
        event_start_time=event_start_time or None,
        active=_bool_from_any(_first_present(payload, ("active",))),
        closed=_bool_from_any(_first_present(payload, ("closed",))),
        accepting_orders=_bool_from_any(
            _first_present(payload, ("acceptingOrders", "accepting_orders"))
        ),
        enable_order_book=_bool_from_any(
            _first_present(payload, ("enableOrderBook", "enable_order_book"))
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
    description: str = "",
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
        yes_direction = _infer_yes_direction(
            question=question,
            slug=slug,
            description=description,
        )
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
            reject_reason="no_direction_mapping",
        )

    return _DirectionMapping(reject_reason="no_direction_mapping")


def infer_duration_minutes(payload: dict[str, Any]) -> int | None:
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "title", "slug", "market_slug", "description", "groupItemTitle")
    ).lower()
    rolling_parts = _rolling_slug_parts(text)
    if rolling_parts is not None:
        _, horizon, _ = rolling_parts
        return 15 if horizon == "15m" else 5
    if re.search(r"(?<!\d)15[\s-]?(?:m|min|minute)s?(?!\d)", text):
        return 15
    if re.search(r"(?<!\d)5[\s-]?(?:m|min|minute)s?(?!\d)", text):
        return 5

    start = _parse_datetime(_best_timestamp_text(payload, ("startDate", "start_time", "startDateIso")))
    end = _parse_datetime(_best_timestamp_text(payload, ("endDate", "end_time", "endDateIso")))
    if start is None or end is None:
        return None
    minutes = round((end - start).total_seconds() / 60)
    return minutes if minutes in {5, 15} else None


def _normalize_outcome(outcome: str) -> str:
    return outcome.strip().lower().replace("_", " ").replace("-", " ")


def _infer_yes_direction(
    *,
    question: str,
    slug: str,
    description: str = "",
) -> GapDirection | None:
    text = f"{question} {slug} {description}".lower()
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
    text = " ".join(
        str(payload.get(key, ""))
        for key in ("question", "title", "slug", "market_slug", "description")
    ).lower()
    rolling_parts = _rolling_slug_parts(text)
    if rolling_parts is not None:
        asset, _, _ = rolling_parts
        return asset.upper()
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


def _unix_seconds(value: object) -> int | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def _cache_age_ms(discovered_at_ts: int | None) -> float | None:
    if discovered_at_ts is None:
        return None
    now_ns = utc_now_ns()
    if discovered_at_ts > 10_000_000_000_000:
        return max(0.0, (now_ns - discovered_at_ts) / 1_000_000.0)
    if discovered_at_ts > 10_000_000_000:
        return max(0.0, (now_ns / 1_000_000.0) - discovered_at_ts)
    return max(0.0, (now_ns / 1_000_000_000.0 - discovered_at_ts) * 1000.0)


def _iso_from_unix_seconds(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")


def _best_timestamp_text(payload: dict[str, Any], keys: Sequence[str]) -> str:
    values = [
        str(payload[key]).strip()
        for key in keys
        if payload.get(key) is not None and str(payload[key]).strip()
    ]
    for value in values:
        if "T" in value or " " in value:
            return value
    return values[0] if values else ""


def _first_present(payload: dict[str, Any], keys: Sequence[str]) -> object:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _bool_from_any(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    if isinstance(value, int | float):
        return bool(value)
    return None


def _float_from_any(value: object) -> float:
    if isinstance(value, int | float | str | bytes):
        return float(value)
    return 0.0


def _rolling_slug_parts(text: str) -> tuple[str, str, int] | None:
    for match in ROLLING_UPDOWN_SLUG_RE.finditer(text.lower()):
        return match.group(1), match.group(2), int(match.group(3))
    return None


def _rolling_slug_start_ts(slug: str) -> int | None:
    parts = _rolling_slug_parts(slug)
    return None if parts is None else parts[2]


def _market_duration_seconds(market: PolymarketMarketMetadata) -> int | None:
    if market.duration_minutes is None:
        return None
    return market.duration_minutes * 60


def _market_start_sort_key(market: PolymarketMarketMetadata) -> int:
    return (
        _unix_seconds(market.event_start_time)
        or _unix_seconds(market.start_time)
        or _rolling_slug_start_ts(market.market_slug)
        or 0
    )


def _is_current_window(
    market: PolymarketMarketMetadata,
    *,
    now_ts: int,
) -> bool:
    end_ts = _unix_seconds(market.end_time)
    if end_ts is None:
        return False
    return _market_start_sort_key(market) <= now_ts < end_ts


def _is_rolling_window_compatible(
    market: PolymarketMarketMetadata,
    *,
    now_ts: int,
    lookahead_windows: int,
) -> bool:
    if market.base_asset not in {"BTC", "ETH"}:
        return False
    duration_seconds = _market_duration_seconds(market)
    if duration_seconds not in {300, 900}:
        return False
    end_ts = _unix_seconds(market.end_time)
    if end_ts is None or end_ts <= now_ts:
        return False
    start_ts = _market_start_sort_key(market)
    min_start = floor_to_window(now_ts, duration_seconds) - duration_seconds
    max_start = floor_to_window(now_ts, duration_seconds) + max(1, lookahead_windows) * duration_seconds
    return min_start <= start_ts <= max_start


def _extract_market_payloads(
    payload: dict[str, Any],
    *,
    fallback_slug: str | None,
) -> list[dict[str, Any]]:
    nested_markets = payload.get("markets")
    if isinstance(nested_markets, list):
        extracted: list[dict[str, Any]] = []
        for market in nested_markets:
            if isinstance(market, dict):
                extracted.append(_merge_event_market_payload(payload, market, fallback_slug))
        if extracted:
            return extracted
    return [_merge_event_market_payload({}, payload, fallback_slug)]


def _merge_event_market_payload(
    event: dict[str, Any],
    market: dict[str, Any],
    fallback_slug: str | None,
) -> dict[str, Any]:
    merged = dict(market)

    slug = (
        merged.get("slug")
        or merged.get("market_slug")
        or event.get("slug")
        or event.get("market_slug")
        or fallback_slug
    )
    if slug:
        merged["slug"] = slug

    question = (
        merged.get("question")
        or merged.get("title")
        or event.get("question")
        or event.get("title")
        or event.get("groupItemTitle")
        or slug
    )
    if question:
        merged["question"] = question

    for key in (
        "endDate",
        "endDateIso",
        "end_time",
        "startDate",
        "startDateIso",
        "start_time",
        "eventStartTime",
        "event_start_time",
        "active",
        "closed",
        "acceptingOrders",
        "accepting_orders",
        "enableOrderBook",
        "enable_order_book",
        "orderMinSize",
        "order_min_size",
        "minimum_order_size",
        "min_order_size",
        "rewardsMinSize",
        "clobTokenIds",
        "clob_token_ids",
        "outcomes",
        "groupItemTitle",
        "subtitle",
        "description",
        "conditionId",
        "condition_id",
    ):
        if key not in merged and event.get(key) is not None:
            merged[key] = event[key]

    return merged
