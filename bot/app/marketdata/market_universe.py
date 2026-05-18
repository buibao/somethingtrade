from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from app.core.clock import utc_now_ns
from app.marketdata.polymarket_discovery import (
    DEFAULT_MARKET_CACHE_TTL_MS,
    PolymarketDiscoveryClient,
    PolymarketMarketMetadata,
    annotate_runtime_market_roles,
    classify_market_window,
    flatten_token_ids,
    is_runtime_tradable_market,
    write_discovery_attempt_jsonl,
)


@dataclass(frozen=True, slots=True)
class MarketUniverseDiff:
    added_markets: tuple[dict[str, Any], ...] = ()
    removed_markets: tuple[dict[str, Any], ...] = ()
    expired_markets: tuple[dict[str, Any], ...] = ()
    closed_markets: tuple[dict[str, Any], ...] = ()
    current_signal_markets: tuple[dict[str, Any], ...] = ()
    next_warmup_markets: tuple[dict[str, Any], ...] = ()
    future_tracked_markets: tuple[dict[str, Any], ...] = ()
    new_token_ids: tuple[str, ...] = ()
    removed_token_ids: tuple[str, ...] = ()
    forced: bool = False
    error: str | None = None

    @property
    def changed(self) -> bool:
        return bool(
            self.added_markets
            or self.removed_markets
            or self.expired_markets
            or self.closed_markets
            or self.new_token_ids
            or self.removed_token_ids
        )


@dataclass(frozen=True, slots=True)
class MarketUniverseSnapshot:
    markets: tuple[PolymarketMarketMetadata, ...]
    current_signal_markets: tuple[PolymarketMarketMetadata, ...]
    next_warmup_markets: tuple[PolymarketMarketMetadata, ...]
    future_tracked_markets: tuple[PolymarketMarketMetadata, ...]
    expired_selected_markets: tuple[PolymarketMarketMetadata, ...]
    closed_removed_markets: tuple[PolymarketMarketMetadata, ...]
    token_ids: tuple[str, ...]
    last_market_discovery_ts: int | None = None
    next_market_discovery_ts: int | None = None
    market_refresh_count: int = 0
    forced_market_refresh_count: int = 0
    market_refresh_error_count: int = 0
    discovery_failure_reason: str | None = None
    last_discovery_attempt_summary: dict[str, Any] = field(default_factory=dict)
    last_successful_discovery_ts: int | None = None
    last_successful_current_signal_slugs: tuple[str, ...] = ()
    last_diff: MarketUniverseDiff = field(default_factory=MarketUniverseDiff)

    @property
    def tracked_market_count(self) -> int:
        return len(self.markets)


class RuntimeMarketUniverseManager:
    """Owns rolling Polymarket runtime discovery and lifecycle diffs."""

    def __init__(
        self,
        discovery: PolymarketDiscoveryClient,
        markets: Sequence[PolymarketMarketMetadata],
        *,
        refresh_interval_ms: int = 60_000,
        lookahead_windows: int = 3,
        market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
        discovery_debug_jsonl: str | None = None,
    ) -> None:
        self.discovery = discovery
        self.refresh_interval_ms = max(1, refresh_interval_ms)
        self.lookahead_windows = max(1, lookahead_windows)
        self.market_cache_ttl_ms = max(1, market_cache_ttl_ms)
        self.discovery_debug_jsonl = discovery_debug_jsonl
        self.market_refresh_count = 0
        self.forced_market_refresh_count = 0
        self.market_refresh_error_count = 0
        self.discovery_failure_reason: str | None = None
        self.last_discovery_attempt_summary: dict[str, Any] = {}
        self.last_successful_discovery_ts: int | None = None
        self.last_successful_current_signal_slugs: tuple[str, ...] = ()
        self._last_discovery_ts: int | None = None
        self._next_discovery_ts: int | None = None
        self._last_diff = MarketUniverseDiff()
        now_ts = utc_now_ns() // 1_000_000_000
        selected = select_runtime_market_universe(
            markets,
            now_ts=now_ts,
            lookahead_windows=self.lookahead_windows,
        )
        self._markets = selected
        if selected:
            self.last_successful_discovery_ts = now_ts
            self.last_successful_current_signal_slugs = _current_signal_slugs(
                selected,
                now_ts=now_ts,
            )
        self._last_diff = build_market_universe_diff((), selected, now_ts=now_ts)

    @property
    def markets(self) -> tuple[PolymarketMarketMetadata, ...]:
        return self._markets

    @property
    def last_market_discovery_ts(self) -> int | None:
        return self._last_discovery_ts

    @property
    def next_market_discovery_ts(self) -> int | None:
        return self._next_discovery_ts

    def snapshot(self, *, now_ts: int | None = None) -> MarketUniverseSnapshot:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        return build_market_universe_snapshot(
            self._markets,
            now_ts=current_ts,
            last_market_discovery_ts=self._last_discovery_ts,
            next_market_discovery_ts=self._next_discovery_ts,
            market_refresh_count=self.market_refresh_count,
            forced_market_refresh_count=self.forced_market_refresh_count,
            market_refresh_error_count=self.market_refresh_error_count,
            discovery_failure_reason=self.discovery_failure_reason,
            last_discovery_attempt_summary=self.last_discovery_attempt_summary,
            last_successful_discovery_ts=self.last_successful_discovery_ts,
            last_successful_current_signal_slugs=self.last_successful_current_signal_slugs,
            last_diff=self._last_diff,
        )

    def refresh_due(self, *, now_ts: int | None = None) -> bool:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        return self._next_discovery_ts is None or current_ts >= self._next_discovery_ts

    async def refresh(
        self,
        *,
        now_ts: int | None = None,
        forced: bool = False,
        refresh_reason: str | None = None,
    ) -> MarketUniverseDiff:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        previous = self._markets
        reason = refresh_reason or ("forced_no_signal" if forced else "scheduled")
        if forced:
            self.forced_market_refresh_count += 1
        try:
            if self.discovery.enable_direct_slug_lookup:
                rolling_result = await self.discovery.discover_rolling_markets_robust(
                    write_cache=True,
                    now_ts=current_ts,
                    lookahead_windows=self.lookahead_windows,
                    market_cache_ttl_ms=self.market_cache_ttl_ms,
                    discovery_debug_jsonl=self.discovery_debug_jsonl,
                    refresh_reason=reason,
                )
                discovered = rolling_result.markets
                attempt = rolling_result.attempt
            else:
                discovered = await self.discovery.discover(
                    write_cache=True,
                    now_ts=current_ts,
                    rolling_lookahead_windows=self.lookahead_windows,
                    market_cache_ttl_ms=self.market_cache_ttl_ms,
                    discovery_debug_jsonl=self.discovery_debug_jsonl,
                    refresh_reason=reason,
                )
                selected_for_attempt = select_runtime_market_universe(
                    discovered,
                    now_ts=current_ts,
                    lookahead_windows=self.lookahead_windows,
                )
                attempt = _legacy_discovery_attempt(
                    discovered=discovered,
                    selected=selected_for_attempt,
                    now_ts=current_ts,
                    refresh_reason=reason,
                )
                if self.discovery_debug_jsonl is not None:
                    write_discovery_attempt_jsonl(self.discovery_debug_jsonl, attempt)
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            self.market_refresh_error_count += 1
            self.discovery_failure_reason = "market_discovery_exception"
            preserved = annotate_runtime_market_roles(
                previous,
                now_ts=current_ts,
                lookahead_windows=self.lookahead_windows,
            )
            self._markets = preserved
            self.last_discovery_attempt_summary = {
                "refresh_reason": reason,
                "failure_reason": self.discovery_failure_reason,
                "error": f"{type(exc).__name__}: {exc}",
            }
            diff = MarketUniverseDiff(
                forced=forced,
                error=(
                    "market_discovery_failed_preserving_previous_universe "
                    f"{type(exc).__name__}: {exc}"
                ),
            )
            self._last_diff = diff
            self._last_discovery_ts = current_ts
            self._schedule_next(current_ts)
            return diff

        self.last_discovery_attempt_summary = _attempt_summary(attempt)
        selected = select_runtime_market_universe(
            discovered,
            now_ts=current_ts,
            lookahead_windows=self.lookahead_windows,
        )
        if not selected:
            self.market_refresh_error_count += 1
            self.discovery_failure_reason = _derive_discovery_failure_reason(attempt)
            preserved = annotate_runtime_market_roles(
                previous,
                now_ts=current_ts,
                lookahead_windows=self.lookahead_windows,
            )
            self._markets = preserved
            diff = build_market_universe_diff(
                previous,
                preserved,
                now_ts=current_ts,
                forced=forced,
                error=(
                    "market_discovery_failed_preserving_previous_universe "
                    f"{self.discovery_failure_reason}"
                ),
            )
            self._last_diff = diff
            self._last_discovery_ts = current_ts
            self._schedule_next(current_ts)
            return diff

        self.market_refresh_count += 1
        self._markets = selected
        self._last_discovery_ts = current_ts
        current_signal_slugs = _current_signal_slugs(selected, now_ts=current_ts)
        if current_signal_slugs:
            self.discovery_failure_reason = None
            self.last_successful_discovery_ts = current_ts
            self.last_successful_current_signal_slugs = current_signal_slugs
        else:
            self.discovery_failure_reason = "no_current_signal_markets_after_full_discovery"
        self._schedule_next(current_ts)
        diff = build_market_universe_diff(previous, selected, now_ts=current_ts, forced=forced)
        self._last_diff = diff
        return diff

    def _schedule_next(self, current_ts: int) -> None:
        interval_s = max(1, self.refresh_interval_ms // 1000)
        self._next_discovery_ts = current_ts + interval_s


def _legacy_discovery_attempt(
    *,
    discovered: Sequence[PolymarketMarketMetadata],
    selected: Sequence[PolymarketMarketMetadata],
    now_ts: int,
    refresh_reason: str,
) -> dict[str, Any]:
    current_signal_slugs = [
        market.market_slug
        for market in selected
        if classify_market_window(market, now_ts=now_ts) == "current_signal"
    ]
    next_warmup_slugs = [
        market.market_slug
        for market in selected
        if classify_market_window(market, now_ts=now_ts) == "next_warmup"
        or market.runtime_selection_reason == "next_warmup"
    ]
    return {
        "event_type": "polymarket_discovery_attempt",
        "refresh_reason": refresh_reason,
        "direct_found_count": 0,
        "direct_runtime_count": 0,
        "active_events_found_count": 0,
        "active_events_runtime_count": 0,
        "cache_runtime_count": 0,
        "runtime_tradable_count": len(selected),
        "current_signal_count": len(current_signal_slugs),
        "next_warmup_count": len(next_warmup_slugs),
        "fallback_used": False,
        "cache_used": False,
        "cache_rejected": False,
        "cache_rejected_reason": None,
        "failure_reason": None if selected else "no_runtime_tradable_markets",
        "selected_market_slugs": [market.market_slug for market in selected],
        "current_signal_slugs": current_signal_slugs,
        "next_warmup_slugs": next_warmup_slugs,
        "diagnostics": [],
        "strategy_results": {
            "legacy_discover": {
                "attempted": True,
                "market_count": len(discovered),
                "runtime_tradable_count": len(selected),
            }
        },
    }


def _attempt_summary(attempt: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "refresh_reason",
        "direct_found_count",
        "direct_runtime_count",
        "active_events_found_count",
        "active_events_runtime_count",
        "active_events_found_runtime_count",
        "cache_runtime_count",
        "fallback_used",
        "cache_used",
        "cache_rejected",
        "cache_rejected_reason",
        "cache_not_updated_reason",
        "failure_reason",
        "selected_market_slugs",
        "current_signal_slugs",
        "next_warmup_slugs",
        "diagnostics",
    )
    summary = {key: attempt.get(key) for key in keys if key in attempt}
    if "active_events_runtime_count" not in summary and "active_events_found_runtime_count" in summary:
        summary["active_events_runtime_count"] = summary["active_events_found_runtime_count"]
    summary["failure_reasons"] = _derive_discovery_failure_reasons(attempt)
    return summary


def _derive_discovery_failure_reason(attempt: dict[str, Any]) -> str:
    reasons = _derive_discovery_failure_reasons(attempt)
    if reasons:
        for reason in (
            "direct_slug_all_closed",
            "active_events_no_runtime_markets",
            "cache_rejected_for_runtime",
            "no_runtime_markets_after_full_discovery",
        ):
            if reason in reasons:
                return reason
        return reasons[0]
    return str(attempt.get("failure_reason") or "no_runtime_markets_after_full_discovery")


def _derive_discovery_failure_reasons(attempt: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    diagnostics = {str(value) for value in attempt.get("diagnostics") or ()}
    if attempt.get("failure_reason") == "direct_slug_found_but_all_closed":
        reasons.append("direct_slug_all_closed")
    if "direct_slug_found_but_all_closed" in diagnostics:
        reasons.append("direct_slug_all_closed")
    if attempt.get("direct_found_count", 0) and attempt.get("direct_runtime_count", 0) == 0:
        if attempt.get("closed_count", 0) >= attempt.get("direct_found_count", 0):
            reasons.append("direct_slug_all_closed")
    if attempt.get("cache_rejected") is True:
        reasons.append("cache_rejected_for_runtime")
    if attempt.get("fallback_used") and attempt.get("active_events_runtime_count", 0) == 0:
        reasons.append("active_events_no_runtime_markets")
    if not attempt.get("runtime_tradable_count", 0):
        reasons.append("no_runtime_markets_after_full_discovery")
    if attempt.get("failure_reason") and not reasons:
        reasons.append(str(attempt["failure_reason"]))
    return sorted(set(reasons))


def _current_signal_slugs(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
) -> tuple[str, ...]:
    return tuple(
        market.market_slug
        for market in markets
        if is_runtime_tradable_market(market, now_ts=now_ts)
        and classify_market_window(market, now_ts=now_ts) == "current_signal"
    )


def select_runtime_market_universe(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    lookahead_windows: int = 3,
) -> tuple[PolymarketMarketMetadata, ...]:
    """Select current plus rolling next/future markets per asset and duration."""

    grouped: dict[tuple[str | None, int | None], list[PolymarketMarketMetadata]] = defaultdict(list)
    group_order: list[tuple[str | None, int | None]] = []
    for market in markets:
        key = (market.base_asset, market.duration_minutes)
        if key not in grouped:
            group_order.append(key)
        grouped[key].append(market)

    selected: list[PolymarketMarketMetadata] = []
    for key in group_order:
        group = grouped[key]
        current = [
            market
            for market in group
            if is_runtime_tradable_market(market, now_ts=now_ts)
            and classify_market_window(market, now_ts=now_ts) == "current_signal"
        ]
        current.sort(key=_market_sort_key)
        selected.extend(_annotate_universe_role(current, "current"))

        future_candidates = [
            market
            for market in group
            if is_runtime_tradable_market(market, now_ts=now_ts)
            and classify_market_window(market, now_ts=now_ts) in {"next_warmup", "future_tracked"}
        ]
        future_candidates.sort(key=_market_sort_key)
        for index, market in enumerate(future_candidates[:lookahead_windows]):
            role = "next" if index == 0 else "future"
            selected.extend(_annotate_universe_role((market,), role))

    return tuple(selected)


def build_market_universe_snapshot(
    markets: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    last_market_discovery_ts: int | None = None,
    next_market_discovery_ts: int | None = None,
    market_refresh_count: int = 0,
    forced_market_refresh_count: int = 0,
    market_refresh_error_count: int = 0,
    discovery_failure_reason: str | None = None,
    last_discovery_attempt_summary: dict[str, Any] | None = None,
    last_successful_discovery_ts: int | None = None,
    last_successful_current_signal_slugs: Sequence[str] = (),
    last_diff: MarketUniverseDiff | None = None,
) -> MarketUniverseSnapshot:
    current: list[PolymarketMarketMetadata] = []
    next_warmup: list[PolymarketMarketMetadata] = []
    future: list[PolymarketMarketMetadata] = []
    expired: list[PolymarketMarketMetadata] = []
    closed: list[PolymarketMarketMetadata] = []

    annotated = annotate_runtime_market_roles(markets, now_ts=now_ts)
    selected_by_key = {_market_key(market): market for market in markets}
    for annotated_market in annotated:
        market = selected_by_key.get(_market_key(annotated_market), annotated_market)
        classification = classify_market_window(market, now_ts=now_ts)
        reason = market.runtime_selection_reason
        if classification == "closed":
            closed.append(market)
        elif classification == "expired":
            expired.append(market)
        elif classification == "current_signal":
            current.append(
                market.model_copy(
                    update={
                        "classification": "current_signal",
                        "selected_for_runtime": True,
                        "signal_enabled": True,
                        "runtime_selection_reason": "current_signal",
                    }
                )
            )
        elif reason == "future_tracked" or classification == "future_tracked":
            future.append(market)
        elif classification == "next_warmup" or reason == "next_warmup":
            next_warmup.append(market)

    return MarketUniverseSnapshot(
        markets=tuple(markets),
        current_signal_markets=tuple(current),
        next_warmup_markets=tuple(next_warmup),
        future_tracked_markets=tuple(future),
        expired_selected_markets=tuple(expired),
        closed_removed_markets=tuple(closed),
        token_ids=flatten_token_ids(markets),
        last_market_discovery_ts=last_market_discovery_ts,
        next_market_discovery_ts=next_market_discovery_ts,
        market_refresh_count=market_refresh_count,
        forced_market_refresh_count=forced_market_refresh_count,
        market_refresh_error_count=market_refresh_error_count,
        discovery_failure_reason=discovery_failure_reason,
        last_discovery_attempt_summary=dict(last_discovery_attempt_summary or {}),
        last_successful_discovery_ts=last_successful_discovery_ts,
        last_successful_current_signal_slugs=tuple(last_successful_current_signal_slugs),
        last_diff=last_diff or MarketUniverseDiff(),
    )


def build_market_universe_diff(
    previous: Sequence[PolymarketMarketMetadata],
    current: Sequence[PolymarketMarketMetadata],
    *,
    now_ts: int,
    forced: bool = False,
    error: str | None = None,
) -> MarketUniverseDiff:
    previous_by_key = {_market_key(market): market for market in previous}
    current_by_key = {_market_key(market): market for market in current}
    previous_keys = set(previous_by_key)
    current_keys = set(current_by_key)
    added = tuple(_market_payload(current_by_key[key], now_ts=now_ts) for key in sorted(current_keys - previous_keys))
    removed_markets = [previous_by_key[key] for key in sorted(previous_keys - current_keys)]
    removed = tuple(_market_payload(market, now_ts=now_ts) for market in removed_markets)
    expired = tuple(
        _market_payload(market, now_ts=now_ts)
        for market in removed_markets
        if classify_market_window(market, now_ts=now_ts) == "expired"
    )
    closed = tuple(
        _market_payload(market, now_ts=now_ts)
        for market in removed_markets
        if classify_market_window(market, now_ts=now_ts) == "closed"
    )

    previous_tokens = set(flatten_token_ids(previous))
    current_tokens = set(flatten_token_ids(current))
    snapshot = build_market_universe_snapshot(current, now_ts=now_ts)
    return MarketUniverseDiff(
        added_markets=added,
        removed_markets=removed,
        expired_markets=expired,
        closed_markets=closed,
        current_signal_markets=tuple(
            _market_payload(market, now_ts=now_ts) for market in snapshot.current_signal_markets
        ),
        next_warmup_markets=tuple(
            _market_payload(market, now_ts=now_ts) for market in snapshot.next_warmup_markets
        ),
        future_tracked_markets=tuple(
            _market_payload(market, now_ts=now_ts) for market in snapshot.future_tracked_markets
        ),
        new_token_ids=tuple(sorted(current_tokens - previous_tokens)),
        removed_token_ids=tuple(sorted(previous_tokens - current_tokens)),
        forced=forced,
        error=error,
    )


def _cached_runtime_markets(
    discovery: PolymarketDiscoveryClient,
    *,
    now_ts: int,
    lookahead_windows: int,
) -> tuple[PolymarketMarketMetadata, ...]:
    cache_validation = discovery.validate_cache_for_runtime(
        now_ts=now_ts,
        lookahead_windows=lookahead_windows,
    )
    if not cache_validation.valid:
        return ()
    return cache_validation.runtime_markets


def _annotate_universe_role(
    markets: Iterable[PolymarketMarketMetadata],
    role: str,
) -> tuple[PolymarketMarketMetadata, ...]:
    updates_by_role = {
        "current": {
            "classification": "current_signal",
            "selected_for_runtime": True,
            "signal_enabled": True,
            "runtime_selection_reason": "current_signal",
        },
        "next": {
            "classification": "next_warmup",
            "selected_for_runtime": True,
            "signal_enabled": False,
            "runtime_selection_reason": "next_warmup",
        },
        "future": {
            "classification": "future_tracked",
            "selected_for_runtime": True,
            "signal_enabled": False,
            "runtime_selection_reason": "future_tracked",
        },
    }
    update = updates_by_role[role]
    return tuple(market.model_copy(update=update) for market in markets)


def _market_payload(market: PolymarketMarketMetadata, *, now_ts: int) -> dict[str, Any]:
    return {
        "market_id": market.market_id,
        "market_slug": market.market_slug,
        "base_asset": market.base_asset,
        "duration_minutes": market.duration_minutes,
        "classification": classify_market_window(market, now_ts=now_ts),
        "runtime_selection_reason": market.runtime_selection_reason,
    }


def _market_key(market: PolymarketMarketMetadata) -> tuple[str, tuple[str, ...]]:
    return market.market_id, market.token_ids


def _market_sort_key(market: PolymarketMarketMetadata) -> tuple[str, str]:
    return market.event_start_time or market.start_time or market.market_slug, market.market_id
