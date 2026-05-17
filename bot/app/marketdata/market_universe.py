from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from app.core.clock import utc_now_ns
from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    PolymarketMarketMetadata,
    annotate_runtime_market_roles,
    classify_market_window,
    flatten_token_ids,
    is_runtime_tradable_market,
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
    ) -> None:
        self.discovery = discovery
        self.refresh_interval_ms = max(1, refresh_interval_ms)
        self.lookahead_windows = max(1, lookahead_windows)
        self.market_refresh_count = 0
        self.forced_market_refresh_count = 0
        self.market_refresh_error_count = 0
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
            last_diff=self._last_diff,
        )

    def refresh_due(self, *, now_ts: int | None = None) -> bool:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        return self._next_discovery_ts is None or current_ts >= self._next_discovery_ts

    async def refresh(self, *, now_ts: int | None = None, forced: bool = False) -> MarketUniverseDiff:
        current_ts = now_ts or utc_now_ns() // 1_000_000_000
        previous = self._markets
        try:
            discovered = await self.discovery.discover(
                write_cache=True,
                now_ts=current_ts,
                rolling_lookahead_windows=self.lookahead_windows,
            )
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            self.market_refresh_error_count += 1
            diff = MarketUniverseDiff(forced=forced, error=f"{type(exc).__name__}: {exc}")
            self._last_diff = diff
            self._schedule_next(current_ts)
            return diff

        selected = select_runtime_market_universe(
            discovered,
            now_ts=current_ts,
            lookahead_windows=self.lookahead_windows,
        )
        if not selected:
            selected = _cached_runtime_markets(
                self.discovery,
                now_ts=current_ts,
                lookahead_windows=self.lookahead_windows,
            )

        self.market_refresh_count += 1
        if forced:
            self.forced_market_refresh_count += 1
        self._markets = selected
        self._last_discovery_ts = current_ts
        self._schedule_next(current_ts)
        diff = build_market_universe_diff(previous, selected, now_ts=current_ts, forced=forced)
        self._last_diff = diff
        return diff

    def _schedule_next(self, current_ts: int) -> None:
        interval_s = max(1, self.refresh_interval_ms // 1000)
        self._next_discovery_ts = current_ts + interval_s


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
            and classify_market_window(market, now_ts=now_ts) == "current"
        ]
        current.sort(key=_market_sort_key)
        selected.extend(_annotate_universe_role(current, "current"))

        future_candidates = [
            market
            for market in group
            if is_runtime_tradable_market(market, now_ts=now_ts)
            and classify_market_window(market, now_ts=now_ts) in {"next", "future"}
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
        elif classification == "current":
            current.append(
                market.model_copy(
                    update={
                        "classification": "current",
                        "selected_for_runtime": True,
                        "signal_enabled": True,
                        "runtime_selection_reason": "current_signal",
                    }
                )
            )
        elif reason == "future_tracked" or classification == "future":
            future.append(market)
        elif classification == "next" or reason == "next_warmup":
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
    try:
        cached = discovery.read_cache()
    except (OSError, ValueError):
        return ()
    return select_runtime_market_universe(
        cached.markets,
        now_ts=now_ts,
        lookahead_windows=lookahead_windows,
    )


def _annotate_universe_role(
    markets: Iterable[PolymarketMarketMetadata],
    role: str,
) -> tuple[PolymarketMarketMetadata, ...]:
    updates_by_role = {
        "current": {
            "classification": "current",
            "selected_for_runtime": True,
            "signal_enabled": True,
            "runtime_selection_reason": "current_signal",
        },
        "next": {
            "classification": "next",
            "selected_for_runtime": True,
            "signal_enabled": False,
            "runtime_selection_reason": "next_warmup",
        },
        "future": {
            "classification": "future",
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
