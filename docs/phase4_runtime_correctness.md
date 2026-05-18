# Phase 4 Runtime Correctness Notes

Phase 4 datasets are only valid when the monitor is measuring the current rolling Polymarket markets that correspond to live Binance moves. A silent no-signal runtime is a P0 correctness failure because the process can stay healthy, Binance events can keep increasing, and the JSONL output can still stop covering the active runtime after the first rolling market window expires.

## Rolling Market Refresh

`gap-monitor` periodically rediscoveries BTC/ETH 5m/15m rolling markets with `--market-refresh-interval-ms` (default `60000`). It tracks current signal markets, next warmup markets, and additional future markets controlled by `--market-refresh-lookahead-windows` (default `3`).

If Binance moves continue while no signal-enabled markets are active, `--market-refresh-force-when-no-signal` forces an early rediscovery. Lifecycle diffs include added, removed, expired, closed, current, warmup, future, new token, and removed token sets.

## Rolling Discovery Fallbacks

Rolling Polymarket discovery now uses three runtime-safe sources. First, it generates expected `btc/eth-updown-5m/15m-<window>` slugs and queries Gamma slug endpoints. Direct slug results are kept for diagnostics even when they are historical or closed, but they are not enough for startup unless at least one market is runtime-tradable.

Direct slug lookup can return closed historical markets for the correct current-looking slug. That is expected Gamma behavior, so direct slug success is not treated as runtime success unless the market is current or warmup and runtime-tradable.

If direct slugs produce no runtime-tradable market, no current signal market, all closed markets, all `acceptingOrders=false` markets, or the cache is rejected, discovery must fall back to Gamma active events using `active=true`, `closed=false`, limit, and offset pagination. Nested event markets are parsed and accepted only when they are BTC/ETH 5m/15m up/down markets with `active=true`, `closed=false`, `acceptingOrders=true`, `enableOrderBook=true`, UP/DOWN token IDs, and a current or near-future rolling window.

The market cache is only a runtime fallback after validation. `--market-cache-ttl-ms` defaults to `60000`; a cache is rejected when it is expired, all closed, missing required fields, has no runtime markets, or has no current/warmup market. Rejections are logged as `cache_rejected_for_runtime` with reasons such as `cache_all_closed`, `cache_no_runtime_markets`, `cache_expired`, and `cache_missing_required_fields`.

Discovery only updates the runtime cache when runtime markets exist, at least one market is current or warmup, not all markets are closed, and not all markets have `acceptingOrders=false`. An all-closed direct slug result never overwrites a previous valid cache.

## Startup Waiting

`gap-monitor` defaults to `--wait-for-markets`. When no runtime BTC/ETH 5m/15m markets are found at startup, it prints a discovery summary, writes a JSONL attempt artifact, sleeps for `--market-discovery-retry-ms` (default `30000`), and retries until markets appear or `--market-discovery-startup-timeout-ms` (default `300000`) expires. Timeout exits with `no_active_markets_after_startup_timeout`. Use `--no-wait-for-markets` to keep fail-fast startup semantics with the improved diagnostics.

Discovery attempts are written as UTF-8 JSONL to `data/debug/polymarket_discovery_attempts.jsonl` by default, or to `--discovery-debug-jsonl <path>`. This audit artifact is required for production diagnosis. Startup and periodic runtime refresh attempts include `refresh_reason`, direct slug counts, active-events fallback counts, cache validation, selected/current/warmup slugs, cache update decisions, time sanity diagnostics, and failure reasons such as `direct_slug_found_but_all_closed`.

For manual diagnosis, run:

```bash
python -m app.main polymarket-rolling-discovery-debug
```

The command prints direct slug results, active-events fallback results, cache validation, rejection reasons, and final selected runtime markets.

## WebSocket Subscription Lifecycle

When the runtime market universe changes, `PolymarketWSClient.update_markets()` rebuilds token metadata, preserves still-active local books, initializes newly added token books, and reconnects the active market websocket so the next subscription payload contains the full current runtime token universe. Runtime summaries expose `runtime_token_count`, `active_ws_token_subscription_count`, missing/extra subscription token samples, transition state, update count, and reconnect count.

Before the first active websocket subscription, subscription status is reported as pending/unknown rather than falsely matched. Outside an explicitly logged transition, `subscription_token_set_matches_runtime_universe` should be true. Persistent divergence emits `websocket_subscription_out_of_sync`, and token-count divergence emits `market_subscriptions_stale`.

## Runtime Summary JSONL

`--runtime-summary-jsonl <path>` writes one UTF-8 JSON object per runtime summary interval. This does not replace `gap_events`; it is diagnostic coverage metadata for explaining collection health, market lifecycle, websocket subscriptions, book readiness, rejects, and no-event periods.

## No-Event Warnings

If gap events stop for ten minutes while Binance moves continue, runtime summaries emit counter-based warnings such as no signal markets, books not ready, rejected or suppressed candidates, stale subscriptions, market refresh failures, websocket subscription mismatch, or stuck pending observations.

No-signal runtime is a P0 data-collection failure. Runtime refresh preserves the previous universe for diagnostics during transient all-closed discoveries, but expired preserved markets are not reported as current signal markets. Summary JSONL records `discovery_failure_reason`, direct/active-events/cache runtime counts, `last_discovery_attempt_summary`, `last_successful_discovery_ts`, and `last_successful_current_signal_slugs`.

These warnings are diagnostic only. They do not add prediction, training, trading signals, or live execution.

## Quote Age And Stale Quotes

Gap observations populate quote-age and low-latency fields for success, timeout, window reject, pre-entry reject, and lifecycle-close rows when monotonic timestamps are available. Missing instrumentation is represented as null, not `0`, and wall-clock timestamps are not mixed into monotonic quote-age calculations.

Stale Polymarket quotes are visible to the detector as stale diagnostic data. They are not used as clean tradable data, and affected pending observations close with `quote_stale` rather than falling through to a generic timeout when stale feed state is the cause.

## Tick Size And Lifecycle Semantics

`tick_size_change` updates tick-size metadata by default. It does not invalidate the market or close pending observations unless `GapDetector(close_pending_on_tick_size_change=True)` is explicitly used because measurement assumptions were invalidated.

`market_resolved`, `closed`, and `expired` lifecycle events invalidate the market and close pending observations with lifecycle reject reasons. `new_market` requests a market-universe refresh so rolling markets can be discovered without waiting for the next periodic interval.

## Runtime Coverage

`dataset-quality-report --runtime-summary-jsonl <path>` ingests runtime summaries and reports gap-event time coverage versus process runtime. If `gap_events` cover much less time than the runtime summary stream, the report emits `gap_event_coverage_shorter_than_runtime` and keeps readiness conservative. Runtime JSONL remains the audit artifact for explaining long-running collection health.
