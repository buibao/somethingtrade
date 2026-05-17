# Phase 4 Runtime Correctness Notes

Phase 4 datasets are only valid when the monitor is measuring the current rolling Polymarket markets that correspond to live Binance moves. A silent no-signal runtime is a P0 correctness failure because the process can stay healthy, Binance events can keep increasing, and the JSONL output can still stop covering the active runtime after the first rolling market window expires.

## Rolling Market Refresh

`gap-monitor` periodically rediscoveries BTC/ETH 5m/15m rolling markets with `--market-refresh-interval-ms` (default `60000`). It tracks current signal markets, next warmup markets, and additional future markets controlled by `--market-refresh-lookahead-windows` (default `3`).

If Binance moves continue while no signal-enabled markets are active, `--market-refresh-force-when-no-signal` forces an early rediscovery. Lifecycle diffs include added, removed, expired, closed, current, warmup, future, new token, and removed token sets.

## WebSocket Subscription Lifecycle

When the runtime market universe changes, `PolymarketWSClient.update_markets()` rebuilds token metadata, preserves still-active local books, initializes newly added token books, and reconnects the active market websocket so the next subscription payload contains the full current runtime token universe. Runtime summaries expose `runtime_token_count`, `active_ws_token_subscription_count`, missing/extra subscription token samples, transition state, update count, and reconnect count.

Outside an explicitly logged transition, `subscription_token_set_matches_runtime_universe` should be true. Persistent divergence emits `websocket_subscription_out_of_sync`, and token-count divergence emits `market_subscriptions_stale`.

## Runtime Summary JSONL

`--runtime-summary-jsonl <path>` writes one UTF-8 JSON object per runtime summary interval. This does not replace `gap_events`; it is diagnostic coverage metadata for explaining collection health, market lifecycle, websocket subscriptions, book readiness, rejects, and no-event periods.

## No-Event Warnings

If gap events stop for ten minutes while Binance moves continue, runtime summaries emit counter-based warnings such as no signal markets, books not ready, rejected or suppressed candidates, stale subscriptions, market refresh failures, websocket subscription mismatch, or stuck pending observations.

These warnings are diagnostic only. They do not add prediction, training, trading signals, or live execution.

## Quote Age And Stale Quotes

Gap observations populate quote-age and low-latency fields for success, timeout, window reject, pre-entry reject, and lifecycle-close rows when monotonic timestamps are available. Missing instrumentation is represented as null, not `0`, and wall-clock timestamps are not mixed into monotonic quote-age calculations.

Stale Polymarket quotes are visible to the detector as stale diagnostic data. They are not used as clean tradable data, and affected pending observations close with `quote_stale` rather than falling through to a generic timeout when stale feed state is the cause.

## Tick Size And Lifecycle Semantics

`tick_size_change` updates tick-size metadata by default. It does not invalidate the market or close pending observations unless `GapDetector(close_pending_on_tick_size_change=True)` is explicitly used because measurement assumptions were invalidated.

`market_resolved`, `closed`, and `expired` lifecycle events invalidate the market and close pending observations with lifecycle reject reasons. `new_market` requests a market-universe refresh so rolling markets can be discovered without waiting for the next periodic interval.

## Runtime Coverage TODO

Future dataset-quality reporting should ingest `runtime_summary_jsonl` directly and report process duration, gap-event time coverage, coverage/runtime ratio, longest no-event interval, and no-event warnings. Until then, runtime JSONL remains the audit artifact for explaining long-running collection health.
