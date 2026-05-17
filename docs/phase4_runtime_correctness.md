# Phase 4.0b Runtime Correctness Notes

Phase 4 datasets are only valid when the monitor is measuring the current rolling Polymarket markets that correspond to live Binance moves. A silent no-signal runtime is a P0 correctness failure because the process can stay healthy, Binance events can keep increasing, and the JSONL output can still stop covering the active runtime after the first rolling market window expires.

## Rolling Market Refresh

`gap-monitor` periodically rediscoveries BTC/ETH 5m/15m rolling markets with `--market-refresh-interval-ms` (default `60000`). It tracks current signal markets, next warmup markets, and additional future markets controlled by `--market-refresh-lookahead-windows` (default `3`).

If Binance moves continue while no signal-enabled markets are active, `--market-refresh-force-when-no-signal` forces an early rediscovery. Lifecycle diffs include added, removed, expired, closed, current, warmup, future, new token, and removed token sets.

## Runtime Summary JSONL

`--runtime-summary-jsonl <path>` writes one UTF-8 JSON object per runtime summary interval. This does not replace `gap_events`; it is diagnostic coverage metadata for explaining collection health, market lifecycle, websocket subscriptions, book readiness, rejects, and no-event periods.

## No-Event Warnings

If gap events stop for ten minutes while Binance moves continue, runtime summaries emit counter-based warnings such as no signal markets, books not ready, rejected or suppressed candidates, stale subscriptions, market refresh failures, websocket subscription mismatch, or stuck pending observations.

These warnings are diagnostic only. They do not add prediction, training, trading signals, or live execution.
