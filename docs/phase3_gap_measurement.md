# Phase 3.9 Gap Measurement

Phase 3.9 measures whether Binance price discovery appears before Polymarket reprices the matching short-duration BTC/ETH CLOB market. It is a measurement system, not a live trading signal.

## Three Different Ideas

Repricing delay:
There are two delays:

- `mid_repricing_delay_ms`: time from Binance move detection until Polymarket mid probability moves in the expected direction.
- `executable_repricing_delay_ms`: time from Binance move detection until the selected token has an executable exit bid above `entry_ask + GAP_MIN_EXIT_EDGE`.

Tradable window:
The period where the stale Polymarket quote remains enterable at the ask. For a hypothetical BUY, the detector requires a complete local book, a fresh quote, a non-invalid market, a best ask, enough `best_ask_size`, acceptable spread, and an ask no higher than `entry_ask + GAP_MAX_ENTRY_PRICE_MOVE`. A low current bid does not end the tradable window.

Estimated edge:
Only counted when executable repricing occurs. For a hypothetical BUY, `exit_edge_after_spread = executable_exit_bid - entry_ask`. Mid repricing without a profitable bid is not executable repricing.

Fillability:
For a BUY measurement, the visible entry is `best_ask`. The quote is not fillable if the ask is missing, the ask size is missing, the ask size is below `min_order_size`, the spread is too wide, the quote/book is stale, or the market lifecycle invalidates the book.

## Outcome Mapping

The detector never assumes the first CLOB token is UP or that YES always means UP. Gamma metadata is parsed into explicit fields:

- `up_token_id`
- `down_token_id`
- `yes_token_id`
- `no_token_id`
- `token_outcomes`

Supported direct outcome pairs include:

- `Up` / `Down`
- `Above` / `Below`
- `Higher` / `Lower`

For `Yes` / `No`, the market question or slug must make the direction unambiguous. Ambiguous markets are rejected during discovery and the reject reason is logged.

## What Gets Measured

The detector watches Binance microstructure state:

- `return_1s`
- `return_5s`
- `return_15s`
- `return_30s`
- `volatility_30s`
- `bid_ask_spread`

Strict candidate thresholds:

- `GAP_BINANCE_STALE_MS`, default `500`
- `GAP_POLYMARKET_STALE_MS`, default `1000`

Wider monitoring threshold:

- `GAP_MEASUREMENT_STALE_MS`, default `5000`

Entry-window controls:

- `GAP_MAX_ENTRY_SPREAD`, default `0.05`
- `GAP_MAX_ENTRY_PRICE_MOVE`, default `0.02`
- `GAP_MIN_EXIT_EDGE`, default `0.0`
- `GAP_MAX_PENDING_MS`, default `5000`

The tradable window ends when a quote update makes the selected token non-enterable: ask moves beyond the configured entry tolerance, ask size disappears, spread widens beyond the configured threshold, the quote becomes stale, the book becomes incomplete, the tick size changes, or the market resolves. It does not end merely because the current bid is below the entry ask.

## Local Order Book

Polymarket CLOB websocket messages are applied to an in-memory `PolymarketLocalOrderBook` per token:

- `book` replaces the full bid/ask ladder for the token.
- `price_change` mutates the relevant side: `BUY` updates bids, `SELL` updates asks, and size `0` removes the level.
- `best_bid_ask` carries no size, so it is never allowed to erase known size. If it disagrees with the local book, the emitted quote is marked incomplete with a validation error and the affected size is unknown.

Phase 3.15 splits quote completeness into diagnostic fields:

- `book_has_snapshot`: a full book snapshot has been received for the token.
- `book_structurally_complete`: the local ladder has usable bid/ask prices and sizes.
- `reported_best_validation_ok`: reported best prices agree with local book validation rules.
- `book_complete`: conservative compatibility flag used by the detector.

Reported best validation can run in `strict`, `tolerant`, or `diagnostic` mode. Phase 3.16 uses `tolerant` with one tick as the research default after live diagnostics showed most reported-best mismatches were one-tick sequencing differences. Strict remains the audit/safety mode. Diagnostic remains debug-only and should not be used as clean ground truth. See `docs/phase3_orderbook_diagnostics.md` for the live-run comparison workflow and mismatch sample format.

`PolymarketQuote.best_bid_size` and `best_ask_size` come from the local book. The older `available_liquidity_at_best` field is only a backward-compatible computed summary and should not drive execution simulation.

## Why Price Change Alone Is Not Enough

A standalone `price_change` row can show a price level delta and sometimes reports best bid/ask fields, but those reported bests do not prove executable size at the top of book. Without a prior snapshot and local deltas, a detector can mistake a price update for fillable liquidity. Phase 3.9 treats the local book as source of truth and marks quotes incomplete when size cannot be established.

## Lifecycle Events

The Polymarket websocket can emit lifecycle events:

- `tick_size_change`
- `market_resolved`
- `new_market`

Tick-size changes and resolved markets invalidate related local books and close pending gap measurements for that market with a reject reason. New-market events are surfaced so discovery can be rerun; they are not silently ignored.

## Reject Taxonomy

Each completed observation carries:

- `pre_entry_reject_reason`
- `window_end_reason`
- `exit_reject_reason`
- `reject_stage`: `pre_entry`, `window`, `exit`, `lifecycle`, `timeout`, or `none`
- `reject_reason`: final summary reason for dashboards and quick scans

Pending gaps are closed by executable repricing, window failure, lifecycle invalidation, or `GAP_MAX_PENDING_MS`. Lifecycle invalidation and timeout always produce completed observations; pending gaps are not silently deleted. If a timeout happens before mid repricing, `exit_reject_reason` is `no_mid_repricing_before_timeout`. If mid repricing happened but no profitable executable bid appeared, `exit_reject_reason` is `no_executable_repricing_before_timeout`.

Timeout precedence:
When a Polymarket quote arrives after `GAP_MAX_PENDING_MS`, timeout is recorded before structural, entry-window, or executable checks on that same quote. This keeps old observations from being reclassified by a late update. In that case `reject_stage` is `timeout`, `reject_reason` is `max_observation_lifetime_reached`, and `window_end_reason` remains unset unless the implementation later chooses to store secondary context.

## Observation Fields

Completed observations are `TradableGapObservation` JSON events with:

- Binance move data: `symbol`, `direction`, `binance_move_pct`, `binance_event_ts_ns`
- Polymarket quote data: before/after bid, ask, sizes, mids, and spreads
- Timing data: `detected_ts_ns`, `poly_quote_ts_ns`, `mid_repricing_delay_ms`, `executable_repricing_delay_ms`, `tradable_window_ms`
- Repricing timestamps: `first_mid_repriced_ts_ns`, `first_executable_repriced_ts_ns`
- Hypothetical prices: `hypothetical_entry_price`, `hypothetical_exit_price`
- Entry/exit prices: `entry_ask`, `entry_ask_size`, `executable_exit_bid`
- Fillability data: `quote_was_fillable`, `reject_reason`
- Edge data: `estimated_edge_raw`, `estimated_edge_after_spread`, `exit_edge_after_spread`
- Tick-normalized context: `tick_size_at_detection`, `spread_ticks_at_detection`, `entry_ask_ticks`, `exit_edge_ticks`, `estimated_edge_ticks`, and `effective_reprice_threshold_ticks`
- Data-quality context: `validation_mode`, `validation_tolerance_ticks`, quote/mismatch rates, `data_quality_tier`, and `data_quality_reason`

Internal processing latency uses monotonic timestamps:

- `recv_monotonic_ns`
- `parse_done_monotonic_ns`
- `state_updated_monotonic_ns`

Exchange timestamps remain separate wall-clock data and are not used for internal processing latency.

## Runtime Pipeline Invariant

Every raw or normalized event should be applied to `MarketState` before being passed to `GapDetector`.

Correct runtime order:

1. `normalized = state.apply(event)`
2. `detector.on_market_event(normalized, state, now_ts=...)`

The detector reads latest Binance state, Polymarket quote state, invalid markets, and lifecycle state from `MarketState`. If lifecycle events reach the detector before `state.apply()` runs, pending gaps may still close because the lifecycle event is self-contained, but `MarketState` invalidation can be stale. Runtime code must enforce this ordering.

## Monitor

```bash
python -m app.main gap-monitor
```

The monitor prints:

- number of detected gaps
- number of completed observations
- fillable and non-fillable detection counts
- median and p95 mid repricing delay
- median and p95 executable repricing delay
- median tradable window
- p95 tradable window
- reject counts by reason
- reject counts by stage
- stale feed count

Completed observations are written asynchronously to:

```text
data/logs/gap_events_YYYYMMDD.jsonl
```

These JSONL files are append-only measurement artifacts. They are not database writes and they are not used for realtime order execution.

Runtime JSONL logs are local artifacts and should not be committed wholesale. Commit only curated summaries or small anonymized examples under `docs/examples/`.

## Dataset Quality Report

Before Phase 4 modeling, run:

```bash
python -m app.main dataset-quality-report \
  --input data/logs/<your_gap_events_file>.jsonl \
  --output data/reports/dataset_quality_latest.json
```

The report summarizes rows, outcomes, timing, edge, validation mode, data-quality tiers, stale sources, and warnings. Warnings flag small samples, zero successes, excessive D-tier rows, high `book_incomplete`, high `quote_stale`, diagnostic-mode dominated data, and missing tick sizes. Use this report to decide whether a live run is suitable for Phase 4 analysis; it does not train a model.

## Why This Is Not A Live Trading Signal

Even a positive observation is not enough to trade. A real execution system would still need deeper book simulation, queue position, cancellation risk, websocket gap recovery, Polymarket order placement latency, fees, market-specific resolution risk, wallet and allowance safety, inventory limits, and kill switches.

This phase answers a narrower research question: did Binance move first, did Polymarket lag, and was the visible stale quote plausibly fillable after spread?
