# Phase 3.7 Gap Measurement

Phase 3.7 measures whether Binance price discovery appears before Polymarket reprices the matching short-duration BTC/ETH CLOB market. It is a measurement system, not a live trading signal.

## Three Different Ideas

Repricing delay:
The time from a Binance-led move being detected to the first matching Polymarket quote update in the expected direction.

Tradable window:
The subset of that delay where the stale Polymarket quote was actually fillable at the relevant side of the local CLOB. For a hypothetical BUY, the detector requires a complete local book, a best ask, enough `best_ask_size`, acceptable spread, and a non-invalid market. `tradable_window_ms` is allowed to end before `repricing_delay_ms`, and it may be recorded even when repricing never occurs.

Estimated edge:
Only counted for monitor averages when the stale quote was fillable, the spread was below the configured threshold, and the repriced quote leaves positive spread-adjusted value. For a hypothetical BUY, `estimated_edge_after_spread = later_best_bid - detection_best_ask`. A repricing delay without fillability is not treated as executable edge.

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

The tradable window ends when a quote update makes the selected token non-executable: ask moves beyond the configured entry tolerance, ask size disappears, spread widens beyond the configured threshold, spread-adjusted edge decays to zero or below, the quote becomes stale, the book becomes incomplete, the tick size changes, or the market resolves.

## Local Order Book

Polymarket CLOB websocket messages are applied to an in-memory `PolymarketLocalOrderBook` per token:

- `book` replaces the full bid/ask ladder for the token.
- `price_change` mutates the relevant side: `BUY` updates bids, `SELL` updates asks, and size `0` removes the level.
- `best_bid_ask` carries no size, so it is never allowed to erase known size. If it disagrees with the local book, the emitted quote is marked incomplete with a validation error and the affected size is unknown.

`PolymarketQuote.best_bid_size` and `best_ask_size` come from the local book. The older `available_liquidity_at_best` field is only a backward-compatible computed summary and should not drive execution simulation.

## Why Price Change Alone Is Not Enough

A standalone `price_change` row can show a price level delta and sometimes reports best bid/ask fields, but those reported bests do not prove executable size at the top of book. Without a prior snapshot and local deltas, a detector can mistake a price update for fillable liquidity. Phase 3.7 treats the local book as source of truth and marks quotes incomplete when size cannot be established.

## Lifecycle Events

The Polymarket websocket can emit lifecycle events:

- `tick_size_change`
- `market_resolved`
- `new_market`

Tick-size changes and resolved markets invalidate related local books and close pending gap measurements for that market with a reject reason. New-market events are surfaced so discovery can be rerun; they are not silently ignored.

## Observation Fields

Completed observations are `TradableGapObservation` JSON events with:

- Binance move data: `symbol`, `direction`, `binance_move_pct`, `binance_event_ts_ns`
- Polymarket quote data: before/after bid, ask, sizes, mids, and spreads
- Timing data: `detected_ts_ns`, `poly_quote_ts_ns`, `repricing_delay_ms`, `tradable_window_ms`
- Hypothetical prices: `hypothetical_entry_price`, `hypothetical_exit_price`
- Fillability data: `quote_was_fillable`, `reject_reason`
- Edge data: `estimated_edge_raw`, `estimated_edge_after_spread`

Internal processing latency uses monotonic timestamps:

- `recv_monotonic_ns`
- `parse_done_monotonic_ns`
- `state_updated_monotonic_ns`

Exchange timestamps remain separate wall-clock data and are not used for internal processing latency.

## Monitor

```bash
python -m app.main gap-monitor
```

The monitor prints:

- number of detected gaps
- number of completed observations
- median repricing delay
- p95 repricing delay
- median tradable window
- p95 tradable window
- average executable estimated edge after spread
- reject counts by reason
- stale feed count

Completed observations are written asynchronously to:

```text
data/logs/gap_events_YYYYMMDD.jsonl
```

These JSONL files are append-only measurement artifacts. They are not database writes and they are not used for realtime order execution.

## Why This Is Not A Live Trading Signal

Even a positive observation is not enough to trade. A real execution system would still need deeper book simulation, queue position, cancellation risk, websocket gap recovery, Polymarket order placement latency, fees, market-specific resolution risk, wallet and allowance safety, inventory limits, and kill switches.

This phase answers a narrower research question: did Binance move first, did Polymarket lag, and was the visible stale quote plausibly fillable after spread?
