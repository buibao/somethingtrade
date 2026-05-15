# Phase 3.5 Gap Measurement

Phase 3.5 measures whether Binance price discovery appears before Polymarket reprices the matching short-duration BTC/ETH CLOB market. It is a measurement system, not a live trading signal.

## Three Different Ideas

Repricing delay:
The time from a Binance-led move being detected to the first matching Polymarket quote update in the expected direction.

Tradable window:
The subset of that delay where the stale Polymarket quote was actually fillable at the relevant side of the book. For a hypothetical BUY, the detector requires a best ask and enough `best_ask_size`.

Estimated edge:
Only counted for monitor averages when the stale quote was fillable, the spread was below the configured threshold, and the repriced quote leaves positive spread-adjusted value. A repricing delay without fillability is not treated as executable edge.

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

## Observation Fields

Completed observations are `TradableGapObservation` JSON events with:

- Binance move data: `symbol`, `direction`, `binance_move_pct`, `binance_event_ts_ns`
- Polymarket quote data: before/after bid, ask, sizes, mids, and spreads
- Timing data: `detected_ts_ns`, `poly_quote_ts_ns`, `gap_duration_ms`, `tradable_window_ms`
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
- median gap duration
- p95 gap duration
- average executable estimated edge after spread
- stale feed count

Completed observations are written asynchronously to:

```text
data/logs/gap_events_YYYYMMDD.jsonl
```

These JSONL files are append-only measurement artifacts. They are not database writes and they are not used for realtime order execution.

## Why This Is Not A Live Trading Signal

Even a positive observation is not enough to trade. A real execution system would still need order-book depth beyond top-of-book, queue position, cancellation risk, matching latency, fee modeling, market-specific resolution risk, wallet and allowance safety, inventory limits, and kill switches.

This phase answers a narrower research question: did Binance move first, did Polymarket lag, and was the visible stale quote plausibly fillable after spread?
