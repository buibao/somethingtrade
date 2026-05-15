# Phase 3 Gap Measurement

Phase 3 measures whether Binance price discovery appears before Polymarket reprices the matching short-duration BTC/ETH CLOB market.

## What Gets Measured

The detector watches Binance microstructure state:

- `return_1s`
- `return_5s`
- `return_15s`
- `return_30s`
- `volatility_30s`
- `bid_ask_spread`

When the strongest Binance return exceeds `GAP_MIN_MOVE_PCT`, the detector opens a pending gap for each active matching Polymarket market:

- BTC Binance moves are matched to BTC Polymarket markets.
- ETH Binance moves are matched to ETH Polymarket markets.
- Up pressure watches the YES token.
- Down pressure watches the NO token.

The pending gap closes when the matching Polymarket token mid price or best ask moves by at least `GAP_REPRICE_THRESHOLD` or the market tick size, whichever is larger.

## GapEvent Fields

- `symbol`: Binance symbol such as `BTCUSDT`.
- `timeframe`: Polymarket contract window, currently `5m` or `15m`.
- `direction`: `UP` or `DOWN` Binance pressure.
- `binance_move_pct`: strongest Binance move in percent.
- `poly_market_price_before`: Polymarket mid probability before repricing.
- `poly_market_price_after`: Polymarket mid probability after repricing.
- `detected_ts`: nanosecond timestamp when the Binance-led gap was detected.
- `repriced_ts`: nanosecond timestamp from the Polymarket quote that closed the gap.
- `gap_duration_ms`: measured repricing delay.
- `estimated_edge`: rough probability-space edge estimate, not a trading signal.

## Monitor

```bash
python -m app.main gap-monitor
```

The monitor prints:

- number of detected gaps
- number of completed gaps
- median gap duration
- p95 gap duration
- average estimated edge
- stale feed count

Completed gaps are written asynchronously to:

```text
data/logs/gap_events_YYYYMMDD.jsonl
```

These JSONL files are append-only measurement artifacts. They are not database writes and they are not used for realtime order execution.

## Interpreting Results

Short median and p95 durations mean Polymarket reprices quickly after Binance moves, leaving little time to trade. Longer durations may indicate lag, but they are not sufficient on their own: fees, spread, fill probability, queue position, stale feeds, adverse selection, and market resolution mechanics can erase apparent edge.

`estimated_edge` is intentionally conservative and approximate. Treat it as a research metric for ranking episodes, not as an executable price or order instruction.
