# Phase 3.15 Order Book Diagnostics

Phase 3.15 is diagnostics only. It does not change executable success semantics and it does not place orders.

## Why Reported Best Mismatch Is Diagnostic

Polymarket websocket `best_bid_ask` and `price_change` messages can report best prices that disagree with the locally maintained ladder. That disagreement can come from a local update bug, message sequencing, an out-of-date snapshot, or a harmless one-tick race between message types.

The local ladder remains the source of truth for sizes. A reported best mismatch is recorded so strict and diagnostic live runs can be compared before deciding whether validation is too conservative.

## Completeness Fields

`book_has_snapshot`:
The token has received at least one full `book` snapshot. A `price_change` before a snapshot never sets this true.

`book_structurally_complete`:
The local ladder is internally usable: it has a snapshot, is not invalidated, has computable best bid/ask, and has known best sizes.

`reported_best_validation_ok`:
The most recent reported best price, when present, agrees with the local ladder under the configured validation mode and tick tolerance. Missing reported best fields do not by themselves make this false.

`book_complete`:
Backward-compatible conservative flag used by `GapDetector`. In strict mode a reported best mismatch can make this false. In diagnostic mode a reported best mismatch is recorded but does not make the quote incomplete by itself.

## Validation Modes

`strict`:
Reported best bid/ask mismatches mark the quote incomplete. This is the conservative measurement mode.

`tolerant`:
Reported best mismatches within `POLYMARKET_BEST_VALIDATION_TOLERANCE_TICKS * tick_size` are allowed, but counters and samples are still recorded.

`diagnostic`:
Reported best mismatches are recorded and sampled, but they do not mark the quote incomplete by themselves. Real structural failures such as `missing_snapshot`, invalid ladders, missing best sizes, lifecycle invalidation, or stale books remain non-tradable.

## Mismatch Samples

Mismatch samples are written to:

```text
data/debug/polymarket_orderbook_mismatch_samples.jsonl
```

Each row includes market identity, token identity, local best prices and sizes, reported best prices and sizes when present, payload/hash fields, local sequence, receive monotonic timestamp, validation error, and compact raw payload fields.

Sampling is rate-limited per token per minute by:

```text
POLYMARKET_MISMATCH_SAMPLE_PER_TOKEN_PER_MIN
```

## Comparing Strict And Diagnostic Runs

Run strict mode for 5-10 minutes:

```bash
python -m app.main gap-monitor \
  --min-move-pct 0.02 \
  --reprice-threshold 0.002 \
  --binance-stale-ms 1000 \
  --polymarket-stale-ms 2000 \
  --require-book-ready \
  --book-warmup-max-ms 3000 \
  --best-validation-mode strict
```

Run diagnostic mode for 5-10 minutes:

```bash
python -m app.main gap-monitor \
  --min-move-pct 0.02 \
  --reprice-threshold 0.002 \
  --binance-stale-ms 1000 \
  --polymarket-stale-ms 2000 \
  --require-book-ready \
  --book-warmup-max-ms 3000 \
  --best-validation-mode diagnostic
```

Compare:

- `book_incomplete` count
- `reported_best_bid_mismatch` and `reported_best_ask_mismatch` counts
- `quote_stale` count
- success count
- `data/debug/polymarket_orderbook_mismatch_samples.jsonl`
- `data/debug/polymarket_book_readiness.json`

If diagnostic mode materially lowers `book_incomplete` without increasing structural errors, the next step is to inspect mismatch samples and decide whether tolerant mode is justified. That is still measurement work, not trading.

## Stale Diagnostics

`quote_stale` observations include:

- `stale_source`: `binance`, `polymarket`, `both`, or `unknown`
- `binance_quote_age_ms`
- `polymarket_quote_age_ms`
- `now_monotonic_ns`
- `last_binance_update_monotonic_ns`
- `last_polymarket_update_monotonic_ns`

When monotonic timestamps are missing or incompatible, `stale_source` is `unknown` rather than mixing wall-clock and monotonic clocks.

## Measurement Only

Phase 3.15 helps explain quote completeness and stale-feed attribution. It does not decide profitability, train Phase 4 models, simulate queue position, place orders, or handle private keys.
