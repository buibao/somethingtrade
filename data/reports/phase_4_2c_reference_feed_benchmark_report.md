# Phase 4.2C Reference Feed Benchmark Report

Status: **fail**

## Status

- Implementation: `pass`
- Runtime: `pass`
- Benchmark: `fail`
- Primary failure: `REFERENCE_FEED_EMPTY`

## Ranking

- `depth_mid` valid_rate_eligible_rows=`0.8111561753430746` gap_p95_ms=`104.3829` gap_p99_ms=`236.2084` passes_100ms_gate=`False` semantic=`quote_mid`
- `bookTicker_mid` valid_rate_eligible_rows=`0.6018132265420769` gap_p95_ms=`93.0` gap_p99_ms=`312.0` passes_100ms_gate=`False` semantic=`quote_mid`
- `trade_price` valid_rate_eligible_rows=`0.0` gap_p95_ms=`None` gap_p99_ms=`None` passes_100ms_gate=`False` semantic=`transaction_price`
- `aggTrade_price` valid_rate_eligible_rows=`0.0` gap_p95_ms=`None` gap_p99_ms=`None` passes_100ms_gate=`False` semantic=`transaction_price`

## Selected Source

- Selected reference source: `None`
- Semantic warning: `None`

## Sources

- `depth_mid` events=`18000` valid_events=`18000` eligible_rate=`0.8111561753430746` future_gap_p95/p99=`103.247`/`224.591` bad_time_ratio=`0.03557677220770099` leakage=`0`
- `bookTicker_mid` events=`112249` valid_events=`112249` eligible_rate=`0.6018132265420769` future_gap_p95/p99=`451.9467`/`779.1944` bad_time_ratio=`0.40290150349321946` leakage=`0`
- `trade_price` events=`0` valid_events=`0` eligible_rate=`0.0` future_gap_p95/p99=`None`/`None` bad_time_ratio=`0.0` leakage=`0`
- `aggTrade_price` events=`0` valid_events=`0` eligible_rate=`0.0` future_gap_p95/p99=`None`/`None` bad_time_ratio=`0.0` leakage=`0`

## Hard Fail Reasons

- reference datasets missing: trade_price,aggTrade_price
- no reference source achieved valid_rate_eligible_rows >= 0.95 with strict 100ms gate

## Recommendation

Keep 100ms as a hard gate. Next engineering step: collect during a more active session, benchmark futures/SBE/paid feeds later, or improve capture locality. Do not move to strategy/model/execution/PnL from this failing benchmark.
