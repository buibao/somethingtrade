# Phase 4.2C Reference Feed Benchmark Report

Status: **fail**

## Status

- Implementation: `pass`
- Runtime: `pass`
- Benchmark: `fail`
- Primary failure: `NO_REFERENCE_SOURCE_PASSED_100MS`

## Ranking

- `depth_mid` valid_rate_eligible_rows=`0.8130451691760653` gap_p95_ms=`106.7319` gap_p99_ms=`252.7549` passes_100ms_gate=`False` semantic=`quote_mid`
- `bookTicker_mid` valid_rate_eligible_rows=`0.810678531701891` gap_p95_ms=`31.0` gap_p99_ms=`110.0` passes_100ms_gate=`False` semantic=`quote_mid`
- `trade_price` valid_rate_eligible_rows=`0.26692252071861616` gap_p95_ms=`32.0` gap_p99_ms=`563.0` passes_100ms_gate=`False` semantic=`transaction_price`
- `aggTrade_price` valid_rate_eligible_rows=`0.26558763001279273` gap_p95_ms=`469.0` gap_p99_ms=`1203.0` passes_100ms_gate=`False` semantic=`transaction_price`

## Selected Source

- Selected reference source: `None`
- Semantic warning: `None`

## Sources

- `depth_mid` events=`18000` valid_events=`18000` eligible_rate=`0.8130451691760653` future_gap_p95/p99=`105.8043`/`230.1305` bad_time_ratio=`0.04284274786763263` leakage=`0`
- `bookTicker_mid` events=`301538` valid_events=`301538` eligible_rate=`0.810678531701891` future_gap_p95/p99=`249.825`/`497.2529` bad_time_ratio=`0.19960306366820968` leakage=`0`
- `trade_price` events=`96705` valid_events=`96705` eligible_rate=`0.26692252071861616` future_gap_p95/p99=`1456.8406`/`2456.6371` bad_time_ratio=`0.7391516764378281` leakage=`0`
- `aggTrade_price` events=`24297` valid_events=`24297` eligible_rate=`0.26558763001279273` future_gap_p95/p99=`1456.8406`/`2456.6371` bad_time_ratio=`0.7397429683696133` leakage=`0`

## Hard Fail Reasons

- measured reference sources are below valid_rate_eligible_rows 0.95
- no reference source achieved valid_rate_eligible_rows >= 0.95 with strict 100ms gate

## Recommendation

Keep 100ms as a hard gate. Next engineering step: collect during a more active session, benchmark futures/SBE/paid feeds later, or improve capture locality. Do not move to strategy/model/execution/PnL from this failing benchmark.
