# Phase 4.2B BookTicker Reference Report

Status: **fail**

## Status Separation

- Implementation: `pass`
- Runtime: `pass`
- Reference feed: `fail`
- Dataset coverage: `fail`
- Primary failure: `horizon_100ms_valid_rate_below_threshold`

## Reference Feed

- Reference quote count: `112249`
- Valid reference quote count: `112249`
- Reference gap p95/p99 ms: `93.0` / `312.0`

## 100ms Coverage

- Reference source: `bookTicker`
- Max future gap ms: `100`
- Eligible rows: `17979`
- Valid rows: `10820`
- Eligible valid rate: `0.6018132265420769`
- Invalid reasons: `{"FUTURE_REFERENCE_GAP_TOO_LARGE": 7159, "NO_FUTURE_REFERENCE": 21}`

## Hard Fail Reasons

- reference_gap_p99_ms > 200: 312.0
- horizon_100ms valid_rate_eligible_rows 0.601813 below threshold 0.95

## Bottleneck Assessment

BookTicker reference feed cadence/network session gaps appear insufficient for strict 100ms labels.
