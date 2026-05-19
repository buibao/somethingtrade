# Phase 4.2A 100ms Coverage Report

Status: **fail**

## Status Separation

- Implementation: `pass`
- Runtime: `pass`
- Dataset coverage: `fail`
- Primary failure: `horizon_100ms_valid_rate_below_threshold`

## 100ms Coverage

- Max future gap ms: `100`
- Eligible rows: `17999`
- Valid rows: `14430`
- Eligible valid rate: `0.801711206178121`
- Invalid reasons: `{"FUTURE_GAP_TOO_LARGE": 3569, "NO_FUTURE_SAMPLE": 1}`

## Sample Gaps

- Gap p95 ms: `105.2301`
- Gap p99 ms: `306.3938`
- Gap max ms: `1554.0005`

## Leakage

- Passed: `True`
- Feature leakage violations: `0`
- Label leakage violations: `0`

## Bottleneck Assessment

Coverage failure appears driven by clean sample cadence/public WS jitter: sample gap percentiles exceed the 100ms research requirement, producing FUTURE_GAP_TOO_LARGE 100ms labels.

## Hard Fail Reasons

- sample_gap_p95_ms > 100: 105.2301
- sample_gap_p99_ms > 200: 306.3938
- horizon_100ms valid_rate_eligible_rows 0.801711 below threshold 0.95

## Recommendation

Keep 100ms as a hard gate. Next engineering step: improve capture cadence/protocol and reduce public WebSocket/local processing jitter, then rerun Phase 4.2A. Do not move to Phase 5 while this report is failing.
