# Phase 4.2A Failure Investigation

- Failure classification: `LABEL_VALID_RATE_FAILURE`
- Definition of Done status: `fail`
- Primary failure: `horizon_100ms_valid_rate_below_threshold`
- Report path: `data/reports/phase_4_2a_100ms_coverage_report.json`

## Hard Fail Reasons

- sample_gap_p95_ms > 100: 105.2301
- sample_gap_p99_ms > 200: 306.3938
- horizon_100ms valid_rate_eligible_rows 0.801711 below threshold 0.95

## Bottleneck Assessment

Coverage failure appears driven by clean sample cadence/public WS jitter: sample gap percentiles exceed the 100ms research requirement, producing FUTURE_GAP_TOO_LARGE 100ms labels.

## Recommendation

Keep 100ms as a hard gate. Next engineering step: improve capture cadence/protocol and reduce public WebSocket/local processing jitter, then rerun Phase 4.2A. Do not move to Phase 5 while this report is failing.

## Fix Applied

No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.

## Rerun Result

See `data/reports/phase42a_self_check.json` and this report's hard fail reasons.
