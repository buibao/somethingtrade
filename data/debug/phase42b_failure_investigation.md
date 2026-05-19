# Phase 4.2B Failure Investigation

- Failure classification: `LABEL_VALID_RATE_FAILURE`
- Definition of Done status: `fail`
- Primary failure: `horizon_100ms_valid_rate_below_threshold`
- Report path: `data/reports/phase_4_2b_bookticker_reference_report.json`

## Hard Fail Reasons

- reference_gap_p99_ms > 200: 312.0
- horizon_100ms valid_rate_eligible_rows 0.601813 below threshold 0.95

## Bottleneck Assessment

BookTicker reference feed cadence/network session gaps appear insufficient for strict 100ms labels.

## Recommendation

Keep 100ms as a hard gate; inspect bookTicker cadence, local capture timing, and network/session conditions before rerunning Phase 4.2B.

## Fix Applied

No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.
