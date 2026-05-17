# Phase 4.0 Dataset Quality Report & Empirical Calibration

## 1. Executive Summary

- Readiness classification: `NEEDS_MORE_DATA`
- Parsed rows: 25
- Included rows: 25
- Primary rows (B or better): 14
- Measured executable repricing success count: 0 (0.00%)
- Recommended next phase: Collect more Phase 3 data

## 2. Input Audit

- Input path: `data\logs\gap_events_20260517.jsonl`
- File exists: True
- File size bytes: 65621
- Physical lines: 25
- Blank lines skipped: 0
- Malformed JSON lines: 0
- Time range: 2026-05-17T16:16:36.939183+00:00 to 2026-05-17T16:30:51.751448+00:00

## 3. Dataset Health

- Rows by quality tier: `{"A": 0, "B": 14, "C": 11, "D": 0}`
- Rows by validation mode: `{"diagnostic": 0, "strict": 0, "tolerant": 25, "unknown": 0}`
- Pre-entry reject rate: 52.00%
- Window reject rate: 40.00%
- Timeout rate: 8.00%
- Lifecycle reject rate: 0.00%

## 4. Quality Tier Analysis

| Tier | Rows | Success Rate | Median Exec Delay | Median Edge Ticks | Warnings |
| --- | ---: | ---: | ---: | ---: | --- |
| A | 0 | 0.00% | - | - | no_rows |
| B | 14 | 0.00% | - | - | no_measured_executable_repricing_success, executable_repricing_delay_missing |
| C | 11 | 0.00% | - | - | no_measured_executable_repricing_success, executable_repricing_delay_missing |
| D | 0 | 0.00% | - | - | no_rows |

## 5. Validation Mode Analysis

| Mode | Rows | Success Rate | Median Exec Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| strict | 0 | 0.00% | - | - |
| tolerant | 25 | 0.00% | - | - |
| diagnostic | 0 | 0.00% | - | - |
| unknown | 0 | 0.00% | - | - |

- Tolerant mode materially changes distribution: unknown
- Reason: strict and tolerant cohorts do not both have enough rows

## 6. Reject Taxonomy

| Category | Count | Pct Total | Pct Rejected |
| --- | ---: | ---: | ---: |
| pre_entry_data_unavailable | 0 | 0.00% | 0.00% |
| book_quality | 11 | 44.00% | 44.00% |
| liquidity | 7 | 28.00% | 28.00% |
| spread_and_entry_quality | 5 | 20.00% | 20.00% |
| staleness | 0 | 0.00% | 0.00% |
| lifecycle | 0 | 0.00% | 0.00% |
| timeout | 2 | 8.00% | 8.00% |
| edge_failure | 0 | 0.00% | 0.00% |
| unknown | 0 | 0.00% | 0.00% |

## 7. Timing Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| mid_repricing_delay_ms | 6 | 19 | 197.8 | 2528 | 2528 |
| executable_repricing_delay_ms | 0 | 25 | - | - | - |
| tradable_window_ms | 25 | 0 | 0 | 4969 | 4995 |
| binance_quote_age_ms | 0 | 25 | - | - | - |
| polymarket_quote_age_ms | 0 | 25 | - | - | - |

## 8. Edge Analysis

Measured edge is exit bid minus entry ask under measurement assumptions before full fee, slippage, and queue modeling.

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| exit_edge_after_spread | 0 | 25 | - | - | - |
| estimated_edge_after_spread | 0 | 25 | - | - | - |
| exit_edge_ticks | 0 | 25 | - | - | - |

## 9. Liquidity & Spread Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| spread_before | 25 | 0 | 0.02 | 0.1 | 0.12 |
| spread_after | 25 | 0 | 0.03 | 0.1 | 0.12 |
| spread_ticks_at_detection | 25 | 0 | 2 | 10 | 12 |
| before_best_bid_size | 25 | 0 | 20 | 179.5 | 184.5 |
| before_best_ask_size | 24 | 1 | 11.25 | 32 | 40.63 |
| entry_ask_size | 24 | 1 | 11.25 | 32 | 40.63 |

## 10. Stale Feed Analysis

- Stale source distribution: `{"unknown": 25}`
- Quote stale rate: 0.00%
- Binance stale rate: 0.00%
- Polymarket stale rate: 0.00%
- Both stale rate: 0.00%

## 11. Tick Calibration Analysis

- Tick size distribution: `{"0.01": 25}`
- Tolerated mismatch row count: 14
- Mismatch sample status: skipped_missing_mismatch_sample_input

## 12. Empirical Bucket Analysis

These buckets are descriptive historical measurements only; they are not forecasts, model outputs, or execution signals.

| Feature | Bucket | Rows | Success Rate | Sparse |
| --- | --- | ---: | ---: | --- |
| binance_move_pct | <= -0.20 | 0 | 0.00% | True |
| binance_move_pct | -0.20 to -0.10 | 0 | 0.00% | True |
| binance_move_pct | -0.10 to -0.05 | 13 | 0.00% | True |
| binance_move_pct | -0.05 to 0 | 0 | 0.00% | True |
| binance_move_pct | 0 to 0.05 | 0 | 0.00% | True |
| binance_move_pct | 0.05 to 0.10 | 1 | 0.00% | True |
| binance_move_pct | 0.10 to 0.20 | 0 | 0.00% | True |
| binance_move_pct | > 0.20 | 0 | 0.00% | True |
| spread_ticks_at_detection | 0 | 0 | 0.00% | True |
| spread_ticks_at_detection | 1 | 0 | 0.00% | True |
| spread_ticks_at_detection | 2 | 0 | 0.00% | True |
| spread_ticks_at_detection | 3-5 | 3 | 0.00% | True |
| spread_ticks_at_detection | 6-10 | 1 | 0.00% | True |
| spread_ticks_at_detection | >10 | 1 | 0.00% | True |
| tradable_window_ms | 0-50ms | 9 | 0.00% | True |
| tradable_window_ms | 50-100ms | 0 | 0.00% | True |
| tradable_window_ms | 100-250ms | 2 | 0.00% | True |
| tradable_window_ms | 250-500ms | 1 | 0.00% | True |
| tradable_window_ms | 500-1000ms | 0 | 0.00% | True |
| tradable_window_ms | >1000ms | 2 | 0.00% | True |

## 13. Cohort Sensitivity

| Cohort | Rows | Success Rate | Median Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| A only | 0 | 0.00% | - | - |
| A/B | 14 | 0.00% | - | - |
| A/B/C | 25 | 0.00% | - | - |
| all rows | 25 | 0.00% | - | - |
- Conclusion: insufficient_data

## 14. Readiness Assessment

- Classification: `NEEDS_MORE_DATA`
- Blocking issues: `["primary rows should meet the Phase 4 minimum", "book incomplete reject rate should be below threshold", "executable repricing delay should be available", "tick-normalized edge should be available"]`
- Non-blocking warnings: `["Do not proceed to model prediction yet", "at least one empirical bucket should have enough rows", "measured executable repricing success rows should support empirical summaries"]`

## 15. Recommended Next Phase

Collect more Phase 3 data

## 16. Non-Goals Confirmed

- no model prediction was added
- no ML training was added
- no trading signal was added
- no live execution was added
- no private-key handling was added
- no wallet copy trading or on-chain wallet logic was added
- no LLM was added to realtime path
