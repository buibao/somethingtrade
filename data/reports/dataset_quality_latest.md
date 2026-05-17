# Phase 4.0 Dataset Quality Report & Empirical Calibration

## 1. Executive Summary

- Readiness classification: `NEEDS_MORE_DATA`
- Parsed rows: 54
- Included rows: 54
- Primary rows (B or better): 32
- Measured executable repricing success count: 2 (3.70%)
- Recommended next phase: Collect more Phase 3 data

## 2. Input Audit

- Input path: `data\logs\gap_events_20260517.jsonl`
- File exists: True
- File size bytes: 140198
- Physical lines: 54
- Blank lines skipped: 0
- Malformed JSON lines: 0
- Time range: 2026-05-17T18:04:24.627514+00:00 to 2026-05-17T18:22:33.446101+00:00

## 3. Dataset Health

- Rows by quality tier: `{"A": 2, "B": 30, "C": 0, "D": 22}`
- Rows by validation mode: `{"diagnostic": 0, "strict": 0, "tolerant": 54, "unknown": 0}`
- Pre-entry reject rate: 61.11%
- Window reject rate: 24.07%
- Timeout rate: 11.11%
- Lifecycle reject rate: 0.00%

## 4. Quality Tier Analysis

| Tier | Rows | Success Rate | Median Exec Delay | Median Edge Ticks | Warnings |
| --- | ---: | ---: | ---: | ---: | --- |
| A | 2 | 0.00% | - | - | no_measured_executable_repricing_success, executable_repricing_delay_missing |
| B | 30 | 6.67% | 1040 | 1 | - |
| C | 0 | 0.00% | - | - | no_rows |
| D | 22 | 0.00% | - | - | diagnostic_only_not_for_clean_empirical_buckets, no_measured_executable_repricing_success, executable_repricing_delay_missing |

## 5. Validation Mode Analysis

| Mode | Rows | Success Rate | Median Exec Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| strict | 0 | 0.00% | - | - |
| tolerant | 54 | 3.70% | 1040 | 1 |
| diagnostic | 0 | 0.00% | - | - |
| unknown | 0 | 0.00% | - | - |

- Tolerant mode materially changes distribution: unknown
- Reason: strict and tolerant cohorts do not both have enough rows

## 6. Reject Taxonomy

| Category | Count | Pct Total | Pct Rejected |
| --- | ---: | ---: | ---: |
| pre_entry_data_unavailable | 0 | 0.00% | 0.00% |
| book_quality | 42 | 77.78% | 80.77% |
| liquidity | 2 | 3.70% | 3.85% |
| spread_and_entry_quality | 2 | 3.70% | 3.85% |
| staleness | 0 | 0.00% | 0.00% |
| lifecycle | 0 | 0.00% | 0.00% |
| timeout | 6 | 11.11% | 11.54% |
| edge_failure | 0 | 0.00% | 0.00% |
| unknown | 0 | 0.00% | 0.00% |

## 7. Timing Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| mid_repricing_delay_ms | 13 | 41 | 883.5 | 3092 | 4388 |
| executable_repricing_delay_ms | 2 | 52 | 1040 | 1278 | 1278 |
| tradable_window_ms | 54 | 0 | 0 | 4798 | 4968 |
| binance_quote_age_ms | 0 | 54 | - | - | - |
| polymarket_quote_age_ms | 0 | 54 | - | - | - |

## 8. Edge Analysis

Measured edge is exit bid minus entry ask under measurement assumptions before full fee, slippage, and queue modeling.

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| exit_edge_after_spread | 2 | 52 | 0.01 | 0.01 | 0.01 |
| estimated_edge_after_spread | 2 | 52 | 0.01 | 0.01 | 0.01 |
| exit_edge_ticks | 2 | 52 | 1 | 1 | 1 |

## 9. Liquidity & Spread Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| spread_before | 54 | 0 | 0.01 | 0.03 | 0.05 |
| spread_after | 54 | 0 | 0.01 | 0.04 | 0.05 |
| spread_ticks_at_detection | 54 | 0 | 1 | 3 | 5 |
| before_best_bid_size | 36 | 18 | 37.25 | 1.638e+04 | 1.658e+04 |
| before_best_ask_size | 50 | 4 | 65.03 | 8371 | 8371 |
| entry_ask_size | 50 | 4 | 65.03 | 8371 | 8371 |

## 10. Stale Feed Analysis

- Staleness status: unknown_missing_quote_age_fields
- Stale source distribution: `{"unknown": 54}`
- Quote stale rate: -
- Binance stale rate: -
- Polymarket stale rate: -
- Both stale rate: -

## 11. Tick Calibration Analysis

- Tick size distribution: `{"0.01": 54}`
- Tolerated mismatch row count: 30
- Mismatch sample status: loaded
- Warnings: -

## 12. Empirical Bucket Analysis

By default, empirical buckets are computed on primary rows only. For `--primary-min-tier B`, primary rows are A/B.
These buckets are descriptive historical measurements only; they are not forecasts, model outputs, or execution signals.

| Feature | Bucket | Rows | Success Rate | Sparse |
| --- | --- | ---: | ---: | --- |
| binance_move_pct | <= -0.20 | 0 | 0.00% | True |
| binance_move_pct | -0.20 to -0.10 | 0 | 0.00% | True |
| binance_move_pct | -0.10 to -0.05 | 17 | 5.88% | True |
| binance_move_pct | -0.05 to 0 | 0 | 0.00% | True |
| binance_move_pct | 0 to 0.05 | 0 | 0.00% | True |
| binance_move_pct | 0.05 to 0.10 | 15 | 6.67% | True |
| binance_move_pct | 0.10 to 0.20 | 0 | 0.00% | True |
| binance_move_pct | > 0.20 | 0 | 0.00% | True |
| spread_ticks_at_detection | 0 | 0 | 0.00% | True |
| spread_ticks_at_detection | 1 | 0 | 0.00% | True |
| spread_ticks_at_detection | 2 | 0 | 0.00% | True |
| spread_ticks_at_detection | 3-5 | 6 | 0.00% | True |
| spread_ticks_at_detection | 6-10 | 0 | 0.00% | True |
| spread_ticks_at_detection | >10 | 0 | 0.00% | True |
| tradable_window_ms | 0-50ms | 13 | 0.00% | True |
| tradable_window_ms | 50-100ms | 1 | 0.00% | True |
| tradable_window_ms | 100-250ms | 2 | 0.00% | True |
| tradable_window_ms | 250-500ms | 0 | 0.00% | True |
| tradable_window_ms | 500-1000ms | 2 | 50.00% | True |
| tradable_window_ms | >1000ms | 14 | 7.14% | True |

## 13. Cohort Sensitivity

| Cohort | Rows | Success Rate | Median Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| A only | 2 | 0.00% | - | - |
| A/B | 32 | 6.25% | 1040 | 1 |
| A/B/C | 32 | 6.25% | 1040 | 1 |
| all rows | 54 | 3.70% | 1040 | 1 |
- Conclusion: stable

## 14. Readiness Assessment

- Classification: `NEEDS_MORE_DATA`
- Blocking issues: `["primary rows should meet the Phase 4 minimum", "D-tier diagnostic row share should be below threshold", "book incomplete reject rate should be below threshold"]`
- Non-blocking warnings: `["Do not proceed to model prediction yet", "measured executable repricing success rows should support empirical summaries", "quote stale rate cannot be confidently assessed because quote-age fields are missing", "quote_age_fields_missing"]`

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
