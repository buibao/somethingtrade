# Phase 4.0 Dataset Quality Report & Empirical Calibration

## 1. Executive Summary

- Readiness classification: `NEEDS_MORE_CLEANING`
- Parsed rows: 2457
- Included rows: 2457
- Primary rows (B or better): 1778
- Measured executable repricing success count: 110 (4.48%)
- Recommended next phase: Fix order book/data quality issues

## 2. Input Audit

- Input path: `data\logs\gap_events_20260518.jsonl`
- File exists: True
- File size bytes: 7516388
- Physical lines: 2457
- Blank lines skipped: 0
- Malformed JSON lines: 0
- Time range: 2026-05-18T14:18:52.094132+00:00 to 2026-05-18T14:45:41.779204+00:00

## 3. Dataset Health

- Rows by quality tier: `{"A": 466, "B": 1312, "C": 159, "D": 520}`
- Rows by validation mode: `{"diagnostic": 0, "strict": 0, "tolerant": 2457, "unknown": 0}`
- Pre-entry reject rate: 45.30%
- Window reject rate: 44.24%
- Timeout rate: 5.98%
- Lifecycle reject rate: 0.00%

## 4. Quality Tier Analysis

| Tier | Rows | Success Rate | Median Exec Delay | Median Edge Ticks | Warnings |
| --- | ---: | ---: | ---: | ---: | --- |
| A | 466 | 4.08% | 1560 | 1 | - |
| B | 1312 | 6.25% | 1136 | 1 | - |
| C | 159 | 5.66% | 1681 | 1 | - |
| D | 520 | 0.00% | - | - | diagnostic_only_not_for_clean_empirical_buckets, no_measured_executable_repricing_success, executable_repricing_delay_missing |

## 5. Validation Mode Analysis

| Mode | Rows | Success Rate | Median Exec Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| strict | 0 | 0.00% | - | - |
| tolerant | 2457 | 4.48% | 1341 | 1 |
| diagnostic | 0 | 0.00% | - | - |
| unknown | 0 | 0.00% | - | - |

- Tolerant mode materially changes distribution: unknown
- Reason: strict and tolerant cohorts do not both have enough rows

## 6. Reject Taxonomy

| Category | Count | Pct Total | Pct Rejected |
| --- | ---: | ---: | ---: |
| pre_entry_data_unavailable | 0 | 0.00% | 0.00% |
| book_quality | 1297 | 52.79% | 55.26% |
| liquidity | 454 | 18.48% | 19.34% |
| spread_and_entry_quality | 283 | 11.52% | 12.06% |
| staleness | 166 | 6.76% | 7.07% |
| lifecycle | 0 | 0.00% | 0.00% |
| timeout | 147 | 5.98% | 6.26% |
| edge_failure | 0 | 0.00% | 0.00% |
| unknown | 0 | 0.00% | 0.00% |

## 7. Timing Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| mid_repricing_delay_ms | 555 | 1902 | 500.2 | 3428 | 4824 |
| executable_repricing_delay_ms | 110 | 2347 | 1341 | 3890 | 4628 |
| tradable_window_ms | 2457 | 0 | 52.03 | 4867 | 5292 |
| binance_quote_age_ms | 2457 | 0 | 2.46 | 106.6 | 1284 |
| polymarket_quote_age_ms | 2457 | 0 | 0.4141 | 1506 | 1.753e+04 |

## 8. Edge Analysis

Measured edge is exit bid minus entry ask under measurement assumptions before full fee, slippage, and queue modeling.

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| exit_edge_after_spread | 110 | 2347 | 0.01 | 0.01 | 0.02 |
| estimated_edge_after_spread | 110 | 2347 | 0.01 | 0.01 | 0.02 |
| exit_edge_ticks | 110 | 2347 | 1 | 1 | 2 |

## 9. Liquidity & Spread Analysis

| Metric | Count | Missing | Median | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| spread_before | 2419 | 38 | 0.01 | 0.03 | 0.14 |
| spread_after | 2419 | 38 | 0.01 | 0.05 | 0.14 |
| spread_ticks_at_detection | 2419 | 38 | 1 | 3 | 14 |
| before_best_bid_size | 2216 | 241 | 50.41 | 5.911e+05 | 1.152e+06 |
| before_best_ask_size | 2177 | 280 | 33 | 6.393e+05 | 1.152e+06 |
| entry_ask_size | 2177 | 280 | 33 | 6.393e+05 | 1.152e+06 |

## 10. Stale Feed Analysis

- Staleness status: measured_from_stale_source_or_reject_reason
- Stale source distribution: `{"binance": 25, "polymarket": 144, "unknown": 2288}`
- Quote stale rate: 100.00%
- Binance stale rate: 1.02%
- Polymarket stale rate: 5.86%
- Both stale rate: 0.00%

## 10.5 Runtime Coverage Analysis

- Runtime coverage status: analyzed
- Runtime summary rows: 25
- Gap-event/runtime coverage ratio: 106.82%
- Runtime coverage warnings: -

## 11. Tick Calibration Analysis

- Tick size distribution: `{"0.01": 2457}`
- Tolerated mismatch row count: 1312
- Mismatch sample status: loaded
- Mismatch sample total: 2120
- Mismatch by error type: `{"best_ask_size_unknown": 163, "best_bid_size_unknown": 163, "reported_best_ask_mismatch": 912, "reported_best_bid_mismatch": 882}`
- Top affected markets: `{"2284526": 130, "2284527": 130, "2284544": 110, "2284548": 110, "2284558": 236, "2284559": 214, "2284569": 110, "2284592": 110, "2284594": 120, "2284625": 122}`
- Top affected tokens: `{"100612417448368056653081700792465301672935968771525975369509207914619205254975": 118, "24463247630057723549138122437205271395024024876361849372054065508310917270031": 118, "26050532070411626602806071511906939306624197388229054187267397649393009437128": 65, "46132721015651855109750727693994082346794979657873879220920600465816575428632": 107, "560208703489649642408371043831231583070923403601460648127103316943331245372": 65, "75148472601129288871721643642444504231949217902990664645554344419280768689819": 65, "7551272508672865727040178739403960231661404005153176891927031308635539837920": 107, "83678854090518230352464222413865896305975596111869562733723286233276982189867": 65, "83938176899270943070396756218618387377090936286373530949876573065340297568877": 61, "87965743379865215929851537809941802658857025694067153043985759847861113377071": 61}`
- Pct within 1 tick: 71.42%
- Pct above 2 ticks: 4.58%
- Warnings: -

## 12. Empirical Bucket Analysis

By default, empirical buckets are computed on primary rows only. For `--primary-min-tier B`, primary rows are A/B.
These buckets are descriptive historical measurements only; they are not forecasts, model outputs, or execution signals.

| Feature | Bucket | Rows | Success Rate | Sparse |
| --- | --- | ---: | ---: | --- |
| binance_move_pct | <= -0.20 | 9 | 0.00% | True |
| binance_move_pct | -0.20 to -0.10 | 123 | 2.44% | False |
| binance_move_pct | -0.10 to -0.05 | 790 | 6.96% | False |
| binance_move_pct | -0.05 to 0 | 0 | 0.00% | True |
| binance_move_pct | 0 to 0.05 | 0 | 0.00% | True |
| binance_move_pct | 0.05 to 0.10 | 652 | 3.53% | False |
| binance_move_pct | 0.10 to 0.20 | 179 | 10.61% | False |
| binance_move_pct | > 0.20 | 25 | 4.00% | True |
| spread_ticks_at_detection | 0 | 0 | 0.00% | True |
| spread_ticks_at_detection | 1 | 3 | 0.00% | True |
| spread_ticks_at_detection | 2 | 0 | 0.00% | True |
| spread_ticks_at_detection | 3-5 | 202 | 0.50% | False |
| spread_ticks_at_detection | 6-10 | 26 | 0.00% | True |
| spread_ticks_at_detection | >10 | 2 | 0.00% | True |
| tradable_window_ms | 0-50ms | 643 | 0.00% | False |
| tradable_window_ms | 50-100ms | 79 | 0.00% | False |
| tradable_window_ms | 100-250ms | 144 | 1.39% | False |
| tradable_window_ms | 250-500ms | 164 | 6.71% | False |
| tradable_window_ms | 500-1000ms | 186 | 13.44% | False |
| tradable_window_ms | >1000ms | 562 | 11.21% | False |

## 13. Cohort Sensitivity

| Cohort | Rows | Success Rate | Median Delay | Median Edge Ticks |
| --- | ---: | ---: | ---: | ---: |
| A only | 466 | 4.08% | 1560 | 1 |
| A/B | 1778 | 5.68% | 1298 | 1 |
| A/B/C | 1937 | 5.68% | 1341 | 1 |
| all rows | 2457 | 4.48% | 1341 | 1 |
- Conclusion: stable

## 14. Readiness Assessment

- Classification: `NEEDS_MORE_CLEANING`
- Blocking issues: `["D-tier diagnostic row share should be below threshold", "quote stale rate should be below threshold", "book incomplete reject rate should be below threshold"]`
- Non-blocking warnings: `["Do not proceed to model prediction yet"]`

## 15. Recommended Next Phase

Fix order book/data quality issues

## 16. Non-Goals Confirmed

- no model prediction was added
- no ML training was added
- no trading signal was added
- no live execution was added
- no private-key handling was added
- no wallet copy trading or on-chain wallet logic was added
- no LLM was added to realtime path
