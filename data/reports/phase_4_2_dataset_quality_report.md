# Phase 4.2 Dataset Quality Report

Status: **fail**

## Paths

- Input: `data/dataset/orderbook_clean_samples.jsonl`
- Output: `data/dataset/orderbook_labeled_samples.jsonl`

## Sample Coverage

- Input samples: `18000`
- Labeled samples: `18000`
- Duration seconds: `1799.8835632`
- Sample rate per second: `10.000646912958041`

## Timestamp Quality

```json
{
  "duplicate_timestamp_count": 0,
  "large_gap_count": 0,
  "large_gap_threshold_ms": 1000.0,
  "max_gap_ms": 953.0022,
  "p50_gap_ms": 100.4747,
  "p95_gap_ms": 122.9177,
  "p99_gap_ms": 165.3203,
  "timestamp_monotonic_violations": 0
}
```

## Feature Quality

```json
{
  "feature_leakage_violations": 0,
  "feature_warning_counts": {
    "past_mid_return_1000ms_bps_no_past_sample": 11,
    "past_mid_return_100ms_bps_gap_too_large": 3001,
    "past_mid_return_100ms_bps_no_past_sample": 1,
    "past_mid_return_500ms_bps_gap_too_large": 8,
    "past_mid_return_500ms_bps_no_past_sample": 6,
    "past_spread_change_500ms_bps_gap_too_large": 8,
    "past_spread_change_500ms_bps_no_past_sample": 6
  },
  "null_feature_counts": {
    "past_mid_return_1000ms_bps": 11,
    "past_mid_return_100ms_bps": 3002,
    "past_mid_return_500ms_bps": 14,
    "past_spread_change_500ms_bps": 14
  }
}
```

## Label Valid Rates

| Horizon | Eligible valid rate | Valid | Invalid | Up | Down | Flat |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| horizon_100ms | 0.8392 | 15105 | 2895 | 190 | 257 | 14658 |
| horizon_250ms | 0.9937 | 17884 | 116 | 448 | 554 | 16882 |
| horizon_500ms | 0.9978 | 17956 | 44 | 743 | 874 | 16339 |
| horizon_1000ms | 0.9992 | 17976 | 24 | 1261 | 1420 | 15295 |
| horizon_2000ms | 1.0000 | 17980 | 20 | 2155 | 2278 | 13547 |
| horizon_5000ms | 1.0000 | 17949 | 51 | 4197 | 4094 | 9658 |

## Invalid Label Reasons

```json
{
  "horizon_1000ms": {
    "FUTURE_GAP_TOO_LARGE": 14,
    "NO_FUTURE_SAMPLE": 10
  },
  "horizon_100ms": {
    "FUTURE_GAP_TOO_LARGE": 2894,
    "NO_FUTURE_SAMPLE": 1
  },
  "horizon_2000ms": {
    "NO_FUTURE_SAMPLE": 20
  },
  "horizon_250ms": {
    "FUTURE_GAP_TOO_LARGE": 113,
    "NO_FUTURE_SAMPLE": 3
  },
  "horizon_5000ms": {
    "NO_FUTURE_SAMPLE": 51
  },
  "horizon_500ms": {
    "FUTURE_GAP_TOO_LARGE": 39,
    "NO_FUTURE_SAMPLE": 5
  }
}
```

## Leakage Check

```json
{
  "feature_leakage_violations": 0,
  "label_leakage_violations": 0,
  "passed": true,
  "violations": []
}
```

## Warnings And Limitations

- horizon_1000ms:null_future_labels_near_end_of_file
- horizon_100ms:class_imbalance
- horizon_100ms:low_volatility_period
- horizon_100ms:null_future_labels_near_end_of_file
- horizon_100ms:too_many_flat_labels_or_low_volatility_period
- horizon_2000ms:null_future_labels_near_end_of_file
- horizon_250ms:class_imbalance
- horizon_250ms:null_future_labels_near_end_of_file
- horizon_250ms:too_many_flat_labels_or_low_volatility_period
- horizon_5000ms:null_future_labels_near_end_of_file
- horizon_500ms:class_imbalance
- horizon_500ms:null_future_labels_near_end_of_file
- horizon_500ms:too_many_flat_labels_or_low_volatility_period
- null_past_change_features_near_beginning_or_sparse_gaps

## Readiness

Dataset needs more cleanup or collection before Phase 5.

## Hard Fail Reasons

- horizon_100ms valid_rate_eligible_rows 0.839213 below threshold 0.95
