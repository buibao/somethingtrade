# Phase 4.2 Failure Investigation

- Failure classification: `LABEL_VALID_RATE_FAILURE`
- Failed Definition of Done item: `horizon_100ms valid_rate_eligible_rows 0.839213 below threshold 0.95`
- Report path: `D:/somethingtrade/data/reports/phase_4_2_dataset_quality_report.json`

## Debug Artifacts

- `D:/somethingtrade/data/debug/phase_4_2_label_generation_summary.json`
- `D:/somethingtrade/data/debug/phase_4_2_label_invalid_cases.jsonl`
- `D:/somethingtrade/data/debug/phase_4_2_leakage_check.json`
- `D:/somethingtrade/data/debug/phase_4_2_dataset_schema_violations.jsonl`
- `D:/somethingtrade/data/debug/phase_4_2_pytest_output.txt`

## Hypothesis

Definition of Done failed.

Hard fail reasons:
- horizon_100ms valid_rate_eligible_rows 0.839213 below threshold 0.95

Per-horizon eligible valid rates:
- horizon_100ms: valid_rate_eligible_rows=0.8392132896272015, eligible_count=17999, valid_count=15105, invalid_reasons={'FUTURE_GAP_TOO_LARGE': 2894, 'NO_FUTURE_SAMPLE': 1}
- horizon_250ms: valid_rate_eligible_rows=0.9937211757515142, eligible_count=17997, valid_count=17884, invalid_reasons={'FUTURE_GAP_TOO_LARGE': 113, 'NO_FUTURE_SAMPLE': 3}
- horizon_500ms: valid_rate_eligible_rows=0.997832731314254, eligible_count=17995, valid_count=17956, invalid_reasons={'FUTURE_GAP_TOO_LARGE': 39, 'NO_FUTURE_SAMPLE': 5}
- horizon_1000ms: valid_rate_eligible_rows=0.9992217898832685, eligible_count=17990, valid_count=17976, invalid_reasons={'FUTURE_GAP_TOO_LARGE': 14, 'NO_FUTURE_SAMPLE': 10}
- horizon_2000ms: valid_rate_eligible_rows=1.0, eligible_count=17980, valid_count=17980, invalid_reasons={'NO_FUTURE_SAMPLE': 20}
- horizon_5000ms: valid_rate_eligible_rows=1.0, eligible_count=17949, valid_count=17949, invalid_reasons={'NO_FUTURE_SAMPLE': 51}

Leakage check: passed=True, feature_leakage_violations=0, label_leakage_violations=0

If pytest, schema, timestamp monotonicity, and leakage all pass while only eligible label valid rate fails, the blocker is source dataset coverage for the affected horizon under the required max_future_gap_ms policy.

## Fix Applied

Artifact/report compliance was enforced. Label logic, max_future_gap_ms thresholds, and valid-rate thresholds were not changed.

## Rerun Result

This invocation completed pytest, generated reports/debug artifacts, failed Definition of Done, and did not create the final bundle.
