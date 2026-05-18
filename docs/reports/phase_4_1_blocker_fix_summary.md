# Phase 4.1 Blocker Fix Summary

## 1. Pass/fail evaluation

Added a central `evaluate_phase_4_1_pass(report)` evaluator. Reports now include `phase_4_1_pass`, `phase_4_1_status`, and explicit `phase_4_1_failure_reasons`. Sequence gaps, sequence gap/reset aliases, invalid deltas, sample-before-ready, crossed books, stale books, emitted-clean schema violations, ready guard violations, queue drops/backpressure, and snapshot budget failure are all treated as blockers.

## 2. Stale detection

Separated message receive time from successful book update time. `last_book_update_monotonic_ns` and `last_successful_apply_monotonic_ns` only update on successful snapshot/delta application. Rejected, invalid, old, and duplicate deltas do not refresh successful book timestamps. The processor now checks stale periods using local monotonic time before applying incoming deltas, during idle runtime timeouts, and at report generation. Stale periods are written to `data/debug/stale_period_cases.jsonl` and surfaced in the JSON/Markdown report.

## 3. Invalid delta fail-closed

Expected-sequence deltas with invalid price/size, NaN, infinity, or malformed levels now call `mark_not_ready("invalid_delta_fail_closed")`, set `snapshot_ready=false`, set `ready_to_emit=false`, increment `invalid_delta_count`, write `data/debug/invalid_delta_cases.jsonl`, and require a fresh valid snapshot before recovery. Old duplicate invalid updates remain safely skipped and do not disable readiness.

## 4. Tests added or updated

Added focused blocker regressions in `tests/test_orderbook_phase41_blockers.py` for pass/fail semantics, monotonic stale detection, invalid delta fail-closed, duplicate safety, fresh snapshot recovery, and report failure reasons. Existing Phase 4.1 TC-01..TC-75 tests remain in place.

## 5. Full pytest

PASS: `367 passed in 11.12s`. Output saved to `data/debug/phase4_1_pytest_output.txt`.

## 6. Runtime capture result

The short runtime capture completed and generated all required reports. Phase 4.1 runtime status is FAIL, truthfully, because the capture observed blocker counters.

## 7. Runtime failure reasons

Current runtime failure reasons:

```json
[
  "sequence_gap_count > 0",
  "stale_book_count > 0"
]
```

This is expected after the blocker fix: a report with a real sequence gap or stale period can no longer pass.

## 8. Missing outputs

No required outputs are intentionally missing. Zero-record case files are created as empty files where no cases occurred.
