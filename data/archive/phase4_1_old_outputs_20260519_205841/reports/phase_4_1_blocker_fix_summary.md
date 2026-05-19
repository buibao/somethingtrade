# Phase 4.1 Blocker Fix Summary

## 1. Pass/fail evaluation

Added a central `evaluate_phase_4_1_pass(report)` evaluator. Reports now include `phase_4_1_pass`, `phase_4_1_status`, and explicit `phase_4_1_failure_reasons`. Sequence gaps, sequence gap/reset aliases, invalid deltas, sample-before-ready, crossed books, active-feed stale periods, emitted-clean schema violations, ready guard violations, queue drops/backpressure, and snapshot budget failure are treated as blockers.

## 2. Stale detection

Separated message receive time from successful book update time. `last_book_update_monotonic_ns` and `last_successful_apply_monotonic_ns` only update on successful snapshot/delta application. Rejected, invalid, old, and duplicate deltas do not refresh successful book timestamps.

Stale classification is now split between `active_feed_stale_count` and `post_capture_age_warning_count`. Active-feed stale periods are fail-closed blockers and are written to `data/debug/stale_period_cases.jsonl`. Post-capture/report-time age warnings remain visible in the JSON/Markdown report but do not fail Phase 4.1 by themselves.

## 3. Invalid delta fail-closed

Expected-sequence deltas with invalid price/size, NaN, infinity, or malformed levels now call `mark_not_ready("invalid_delta_fail_closed")`, set `snapshot_ready=false`, set `ready_to_emit=false`, increment `invalid_delta_count`, write `data/debug/invalid_delta_cases.jsonl`, and require a fresh valid snapshot before recovery. Old duplicate invalid updates remain safely skipped and do not disable readiness.

## 4. Tests added or updated

Added focused blocker regressions in `tests/test_orderbook_phase41_blockers.py` for pass/fail semantics, monotonic stale detection, active-vs-post-capture stale classification, invalid delta fail-closed, duplicate safety, fresh snapshot recovery, sequence recovery tracing, and report failure reasons. Existing Phase 4.1 TC-01..TC-75 tests remain in place.

## 5. Full pytest

PASS: `371 passed in 9.77s`. Output saved to `data/debug/phase4_1_pytest_output.txt`.

## 6. Runtime capture result

The 600-second BTCUSDT runtime capture completed and generated all required reports. Phase 4.1 runtime status is FAIL, truthfully, because the capture observed a real sequence gap. It did not observe active-feed stale; it observed one post-capture age warning during shutdown/report generation.

## 7. Runtime failure reasons

Current runtime failure reasons:

```json
[
  "sequence_gap_count > 0"
]
```

This is expected after the blocker fix: a report with a real sequence gap can no longer pass. A post-capture age warning alone is diagnostic and non-blocking.

## 8. Missing outputs

No required outputs are intentionally missing. Zero-record case files are created as empty files where no cases occurred.
