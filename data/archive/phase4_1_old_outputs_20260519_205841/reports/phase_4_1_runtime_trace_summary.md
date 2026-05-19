# Phase 4.1 Runtime Trace Summary

## Runtime result

- Capture: BTCUSDT, 600 seconds requested, 601.3107497 seconds observed.
- Phase 4.1 empirical pass: `False`
- Failure reasons: `["sequence_gap_count > 0"]`
- Messages received/parsed: 5996 / 5996
- Deltas accepted/rejected: 5993 / 3
- Samples emitted/blocked: 5993 / 0

## 1. Did sequence_gap_count occur?

Yes. `sequence_gap_count=1` and `sequence_gap_or_reset_count=1`.

The trace shows the gap occurred immediately after the initial snapshot, before runtime readiness was restored:

- Initial snapshot `lastUpdateId`: 93912030557
- Expected next update ID: 93912030558
- First received update ID: 93912030873
- Final received update ID: 93912030947
- Gap size: 315
- Queue size at gap: 0
- Updates processed since snapshot: 0

## 2. What does sequence_recovery_trace show?

`data/debug/sequence_recovery_trace.jsonl` contains:

- `snapshot_requested`: 1
- `snapshot_loaded`: 1
- `sequence_gap_detected`: 1
- `recovery_snapshot_requested`: 1
- `recovery_snapshot_loaded`: 1
- `recovery_ready_restored`: 1
- `post_snapshot_update_range`: 21

The recovery snapshot loaded at generation 1 with `snapshot_last_update_id=93912031168`. Two queued old/duplicate updates were skipped, then update range `U=93912031169`, `u=93912031264` bridged the recovery snapshot and restored `ready_to_emit=true`.

## 3. Did active_feed_stale_count occur?

No. `active_feed_stale_count=0` and `stale_periods=[]`.

## 4. Did post_capture_age_warning_count occur?

Yes. `post_capture_age_warning_count=1`.

This warning was recorded after capture shutdown/report generation with `age_ms=1119.2663`, `snapshot_ready=true`, and `ready_to_emit=true`. It is not a Phase 4.1 blocker by itself.

## 5. Did readiness recover after any gap?

Yes. The trace includes `recovery_ready_restored` after a fresh recovery snapshot and a valid bridge update. Lifecycle also reports `snapshot_loaded_count=2`, `snapshot_refresh_count=2`, and `state_reset_count=1`.

## 6. Is Phase 4.1 empirically pass or fail?

FAIL.

## 7. Exact failure reasons

```json
[
  "sequence_gap_count > 0"
]
```

Phase 4.1 remains blocked from Phase 4.2 because a real sequence gap occurred during live capture. The stale diagnosis is now separated: no active-feed stale failure occurred in this run.
