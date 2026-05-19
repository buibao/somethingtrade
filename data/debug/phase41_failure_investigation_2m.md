# Phase 4.1.1 Failure Investigation: 2m

- Gate: `2m`
- Classification: `FEED_RECEIVE_STALE`, `SAMPLE_BEFORE_READY`

## Failed Criteria

Initial 2-minute run failed with:

```json
{
  "sample_before_ready_count": 5,
  "feed_receive_stale_count": 5,
  "snapshot_loaded_count": 6,
  "sequence_gap_count": 0,
  "queue_lag_p99_ms": 26.7773
}
```

## Relevant Report Fields

The local book mutation path remained clean: no sequence gaps, invalid deltas, crossed/empty/one-sided books, or queue drops. The failure came from quiet websocket intervals just above the 1s book-apply threshold being classified as feed failure. That marked the state not-ready, causing the next valid depth update to arrive while no snapshot was ready and increment `delta_before_snapshot_count` / `sample_before_ready_count`.

## Last Sequence Trace Evidence

See `data/debug/sequence_recovery_trace.jsonl` from the failed run. It showed repeated snapshot reloads without sequence gaps.

## Hypothesis

Binance depth streams are event-driven; a one-second gap in depth updates can be a quiet book, not necessarily a dead feed. Reusing the book-apply stale threshold as the feed-receive stale threshold creates false recovery loops.

## Fix Applied

Added a separate runtime `feed_receive_stale_after_ms` threshold and configured live capture to use 10000ms while keeping unit tests able to inject a lower threshold. Feed-receive stale remains a hard failure if that runtime threshold is exceeded.

## Rerun Result

Pending rerun after the fix.

## Rerun Result

PASS after the threshold split.

```json
{
  "gate": "2m",
  "passed": true,
  "sample_before_ready_count": 0,
  "feed_receive_stale_count": 0,
  "sequence_gap_count": 0,
  "queue_lag_p99_ms": 23.0145,
  "snapshot_loaded_count": 1
}
```
