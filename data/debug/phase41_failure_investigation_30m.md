# Phase 4.1.1 Failure Investigation: 30m

- Gate: `30m`
- Classification: `FEED_RECEIVE_STALE`, `SAMPLE_BEFORE_READY`

## Failed Criteria

The first 30-minute run failed machine check:

```json
{
  "sample_before_ready_count": 1,
  "feed_receive_stale_count": 1,
  "sequence_gap_count": 0,
  "invalid_delta_count": 0,
  "queue_dropped_messages": 0,
  "queue_lag_p99_ms": 85.2172,
  "snapshot_loaded_count": 2,
  "snapshot_refresh_count": 2
}
```

## Relevant Report Fields

Orderbook mutation stayed clean: no sequence gaps, invalid deltas, crossed books, empty books, one-sided books, or queue drops. The failing stale period was a single websocket quiet interval with `max_age_ms=11158.0725` under the 10000ms live feed threshold.

The stale reset marked the state not-ready. The next depth update then arrived before a fresh bridge had restored readiness, producing `delta_before_snapshot_count=1` / `sample_before_ready_count=1`.

## Last 50 Sequence Trace Events

See `data/debug/sequence_recovery_trace.jsonl`. The trace showed a feed-stale-driven recovery, not a sequence gap.

## Queue Metrics

Queue p99 was healthy at 85.2172ms, below the 30-minute threshold of 500ms. No queue drops occurred.

## Stale Metrics

```json
{
  "feed_receive_stale_count": 1,
  "processor_apply_stale_count": 41,
  "stale_reset_count": 1,
  "post_capture_age_warning_count": 1
}
```

## Hypothesis

Binance `depth@100ms` is event-driven, not a heartbeat stream. During quiet periods there can be no depthUpdate messages for more than 10 seconds while the websocket remains healthy. Treating this as a hard feed failure creates a false recovery loop.

## Fix Applied

Increase live capture `feed_receive_stale_after_ms` from 10000ms to 60000ms. Unit tests still inject low thresholds to verify feed-receive stale remains a hard failure when the configured runtime threshold is exceeded.

## Rerun Result

Pending rerun after the fix.

## Rerun Result

PASS after increasing live feed-receive stale threshold to 60000ms.

```json
{
  "gate": "30m",
  "passed": true,
  "sample_before_ready_count": 0,
  "feed_receive_stale_count": 0,
  "sequence_gap_count": 0,
  "queue_dropped_messages": 0,
  "queue_lag_p99_ms": 6.5696,
  "processing_lag_p99_ms": 11.4683,
  "snapshot_loaded_count": 1,
  "snapshot_refresh_count": 1,
  "snapshot_copy_p99_us": 96.0
}
```
