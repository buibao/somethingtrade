# Phase 4.1 Orderbook Quality Report

- Phase 4.1 pass: `True`
- Messages received: 5997
- Messages parsed successfully: 5997
- Deltas accepted: 5995
- Deltas rejected: 2
- Sequence gaps: 1
- Duplicate/old updates skipped: 1
- Samples emitted: 5995
- Samples blocked by ready_to_emit: 0
- Samples blocked by quality error: `{}`
- Strict mismatch count: 0
- Tolerant mismatch count: 0
- Tolerant mode materially reduced mismatch: False
- Top mismatch root causes: see `data/debug/orderbook_mismatch_cases.jsonl`
- Crossed book count: 0
- Stale book count: 0
- Queue backpressure events: 0
- Max queue lag ms: 23.3282
- Snapshot copy p99 us: 94.9 (budget 200.0, met=True)
- Binance lifecycle placeholders unknown: True
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: True

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 1, "duplicate_messages_skipped": 1, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_unpaused_count": 0, "messages_before_ready_count": 0, "queue_backpressure_events": 0, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 829.4489, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 1, "snapshot_failed_count": 0, "snapshot_loaded_count": 2, "snapshot_refresh_count": 2, "state_reset_count": 1}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 23.3282, "enqueue_to_dequeue_lag_ms_p50": 0.3452, "enqueue_to_dequeue_lag_ms_p95": 0.9937, "enqueue_to_dequeue_lag_ms_p99": 4.3998, "max_processing_lag_ms": 0.0, "queue_backpressure_events": 0, "queue_capacity": 1024, "queue_current_size": 0, "queue_dropped_messages": 0, "queue_max_size": 3}`
