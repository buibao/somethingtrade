# Phase 4.1 Orderbook Quality Report

- Phase 4.1 pass: `False`
- Phase 4.1 status: `fail`
- Failure reasons: `["sequence_gap_count > 0", "stale_book_count > 0"]`
- Messages received: 296
- Messages parsed successfully: 296
- Deltas accepted: 294
- Deltas rejected: 2
- Sequence gaps: 1
- Sequence gap/reset count: 1
- Invalid delta count: 0
- Duplicate/old updates skipped: 1
- Samples emitted: 294
- Samples blocked by ready_to_emit: 0
- Sample-before-ready count: 0
- ready_to_emit disabled count: 0
- Samples blocked by quality error: `{}`
- Strict mismatch count: 0
- Tolerant mismatch count: 0
- Tolerant mode materially reduced mismatch: False
- Top mismatch root causes: see `data/debug/orderbook_mismatch_cases.jsonl`
- Crossed book count: 0
- Stale book count: 1
- Stale threshold ms: 1000.0
- Max book age ms: 1052.2301
- Last book update age ms at report: 1052.2301
- Stale periods: `[{"case_type": "stale_period", "ended_monotonic_ns": null, "generation_id": 2, "last_book_update_monotonic_ns": 106483110919000, "last_message_recv_monotonic_ns": 106483110919000, "max_age_ms": 1052.2301, "ready_to_emit": false, "reason": "no_successful_book_update", "snapshot_ready": false, "stale_threshold_ms": 1000.0, "started_monotonic_ns": 106484110919000, "symbol": "BTCUSDT"}]`
- Queue backpressure events: 0
- Max queue lag ms: 64.9124
- Snapshot copy p99 us: 95.7 (budget 200.0, met=True)
- Binance lifecycle placeholders unknown: True
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: False

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 1, "duplicate_messages_skipped": 1, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_unpaused_count": 0, "messages_before_ready_count": 0, "queue_backpressure_events": 0, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 1044.2616, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 1, "snapshot_failed_count": 0, "snapshot_loaded_count": 2, "snapshot_refresh_count": 2, "state_reset_count": 1}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 64.9124, "enqueue_to_dequeue_lag_ms_p50": 0.3354, "enqueue_to_dequeue_lag_ms_p95": 0.8573, "enqueue_to_dequeue_lag_ms_p99": 1.6776, "max_processing_lag_ms": 33.5051, "queue_backpressure_events": 0, "queue_capacity": 1024, "queue_current_size": 0, "queue_dropped_messages": 0, "queue_max_size": 1}`
