# Phase 4.1 Orderbook Quality Report

- Phase 4.1 pass: `False`
- Phase 4.1 status: `fail`
- Failure reasons: `["sequence_gap_count > 0"]`
- Messages received: 5996
- Messages parsed successfully: 5996
- Deltas accepted: 5993
- Deltas rejected: 3
- Sequence gaps: 1
- Sequence gap/reset count: 1
- Invalid delta count: 0
- Duplicate/old updates skipped: 2
- Samples emitted: 5993
- Samples blocked by ready_to_emit: 0
- Sample-before-ready count: 0
- ready_to_emit disabled count: 0
- Samples blocked by quality error: `{}`
- Strict mismatch count: 0
- Tolerant mismatch count: 0
- Tolerant mode materially reduced mismatch: False
- Top mismatch root causes: see `data/debug/orderbook_mismatch_cases.jsonl`
- Crossed book count: 0
- Stale book count: 0
- Active-feed stale count: 0
- Post-capture age warning count: 1
- Stale threshold ms: 1000.0
- Max book age ms: 1119.2663
- Last book update age ms at report: 1119.2663
- Stale periods: `[]`
- Post-capture age warnings: `[{"age_ms": 1119.2663, "event": "post_capture_age_warning", "generation_id": 1, "last_book_update_monotonic_ns": 108546791044600, "last_message_recv_monotonic_ns": 108546791044600, "observed_monotonic_ns": 108547910310900, "ready_to_emit": true, "reason": "book_age_exceeded_threshold_after_capture_end", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Queue backpressure events: 0
- Max queue lag ms: 184.3554
- Snapshot copy p99 us: 96.3 (budget 200.0, met=True)
- Binance lifecycle placeholders unknown: True
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: False

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 2, "duplicate_messages_skipped": 2, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_unpaused_count": 0, "messages_before_ready_count": 0, "queue_backpressure_events": 0, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 987.6511, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 1, "snapshot_failed_count": 0, "snapshot_loaded_count": 2, "snapshot_refresh_count": 2, "state_reset_count": 1}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 184.3554, "enqueue_to_dequeue_lag_ms_p50": 0.4066, "enqueue_to_dequeue_lag_ms_p95": 1.4127, "enqueue_to_dequeue_lag_ms_p99": 5.0786, "max_processing_lag_ms": 100.6786, "queue_backpressure_events": 0, "queue_capacity": 1024, "queue_current_size": 1, "queue_dropped_messages": 0, "queue_max_size": 4}`
