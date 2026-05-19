# Phase 4.1 Orderbook Quality Report

- Phase 4.1 pass: `True`
- Phase 4.1 status: `pass`
- Failure reasons: `[]`
- Messages received: 18000
- Messages parsed successfully: 18000
- Deltas accepted: 18000
- Deltas rejected: 0
- Sequence gaps: 0
- Sequence gap/reset count: 0
- Invalid delta count: 0
- Duplicate/old updates skipped: 0
- Samples emitted: 18000
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
- Feed receive stale count: 0
- Processor apply stale count: 1
- Stale reset count: 0
- Post-capture age warning count: 1
- Stale threshold ms: 1000.0
- Feed receive stale threshold ms: 60000.0
- Max book age ms: 1548.9514
- Last book update age ms at report: 1084.282
- Stale periods: `[]`
- Processor apply stale warnings: `[{"apply_age_ms": 1548.9514, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1548.9514, "observed_monotonic_ns": 6563772643300, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Post-capture age warnings: `[{"age_ms": 1084.282, "event": "post_capture_age_warning", "generation_id": 0, "last_book_update_monotonic_ns": 6708820763500, "last_message_recv_monotonic_ns": 6708820763500, "observed_monotonic_ns": 6709905045500, "ready_to_emit": true, "reason": "book_age_exceeded_threshold_after_capture_end", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Queue backpressure events: 49
- Queue size backpressure events: 0
- Queue lag backpressure events: 0
- Processing lag backpressure events: 49
- Snapshot blocking lag events: 0
- Max queue lag ms: 147.0124
- Queue lag p95 ms: 10.0405
- Queue lag p99 ms: 26.548
- Processing lag p99 ms: 15.827
- Snapshot copy p99 us: 159.8 (budget 200.0, met=True)
- Binance spot market status mode: `not_applicable_for_binance_spot_orderbook`
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: True

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 0, "duplicate_messages_skipped": 0, "feed_receive_stale_count": 0, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_status_mode": "not_applicable_for_binance_spot_orderbook", "market_unpaused_count": 0, "messages_before_ready_count": 0, "post_capture_age_warning_count": 1, "processor_apply_stale_count": 1, "queue_backpressure_events": 49, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 917.5515, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 0, "snapshot_failed_count": 0, "snapshot_loaded_count": 1, "snapshot_refresh_count": 1, "stale_reset_count": 0, "state_reset_count": 0}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 147.0124, "enqueue_to_dequeue_lag_ms_p50": 0.33, "enqueue_to_dequeue_lag_ms_p95": 10.0405, "enqueue_to_dequeue_lag_ms_p99": 26.548, "enqueue_to_dequeue_lag_p50_ms": 0.33, "enqueue_to_dequeue_lag_p95_ms": 10.0405, "enqueue_to_dequeue_lag_p99_ms": 26.548, "max_processing_lag_ms": 112.0954, "processing_lag_backpressure_events": 49, "processing_lag_p50_ms": 6.8003, "processing_lag_p95_ms": 12.2781, "processing_lag_p99_ms": 15.827, "queue_backpressure_events": 49, "queue_capacity": 1024, "queue_current_size": 0, "queue_dropped_messages": 0, "queue_lag_backpressure_events": 0, "queue_max_size": 16, "queue_size_backpressure_events": 0, "snapshot_apply_duration_p50_ms": 14.3329, "snapshot_apply_duration_p95_ms": 14.3329, "snapshot_apply_duration_p99_ms": 14.3329, "snapshot_blocking_lag_events": 0, "snapshot_request_duration_p50_ms": 239.2905, "snapshot_request_duration_p95_ms": 239.2905, "snapshot_request_duration_p99_ms": 239.2905}`
