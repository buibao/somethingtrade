# Phase 4.1 Orderbook Quality Report

- Phase 4.1 pass: `True`
- Phase 4.1 status: `pass`
- Failure reasons: `[]`
- Messages received: 1200
- Messages parsed successfully: 1200
- Deltas accepted: 1200
- Deltas rejected: 0
- Sequence gaps: 0
- Sequence gap/reset count: 0
- Invalid delta count: 0
- Duplicate/old updates skipped: 0
- Samples emitted: 1200
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
- Processor apply stale count: 0
- Stale reset count: 0
- Post-capture age warning count: 1
- Stale threshold ms: 1000.0
- Feed receive stale threshold ms: 60000.0
- Max book age ms: 1095.4718
- Last book update age ms at report: 1095.4718
- Stale periods: `[]`
- Processor apply stale warnings: `[]`
- Post-capture age warnings: `[{"age_ms": 1095.4718, "event": "post_capture_age_warning", "generation_id": 0, "last_book_update_monotonic_ns": 116089867351900, "last_message_recv_monotonic_ns": 116089867351900, "observed_monotonic_ns": 116090962823700, "ready_to_emit": true, "reason": "book_age_exceeded_threshold_after_capture_end", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Queue backpressure events: 0
- Queue size backpressure events: 0
- Queue lag backpressure events: 0
- Processing lag backpressure events: 0
- Snapshot blocking lag events: 0
- Max queue lag ms: 11.6072
- Queue lag p95 ms: 0.1743
- Queue lag p99 ms: 3.3084
- Processing lag p99 ms: 4.4383
- Snapshot copy p50/p95/p99/max us: 35.8 / 55.7 / 71.7 / 89.3 (samples 1200, budget 200.0, met=True)
- Snapshot copy strategy: top_n_index_slice_immutable_tuples_no_full_book_copy (copied bids=20, asks=20)
- Binance spot market status mode: `not_applicable_for_binance_spot_orderbook`
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: True

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 0, "duplicate_messages_skipped": 0, "feed_receive_stale_count": 0, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_status_mode": "not_applicable_for_binance_spot_orderbook", "market_unpaused_count": 0, "messages_before_ready_count": 0, "post_capture_age_warning_count": 1, "processor_apply_stale_count": 0, "queue_backpressure_events": 0, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 785.3702, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 0, "snapshot_failed_count": 0, "snapshot_loaded_count": 1, "snapshot_refresh_count": 1, "stale_reset_count": 0, "state_reset_count": 0}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 11.6072, "enqueue_to_dequeue_lag_ms_p50": 0.0725, "enqueue_to_dequeue_lag_ms_p95": 0.1743, "enqueue_to_dequeue_lag_ms_p99": 3.3084, "enqueue_to_dequeue_lag_p50_ms": 0.0725, "enqueue_to_dequeue_lag_p95_ms": 0.1743, "enqueue_to_dequeue_lag_p99_ms": 3.3084, "max_processing_lag_ms": 7.8075, "processing_lag_backpressure_events": 0, "processing_lag_p50_ms": 1.9907, "processing_lag_p95_ms": 3.3531, "processing_lag_p99_ms": 4.4383, "queue_backpressure_events": 0, "queue_capacity": 1024, "queue_current_size": 0, "queue_depth_p50": 1.0, "queue_depth_p95": 1.0, "queue_depth_p99": 2.0, "queue_dropped_messages": 0, "queue_lag_backpressure_events": 0, "queue_max_size": 6, "queue_put_block_count": 0, "queue_put_block_p50_ms": 0.0155, "queue_put_block_p95_ms": 0.0214, "queue_put_block_p99_ms": 0.0287, "queue_size_backpressure_events": 0, "snapshot_apply_duration_p50_ms": 14.3072, "snapshot_apply_duration_p95_ms": 14.3072, "snapshot_apply_duration_p99_ms": 14.3072, "snapshot_blocking_lag_events": 0, "snapshot_request_duration_p50_ms": 239.0747, "snapshot_request_duration_p95_ms": 239.0747, "snapshot_request_duration_p99_ms": 239.0747}`
