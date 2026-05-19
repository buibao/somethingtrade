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
- Processor apply stale count: 16
- Stale reset count: 0
- Post-capture age warning count: 1
- Stale threshold ms: 1000.0
- Feed receive stale threshold ms: 60000.0
- Max book age ms: 2596.9951
- Last book update age ms at report: 1086.1766
- Stale periods: `[]`
- Processor apply stale warnings: `[{"apply_age_ms": 1027.4273, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1027.4273, "observed_monotonic_ns": 9552974874200, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 2133.5154, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 2133.5154, "observed_monotonic_ns": 9622460214200, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1009.6482, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1009.6482, "observed_monotonic_ns": 10069031443000, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1183.9045, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1183.9045, "observed_monotonic_ns": 10072911500500, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1558.0451, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1558.0451, "observed_monotonic_ns": 10092437077800, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1104.2087, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1104.2087, "observed_monotonic_ns": 10093565611900, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 2596.9951, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 2596.9951, "observed_monotonic_ns": 10193844309700, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1294.1714, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1294.1714, "observed_monotonic_ns": 10195272795300, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1313.6986, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1313.6986, "observed_monotonic_ns": 10259979859900, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1871.1092, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1871.1092, "observed_monotonic_ns": 10334814686200, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1053.6632, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1053.6632, "observed_monotonic_ns": 10339483900400, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1135.4044, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1135.4044, "observed_monotonic_ns": 10341616813600, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1135.2733, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1135.2733, "observed_monotonic_ns": 10343969871200, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 2263.2383, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 2263.2383, "observed_monotonic_ns": 10392745992600, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1079.0061, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1079.0061, "observed_monotonic_ns": 10454562577500, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}, {"apply_age_ms": 1629.495, "case_type": "processor_apply_stale", "consecutive_checks": 1, "feed_receive_age_ms": 0.0, "generation_id": 0, "max_apply_age_ms": 1629.495, "observed_monotonic_ns": 11039243139000, "queue_size": 0, "ready_to_emit": true, "reason": "websocket_messages_arriving_but_no_successful_apply", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Post-capture age warnings: `[{"age_ms": 1086.1766, "event": "post_capture_age_warning", "generation_id": 0, "last_book_update_monotonic_ns": 11333303978100, "last_message_recv_monotonic_ns": 11333303978100, "observed_monotonic_ns": 11334390154700, "ready_to_emit": true, "reason": "book_age_exceeded_threshold_after_capture_end", "snapshot_ready": true, "stale_threshold_ms": 1000.0, "symbol": "BTCUSDT"}]`
- Queue backpressure events: 37
- Queue size backpressure events: 0
- Queue lag backpressure events: 0
- Processing lag backpressure events: 36
- Snapshot blocking lag events: 1
- Max queue lag ms: 124.0929
- Queue lag p95 ms: 1.0996
- Queue lag p99 ms: 6.5696
- Processing lag p99 ms: 11.4683
- Snapshot copy p99 us: 96.0 (budget 200.0, met=True)
- Binance spot market status mode: `not_applicable_for_binance_spot_orderbook`
- Clean sample schema: `phase_4_1_clean_orderbook_v1`
- Dataset clean enough for Phase 4.2: True

## Lifecycle

`{"connect_count": 1, "delta_before_snapshot_count": 0, "disconnect_count": 1, "duplicate_messages_detected": 0, "duplicate_messages_skipped": 0, "market_paused_count": 0, "market_resolved_count": 0, "market_status_known": false, "market_status_mode": "not_applicable_for_binance_spot_orderbook", "market_unpaused_count": 0, "messages_before_ready_count": 0, "queue_backpressure_events": 36, "queue_dropped_messages": 0, "ready_to_emit_false_duration_ms_max": 1436.1053, "ready_to_emit_false_warning_count": 0, "reconnect_count": 0, "resubscribe_count": 0, "sample_blocked_by_ready_guard": 0, "sequence_gap_count": 0, "snapshot_failed_count": 0, "snapshot_loaded_count": 1, "snapshot_refresh_count": 1, "state_reset_count": 0}`

## Queue

`{"enqueue_to_dequeue_lag_ms_max": 124.0929, "enqueue_to_dequeue_lag_ms_p50": 0.2873, "enqueue_to_dequeue_lag_ms_p95": 1.0996, "enqueue_to_dequeue_lag_ms_p99": 6.5696, "enqueue_to_dequeue_lag_p50_ms": 0.2873, "enqueue_to_dequeue_lag_p95_ms": 1.0996, "enqueue_to_dequeue_lag_p99_ms": 6.5696, "max_processing_lag_ms": 110.4736, "processing_lag_backpressure_events": 36, "processing_lag_p50_ms": 5.7227, "processing_lag_p95_ms": 8.5086, "processing_lag_p99_ms": 11.4683, "queue_backpressure_events": 37, "queue_capacity": 1024, "queue_current_size": 0, "queue_dropped_messages": 0, "queue_lag_backpressure_events": 0, "queue_max_size": 26, "queue_size_backpressure_events": 0, "snapshot_apply_duration_p50_ms": 12.2262, "snapshot_apply_duration_p95_ms": 12.2262, "snapshot_apply_duration_p99_ms": 12.2262, "snapshot_blocking_lag_events": 1, "snapshot_request_duration_p50_ms": 545.2526, "snapshot_request_duration_p95_ms": 545.2526, "snapshot_request_duration_p99_ms": 545.2526}`
