from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.research.phase53_dataset_integrity import Phase53Config


def phase53_config(tmp_path: Path) -> Phase53Config:
    root = tmp_path
    return Phase53Config.from_paths(
        repo_root=root,
        phase52_sessions="data/phase_5_2/sessions",
        failed_runs="data/cache/phase_5_2_failed_runs",
        preflight_sessions="data/sessions",
        phase52f_artifacts="artifacts/phase_5_2f",
        backup_meta="backup_meta/destroy_safety_backup_20260530T131704Z",
        output_root="data/phase_5_3",
        strict=True,
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def label_row(*, valid: bool = True, future_leak: bool = False, invalid_reason: str | None = None) -> dict[str, Any]:
    feature_ns = 1_000_000_000
    future_ns = 900_000_000 if future_leak else 1_100_000_000
    feature_ms = 1_700_000_000_000.0
    future_ms = feature_ms - 10.0 if future_leak else feature_ms + 100.0
    return {
        "schema_version": "orderbook_time_protocol_benchmark_v1",
        "symbol": "BTCUSDT",
        "feature_best_bid": 100.0,
        "feature_best_ask": 101.0,
        "feature_mid_price": 100.5,
        "feature_exchange_ts_ms": feature_ms,
        "local_recv_monotonic_ns": feature_ns,
        "protocol_labels": {
            "depth_mid": {
                "hybrid_100ms": {
                    "horizon_ms": 100,
                    "max_future_gap_ms": 100,
                    "feature_lag_budget_ms": 100,
                    "valid": valid,
                    "invalid_reason": invalid_reason,
                    "future_reference_local_recv_monotonic_ns": future_ns,
                    "future_reference_exchange_ts_ms": future_ms,
                    "future_reference_price": 100.6,
                    "reference_source": "depth_mid",
                }
            }
        },
    }


def clean_row(*, receive_ns: int = 1_000_000_000, wall: str = "2026-05-30T00:00:00+00:00", exchange_ns: int = 1_779_926_400_000_000_000) -> dict[str, Any]:
    return {
        "schema_version": "phase_4_1_clean_orderbook_v1",
        "symbol": "BTCUSDT",
        "best_bid": 100.0,
        "best_ask": 101.0,
        "spread": 1.0,
        "mid": 100.5,
        "local_recv_monotonic_ns": receive_ns,
        "local_recv_wall_ts": wall,
        "exchange_event_ts": exchange_ns,
    }


def reference_row() -> dict[str, Any]:
    return {
        "schema_version": "bookticker_reference_v1",
        "symbol": "BTCUSDT",
        "best_bid": 100.0,
        "best_ask": 101.0,
        "price": 100.5,
        "quantity": 1.0,
        "local_recv_monotonic_ns": 1_100_000_000,
    }


def write_good_session(root: Path, session_id: str = "session_001_sanity_30m", *, repaired: bool = False) -> Path:
    session_dir = root / "data/phase_5_2/sessions" / session_id
    dataset = session_dir / "data/dataset"
    debug = session_dir / "data/debug"
    reports = session_dir / "data/reports"
    write_jsonl(dataset / "orderbook_clean_samples.jsonl", [clean_row()])
    write_jsonl(dataset / "bookticker_reference_quotes.jsonl", [reference_row()])
    write_jsonl(dataset / "trade_reference_events.jsonl", [reference_row()])
    write_jsonl(dataset / "aggtrade_reference_events.jsonl", [reference_row()])
    write_jsonl(dataset / "phase_4_2h_corrected_time_protocol_labels.jsonl", [label_row()])
    write_json(debug / "phase_4_2h_queue_backpressure_report.json", {"queue_backpressure_detected": False, "queue_dropped_messages": 0})
    write_json(debug / "phase_4_2h_writer_batch_report.json", {"writer_dropped_records": 0, "writer_error_count": 0, "writer_shutdown_flush_completed": True})
    write_json(debug / "ws_lifecycle_report.json", {"sequence_gap_count": 0, "queue_dropped_messages": 0})
    write_json(debug / "phase_4_2h_clock_sanity_report.json", {"clock_offset_drift_valid": True, "clock_sample_quality_valid": True})
    for name in (
        "duplicate_update_cases.jsonl",
        "sequence_gap_cases.jsonl",
        "stale_period_cases.jsonl",
        "book_incomplete_cases.jsonl",
        "invalid_delta_cases.jsonl",
        "orderbook_mismatch_cases.jsonl",
    ):
        write_jsonl(debug / name, [])
    hotpath = {
        "status": "pass",
        "primary_failure": None,
        "strict_100ms_observability_ready": True,
        "low_latency_ready": True,
        "clock_sync_status": "pass",
        "clock_offset_summary": {"clock_offset_drift_valid": True, "clock_offset_sample_quality_valid": True},
        "evaluation_mode": "existing_artifacts" if repaired else None,
        "derived_artifact_mode": "reuse_existing" if repaired else None,
        "rebuild_derived_artifacts": False if repaired else None,
        "fresh_capture_performed": False if repaired else True,
        "skip_capture": True if repaired else False,
        "capture": {"capture_diagnostics": {"parse_error_count_by_source": {"depth_mid": 0}}},
    }
    write_json(reports / "phase_4_2h_hotpath_environment_latency_report.json", hotpath)
    write_json(session_dir / f"phase_5_2_{session_id}_metadata.json", {"session_id": session_id, "runtime_status": "pass", "research_eligible": True})
    write_json(session_dir / f"phase_5_2_{session_id}_quality_report.json", {"status": "pass", "research_eligible": True, "bundle_sha256_valid": True})
    (session_dir / f"phase_5_2_{session_id}_console.log").write_text("ok\n", encoding="utf-8")
    return session_dir
