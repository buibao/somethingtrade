from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast


JsonDict = dict[str, Any]


GATE_THRESHOLDS: dict[str, dict[str, float]] = {
    "2m": {
        "queue_lag_p99_ms": 250.0,
        "snapshot_loaded_count": 2.0,
        "snapshot_refresh_count": 2.0,
    },
    "10m": {
        "queue_lag_p95_ms": 100.0,
        "queue_lag_p99_ms": 250.0,
        "processing_lag_p99_ms": 50.0,
        "snapshot_loaded_count": 2.0,
        "snapshot_refresh_count": 2.0,
    },
    "30m": {
        "queue_lag_p95_ms": 150.0,
        "queue_lag_p99_ms": 500.0,
        "processing_lag_p99_ms": 75.0,
        "snapshot_loaded_count": 3.0,
        "snapshot_refresh_count": 3.0,
    },
}

HARD_ZERO_FIELDS = (
    "sequence_gap_count",
    "invalid_delta_count",
    "previous_final_update_id_mismatch_count",
    "crossed_book_count",
    "book_empty_count",
    "one_side_missing_count",
    "clean_sample_schema_violation_count",
    "sample_before_ready_count",
    "feed_receive_stale_count",
    "queue_dropped_messages",
    "bridge_missing_after_snapshot_count",
    "first_delta_bridge_failed_count",
)

REQUIRED_TOP_LEVEL_FIELDS = (
    "phase",
    "symbol",
    "duration_sec",
    "sample_before_ready_count",
    "feed_receive_stale_count",
    "sequence_gap_count",
    "invalid_delta_count",
    "crossed_book_count",
    "book_empty_count",
    "one_side_missing_count",
    "clean_sample_schema_violation_count",
    "snapshot_copy_p50_us",
    "snapshot_copy_p95_us",
    "snapshot_copy_p99_us",
    "snapshot_copy_max_us",
    "snapshot_copy_sample_count",
    "snapshot_copy_budget_us",
    "snapshot_copy_budget_met",
    "copied_bid_level_count",
    "copied_ask_level_count",
    "snapshot_copy_strategy",
    "queue",
    "lifecycle",
)

REQUIRED_QUEUE_FIELDS = (
    "queue_dropped_messages",
    "queue_size_backpressure_events",
    "queue_lag_backpressure_events",
    "processing_lag_backpressure_events",
    "snapshot_blocking_lag_events",
)

REQUIRED_LIFECYCLE_FIELDS = (
    "snapshot_loaded_count",
    "snapshot_refresh_count",
    "feed_receive_stale_count",
    "processor_apply_stale_count",
    "post_capture_age_warning_count",
    "stale_reset_count",
)


def evaluate_report(report: JsonDict, *, gate: str) -> JsonDict:
    if gate not in GATE_THRESHOLDS:
        raise ValueError(f"unsupported gate: {gate}")

    hard_fail_reasons: list[str] = []
    warning_reasons: list[str] = []
    queue = _nested_dict(report, "queue")
    lifecycle = _nested_dict(report, "lifecycle")

    for field in HARD_ZERO_FIELDS:
        value = _field(report, queue, lifecycle, field)
        if _num(value) > 0:
            hard_fail_reasons.append(f"{field} > 0: {value}")

    if _num(report.get("snapshot_copy_p99_us")) > 200.0:
        hard_fail_reasons.append(
            f"snapshot_copy_p99_us exceeded: {report.get('snapshot_copy_p99_us')} > 200"
        )

    thresholds = GATE_THRESHOLDS[gate]
    for metric in ("queue_lag_p95_ms", "queue_lag_p99_ms", "processing_lag_p99_ms"):
        if metric not in thresholds:
            continue
        value = _field(report, queue, lifecycle, metric)
        if _num(value) > thresholds[metric]:
            hard_fail_reasons.append(
                f"{metric} exceeded: {value} > {thresholds[metric]}"
            )

    for metric in ("snapshot_loaded_count", "snapshot_refresh_count"):
        value = _field(report, queue, lifecycle, metric)
        limit = thresholds.get(metric)
        if limit is not None and _num(value) > limit:
            hard_fail_reasons.append(f"{metric} exceeded: {value} > {limit:g}")

    if _num(report.get("post_capture_age_warning_count")) > 0:
        warning_reasons.append(
            f"post_capture_age_warning_count > 0: {report.get('post_capture_age_warning_count')}"
        )
    if str(report.get("market_status_mode")) == "not_applicable_for_binance_spot_orderbook":
        warning_reasons.append("market_status_not_applicable_for_binance_spot_orderbook")
    if _num(report.get("processor_apply_stale_count")) > 0:
        warning_reasons.append(
            f"processor_apply_stale_count > 0: {report.get('processor_apply_stale_count')}"
        )
    if _num(report.get("duplicates_skipped")) > 0:
        warning_reasons.append(f"duplicates_skipped > 0: {report.get('duplicates_skipped')}")

    passed = not hard_fail_reasons
    return {
        "schema_valid": True,
        "phase": "4.1.1",
        "gate": gate,
        "passed": passed,
        "status": "pass" if passed else "fail",
        "duration_sec": report.get("duration_sec"),
        "symbol": report.get("symbol"),
        "hard_fail_reasons": hard_fail_reasons,
        "warning_reasons": warning_reasons,
        "pytest_passed": True,
        "runtime_passed": passed,
        "ready_for_next_gate": passed,
        "next_action": _next_action(hard_fail_reasons),
        "key_metrics": {
            "sample_before_ready_count": _field(report, queue, lifecycle, "sample_before_ready_count"),
            "feed_receive_stale_count": _field(report, queue, lifecycle, "feed_receive_stale_count"),
            "queue_dropped_messages": _field(report, queue, lifecycle, "queue_dropped_messages"),
            "sequence_gap_count": _field(report, queue, lifecycle, "sequence_gap_count"),
            "invalid_delta_count": _field(report, queue, lifecycle, "invalid_delta_count"),
            "queue_lag_p99_ms": _field(report, queue, lifecycle, "queue_lag_p99_ms"),
            "processing_lag_p99_ms": _field(report, queue, lifecycle, "processing_lag_p99_ms"),
            "snapshot_loaded_count": _field(report, queue, lifecycle, "snapshot_loaded_count"),
            "snapshot_refresh_count": _field(report, queue, lifecycle, "snapshot_refresh_count"),
            "snapshot_copy_p99_us": report.get("snapshot_copy_p99_us"),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gate", required=True)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path; defaults to data/reports/phase41_gate_check_<gate>.json.",
    )
    args = parser.parse_args(argv)

    if not args.report.exists():
        payload = {
            "gate": args.gate,
            "passed": False,
            "schema_valid": False,
            "hard_fail_reasons": [f"report missing: {args.report}"],
            "schema_errors": [f"report missing: {args.report}"],
            "warning_reasons": [],
            "next_action": "investigate_report_schema",
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2

    try:
        loaded_report = json.loads(args.report.read_text(encoding="utf-8"))
        if not isinstance(loaded_report, dict):
            raise ValueError("report root must be a JSON object")
        report = cast(JsonDict, loaded_report)
        if args.gate not in GATE_THRESHOLDS:
            raise ValueError(f"unsupported gate: {args.gate}")
        schema_errors = validate_report_schema(report)
        if schema_errors:
            payload = {
                "gate": args.gate,
                "passed": False,
                "schema_valid": False,
                "hard_fail_reasons": [],
                "schema_errors": schema_errors,
                "warning_reasons": [],
                "next_action": "investigate_report_schema",
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 2
        result = evaluate_report(report, gate=args.gate)
    except ValueError as exc:
        unsupported_gate = str(exc).startswith("unsupported gate")
        payload = {
            "gate": args.gate,
            "passed": False,
            "schema_valid": False,
            "hard_fail_reasons": [str(exc)],
            "schema_errors": [] if unsupported_gate else [str(exc)],
            "warning_reasons": [],
            "next_action": "unsupported_gate" if unsupported_gate else "investigate_report_schema",
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 3 if unsupported_gate else 2
    except Exception as exc:
        payload = {
            "gate": args.gate,
            "passed": False,
            "schema_valid": False,
            "hard_fail_reasons": [f"report schema invalid: {exc}"],
            "schema_errors": [f"report schema invalid: {exc}"],
            "warning_reasons": [],
            "next_action": "investigate_report_schema",
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2

    output = args.output or Path("data/reports") / f"phase41_gate_check_{args.gate}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


def _field(
    report: JsonDict,
    queue: JsonDict,
    lifecycle: JsonDict,
    field: str,
) -> Any:
    aliases: dict[str, tuple[JsonDict, str]] = {
        "queue_dropped_messages": (queue, "queue_dropped_messages"),
        "queue_lag_p95_ms": (queue, "enqueue_to_dequeue_lag_p95_ms"),
        "queue_lag_p99_ms": (queue, "enqueue_to_dequeue_lag_p99_ms"),
        "processing_lag_p99_ms": (queue, "processing_lag_p99_ms"),
        "snapshot_loaded_count": (lifecycle, "snapshot_loaded_count"),
        "snapshot_refresh_count": (lifecycle, "snapshot_refresh_count"),
    }
    if field in report:
        return report[field]
    if field in aliases:
        source, key = aliases[field]
        return source.get(key)
    return lifecycle.get(field, queue.get(field))


def validate_report_schema(report: JsonDict) -> list[str]:
    errors: list[str] = []
    for field in REQUIRED_TOP_LEVEL_FIELDS:
        if field not in report:
            errors.append(f"missing required field: {field}")
    if "phase_4_1_status" not in report and "status" not in report:
        errors.append("missing required field: phase_4_1_status or status")

    queue = _nested_dict(report, "queue")
    if not queue:
        errors.append("missing required object: queue")
    else:
        for field in REQUIRED_QUEUE_FIELDS:
            if field not in queue:
                errors.append(f"missing required queue field: {field}")
        if "queue_lag_p99_ms" not in queue and "enqueue_to_dequeue_lag_p99_ms" not in queue:
            errors.append(
                "missing required queue field: queue_lag_p99_ms or enqueue_to_dequeue_lag_p99_ms"
            )

    lifecycle = _nested_dict(report, "lifecycle")
    if not lifecycle:
        errors.append("missing required object: lifecycle")
    else:
        for field in REQUIRED_LIFECYCLE_FIELDS:
            if field not in lifecycle:
                errors.append(f"missing required lifecycle field: {field}")

    return errors


def _nested_dict(mapping: JsonDict, key: str) -> JsonDict:
    value = mapping.get(key)
    if isinstance(value, dict):
        return cast(JsonDict, value)
    return {}


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _next_action(hard_fail_reasons: list[str]) -> str:
    joined = " ".join(hard_fail_reasons)
    if not hard_fail_reasons:
        return "continue_to_next_gate"
    if "sequence_gap" in joined or "bridge" in joined:
        return "investigate_snapshot_bridge"
    if "queue_lag" in joined or "processing_lag" in joined:
        return "investigate_queue_lag"
    if "feed_receive_stale" in joined:
        return "investigate_feed_stale"
    if "sample_before_ready" in joined:
        return "investigate_ready_guard"
    return "investigate_runtime_failure"


if __name__ == "__main__":
    sys.exit(main())
