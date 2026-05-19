from __future__ import annotations

from collections import Counter
import json
import math
from pathlib import Path
import shutil
from typing import Any
import zipfile

from app.research.orderbook_labeled_dataset import (
    HORIZONS,
    MAX_FUTURE_GAP_MS,
    REQUIRED_VALID_LABEL_FIELDS,
    generate_labeled_rows,
    run_leakage_check,
    validate_clean_samples,
    validate_labeled_rows,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
REQUIRED_100MS_MAX_FUTURE_GAP_MS = 100
REQUIRED_100MS_VALID_RATE = 0.95
SAMPLE_GAP_P95_MAX_MS = 100.0
SAMPLE_GAP_P99_MAX_MS = 200.0
LARGE_GAP_THRESHOLD_MS = 200.0
PHASE42A_REPORT_JSON = Path("data/reports/phase_4_2a_100ms_coverage_report.json")
PHASE42A_REPORT_MD = Path("data/reports/phase_4_2a_100ms_coverage_report.md")
PHASE42A_SELF_CHECK_JSON = Path("data/reports/phase42a_self_check.json")
PHASE42A_GAP_DISTRIBUTION = Path("data/debug/phase_4_2a_sample_gap_distribution.json")
PHASE42A_INVALID_CASES = Path("data/debug/phase_4_2a_100ms_invalid_cases.jsonl")
PHASE42A_COVERAGE_SUMMARY = Path("data/debug/phase_4_2a_coverage_summary.json")
PHASE42A_PYTEST_OUTPUT = Path("data/debug/phase_4_2a_pytest_output.txt")
PHASE42A_INVESTIGATION = Path("data/debug/phase42a_failure_investigation.md")
PHASE42A_BUNDLE = Path("phase_4_2a_100ms_coverage_pass_bundle.zip")

PHASE42A_REQUIRED_REPORT_FIELDS = frozenset(
    {
        "phase",
        "symbol",
        "status",
        "implementation_status",
        "runtime_status",
        "dataset_coverage_status",
        "definition_of_done_status",
        "primary_failure",
        "input_paths",
        "capture",
        "runtime_quality",
        "timestamp_quality",
        "horizon_100ms",
        "leakage_check",
        "hard_fail_reasons",
        "warning_reasons",
    }
)

PHASE42A_REQUIRED_BUNDLE_FILES = (
    "app/",
    "tests/",
    "scripts/",
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/orderbook_labeled_samples.jsonl",
    "data/reports/phase_4_2a_100ms_coverage_report.json",
    "data/reports/phase_4_2a_100ms_coverage_report.md",
    "data/reports/phase42a_self_check.json",
    "data/debug/phase_4_2a_sample_gap_distribution.json",
    "data/debug/phase_4_2a_100ms_invalid_cases.jsonl",
    "data/debug/phase_4_2a_coverage_summary.json",
    "data/debug/phase_4_2a_pytest_output.txt",
)

RUNTIME_HARD_ZERO_FIELDS = (
    "sample_before_ready_count",
    "feed_receive_stale_count",
    "queue_dropped_messages",
    "sequence_gap_count",
    "invalid_delta_count",
    "crossed_book_count",
    "book_empty_count",
    "one_side_missing_count",
    "clean_sample_schema_violation_count",
)


def analyze_sample_gap_distribution(samples: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [
        int(sample["local_recv_monotonic_ns"])
        for sample in samples
        if isinstance(sample.get("local_recv_monotonic_ns"), int)
    ]
    gaps = [
        (timestamps[index] - timestamps[index - 1]) / 1_000_000.0
        for index in range(1, len(timestamps))
    ]
    non_negative_gaps = [gap for gap in gaps if gap >= 0]
    duplicate_count = sum(1 for gap in gaps if gap == 0)
    monotonic_violations = sum(1 for gap in gaps if gap < 0)
    mean_gap = (
        sum(non_negative_gaps) / len(non_negative_gaps)
        if non_negative_gaps
        else None
    )
    return {
        "gap_count": len(gaps),
        "gap_mean_ms": mean_gap,
        "gap_p50_ms": _percentile(non_negative_gaps, 0.50),
        "gap_p90_ms": _percentile(non_negative_gaps, 0.90),
        "gap_p95_ms": _percentile(non_negative_gaps, 0.95),
        "sample_gap_p95_ms": _percentile(non_negative_gaps, 0.95),
        "gap_p99_ms": _percentile(non_negative_gaps, 0.99),
        "sample_gap_p99_ms": _percentile(non_negative_gaps, 0.99),
        "gap_max_ms": max(non_negative_gaps) if non_negative_gaps else None,
        "large_gap_count": sum(1 for gap in non_negative_gaps if gap > LARGE_GAP_THRESHOLD_MS),
        "large_gap_threshold_ms": LARGE_GAP_THRESHOLD_MS,
        "timestamp_monotonic_violations": monotonic_violations,
        "duplicate_timestamp_count": duplicate_count,
    }


def extract_horizon_100ms_coverage(
    clean_samples: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    *,
    invalid_cases_path: str | Path | None = None,
) -> dict[str, Any]:
    last_ts = (
        int(clean_samples[-1]["local_recv_monotonic_ns"])
        if clean_samples
        else None
    )
    eligible_count = 0
    if last_ts is not None:
        eligible_count = sum(
            1
            for sample in clean_samples
            if int(sample["local_recv_monotonic_ns"]) + 100_000_000 <= last_ts
        )
    valid_count = 0
    invalid_count = 0
    tail_no_future_count = 0
    invalid_reason_counts: Counter[str] = Counter()
    future_gaps: list[float] = []
    invalid_cases: list[dict[str, Any]] = []
    for row in labeled_rows:
        label = row.get("labels", {}).get("horizon_100ms")
        if not isinstance(label, dict):
            invalid_count += 1
            invalid_reason_counts["MISSING_LABEL"] += 1
            continue
        if isinstance(label.get("future_gap_ms"), (int, float)):
            future_gaps.append(float(label["future_gap_ms"]))
        if label.get("valid") is True:
            valid_count += 1
            continue
        invalid_count += 1
        reason = str(label.get("invalid_reason") or "UNKNOWN_INVALID_REASON")
        invalid_reason_counts[reason] += 1
        target_ts = label.get("target_local_recv_monotonic_ns")
        if (
            reason == "NO_FUTURE_SAMPLE"
            and isinstance(target_ts, int)
            and last_ts is not None
            and target_ts > last_ts
        ):
            tail_no_future_count += 1
        invalid_cases.append(
            {
                "symbol": row.get("symbol"),
                "generation_id": row.get("generation_id"),
                "last_update_id": row.get("last_update_id"),
                "local_recv_monotonic_ns": row.get("local_recv_monotonic_ns"),
                "horizon": "horizon_100ms",
                "invalid_reason": reason,
                "target_local_recv_monotonic_ns": target_ts,
                "future_local_recv_monotonic_ns": label.get(
                    "future_local_recv_monotonic_ns"
                ),
                "future_gap_ms": label.get("future_gap_ms"),
            }
        )
    if invalid_cases_path is not None:
        write_jsonl(invalid_cases_path, invalid_cases)
    total_rows = len(labeled_rows)
    return {
        "max_future_gap_ms": MAX_FUTURE_GAP_MS.get("horizon_100ms"),
        "eligible_count": eligible_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "tail_no_future_count": tail_no_future_count,
        "valid_rate_all_rows": valid_count / total_rows if total_rows else 0.0,
        "valid_rate_eligible_rows": valid_count / eligible_count if eligible_count else 0.0,
        "invalid_reason_counts": dict(sorted(invalid_reason_counts.items())),
        "future_gap_ms_p50": _percentile(future_gaps, 0.50),
        "future_gap_ms_p90": _percentile(future_gaps, 0.90),
        "future_gap_ms_p95": _percentile(future_gaps, 0.95),
        "future_gap_ms_p99": _percentile(future_gaps, 0.99),
        "future_gap_ms_max": max(future_gaps) if future_gaps else None,
    }


def build_phase42a_report(
    *,
    symbol: str,
    clean_samples: list[dict[str, Any]],
    labeled_rows: list[dict[str, Any]],
    leakage_result: dict[str, Any],
    runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
    input_clean_path: str | Path = "data/dataset/orderbook_clean_samples.jsonl",
    labeled_path: str | Path = "data/dataset/orderbook_labeled_samples.jsonl",
    invalid_cases_path: str | Path | None = None,
) -> dict[str, Any]:
    timestamp_quality = analyze_sample_gap_distribution(clean_samples)
    horizon_100ms = extract_horizon_100ms_coverage(
        clean_samples,
        labeled_rows,
        invalid_cases_path=invalid_cases_path,
    )
    report = {
        "phase": "4.2A",
        "symbol": symbol,
        "status": "pass",
        "implementation_status": "pass",
        "runtime_status": "pass",
        "dataset_coverage_status": "pass",
        "definition_of_done_status": "pass",
        "primary_failure": None,
        "input_paths": {
            "clean_samples": _display_path(input_clean_path),
            "labeled_samples": _display_path(labeled_path),
        },
        "capture": {
            "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
            "fixture_mode": bool(capture.get("fixture_mode", False)),
            "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
            "sample_count": int(capture.get("sample_count", len(clean_samples)) or 0),
            "sample_rate_per_sec": float(
                capture.get("sample_rate_per_sec", _sample_rate(clean_samples)) or 0.0
            ),
            "downsampling_enabled": bool(capture.get("downsampling_enabled", False)),
            "emits_every_accepted_delta": bool(
                capture.get("emits_every_accepted_delta", True)
            ),
            "sample_source": str(capture.get("sample_source", "accepted_depth_delta")),
        },
        "capture_policy": {
            "downsampling_enabled": bool(capture.get("downsampling_enabled", False)),
            "emits_every_accepted_delta": bool(
                capture.get("emits_every_accepted_delta", True)
            ),
            "sample_source": str(capture.get("sample_source", "accepted_depth_delta")),
        },
        "fresh_capture_required": fresh_capture_required,
        "runtime_quality": _normalize_runtime_quality(runtime_quality),
        "timestamp_quality": timestamp_quality,
        "horizon_100ms": horizon_100ms,
        "leakage_check": {
            "passed": bool(leakage_result.get("passed", False)),
            "feature_leakage_violations": int(
                leakage_result.get("feature_leakage_violations", 0) or 0
            ),
            "label_leakage_violations": int(
                leakage_result.get("label_leakage_violations", 0) or 0
            ),
            "violations": leakage_result.get("violations", []),
        },
        "clean_sample_count": len(clean_samples),
        "labeled_sample_count": len(labeled_rows),
        "hard_fail_reasons": [],
        "warning_reasons": [],
        "bottleneck_assessment": _bottleneck_assessment(timestamp_quality, horizon_100ms),
    }
    return evaluate_phase42a_report(report)


def evaluate_phase42a_report(report: dict[str, Any]) -> dict[str, Any]:
    evaluated = json.loads(json.dumps(report))
    hard_fail_reasons: list[str] = []
    warning_reasons = list(evaluated.get("warning_reasons") or [])
    implementation_status = "pass"
    runtime_status = "pass"
    dataset_status = "pass"
    primary_failure: str | None = None

    schema_errors = validate_phase42a_report_schema(evaluated)
    if schema_errors:
        implementation_status = "fail"
        hard_fail_reasons.extend(f"report schema invalid: {error}" for error in schema_errors)
        primary_failure = primary_failure or "report_schema_invalid"

    capture = _dict(evaluated.get("capture"))
    if evaluated.get("fresh_capture_required") is True and not capture.get(
        "fresh_capture_performed"
    ):
        implementation_status = "fail"
        hard_fail_reasons.append("fresh capture required but not performed")
        primary_failure = primary_failure or "fresh_capture_not_performed"
    if capture.get("fresh_capture_performed") is True and _num(capture.get("duration_sec")) < 1800.0:
        runtime_status = "fail"
        hard_fail_reasons.append("fresh capture duration_sec < 1800")
        primary_failure = primary_failure or "fresh_capture_duration_too_short"
    if capture.get("downsampling_enabled") is True:
        runtime_status = "fail"
        hard_fail_reasons.append("downsampling_enabled must be false")
        primary_failure = primary_failure or "downsampling_enabled"
    if capture.get("emits_every_accepted_delta") is not True:
        runtime_status = "fail"
        hard_fail_reasons.append("emits_every_accepted_delta must be true")
        primary_failure = primary_failure or "not_emitting_every_accepted_delta"

    runtime_quality = _dict(evaluated.get("runtime_quality"))
    for field in RUNTIME_HARD_ZERO_FIELDS:
        if _num(runtime_quality.get(field)) > 0:
            runtime_status = "fail"
            hard_fail_reasons.append(f"{field} > 0: {runtime_quality.get(field)}")
            primary_failure = primary_failure or "phase_4_1_1_runtime_invariant_failed"
    if _num(runtime_quality.get("snapshot_copy_p99_us")) > 200.0:
        runtime_status = "fail"
        hard_fail_reasons.append(
            f"snapshot_copy_p99_us > 200: {runtime_quality.get('snapshot_copy_p99_us')}"
        )
        primary_failure = primary_failure or "phase_4_1_1_runtime_invariant_failed"

    if _num(evaluated.get("clean_sample_count")) <= 0:
        dataset_status = "fail"
        hard_fail_reasons.append("clean sample count = 0")
        primary_failure = primary_failure or "clean_sample_count_zero"
    if _num(evaluated.get("labeled_sample_count")) <= 0:
        dataset_status = "fail"
        hard_fail_reasons.append("labeled sample count = 0")
        primary_failure = primary_failure or "labeled_sample_count_zero"

    timestamp_quality = _dict(evaluated.get("timestamp_quality"))
    if _num(timestamp_quality.get("timestamp_monotonic_violations")) > 0:
        dataset_status = "fail"
        hard_fail_reasons.append(
            "timestamp_monotonic_violations > 0: "
            f"{timestamp_quality.get('timestamp_monotonic_violations')}"
        )
        primary_failure = primary_failure or "timestamp_monotonic_violation"
    if _num(timestamp_quality.get("duplicate_timestamp_count")) > 0:
        dataset_status = "fail"
        hard_fail_reasons.append(
            f"duplicate_timestamp_count > 0: {timestamp_quality.get('duplicate_timestamp_count')}"
        )
        primary_failure = primary_failure or "duplicate_timestamp"
    if _num(timestamp_quality.get("gap_p95_ms")) > SAMPLE_GAP_P95_MAX_MS:
        dataset_status = "fail"
        hard_fail_reasons.append(
            f"sample_gap_p95_ms > 100: {timestamp_quality.get('gap_p95_ms')}"
        )
        primary_failure = primary_failure or "sample_gap_p95_above_threshold"
    if _num(timestamp_quality.get("gap_p99_ms")) > SAMPLE_GAP_P99_MAX_MS:
        dataset_status = "fail"
        hard_fail_reasons.append(
            f"sample_gap_p99_ms > 200: {timestamp_quality.get('gap_p99_ms')}"
        )
        primary_failure = primary_failure or "sample_gap_p99_above_threshold"

    horizon = _dict(evaluated.get("horizon_100ms"))
    policy_relaxed = int(horizon.get("max_future_gap_ms", -1) or -1) != REQUIRED_100MS_MAX_FUTURE_GAP_MS
    valid_rate_failed = _num(horizon.get("valid_rate_eligible_rows")) < REQUIRED_100MS_VALID_RATE
    if policy_relaxed:
        dataset_status = "fail"
        hard_fail_reasons.append("horizon_100ms max_future_gap_ms != 100")
        primary_failure = "horizon_100ms_policy_relaxed"
    if valid_rate_failed:
        dataset_status = "fail"
        hard_fail_reasons.append(
            "horizon_100ms valid_rate_eligible_rows "
            f"{_num(horizon.get('valid_rate_eligible_rows')):.6f} below threshold 0.95"
        )
        if not policy_relaxed:
            primary_failure = "horizon_100ms_valid_rate_below_threshold"

    leakage = _dict(evaluated.get("leakage_check"))
    if _num(leakage.get("feature_leakage_violations")) > 0:
        implementation_status = "fail"
        hard_fail_reasons.append(
            f"feature_leakage_violations > 0: {leakage.get('feature_leakage_violations')}"
        )
        primary_failure = primary_failure or "feature_leakage"
    if _num(leakage.get("label_leakage_violations")) > 0:
        implementation_status = "fail"
        hard_fail_reasons.append(
            f"label_leakage_violations > 0: {leakage.get('label_leakage_violations')}"
        )
        primary_failure = primary_failure or "label_leakage"

    hard_fail_reasons = list(dict.fromkeys(hard_fail_reasons))
    evaluated["implementation_status"] = implementation_status
    evaluated["runtime_status"] = runtime_status
    evaluated["dataset_coverage_status"] = dataset_status
    evaluated["definition_of_done_status"] = "fail" if hard_fail_reasons else "pass"
    evaluated["status"] = evaluated["definition_of_done_status"]
    evaluated["primary_failure"] = primary_failure if hard_fail_reasons else None
    evaluated["hard_fail_reasons"] = hard_fail_reasons
    evaluated["warning_reasons"] = sorted(set(str(item) for item in warning_reasons))
    return evaluated


def classify_phase42a_failure(report: dict[str, Any]) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "UNKNOWN_PHASE42A_FAILURE"
    reasons = " ".join(str(reason) for reason in report.get("hard_fail_reasons", []))
    primary = str(report.get("primary_failure"))
    if "report schema invalid" in reasons:
        return "REPORT_SCHEMA_FAILURE"
    if "horizon_100ms_policy_relaxed" in primary or "max_future_gap_ms != 100" in reasons:
        return "HORIZON_100MS_POLICY_RELAXED"
    if "horizon_100ms_valid_rate_below_threshold" in primary or "valid_rate_eligible_rows" in reasons:
        return "LABEL_VALID_RATE_FAILURE"
    if "timestamp_monotonic" in reasons:
        return "TIMESTAMP_MONOTONIC_FAILURE"
    if "duplicate_timestamp" in reasons:
        return "DUPLICATE_TIMESTAMP_FAILURE"
    if "sample_gap_p95" in reasons:
        return "SAMPLE_GAP_P95_FAILURE"
    if "sample_gap_p99" in reasons:
        return "SAMPLE_GAP_P99_FAILURE"
    if "feature_leakage" in reasons:
        return "FEATURE_LEAKAGE_FAILURE"
    if "label_leakage" in reasons:
        return "LABEL_LEAKAGE_FAILURE"
    if "fresh capture required" in reasons or "capture command" in reasons:
        return "RUNTIME_CAPTURE_FAILURE"
    if "runtime_invariant" in primary or any(field in reasons for field in RUNTIME_HARD_ZERO_FIELDS):
        return "RUNTIME_QUALITY_FAILURE"
    if "clean sample count = 0" in reasons:
        return "INPUT_EMPTY"
    return "UNKNOWN_PHASE42A_FAILURE"


def validate_phase42a_report_schema(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in sorted(PHASE42A_REQUIRED_REPORT_FIELDS):
        if field not in report:
            errors.append(f"missing required field: {field}")
    for field in (
        "implementation_status",
        "runtime_status",
        "dataset_coverage_status",
        "definition_of_done_status",
    ):
        if field in report and report.get(field) not in {"pass", "fail"}:
            errors.append(f"invalid status field: {field}")
    horizon = report.get("horizon_100ms")
    if not isinstance(horizon, dict):
        errors.append("missing required object: horizon_100ms")
    else:
        for field in (
            "max_future_gap_ms",
            "eligible_count",
            "valid_count",
            "invalid_count",
            "valid_rate_all_rows",
            "valid_rate_eligible_rows",
        ):
            if field not in horizon:
                errors.append(f"missing horizon_100ms field: {field}")
    timestamp = report.get("timestamp_quality")
    if not isinstance(timestamp, dict):
        errors.append("missing required object: timestamp_quality")
    else:
        for field in ("gap_p95_ms", "gap_p99_ms", "timestamp_monotonic_violations", "duplicate_timestamp_count"):
            if field not in timestamp:
                errors.append(f"missing timestamp_quality field: {field}")
    return errors


def runtime_quality_from_phase41_report(path: str | Path) -> tuple[dict[str, Any], list[str]]:
    report_path = Path(path)
    if not report_path.exists():
        return _normalize_runtime_quality({}), [f"Phase 4.1.1 report missing: {report_path}"]
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _normalize_runtime_quality({}), [f"Phase 4.1.1 report invalid JSON: {exc}"]
    queue = report.get("queue") if isinstance(report.get("queue"), dict) else {}
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), dict) else {}
    quality = {
        "sample_before_ready_count": _field(report, queue, lifecycle, "sample_before_ready_count"),
        "feed_receive_stale_count": _field(report, queue, lifecycle, "feed_receive_stale_count"),
        "queue_dropped_messages": _field(report, queue, lifecycle, "queue_dropped_messages"),
        "sequence_gap_count": _field(report, queue, lifecycle, "sequence_gap_count"),
        "invalid_delta_count": _field(report, queue, lifecycle, "invalid_delta_count"),
        "crossed_book_count": _field(report, queue, lifecycle, "crossed_book_count"),
        "book_empty_count": _field(report, queue, lifecycle, "book_empty_count"),
        "one_side_missing_count": _field(report, queue, lifecycle, "one_side_missing_count"),
        "clean_sample_schema_violation_count": _field(report, queue, lifecycle, "clean_sample_schema_violation_count"),
        "snapshot_copy_p99_us": report.get("snapshot_copy_p99_us"),
    }
    return _normalize_runtime_quality(quality), []


def run_phase42a_analysis(
    *,
    root: str | Path,
    symbol: str,
    clean_samples_path: str | Path,
    labeled_samples_path: str | Path,
    runtime_quality: dict[str, Any],
    capture: dict[str, Any],
    fresh_capture_required: bool,
) -> dict[str, Any]:
    root_path = Path(root)
    clean_path = Path(clean_samples_path)
    labeled_path = Path(labeled_samples_path)
    if not clean_path.is_absolute():
        clean_path = root_path / clean_path
    if not labeled_path.is_absolute():
        labeled_path = root_path / labeled_path
    schema_path = root_path / "data/debug/phase_4_2a_dataset_schema_violations.jsonl"
    validation = validate_clean_samples(clean_path, violation_output_path=schema_path)
    clean_samples = validation.samples if validation.valid else []
    labeled_rows: list[dict[str, Any]] = []
    leakage_result = {
        "passed": True,
        "feature_leakage_violations": 0,
        "label_leakage_violations": 0,
        "violations": [],
    }
    if clean_samples:
        labeled_rows = generate_labeled_rows(clean_samples)
        write_jsonl(labeled_path, labeled_rows)
        validate_labeled_rows(labeled_rows, violation_output_path=schema_path)
        leakage_result = run_leakage_check(
            labeled_rows,
            output_path=root_path / "data/debug/phase_4_2_leakage_check.json",
        )
    else:
        write_jsonl(labeled_path, [])
    report = build_phase42a_report(
        symbol=symbol,
        clean_samples=clean_samples,
        labeled_rows=labeled_rows,
        leakage_result=leakage_result,
        runtime_quality=runtime_quality,
        capture={**capture, "sample_count": len(clean_samples), "sample_rate_per_sec": _sample_rate(clean_samples)},
        fresh_capture_required=fresh_capture_required,
        input_clean_path=clean_path.relative_to(root_path) if clean_path.is_relative_to(root_path) else clean_path,
        labeled_path=labeled_path.relative_to(root_path) if labeled_path.is_relative_to(root_path) else labeled_path,
        invalid_cases_path=root_path / PHASE42A_INVALID_CASES,
    )
    if validation.failure_classification == "INPUT_FILE_MISSING":
        report["hard_fail_reasons"].append("input clean sample file missing")
        report["primary_failure"] = report["primary_failure"] or "input_file_missing"
        report["dataset_coverage_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
    if validation.failure_classification == "INPUT_EMPTY":
        report["hard_fail_reasons"].append("input clean sample file empty")
        report["primary_failure"] = report["primary_failure"] or "input_empty"
        report["dataset_coverage_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
    return report


def write_phase42a_artifacts(
    report: dict[str, Any],
    *,
    root: str | Path,
    pytest_output: str,
    bundle_created: bool = False,
) -> None:
    root_path = Path(root)
    report = evaluate_phase42a_report(report)
    _write_json(root_path / PHASE42A_REPORT_JSON, report)
    _write_text(root_path / PHASE42A_REPORT_MD, render_phase42a_markdown(report))
    _write_json(root_path / PHASE42A_GAP_DISTRIBUTION, report.get("timestamp_quality", {}))
    _write_json(
        root_path / PHASE42A_COVERAGE_SUMMARY,
        {
            "phase": "4.2A",
            "status": report.get("status"),
            "definition_of_done_status": report.get("definition_of_done_status"),
            "primary_failure": report.get("primary_failure"),
            "horizon_100ms": report.get("horizon_100ms"),
            "bottleneck_assessment": report.get("bottleneck_assessment"),
        },
    )
    _write_text(root_path / PHASE42A_PYTEST_OUTPUT, pytest_output)
    classification = (
        None
        if report.get("definition_of_done_status") == "pass"
        else classify_phase42a_failure(report)
    )
    self_check = {
        "phase": "4.2A",
        "passed": report.get("definition_of_done_status") == "pass",
        "status": report.get("definition_of_done_status"),
        "definition_of_done_status": report.get("definition_of_done_status"),
        "failure_classification": classification,
        "summary": _self_check_summary(report, classification),
        "report_json_path": _display_path(PHASE42A_REPORT_JSON),
        "report_md_path": _display_path(PHASE42A_REPORT_MD),
        "pytest_output_path": _display_path(PHASE42A_PYTEST_OUTPUT),
        "bundle_path": _display_path(PHASE42A_BUNDLE),
        "bundle_created": bundle_created,
    }
    _write_json(root_path / PHASE42A_SELF_CHECK_JSON, self_check)
    if report.get("definition_of_done_status") != "pass":
        write_phase42a_failure_investigation(root=root_path, report=report, classification=classification)


def render_phase42a_markdown(report: dict[str, Any]) -> str:
    horizon = report.get("horizon_100ms", {})
    timestamp = report.get("timestamp_quality", {})
    leakage = report.get("leakage_check", {})
    lines = [
        "# Phase 4.2A 100ms Coverage Report",
        "",
        f"Status: **{report.get('definition_of_done_status')}**",
        "",
        "## Status Separation",
        "",
        f"- Implementation: `{report.get('implementation_status')}`",
        f"- Runtime: `{report.get('runtime_status')}`",
        f"- Dataset coverage: `{report.get('dataset_coverage_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        "",
        "## 100ms Coverage",
        "",
        f"- Max future gap ms: `{horizon.get('max_future_gap_ms')}`",
        f"- Eligible rows: `{horizon.get('eligible_count')}`",
        f"- Valid rows: `{horizon.get('valid_count')}`",
        f"- Eligible valid rate: `{horizon.get('valid_rate_eligible_rows')}`",
        f"- Invalid reasons: `{json.dumps(horizon.get('invalid_reason_counts', {}), sort_keys=True)}`",
        "",
        "## Sample Gaps",
        "",
        f"- Gap p95 ms: `{timestamp.get('gap_p95_ms')}`",
        f"- Gap p99 ms: `{timestamp.get('gap_p99_ms')}`",
        f"- Gap max ms: `{timestamp.get('gap_max_ms')}`",
        "",
        "## Leakage",
        "",
        f"- Passed: `{leakage.get('passed')}`",
        f"- Feature leakage violations: `{leakage.get('feature_leakage_violations')}`",
        f"- Label leakage violations: `{leakage.get('label_leakage_violations')}`",
        "",
        "## Bottleneck Assessment",
        "",
        str(report.get("bottleneck_assessment")),
        "",
        "## Hard Fail Reasons",
        "",
    ]
    reasons = report.get("hard_fail_reasons", [])
    if reasons:
        lines.extend(f"- {reason}" for reason in reasons)
    else:
        lines.append("- None")
    lines.extend(["", "## Recommendation", "", _recommendation(report), ""])
    return "\n".join(lines)


def create_phase42a_bundle(
    *,
    root: str | Path,
    source_root: str | Path = REPO_ROOT,
    bundle_path: str | Path | None = None,
) -> Path:
    root_path = Path(root)
    source_path = Path(source_root)
    target = Path(bundle_path) if bundle_path is not None else root_path / PHASE42A_BUNDLE
    if target.exists():
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for directory_name in ("app", "tests", "scripts"):
            archive.writestr(f"{directory_name}/", "")
        _write_directory_to_archive(archive, source_path / "bot/app", "app")
        _write_directory_to_archive(archive, source_path / "tests", "tests")
        _write_directory_to_archive(archive, source_path / "scripts", "scripts")
        for relative in PHASE42A_REQUIRED_BUNDLE_FILES:
            if relative.endswith("/"):
                continue
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative)
        investigation = root_path / PHASE42A_INVESTIGATION
        if investigation.exists():
            archive.write(investigation, _display_path(PHASE42A_INVESTIGATION))
    missing = phase42a_bundle_missing_files(target)
    if missing:
        raise RuntimeError(f"Phase 4.2A bundle missing required files: {missing}")
    return target


def phase42a_bundle_missing_files(bundle_path: str | Path) -> list[str]:
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    return [name for name in PHASE42A_REQUIRED_BUNDLE_FILES if name not in names]


def write_phase42a_failure_investigation(
    *,
    root: str | Path,
    report: dict[str, Any],
    classification: str | None,
) -> None:
    lines = [
        "# Phase 4.2A Failure Investigation",
        "",
        f"- Failure classification: `{classification}`",
        f"- Definition of Done status: `{report.get('definition_of_done_status')}`",
        f"- Primary failure: `{report.get('primary_failure')}`",
        f"- Report path: `{_display_path(PHASE42A_REPORT_JSON)}`",
        "",
        "## Hard Fail Reasons",
        "",
        *[f"- {reason}" for reason in report.get("hard_fail_reasons", [])],
        "",
        "## Bottleneck Assessment",
        "",
        str(report.get("bottleneck_assessment")),
        "",
        "## Recommendation",
        "",
        _recommendation(report),
        "",
        "## Fix Applied",
        "",
        "No 100ms threshold relaxation was applied. No strategy/model/execution/PnL work was added.",
        "",
        "## Rerun Result",
        "",
        "See `data/reports/phase42a_self_check.json` and this report's hard fail reasons.",
        "",
    ]
    _write_text(Path(root) / PHASE42A_INVESTIGATION, "\n".join(lines))


def _normalize_runtime_quality(quality: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        field: int(_num(quality.get(field))) for field in RUNTIME_HARD_ZERO_FIELDS
    }
    normalized["snapshot_copy_p99_us"] = _num(quality.get("snapshot_copy_p99_us"))
    return normalized


def _field(report: dict[str, Any], queue: dict[str, Any], lifecycle: dict[str, Any], field: str) -> Any:
    if field in report:
        return report.get(field)
    if field == "queue_dropped_messages":
        return queue.get("queue_dropped_messages")
    if field in lifecycle:
        return lifecycle.get(field)
    return queue.get(field)


def _bottleneck_assessment(timestamp_quality: dict[str, Any], horizon: dict[str, Any]) -> str:
    valid_rate = _num(horizon.get("valid_rate_eligible_rows"))
    gap_p95 = _num(timestamp_quality.get("gap_p95_ms"))
    gap_p99 = _num(timestamp_quality.get("gap_p99_ms"))
    invalid_reasons = horizon.get("invalid_reason_counts", {})
    if valid_rate >= REQUIRED_100MS_VALID_RATE and gap_p95 <= SAMPLE_GAP_P95_MAX_MS and gap_p99 <= SAMPLE_GAP_P99_MAX_MS:
        return "100ms temporal coverage is sufficient under the required hard gates."
    if isinstance(invalid_reasons, dict) and _num(invalid_reasons.get("FUTURE_GAP_TOO_LARGE")) > 0:
        if gap_p95 > SAMPLE_GAP_P95_MAX_MS or gap_p99 > SAMPLE_GAP_P99_MAX_MS:
            return (
                "Coverage failure appears driven by clean sample cadence/public WS jitter: "
                "sample gap percentiles exceed the 100ms research requirement, producing "
                "FUTURE_GAP_TOO_LARGE 100ms labels."
            )
        return (
            "Coverage failure appears driven by target-time future gaps despite acceptable "
            "overall sample percentiles; inspect capture protocol and local processing jitter."
        )
    return "Coverage failure root cause is inconclusive from current artifacts."


def _recommendation(report: dict[str, Any]) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "Proceed only after the pass bundle is reviewed."
    return (
        "Keep 100ms as a hard gate. Next engineering step: improve capture cadence/protocol "
        "and reduce public WebSocket/local processing jitter, then rerun Phase 4.2A. "
        "Do not move to Phase 5 while this report is failing."
    )


def _self_check_summary(report: dict[str, Any], classification: str | None) -> str:
    if report.get("definition_of_done_status") == "pass":
        return "Phase 4.2A Definition of Done passed; pass bundle may be created."
    if classification == "LABEL_VALID_RATE_FAILURE":
        return (
            "Phase 4.2A failed because horizon_100ms eligible-row valid rate is below "
            "95% with max_future_gap_ms fixed at 100. No pass bundle was created."
        )
    return f"Phase 4.2A failed with classification {classification}. No pass bundle was created."


def _sample_rate(samples: list[dict[str, Any]]) -> float:
    if len(samples) < 2:
        return 0.0
    duration_sec = (
        int(samples[-1]["local_recv_monotonic_ns"]) - int(samples[0]["local_recv_monotonic_ns"])
    ) / 1_000_000_000.0
    return len(samples) / duration_sec if duration_sec > 0 else 0.0


def _percentile(values: list[float], percentile: float) -> float | None:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = int(round((len(clean) - 1) * percentile))
    return clean[min(max(index, 0), len(clean) - 1)]


def _num(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _write_directory_to_archive(
    archive: zipfile.ZipFile,
    directory: Path,
    archive_prefix: str,
) -> None:
    if not directory.exists():
        return
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        archive.write(path, str(Path(archive_prefix) / path.relative_to(directory)))
