from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
from typing import Any
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE = "5.0"
PRIMARY_LABEL_HORIZON_MS = 100
DIAGNOSTIC_LABEL_HORIZON_MS = 250
PHASE42H_BUNDLE_NAME = "phase_4_2h_hotpath_environment_latency_bundle.zip"
PHASE42H_SHA256_NAME = "phase_4_2h_bundle_sha256.txt"

PHASE50_SOURCE_REPRODUCIBILITY_REPORT = Path("data/debug/phase_5_0_source_reproducibility_gate.json")
PHASE50_EVIDENCE_INTEGRITY_REPORT = Path("data/debug/phase_5_0_evidence_integrity_report.json")
PHASE50_DATASET_MANIFEST = Path("data/debug/phase_5_0_dataset_manifest.json")
PHASE50_SPLIT_REPORT = Path("data/debug/phase_5_0_split_report.json")
PHASE50_FEATURE_SCHEMA = Path("data/debug/phase_5_0_feature_schema.json")
PHASE50_LABEL_VALIDATION_REPORT = Path("data/debug/phase_5_0_label_validation_report.json")
PHASE50_LEAKAGE_CHECK = Path("data/debug/phase_5_0_leakage_check.json")
PHASE50_BUCKET_EDGE_REPORT = Path("data/debug/phase_5_0_bucket_edge_report.json")
PHASE50_MODEL_BASELINE_REPORT = Path("data/debug/phase_5_0_model_baseline_report.json")
PHASE50_RUNNER_COMMAND_LOG = Path("data/debug/phase_5_0_runner_command_log.txt")
PHASE50_PYTEST_CONSOLE_LOG = Path("data/debug/phase_5_0_pytest_console_log.txt")
PHASE50_FINAL_REPORT_JSON = Path("data/reports/phase_5_0_empirical_signal_report.json")
PHASE50_FINAL_REPORT_MD = Path("data/reports/phase_5_0_empirical_signal_report.md")
PHASE50_BUNDLE = Path("phase_5_0_empirical_signal_research_bundle.zip")

PHASE42H_REPORT_MEMBER = "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
PHASE42H_DATASET_ZIP_MEMBER = "data/dataset/phase_4_2h_latency_profile_datasets.zip"

DATASET_MEMBERS = {
    "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
    "bookticker": "data/dataset/bookticker_reference_quotes.jsonl",
    "trade": "data/dataset/trade_reference_events.jsonl",
    "aggtrade": "data/dataset/aggtrade_reference_events.jsonl",
    "corrected_labels": "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
    "latency_profile": "data/dataset/phase_4_2h_latency_profile_samples.jsonl",
}

CONSERVATIVE_COST_ASSUMPTIONS = {
    "fee_bps": 2.0,
    "slippage_bps": 1.0,
    "total_cost_bps": 3.0,
    "description": "Research-only conservative round-trip cost proxy; no execution logic is implemented.",
}

FEATURE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "name": "target_mid_price",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": False,
        "description": "Target-market mid price from the current clean order book sample.",
    },
    {
        "name": "target_spread_bps",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": False,
        "description": "Target-market best ask minus best bid in basis points.",
    },
    {
        "name": "target_book_age_ms",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Age of the target book update as emitted by the clean sample.",
    },
    {
        "name": "target_top_bid_qty_1",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Top-of-book bid quantity.",
    },
    {
        "name": "target_top_ask_qty_1",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Top-of-book ask quantity.",
    },
    {
        "name": "target_book_imbalance_1",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Bid-minus-ask quantity imbalance at depth 1.",
    },
    {
        "name": "target_book_imbalance_5",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Bid-minus-ask quantity imbalance across top 5 levels.",
    },
    {
        "name": "target_book_imbalance_20",
        "dtype": "float",
        "group": "polymarket_target_market",
        "timestamp_rule": "current target order book snapshot at feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Bid-minus-ask quantity imbalance across top 20 levels.",
    },
    {
        "name": "reference_bookticker_mid_price",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "last reference quote with quote_ts <= feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Most recent reference bookTicker mid price available by feature time.",
    },
    {
        "name": "reference_bookticker_spread_bps",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "last reference quote with quote_ts <= feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Most recent reference bookTicker spread in basis points.",
    },
    {
        "name": "reference_bookticker_age_ms",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "last reference quote with quote_ts <= feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Age of the last reference quote at feature time.",
    },
    {
        "name": "reference_mid_return_1s_bps",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "reference quote at feature_ts compared with last quote at or before feature_ts minus 1s",
        "uses_future_data": False,
        "nullable": True,
        "description": "Trailing one-second reference mid return in basis points.",
    },
    {
        "name": "reference_trade_count_1s",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "trades with trade_ts in (feature_ts - 1s, feature_ts]",
        "uses_future_data": False,
        "nullable": False,
        "description": "Count of reference trades observed in the trailing one-second window.",
    },
    {
        "name": "reference_signed_trade_qty_1s",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "trades with trade_ts in (feature_ts - 1s, feature_ts]",
        "uses_future_data": False,
        "nullable": False,
        "description": "Signed trailing trade quantity, positive for buyer-initiated prints.",
    },
    {
        "name": "reference_signed_trade_notional_1s",
        "dtype": "float",
        "group": "reference_market",
        "timestamp_rule": "trades with trade_ts in (feature_ts - 1s, feature_ts]",
        "uses_future_data": False,
        "nullable": False,
        "description": "Signed trailing trade notional, positive for buyer-initiated prints.",
    },
    {
        "name": "repricing_gap_bps",
        "dtype": "float",
        "group": "cross_market_latency",
        "timestamp_rule": "last reference quote with quote_ts <= feature_ts versus current target mid",
        "uses_future_data": False,
        "nullable": True,
        "description": "Reference mid minus target mid in basis points.",
    },
    {
        "name": "latency_end_to_end_hot_path_ms",
        "dtype": "float",
        "group": "cross_market_latency",
        "timestamp_rule": "latency profile sample with latency_ts <= feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Observed local hot-path latency paired to the feature sample.",
    },
    {
        "name": "latency_queue_wait_ms",
        "dtype": "float",
        "group": "cross_market_latency",
        "timestamp_rule": "latency profile sample with latency_ts <= feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Observed queue wait latency paired to the feature sample.",
    },
    {
        "name": "latency_quality_score",
        "dtype": "float",
        "group": "cross_market_latency",
        "timestamp_rule": "computed only from quote age and latency values available by feature_ts",
        "uses_future_data": False,
        "nullable": True,
        "description": "Bounded quality proxy that decreases as quote age and hot-path latency rise.",
    },
)

MODEL_FEATURE_NAMES = tuple(feature["name"] for feature in FEATURE_DEFINITIONS)
LABEL_FIELD_NAMES = (
    "future_return_100ms_bps",
    "direction_100ms",
    "spread_adjusted_direction_100ms",
    "valid_100ms_label",
)


def run_phase50(
    root: str | Path = REPO_ROOT,
    *,
    bundle_path: str | Path | None = None,
    sha256_path: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    _ensure_output_dirs(root_path)
    bundle = Path(bundle_path) if bundle_path is not None else root_path / PHASE42H_BUNDLE_NAME
    sha_file = Path(sha256_path) if sha256_path is not None else root_path / PHASE42H_SHA256_NAME
    if not bundle.is_absolute():
        bundle = root_path / bundle
    if not sha_file.is_absolute():
        sha_file = root_path / sha_file

    source_gate = run_source_reproducibility_gate(root_path)
    _write_json(root_path / PHASE50_SOURCE_REPRODUCIBILITY_REPORT, source_gate)

    evidence, extracted = verify_phase42h_evidence(root_path, bundle, sha_file)
    _write_json(root_path / PHASE50_EVIDENCE_INTEGRITY_REPORT, evidence)

    samples, dataset_summary = build_research_samples(extracted["dataset_paths"])
    label_report = build_label_validation_report(samples)
    _write_json(root_path / PHASE50_LABEL_VALIDATION_REPORT, label_report)

    manifest = build_dataset_manifest(
        root_path=root_path,
        bundle_path=bundle,
        evidence=evidence,
        runtime_report=extracted["runtime_report"],
        dataset_summary=dataset_summary,
    )
    _write_json(root_path / PHASE50_DATASET_MANIFEST, manifest)

    split_report = build_split_report(samples)
    _write_json(root_path / PHASE50_SPLIT_REPORT, split_report)

    feature_schema = build_feature_schema_report(samples)
    _write_json(root_path / PHASE50_FEATURE_SCHEMA, feature_schema)

    leakage = build_leakage_report(samples, split_report)
    _write_json(root_path / PHASE50_LEAKAGE_CHECK, leakage)

    bucket_edge = build_bucket_edge_report(samples, split_report)
    _write_json(root_path / PHASE50_BUCKET_EDGE_REPORT, bucket_edge)

    model_baseline = build_model_baseline_report(samples, split_report)
    _write_json(root_path / PHASE50_MODEL_BASELINE_REPORT, model_baseline)

    final_report = build_final_report(
        source_gate=source_gate,
        evidence=evidence,
        manifest=manifest,
        split_report=split_report,
        feature_schema=feature_schema,
        label_report=label_report,
        leakage=leakage,
        bucket_edge=bucket_edge,
        model_baseline=model_baseline,
    )
    _write_json(root_path / PHASE50_FINAL_REPORT_JSON, final_report)
    _write_text(root_path / PHASE50_FINAL_REPORT_MD, render_final_markdown(final_report))
    _write_text(root_path / PHASE50_RUNNER_COMMAND_LOG, render_runner_command_log(final_report, bundle, sha_file))
    bundle_report = create_phase50_bundle(root_path)
    final_report["bundle"] = bundle_report
    _write_json(root_path / PHASE50_FINAL_REPORT_JSON, final_report)
    _write_text(root_path / PHASE50_FINAL_REPORT_MD, render_final_markdown(final_report))
    return final_report


def run_source_reproducibility_gate(root_path: Path) -> dict[str, Any]:
    script_path = root_path / "scripts/run_phase42h_hotpath_environment_latency.py"
    text = script_path.read_text(encoding="utf-8", errors="ignore") if script_path.exists() else ""
    expected_pythonpath = os.pathsep.join([str(root_path), str(root_path / "bot")])
    bot_only_assignment_present = 'env["PYTHONPATH"] = str(SOURCE_ROOT / "bot")' in text
    helper_present = "_subprocess_pythonpath" in text and "_subprocess_env" in text
    import_command = [
        sys.executable,
        "-X",
        "utf8",
        "-c",
        "import tests.test_phase42h_hotpath_environment_latency as t; print(t._report()['phase'])",
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = expected_pythonpath
    process = subprocess.run(import_command, cwd=root_path, env=env, text=True, capture_output=True, check=False)
    tests_import_ok = process.returncode == 0 and "4.2H" in process.stdout
    passed = script_path.exists() and helper_present and not bot_only_assignment_present and tests_import_ok
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_source_reproducibility_gate_v1",
        "status": "pass" if passed else "fail",
        "phase42h_script": _relative_display(root_path, script_path),
        "expected_pythonpath": expected_pythonpath,
        "repo_root_in_pythonpath": str(root_path) in expected_pythonpath.split(os.pathsep),
        "bot_in_pythonpath": str(root_path / "bot") in expected_pythonpath.split(os.pathsep),
        "bot_only_assignment_present": bot_only_assignment_present,
        "runner_subprocess_helper_present": helper_present,
        "tests_import_subprocess_exit_code": process.returncode,
        "tests_import_subprocess_stdout": process.stdout.strip(),
        "tests_import_subprocess_stderr": process.stderr.strip(),
        "tests_import_subprocess_ok": tests_import_ok,
    }


def verify_phase42h_evidence(root_path: Path, bundle_path: Path, sha256_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_sha = _parse_sha256_file(sha256_path)
    actual_sha = _sha256_file(bundle_path) if bundle_path.exists() else ""
    extract_dir = root_path / "data/cache/phase_5_0_phase42h_bundle"
    dataset_dir = extract_dir / "datasets"
    errors: list[str] = []
    bundle_extractable = False
    runtime_report: dict[str, Any] = {}
    dataset_paths: dict[str, Path] = {}

    try:
        _safe_clear_directory(root_path, extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(bundle_path) as archive:
            _safe_extract(archive, extract_dir)
        bundle_extractable = True
        report_path = extract_dir / PHASE42H_REPORT_MEMBER
        runtime_report = _read_json(report_path)
        nested_zip_path = extract_dir / PHASE42H_DATASET_ZIP_MEMBER
        dataset_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(nested_zip_path) as dataset_archive:
            _safe_extract(dataset_archive, dataset_dir)
        dataset_paths = {name: dataset_dir / member for name, member in DATASET_MEMBERS.items()}
    except Exception as exc:  # pragma: no cover - defensive audit detail
        errors.append(f"{type(exc).__name__}: {exc}")

    phase41 = _dict(runtime_report.get("phase41_runtime_report"))
    clock_summary = _dict(runtime_report.get("clock_offset_summary"))
    clock_sanity = _dict(runtime_report.get("clock_sanity_report"))
    phase41_status = phase41.get("phase_4_1_status") or runtime_report.get("phase41_runtime_report_status")
    phase5_ready = runtime_report.get("phase5_ready")

    checks = {
        "bundle_sha256_valid": bool(expected_sha) and actual_sha == expected_sha,
        "bundle_extractable": bundle_extractable,
        "runtime_status_pass": runtime_report.get("status") == "pass",
        "primary_failure_none": runtime_report.get("primary_failure") is None,
        "phase41_status_pass": phase41_status == "pass",
        "clock_sync_status_pass": runtime_report.get("clock_sync_status") == "pass",
        "clock_offset_drift_valid": clock_summary.get("clock_offset_drift_valid") is True
        or clock_sanity.get("clock_offset_drift_valid") is True,
        "clock_offset_sample_quality_valid": clock_summary.get("clock_offset_sample_quality_valid") is True
        or clock_sanity.get("clock_offset_sample_quality_valid") is True,
        "snapshot_copy_budget_met": phase41.get("snapshot_copy_budget_met") is True,
        "strict_100ms_observability_ready": runtime_report.get("strict_100ms_observability_ready") is True,
        "low_latency_ready": runtime_report.get("low_latency_ready") is True,
    }
    passed = all(checks.values()) and not errors
    report = {
        "phase": PHASE,
        "schema_version": "phase_5_0_evidence_integrity_v1",
        "status": "pass" if passed else "fail",
        "bundle_filename": bundle_path.name,
        "bundle_path": _relative_display(root_path, bundle_path),
        "bundle_sha256_expected": expected_sha,
        "bundle_sha256_actual": actual_sha,
        "bundle_sha256_valid": checks["bundle_sha256_valid"],
        "bundle_extractable": bundle_extractable,
        "runtime_status": runtime_report.get("status"),
        "primary_failure": runtime_report.get("primary_failure"),
        "phase41_status": phase41_status,
        "clock_sync_status": runtime_report.get("clock_sync_status"),
        "clock_offset_drift_valid": checks["clock_offset_drift_valid"],
        "clock_offset_sample_quality_valid": checks["clock_offset_sample_quality_valid"],
        "snapshot_copy_budget_met": checks["snapshot_copy_budget_met"],
        "strict_100ms_observability_ready": checks["strict_100ms_observability_ready"],
        "low_latency_ready": checks["low_latency_ready"],
        "phase5_ready": phase5_ready,
        "phase5_ready_false_interpretation": "acceptable_before_phase5_implementation" if phase5_ready is False else "unexpected",
        "checks": checks,
        "errors": errors,
        "runtime_metrics": _runtime_metrics(runtime_report),
        "extracted_dataset_files": {
            name: _relative_display(root_path, path) for name, path in sorted(dataset_paths.items())
        },
    }
    return report, {"extract_dir": extract_dir, "dataset_dir": dataset_dir, "dataset_paths": dataset_paths, "runtime_report": runtime_report}


def build_research_samples(dataset_paths: dict[str, Path]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    clean_rows = _read_jsonl(dataset_paths["clean_samples"])
    label_rows = _read_jsonl(dataset_paths["corrected_labels"])
    bookticker_rows = sorted(_read_jsonl(dataset_paths["bookticker"]), key=lambda row: _int(row.get("local_recv_monotonic_ns")) or 0)
    trade_rows = sorted(_read_jsonl(dataset_paths["trade"]), key=lambda row: _int(row.get("local_recv_monotonic_ns")) or 0)
    aggtrade_rows = sorted(_read_jsonl(dataset_paths["aggtrade"]), key=lambda row: _int(row.get("local_recv_monotonic_ns")) or 0)
    latency_rows = sorted(_read_jsonl(dataset_paths["latency_profile"]), key=lambda row: _int(row.get("local_recv_monotonic_ns")) or 0)

    clean_by_ts: dict[int, dict[str, Any]] = {}
    for row in clean_rows:
        row_ts = _int(row.get("local_recv_monotonic_ns"))
        if row_ts is not None:
            clean_by_ts[row_ts] = row
    book_ts = [_int(row.get("local_recv_monotonic_ns")) or 0 for row in bookticker_rows]
    trade_ts = [_int(row.get("local_recv_monotonic_ns")) or 0 for row in trade_rows]
    aggtrade_ts = [_int(row.get("local_recv_monotonic_ns")) or 0 for row in aggtrade_rows]
    latency_ts = [_int(row.get("local_recv_monotonic_ns")) or 0 for row in latency_rows]
    clean_ts = sorted(ts for ts in clean_by_ts if ts is not None)

    samples: list[dict[str, Any]] = []
    for index, label_row in enumerate(label_rows):
        feature_ts = _int(label_row.get("local_recv_monotonic_ns"))
        if feature_ts is None:
            continue
        clean = clean_by_ts.get(feature_ts, {})
        book = _last_row_at_or_before(bookticker_rows, book_ts, feature_ts)
        book_past = _last_row_at_or_before(bookticker_rows, book_ts, feature_ts - 1_000_000_000)
        latency = _last_row_at_or_before(latency_rows, latency_ts, feature_ts)
        trades = _window_rows(trade_rows, trade_ts, feature_ts, 1_000)
        aggtrades = _window_rows(aggtrade_rows, aggtrade_ts, feature_ts, 1_000)

        target_mid = _float(clean.get("mid")) or _float(label_row.get("feature_mid_price"))
        best_bid = _float(clean.get("best_bid")) or _float(label_row.get("feature_best_bid"))
        best_ask = _float(clean.get("best_ask")) or _float(label_row.get("feature_best_ask"))
        target_spread_bps = _float(label_row.get("feature_spread_bps"))
        if target_spread_bps is None and target_mid and best_bid is not None and best_ask is not None:
            target_spread_bps = (best_ask - best_bid) / target_mid * 10_000.0
        raw_bids = clean.get("bids")
        raw_asks = clean.get("asks")
        bids: list[Any] = raw_bids if isinstance(raw_bids, list) else []
        asks: list[Any] = raw_asks if isinstance(raw_asks, list) else []

        ref_mid = _float(book.get("mid_price")) if book else None
        ref_spread_bps = _float(book.get("spread_bps")) if book else None
        ref_ts = _int(book.get("local_recv_monotonic_ns")) if book else None
        quote_age_ms = (feature_ts - ref_ts) / 1_000_000.0 if ref_ts is not None else None
        ref_past_mid = _float(book_past.get("mid_price")) if book_past else None
        ref_return_1s = _return_bps(ref_past_mid, ref_mid)

        signed_qty = 0.0
        signed_notional = 0.0
        for trade in trades:
            qty = _float(trade.get("quantity")) or 0.0
            price = _float(trade.get("price")) or 0.0
            sign = -1.0 if trade.get("is_buyer_market_maker") is True else 1.0
            signed_qty += sign * qty
            signed_notional += sign * qty * price

        agg_signed_qty = 0.0
        for trade in aggtrades:
            qty = _float(trade.get("quantity")) or 0.0
            sign = -1.0 if trade.get("is_buyer_market_maker") is True else 1.0
            agg_signed_qty += sign * qty

        latency_metrics = _dict(latency.get("metrics")) if latency else {}
        latency_end_to_end = _float(latency_metrics.get("end_to_end_local_hot_path_ms"))
        latency_queue_wait = _float(latency_metrics.get("queue_wait_ms"))
        latency_quality = _latency_quality_score(latency_end_to_end, quote_age_ms)

        features = {
            "target_mid_price": target_mid,
            "target_spread_bps": target_spread_bps,
            "target_book_age_ms": _float(clean.get("book_age_ms")),
            "target_top_bid_qty_1": _level_qty(bids, 1),
            "target_top_ask_qty_1": _level_qty(asks, 1),
            "target_book_imbalance_1": _book_imbalance(bids, asks, 1),
            "target_book_imbalance_5": _book_imbalance(bids, asks, 5),
            "target_book_imbalance_20": _book_imbalance(bids, asks, 20),
            "reference_bookticker_mid_price": ref_mid,
            "reference_bookticker_spread_bps": ref_spread_bps,
            "reference_bookticker_age_ms": quote_age_ms,
            "reference_mid_return_1s_bps": ref_return_1s,
            "reference_trade_count_1s": float(len(trades) + len(aggtrades)),
            "reference_signed_trade_qty_1s": signed_qty + agg_signed_qty,
            "reference_signed_trade_notional_1s": signed_notional,
            "repricing_gap_bps": _return_bps(target_mid, ref_mid),
            "latency_end_to_end_hot_path_ms": latency_end_to_end,
            "latency_queue_wait_ms": latency_queue_wait,
            "latency_quality_score": latency_quality,
        }

        source_ts_by_feature = {
            "target_mid_price": feature_ts,
            "target_spread_bps": feature_ts,
            "target_book_age_ms": feature_ts,
            "target_top_bid_qty_1": feature_ts,
            "target_top_ask_qty_1": feature_ts,
            "target_book_imbalance_1": feature_ts,
            "target_book_imbalance_5": feature_ts,
            "target_book_imbalance_20": feature_ts,
            "reference_bookticker_mid_price": ref_ts,
            "reference_bookticker_spread_bps": ref_ts,
            "reference_bookticker_age_ms": ref_ts,
            "reference_mid_return_1s_bps": ref_ts,
            "reference_trade_count_1s": _window_max_ts(trades, aggtrades),
            "reference_signed_trade_qty_1s": _window_max_ts(trades, aggtrades),
            "reference_signed_trade_notional_1s": _window_max_ts(trades, []),
            "repricing_gap_bps": max([ts for ts in (feature_ts, ref_ts) if ts is not None]),
            "latency_end_to_end_hot_path_ms": _int(latency.get("local_recv_monotonic_ns")) if latency else None,
            "latency_queue_wait_ms": _int(latency.get("local_recv_monotonic_ns")) if latency else None,
            "latency_quality_score": max([ts for ts in (ref_ts, _int(latency.get("local_recv_monotonic_ns")) if latency else None) if ts is not None], default=None),
        }
        feature_source_max_ts = max((ts for ts in source_ts_by_feature.values() if ts is not None), default=None)

        label = _dict(_dict(_dict(label_row.get("protocol_labels")).get("depth_mid")).get("exchange_time"))
        label_horizon_ms = _int(label.get("horizon_ms"))
        policy_gap_ms = _float(label.get("max_future_gap_ms"))
        observed_future_gap_ms = _float(label.get("exchange_future_gap_ms"))
        label_future_ts = _int(label.get("future_reference_local_recv_monotonic_ns"))
        future_return = _float(label.get("return_bps"))
        label_valid = (
            label.get("valid") is True
            and label_horizon_ms == PRIMARY_LABEL_HORIZON_MS
            and policy_gap_ms is not None
            and policy_gap_ms <= PRIMARY_LABEL_HORIZON_MS
            and observed_future_gap_ms is not None
            and observed_future_gap_ms <= PRIMARY_LABEL_HORIZON_MS
            and label_future_ts is not None
            and label_future_ts > feature_ts
            and future_return is not None
        )
        direction = _direction(future_return) if label_valid else None
        spread_adjusted_direction = _spread_adjusted_direction(future_return, target_spread_bps) if label_valid else None
        diagnostic_250 = _diagnostic_250ms_return(clean_by_ts, clean_ts, feature_ts, target_mid)

        samples.append(
            {
                "sample_id": f"phase50-{index:08d}-{feature_ts}",
                "source_row_index": index,
                "symbol": label_row.get("symbol") or clean.get("symbol"),
                "feature_ts_ns": feature_ts,
                "feature_ts_wall": label_row.get("local_recv_wall_ts") or clean.get("local_recv_wall_ts"),
                "label_start_ts_ns": feature_ts,
                "label_future_ts_ns": label_future_ts,
                "label_horizon_ms": label_horizon_ms,
                "label_max_future_gap_policy_ms": policy_gap_ms,
                "label_observed_future_gap_ms": observed_future_gap_ms,
                "future_return_100ms_bps": future_return if label_valid else None,
                "direction_100ms": direction,
                "spread_adjusted_direction_100ms": spread_adjusted_direction,
                "valid_100ms_label": label_valid,
                "diagnostic_future_return_250ms_bps": diagnostic_250,
                "features": features,
                "feature_source_ts_ns": source_ts_by_feature,
                "feature_source_max_ts_ns": feature_source_max_ts,
            }
        )

    dataset_summary = {
        "schema_version": "phase_5_0_research_dataset_summary_v1",
        "input_counts": {
            "clean_samples": len(clean_rows),
            "corrected_labels": len(label_rows),
            "bookticker": len(bookticker_rows),
            "trade": len(trade_rows),
            "aggtrade": len(aggtrade_rows),
            "latency_profile": len(latency_rows),
        },
        "sample_count": len(samples),
        "valid_100ms_label_count": sum(1 for sample in samples if sample["valid_100ms_label"]),
        "feature_time_range_ns": _time_range([sample["feature_ts_ns"] for sample in samples]),
        "input_files": {
            name: {
                "path": str(path),
                "size_bytes": path.stat().st_size if path.exists() else 0,
                "sha256": _sha256_file(path) if path.exists() else "",
            }
            for name, path in sorted(dataset_paths.items())
        },
    }
    return samples, dataset_summary


def build_dataset_manifest(
    *,
    root_path: Path,
    bundle_path: Path,
    evidence: dict[str, Any],
    runtime_report: dict[str, Any],
    dataset_summary: dict[str, Any],
) -> dict[str, Any]:
    commit, dirty, status_lines = _git_identity(root_path)
    passed = evidence.get("status") == "pass" and dataset_summary.get("sample_count", 0) > 0
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_dataset_manifest_v1",
        "status": "pass" if passed else "fail",
        "created_at_utc": _utc_now(),
        "source_repo_commit": commit,
        "source_repo_dirty": dirty,
        "source_repo_status_short": status_lines,
        "bundle_filename": bundle_path.name,
        "bundle_sha256": evidence.get("bundle_sha256_actual"),
        "phase42h_pass_evidence": {
            "status": evidence.get("runtime_status"),
            "primary_failure": evidence.get("primary_failure"),
            "phase41_status": evidence.get("phase41_status"),
            "clock_sync_status": evidence.get("clock_sync_status"),
            "strict_100ms_observability_ready": evidence.get("strict_100ms_observability_ready"),
            "low_latency_ready": evidence.get("low_latency_ready"),
            "phase5_ready_false_accepted": evidence.get("phase5_ready") is False,
        },
        "runtime_metrics": _runtime_metrics(runtime_report),
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "diagnostic_horizon_ms": DIAGNOSTIC_LABEL_HORIZON_MS,
        "dataset_summary": dataset_summary,
    }


def build_split_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    valid_samples = sorted((sample for sample in samples if sample.get("valid_100ms_label") is True), key=lambda sample: (sample["feature_ts_ns"], sample["sample_id"]))
    n = len(valid_samples)
    split_ids: dict[str, list[str]] = {"train": [], "validation": [], "test": []}
    if n >= 3:
        train_end = max(1, int(n * 0.60))
        validation_end = max(train_end + 1, int(n * 0.80))
        validation_end = min(validation_end, n - 1)
        assignments = {
            "train": valid_samples[:train_end],
            "validation": valid_samples[train_end:validation_end],
            "test": valid_samples[validation_end:],
        }
    else:
        assignments = {"train": valid_samples, "validation": [], "test": []}
    for split, rows in assignments.items():
        split_ids[split] = [row["sample_id"] for row in rows]
        for row in rows:
            row["split"] = split

    report = {
        "phase": PHASE,
        "schema_version": "phase_5_0_split_report_v1",
        "split_method": "deterministic_chronological_time_based",
        "random_split_used": False,
        "random_split_rejected": True,
        "split_sample_basis": "valid_100ms_label_rows",
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "sample_count": n,
        "splits": {
            split: {
                "sample_count": len(rows),
                "sample_ids": [row["sample_id"] for row in rows],
                "time_range_ns": _time_range([row["feature_ts_ns"] for row in rows]),
            }
            for split, rows in assignments.items()
        },
        "duplicate_sample_ids": _duplicates([sample["sample_id"] for sample in valid_samples]),
        "overlap_pairs": _split_overlap_pairs(split_ids),
        "time_overlap_violations": _time_overlap(assignments),
    }
    validation = validate_split_integrity_report(report)
    report["status"] = validation["status"]
    report["integrity_failure_reasons"] = validation["failure_reasons"]
    return report


def validate_split_integrity_report(split_report: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    if split_report.get("split_method") != "deterministic_chronological_time_based":
        reasons.append("split_method_not_deterministic_chronological")
    if split_report.get("random_split_used") is True:
        reasons.append("random_split_used")
    if split_report.get("random_split_rejected") is not True:
        reasons.append("random_split_not_explicitly_rejected")
    if split_report.get("sample_count", 0) < 3:
        reasons.append("insufficient_samples_for_three_way_split")
    if split_report.get("duplicate_sample_ids"):
        reasons.append("duplicate_sample_ids")
    if split_report.get("overlap_pairs"):
        reasons.append("split_sample_id_overlap")
    if split_report.get("time_overlap_violations"):
        reasons.append("reported_time_overlap")

    splits = _dict(split_report.get("splits"))
    for split in ("train", "validation", "test"):
        payload = _dict(splits.get(split))
        if int(payload.get("sample_count") or 0) <= 0:
            reasons.append(f"{split}_split_empty")

    split_ids = {split: list(_dict(splits.get(split)).get("sample_ids") or []) for split in ("train", "validation", "test")}
    computed_duplicates = _duplicates([sample_id for ids in split_ids.values() for sample_id in ids])
    if computed_duplicates:
        reasons.append("computed_duplicate_sample_ids_across_splits")
    computed_overlap = _split_overlap_pairs(split_ids)
    if computed_overlap:
        reasons.append("computed_split_sample_id_overlap")

    ranges = {split: _dict(_dict(splits.get(split)).get("time_range_ns")) for split in ("train", "validation", "test")}
    train = ranges["train"]
    validation = ranges["validation"]
    test = ranges["test"]
    if _range_has_inversion(train):
        reasons.append("train_time_range_inverted")
    if _range_has_inversion(validation):
        reasons.append("validation_time_range_inverted")
    if _range_has_inversion(test):
        reasons.append("test_time_range_inverted")
    if train.get("max") is not None and validation.get("min") is not None and train["max"] >= validation["min"]:
        reasons.append("train_validation_time_overlap_or_out_of_order")
    if validation.get("max") is not None and test.get("min") is not None and validation["max"] >= test["min"]:
        reasons.append("validation_test_time_overlap_or_out_of_order")
    if train.get("min") is not None and validation.get("min") is not None and train["min"] > validation["min"]:
        reasons.append("train_validation_chronology_out_of_order")
    if validation.get("min") is not None and test.get("min") is not None and validation["min"] > test["min"]:
        reasons.append("validation_test_chronology_out_of_order")

    return {
        "status": "pass" if not reasons else "fail",
        "failure_reasons": sorted(set(reasons)),
    }


def build_feature_schema_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    null_rates: dict[str, float] = {}
    feature_count = max(1, len(samples))
    for definition in FEATURE_DEFINITIONS:
        name = definition["name"]
        null_count = sum(1 for sample in samples if _is_null(_dict(sample.get("features")).get(name)))
        null_rates[name] = null_count / feature_count
    missing_definitions = [
        name
        for name in MODEL_FEATURE_NAMES
        if any(name not in _dict(sample.get("features")) for sample in samples[: min(10, len(samples))])
    ]
    future_flags = [feature["name"] for feature in FEATURE_DEFINITIONS if feature.get("uses_future_data") is not False]
    passed = bool(samples) and not missing_definitions and not future_flags
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_feature_schema_v0",
        "status": "pass" if passed else "fail",
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "features": list(FEATURE_DEFINITIONS),
        "feature_count": len(FEATURE_DEFINITIONS),
        "sample_count": len(samples),
        "null_rates": null_rates,
        "missing_feature_definitions": missing_definitions,
        "features_marked_as_using_future_data": future_flags,
    }


def build_label_validation_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    validate_primary_horizon_ms(PRIMARY_LABEL_HORIZON_MS)
    valid_samples = [sample for sample in samples if sample.get("valid_100ms_label") is True]
    horizon_violations = [
        sample["sample_id"]
        for sample in samples
        if sample.get("label_horizon_ms") not in (None, PRIMARY_LABEL_HORIZON_MS)
    ]
    future_gap_values = [
        float(sample["label_observed_future_gap_ms"])
        for sample in valid_samples
        if sample.get("label_observed_future_gap_ms") is not None
    ]
    max_future_gap_ms = max(future_gap_values) if future_gap_values else None
    future_ts_violations = [
        sample["sample_id"]
        for sample in valid_samples
        if sample.get("label_future_ts_ns") is None or sample["label_future_ts_ns"] <= sample["feature_ts_ns"]
    ]
    max_gap_violation_count = sum(1 for gap in future_gap_values if gap > PRIMARY_LABEL_HORIZON_MS)
    direction_counts = _count_values(str(sample.get("direction_100ms")) for sample in valid_samples)
    diagnostic_count = sum(1 for sample in samples if sample.get("diagnostic_future_return_250ms_bps") is not None)
    passed = bool(valid_samples) and not horizon_violations and not future_ts_violations and max_gap_violation_count == 0
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_label_validation_v1",
        "status": "pass" if passed else "fail",
        "primary_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "primary_horizon_relaxed_to_250ms": False,
        "max_future_gap_policy_ms": PRIMARY_LABEL_HORIZON_MS,
        "max_future_gap_ms": max_future_gap_ms,
        "generated_fields": list(LABEL_FIELD_NAMES),
        "sample_count": len(samples),
        "valid_100ms_label_count": len(valid_samples),
        "valid_100ms_label_rate": len(valid_samples) / len(samples) if samples else 0.0,
        "direction_counts": direction_counts,
        "spread_adjusted_direction_counts": _count_values(str(sample.get("spread_adjusted_direction_100ms")) for sample in valid_samples),
        "horizon_violation_count": len(horizon_violations),
        "horizon_violation_sample_ids": horizon_violations[:20],
        "future_ts_violation_count": len(future_ts_violations),
        "future_ts_violation_sample_ids": future_ts_violations[:20],
        "max_future_gap_violation_count": max_gap_violation_count,
        "diagnostic_horizon_ms": DIAGNOSTIC_LABEL_HORIZON_MS,
        "diagnostic_is_primary": False,
        "diagnostic_250ms_label_count": diagnostic_count,
    }


def validate_primary_horizon_ms(primary_horizon_ms: int) -> None:
    if primary_horizon_ms != PRIMARY_LABEL_HORIZON_MS:
        raise ValueError("Phase 5.0 primary label horizon must remain exactly 100ms; 250ms is diagnostic only")


def build_leakage_report(
    samples: list[dict[str, Any]],
    split_report: dict[str, Any],
    *,
    feature_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    feature_after_label_start = [
        sample["sample_id"]
        for sample in samples
        if sample.get("feature_ts_ns") is not None
        and sample.get("label_start_ts_ns") is not None
        and sample["feature_ts_ns"] > sample["label_start_ts_ns"]
    ]
    label_future_not_after_feature = [
        sample["sample_id"]
        for sample in samples
        if sample.get("valid_100ms_label") is True
        and (sample.get("label_future_ts_ns") is None or sample["label_future_ts_ns"] <= sample["feature_ts_ns"])
    ]
    feature_source_future = [
        sample["sample_id"]
        for sample in samples
        if _sample_feature_source_max_ts(sample) is not None and _sample_feature_source_max_ts(sample) > sample["feature_ts_ns"]
    ]
    detected_feature_names = set(feature_names or MODEL_FEATURE_NAMES)
    for sample in samples:
        detected_feature_names.update(_dict(sample.get("features")).keys())
    future_price_or_orderbook_fields = sorted(
        name
        for name in detected_feature_names
        if _looks_like_future_price_or_orderbook_feature(name)
    )
    label_derived_features = sorted(name for name in detected_feature_names if _looks_like_label_derived_feature(name))
    duplicate_ids: list[str] = list(split_report.get("duplicate_sample_ids", []))
    overlap_pairs: list[list[str]] = list(split_report.get("overlap_pairs", []))
    violation_count = (
        len(feature_after_label_start)
        + len(label_future_not_after_feature)
        + len(feature_source_future)
        + len(future_price_or_orderbook_fields)
        + len(label_derived_features)
        + len(duplicate_ids)
        + len(overlap_pairs)
    )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_leakage_check_v1",
        "status": "pass" if violation_count == 0 else "fail",
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "feature_ts_lte_label_start_ts": len(feature_after_label_start) == 0,
        "feature_ts_after_label_start_count": len(feature_after_label_start),
        "feature_ts_after_label_start_sample_ids": feature_after_label_start[:20],
        "label_future_ts_gt_feature_ts": len(label_future_not_after_feature) == 0,
        "label_future_ts_violation_count": len(label_future_not_after_feature),
        "label_future_ts_violation_sample_ids": label_future_not_after_feature[:20],
        "feature_source_ts_lte_feature_ts": len(feature_source_future) == 0,
        "feature_source_future_violation_count": len(feature_source_future),
        "feature_source_future_sample_ids": feature_source_future[:20],
        "future_price_or_orderbook_fields_in_features": future_price_or_orderbook_fields,
        "label_derived_features": label_derived_features,
        "split_overlap_pairs": overlap_pairs,
        "duplicate_sample_ids_across_splits": duplicate_ids,
        "total_violation_count": violation_count,
    }


def build_bucket_edge_report(samples: list[dict[str, Any]], split_report: dict[str, Any]) -> dict[str, Any]:
    split_lookup = _split_lookup(split_report)
    valid_samples = [sample for sample in samples if sample.get("sample_id") in split_lookup]
    for sample in valid_samples:
        sample["split"] = split_lookup[sample["sample_id"]]
    specs = (
        ("repricing_gap_bps", "repricing_gap_bps"),
        ("book_imbalance", "target_book_imbalance_5"),
        ("spread", "target_spread_bps"),
        ("quote_age", "reference_bookticker_age_ms"),
        ("latency_quality", "latency_quality_score"),
        ("trade_pressure", "reference_signed_trade_qty_1s"),
    )
    low_sample_threshold = 30
    bucket_reports: list[dict[str, Any]] = []
    for display_name, feature_name in specs:
        train_values = [
            _float(_dict(sample.get("features")).get(feature_name))
            for sample in valid_samples
            if sample.get("split") == "train" and _float(_dict(sample.get("features")).get(feature_name)) is not None
        ]
        edges = _quantile_edges([value for value in train_values if value is not None], bins=5)
        bucket_names = sorted({_bucket_for_value(_float(_dict(sample.get("features")).get(feature_name)), edges) for sample in valid_samples})
        for bucket_name in bucket_names:
            train_bucket = [
                sample
                for sample in valid_samples
                if sample.get("split") == "train"
                and _bucket_for_value(_float(_dict(sample.get("features")).get(feature_name)), edges) == bucket_name
            ]
            signal_direction = _sign(_mean([_float(sample.get("future_return_100ms_bps")) for sample in train_bucket]))
            if signal_direction == 0:
                signal_direction = 1
            split_stats = {
                split: _bucket_stats(
                    [
                        sample
                        for sample in valid_samples
                        if sample.get("split") == split
                        and _bucket_for_value(_float(_dict(sample.get("features")).get(feature_name)), edges) == bucket_name
                    ],
                    signal_direction=signal_direction,
                    low_sample_threshold=low_sample_threshold,
                )
                for split in ("train", "validation", "test")
            }
            validation_supported = split_stats["validation"]["sample_count"] >= low_sample_threshold and split_stats["validation"]["edge_after_cost_bps"] > 0
            test_supported = split_stats["test"]["sample_count"] >= low_sample_threshold and split_stats["test"]["edge_after_cost_bps"] > 0
            bucket_reports.append(
                {
                    "bucket_feature": display_name,
                    "feature_name": feature_name,
                    "bucket": bucket_name,
                    "train_quantile_edges": edges,
                    "train_signal_direction": signal_direction,
                    "conservative_cost_assumptions": CONSERVATIVE_COST_ASSUMPTIONS,
                    "splits": split_stats,
                    "low_sample_bucket": any(stats["low_sample_bucket"] for stats in split_stats.values()),
                    "split_stability": {
                        "validation_supports_train_direction_after_cost": validation_supported,
                        "test_supports_train_direction_after_cost": test_supported,
                        "validation_and_test_support": validation_supported and test_supported,
                    },
                }
            )
    stable_buckets = [
        bucket
        for bucket in bucket_reports
        if bucket["split_stability"]["validation_and_test_support"] is True
        and not bucket["low_sample_bucket"]
    ]
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_bucket_edge_v1",
        "status": "pass" if valid_samples else "fail",
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "sample_count": len(valid_samples),
        "bucket_features": [spec[0] for spec in specs],
        "low_sample_threshold": low_sample_threshold,
        "conservative_cost_assumptions": CONSERVATIVE_COST_ASSUMPTIONS,
        "buckets": bucket_reports,
        "stable_edge_bucket_count": len(stable_buckets),
        "edge_claim_allowed": len(stable_buckets) > 0,
    }


def build_model_baseline_report(samples: list[dict[str, Any]], split_report: dict[str, Any]) -> dict[str, Any]:
    split_lookup = _split_lookup(split_report)
    valid_samples = [sample for sample in samples if sample.get("sample_id") in split_lookup]
    for sample in valid_samples:
        sample["split"] = split_lookup[sample["sample_id"]]
    train = [sample for sample in valid_samples if sample.get("split") == "train"]
    validation = [sample for sample in valid_samples if sample.get("split") == "validation"]
    test = [sample for sample in valid_samples if sample.get("split") == "test"]
    y_train = [1 if sample.get("direction_100ms") == 1 else 0 for sample in train]
    if len(set(y_train)) < 2:
        return {
            "phase": PHASE,
            "schema_version": "phase_5_0_model_baseline_v1",
            "status": "skipped",
            "allowed_model_type": "l2_logistic_regression",
            "skip_reason": "training split has fewer than two classes",
            "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
            "sample_counts": {"train": len(train), "validation": len(validation), "test": len(test)},
            "edge_claim_allowed": False,
        }
    model = _fit_logistic_regression(train, MODEL_FEATURE_NAMES)
    split_metrics = {
        "train": _model_split_metrics(train, model),
        "validation": _model_split_metrics(validation, model),
        "test": _model_split_metrics(test, model),
    }
    validation_auc = split_metrics["validation"]["auc"]
    test_auc = split_metrics["test"]["auc"]
    validation_top_edge = _top_bucket_edge(split_metrics["validation"])
    test_top_edge = _top_bucket_edge(split_metrics["test"])
    edge_claim_allowed = (
        validation_top_edge is not None
        and test_top_edge is not None
        and validation_top_edge > 0
        and test_top_edge > 0
        and validation_auc is not None
        and test_auc is not None
        and validation_auc >= 0.52
        and test_auc >= 0.52
    )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_model_baseline_v1",
        "status": "pass",
        "allowed_model_type": "l2_logistic_regression",
        "forbidden_model_families_excluded": ["deep_learning", "large_ensemble", "reinforcement_learning"],
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "feature_names": list(MODEL_FEATURE_NAMES),
        "sample_counts": {"train": len(train), "validation": len(validation), "test": len(test)},
        "regularization": {"l2_lambda": model["l2_lambda"], "epochs": model["epochs"]},
        "metrics": split_metrics,
        "validation_to_test_degradation": {
            "auc_degradation": validation_auc - test_auc if validation_auc is not None and test_auc is not None else None,
            "top_prediction_bucket_edge_after_cost_bps_degradation": validation_top_edge - test_top_edge
            if validation_top_edge is not None and test_top_edge is not None
            else None,
        },
        "conservative_cost_assumptions": CONSERVATIVE_COST_ASSUMPTIONS,
        "edge_claim_allowed": edge_claim_allowed,
    }


def bucket_edge_claim_supported(bucket_edge: dict[str, Any]) -> bool:
    if bucket_edge.get("status") != "pass" or bucket_edge.get("edge_claim_allowed") is not True:
        return False
    if int(bucket_edge.get("stable_edge_bucket_count") or 0) <= 0:
        return False
    for bucket in bucket_edge.get("buckets", []):
        if bucket.get("low_sample_bucket") is True:
            continue
        stability = _dict(bucket.get("split_stability"))
        splits = _dict(bucket.get("splits"))
        validation = _dict(splits.get("validation"))
        test = _dict(splits.get("test"))
        if (
            stability.get("validation_and_test_support") is True
            and validation.get("edge_after_cost_bps") is not None
            and test.get("edge_after_cost_bps") is not None
            and validation["edge_after_cost_bps"] > 0
            and test["edge_after_cost_bps"] > 0
        ):
            return True
    return False


def model_edge_claim_supported(model_baseline: dict[str, Any]) -> bool:
    if model_baseline.get("status") != "pass" or model_baseline.get("edge_claim_allowed") is not True:
        return False
    metrics = _dict(model_baseline.get("metrics"))
    validation = _dict(metrics.get("validation"))
    test = _dict(metrics.get("test"))
    validation_auc = _float(validation.get("auc"))
    test_auc = _float(test.get("auc"))
    validation_top_edge = _top_bucket_edge(validation)
    test_top_edge = _top_bucket_edge(test)
    return (
        validation_auc is not None
        and test_auc is not None
        and validation_auc >= 0.52
        and test_auc >= 0.52
        and validation_top_edge is not None
        and test_top_edge is not None
        and validation_top_edge > 0
        and test_top_edge > 0
    )


def build_final_report(
    *,
    source_gate: dict[str, Any],
    evidence: dict[str, Any],
    manifest: dict[str, Any],
    split_report: dict[str, Any],
    feature_schema: dict[str, Any],
    label_report: dict[str, Any],
    leakage: dict[str, Any],
    bucket_edge: dict[str, Any],
    model_baseline: dict[str, Any],
) -> dict[str, Any]:
    split_validation = validate_split_integrity_report(split_report)
    bucket_claim_supported = bucket_edge_claim_supported(bucket_edge)
    model_claim_supported = model_edge_claim_supported(model_baseline)
    gates = {
        "source_reproducibility_gate": source_gate.get("status") == "pass",
        "phase42h_evidence_integrity_gate": evidence.get("status") == "pass",
        "dataset_manifest_gate": manifest.get("status") == "pass",
        "time_based_split_gate": split_report.get("status") == "pass" and split_validation["status"] == "pass",
        "feature_schema_gate": feature_schema.get("status") == "pass",
        "strict_100ms_label_gate": label_report.get("status") == "pass" and label_report.get("primary_horizon_ms") == PRIMARY_LABEL_HORIZON_MS,
        "leakage_gate": leakage.get("status") == "pass",
        "bucket_edge_report_gate": bucket_edge.get("status") == "pass",
        "model_baseline_report_gate": model_baseline.get("status") in {"pass", "skipped"},
        "no_live_trading_or_execution_scope_gate": True,
    }
    blockers = [name for name, passed in gates.items() if not passed]
    warnings: list[str] = []
    if not bucket_claim_supported:
        warnings.append("No bucket edge survived conservative costs with validation and test support.")
    if not model_claim_supported:
        warnings.append("Baseline model did not provide validation and test support after conservative costs.")
    if blockers:
        conclusion = "EDGE_FAILED"
    elif bucket_claim_supported and model_claim_supported:
        conclusion = "EDGE_PROVEN"
    else:
        conclusion = "EDGE_INCONCLUSIVE"
    return {
        "phase": PHASE,
        "schema_version": "phase_5_0_final_report_v1",
        "created_at_utc": _utc_now(),
        "edge_conclusion": conclusion,
        "allowed_edge_conclusions": ["EDGE_PROVEN", "EDGE_INCONCLUSIVE", "EDGE_FAILED"],
        "primary_label_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "diagnostic_horizon_ms": DIAGNOSTIC_LABEL_HORIZON_MS,
        "gates": gates,
        "blockers": blockers,
        "warnings": warnings,
        "audit_validation": {
            "split_integrity": split_validation,
            "bucket_edge_claim_supported": bucket_claim_supported,
            "model_edge_claim_supported": model_claim_supported,
        },
        "research_scope_confirmation": {
            "live_trading": False,
            "order_execution": False,
            "private_key_or_wallet_logic": False,
            "copy_trading": False,
            "production_strategy_execution": False,
        },
        "evidence_integrity_report": str(PHASE50_EVIDENCE_INTEGRITY_REPORT).replace("\\", "/"),
        "dataset_manifest": str(PHASE50_DATASET_MANIFEST).replace("\\", "/"),
        "split_report": str(PHASE50_SPLIT_REPORT).replace("\\", "/"),
        "feature_schema": str(PHASE50_FEATURE_SCHEMA).replace("\\", "/"),
        "label_validation_report": str(PHASE50_LABEL_VALIDATION_REPORT).replace("\\", "/"),
        "leakage_check": str(PHASE50_LEAKAGE_CHECK).replace("\\", "/"),
        "bucket_edge_report": str(PHASE50_BUCKET_EDGE_REPORT).replace("\\", "/"),
        "model_baseline_report": str(PHASE50_MODEL_BASELINE_REPORT).replace("\\", "/"),
        "runner_command_log": str(PHASE50_RUNNER_COMMAND_LOG).replace("\\", "/"),
    }


def render_final_markdown(report: dict[str, Any]) -> str:
    gates = _dict(report.get("gates"))
    lines = [
        "# Phase 5.0 Microstructure Empirical Signal Research",
        "",
        f"Created UTC: {report.get('created_at_utc')}",
        f"Primary label horizon: {report.get('primary_label_horizon_ms')}ms",
        f"Diagnostic horizon: {report.get('diagnostic_horizon_ms')}ms",
        "",
        f"Edge conclusion: {report.get('edge_conclusion')}",
        "",
        "## Gates",
    ]
    for name, passed in gates.items():
        lines.append(f"- {name}: {'pass' if passed else 'fail'}")
    lines.extend(["", "## Blockers"])
    blockers = list(report.get("blockers") or [])
    lines.extend([f"- {blocker}" for blocker in blockers] or ["- none"])
    lines.extend(["", "## Warnings"])
    warnings = list(report.get("warnings") or [])
    lines.extend([f"- {warning}" for warning in warnings] or ["- none"])
    lines.extend(
        [
            "",
            "## Scope",
            "- Research only: no live trading, no order execution, no wallet or private-key logic, no copy trading, and no production strategy execution.",
            "",
            "## Artifact Paths",
            f"- JSON report: {PHASE50_FINAL_REPORT_JSON.as_posix()}",
            f"- Markdown report: {PHASE50_FINAL_REPORT_MD.as_posix()}",
            f"- Bundle: {PHASE50_BUNDLE.as_posix()}",
            "",
        ]
    )
    return "\n".join(lines)


def render_runner_command_log(report: dict[str, Any], bundle_path: Path, sha256_path: Path) -> str:
    return "\n".join(
        [
            "Phase 5.0 offline runner summary",
            f"created_at_utc: {report.get('created_at_utc')}",
            "command: python -X utf8 scripts/run_phase50_microstructure_signal_research.py",
            f"input_bundle: {bundle_path}",
            f"input_sha256_file: {sha256_path}",
            f"final_report_json: {PHASE50_FINAL_REPORT_JSON.as_posix()}",
            f"final_report_md: {PHASE50_FINAL_REPORT_MD.as_posix()}",
            f"bundle: {PHASE50_BUNDLE.as_posix()}",
            f"edge_conclusion: {report.get('edge_conclusion')}",
            "offline_mode: true",
            "live_vps_required: false",
            "",
        ]
    )


def create_phase50_bundle(root_path: Path) -> dict[str, Any]:
    bundle_path = root_path / PHASE50_BUNDLE
    required = [
        PHASE50_SOURCE_REPRODUCIBILITY_REPORT,
        PHASE50_EVIDENCE_INTEGRITY_REPORT,
        PHASE50_DATASET_MANIFEST,
        PHASE50_SPLIT_REPORT,
        PHASE50_FEATURE_SCHEMA,
        PHASE50_LABEL_VALIDATION_REPORT,
        PHASE50_LEAKAGE_CHECK,
        PHASE50_BUCKET_EDGE_REPORT,
        PHASE50_MODEL_BASELINE_REPORT,
        PHASE50_RUNNER_COMMAND_LOG,
        PHASE50_FINAL_REPORT_JSON,
        PHASE50_FINAL_REPORT_MD,
        Path("scripts/run_phase50_microstructure_signal_research.py"),
        Path("bot/app/research/microstructure_signal_research.py"),
        Path("pyproject.toml"),
    ]
    if (root_path / PHASE50_PYTEST_CONSOLE_LOG).exists():
        required.append(PHASE50_PYTEST_CONSOLE_LOG)
    required.extend(sorted(Path("tests").glob("test_phase50_*.py")))
    required.append(Path("tests/phase50_test_utils.py"))
    missing = [path.as_posix() for path in required if not (root_path / path).exists()]
    if bundle_path.exists():
        bundle_path.unlink()
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in required:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative.as_posix())
    return {
        "path": PHASE50_BUNDLE.as_posix(),
        "sha256": _sha256_file(bundle_path),
        "size_bytes": bundle_path.stat().st_size,
        "missing_files": missing,
        "created": True,
    }


def _runtime_metrics(runtime_report: dict[str, Any]) -> dict[str, Any]:
    phase41 = _dict(runtime_report.get("phase41_runtime_report"))
    return {
        "duration_sec": runtime_report.get("duration_sec"),
        "symbol": runtime_report.get("symbol"),
        "clock_offset_summary": runtime_report.get("clock_offset_summary"),
        "hot_path_latency_summary": runtime_report.get("hot_path_latency_summary"),
        "queue_backpressure_summary": runtime_report.get("queue_backpressure_summary"),
        "writer_batch_report": runtime_report.get("writer_batch_report"),
        "phase41_snapshot_copy_budget_met": phase41.get("snapshot_copy_budget_met"),
        "phase41_snapshot_copy_p99_us": phase41.get("snapshot_copy_p99_us"),
        "phase41_snapshot_copy_budget_us": phase41.get("snapshot_copy_budget_us"),
    }


def _fit_logistic_regression(train: list[dict[str, Any]], feature_names: tuple[str, ...]) -> dict[str, Any]:
    matrix = [[_float(_dict(sample.get("features")).get(name)) for name in feature_names] for sample in train]
    means: list[float] = []
    stds: list[float] = []
    for column_index in range(len(feature_names)):
        values: list[float] = []
        for row in matrix:
            value = row[column_index]
            if value is not None and not _is_null(value):
                values.append(float(value))
        mean = statistics.fmean(values) if values else 0.0
        std = statistics.pstdev(values) if len(values) > 1 else 1.0
        means.append(mean)
        stds.append(std if std > 1e-12 else 1.0)
    x_train = [_standardize(row, means, stds) for row in matrix]
    y_train = [1.0 if sample.get("direction_100ms") == 1 else 0.0 for sample in train]
    positive_rate = min(0.99, max(0.01, statistics.fmean(y_train)))
    bias = math.log(positive_rate / (1.0 - positive_rate))
    weights = [0.0 for _ in feature_names]
    l2_lambda = 0.01
    epochs = 100
    learning_rate = 0.08
    n = max(1, len(x_train))
    for _ in range(epochs):
        grad_w = [0.0 for _ in feature_names]
        grad_b = 0.0
        for row, target in zip(x_train, y_train):
            pred = _sigmoid(sum(weight * value for weight, value in zip(weights, row)) + bias)
            error = pred - target
            grad_b += error
            for index, value in enumerate(row):
                grad_w[index] += error * value
        bias -= learning_rate * grad_b / n
        for index, grad in enumerate(grad_w):
            weights[index] -= learning_rate * ((grad / n) + l2_lambda * weights[index])
    return {
        "feature_names": list(feature_names),
        "means": means,
        "stds": stds,
        "weights": weights,
        "bias": bias,
        "l2_lambda": l2_lambda,
        "epochs": epochs,
    }


def _model_split_metrics(samples: list[dict[str, Any]], model: dict[str, Any]) -> dict[str, Any]:
    scored: list[dict[str, Any]] = []
    for sample in samples:
        probability = _predict_probability(sample, model)
        scored.append(
            {
                "sample_id": sample["sample_id"],
                "probability": probability,
                "target": 1 if sample.get("direction_100ms") == 1 else 0,
                "future_return_100ms_bps": _float(sample.get("future_return_100ms_bps")) or 0.0,
            }
        )
    y = [row["target"] for row in scored]
    scores = [row["probability"] for row in scored]
    return {
        "sample_count": len(scored),
        "positive_rate": statistics.fmean(y) if y else None,
        "auc": _auc(y, scores),
        "precision_at_top_k": {
            "top_5pct": _precision_at_top_k(scored, 0.05),
            "top_10pct": _precision_at_top_k(scored, 0.10),
        },
        "calibration": _calibration(scored),
        "expected_return_bps_by_prediction_bucket": _prediction_return_buckets(scored),
    }


def _predict_probability(sample: dict[str, Any], model: dict[str, Any]) -> float:
    feature_names = list(model["feature_names"])
    row = [_float(_dict(sample.get("features")).get(name)) for name in feature_names]
    x = _standardize(row, list(model["means"]), list(model["stds"]))
    logit = sum(weight * value for weight, value in zip(model["weights"], x)) + model["bias"]
    return _sigmoid(logit)


def _standardize(row: list[float | None], means: list[float], stds: list[float]) -> list[float]:
    values: list[float] = []
    for value, mean, std in zip(row, means, stds):
        numeric = mean if value is None or _is_null(value) else float(value)
        values.append((numeric - mean) / std)
    return values


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _auc(targets: list[int], scores: list[float]) -> float | None:
    positives = sum(1 for target in targets if target == 1)
    negatives = len(targets) - positives
    if positives == 0 or negatives == 0:
        return None
    order = sorted(zip(scores, targets), key=lambda item: item[0])
    rank_sum = 0.0
    rank = 1
    index = 0
    while index < len(order):
        end = index + 1
        while end < len(order) and order[end][0] == order[index][0]:
            end += 1
        avg_rank = (rank + rank + (end - index) - 1) / 2.0
        rank_sum += avg_rank * sum(1 for _, target in order[index:end] if target == 1)
        rank += end - index
        index = end
    return (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def _precision_at_top_k(scored: list[dict[str, Any]], fraction: float) -> float | None:
    if not scored:
        return None
    count = max(1, int(math.ceil(len(scored) * fraction)))
    top = sorted(scored, key=lambda row: row["probability"], reverse=True)[:count]
    return sum(row["target"] for row in top) / len(top)


def _calibration(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not scored:
        return []
    ordered = sorted(scored, key=lambda row: row["probability"])
    buckets: list[dict[str, Any]] = []
    for bucket_index, rows in enumerate(_equal_count_chunks(ordered, 5), start=1):
        if not rows:
            continue
        buckets.append(
            {
                "bucket": f"p{bucket_index}",
                "sample_count": len(rows),
                "avg_predicted_probability": statistics.fmean(row["probability"] for row in rows),
                "observed_positive_rate": statistics.fmean(row["target"] for row in rows),
            }
        )
    return buckets


def _prediction_return_buckets(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not scored:
        return []
    ordered = sorted(scored, key=lambda row: row["probability"])
    buckets: list[dict[str, Any]] = []
    for bucket_index, rows in enumerate(_equal_count_chunks(ordered, 5), start=1):
        returns = [row["future_return_100ms_bps"] for row in rows]
        avg_return = statistics.fmean(returns) if returns else 0.0
        buckets.append(
            {
                "bucket": f"p{bucket_index}",
                "sample_count": len(rows),
                "min_probability": min(row["probability"] for row in rows),
                "max_probability": max(row["probability"] for row in rows),
                "avg_future_return_100ms_bps": avg_return,
                "edge_after_cost_bps": avg_return - CONSERVATIVE_COST_ASSUMPTIONS["total_cost_bps"],
            }
        )
    return buckets


def _top_bucket_edge(metrics: dict[str, Any]) -> float | None:
    buckets = list(metrics.get("expected_return_bps_by_prediction_bucket") or [])
    if not buckets:
        return None
    return buckets[-1].get("edge_after_cost_bps")


def _range_has_inversion(time_range: dict[str, Any]) -> bool:
    return time_range.get("min") is not None and time_range.get("max") is not None and time_range["min"] > time_range["max"]


def _sample_feature_source_max_ts(sample: dict[str, Any]) -> int | None:
    explicit = _int(sample.get("feature_source_max_ts_ns"))
    if explicit is not None:
        return explicit
    values: list[int] = []
    for value in _dict(sample.get("feature_source_ts_ns")).values():
        parsed = _int(value)
        if parsed is not None:
            values.append(parsed)
    return max(values) if values else None


def _looks_like_future_price_or_orderbook_feature(name: str) -> bool:
    lowered = name.lower()
    has_future_marker = any(marker in lowered for marker in ("future", "next_", "ahead", "lookahead"))
    has_market_field = any(marker in lowered for marker in ("price", "mid", "bid", "ask", "book", "orderbook", "quote"))
    return has_future_marker and has_market_field


def _looks_like_label_derived_feature(name: str) -> bool:
    lowered = name.lower()
    return (
        name in LABEL_FIELD_NAMES
        or "label" in lowered
        or "direction_100ms" in lowered
        or "return_100ms" in lowered
        or "valid_100ms" in lowered
    )


def _bucket_stats(samples: list[dict[str, Any]], *, signal_direction: int, low_sample_threshold: int) -> dict[str, Any]:
    returns = [_float(sample.get("future_return_100ms_bps")) for sample in samples]
    clean_returns = [float(value) for value in returns if value is not None]
    signal_returns = [value * signal_direction for value in clean_returns]
    avg_return = statistics.fmean(clean_returns) if clean_returns else 0.0
    expected_signal_return = statistics.fmean(signal_returns) if signal_returns else 0.0
    return {
        "sample_count": len(samples),
        "hit_rate": sum(1 for value in signal_returns if value > 0) / len(signal_returns) if signal_returns else None,
        "avg_future_return_bps": avg_return,
        "median_future_return_bps": _quantile(clean_returns, 0.50),
        "p25_future_return_bps": _quantile(clean_returns, 0.25),
        "p75_future_return_bps": _quantile(clean_returns, 0.75),
        "expected_signal_return_bps": expected_signal_return,
        "edge_after_cost_bps": expected_signal_return - CONSERVATIVE_COST_ASSUMPTIONS["total_cost_bps"],
        "low_sample_bucket": len(samples) < low_sample_threshold,
    }


def _quantile_edges(values: list[float], *, bins: int) -> list[float]:
    clean = sorted(value for value in values if not _is_null(value))
    if not clean:
        return []
    edges = []
    for index in range(1, bins):
        edge = _quantile(clean, index / bins)
        if edge is not None:
            edges.append(edge)
    unique: list[float] = []
    for edge in edges:
        if not unique or abs(edge - unique[-1]) > 1e-12:
            unique.append(edge)
    return unique


def _bucket_for_value(value: float | None, edges: list[float]) -> str:
    if value is None or _is_null(value):
        return "null"
    bucket_index = bisect_right(edges, float(value)) + 1
    return f"q{bucket_index}"


def _quantile(values: list[float], q: float) -> float | None:
    clean = sorted(value for value in values if not _is_null(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[int(position)]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _last_row_at_or_before(rows: list[dict[str, Any]], ts_values: list[int], ts: int) -> dict[str, Any] | None:
    index = bisect_right(ts_values, ts) - 1
    if index < 0:
        return None
    return rows[index]


def _window_rows(rows: list[dict[str, Any]], ts_values: list[int], end_ts: int, window_ms: int) -> list[dict[str, Any]]:
    start_ts = end_ts - window_ms * 1_000_000
    left = bisect_left(ts_values, start_ts)
    right = bisect_right(ts_values, end_ts)
    return rows[left:right]


def _window_max_ts(*row_groups: list[dict[str, Any]]) -> int | None:
    values: list[int] = []
    for rows in row_groups:
        for row in rows:
            parsed = _int(row.get("local_recv_monotonic_ns"))
            if parsed is not None:
                values.append(parsed)
    return max(values) if values else None


def _diagnostic_250ms_return(clean_by_ts: dict[int, dict[str, Any]], clean_ts: list[int], feature_ts: int, start_mid: float | None) -> float | None:
    if start_mid is None:
        return None
    target_ts = feature_ts + DIAGNOSTIC_LABEL_HORIZON_MS * 1_000_000
    index = bisect_left(clean_ts, target_ts)
    if index >= len(clean_ts):
        return None
    future_ts = clean_ts[index]
    if (future_ts - target_ts) / 1_000_000.0 > DIAGNOSTIC_LABEL_HORIZON_MS:
        return None
    return _return_bps(start_mid, _float(clean_by_ts[future_ts].get("mid")))


def _book_imbalance(bids: list[Any], asks: list[Any], depth: int) -> float | None:
    bid_qty = _level_qty(bids, depth)
    ask_qty = _level_qty(asks, depth)
    if bid_qty is None or ask_qty is None:
        return None
    total = bid_qty + ask_qty
    if total <= 0:
        return None
    return (bid_qty - ask_qty) / total


def _level_qty(levels: list[Any], depth: int) -> float | None:
    total = 0.0
    count = 0
    for level in levels[:depth]:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            qty = _float(level[1])
            if qty is not None:
                total += qty
                count += 1
    return total if count else None


def _return_bps(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start * 10_000.0


def _direction(value: float | None) -> int | None:
    if value is None:
        return None
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _spread_adjusted_direction(value: float | None, spread_bps: float | None) -> int | None:
    if value is None:
        return None
    threshold = max(0.0, spread_bps or 0.0)
    if value > threshold:
        return 1
    if value < -threshold:
        return -1
    return 0


def _latency_quality_score(latency_ms: float | None, quote_age_ms: float | None) -> float | None:
    if latency_ms is None and quote_age_ms is None:
        return None
    latency = max(0.0, latency_ms or 0.0)
    quote_age = max(0.0, quote_age_ms or 0.0)
    return 1.0 / (1.0 + latency + quote_age / 100.0)


def _git_identity(root_path: Path) -> tuple[str, bool, list[str]]:
    commit = _run_git(root_path, ["rev-parse", "HEAD"]).strip() or "unknown"
    status = _run_git(root_path, ["status", "--short"]).splitlines()
    return commit, bool(status), status


def _run_git(root_path: Path, args: list[str]) -> str:
    try:
        process = subprocess.run(["git", *args], cwd=root_path, text=True, capture_output=True, check=False)
    except OSError:
        return ""
    return process.stdout if process.returncode == 0 else ""


def _parse_sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"sha256:\s*([a-fA-F0-9]{64})", text)
    if match:
        return match.group(1).lower()
    match = re.search(r"\b([a-fA-F0-9]{64})\b", text)
    return match.group(1).lower() if match else ""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_clear_directory(root_path: Path, target: Path) -> None:
    resolved_root = root_path.resolve()
    resolved_target = target.resolve()
    if target.exists():
        if not resolved_target.is_relative_to(resolved_root):
            raise RuntimeError(f"refusing to clear path outside repository: {target}")
        shutil.rmtree(target)


def _safe_extract(archive: zipfile.ZipFile, destination: Path) -> None:
    resolved_destination = destination.resolve()
    for member in archive.namelist():
        target = (destination / member).resolve()
        if not target.is_relative_to(resolved_destination):
            raise RuntimeError(f"unsafe zip member path: {member}")
    archive.extractall(destination)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _ensure_output_dirs(root_path: Path) -> None:
    (root_path / "data/debug").mkdir(parents=True, exist_ok=True)
    (root_path / "data/reports").mkdir(parents=True, exist_ok=True)
    (root_path / "data/cache").mkdir(parents=True, exist_ok=True)


def _relative_display(root_path: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root_path.resolve()).as_posix()
    except ValueError:
        return str(path)


def _time_range(values: list[int]) -> dict[str, int | None]:
    clean = [value for value in values if value is not None]
    return {"min": min(clean) if clean else None, "max": max(clean) if clean else None}


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _split_overlap_pairs(split_ids: dict[str, list[str]]) -> list[list[str]]:
    pairs: list[list[str]] = []
    names = list(split_ids)
    for left_index, left in enumerate(names):
        for right in names[left_index + 1 :]:
            overlap = sorted(set(split_ids[left]) & set(split_ids[right]))
            if overlap:
                pairs.append([left, right, ",".join(overlap[:5])])
    return pairs


def _time_overlap(assignments: dict[str, list[dict[str, Any]]]) -> list[str]:
    ranges = {
        split: _time_range([sample["feature_ts_ns"] for sample in rows])
        for split, rows in assignments.items()
    }
    violations: list[str] = []
    ordered = ["train", "validation", "test"]
    for left, right in zip(ordered, ordered[1:]):
        left_max = ranges[left]["max"]
        right_min = ranges[right]["min"]
        if left_max is not None and right_min is not None and left_max >= right_min:
            violations.append(f"{left}_max_ts_overlaps_{right}_min_ts")
    return violations


def _split_lookup(split_report: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for split, payload in _dict(split_report.get("splits")).items():
        for sample_id in payload.get("sample_ids", []):
            lookup[str(sample_id)] = split
    return lookup


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and not _is_null(value)]
    return statistics.fmean(clean) if clean else None


def _sign(value: float | None) -> int:
    if value is None:
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _is_null(value: Any) -> bool:
    return value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))


def _count_values(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _equal_count_chunks(rows: list[dict[str, Any]], count: int) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    chunks: list[list[dict[str, Any]]] = []
    for index in range(count):
        start = int(index * len(rows) / count)
        end = int((index + 1) * len(rows) / count)
        chunks.append(rows[start:end])
    return chunks


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
