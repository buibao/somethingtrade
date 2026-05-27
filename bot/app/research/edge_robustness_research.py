from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any
import zipfile

from app.research.microstructure_signal_research import (
    MODEL_FEATURE_NAMES,
    PHASE42H_BUNDLE_NAME,
    PHASE42H_SHA256_NAME,
    PHASE50_BUNDLE,
    PRIMARY_LABEL_HORIZON_MS,
    build_research_samples,
    build_split_report,
    verify_phase42h_evidence,
    _auc,
    _predict_probability,
    _fit_logistic_regression,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE = "5.1"
ALLOWED_INPUT_MODES = {"phase50_existing_dataset", "single_bundle", "multi_bundle"}
ALLOWED_DECISIONS = {
    "EDGE_ROBUST_ENOUGH_FOR_STRATEGY_SIMULATION",
    "EDGE_WEAK_BUT_WORTH_MORE_DATA",
    "EDGE_INCONCLUSIVE_NEEDS_MORE_DATA",
    "EDGE_FAILED_STOP_SIGNAL_BRANCH",
}

BASELINE_REQUIRED_ARTIFACTS = (
    "data/reports/phase_5_0_empirical_signal_report.json",
    "data/reports/phase_5_0_empirical_signal_report.md",
    "data/debug/phase_5_0_dataset_manifest.json",
    "data/debug/phase_5_0_label_validation_report.json",
    "data/debug/phase_5_0_leakage_check.json",
    "data/debug/phase_5_0_bucket_edge_report.json",
    "data/debug/phase_5_0_model_baseline_report.json",
)

PHASE51_INPUT_BUNDLE_MANIFEST = Path("data/debug/phase_5_1_input_bundle_manifest.json")
PHASE51_DATASET_EXPANSION_REPORT = Path("data/debug/phase_5_1_dataset_expansion_report.json")
PHASE51_LABEL_DISTRIBUTION_REPORT = Path("data/debug/phase_5_1_label_distribution_report.json")
PHASE51_OPPORTUNITY_FILTER_REPORT = Path("data/debug/phase_5_1_opportunity_filter_report.json")
PHASE51_COST_SENSITIVITY_REPORT = Path("data/debug/phase_5_1_cost_sensitivity_report.json")
PHASE51_AUC_EDGE_DECOMPOSITION_REPORT = Path("data/debug/phase_5_1_auc_edge_decomposition_report.json")
PHASE51_REGIME_ROBUSTNESS_REPORT = Path("data/debug/phase_5_1_regime_robustness_report.json")
PHASE51_TOPK_EDGE_REPORT = Path("data/debug/phase_5_1_topk_edge_report.json")
PHASE51_DECISION_GATE_REPORT = Path("data/debug/phase_5_1_decision_gate_report.json")
PHASE51_FINAL_REPORT_JSON = Path("data/reports/phase_5_1_edge_robustness_report.json")
PHASE51_FINAL_REPORT_MD = Path("data/reports/phase_5_1_edge_robustness_report.md")
PHASE51_BUNDLE = Path("phase_5_1_edge_robustness_research_bundle.zip")

TRADABLE_MOVE_THRESHOLD_BPS = 2.0
BASE_COST_BPS = 2.0
LOW_SAMPLE_THRESHOLD = 30
SUFFICIENT_EXPANSION_RATIO = 2.0

COST_SCENARIOS = {
    "zero_cost": {"fee_bps": 0.0, "slippage_bps": 0.0},
    "optimistic_cost": {"fee_bps": 0.5, "slippage_bps": 0.5},
    "base_cost": {"fee_bps": 1.0, "slippage_bps": 1.0},
    "conservative_cost": {"fee_bps": 2.0, "slippage_bps": 1.0},
    "stress_cost": {"fee_bps": 3.0, "slippage_bps": 2.0},
}


def run_phase51(
    root: str | Path = REPO_ROOT,
    *,
    phase50_report: str | Path = "data/reports/phase_5_0_empirical_signal_report.json",
    phase50_bundle: str | Path = PHASE50_BUNDLE,
    input_mode: str = "phase50_existing_dataset",
    bundle_manifest: str | Path | None = None,
    bundle_paths: list[str | Path] | None = None,
    sha256_paths: list[str | Path] | None = None,
    primary_horizon_ms: int = PRIMARY_LABEL_HORIZON_MS,
    output_dir: str | Path = "data",
    create_bundle: bool = True,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    data_root = _resolve_output_dir(root_path, output_dir)
    _ensure_output_dirs(data_root)

    baseline_lock = build_phase50_baseline_lock(
        root_path=root_path,
        phase50_report_path=_resolve(root_path, phase50_report),
        primary_horizon_ms=primary_horizon_ms,
    )
    input_manifest, samples = build_input_bundle_manifest_and_samples(
        root_path=root_path,
        phase50_bundle_path=_resolve(root_path, phase50_bundle),
        input_mode=input_mode,
        bundle_manifest_path=_resolve(root_path, bundle_manifest) if bundle_manifest is not None else None,
        bundle_paths=[_resolve(root_path, path) for path in (bundle_paths or [])],
        sha256_paths=[_resolve(root_path, path) for path in (sha256_paths or [])],
    )
    split_report = build_split_report(samples)
    split_lookup = _split_lookup(split_report)
    for sample in samples:
        if sample["sample_id"] in split_lookup:
            sample["split"] = split_lookup[sample["sample_id"]]

    dataset_expansion = build_dataset_expansion_report(baseline_lock, input_manifest, samples)
    label_distribution = build_label_distribution_report(samples)
    model_context = build_model_context(samples)
    opportunity_filter = build_opportunity_filter_report(samples, model_context)
    cost_sensitivity = build_cost_sensitivity_report(samples, opportunity_filter)
    auc_edge = build_auc_edge_decomposition_report(samples, model_context)
    regime = build_regime_robustness_report(samples)
    topk = build_topk_edge_report(samples, model_context)
    decision = build_decision_gate_report(
        baseline_lock=baseline_lock,
        input_manifest=input_manifest,
        dataset_expansion=dataset_expansion,
        label_distribution=label_distribution,
        opportunity_filter=opportunity_filter,
        cost_sensitivity=cost_sensitivity,
        auc_edge=auc_edge,
        regime=regime,
        topk=topk,
    )
    final_report = build_final_report(
        baseline_lock=baseline_lock,
        input_manifest=input_manifest,
        dataset_expansion=dataset_expansion,
        label_distribution=label_distribution,
        opportunity_filter=opportunity_filter,
        cost_sensitivity=cost_sensitivity,
        auc_edge=auc_edge,
        regime=regime,
        topk=topk,
        decision=decision,
    )

    outputs = {
        PHASE51_INPUT_BUNDLE_MANIFEST: input_manifest,
        PHASE51_DATASET_EXPANSION_REPORT: dataset_expansion,
        PHASE51_LABEL_DISTRIBUTION_REPORT: label_distribution,
        PHASE51_OPPORTUNITY_FILTER_REPORT: opportunity_filter,
        PHASE51_COST_SENSITIVITY_REPORT: cost_sensitivity,
        PHASE51_AUC_EDGE_DECOMPOSITION_REPORT: auc_edge,
        PHASE51_REGIME_ROBUSTNESS_REPORT: regime,
        PHASE51_TOPK_EDGE_REPORT: topk,
        PHASE51_DECISION_GATE_REPORT: decision,
        PHASE51_FINAL_REPORT_JSON: final_report,
    }
    for relative, payload in outputs.items():
        _write_json(root_path / relative, payload)
    _write_text(root_path / PHASE51_FINAL_REPORT_MD, render_markdown(final_report))
    if create_bundle:
        final_report["bundle"] = create_phase51_bundle(root_path)
        _write_json(root_path / PHASE51_FINAL_REPORT_JSON, final_report)
        _write_text(root_path / PHASE51_FINAL_REPORT_MD, render_markdown(final_report))
    return final_report


def build_phase50_baseline_lock(
    *,
    root_path: Path,
    phase50_report_path: Path,
    primary_horizon_ms: int,
) -> dict[str, Any]:
    missing = [path for path in BASELINE_REQUIRED_ARTIFACTS if not (root_path / path).exists()]
    final_report = _read_json(phase50_report_path) if phase50_report_path.exists() else {}
    label_report = _read_json(root_path / "data/debug/phase_5_0_label_validation_report.json") if (root_path / "data/debug/phase_5_0_label_validation_report.json").exists() else {}
    leakage = _read_json(root_path / "data/debug/phase_5_0_leakage_check.json") if (root_path / "data/debug/phase_5_0_leakage_check.json").exists() else {}
    manifest = _read_json(root_path / "data/debug/phase_5_0_dataset_manifest.json") if (root_path / "data/debug/phase_5_0_dataset_manifest.json").exists() else {}
    scope = _dict(final_report.get("research_scope_confirmation"))
    horizon = final_report.get("primary_label_horizon_ms") or label_report.get("primary_horizon_ms")
    edge_conclusion = final_report.get("edge_conclusion")
    leakage_pass = leakage.get("status") == "pass"
    no_live_execution_wallet = (
        scope.get("live_trading") is False
        and scope.get("order_execution") is False
        and scope.get("private_key_or_wallet_logic") is False
    )
    allowed = (
        not missing
        and phase50_report_path.exists()
        and primary_horizon_ms == PRIMARY_LABEL_HORIZON_MS
        and horizon == PRIMARY_LABEL_HORIZON_MS
        and leakage_pass
        and no_live_execution_wallet
        and edge_conclusion in {"EDGE_PROVEN", "EDGE_INCONCLUSIVE", "EDGE_FAILED"}
    )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_phase50_baseline_lock_v1",
        "status": "pass" if allowed else "fail",
        "phase50_artifacts_present": not missing,
        "missing_artifacts": missing,
        "phase50_primary_horizon_ms": horizon,
        "requested_primary_horizon_ms": primary_horizon_ms,
        "phase50_leakage_check_pass": leakage_pass,
        "phase50_edge_conclusion": edge_conclusion,
        "phase50_edge_inconclusive_accepted": edge_conclusion == "EDGE_INCONCLUSIVE",
        "phase50_no_live_trading": scope.get("live_trading") is False,
        "phase50_no_execution": scope.get("order_execution") is False,
        "phase50_no_wallet_logic": scope.get("private_key_or_wallet_logic") is False,
        "phase51_allowed_to_start": allowed,
        "phase50_valid_label_count": int(label_report.get("valid_100ms_label_count") or manifest.get("dataset_summary", {}).get("valid_100ms_label_count") or 0),
        "phase50_capture_duration_sec": _extract_phase50_duration(manifest),
    }


def build_input_bundle_manifest_and_samples(
    *,
    root_path: Path,
    phase50_bundle_path: Path,
    input_mode: str,
    bundle_manifest_path: Path | None = None,
    bundle_paths: list[Path] | None = None,
    sha256_paths: list[Path] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if input_mode not in ALLOWED_INPUT_MODES:
        return _failed_input_manifest(input_mode, f"unsupported input mode: {input_mode}"), []
    entries = _input_entries_for_mode(
        root_path=root_path,
        input_mode=input_mode,
        bundle_manifest_path=bundle_manifest_path,
        bundle_paths=bundle_paths or [],
        sha256_paths=sha256_paths or [],
    )
    bundles: list[dict[str, Any]] = []
    all_samples: list[dict[str, Any]] = []
    for index, entry in enumerate(entries, start=1):
        bundle_path = entry["bundle_path"]
        sha_path = entry["sha256_path"]
        bundle_id = entry.get("bundle_id") or f"session_{index:03d}"
        evidence, extracted = verify_phase42h_evidence(root_path, bundle_path, sha_path)
        dataset_paths = extracted.get("dataset_paths", {})
        runtime = extracted.get("runtime_report", {})
        session_samples: list[dict[str, Any]] = []
        if evidence.get("status") == "pass" and dataset_paths:
            session_samples, _ = build_research_samples(dataset_paths)
            for sample in session_samples:
                sample["sample_id"] = f"{bundle_id}-{sample['sample_id']}"
                sample["bundle_id"] = bundle_id
                sample["session_id"] = bundle_id
        bundles.append(
            {
                "bundle_id": bundle_id,
                "bundle_path": _relative_display(root_path, bundle_path),
                "sha256_path": _relative_display(root_path, sha_path),
                "sha256_valid": evidence.get("bundle_sha256_valid") is True,
                "capture_duration_sec": _float(runtime.get("duration_sec")) or _float(_dict(runtime.get("capture")).get("duration_sec")) or 0.0,
                "runtime_status": evidence.get("runtime_status"),
                "strict_100ms_observability_ready": evidence.get("strict_100ms_observability_ready") is True,
                "low_latency_ready": evidence.get("low_latency_ready") is True,
                "session_start_ts": _session_boundary(session_samples, "min"),
                "session_end_ts": _session_boundary(session_samples, "max"),
                "valid_100ms_label_count": sum(1 for sample in session_samples if sample.get("valid_100ms_label") is True),
                "status": evidence.get("status"),
                "errors": evidence.get("errors", []),
            }
        )
        all_samples.extend(session_samples)
    all_valid = bool(bundles) and all(bundle.get("status") == "pass" for bundle in bundles)
    if input_mode == "phase50_existing_dataset" and not phase50_bundle_path.exists():
        all_valid = False
    manifest = {
        "phase": PHASE,
        "schema_version": "phase_5_1_input_bundle_manifest_v1",
        "input_mode": input_mode,
        "phase50_bundle_path": _relative_display(root_path, phase50_bundle_path),
        "phase50_bundle_present": phase50_bundle_path.exists(),
        "bundle_count": len(bundles),
        "bundles": bundles,
        "all_bundles_valid": all_valid,
        "input_manifest_created": True,
        "bundle_count_recorded": True,
        "invalid_bundles_excluded_or_failed": all_valid,
        "status": "pass" if all_valid else "fail",
    }
    return manifest, all_samples


def build_dataset_expansion_report(baseline_lock: dict[str, Any], input_manifest: dict[str, Any], samples: list[dict[str, Any]]) -> dict[str, Any]:
    phase50_count = int(baseline_lock.get("phase50_valid_label_count") or 0)
    phase51_count = sum(1 for sample in samples if sample.get("valid_100ms_label") is True)
    input_mode = str(input_manifest.get("input_mode"))
    analysis_only = input_mode == "phase50_existing_dataset"
    session_count = int(input_manifest.get("bundle_count") or 0)
    duration = sum(_float(bundle.get("capture_duration_sec")) or 0.0 for bundle in input_manifest.get("bundles", []))
    increase_ratio = phase51_count / phase50_count if phase50_count else 0.0
    sufficient = (not analysis_only) and session_count >= 2 and increase_ratio >= SUFFICIENT_EXPANSION_RATIO
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_dataset_expansion_v1",
        "status": "pass",
        "dataset_expansion_mode": "analysis_only_no_new_data" if analysis_only else "expanded_bundle_analysis",
        "phase50_valid_label_count": phase50_count,
        "phase51_valid_label_count": phase51_count,
        "additional_valid_label_count": max(0, phase51_count - phase50_count),
        "increase_ratio": increase_ratio,
        "session_count": session_count,
        "total_capture_duration_sec": duration,
        "unique_market_condition_windows": _unique_time_windows(samples),
        "sufficient_expansion_for_edge_claim": sufficient,
        "unchanged_inconclusive_data_blocks_strong_conclusion": analysis_only and baseline_lock.get("phase50_edge_conclusion") == "EDGE_INCONCLUSIVE",
        "expansion_limitation_documented": analysis_only,
    }


def build_label_distribution_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    valid = _valid_samples(samples)
    counts = {
        "-1": sum(1 for sample in valid if sample.get("direction_100ms") == -1),
        "0": sum(1 for sample in valid if sample.get("direction_100ms") == 0),
        "1": sum(1 for sample in valid if sample.get("direction_100ms") == 1),
    }
    total = max(1, len(valid))
    flat_ratio = counts["0"] / total
    nonflat_count = counts["-1"] + counts["1"]
    nonflat_ratio = nonflat_count / total
    up_down_balance = min(counts["-1"], counts["1"]) / max(1, max(counts["-1"], counts["1"]))
    tradable = [sample for sample in valid if abs(_return(sample)) >= TRADABLE_MOVE_THRESHOLD_BPS]
    tradable_rate = len(tradable) / total
    risk = "high" if flat_ratio >= 0.90 or tradable_rate < 0.01 else "medium" if flat_ratio >= 0.70 else "low"
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_label_distribution_v1",
        "status": "pass",
        "primary_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "sample_count": len(valid),
        "direction_distribution": counts,
        "flat_ratio": flat_ratio,
        "nonflat_ratio": nonflat_ratio,
        "up_down_balance": up_down_balance,
        "tradable_move_threshold_bps": TRADABLE_MOVE_THRESHOLD_BPS,
        "tradable_move_count": len(tradable),
        "tradable_move_rate": tradable_rate,
        "flat_label_risk": risk,
        "nonflat_only_diagnostics": _subset_diagnostics([sample for sample in valid if sample.get("direction_100ms") != 0]),
        "tradable_move_only_diagnostics": _subset_diagnostics(tradable),
        "auc_may_be_inflated_by_flat_label_dominance": risk in {"medium", "high"},
        "recommendation": "Collect more sessions or use opportunity filtering before interpreting 100ms AUC as economic edge." if risk == "high" else "Continue robustness checks.",
    }


def build_model_context(samples: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [sample for sample in _valid_samples(samples) if sample.get("split") in {"train", "validation", "test"}]
    train = [sample for sample in valid if sample.get("split") == "train"]
    y_train = [1 if sample.get("direction_100ms") == 1 else 0 for sample in train]
    if len(set(y_train)) < 2:
        for sample in valid:
            sample["model_probability"] = None
            sample["model_confidence"] = None
        return {"status": "skipped", "skip_reason": "training split has fewer than two classes", "base_positive_rate": 0.0}
    model = _fit_logistic_regression(train, MODEL_FEATURE_NAMES)
    base_rate = statistics.fmean(y_train)
    for sample in valid:
        probability = _predict_probability(sample, model)
        sample["model_probability"] = probability
        sample["model_confidence"] = abs(probability - base_rate)
    return {"status": "pass", "model": model, "base_positive_rate": base_rate}


def build_opportunity_filter_report(samples: list[dict[str, Any]], model_context: dict[str, Any] | None = None) -> dict[str, Any]:
    valid = _valid_samples(samples)
    thresholds = {
        "repricing_gap_bps_abs_min": [1.0, 2.0, 3.0, 5.0, 10.0],
        "pm_spread_bps_max": [5.0, 10.0, 20.0, 50.0],
        "quote_age_ms_max": [50.0, 100.0, 250.0, 500.0],
        "latency_quality_score_min": [0.5, 0.7, 0.9],
        "reference_trade_intensity_min": [1.0, 3.0, 5.0, 10.0],
    }
    if model_context and model_context.get("status") == "pass":
        thresholds["model_confidence_min"] = [0.001, 0.003, 0.005, 0.01]
    filters: list[dict[str, Any]] = []
    for name, values in thresholds.items():
        for threshold in values:
            selected = [sample for sample in valid if _filter_match(sample, name, threshold)]
            split_metrics = {split: _selection_stats(selected, valid, split=split, cost_bps=BASE_COST_BPS) for split in ("train", "validation", "test")}
            stable = _validation_test_supported(split_metrics)
            filters.append(
                {
                    "filter_name": name,
                    "threshold_config": {"threshold": threshold},
                    "splits": split_metrics,
                    "stable_across_splits": stable,
                    "low_sample_filter": any(metric["low_sample_warning"] for metric in split_metrics.values()),
                    "edge_claim_allowed": stable and not any(metric["low_sample_warning"] for metric in split_metrics.values()),
                }
            )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_opportunity_filter_v1",
        "status": "pass",
        "base_cost_bps": BASE_COST_BPS,
        "threshold_sweep_completed": True,
        "validation_test_support_required": True,
        "filters": filters,
        "stable_filter_count": sum(1 for item in filters if item["edge_claim_allowed"]),
    }


def build_cost_sensitivity_report(samples: list[dict[str, Any]], opportunity_filter: dict[str, Any]) -> dict[str, Any]:
    valid = _valid_samples(samples)
    candidates = [
        {"candidate": "all_samples_opportunity_score", "samples": valid},
    ]
    for item in opportunity_filter.get("filters", []):
        selected = [sample for sample in valid if _filter_match(sample, item["filter_name"], item["threshold_config"]["threshold"])]
        candidates.append({"candidate": f"{item['filter_name']}={item['threshold_config']['threshold']}", "samples": selected})
    candidate_reports: list[dict[str, Any]] = []
    best_break_even = -math.inf
    for candidate in candidates:
        selected = candidate["samples"]
        raw_by_split = {split: _avg_signed_return([sample for sample in selected if sample.get("split") == split]) for split in ("train", "validation", "test")}
        validation_test_break_even = min(raw_by_split["validation"], raw_by_split["test"])
        best_break_even = max(best_break_even, validation_test_break_even)
        candidate_reports.append(
            {
                "candidate": candidate["candidate"],
                "sample_count": len(selected),
                "raw_edge_before_cost_bps_by_split": raw_by_split,
                "scenarios": {
                    name: {
                        "total_cost_bps": costs["fee_bps"] + costs["slippage_bps"],
                        "edge_after_cost_bps_by_split": {
                            split: raw - (costs["fee_bps"] + costs["slippage_bps"]) for split, raw in raw_by_split.items()
                        },
                    }
                    for name, costs in COST_SCENARIOS.items()
                },
            }
        )
    break_even = best_break_even if best_break_even != -math.inf else 0.0
    zero_hint = any(
        report["scenarios"]["zero_cost"]["edge_after_cost_bps_by_split"]["validation"] > 0
        and report["scenarios"]["zero_cost"]["edge_after_cost_bps_by_split"]["test"] > 0
        for report in candidate_reports
    )
    base_supported = any(
        report["scenarios"]["base_cost"]["edge_after_cost_bps_by_split"]["validation"] > 0
        and report["scenarios"]["base_cost"]["edge_after_cost_bps_by_split"]["test"] > 0
        for report in candidate_reports
    )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_cost_sensitivity_v1",
        "status": "pass",
        "cost_scenarios": COST_SCENARIOS,
        "candidate_reports": candidate_reports,
        "break_even_cost_bps": break_even,
        "break_even_cost_realistic": break_even >= BASE_COST_BPS,
        "positive_edge_before_cost": zero_hint,
        "positive_edge_after_base_cost": base_supported,
        "edge_disappears_only_after_cost": zero_hint and not base_supported,
        "raw_edge_assessment": _raw_edge_assessment(zero_hint, base_supported),
        "realistic_cost_assessment": "Base cost is not supported by validation and test." if not base_supported else "Base cost supported in validation and test.",
    }


def build_auc_edge_decomposition_report(samples: list[dict[str, Any]], model_context: dict[str, Any]) -> dict[str, Any]:
    valid = _valid_samples(samples)
    scored = [sample for sample in valid if _float(sample.get("model_probability")) is not None]
    nonflat = [sample for sample in scored if sample.get("direction_100ms") != 0]
    tradable = [sample for sample in scored if abs(_return(sample)) >= TRADABLE_MOVE_THRESHOLD_BPS]
    deciles = _score_deciles(scored, "model_probability", cost_bps=BASE_COST_BPS)
    topk = _topk_metrics(scored, ranker="model_confidence", percents=[1.0, 5.0, 10.0], cost_bps=BASE_COST_BPS)
    all_auc = _sample_auc(scored)
    nonflat_auc = _sample_auc(nonflat)
    tradable_auc = _sample_auc(tradable)
    risk = "high" if len(nonflat) / max(1, len(scored)) < 0.05 else "medium" if len(nonflat) / max(1, len(scored)) < 0.20 else "low"
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_auc_edge_decomposition_v1",
        "status": "pass",
        "model_context_status": model_context.get("status"),
        "all_sample_auc": all_auc,
        "nonflat_only_auc": nonflat_auc,
        "tradable_move_only_auc": tradable_auc,
        "prediction_deciles": deciles,
        "topk_analysis": topk,
        "flat_label_auc_inflation_risk": risk,
        "auc_edge_mismatch_explanation": _auc_edge_explanation(all_auc, deciles),
    }


def build_regime_robustness_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    valid = _valid_samples(samples)
    groupings = {
        "session_id": lambda sample: str(sample.get("session_id") or sample.get("bundle_id") or "unknown"),
        "time_bucket": _time_bucket,
        "spread_bucket": lambda sample: _numeric_bucket(_feature(sample, "target_spread_bps"), [0.01, 0.05, 0.10]),
        "latency_quality_bucket": lambda sample: _numeric_bucket(_feature(sample, "latency_quality_score"), [0.25, 0.50, 0.75]),
        "quote_age_bucket": lambda sample: _numeric_bucket(_feature(sample, "reference_bookticker_age_ms"), [50.0, 100.0, 250.0]),
        "activity_bucket": lambda sample: _numeric_bucket(_feature(sample, "reference_trade_count_1s"), [1.0, 3.0, 10.0]),
    }
    reports: list[dict[str, Any]] = []
    unstable = False
    for name, key_fn in groupings.items():
        buckets: dict[str, list[dict[str, Any]]] = {}
        for sample in valid:
            buckets.setdefault(key_fn(sample), []).append(sample)
        one_regime_only = len(buckets) <= 1
        if one_regime_only and name == "session_id":
            unstable = True
        for bucket, rows in sorted(buckets.items()):
            edge = _avg_signed_return(rows) - BASE_COST_BPS
            stable = len(rows) >= LOW_SAMPLE_THRESHOLD and edge > 0
            reports.append(
                {
                    "regime_name": name,
                    "bucket": bucket,
                    "sample_count": len(rows),
                    "edge_after_cost_bps": edge,
                    "hit_rate": _hit_rate_signed(rows),
                    "nonflat_rate": sum(1 for sample in rows if sample.get("direction_100ms") != 0) / max(1, len(rows)),
                    "stable": stable,
                    "one_regime_only": one_regime_only,
                }
            )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_regime_robustness_v1",
        "status": "pass",
        "groupings": reports,
        "at_least_one_time_or_session_grouping": True,
        "latency_quality_grouping_included": True,
        "one_regime_only_dependency": any(row["regime_name"] == "session_id" and row["one_regime_only"] for row in reports),
        "unstable_regimes_flagged": unstable or any(not row["stable"] for row in reports),
        "robust_across_regimes": not unstable and any(row["stable"] for row in reports),
    }


def build_topk_edge_report(samples: list[dict[str, Any]], model_context: dict[str, Any] | None = None) -> dict[str, Any]:
    valid = _valid_samples(samples)
    percents = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
    rankers = ["abs_repricing_gap_bps", "combined_opportunity_score"]
    if model_context and model_context.get("status") == "pass":
        rankers.append("model_confidence")
    rows: list[dict[str, Any]] = []
    for ranker in rankers:
        rows.extend(_topk_metrics(valid, ranker=ranker, percents=percents, cost_bps=BASE_COST_BPS))
    validation_test_supported = any(row["split"] == "validation" and row["edge_after_cost_bps"] > 0 and not row["low_sample_warning"] for row in rows) and any(
        row["split"] == "test" and row["edge_after_cost_bps"] > 0 and not row["low_sample_warning"] for row in rows
    )
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_topk_edge_v1",
        "status": "pass",
        "topk_slices": percents,
        "rankers": rankers,
        "rows": rows,
        "validation_and_test_reported": True,
        "low_sample_topk_flagged": any(row["low_sample_warning"] for row in rows),
        "topk_edge_after_cost_reported": True,
        "train_only_edge_not_robust": True,
        "validation_test_support": validation_test_supported,
        "robust_topk_edge_claim_allowed": validation_test_supported,
    }


def build_decision_gate_report(
    *,
    baseline_lock: dict[str, Any],
    input_manifest: dict[str, Any],
    dataset_expansion: dict[str, Any],
    label_distribution: dict[str, Any],
    opportunity_filter: dict[str, Any],
    cost_sensitivity: dict[str, Any],
    auc_edge: dict[str, Any],
    regime: dict[str, Any],
    topk: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if baseline_lock.get("status") != "pass":
        blockers.append("phase50_baseline_lock_failed")
    if input_manifest.get("status") != "pass":
        blockers.append("input_bundle_manifest_failed")
    if baseline_lock.get("phase50_primary_horizon_ms") != PRIMARY_LABEL_HORIZON_MS:
        blockers.append("primary_horizon_not_100ms")

    robust_conditions = (
        not blockers
        and cost_sensitivity.get("positive_edge_after_base_cost") is True
        and opportunity_filter.get("stable_filter_count", 0) > 0
        and topk.get("robust_topk_edge_claim_allowed") is True
        and regime.get("robust_across_regimes") is True
        and dataset_expansion.get("sufficient_expansion_for_edge_claim") is True
        and label_distribution.get("flat_label_risk") != "high"
    )
    weak_conditions = (
        not blockers
        and cost_sensitivity.get("positive_edge_before_cost") is True
        and cost_sensitivity.get("positive_edge_after_base_cost") is not True
    )
    no_raw_edge = cost_sensitivity.get("positive_edge_before_cost") is not True and topk.get("validation_test_support") is not True
    inconclusive_conditions = (
        dataset_expansion.get("sufficient_expansion_for_edge_claim") is not True
        or label_distribution.get("flat_label_risk") == "high"
        or regime.get("one_regime_only_dependency") is True
    )
    if robust_conditions:
        conclusion = "EDGE_ROBUST_ENOUGH_FOR_STRATEGY_SIMULATION"
        recommendation = "proceed to Phase 5.2 strategy simulation"
    elif inconclusive_conditions and not blockers:
        conclusion = "EDGE_INCONCLUSIVE_NEEDS_MORE_DATA"
        recommendation = "collect more controlled VPS data sessions"
    elif weak_conditions:
        conclusion = "EDGE_WEAK_BUT_WORTH_MORE_DATA"
        recommendation = "targeted dataset expansion and feature improvement"
    elif no_raw_edge:
        conclusion = "EDGE_FAILED_STOP_SIGNAL_BRANCH"
        recommendation = "stop this signal branch unless new features materially change the hypothesis"
    else:
        conclusion = "EDGE_INCONCLUSIVE_NEEDS_MORE_DATA"
        recommendation = "collect more controlled VPS data sessions"
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_decision_gate_v1",
        "status": "pass" if not blockers else "fail",
        "edge_robustness_conclusion": conclusion,
        "allowed_conclusions": sorted(ALLOWED_DECISIONS),
        "final_conclusion_is_allowed_value": conclusion in ALLOWED_DECISIONS,
        "decision_backed_by_reports": True,
        "blockers": blockers,
        "decision_factors": {
            "robust_conditions_met": robust_conditions,
            "weak_conditions_met": weak_conditions,
            "no_raw_edge_before_cost": no_raw_edge,
            "inconclusive_conditions_met": inconclusive_conditions,
            "phase50_inconclusive_baseline": baseline_lock.get("phase50_edge_conclusion") == "EDGE_INCONCLUSIVE",
        },
        "next_phase_recommendation": recommendation,
        "no_live_trading": True,
        "no_execution": True,
        "no_wallet_logic": True,
    }


def build_final_report(
    *,
    baseline_lock: dict[str, Any],
    input_manifest: dict[str, Any],
    dataset_expansion: dict[str, Any],
    label_distribution: dict[str, Any],
    opportunity_filter: dict[str, Any],
    cost_sensitivity: dict[str, Any],
    auc_edge: dict[str, Any],
    regime: dict[str, Any],
    topk: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    blockers = list(decision.get("blockers", []))
    warnings: list[str] = []
    if dataset_expansion.get("dataset_expansion_mode") == "analysis_only_no_new_data":
        warnings.append("Phase 5.1 ran in analysis-only mode with no new data.")
    if label_distribution.get("flat_label_risk") == "high":
        warnings.append("100ms labels are flat-heavy; AUC may not map to economic returns.")
    if cost_sensitivity.get("positive_edge_after_base_cost") is not True:
        warnings.append("No validation/test edge survived base cost.")
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_final_report_v1",
        "created_at_utc": _utc_now(),
        "status": "pass" if not blockers else "fail",
        "phase50_baseline_valid": baseline_lock.get("status") == "pass",
        "primary_horizon_ms": PRIMARY_LABEL_HORIZON_MS,
        "dataset_expansion_mode": dataset_expansion.get("dataset_expansion_mode"),
        "edge_robustness_conclusion": decision.get("edge_robustness_conclusion"),
        "recommended_next_phase": decision.get("next_phase_recommendation"),
        "blockers": blockers,
        "warnings": warnings,
        "no_live_trading": True,
        "no_execution": True,
        "no_wallet_logic": True,
        "no_order_placement": True,
        "no_copy_trading": True,
        "reports": {
            "input_bundle_manifest": PHASE51_INPUT_BUNDLE_MANIFEST.as_posix(),
            "dataset_expansion": PHASE51_DATASET_EXPANSION_REPORT.as_posix(),
            "label_distribution": PHASE51_LABEL_DISTRIBUTION_REPORT.as_posix(),
            "opportunity_filter": PHASE51_OPPORTUNITY_FILTER_REPORT.as_posix(),
            "cost_sensitivity": PHASE51_COST_SENSITIVITY_REPORT.as_posix(),
            "auc_edge_decomposition": PHASE51_AUC_EDGE_DECOMPOSITION_REPORT.as_posix(),
            "regime_robustness": PHASE51_REGIME_ROBUSTNESS_REPORT.as_posix(),
            "topk_edge": PHASE51_TOPK_EDGE_REPORT.as_posix(),
            "decision_gate": PHASE51_DECISION_GATE_REPORT.as_posix(),
        },
        "summary": {
            "phase50_edge_conclusion": baseline_lock.get("phase50_edge_conclusion"),
            "phase50_valid_label_count": baseline_lock.get("phase50_valid_label_count"),
            "phase51_valid_label_count": dataset_expansion.get("phase51_valid_label_count"),
            "flat_ratio": label_distribution.get("flat_ratio"),
            "tradable_move_rate": label_distribution.get("tradable_move_rate"),
            "break_even_cost_bps": cost_sensitivity.get("break_even_cost_bps"),
            "all_sample_auc": auc_edge.get("all_sample_auc"),
            "topk_validation_test_support": topk.get("validation_test_support"),
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = _dict(report.get("summary"))
    lines = [
        "# Phase 5.1 Edge Robustness Research",
        "",
        "## Executive Summary",
        f"Decision: {report.get('edge_robustness_conclusion')}",
        f"Recommended next phase: {report.get('recommended_next_phase')}",
        "",
        "## Phase 5.0 Baseline Recap",
        f"Phase 5.0 conclusion: {summary.get('phase50_edge_conclusion')}",
        f"Phase 5.0 valid 100ms labels: {summary.get('phase50_valid_label_count')}",
        "",
        "## Dataset Expansion Summary",
        f"Mode: {report.get('dataset_expansion_mode')}",
        f"Phase 5.1 valid 100ms labels: {summary.get('phase51_valid_label_count')}",
        "",
        "## Label/Flatness Diagnostics",
        f"Flat ratio: {summary.get('flat_ratio')}",
        f"Tradable move rate: {summary.get('tradable_move_rate')}",
        "",
        "## Opportunity Filter Results",
        "See the opportunity filter debug report for threshold sweeps and split stability.",
        "",
        "## Cost Sensitivity Results",
        f"Break-even cost bps: {summary.get('break_even_cost_bps')}",
        "",
        "## AUC-vs-Edge Explanation",
        f"All-sample AUC: {summary.get('all_sample_auc')}",
        "AUC can be inflated when flat labels dominate and prediction ranking does not map to positive after-cost returns.",
        "",
        "## Regime/Session Robustness",
        "Session, time, spread, latency quality, quote age, and activity groupings are reported in debug artifacts.",
        "",
        "## Top-K Edge Results",
        f"Validation/test top-k support: {summary.get('topk_validation_test_support')}",
        "",
        "## Decision Gate",
        f"Conclusion: {report.get('edge_robustness_conclusion')}",
        "",
        "## Recommended Next Phase",
        str(report.get("recommended_next_phase")),
        "",
        "## Blockers and Warnings",
    ]
    lines.extend([f"- blocker: {item}" for item in report.get("blockers", [])] or ["- blockers: none"])
    lines.extend([f"- warning: {item}" for item in report.get("warnings", [])] or ["- warnings: none"])
    lines.extend(
        [
            "",
            "## Scope",
            "Research only. No live trading, no execution, no wallet/private-key logic, no order placement, and no copy trading were introduced.",
            "",
        ]
    )
    return "\n".join(lines)


def create_phase51_bundle(root_path: Path) -> dict[str, Any]:
    bundle_path = root_path / PHASE51_BUNDLE
    required = [
        PHASE51_INPUT_BUNDLE_MANIFEST,
        PHASE51_DATASET_EXPANSION_REPORT,
        PHASE51_LABEL_DISTRIBUTION_REPORT,
        PHASE51_OPPORTUNITY_FILTER_REPORT,
        PHASE51_COST_SENSITIVITY_REPORT,
        PHASE51_AUC_EDGE_DECOMPOSITION_REPORT,
        PHASE51_REGIME_ROBUSTNESS_REPORT,
        PHASE51_TOPK_EDGE_REPORT,
        PHASE51_DECISION_GATE_REPORT,
        PHASE51_FINAL_REPORT_JSON,
        PHASE51_FINAL_REPORT_MD,
        Path("bot/app/research/edge_robustness_research.py"),
        Path("scripts/run_phase51_edge_robustness_research.py"),
        Path("pyproject.toml"),
    ]
    required.extend(sorted(Path("tests").glob("test_phase51_*.py")))
    missing = [path.as_posix() for path in required if not (root_path / path).exists()]
    if bundle_path.exists():
        bundle_path.unlink()
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in required:
            path = root_path / relative
            if path.exists() and path.is_file():
                archive.write(path, relative.as_posix())
    return {
        "path": PHASE51_BUNDLE.as_posix(),
        "sha256": _sha256_file(bundle_path),
        "size_bytes": bundle_path.stat().st_size,
        "missing_files": missing,
        "created": True,
    }


def _input_entries_for_mode(
    *,
    root_path: Path,
    input_mode: str,
    bundle_manifest_path: Path | None,
    bundle_paths: list[Path],
    sha256_paths: list[Path],
) -> list[dict[str, Any]]:
    if input_mode == "phase50_existing_dataset":
        return [
            {
                "bundle_id": "phase50_existing_dataset",
                "bundle_path": root_path / PHASE42H_BUNDLE_NAME,
                "sha256_path": root_path / PHASE42H_SHA256_NAME,
            }
        ]
    if bundle_manifest_path is not None and bundle_manifest_path.exists():
        payload = _read_json(bundle_manifest_path)
        entries = payload.get("bundles", payload if isinstance(payload, list) else [])
        return [
            {
                "bundle_id": str(entry.get("bundle_id") or f"session_{index:03d}"),
                "bundle_path": _resolve(root_path, entry.get("bundle_path")),
                "sha256_path": _resolve(root_path, entry.get("sha256_path")),
            }
            for index, entry in enumerate(entries, start=1)
            if isinstance(entry, dict)
        ]
    if input_mode == "single_bundle" and bundle_paths:
        sha_path = sha256_paths[0] if sha256_paths else root_path / PHASE42H_SHA256_NAME
        return [{"bundle_id": "session_001", "bundle_path": bundle_paths[0], "sha256_path": sha_path}]
    return [
        {
            "bundle_id": f"session_{index:03d}",
            "bundle_path": bundle,
            "sha256_path": sha256_paths[index - 1] if index - 1 < len(sha256_paths) else root_path / PHASE42H_SHA256_NAME,
        }
        for index, bundle in enumerate(bundle_paths, start=1)
    ]


def _failed_input_manifest(input_mode: str, reason: str) -> dict[str, Any]:
    return {
        "phase": PHASE,
        "schema_version": "phase_5_1_input_bundle_manifest_v1",
        "input_mode": input_mode,
        "bundle_count": 0,
        "bundles": [],
        "all_bundles_valid": False,
        "status": "fail",
        "errors": [reason],
    }


def _selection_stats(selected: list[dict[str, Any]], universe: list[dict[str, Any]], *, split: str, cost_bps: float) -> dict[str, Any]:
    split_universe = [sample for sample in universe if sample.get("split") == split]
    rows = [sample for sample in selected if sample.get("split") == split]
    returns = [_signed_return(sample) for sample in rows]
    raw = statistics.fmean(returns) if returns else 0.0
    return {
        "split": split,
        "sample_count": len(rows),
        "selection_rate": len(rows) / max(1, len(split_universe)),
        "hit_rate": sum(1 for value in returns if value > 0) / len(returns) if returns else None,
        "avg_future_return_bps": raw,
        "median_future_return_bps": _quantile(returns, 0.50),
        "p25_return_bps": _quantile(returns, 0.25),
        "p75_return_bps": _quantile(returns, 0.75),
        "edge_after_cost_bps": raw - cost_bps,
        "low_sample_warning": len(rows) < LOW_SAMPLE_THRESHOLD,
    }


def _validation_test_supported(split_metrics: dict[str, dict[str, Any]]) -> bool:
    validation = split_metrics.get("validation", {})
    test = split_metrics.get("test", {})
    return (
        validation.get("edge_after_cost_bps", -math.inf) > 0
        and test.get("edge_after_cost_bps", -math.inf) > 0
        and validation.get("low_sample_warning") is False
        and test.get("low_sample_warning") is False
    )


def _filter_match(sample: dict[str, Any], name: str, threshold: float) -> bool:
    if name == "repricing_gap_bps_abs_min":
        return abs(_feature(sample, "repricing_gap_bps")) >= threshold
    if name == "pm_spread_bps_max":
        return _feature(sample, "target_spread_bps") <= threshold
    if name == "quote_age_ms_max":
        return _feature(sample, "reference_bookticker_age_ms") <= threshold
    if name == "latency_quality_score_min":
        return _feature(sample, "latency_quality_score") >= threshold
    if name == "reference_trade_intensity_min":
        return _feature(sample, "reference_trade_count_1s") >= threshold
    if name == "model_confidence_min":
        return (_float(sample.get("model_confidence")) or 0.0) >= threshold
    return False


def _topk_metrics(samples: list[dict[str, Any]], *, ranker: str, percents: list[float], cost_bps: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for split in ("train", "validation", "test"):
        split_rows = [sample for sample in samples if sample.get("split") == split]
        ranked = sorted(split_rows, key=lambda sample: _rank_value(sample, ranker), reverse=True)
        for percent in percents:
            count = max(1, math.ceil(len(ranked) * percent / 100.0)) if ranked else 0
            selected = ranked[:count]
            returns = [_signed_return(sample) for sample in selected]
            avg_return = statistics.fmean(returns) if returns else 0.0
            cumulative = 0.0
            worst = 0.0
            for value in returns:
                cumulative += value
                worst = min(worst, cumulative)
            rows.append(
                {
                    "ranker": ranker,
                    "top_k_percent": percent,
                    "split": split,
                    "sample_count": len(selected),
                    "selection_rate": len(selected) / max(1, len(split_rows)),
                    "avg_future_return_bps": avg_return,
                    "median_future_return_bps": _quantile(returns, 0.50),
                    "edge_after_cost_bps": avg_return - cost_bps,
                    "hit_rate": sum(1 for value in returns if value > 0) / len(returns) if returns else None,
                    "max_drawdown_proxy_bps": worst,
                    "low_sample_warning": len(selected) < LOW_SAMPLE_THRESHOLD,
                }
            )
    return rows


def _score_deciles(samples: list[dict[str, Any]], score_name: str, *, cost_bps: float) -> list[dict[str, Any]]:
    ranked = sorted(samples, key=lambda sample: _float(sample.get(score_name)) or 0.0)
    chunks = _chunks(ranked, 10)
    rows: list[dict[str, Any]] = []
    for index, chunk in enumerate(chunks, start=1):
        returns = [_signed_return(sample) for sample in chunk]
        avg_return = statistics.fmean(returns) if returns else 0.0
        rows.append(
            {
                "decile": index,
                "sample_count": len(chunk),
                "avg_score": statistics.fmean((_float(sample.get(score_name)) or 0.0) for sample in chunk) if chunk else 0.0,
                "avg_future_return_bps": avg_return,
                "edge_after_cost_bps": avg_return - cost_bps,
                "calibration_positive_rate": sum(1 for sample in chunk if sample.get("direction_100ms") == 1) / max(1, len(chunk)),
            }
        )
    return rows


def _sample_auc(samples: list[dict[str, Any]]) -> float | None:
    y = [1 if sample.get("direction_100ms") == 1 else 0 for sample in samples]
    scores = [_float(sample.get("model_probability")) or 0.0 for sample in samples]
    return _auc(y, scores)


def _auc_edge_explanation(all_auc: float | None, deciles: list[dict[str, Any]]) -> str:
    top_edge = deciles[-1]["edge_after_cost_bps"] if deciles else None
    if all_auc is not None and all_auc >= 0.70 and (top_edge is None or top_edge <= 0):
        return "Model ranking separates rare up labels from flat labels, but prediction deciles do not produce positive after-cost returns."
    return "AUC and economic edge are both weak or sample-limited under the current 100ms labels."


def _raw_edge_assessment(zero_hint: bool, base_supported: bool) -> str:
    if base_supported:
        return "Raw edge survives base cost in validation and test."
    if zero_hint:
        return "Some raw edge exists before cost, but it does not survive base cost."
    return "No validation/test raw edge was found before cost."


def _avg_signed_return(samples: list[dict[str, Any]]) -> float:
    values = [_signed_return(sample) for sample in samples]
    return statistics.fmean(values) if values else 0.0


def _signed_return(sample: dict[str, Any]) -> float:
    return _return(sample) * _signal_direction(sample)


def _signal_direction(sample: dict[str, Any]) -> int:
    probability = _float(sample.get("model_probability"))
    if probability is not None:
        base = _float(sample.get("model_base_rate")) or 0.01
        if probability > base:
            return 1
        if probability < base:
            return -1
    gap = _feature(sample, "repricing_gap_bps")
    if gap > 0:
        return 1
    if gap < 0:
        return -1
    return 1


def _hit_rate_signed(samples: list[dict[str, Any]]) -> float | None:
    values = [_signed_return(sample) for sample in samples]
    return sum(1 for value in values if value > 0) / len(values) if values else None


def _rank_value(sample: dict[str, Any], ranker: str) -> float:
    if ranker == "abs_repricing_gap_bps":
        return abs(_feature(sample, "repricing_gap_bps"))
    if ranker == "model_confidence":
        return _float(sample.get("model_confidence")) or 0.0
    if ranker == "combined_opportunity_score":
        return abs(_feature(sample, "repricing_gap_bps")) * max(0.0, _feature(sample, "latency_quality_score")) * (1.0 + _feature(sample, "reference_trade_count_1s"))
    return 0.0


def _feature(sample: dict[str, Any], name: str) -> float:
    return _float(_dict(sample.get("features")).get(name)) or 0.0


def _return(sample: dict[str, Any]) -> float:
    return _float(sample.get("future_return_100ms_bps")) or 0.0


def _valid_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [sample for sample in samples if sample.get("valid_100ms_label") is True]


def _subset_diagnostics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_return(sample) for sample in samples]
    return {
        "sample_count": len(samples),
        "avg_future_return_bps": statistics.fmean(returns) if returns else 0.0,
        "hit_rate_up": sum(1 for value in returns if value > 0) / len(returns) if returns else None,
    }


def _unique_time_windows(samples: list[dict[str, Any]]) -> int:
    windows = {int(sample.get("feature_ts_ns", 0)) // (5 * 60 * 1_000_000_000) for sample in samples}
    return len(windows)


def _time_bucket(sample: dict[str, Any]) -> str:
    ts = int(sample.get("feature_ts_ns") or 0)
    return f"window_{ts // (5 * 60 * 1_000_000_000)}"


def _numeric_bucket(value: float, edges: list[float]) -> str:
    for index, edge in enumerate(edges, start=1):
        if value <= edge:
            return f"b{index}_lte_{edge}"
    return f"b{len(edges) + 1}_gt_{edges[-1]}"


def _quantile(values: list[float], q: float) -> float | None:
    clean = sorted(value for value in values if not math.isnan(value) and not math.isinf(value))
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
    return clean[lower] * (1 - weight) + clean[upper] * weight


def _chunks(rows: list[dict[str, Any]], count: int) -> list[list[dict[str, Any]]]:
    return [rows[int(i * len(rows) / count) : int((i + 1) * len(rows) / count)] for i in range(count)]


def _extract_phase50_duration(manifest: dict[str, Any]) -> float:
    metrics = _dict(manifest.get("runtime_metrics"))
    duration = _float(metrics.get("duration_sec"))
    if duration is not None:
        return duration
    return 0.0


def _session_boundary(samples: list[dict[str, Any]], which: str) -> str | None:
    values = [int(sample["feature_ts_ns"]) for sample in samples if sample.get("feature_ts_ns") is not None]
    if not values:
        return None
    value = min(values) if which == "min" else max(values)
    return str(value)


def _split_lookup(split_report: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for split, payload in _dict(split_report.get("splits")).items():
        for sample_id in payload.get("sample_ids", []):
            lookup[str(sample_id)] = split
    return lookup


def _ensure_output_dirs(data_root: Path) -> None:
    (data_root / "debug").mkdir(parents=True, exist_ok=True)
    (data_root / "reports").mkdir(parents=True, exist_ok=True)
    (data_root / "cache").mkdir(parents=True, exist_ok=True)


def _resolve_output_dir(root_path: Path, output_dir: str | Path) -> Path:
    candidate = Path(output_dir)
    return candidate if candidate.is_absolute() else root_path / candidate


def _resolve(root_path: Path, path: str | Path | None) -> Path:
    if path is None:
        return root_path
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root_path / candidate


def _relative_display(root_path: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root_path.resolve()).as_posix()
    except ValueError:
        return str(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

