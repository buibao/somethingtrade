from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.orderbook_labeled_dataset import (  # noqa: E402
    HORIZONS,
    MIN_VALID_RATE_ELIGIBLE_ROWS,
    bundle_missing_files,
    classify_report_failure,
    create_phase42_bundle,
    run_phase42_pipeline,
    write_failure_investigation,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the strict Phase 4.2 self-check.")
    parser.add_argument("--input", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--output", default="data/dataset/orderbook_labeled_samples.jsonl")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--no-bundle", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    for horizon in HORIZONS:
        parser.add_argument(
            f"--min-valid-rate-{HORIZONS[horizon]}ms",
            type=float,
            default=MIN_VALID_RATE_ELIGIBLE_ROWS[horizon],
        )
    args = parser.parse_args(argv)
    del args.max_attempts

    root = Path(args.root).resolve()
    debug_dir = root / "data/debug"
    reports_dir = root / "data/reports"
    debug_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    pytest_output = debug_dir / "phase_4_2_pytest_output.txt"
    investigation_path = debug_dir / "phase42_failure_investigation.md"
    report_json = reports_dir / "phase_4_2_dataset_quality_report.json"
    report_md = reports_dir / "phase_4_2_dataset_quality_report.md"
    self_check_path = reports_dir / "phase42_self_check.json"
    bundle_path = root / "phase_4_2_dataset_quality_bundle.zip"
    if bundle_path.exists():
        bundle_path.unlink()

    if args.skip_pytest:
        pytest_output.write_text("pytest skipped by explicit --skip-pytest test hook\n", encoding="utf-8")
    else:
        code = _run_pytest(pytest_output)
        if code != 0:
            _write_self_check(self_check_path, passed=False, classification="TEST_FAILURE")
            write_failure_investigation(
                investigation_path,
                classification="TEST_FAILURE",
                failed_item="pytest passes fully offline",
                report_path=report_json,
                debug_paths=[pytest_output],
                hypothesis="pytest failed before dataset generation.",
            )
            return code

    min_rates = {
        horizon: getattr(args, f"min_valid_rate_{HORIZONS[horizon]}ms")
        for horizon in HORIZONS
    }
    pipeline = run_phase42_pipeline(
        input_path=args.input,
        output_path=args.output,
        report_json_path=report_json,
        report_md_path=report_md,
        debug_dir=debug_dir,
        min_valid_rate_by_horizon=min_rates,
    )
    if pipeline.report["status"] != "pass":
        classification = classify_report_failure(pipeline.report)
        _write_self_check(self_check_path, passed=False, classification=classification)
        write_failure_investigation(
            investigation_path,
            classification=classification,
            failed_item=", ".join(pipeline.report.get("hard_fail_reasons", [])),
            report_path=report_json,
            debug_paths=[
                debug_dir / "phase_4_2_label_generation_summary.json",
                debug_dir / "phase_4_2_label_invalid_cases.jsonl",
                debug_dir / "phase_4_2_leakage_check.json",
                debug_dir / "phase_4_2_dataset_schema_violations.jsonl",
                pytest_output,
            ],
            hypothesis=_failure_hypothesis(pipeline.report),
        )
        return 1

    _write_self_check(self_check_path, passed=True, classification=None)
    if not args.no_bundle:
        try:
            create_phase42_bundle(
                root=root,
                source_root=SOURCE_ROOT,
                bundle_path=bundle_path,
            )
            missing = bundle_missing_files(bundle_path)
        except Exception as exc:
            _write_self_check(self_check_path, passed=False, classification="BUNDLE_FAILURE")
            write_failure_investigation(
                investigation_path,
                classification="BUNDLE_FAILURE",
                failed_item="phase_4_2_dataset_quality_bundle.zip exists and contains required files",
                report_path=report_json,
                debug_paths=[pytest_output],
                hypothesis=f"Bundle creation failed: {exc}",
            )
            return 1
        if missing:
            _write_self_check(self_check_path, passed=False, classification="BUNDLE_FAILURE")
            write_failure_investigation(
                investigation_path,
                classification="BUNDLE_FAILURE",
                failed_item=f"bundle missing required files: {missing}",
                report_path=report_json,
                debug_paths=[pytest_output],
                hypothesis="Bundle was created but required files were absent.",
            )
            if bundle_path.exists():
                bundle_path.unlink()
            return 1
    print("Phase 4.2 self-check passed")
    return 0


def _run_pytest(output_path: Path) -> int:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(SOURCE_ROOT / "bot")
    process = subprocess.run(
        [sys.executable, "-X", "utf8", "-m", "pytest", "-q"],
        cwd=SOURCE_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(process.stdout + process.stderr, encoding="utf-8")
    sys.stdout.write(process.stdout)
    sys.stderr.write(process.stderr)
    return process.returncode


def _write_self_check(
    path: Path,
    *,
    passed: bool,
    classification: str | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "phase": "4.2",
        "passed": passed,
        "failure_classification": classification,
        "pytest_output_path": "data/debug/phase_4_2_pytest_output.txt",
        "report_json_path": "data/reports/phase_4_2_dataset_quality_report.json",
        "report_md_path": "data/reports/phase_4_2_dataset_quality_report.md",
        "bundle_path": "phase_4_2_dataset_quality_bundle.zip",
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _failure_hypothesis(report: dict[str, object]) -> str:
    lines = ["Definition of Done failed."]
    hard_fails = report.get("hard_fail_reasons", [])
    if isinstance(hard_fails, list) and hard_fails:
        lines.append("")
        lines.append("Hard fail reasons:")
        lines.extend(f"- {reason}" for reason in hard_fails)
    label_quality = report.get("label_quality", {})
    if isinstance(label_quality, dict):
        horizons = label_quality.get("horizons", {})
        if isinstance(horizons, dict):
            lines.append("")
            lines.append("Per-horizon eligible valid rates:")
            for horizon, stats in horizons.items():
                if not isinstance(stats, dict):
                    continue
                lines.append(
                    "- {horizon}: valid_rate_eligible_rows={rate}, "
                    "eligible_count={eligible}, valid_count={valid}, invalid_reasons={reasons}".format(
                        horizon=horizon,
                        rate=stats.get("valid_rate_eligible_rows"),
                        eligible=stats.get("eligible_count"),
                        valid=stats.get("valid_count"),
                        reasons=stats.get("invalid_reason_counts"),
                    )
                )
    leakage = report.get("leakage_check", {})
    if isinstance(leakage, dict):
        lines.append("")
        lines.append(
            "Leakage check: passed={passed}, feature_leakage_violations={feature}, "
            "label_leakage_violations={label}".format(
                passed=leakage.get("passed"),
                feature=leakage.get("feature_leakage_violations"),
                label=leakage.get("label_leakage_violations"),
            )
        )
    lines.append("")
    lines.append(
        "If pytest, schema, timestamp monotonicity, and leakage all pass while only "
        "eligible label valid rate fails, the blocker is source dataset coverage for "
        "the affected horizon under the required max_future_gap_ms policy."
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
