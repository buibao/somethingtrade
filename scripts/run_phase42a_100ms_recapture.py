from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.orderbook_100ms_coverage import (  # noqa: E402
    PHASE42A_BUNDLE,
    PHASE42A_REPORT_JSON,
    classify_phase42a_failure,
    create_phase42a_bundle,
    run_phase42a_analysis,
    runtime_quality_from_phase41_report,
    write_phase42a_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 4.2A 100ms coverage recapture and hard gates."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=float, default=1800.0)
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--input-clean-samples", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--output-labeled-samples", default="data/dataset/orderbook_labeled_samples.jsonl")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--no-bundle", action="store_true")
    args = parser.parse_args(argv)
    del args.max_attempts

    root = Path(args.root).resolve()
    debug_dir = root / "data/debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = root / PHASE42A_BUNDLE
    if bundle_path.exists():
        bundle_path.unlink()

    pytest_output_path = debug_dir / "phase_4_2a_pytest_output.txt"
    if args.skip_pytest:
        pytest_output = "pytest skipped by explicit --skip-pytest test hook\n"
        pytest_output_path.write_text(pytest_output, encoding="utf-8")
    else:
        pytest_code, pytest_output = _run_pytest(pytest_output_path)
        if pytest_code != 0:
            report = _minimal_failure_report(
                symbol=args.symbol,
                classification="TEST_FAILURE",
                reason="pytest failed",
                capture={
                    "fresh_capture_performed": False,
                    "fixture_mode": bool(args.skip_capture),
                    "duration_sec": 0,
                },
            )
            write_phase42a_artifacts(report, root=root, pytest_output=pytest_output)
            return pytest_code

    capture = {
        "fresh_capture_performed": False,
        "fixture_mode": bool(args.skip_capture),
        "duration_sec": float(args.duration_sec),
        "sample_count": 0,
        "sample_rate_per_sec": 0.0,
        "downsampling_enabled": False,
        "emits_every_accepted_delta": True,
        "sample_source": "accepted_depth_delta",
    }
    runtime_quality: dict[str, object]
    runtime_errors: list[str]

    if args.skip_capture:
        runtime_quality = {
            "sample_before_ready_count": 0,
            "feed_receive_stale_count": 0,
            "queue_dropped_messages": 0,
            "sequence_gap_count": 0,
            "invalid_delta_count": 0,
            "crossed_book_count": 0,
            "book_empty_count": 0,
            "one_side_missing_count": 0,
            "clean_sample_schema_violation_count": 0,
            "snapshot_copy_p99_us": 0.0,
        }
        runtime_errors = []
    else:
        capture_code = _run_capture(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            depth_n=args.depth_n,
            root=SOURCE_ROOT,
        )
        capture["fresh_capture_performed"] = capture_code == 0
        if capture_code != 0:
            report = _minimal_failure_report(
                symbol=args.symbol,
                classification="RUNTIME_CAPTURE_FAILURE",
                reason=f"capture command exited {capture_code}",
                capture=capture,
            )
            write_phase42a_artifacts(report, root=root, pytest_output=pytest_output)
            return capture_code or 1
        runtime_quality, runtime_errors = runtime_quality_from_phase41_report(
            SOURCE_ROOT / "data/reports/phase_4_1_orderbook_quality_report.json"
        )
        if root != SOURCE_ROOT:
            _copy_if_exists(
                SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl",
                root / "data/dataset/orderbook_clean_samples.jsonl",
            )

    report = run_phase42a_analysis(
        root=root,
        symbol=args.symbol,
        clean_samples_path=args.input_clean_samples,
        labeled_samples_path=args.output_labeled_samples,
        runtime_quality=runtime_quality,
        capture=capture,
        fresh_capture_required=not args.skip_capture,
    )
    if runtime_errors:
        report["runtime_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = report.get("primary_failure") or "phase_4_1_1_runtime_report_missing"
        report["hard_fail_reasons"].extend(runtime_errors)
    write_phase42a_artifacts(report, root=root, pytest_output=pytest_output)

    if report.get("definition_of_done_status") != "pass":
        print(f"Phase 4.2A failed: {classify_phase42a_failure(report)}")
        return 1

    if not args.no_bundle:
        try:
            create_phase42a_bundle(root=root, source_root=SOURCE_ROOT, bundle_path=bundle_path)
            report["bundle_created"] = True
            write_phase42a_artifacts(
                report,
                root=root,
                pytest_output=pytest_output,
                bundle_created=True,
            )
        except Exception as exc:
            report["definition_of_done_status"] = "fail"
            report["status"] = "fail"
            report["primary_failure"] = "bundle_failure"
            report["hard_fail_reasons"].append(f"bundle failure: {exc}")
            write_phase42a_artifacts(report, root=root, pytest_output=pytest_output)
            print(f"Phase 4.2A bundle failed: {exc}")
            return 1
    print("Phase 4.2A self-check passed")
    return 0


def _run_pytest(output_path: Path) -> tuple[int, str]:
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
    output = process.stdout + process.stderr
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output, encoding="utf-8")
    sys.stdout.write(process.stdout)
    sys.stderr.write(process.stderr)
    return process.returncode, output


def _run_capture(*, symbol: str, duration_sec: float, depth_n: int, root: Path) -> int:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(root / "bot")
    process = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            "-m",
            "app.main",
            "orderbook-quality-capture",
            "--symbol",
            symbol,
            "--duration-sec",
            str(duration_sec),
            "--depth-n",
            str(depth_n),
        ],
        cwd=root / "bot",
        env=env,
        text=True,
    )
    return process.returncode


def _copy_if_exists(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(source.read_bytes())


def _minimal_failure_report(
    *,
    symbol: str,
    classification: str,
    reason: str,
    capture: dict[str, object],
) -> dict[str, object]:
    primary = {
        "TEST_FAILURE": "pytest_failed",
        "RUNTIME_CAPTURE_FAILURE": "runtime_capture_failed",
    }.get(classification, "phase42a_failed")
    return {
        "phase": "4.2A",
        "symbol": symbol,
        "status": "fail",
        "implementation_status": "fail" if classification == "TEST_FAILURE" else "pass",
        "runtime_status": "fail" if classification == "RUNTIME_CAPTURE_FAILURE" else "pass",
        "dataset_coverage_status": "fail",
        "definition_of_done_status": "fail",
        "primary_failure": primary,
        "input_paths": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "labeled_samples": "data/dataset/orderbook_labeled_samples.jsonl",
        },
        "capture": {
            "fresh_capture_performed": bool(capture.get("fresh_capture_performed", False)),
            "fixture_mode": bool(capture.get("fixture_mode", False)),
            "duration_sec": float(capture.get("duration_sec", 0.0) or 0.0),
            "sample_count": 0,
            "sample_rate_per_sec": 0.0,
            "downsampling_enabled": False,
            "emits_every_accepted_delta": True,
            "sample_source": "accepted_depth_delta",
        },
        "runtime_quality": {},
        "timestamp_quality": {
            "timestamp_monotonic_violations": 0,
            "duplicate_timestamp_count": 0,
            "gap_p95_ms": None,
            "gap_p99_ms": None,
        },
        "horizon_100ms": {
            "max_future_gap_ms": 100,
            "eligible_count": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "tail_no_future_count": 0,
            "valid_rate_all_rows": 0.0,
            "valid_rate_eligible_rows": 0.0,
            "invalid_reason_counts": {},
        },
        "leakage_check": {
            "passed": False,
            "feature_leakage_violations": 0,
            "label_leakage_violations": 0,
        },
        "hard_fail_reasons": [reason],
        "warning_reasons": [],
        "bottleneck_assessment": "Self-check stopped before coverage analysis.",
    }


if __name__ == "__main__":
    raise SystemExit(main())

