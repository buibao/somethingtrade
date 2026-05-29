from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any

import aiohttp


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.clock_sync_receive_lag import build_server_time_sample  # noqa: E402
from app.research.latency_readiness_profile import (  # noqa: E402
    AGGTRADE_REFERENCE_EVENTS,
    BOOKTICKER_REFERENCE_QUOTES,
    CORRECTED_TIME_PROTOCOL_LABELS,
    LATENCY_PROFILE_SAMPLES,
    PHASE42FG_FAIL_AUDIT_BUNDLE,
    PHASE42FG_PASS_BUNDLE,
    PHASE42FG_TYPECHECK_REPORT,
    classify_phase42fg_failure,
    cleanup_phase42fg_artifacts,
    create_phase42fg_bundle,
    create_phase42fg_dataset_zip,
    evaluate_phase42fg_report,
    run_phase42fg_analysis,
    validate_gitignore_rules,
    write_phase42fg_artifacts,
)
from app.research.reference_feed_benchmark import TRADE_REFERENCE_EVENTS  # noqa: E402
from scripts.run_phase42c_reference_feed_benchmark import (  # noqa: E402
    _copy_if_exists,
    _count_jsonl,
    _run_multi_feed_capture,
    required_streams,
    validate_capture_diagnostics,
)


BINANCE_SERVER_TIME_URL = "https://api.binance.com/api/v3/time"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 4.2F+G latency readiness semantics and receive-path profile self-check."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=float, default=1800.0)
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--allow-fixture-mode", action="store_true")
    parser.add_argument("--input-clean-samples", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--input-bookticker", default=str(BOOKTICKER_REFERENCE_QUOTES))
    parser.add_argument("--input-trade", default=str(TRADE_REFERENCE_EVENTS))
    parser.add_argument("--input-aggtrade", default=str(AGGTRADE_REFERENCE_EVENTS))
    parser.add_argument("--input-latency-profile", default=str(LATENCY_PROFILE_SAMPLES))
    parser.add_argument("--output-corrected-labels", default=str(CORRECTED_TIME_PROTOCOL_LABELS))
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--no-bundle", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    debug_dir = root / "data/debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    for bundle in (root / PHASE42FG_PASS_BUNDLE, root / PHASE42FG_FAIL_AUDIT_BUNDLE):
        if bundle.exists():
            bundle.unlink()

    cleanup_report: dict[str, Any] = {
        "cleanup_performed": False,
        "deleted_files": [],
        "missing_files_skipped": [],
        "errors": [],
    }
    pytest_output = ""
    typecheck_summary = ""
    gitignore_validation = validate_gitignore_rules(root)
    if args.clean:
        cleanup_report = cleanup_phase42fg_artifacts(root)
        if cleanup_report.get("errors"):
            report = _failure_report(
                args=args,
                cleanup_report=cleanup_report,
                gitignore_validation=gitignore_validation,
                classification="ARTIFACT_CLEANUP_FAILURE",
                reason=f"artifact cleanup failed: {cleanup_report.get('errors')}",
            )
            _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
            print("Phase 4.2F+G failed: ARTIFACT_CLEANUP_FAILURE")
            return 1
    else:
        _write_json(root / "data/debug/phase_4_2fg_artifact_cleanup.json", cleanup_report)

    pytest_output_path = debug_dir / "phase_4_2fg_pytest_output.txt"
    if args.skip_pytest:
        pytest_output = "pytest skipped by explicit --skip-pytest test hook\n"
        pytest_output_path.write_text(pytest_output, encoding="utf-8")
        pytest_passed = True
    else:
        pytest_code, pytest_output = _run_pytest(pytest_output_path)
        pytest_passed = pytest_code == 0
        if not pytest_passed:
            report = _failure_report(
                args=args,
                cleanup_report=cleanup_report,
                gitignore_validation=gitignore_validation,
                classification="TEST_FAILURE",
                reason="pytest failed",
                pytest_passed=False,
            )
            _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
            print("Phase 4.2F+G failed: TEST_FAILURE")
            return pytest_code or 1

    typecheck_code, typecheck_summary = _run_typecheck(root / PHASE42FG_TYPECHECK_REPORT)
    typecheck_passed = typecheck_code == 0
    if not typecheck_passed:
        report = _failure_report(
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            classification="TYPECHECK_FAILURE",
            reason=typecheck_summary,
            typecheck_passed=False,
            typecheck_summary=typecheck_summary,
        )
        _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
        print("Phase 4.2F+G failed: TYPECHECK_FAILURE")
        return 1

    capture = _capture_summary(args, fresh=False, cleanup_report=cleanup_report)
    clock_samples: list[dict[str, Any]] = []
    if args.skip_capture and not args.allow_fixture_mode:
        report = _failure_report(
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            classification="FRESH_CAPTURE_NOT_PERFORMED",
            reason="--skip-capture is only allowed with --allow-fixture-mode in tests",
            pytest_passed=pytest_passed,
            typecheck_passed=typecheck_passed,
            typecheck_summary=typecheck_summary,
        )
        _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
        print("Phase 4.2F+G failed: FRESH_CAPTURE_NOT_PERFORMED")
        return 1
    if not args.skip_capture and float(args.duration_sec) < 1800.0:
        report = _failure_report(
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            classification="FRESH_CAPTURE_DURATION_FAILURE",
            reason="final fresh capture duration_sec < 1800",
            pytest_passed=pytest_passed,
            typecheck_passed=typecheck_passed,
            typecheck_summary=typecheck_summary,
        )
        _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
        print("Phase 4.2F+G failed: FRESH_CAPTURE_DURATION_FAILURE")
        return 1

    capture_code = 0
    if args.skip_capture:
        clock_samples = _fixture_clock_samples()
        capture.update(_fixture_capture(args, root=root))
    else:
        try:
            clock_samples, capture_code, capture_diagnostics = asyncio.run(
                _run_capture_with_clock_samples(
                    symbol=args.symbol,
                    duration_sec=args.duration_sec,
                    depth_n=args.depth_n,
                )
            )
        except Exception as exc:
            report = _failure_report(
                args=args,
                cleanup_report=cleanup_report,
                gitignore_validation=gitignore_validation,
                classification="FRESH_CAPTURE_NOT_PERFORMED",
                reason=f"capture or server-time sampling failed: {exc}",
                pytest_passed=pytest_passed,
                typecheck_passed=typecheck_passed,
                typecheck_summary=typecheck_summary,
            )
            _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
            print("Phase 4.2F+G failed: FRESH_CAPTURE_NOT_PERFORMED")
            return 1
        diagnostic_errors = validate_capture_diagnostics(capture_diagnostics, symbol=args.symbol)
        capture.update(
            {
                "fresh_capture_performed": capture_code == 0,
                "fixture_mode": False,
                "skip_capture": False,
                "capture_exit_code": capture_code,
                "capture_diagnostics": capture_diagnostics,
                "capture_diagnostic_errors": diagnostic_errors,
                "depth_clean_sample_count": _count_jsonl(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl"),
                "latency_profile_sample_count": _count_jsonl(SOURCE_ROOT / LATENCY_PROFILE_SAMPLES),
                "reference_event_counts": {
                    "bookTicker_mid": _count_jsonl(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES),
                    "trade_price": _count_jsonl(SOURCE_ROOT / TRADE_REFERENCE_EVENTS),
                    "aggTrade_price": _count_jsonl(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS),
                },
            }
        )
        if root != SOURCE_ROOT:
            _copy_if_exists(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl", root / "data/dataset/orderbook_clean_samples.jsonl")
            _copy_if_exists(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES, root / BOOKTICKER_REFERENCE_QUOTES)
            _copy_if_exists(SOURCE_ROOT / TRADE_REFERENCE_EVENTS, root / TRADE_REFERENCE_EVENTS)
            _copy_if_exists(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS, root / AGGTRADE_REFERENCE_EVENTS)
            _copy_if_exists(SOURCE_ROOT / LATENCY_PROFILE_SAMPLES, root / LATENCY_PROFILE_SAMPLES)

    report = run_phase42fg_analysis(
        root=root,
        symbol=args.symbol,
        clock_offset_samples=clock_samples,
        clean_samples_path=args.input_clean_samples,
        bookticker_path=args.input_bookticker,
        trade_path=args.input_trade,
        aggtrade_path=args.input_aggtrade,
        latency_profile_samples_path=args.input_latency_profile,
        corrected_labels_path=args.output_corrected_labels,
        capture={
            **capture,
            "depth_clean_sample_count": _count_jsonl(_resolve(root, args.input_clean_samples)),
            "latency_profile_sample_count": _count_jsonl(_resolve(root, args.input_latency_profile)),
            "reference_event_counts": {
                "bookTicker_mid": _count_jsonl(_resolve(root, args.input_bookticker)),
                "trade_price": _count_jsonl(_resolve(root, args.input_trade)),
                "aggTrade_price": _count_jsonl(_resolve(root, args.input_aggtrade)),
            },
        },
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        fresh_capture_required=not args.skip_capture,
    )
    if capture_code != 0:
        report["hard_fail_reasons"].append(f"multi-feed capture exited {capture_code}")
        report["primary_failure"] = report.get("primary_failure") or "FRESH_CAPTURE_NOT_PERFORMED"
        report = evaluate_phase42fg_report(report)
    if report.get("labeled_sample_count", 0) or report.get("hot_path_latency_summary", {}).get("sample_count", 0):
        create_phase42fg_dataset_zip(root)
    _write_and_bundle(report, root=root, pytest_output=pytest_output, no_bundle=args.no_bundle)
    if report.get("status") != "pass":
        print(f"Phase 4.2F+G failed: {classify_phase42fg_failure(report)}")
        return 1
    print("Phase 4.2F+G self-check passed")
    return 0


async def _run_capture_with_clock_samples(
    *,
    symbol: str,
    duration_sec: float,
    depth_n: int,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    samples.append(await _sample_binance_server_time(sample_id=1, phase="before_capture"))
    capture_task = asyncio.create_task(
        _run_multi_feed_capture(
            symbol=symbol,
            duration_sec=duration_sec,
            depth_n=depth_n,
            root=SOURCE_ROOT,
            latency_profile_samples_path=SOURCE_ROOT / LATENCY_PROFILE_SAMPLES,
        )
    )
    periodic_task = asyncio.create_task(_periodic_server_time_samples(samples, capture_task, interval_sec=300.0))
    capture_code, diagnostics = await capture_task
    await periodic_task
    samples.append(await _sample_binance_server_time(sample_id=len(samples) + 1, phase="after_capture"))
    return samples, capture_code, diagnostics


async def _periodic_server_time_samples(
    samples: list[dict[str, Any]],
    capture_task: asyncio.Task[tuple[int, dict[str, Any]]],
    *,
    interval_sec: float,
) -> None:
    while not capture_task.done():
        try:
            await asyncio.wait_for(asyncio.shield(capture_task), timeout=interval_sec)
        except TimeoutError:
            if capture_task.done():
                break
            try:
                samples.append(
                    await _sample_binance_server_time(
                        sample_id=len(samples) + 1,
                        phase="during_capture",
                    )
                )
            except Exception:
                continue


async def _sample_binance_server_time(*, sample_id: int, phase: str) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        before_ms = time.time() * 1000.0
        async with session.get(BINANCE_SERVER_TIME_URL) as response:
            payload = await response.json()
        after_ms = time.time() * 1000.0
    server_time = payload.get("serverTime") if isinstance(payload, dict) else None
    if not isinstance(server_time, (int, float)) or isinstance(server_time, bool):
        raise RuntimeError(f"invalid Binance server time payload: {payload!r}")
    return build_server_time_sample(
        sample_id=sample_id,
        phase=phase,
        local_wall_before_request_ms=before_ms,
        local_wall_after_response_ms=after_ms,
        binance_server_time_ms=float(server_time),
    )


def _write_and_bundle(
    report: dict[str, Any],
    *,
    root: Path,
    pytest_output: str,
    no_bundle: bool,
) -> None:
    pass_bundle = report.get("status") == "pass"
    bundle_path = root / (PHASE42FG_PASS_BUNDLE if pass_bundle else PHASE42FG_FAIL_AUDIT_BUNDLE)
    write_phase42fg_artifacts(
        report,
        root=root,
        pytest_output=pytest_output,
        bundle_created=not no_bundle,
        bundle_path=bundle_path,
    )
    if no_bundle:
        return
    try:
        create_phase42fg_bundle(root=root, pass_bundle=pass_bundle, bundle_path=bundle_path)
    except Exception as exc:
        report["status"] = "fail"
        report["primary_failure"] = "BUNDLE_FAILURE"
        report["hard_fail_reasons"].append(f"bundle failure: {exc}")
        report = evaluate_phase42fg_report(report)
        fail_path = root / PHASE42FG_FAIL_AUDIT_BUNDLE
        write_phase42fg_artifacts(
            report,
            root=root,
            pytest_output=pytest_output,
            bundle_created=False,
            bundle_path=fail_path,
        )
        raise


def _failure_report(
    *,
    args: argparse.Namespace,
    cleanup_report: dict[str, Any],
    gitignore_validation: dict[str, Any],
    classification: str,
    reason: str,
    pytest_passed: bool = True,
    typecheck_passed: bool = True,
    typecheck_summary: str = "",
) -> dict[str, Any]:
    report = {
        "phase": "4.2F+G",
        "status": "fail",
        "implementation_status": "fail" if classification in {"TEST_FAILURE", "TYPECHECK_FAILURE"} else "pass",
        "fresh_capture_status": "fail",
        "clock_sync_status": "fail",
        "readiness_semantics_status": "pass",
        "latency_profile_status": "fail",
        "strict_100ms_observability_status": "fail",
        "protocol_decision_status": "pass",
        "receive_time_coverage_status": "fail",
        "exchange_time_coverage_status": "fail",
        "corrected_hybrid_status": "fail",
        "primary_failure": classification,
        "failure_classifications": [classification],
        "market_time_label_ready": False,
        "strict_100ms_observability_ready": False,
        "relaxed_250ms_observability_candidate": False,
        "low_latency_ready": False,
        "phase5_ready": False,
        "selected_protocol_candidate": None,
        "selected_operational_budget_ms": None,
        "readiness_decision_reason": "benchmark_failed_before_readiness_decision",
        "symbol": args.symbol.upper(),
        "duration_sec": float(args.duration_sec),
        "fresh_capture_performed": False,
        "fixture_mode": bool(args.skip_capture),
        "skip_capture": bool(args.skip_capture),
        "max_future_gap_ms": 100,
        "future_receive_lag_hard_gate_used": False,
        "fresh_capture_required": not bool(args.skip_capture),
        "capture": _capture_summary(args, fresh=False, cleanup_report=cleanup_report),
        "cleanup_report": cleanup_report,
        "gitignore_validation": gitignore_validation,
        "pytest_passed": pytest_passed,
        "typecheck_passed": typecheck_passed,
        "typecheck_summary": typecheck_summary,
        "clock_offset_samples": [],
        "clock_offset_summary": {},
        "clock_sanity_report": {},
        "leakage_check": {},
        "receive_lag_summary": {},
        "hot_path_latency_summary": {},
        "queue_backpressure_summary": {},
        "sources": {},
        "hard_fail_reasons": [reason],
        "warning_reasons": [],
    }
    return evaluate_phase42fg_report(report)


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


def _run_typecheck(output_path: Path) -> tuple[int, str]:
    if importlib.util.find_spec("pyright") is not None:
        tool = "pyright"
        command = [sys.executable, "-X", "utf8", "-m", "pyright"]
    elif importlib.util.find_spec("mypy") is not None and _mypy_is_configured():
        tool = "mypy"
        command = [sys.executable, "-X", "utf8", "-m", "mypy", "bot/app", "scripts", "tests"]
    else:
        tool = "compileall"
        command = [sys.executable, "-X", "utf8", "-m", "compileall", "bot/app", "scripts", "tests"]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(SOURCE_ROOT / "bot")
    process = subprocess.run(command, cwd=SOURCE_ROOT, env=env, text=True, capture_output=True)
    output = process.stdout + process.stderr
    summary = "passed" if process.returncode == 0 else "failed"
    report = "\n".join(
        [
            f"tool used: {tool}",
            f"command: {' '.join(command)}",
            f"exit code: {process.returncode}",
            f"summary: {summary}",
            "known remaining warnings: none" if process.returncode == 0 else "known remaining warnings: blocking errors above",
            "",
            output,
        ]
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    sys.stdout.write(process.stdout)
    sys.stderr.write(process.stderr)
    return process.returncode, f"typecheck/compileall {summary} with {tool}"


def _mypy_is_configured() -> bool:
    if (SOURCE_ROOT / "mypy.ini").exists() or (SOURCE_ROOT / ".mypy.ini").exists():
        return True
    setup_cfg = SOURCE_ROOT / "setup.cfg"
    if setup_cfg.exists() and "[mypy]" in setup_cfg.read_text(encoding="utf-8", errors="ignore"):
        return True
    pyproject = SOURCE_ROOT / "pyproject.toml"
    return pyproject.exists() and "[tool.mypy" in pyproject.read_text(encoding="utf-8", errors="ignore")


def _capture_summary(
    args: argparse.Namespace,
    *,
    fresh: bool,
    cleanup_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "fresh_capture_performed": fresh,
        "fixture_mode": bool(args.skip_capture),
        "skip_capture": bool(args.skip_capture),
        "cleanup_performed": bool(cleanup_report.get("cleanup_performed", False)),
        "duration_sec": float(args.duration_sec),
        "depth_stream": f"{args.symbol.lower()}@depth@100ms",
        "requested_streams": required_streams(args.symbol),
        "reference_streams": [
            f"{args.symbol.lower()}@bookTicker",
            f"{args.symbol.lower()}@trade",
            f"{args.symbol.lower()}@aggTrade",
        ],
        "downsampling_enabled": False,
        "depth_clean_sample_count": 0,
        "latency_profile_sample_count": 0,
        "reference_event_counts": {
            "bookTicker_mid": 0,
            "trade_price": 0,
            "aggTrade_price": 0,
        },
    }


def _fixture_capture(args: argparse.Namespace, *, root: Path) -> dict[str, Any]:
    diagnostics = {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "skip_capture": True,
        "symbol": args.symbol.upper(),
        "duration_sec": float(args.duration_sec),
        "requested_streams": required_streams(args.symbol),
        "message_count_by_stream": {stream: 1 for stream in required_streams(args.symbol)},
        "parsed_count_by_source": {
            "depth_mid": _count_jsonl(root / args.input_clean_samples),
            "bookTicker_mid": _count_jsonl(root / args.input_bookticker),
            "trade_price": _count_jsonl(root / args.input_trade),
            "aggTrade_price": _count_jsonl(root / args.input_aggtrade),
        },
    }
    return {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "skip_capture": True,
        "capture_diagnostics": diagnostics,
        "capture_diagnostic_errors": [],
        "depth_clean_sample_count": _count_jsonl(root / args.input_clean_samples),
        "latency_profile_sample_count": _count_jsonl(root / args.input_latency_profile),
        "reference_event_counts": {
            "bookTicker_mid": _count_jsonl(root / args.input_bookticker),
            "trade_price": _count_jsonl(root / args.input_trade),
            "aggTrade_price": _count_jsonl(root / args.input_aggtrade),
        },
    }


def _fixture_clock_samples() -> list[dict[str, Any]]:
    return [
        build_server_time_sample(
            sample_id=1,
            phase="before_capture",
            local_wall_before_request_ms=37_500.0,
            local_wall_after_response_ms=37_510.0,
            binance_server_time_ms=5.0,
        ),
        build_server_time_sample(
            sample_id=2,
            phase="after_capture",
            local_wall_before_request_ms=37_520.0,
            local_wall_after_response_ms=37_530.0,
            binance_server_time_ms=25.0,
        ),
    ]


def _count_jsonl(path: str | Path) -> int:
    target = Path(path)
    if not target.exists():
        return 0
    with target.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
