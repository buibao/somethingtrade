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
from typing import Any


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))
SCRIPTS_PATH = SOURCE_ROOT / "scripts"
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PATH))

from app.research.reference_feed_benchmark import (  # noqa: E402
    AGGTRADE_REFERENCE_EVENTS,
    BOOKTICKER_REFERENCE_QUOTES,
    TRADE_REFERENCE_EVENTS,
    required_streams,
    validate_capture_diagnostics,
)
from app.research.time_protocol_benchmark import (  # noqa: E402
    PHASE42D_BUNDLE,
    PHASE42D_CAPTURE_DIAGNOSTICS,
    PHASE42D_REPORT_JSON,
    PHASE42D_TYPECHECK_REPORT,
    TIME_PROTOCOL_LABELS,
    cleanup_phase42d_artifacts,
    create_phase42d_bundle,
    evaluate_phase42d_report,
    run_phase42d_analysis,
    validate_gitignore_rules,
    write_phase42d_artifacts,
)
from run_phase42c_reference_feed_benchmark import (  # noqa: E402
    _build_fixture_capture_diagnostics,
    _run_multi_feed_capture,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 4.2D exchange-time vs receive-time protocol benchmark."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=float, default=1800.0)
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--allow-fixture-mode", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--input-clean-samples", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--input-bookticker", default=str(BOOKTICKER_REFERENCE_QUOTES))
    parser.add_argument("--input-trade", default=str(TRADE_REFERENCE_EVENTS))
    parser.add_argument("--input-aggtrade", default=str(AGGTRADE_REFERENCE_EVENTS))
    parser.add_argument("--output-labels", default=str(TIME_PROTOCOL_LABELS))
    parser.add_argument("--no-bundle", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    (root / "data/debug").mkdir(parents=True, exist_ok=True)
    bundle_path = root / PHASE42D_BUNDLE
    if bundle_path.exists():
        bundle_path.unlink()

    cleanup_report: dict[str, Any] = {
        "cleanup_performed": False,
        "deleted_paths": [],
        "missing_paths_skipped": [],
        "errors": [],
    }
    if args.clean:
        cleanup_report = cleanup_phase42d_artifacts(root)
        if cleanup_report.get("errors"):
            report = _run_analysis_for_failure(
                root=root,
                args=args,
                cleanup_report=cleanup_report,
                gitignore_validation=validate_gitignore_rules(root),
                pytest_passed=True,
                typecheck_passed=True,
                typecheck_summary="not run because cleanup failed",
                capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
            )
            report["hard_fail_reasons"].append(f"artifact cleanup failed: {cleanup_report.get('errors')}")
            report["primary_failure"] = report.get("primary_failure") or "ARTIFACT_CLEANUP_FAILURE"
            report = evaluate_phase42d_report(report)
            write_phase42d_artifacts(report, root=root, pytest_output="")
            print("Phase 4.2D failed: ARTIFACT_CLEANUP_FAILURE")
            return 1
    else:
        _write_json(root / "data/debug/phase_4_2d_artifact_cleanup.json", cleanup_report)

    gitignore_validation = validate_gitignore_rules(root)
    pytest_output_path = root / "data/debug/phase_4_2d_pytest_output.txt"
    if args.skip_pytest:
        pytest_output = "pytest skipped by explicit --skip-pytest test hook\n"
        pytest_output_path.parent.mkdir(parents=True, exist_ok=True)
        pytest_output_path.write_text(pytest_output, encoding="utf-8")
        pytest_passed = True
    else:
        pytest_code, pytest_output = _run_pytest(pytest_output_path)
        pytest_passed = pytest_code == 0
        if not pytest_passed:
            report = _run_analysis_for_failure(
                root=root,
                args=args,
                cleanup_report=cleanup_report,
                gitignore_validation=gitignore_validation,
                pytest_passed=False,
                typecheck_passed=True,
                typecheck_summary="not run because pytest failed",
                capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
            )
            write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2D failed: TEST_FAILURE")
            return 1

    typecheck_code, typecheck_summary = _run_typecheck(root / PHASE42D_TYPECHECK_REPORT)
    typecheck_passed = typecheck_code == 0
    if not typecheck_passed:
        report = _run_analysis_for_failure(
            root=root,
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            pytest_passed=pytest_passed,
            typecheck_passed=False,
            typecheck_summary=typecheck_summary,
            capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
        )
        write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2D failed: TYPECHECK_FAILURE")
        return 1

    capture = _capture_summary(args, fresh=False, cleanup_report=cleanup_report)
    if args.skip_capture and not args.allow_fixture_mode:
        report = _run_analysis_for_failure(
            root=root,
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            pytest_passed=pytest_passed,
            typecheck_passed=typecheck_passed,
            typecheck_summary=typecheck_summary,
            capture=capture,
        )
        write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2D failed: FRESH_CAPTURE_NOT_PERFORMED")
        return 1

    if not args.skip_capture and float(args.duration_sec) < 1800.0:
        report = _run_analysis_for_failure(
            root=root,
            args=args,
            cleanup_report=cleanup_report,
            gitignore_validation=gitignore_validation,
            pytest_passed=pytest_passed,
            typecheck_passed=typecheck_passed,
            typecheck_summary=typecheck_summary,
            capture=capture,
        )
        write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2D failed: FRESH_CAPTURE_DURATION_FAILURE")
        return 1

    if args.skip_capture:
        diagnostics = _build_fixture_capture_diagnostics(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            root=root,
        )
        _write_json(root / PHASE42D_CAPTURE_DIAGNOSTICS, diagnostics)
        capture = _capture_summary(args, fresh=False, cleanup_report=cleanup_report)
        capture["capture_diagnostics"] = diagnostics
        capture["depth_clean_sample_count"] = _count_jsonl(root / args.input_clean_samples)
        capture["reference_event_counts"] = {
            "bookTicker_mid": _count_jsonl(root / args.input_bookticker),
            "trade_price": _count_jsonl(root / args.input_trade),
            "aggTrade_price": _count_jsonl(root / args.input_aggtrade),
        }
    else:
        capture_code, diagnostics = asyncio.run(
            _run_multi_feed_capture(
                symbol=args.symbol,
                duration_sec=args.duration_sec,
                depth_n=args.depth_n,
                root=SOURCE_ROOT,
            )
        )
        diagnostic_errors = validate_capture_diagnostics(diagnostics, symbol=args.symbol)
        _write_json(root / PHASE42D_CAPTURE_DIAGNOSTICS, diagnostics)
        capture = _capture_summary(args, fresh=capture_code == 0 and not diagnostic_errors, cleanup_report=cleanup_report)
        capture["capture_diagnostics"] = diagnostics
        capture["depth_clean_sample_count"] = _count_jsonl(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl")
        capture["reference_event_counts"] = {
            "bookTicker_mid": _count_jsonl(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES),
            "trade_price": _count_jsonl(SOURCE_ROOT / TRADE_REFERENCE_EVENTS),
            "aggTrade_price": _count_jsonl(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS),
        }
        capture["capture_exit_code"] = capture_code
        capture["capture_diagnostic_errors"] = diagnostic_errors
        if root != SOURCE_ROOT:
            _copy_if_exists(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl", root / "data/dataset/orderbook_clean_samples.jsonl")
            _copy_if_exists(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES, root / BOOKTICKER_REFERENCE_QUOTES)
            _copy_if_exists(SOURCE_ROOT / TRADE_REFERENCE_EVENTS, root / TRADE_REFERENCE_EVENTS)
            _copy_if_exists(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS, root / AGGTRADE_REFERENCE_EVENTS)

    report = run_phase42d_analysis(
        root=root,
        symbol=args.symbol,
        clean_samples_path=args.input_clean_samples,
        bookticker_path=args.input_bookticker,
        trade_path=args.input_trade,
        aggtrade_path=args.input_aggtrade,
        time_protocol_labels_path=args.output_labels,
        capture=capture,
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        fresh_capture_required=not args.skip_capture,
    )
    write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)

    if report.get("status") != "pass":
        print(f"Phase 4.2D failed: {report.get('primary_failure')}")
        return 1

    if not args.no_bundle:
        try:
            create_phase42d_bundle(root=root, source_root=SOURCE_ROOT, bundle_path=bundle_path)
            report["bundle_created"] = True
            write_phase42d_artifacts(report, root=root, pytest_output=pytest_output, bundle_created=True)
        except Exception as exc:
            report["status"] = "fail"
            report["primary_failure"] = "BUNDLE_FAILURE"
            report["hard_fail_reasons"].append(f"bundle failure: {exc}")
            report = evaluate_phase42d_report(report)
            write_phase42d_artifacts(report, root=root, pytest_output=pytest_output)
            if bundle_path.exists():
                bundle_path.unlink()
            print(f"Phase 4.2D bundle failed: {exc}")
            return 1
    print("Phase 4.2D self-check passed")
    return 0


def _run_analysis_for_failure(
    *,
    root: Path,
    args: argparse.Namespace,
    cleanup_report: dict[str, Any],
    gitignore_validation: dict[str, Any],
    pytest_passed: bool,
    typecheck_passed: bool,
    typecheck_summary: str,
    capture: dict[str, Any],
) -> dict[str, Any]:
    return run_phase42d_analysis(
        root=root,
        symbol=args.symbol,
        clean_samples_path=args.input_clean_samples,
        bookticker_path=args.input_bookticker,
        trade_path=args.input_trade,
        aggtrade_path=args.input_aggtrade,
        time_protocol_labels_path=args.output_labels,
        capture=capture,
        cleanup_report=cleanup_report,
        gitignore_validation=gitignore_validation,
        pytest_passed=pytest_passed,
        typecheck_passed=typecheck_passed,
        typecheck_summary=typecheck_summary,
        fresh_capture_required=not bool(args.skip_capture),
    )


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
    process = subprocess.run(
        command,
        cwd=SOURCE_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
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
        "reference_event_counts": {
            "bookTicker_mid": 0,
            "trade_price": 0,
            "aggTrade_price": 0,
        },
    }


def _count_jsonl(path: str | Path) -> int:
    target = Path(path)
    if not target.exists():
        return 0
    with target.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _copy_if_exists(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
