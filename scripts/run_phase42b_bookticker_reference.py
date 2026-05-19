from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

import aiohttp


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.bookticker_reference import (  # noqa: E402
    BOOKTICKER_REFERENCE_QUOTES,
    PHASE42B_BUNDLE,
    classify_phase42b_failure,
    create_phase42b_bundle,
    parse_bookticker_payload,
    run_phase42b_analysis,
    write_phase42b_artifacts,
)
from app.research.orderbook_100ms_coverage import runtime_quality_from_phase41_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 4.2B bookTicker reference label feed self-check."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=float, default=1800.0)
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--input-clean-samples", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--input-reference-quotes", default=str(BOOKTICKER_REFERENCE_QUOTES))
    parser.add_argument("--output-labeled-samples", default="data/dataset/orderbook_labeled_samples.jsonl")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--no-bundle", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    debug_dir = root / "data/debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = root / PHASE42B_BUNDLE
    if bundle_path.exists():
        bundle_path.unlink()

    pytest_output_path = debug_dir / "phase_4_2b_pytest_output.txt"
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
                capture=_capture_summary(args, fresh=False),
            )
            write_phase42b_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2B failed: TEST_FAILURE")
            return pytest_code

    capture = _capture_summary(args, fresh=False)
    runtime_quality: dict[str, Any]
    runtime_errors: list[str]

    if args.skip_capture:
        runtime_quality = _passing_depth_runtime_quality()
        runtime_errors = []
    else:
        capture_code = asyncio.run(
            _run_dual_feed_capture(
                symbol=args.symbol,
                duration_sec=args.duration_sec,
                depth_n=args.depth_n,
            )
        )
        capture["fresh_capture_performed"] = capture_code == 0
        capture["fixture_mode"] = False
        capture["depth_clean_sample_count"] = _count_jsonl(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl")
        capture["reference_quote_count"] = _count_jsonl(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES)
        if capture_code != 0:
            report = _minimal_failure_report(
                symbol=args.symbol,
                classification="DUAL_FEED_CAPTURE_FAILURE",
                reason=f"dual-feed capture exited {capture_code}",
                capture=capture,
            )
            write_phase42b_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2B failed: DUAL_FEED_CAPTURE_FAILURE")
            return capture_code or 1
        runtime_quality, runtime_errors = runtime_quality_from_phase41_report(
            SOURCE_ROOT / "data/reports/phase_4_1_orderbook_quality_report.json"
        )
        if root != SOURCE_ROOT:
            _copy_if_exists(
                SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl",
                root / "data/dataset/orderbook_clean_samples.jsonl",
            )
            _copy_if_exists(
                SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES,
                root / BOOKTICKER_REFERENCE_QUOTES,
            )

    report = run_phase42b_analysis(
        root=root,
        symbol=args.symbol,
        clean_samples_path=args.input_clean_samples,
        reference_quotes_path=args.input_reference_quotes,
        labeled_samples_path=args.output_labeled_samples,
        depth_runtime_quality=runtime_quality,
        capture=capture,
        fresh_capture_required=not args.skip_capture,
    )
    if runtime_errors:
        report["runtime_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = report.get("primary_failure") or "depth_runtime_quality_failed"
        report["hard_fail_reasons"].extend(runtime_errors)
    write_phase42b_artifacts(report, root=root, pytest_output=pytest_output)

    if report.get("definition_of_done_status") != "pass":
        print(f"Phase 4.2B failed: {classify_phase42b_failure(report)}")
        return 1

    if not args.no_bundle:
        try:
            create_phase42b_bundle(root=root, source_root=SOURCE_ROOT, bundle_path=bundle_path)
            report["bundle_created"] = True
            write_phase42b_artifacts(
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
            write_phase42b_artifacts(report, root=root, pytest_output=pytest_output)
            if bundle_path.exists():
                bundle_path.unlink()
            print(f"Phase 4.2B bundle failed: {exc}")
            return 1
    print("Phase 4.2B self-check passed")
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


async def _run_dual_feed_capture(*, symbol: str, duration_sec: float, depth_n: int) -> int:
    reference_path = SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    reference_path.write_text("", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(SOURCE_ROOT / "bot")
    depth_process = await asyncio.create_subprocess_exec(
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
        cwd=SOURCE_ROOT / "bot",
        env=env,
    )
    reference_task = asyncio.create_task(
        _capture_bookticker_reference(symbol=symbol, duration_sec=duration_sec, output_path=reference_path)
    )
    depth_code, reference_code = await asyncio.gather(depth_process.wait(), reference_task)
    return depth_code if depth_code != 0 else reference_code


async def _capture_bookticker_reference(*, symbol: str, duration_sec: float, output_path: Path) -> int:
    stream = f"{symbol.lower()}@bookTicker"
    url = f"wss://stream.binance.com:9443/ws/{stream}"
    deadline = time.monotonic() + duration_sec
    message_count = 0
    reconnect_count = 0
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=None)
    with output_path.open("a", encoding="utf-8", newline="\n") as handle:
        while time.monotonic() < deadline:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as websocket:
                        while time.monotonic() < deadline:
                            remaining = max(0.1, min(5.0, deadline - time.monotonic()))
                            message = await asyncio.wait_for(websocket.receive(), timeout=remaining)
                            if message.type == aiohttp.WSMsgType.TEXT:
                                received_ns = time.monotonic_ns()
                                payload = json.loads(message.data)
                                row = parse_bookticker_payload(
                                    payload,
                                    local_recv_monotonic_ns=received_ns,
                                    local_recv_wall_ts=datetime.now(timezone.utc).isoformat(),
                                )
                                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
                                message_count += 1
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                break
            except asyncio.TimeoutError:
                continue
            except (aiohttp.ClientError, OSError, json.JSONDecodeError):
                reconnect_count += 1
                if time.monotonic() < deadline:
                    await asyncio.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    del reconnect_count
    return 0 if message_count > 0 else 1


def _capture_summary(args: argparse.Namespace, *, fresh: bool) -> dict[str, Any]:
    return {
        "fresh_capture_performed": fresh,
        "fixture_mode": bool(args.skip_capture),
        "duration_sec": float(args.duration_sec),
        "depth_stream": f"{args.symbol.lower()}@depth@100ms",
        "reference_stream": f"{args.symbol.lower()}@bookTicker",
        "downsampling_enabled": False,
        "depth_clean_sample_count": 0,
        "reference_quote_count": 0,
    }


def _passing_depth_runtime_quality() -> dict[str, Any]:
    return {
        "sample_before_ready_count": 0,
        "feed_receive_stale_count": 0,
        "queue_dropped_messages": 0,
        "sequence_gap_count": 0,
        "invalid_delta_count": 0,
        "crossed_book_count": 0,
        "book_empty_count": 0,
        "one_side_missing_count": 0,
        "clean_sample_schema_violation_count": 0,
    }


def _minimal_failure_report(
    *,
    symbol: str,
    classification: str,
    reason: str,
    capture: dict[str, Any],
) -> dict[str, Any]:
    return {
        "phase": "4.2B",
        "status": "fail",
        "implementation_status": "fail" if classification == "TEST_FAILURE" else "pass",
        "runtime_status": "fail" if classification == "DUAL_FEED_CAPTURE_FAILURE" else "pass",
        "reference_feed_status": "fail",
        "dataset_coverage_status": "fail",
        "definition_of_done_status": "fail",
        "primary_failure": "pytest_failed" if classification == "TEST_FAILURE" else "dual_feed_capture_failed",
        "pytest_failed": classification == "TEST_FAILURE",
        "symbol": symbol,
        "inputs": {
            "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
            "bookticker_reference_quotes": "data/dataset/bookticker_reference_quotes.jsonl",
        },
        "outputs": {"labeled_samples": "data/dataset/orderbook_labeled_samples.jsonl"},
        "capture": capture,
        "fresh_capture_required": True,
        "depth_runtime_quality": _passing_depth_runtime_quality(),
        "reference_feed_quality": {
            "reference_quote_count": 0,
            "valid_reference_quote_count": 0,
            "invalid_reference_quote_count": 0,
            "reference_sample_rate_per_sec": 0.0,
            "reference_gap_p50_ms": None,
            "reference_gap_p90_ms": None,
            "reference_gap_p95_ms": None,
            "reference_gap_p99_ms": None,
            "reference_gap_max_ms": None,
            "duplicate_update_id_count": 0,
            "non_monotonic_reference_timestamp_count": 0,
            "invalid_quote_reason_counts": {},
        },
        "alignment_quality": {
            "feature_sample_count": 0,
            "labeled_sample_count": 0,
            "feature_to_reference_no_future_count": 0,
            "feature_to_reference_gap_too_large_count": 0,
        },
        "horizon_100ms": {
            "reference_source": "bookTicker",
            "max_future_gap_ms": 100,
            "eligible_count": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "tail_no_future_count": 0,
            "valid_rate_all_rows": 0.0,
            "valid_rate_eligible_rows": 0.0,
            "invalid_reason_counts": {},
            "future_gap_ms_p50": None,
            "future_gap_ms_p90": None,
            "future_gap_ms_p95": None,
            "future_gap_ms_p99": None,
            "future_gap_ms_max": None,
        },
        "leakage_check": {
            "passed": False,
            "feature_leakage_violations": 0,
            "label_leakage_violations": 0,
            "violations": [],
        },
        "clean_sample_count": 0,
        "labeled_sample_count": 0,
        "hard_fail_reasons": [reason],
        "warning_reasons": [],
        "bottleneck_assessment": "Self-check stopped before bookTicker coverage analysis.",
    }


def _copy_if_exists(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(source.read_bytes())


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


if __name__ == "__main__":
    raise SystemExit(main())
