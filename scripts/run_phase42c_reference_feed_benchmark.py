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

from app.marketdata.binance_aggtrade_source import parse_aggtrade_payload  # noqa: E402
from app.marketdata.binance_trade_source import parse_trade_payload  # noqa: E402
from app.research.bookticker_reference import parse_bookticker_payload  # noqa: E402
from app.research.orderbook_100ms_coverage import runtime_quality_from_phase41_report  # noqa: E402
from app.research.reference_feed_benchmark import (  # noqa: E402
    AGGTRADE_REFERENCE_EVENTS,
    BOOKTICKER_REFERENCE_QUOTES,
    PHASE42C_BUNDLE,
    TRADE_REFERENCE_EVENTS,
    classify_phase42c_failure,
    create_phase42c_bundle,
    empty_phase42c_report,
    run_phase42c_analysis,
    write_phase42c_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 4.2C multi-reference-feed 100ms benchmark self-check."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-sec", type=float, default=1800.0)
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--input-clean-samples", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--input-bookticker", default=str(BOOKTICKER_REFERENCE_QUOTES))
    parser.add_argument("--input-trade", default=str(TRADE_REFERENCE_EVENTS))
    parser.add_argument("--input-aggtrade", default=str(AGGTRADE_REFERENCE_EVENTS))
    parser.add_argument("--output-labels", default="data/dataset/orderbook_reference_benchmark_labels.jsonl")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--no-bundle", action="store_true")
    args = parser.parse_args(argv)
    del args.max_attempts

    root = Path(args.root).resolve()
    debug_dir = root / "data/debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = root / PHASE42C_BUNDLE
    if bundle_path.exists():
        bundle_path.unlink()

    pytest_output_path = debug_dir / "phase_4_2c_pytest_output.txt"
    if args.skip_pytest:
        pytest_output = "pytest skipped by explicit --skip-pytest test hook\n"
        pytest_output_path.write_text(pytest_output, encoding="utf-8")
    else:
        pytest_code, pytest_output = _run_pytest(pytest_output_path)
        if pytest_code != 0:
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=_capture_summary(args, fresh=False),
                classification="TEST_FAILURE",
                reason="pytest failed",
            )
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2C failed: TEST_FAILURE")
            return pytest_code

    capture = _capture_summary(args, fresh=False)
    runtime_quality: dict[str, Any]
    runtime_errors: list[str]

    if args.skip_capture:
        runtime_quality = _passing_depth_runtime_quality()
        runtime_errors = []
    else:
        capture_code, reference_counts = asyncio.run(
            _run_multi_feed_capture(
                symbol=args.symbol,
                duration_sec=args.duration_sec,
                depth_n=args.depth_n,
            )
        )
        capture["fresh_capture_performed"] = capture_code == 0
        capture["fixture_mode"] = False
        capture["depth_clean_sample_count"] = _count_jsonl(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl")
        capture["reference_event_counts"] = reference_counts
        if capture_code != 0:
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=capture,
                classification="MULTI_FEED_CAPTURE_FAILURE",
                reason=f"multi-feed capture exited {capture_code}; reference_counts={reference_counts}",
            )
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2C failed: MULTI_FEED_CAPTURE_FAILURE")
            return capture_code or 1
        runtime_quality, runtime_errors = runtime_quality_from_phase41_report(
            SOURCE_ROOT / "data/reports/phase_4_1_orderbook_quality_report.json"
        )
        if root != SOURCE_ROOT:
            _copy_if_exists(
                SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl",
                root / "data/dataset/orderbook_clean_samples.jsonl",
            )
            _copy_if_exists(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES, root / BOOKTICKER_REFERENCE_QUOTES)
            _copy_if_exists(SOURCE_ROOT / TRADE_REFERENCE_EVENTS, root / TRADE_REFERENCE_EVENTS)
            _copy_if_exists(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS, root / AGGTRADE_REFERENCE_EVENTS)

    report = run_phase42c_analysis(
        root=root,
        symbol=args.symbol,
        clean_samples_path=args.input_clean_samples,
        bookticker_path=args.input_bookticker,
        trade_path=args.input_trade,
        aggtrade_path=args.input_aggtrade,
        benchmark_labels_path=args.output_labels,
        depth_runtime_quality=runtime_quality,
        capture={
            **capture,
            "depth_clean_sample_count": _count_jsonl(_resolve(root, args.input_clean_samples)),
            "reference_event_counts": {
                "bookTicker_mid": _count_jsonl(_resolve(root, args.input_bookticker)),
                "trade_price": _count_jsonl(_resolve(root, args.input_trade)),
                "aggTrade_price": _count_jsonl(_resolve(root, args.input_aggtrade)),
            },
        },
        fresh_capture_required=not args.skip_capture,
    )
    if runtime_errors:
        report["runtime_status"] = "fail"
        report["definition_of_done_status"] = "fail"
        report["status"] = "fail"
        report["primary_failure"] = report.get("primary_failure") or "DEPTH_RUNTIME_QUALITY_FAILURE"
        report["hard_fail_reasons"].extend(runtime_errors)
    write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)

    if report.get("definition_of_done_status") != "pass":
        print(f"Phase 4.2C failed: {classify_phase42c_failure(report)}")
        return 1

    if not args.no_bundle:
        try:
            create_phase42c_bundle(root=root, source_root=SOURCE_ROOT, bundle_path=bundle_path)
            report["bundle_created"] = True
            write_phase42c_artifacts(
                report,
                root=root,
                pytest_output=pytest_output,
                bundle_created=True,
            )
        except Exception as exc:
            report["definition_of_done_status"] = "fail"
            report["status"] = "fail"
            report["primary_failure"] = "BUNDLE_FAILURE"
            report["hard_fail_reasons"].append(f"bundle failure: {exc}")
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            if bundle_path.exists():
                bundle_path.unlink()
            print(f"Phase 4.2C bundle failed: {exc}")
            return 1
    print("Phase 4.2C self-check passed")
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


async def _run_multi_feed_capture(
    *,
    symbol: str,
    duration_sec: float,
    depth_n: int,
) -> tuple[int, dict[str, int]]:
    for path in (BOOKTICKER_REFERENCE_QUOTES, TRADE_REFERENCE_EVENTS, AGGTRADE_REFERENCE_EVENTS):
        target = SOURCE_ROOT / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")

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
    reference_task = asyncio.create_task(_capture_references(symbol=symbol, duration_sec=duration_sec))
    depth_code, reference_counts = await asyncio.gather(depth_process.wait(), reference_task)
    reference_ok = all(reference_counts.get(source, 0) > 0 for source in ("bookTicker_mid", "trade_price", "aggTrade_price"))
    code = depth_code if depth_code != 0 else (0 if reference_ok else 1)
    return code, reference_counts


async def _capture_references(*, symbol: str, duration_sec: float) -> dict[str, int]:
    streams = [f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@trade", f"{symbol.lower()}@aggTrade"]
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
    deadline = time.monotonic() + duration_sec
    counts = {"bookTicker_mid": 0, "trade_price": 0, "aggTrade_price": 0}
    reconnect_count = 0
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=None)
    paths = {
        "bookTicker_mid": SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES,
        "trade_price": SOURCE_ROOT / TRADE_REFERENCE_EVENTS,
        "aggTrade_price": SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS,
    }
    handles = {
        source: path.open("a", encoding="utf-8", newline="\n")
        for source, path in paths.items()
    }
    try:
        while time.monotonic() < deadline:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as websocket:
                        while time.monotonic() < deadline:
                            remaining = max(0.1, min(5.0, deadline - time.monotonic()))
                            message = await asyncio.wait_for(websocket.receive(), timeout=remaining)
                            if message.type == aiohttp.WSMsgType.TEXT:
                                received_ns = time.monotonic_ns()
                                wall_ts = datetime.now(timezone.utc).isoformat()
                                payload = json.loads(message.data)
                                source, row = _parse_reference_message(
                                    payload,
                                    local_recv_monotonic_ns=received_ns,
                                    local_recv_wall_ts=wall_ts,
                                )
                                if source is None or row is None:
                                    continue
                                handles[source].write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
                                counts[source] += 1
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
    finally:
        for handle in handles.values():
            handle.close()
    del reconnect_count
    return counts


def _parse_reference_message(
    payload: dict[str, Any],
    *,
    local_recv_monotonic_ns: int,
    local_recv_wall_ts: str,
) -> tuple[str | None, dict[str, Any] | None]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    stream = str(payload.get("stream", ""))
    if stream.endswith("@bookTicker") or {"u", "b", "B", "a", "A"}.issubset(data):
        return "bookTicker_mid", parse_bookticker_payload(
            data,
            local_recv_monotonic_ns=local_recv_monotonic_ns,
            local_recv_wall_ts=local_recv_wall_ts,
        )
    if data.get("e") == "trade" or stream.endswith("@trade"):
        return "trade_price", parse_trade_payload(
            data,
            local_recv_monotonic_ns=local_recv_monotonic_ns,
            local_recv_wall_ts=local_recv_wall_ts,
        )
    if data.get("e") == "aggTrade" or stream.endswith("@aggTrade"):
        return "aggTrade_price", parse_aggtrade_payload(
            data,
            local_recv_monotonic_ns=local_recv_monotonic_ns,
            local_recv_wall_ts=local_recv_wall_ts,
        )
    return None, None


def _capture_summary(args: argparse.Namespace, *, fresh: bool) -> dict[str, Any]:
    return {
        "fresh_capture_performed": fresh,
        "fixture_mode": bool(args.skip_capture),
        "duration_sec": float(args.duration_sec),
        "depth_stream": f"{args.symbol.lower()}@depth@100ms",
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
        "snapshot_copy_p99_us": 0.0,
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


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
