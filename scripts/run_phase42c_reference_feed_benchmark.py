from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import importlib.util
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
    PHASE42C_CAPTURE_DIAGNOSTICS,
    PHASE42C_BUNDLE,
    PHASE42C_CLEANUP_REPORT,
    PHASE42C_TYPECHECK_REPORT,
    TRADE_REFERENCE_EVENTS,
    classify_phase42c_failure,
    cleanup_phase42c_artifacts,
    create_phase42c_bundle,
    empty_phase42c_report,
    required_streams,
    run_phase42c_analysis,
    validate_capture_diagnostics,
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
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--allow-fixture-mode", action="store_true")
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

    cleanup_report: dict[str, Any] = {
        "cleanup_performed": False,
        "deleted_files": [],
        "missing_files_skipped": [],
        "errors": [],
    }
    if args.clean:
        cleanup_report = cleanup_phase42c_artifacts(root)
        if cleanup_report.get("errors"):
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
                classification="ARTIFACT_CLEANUP_FAILURE",
                reason=f"artifact cleanup failed: {cleanup_report.get('errors')}",
            )
            report["cleanup_failed"] = True
            write_phase42c_artifacts(report, root=root, pytest_output="")
            print("Phase 4.2C failed: ARTIFACT_CLEANUP_FAILURE")
            return 1
    else:
        _write_json(root / PHASE42C_CLEANUP_REPORT, cleanup_report)

    pytest_output_path = debug_dir / "phase_4_2c_pytest_output.txt"
    if args.skip_pytest:
        pytest_output = "pytest skipped by explicit --skip-pytest test hook\n"
        pytest_output_path.write_text(pytest_output, encoding="utf-8")
    else:
        pytest_code, pytest_output = _run_pytest(pytest_output_path)
        if pytest_code != 0:
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
                classification="TEST_FAILURE",
                reason="pytest failed",
            )
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2C failed: TEST_FAILURE")
            return pytest_code

    typecheck_code, typecheck_summary = _run_typecheck(root / PHASE42C_TYPECHECK_REPORT)
    if typecheck_code != 0:
        report = empty_phase42c_report(
            symbol=args.symbol,
            capture=_capture_summary(args, fresh=False, cleanup_report=cleanup_report),
            classification="TYPECHECK_FAILURE",
            reason=typecheck_summary,
        )
        report["typecheck_failed"] = True
        write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2C failed: TYPECHECK_FAILURE")
        return 1

    capture = _capture_summary(args, fresh=False, cleanup_report=cleanup_report)
    runtime_quality: dict[str, Any]
    runtime_errors: list[str]

    if args.skip_capture and not args.allow_fixture_mode:
        report = empty_phase42c_report(
            symbol=args.symbol,
            capture=capture,
            classification="FRESH_CAPTURE_NOT_PERFORMED",
            reason="--skip-capture is only allowed with --allow-fixture-mode in tests",
        )
        write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2C failed: FRESH_CAPTURE_NOT_PERFORMED")
        return 1

    if not args.skip_capture and float(args.duration_sec) < 1800.0:
        report = empty_phase42c_report(
            symbol=args.symbol,
            capture=capture,
            classification="MULTI_FEED_CAPTURE_INCOMPLETE",
            reason="final fresh capture duration_sec < 1800",
        )
        report["multi_feed_capture_failed"] = True
        write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
        print("Phase 4.2C failed: MULTI_FEED_CAPTURE_INCOMPLETE")
        return 1

    if args.skip_capture:
        runtime_quality = _passing_depth_runtime_quality()
        runtime_errors = []
        fixture_diagnostics = _build_fixture_capture_diagnostics(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            root=root,
        )
        _write_json(root / PHASE42C_CAPTURE_DIAGNOSTICS, fixture_diagnostics)
        capture["capture_diagnostics"] = fixture_diagnostics
    else:
        capture_code, capture_diagnostics = asyncio.run(
            _run_multi_feed_capture(
                symbol=args.symbol,
                duration_sec=args.duration_sec,
                depth_n=args.depth_n,
                root=SOURCE_ROOT,
            )
        )
        capture["fresh_capture_performed"] = capture_code == 0
        capture["fixture_mode"] = False
        capture["skip_capture"] = False
        capture["depth_clean_sample_count"] = _count_jsonl(SOURCE_ROOT / "data/dataset/orderbook_clean_samples.jsonl")
        capture["capture_diagnostics"] = capture_diagnostics
        capture["reference_event_counts"] = {
            "bookTicker_mid": _count_jsonl(SOURCE_ROOT / BOOKTICKER_REFERENCE_QUOTES),
            "trade_price": _count_jsonl(SOURCE_ROOT / TRADE_REFERENCE_EVENTS),
            "aggTrade_price": _count_jsonl(SOURCE_ROOT / AGGTRADE_REFERENCE_EVENTS),
        }
        diagnostic_errors = validate_capture_diagnostics(capture_diagnostics, symbol=args.symbol)
        if capture_code != 0:
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=capture,
                classification="MULTI_FEED_CAPTURE_INCOMPLETE",
                reason=f"multi-feed capture exited {capture_code}; diagnostics_errors={diagnostic_errors}",
            )
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2C failed: MULTI_FEED_CAPTURE_INCOMPLETE")
            return capture_code or 1
        if diagnostic_errors:
            report = empty_phase42c_report(
                symbol=args.symbol,
                capture=capture,
                classification="MULTI_FEED_CAPTURE_INCOMPLETE",
                reason=f"multi-feed capture diagnostics invalid: {diagnostic_errors}",
            )
            report["multi_feed_capture_failed"] = True
            write_phase42c_artifacts(report, root=root, pytest_output=pytest_output)
            print("Phase 4.2C failed: MULTI_FEED_CAPTURE_INCOMPLETE")
            return 1
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
        capture_diagnostics=_dict(capture.get("capture_diagnostics")),
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


async def _run_multi_feed_capture(
    *,
    symbol: str,
    duration_sec: float,
    depth_n: int,
    root: Path,
    latency_profile_samples_path: Path | None = None,
) -> tuple[int, dict[str, Any]]:
    for path in (BOOKTICKER_REFERENCE_QUOTES, TRADE_REFERENCE_EVENTS, AGGTRADE_REFERENCE_EVENTS):
        target = SOURCE_ROOT / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(SOURCE_ROOT / "bot")
    runtime_stdout_path = SOURCE_ROOT / "data/debug/phase_4_2c_runtime_stdout.log"
    runtime_stderr_path = SOURCE_ROOT / "data/debug/phase_4_2c_runtime_stderr.log"
    runtime_stdout_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_stdout = runtime_stdout_path.open("w", encoding="utf-8")
    runtime_stderr = runtime_stderr_path.open("w", encoding="utf-8")
    depth_command = [
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
    ]
    if latency_profile_samples_path is not None:
        depth_command.extend(["--latency-profile-samples", str(latency_profile_samples_path)])
    depth_process = await asyncio.create_subprocess_exec(
        *depth_command,
        cwd=SOURCE_ROOT / "bot",
        env=env,
        stdout=runtime_stdout,
        stderr=runtime_stderr,
    )
    reference_task = asyncio.create_task(_capture_references(symbol=symbol, duration_sec=duration_sec))
    reference_diagnostics: dict[str, Any] = {
        "websocket_url": "",
        "requested_streams": [f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@trade", f"{symbol.lower()}@aggTrade"],
        "connected": False,
        "connect_count": 0,
        "disconnect_count": 0,
        "reconnect_count": 0,
        "message_count_by_stream": {},
        "parsed_count_by_source": {},
        "parse_error_count_by_source": {},
        "unknown_stream_count": 0,
        "first_message_wall_ts_by_stream": {},
        "last_message_wall_ts_by_stream": {},
    }
    try:
        depth_code, reference_diagnostics = await asyncio.gather(depth_process.wait(), reference_task)
    finally:
        runtime_stdout.close()
        runtime_stderr.close()
    diagnostics = _build_capture_diagnostics(
        symbol=symbol,
        duration_sec=duration_sec,
        reference_diagnostics=reference_diagnostics,
        depth_code=depth_code,
        root=root,
    )
    _write_json(root / PHASE42C_CAPTURE_DIAGNOSTICS, diagnostics)
    code = depth_code if depth_code != 0 else 0
    return code, diagnostics


async def _capture_references(*, symbol: str, duration_sec: float) -> dict[str, Any]:
    streams = [f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@trade", f"{symbol.lower()}@aggTrade"]
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
    deadline = time.monotonic() + duration_sec
    message_count_by_stream = {stream: 0 for stream in streams}
    parsed_count_by_source = {"bookTicker_mid": 0, "trade_price": 0, "aggTrade_price": 0}
    parse_error_count_by_source = {"bookTicker_mid": 0, "trade_price": 0, "aggTrade_price": 0}
    first_message_wall_ts_by_stream: dict[str, str] = {}
    last_message_wall_ts_by_stream: dict[str, str] = {}
    unknown_stream_count = 0
    reconnect_count = 0
    connect_count = 0
    disconnect_count = 0
    connected = False
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
                        connected = True
                        connect_count += 1
                        while time.monotonic() < deadline:
                            remaining = max(0.1, min(5.0, deadline - time.monotonic()))
                            message = await asyncio.wait_for(websocket.receive(), timeout=remaining)
                            if message.type == aiohttp.WSMsgType.TEXT:
                                received_ns = time.monotonic_ns()
                                wall_ts = datetime.now(timezone.utc).isoformat()
                                payload = json.loads(message.data)
                                stream_name = str(payload.get("stream", ""))
                                if stream_name in message_count_by_stream:
                                    message_count_by_stream[stream_name] += 1
                                    first_message_wall_ts_by_stream.setdefault(stream_name, wall_ts)
                                    last_message_wall_ts_by_stream[stream_name] = wall_ts
                                else:
                                    unknown_stream_count += 1
                                source, row = _parse_reference_message(
                                    payload,
                                    local_recv_monotonic_ns=received_ns,
                                    local_recv_wall_ts=wall_ts,
                                )
                                if source is None or row is None:
                                    unknown_stream_count += 1
                                    continue
                                handles[source].write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
                                parsed_count_by_source[source] += 1
                                quality = row.get("quality")
                                if isinstance(quality, dict) and quality.get("valid") is not True:
                                    parse_error_count_by_source[source] += 1
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                disconnect_count += 1
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
    return {
        "websocket_url": url,
        "requested_streams": streams,
        "connected": connected,
        "connect_count": connect_count,
        "disconnect_count": disconnect_count,
        "reconnect_count": reconnect_count,
        "message_count_by_stream": message_count_by_stream,
        "parsed_count_by_source": parsed_count_by_source,
        "parse_error_count_by_source": parse_error_count_by_source,
        "unknown_stream_count": unknown_stream_count,
        "first_message_wall_ts_by_stream": first_message_wall_ts_by_stream,
        "last_message_wall_ts_by_stream": last_message_wall_ts_by_stream,
    }


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


def _build_capture_diagnostics(
    *,
    symbol: str,
    duration_sec: float,
    reference_diagnostics: dict[str, Any],
    depth_code: int,
    root: Path,
) -> dict[str, Any]:
    requested = required_streams(symbol)
    depth_stream = requested[0]
    phase41_report = _read_json(root / "data/reports/phase_4_1_orderbook_quality_report.json")
    depth_message_count = int(_num(phase41_report.get("messages_received")))
    clean_count = _count_jsonl(root / "data/dataset/orderbook_clean_samples.jsonl")
    message_count_by_stream = {stream: 0 for stream in requested}
    message_count_by_stream[depth_stream] = depth_message_count
    message_count_by_stream.update(_dict(reference_diagnostics.get("message_count_by_stream")))
    parsed_count_by_source = {
        "depth_mid": clean_count,
        "bookTicker_mid": 0,
        "trade_price": 0,
        "aggTrade_price": 0,
    }
    parsed_count_by_source.update(_dict(reference_diagnostics.get("parsed_count_by_source")))
    parse_error_count_by_source = {
        "depth_mid": 0,
        "bookTicker_mid": 0,
        "trade_price": 0,
        "aggTrade_price": 0,
    }
    parse_error_count_by_source.update(_dict(reference_diagnostics.get("parse_error_count_by_source")))
    output_paths = {
        "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
        "bookticker": str(BOOKTICKER_REFERENCE_QUOTES).replace("\\", "/"),
        "trade": str(TRADE_REFERENCE_EVENTS).replace("\\", "/"),
        "aggtrade": str(AGGTRADE_REFERENCE_EVENTS).replace("\\", "/"),
    }
    output_sizes = {
        key: _file_size(root / path)
        for key, path in output_paths.items()
    }
    return {
        "fresh_capture_performed": depth_code == 0,
        "fixture_mode": False,
        "skip_capture": False,
        "duration_sec": float(duration_sec),
        "symbol": symbol.upper(),
        "websocket_url": str(reference_diagnostics.get("websocket_url", "")),
        "requested_streams": requested,
        "connected": bool(reference_diagnostics.get("connected", False)),
        "connect_count": int(_num(reference_diagnostics.get("connect_count"))),
        "disconnect_count": int(_num(reference_diagnostics.get("disconnect_count"))),
        "reconnect_count": int(_num(reference_diagnostics.get("reconnect_count"))),
        "message_count_by_stream": message_count_by_stream,
        "parsed_count_by_source": parsed_count_by_source,
        "parse_error_count_by_source": parse_error_count_by_source,
        "unknown_stream_count": int(_num(reference_diagnostics.get("unknown_stream_count"))),
        "first_message_wall_ts_by_stream": _dict(reference_diagnostics.get("first_message_wall_ts_by_stream")),
        "last_message_wall_ts_by_stream": _dict(reference_diagnostics.get("last_message_wall_ts_by_stream")),
        "output_file_paths": output_paths,
        "output_file_sizes_bytes": output_sizes,
        "depth_capture_exit_code": depth_code,
    }


def _build_fixture_capture_diagnostics(*, symbol: str, duration_sec: float, root: Path) -> dict[str, Any]:
    requested = required_streams(symbol)
    output_paths = {
        "clean_samples": "data/dataset/orderbook_clean_samples.jsonl",
        "bookticker": str(BOOKTICKER_REFERENCE_QUOTES).replace("\\", "/"),
        "trade": str(TRADE_REFERENCE_EVENTS).replace("\\", "/"),
        "aggtrade": str(AGGTRADE_REFERENCE_EVENTS).replace("\\", "/"),
    }
    return {
        "fresh_capture_performed": False,
        "fixture_mode": True,
        "skip_capture": True,
        "duration_sec": float(duration_sec),
        "symbol": symbol.upper(),
        "websocket_url": "fixture-mode",
        "requested_streams": requested,
        "connected": False,
        "connect_count": 0,
        "disconnect_count": 0,
        "reconnect_count": 0,
        "message_count_by_stream": {
            requested[0]: _count_jsonl(root / output_paths["clean_samples"]),
            requested[1]: _count_jsonl(root / output_paths["bookticker"]),
            requested[2]: _count_jsonl(root / output_paths["trade"]),
            requested[3]: _count_jsonl(root / output_paths["aggtrade"]),
        },
        "parsed_count_by_source": {
            "depth_mid": _count_jsonl(root / output_paths["clean_samples"]),
            "bookTicker_mid": _count_jsonl(root / output_paths["bookticker"]),
            "trade_price": _count_jsonl(root / output_paths["trade"]),
            "aggTrade_price": _count_jsonl(root / output_paths["aggtrade"]),
        },
        "parse_error_count_by_source": {
            "depth_mid": 0,
            "bookTicker_mid": 0,
            "trade_price": 0,
            "aggTrade_price": 0,
        },
        "unknown_stream_count": 0,
        "first_message_wall_ts_by_stream": {},
        "last_message_wall_ts_by_stream": {},
        "output_file_paths": output_paths,
        "output_file_sizes_bytes": {
            key: _file_size(root / path)
            for key, path in output_paths.items()
        },
    }


def _capture_summary(
    args: argparse.Namespace,
    *,
    fresh: bool,
    cleanup_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cleanup = cleanup_report or {}
    return {
        "fresh_capture_performed": fresh,
        "fixture_mode": bool(args.skip_capture),
        "skip_capture": bool(args.skip_capture),
        "cleanup_performed": bool(cleanup.get("cleanup_performed", False)),
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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() and path.is_file() else 0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _num(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result


if __name__ == "__main__":
    raise SystemExit(main())
