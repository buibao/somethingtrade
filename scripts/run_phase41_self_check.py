from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "data/reports/phase_4_1_orderbook_quality_report.json"
GATES = (("2m", 120), ("10m", 600), ("30m", 1800))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--max-gate", choices=("2m", "10m", "30m"), default="30m")
    args = parser.parse_args(argv)

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(ROOT / "bot")

    if not args.skip_pytest:
        code = _run_and_log(
            [sys.executable, "-X", "utf8", "-m", "pytest", "-q"],
            ROOT / "data/debug/phase4_1_1_pytest_output.txt",
            env=env,
        )
        if code != 0:
            _write_investigation("pytest", ["TEST_FAILURE"], "pytest failed", "No runtime gate executed.")
            return code

    for gate, seconds in GATES:
        code = _run_gate(gate, seconds, args.symbol, args.depth_n, env)
        if code != 0:
            return code
        if gate == args.max_gate:
            break

    if args.max_gate == "30m":
        _create_bundle()
    return 0


def _run_gate(
    gate: str,
    seconds: int,
    symbol: str,
    depth_n: int,
    env: dict[str, str],
) -> int:
    console = ROOT / f"data/debug/phase4_1_runtime_{gate}_console.log"
    code = _run_and_log(
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
            str(seconds),
            "--depth-n",
            str(depth_n),
        ],
        console,
        cwd=ROOT / "bot",
        env=env,
    )
    if code != 0:
        _write_investigation(gate, ["NETWORK_UNAVAILABLE"], f"capture command exited {code}", console.read_text(encoding="utf-8", errors="replace")[-4000:])
        return code

    check = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/check_phase41_report.py"),
            "--gate",
            gate,
            "--report",
            str(REPORT),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    sys.stdout.write(check.stdout)
    sys.stderr.write(check.stderr)
    if check.returncode != 0:
        try:
            result = json.loads(check.stdout)
            reasons = result.get("hard_fail_reasons", [])
        except Exception:
            reasons = ["REPORT_SCHEMA_FAILURE"]
        _write_investigation(
            gate,
            _classify_reasons(reasons),
            "\n".join(str(reason) for reason in reasons),
            _tail_trace(),
        )
        return check.returncode
    return 0


def _run_and_log(
    command: list[str],
    log_path: Path,
    *,
    cwd: Path = ROOT,
    env: dict[str, str],
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    with log_path.open("w", encoding="utf-8") as handle:
        for line in process.stdout:
            sys.stdout.write(line)
            handle.write(line)
    return process.wait()


def _write_investigation(gate: str, classifications: list[str], hypothesis: str, detail: str) -> None:
    path = ROOT / f"data/debug/phase41_failure_investigation_{gate}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                f"# Phase 4.1.1 Failure Investigation: {gate}",
                "",
                f"- Gate: `{gate}`",
                f"- Classification: `{classifications}`",
                "",
                "## Failed Criteria",
                "",
                hypothesis or "-",
                "",
                "## Relevant Trace / Log Tail",
                "",
                "```text",
                detail[-8000:],
                "```",
                "",
                "## Fix Applied",
                "",
                "No automatic source edit was applied by the self-check script.",
                "",
                "## Rerun Result",
                "",
                "Not rerun by this script invocation.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _classify_reasons(reasons: list[str]) -> list[str]:
    joined = " ".join(reasons)
    classes: list[str] = []
    if "sequence_gap" in joined or "bridge" in joined:
        classes.append("SNAPSHOT_BRIDGE_FAILURE")
    if "queue_lag" in joined or "processing_lag" in joined:
        classes.append("QUEUE_LAG_FAILURE")
    if "feed_receive_stale" in joined:
        classes.append("FEED_RECEIVE_STALE")
    if "sample_before_ready" in joined:
        classes.append("SAMPLE_BEFORE_READY")
    if "invalid_delta" in joined:
        classes.append("INVALID_DELTA")
    if not classes:
        classes.append("UNKNOWN_RUNTIME_FAILURE")
    return classes


def _tail_trace() -> str:
    parts: list[str] = []
    for path in (
        ROOT / "data/debug/sequence_recovery_trace.jsonl",
        ROOT / "data/debug/sequence_gap_cases.jsonl",
        ROOT / "data/debug/stale_period_cases.jsonl",
    ):
        if path.exists():
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            parts.append(f"## {path}\n" + "\n".join(lines[-50:]))
    return "\n\n".join(parts)


def _create_bundle() -> None:
    target = ROOT / "phase_4_1_1_runtime_pass_bundle.zip"
    if target.exists():
        target.unlink()
    files = [
        ROOT / "bot/app/marketdata/orderbook_phase41.py",
        ROOT / "bot/app/marketdata/queue_monitor.py",
        ROOT / "bot/app/marketdata/orderbook_quality.py",
        ROOT / "bot/app/marketdata/ws_lifecycle.py",
        ROOT / "scripts/check_phase41_report.py",
        ROOT / "scripts/run_phase41_self_check.py",
        ROOT / "scripts/run_phase41_self_check.ps1",
        ROOT / "data/reports/phase_4_1_orderbook_quality_report.json",
        ROOT / "data/reports/phase_4_1_orderbook_quality_report.md",
        ROOT / "data/reports/phase41_gate_check_2m.json",
        ROOT / "data/reports/phase41_gate_check_10m.json",
        ROOT / "data/reports/phase41_gate_check_30m.json",
        ROOT / "data/debug/phase4_1_runtime_2m_console.log",
        ROOT / "data/debug/phase4_1_runtime_10m_console.log",
        ROOT / "data/debug/phase4_1_runtime_30m_console.log",
        ROOT / "data/debug/sequence_recovery_trace.jsonl",
        ROOT / "data/dataset/orderbook_clean_samples.jsonl",
        ROOT / "data/debug/phase4_1_1_pytest_output.txt",
    ]
    investigations = sorted((ROOT / "data/debug").glob("phase41_failure_investigation_*.md"))
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files + investigations:
            if path.exists():
                archive.write(path, path.relative_to(ROOT))
    shutil.copy2(target, ROOT / "data/debug/phase_4_1_1_runtime_pass_bundle.zip")


if __name__ == "__main__":
    raise SystemExit(main())
