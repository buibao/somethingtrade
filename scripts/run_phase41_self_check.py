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
TRANSIENT_FAILURES = {"NETWORK_UNAVAILABLE", "LOG_ENCODING_FAILURE"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--depth-n", type=int, default=20)
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--gate", choices=("2m", "10m", "30m"), default=None)
    parser.add_argument("--max-gate", choices=("2m", "10m", "30m"), default="30m")
    parser.add_argument("--max-attempts", type=int, default=1)
    args = parser.parse_args(argv)

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(ROOT / "bot")

    if not args.skip_pytest:
        code = _run_and_log(
            [sys.executable, "-X", "utf8", "-m", "pytest", "-q"],
            ROOT / "data/debug/phase_4_1_1_pytest_output.txt",
            env=env,
        )
        if code != 0:
            _write_investigation(
                "pytest",
                attempt=1,
                classifications=["TEST_FAILURE"],
                hypothesis="pytest failed",
                detail="No runtime gate executed.",
            )
            return code

    if args.gate is not None:
        selected_gates = tuple(item for item in GATES if item[0] == args.gate)
    else:
        selected_gates = tuple(item for item in GATES[: _gate_index(args.max_gate) + 1])

    for gate, seconds in GATES:
        if (gate, seconds) not in selected_gates:
            continue
        code = _run_gate(
            gate,
            seconds,
            args.symbol,
            args.depth_n,
            env,
            max_attempts=max(1, args.max_attempts),
        )
        if code != 0:
            return code

    if tuple(selected_gates) == GATES:
        _create_bundle()
    return 0


def _run_gate(
    gate: str,
    seconds: int,
    symbol: str,
    depth_n: int,
    env: dict[str, str],
    max_attempts: int,
) -> int:
    last_code = 1
    for attempt in range(1, max_attempts + 1):
        last_code, classifications, reasons = _run_gate_once(
            gate,
            seconds,
            symbol,
            depth_n,
            env,
        )
        if last_code == 0:
            return 0
        _write_investigation(
            gate,
            attempt=attempt,
            classifications=classifications,
            hypothesis="\n".join(reasons),
            detail=_tail_trace(),
        )
        if not _should_retry(classifications, attempt=attempt, max_attempts=max_attempts):
            return last_code
    return last_code


def _run_gate_once(
    gate: str,
    seconds: int,
    symbol: str,
    depth_n: int,
    env: dict[str, str],
) -> tuple[int, list[str], list[str]]:
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
        return code, ["NETWORK_UNAVAILABLE"], [f"capture command exited {code}"]

    log_ok, log_reason = check_console_log_encoding(console)
    if not log_ok:
        return 1, ["LOG_ENCODING_FAILURE"], [log_reason]

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
            reasons = result.get("schema_errors") or result.get("hard_fail_reasons", [])
        except Exception:
            reasons = ["REPORT_SCHEMA_FAILURE"]
        classifications = (
            ["REPORT_SCHEMA_FAILURE"]
            if check.returncode == 2
            else _classify_reasons([str(reason) for reason in reasons])
        )
        return check.returncode, classifications, [str(reason) for reason in reasons]

    sample_errors = _check_clean_sample_metadata(ROOT / "data/dataset/orderbook_clean_samples.jsonl")
    if sample_errors:
        return 1, ["CLEAN_SAMPLE_SCHEMA_FAILURE"], sample_errors
    return 0, [], []


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


def check_console_log_encoding(path: Path) -> tuple[bool, str]:
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return False, f"console log missing: {path}"
    if b"\x00" in payload:
        return False, f"LOG_ENCODING_FAILURE: NUL byte found in {path}"
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        return False, f"LOG_ENCODING_FAILURE: invalid UTF-8 in {path}: {exc}"
    return True, ""


def _check_clean_sample_metadata(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.exists():
        return [f"clean sample file missing: {path}"]
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"clean sample row {index} invalid JSON: {exc}")
            continue
        generation_id = row.get("generation_id")
        wall_ts = row.get("local_recv_wall_ts") or row.get("local_recv_wall_iso") or row.get("local_recv_wall_ts_ns")
        if generation_id is None or isinstance(generation_id, bool) or not isinstance(generation_id, int):
            errors.append(f"clean sample row {index} invalid generation_id")
        if wall_ts is None:
            errors.append(f"clean sample row {index} missing wall debug timestamp")
    return errors


def _write_investigation(
    gate: str,
    *,
    attempt: int,
    classifications: list[str],
    hypothesis: str,
    detail: str,
) -> None:
    path = ROOT / f"data/debug/phase41_failure_investigation_{gate}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                f"# Phase 4.1.1 Failure Investigation: {gate}",
                "",
                f"- Gate: `{gate}`",
                f"- Attempt: `{attempt}`",
                f"- Classification: `{classifications}`",
                f"- Report path: `{REPORT}`",
                f"- Console log path: `data/debug/phase4_1_runtime_{gate}_console.log`",
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
        if "sequence_gap" in joined:
            classes.append("SEQUENCE_GAP")
        classes.append("SNAPSHOT_BRIDGE_FAILURE")
    if "queue_lag" in joined or "processing_lag" in joined:
        classes.append("QUEUE_LAG_FAILURE")
    if "feed_receive_stale" in joined:
        classes.append("FEED_RECEIVE_STALE")
    if "processor_apply_stale" in joined:
        classes.append("PROCESSOR_APPLY_STALE")
    if "sample_before_ready" in joined:
        classes.append("SAMPLE_BEFORE_READY")
    if "invalid_delta" in joined:
        classes.append("INVALID_DELTA")
    if "crossed_book" in joined or "book_empty" in joined or "one_side_missing" in joined:
        classes.append("CROSSED_OR_EMPTY_BOOK")
    if "clean_sample_schema" in joined:
        classes.append("CLEAN_SAMPLE_SCHEMA_FAILURE")
    if "schema" in joined:
        classes.append("REPORT_SCHEMA_FAILURE")
    if not classes:
        classes.append("UNKNOWN_RUNTIME_FAILURE")
    return classes


def _should_retry(classifications: list[str], *, attempt: int, max_attempts: int) -> bool:
    return attempt < max_attempts and any(
        classification in TRANSIENT_FAILURES for classification in classifications
    )


def _gate_index(gate: str) -> int:
    for index, (name, _) in enumerate(GATES):
        if name == gate:
            return index
    raise ValueError(f"unsupported gate: {gate}")


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
        ROOT / "data/debug/phase_4_1_1_pytest_output.txt",
        ROOT / "data/debug/clean_sample_schema_violation_cases.jsonl",
    ]
    investigations = sorted((ROOT / "data/debug").glob("phase41_failure_investigation_*.md"))
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        _write_directory_to_archive(archive, ROOT / "bot/app", "app")
        _write_directory_to_archive(archive, ROOT / "tests", "tests")
        _write_directory_to_archive(archive, ROOT / "scripts", "scripts")
        for path in files + investigations:
            if path.exists():
                archive.write(path, path.relative_to(ROOT))
    shutil.copy2(target, ROOT / "data/debug/phase_4_1_1_runtime_pass_bundle.zip")


def _write_directory_to_archive(
    archive: zipfile.ZipFile,
    directory: Path,
    archive_prefix: str,
) -> None:
    if not directory.exists():
        return
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        archive.write(path, Path(archive_prefix) / path.relative_to(directory))


if __name__ == "__main__":
    raise SystemExit(main())
