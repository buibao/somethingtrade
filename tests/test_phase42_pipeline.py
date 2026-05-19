from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

from app.research.orderbook_labeled_dataset import REQUIRED_BUNDLE_FILES, run_phase42_pipeline


ROOT = Path(__file__).resolve().parents[1]


def _level(price: float, size: float) -> list[str]:
    return [f"{price:.8f}", f"{size:.8f}"]


def _sample(ts_ms: int, *, last_update_id: int = 100) -> dict[str, object]:
    best_bid = 100.0 + (ts_ms / 10_000.0)
    best_ask = best_bid + 1.0
    return {
        "schema_version": "phase_4_1_clean_orderbook_v1",
        "symbol": "BTCUSDT",
        "source": "binance_ws",
        "generation_id": 42,
        "state_version": last_update_id,
        "snapshot_version": last_update_id,
        "last_update_id": last_update_id,
        "local_recv_monotonic_ns": ts_ms * 1_000_000,
        "local_recv_wall_ts": "2026-05-19T17:58:54.000000+00:00",
        "exchange_event_ts": 1_779_213_534_814_000_000 + ts_ms,
        "best_bid": f"{best_bid:.8f}",
        "best_ask": f"{best_ask:.8f}",
        "bids": [_level(best_bid - index, 10.0) for index in range(20)],
        "asks": [_level(best_ask + index, 5.0) for index in range(20)],
        "quality": {"is_valid": True, "errors": [], "warnings": []},
        "lifecycle": {
            "snapshot_ready": True,
            "ready_to_emit": True,
            "sequence_continuous": True,
        },
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase42_labeler_cli_generates_output_files(tmp_path: Path) -> None:
    input_path = tmp_path / "clean.jsonl"
    output_path = tmp_path / "labeled.jsonl"
    _write_jsonl(input_path, [_sample(index * 100, last_update_id=100 + index) for index in range(80)])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/generate_orderbook_labeled_dataset.py"),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--report-json",
            str(tmp_path / "report.json"),
            "--report-md",
            str(tmp_path / "report.md"),
            "--debug-dir",
            str(tmp_path / "debug"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert output_path.exists()
    assert (tmp_path / "report.json").exists()
    assert (tmp_path / "report.md").exists()


def test_phase42_labeler_cli_fails_missing_input(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/generate_orderbook_labeled_dataset.py"),
            "--input",
            str(tmp_path / "missing.jsonl"),
            "--output",
            str(tmp_path / "labeled.jsonl"),
            "--report-json",
            str(tmp_path / "report.json"),
            "--report-md",
            str(tmp_path / "report.md"),
            "--debug-dir",
            str(tmp_path / "debug"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode != 0
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["status"] == "fail"


def test_phase42_pipeline_logs_invalid_label_cases(tmp_path: Path) -> None:
    input_path = tmp_path / "clean.jsonl"
    output_path = tmp_path / "labeled.jsonl"
    _write_jsonl(input_path, [_sample(0), _sample(250)])

    result = run_phase42_pipeline(
        input_path=input_path,
        output_path=output_path,
        report_json_path=tmp_path / "report.json",
        report_md_path=tmp_path / "report.md",
        debug_dir=tmp_path / "debug",
    )

    assert result.report["status"] == "fail"
    invalid_lines = (tmp_path / "debug/phase_4_2_label_invalid_cases.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert invalid_lines
    invalid_case = json.loads(invalid_lines[0])
    assert {
        "symbol",
        "generation_id",
        "last_update_id",
        "local_recv_monotonic_ns",
        "horizon",
        "invalid_reason",
    } <= set(invalid_case)


def test_phase42_self_check_passes_valid_fixture_and_creates_bundle(tmp_path: Path) -> None:
    input_path = tmp_path / "data/dataset/orderbook_clean_samples.jsonl"
    output_path = tmp_path / "data/dataset/orderbook_labeled_samples.jsonl"
    _write_jsonl(input_path, [_sample(index * 100, last_update_id=100 + index) for index in range(80)])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42_self_check.py"),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--root",
            str(tmp_path),
            "--skip-pytest",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads((tmp_path / "data/reports/phase42_self_check.json").read_text())["passed"] is True
    bundle = tmp_path / "phase_4_2_dataset_quality_bundle.zip"
    assert bundle.exists()
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
    for required in REQUIRED_BUNDLE_FILES:
        assert required in names


def test_phase42_self_check_fails_invalid_fixture_without_bundle(tmp_path: Path) -> None:
    input_path = tmp_path / "data/dataset/orderbook_clean_samples.jsonl"
    output_path = tmp_path / "data/dataset/orderbook_labeled_samples.jsonl"
    bad = _sample(0)
    bad["generation_id"] = None
    _write_jsonl(input_path, [bad])

    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(ROOT / "scripts/run_phase42_self_check.py"),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--root",
            str(tmp_path),
            "--skip-pytest",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=120,
    )

    assert result.returncode != 0
    assert not (tmp_path / "phase_4_2_dataset_quality_bundle.zip").exists()
    assert (tmp_path / "data/debug/phase42_failure_investigation.md").exists()

