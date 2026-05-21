from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import subprocess

import pytest

from app.research.hotpath_environment_latency import (
    PHASE42H_ENVIRONMENT_METADATA,
    PHASE42H_VPS_PREFLIGHT_REPORT,
    build_environment_metadata,
    evaluate_phase42h_report,
    run_phase42h_vps_preflight,
    write_phase42h_artifacts,
)
from tests.test_phase42h_hotpath_environment_latency import _report


ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = ROOT / "docs/run_phase42h_on_digitalocean_vps.md"
SETUP_SCRIPT = ROOT / "scripts/setup_phase42h_vps_ubuntu.sh"
RUN_SCRIPT = ROOT / "scripts/run_phase42h_vps_benchmark.sh"
COLLECT_SCRIPT = ROOT / "scripts/collect_phase42h_vps_bundle.sh"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_required_gitignore(root: Path) -> None:
    root.joinpath(".gitignore").write_text(
        "\n".join(
            [
                "*.jsonl",
                "data/dataset/",
                "data/debug/",
                "data/cache/",
                "data/logs/",
                "data/reports/",
                "logs/",
                "reports/",
                "debug/",
                "cache/",
                "*.zip",
                "*.log",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_vps_docs_exist() -> None:
    assert DOC_PATH.exists()
    text = _text(DOC_PATH)
    assert "Singapore" in text
    assert "Ubuntu 24.04" in text
    assert "Ubuntu 22.04" in text
    assert "2 vCPU / 4 GB RAM" in text
    assert "scp" in text
    assert "destroy the droplet" in text


def test_setup_vps_script_exists() -> None:
    assert SETUP_SCRIPT.exists()


def test_run_vps_benchmark_script_exists() -> None:
    assert RUN_SCRIPT.exists()


def test_collect_vps_bundle_script_exists() -> None:
    assert COLLECT_SCRIPT.exists()


def test_setup_script_uses_strict_bash() -> None:
    assert "set -euo pipefail" in _text(SETUP_SCRIPT)


def test_run_script_uses_strict_bash() -> None:
    assert "set -euo pipefail" in _text(RUN_SCRIPT)


def test_collect_script_uses_strict_bash() -> None:
    assert "set -euo pipefail" in _text(COLLECT_SCRIPT)


def test_scripts_do_not_contain_api_tokens() -> None:
    combined = "\n".join(_text(path) for path in (SETUP_SCRIPT, RUN_SCRIPT, COLLECT_SCRIPT)).lower()
    forbidden = ("digitalocean_token", "do_token", "api_token", "api token", "secret_key", "private_key")
    assert not any(token in combined for token in forbidden)


def test_setup_script_does_not_auto_run_30m_benchmark() -> None:
    text = _text(SETUP_SCRIPT)
    assert "run_phase42h_vps_benchmark.sh" not in text
    assert "--duration-sec 1800" not in text


def test_vps_wrapper_requires_mode() -> None:
    text = _text(RUN_SCRIPT)
    assert '--mode is required and must be smoke or final' in text
    assert 'MODE=""' in text


def test_vps_wrapper_final_requires_duration_1800() -> None:
    text = _text(RUN_SCRIPT)
    assert 'MODE" == "final"' in text
    assert "1800" in text
    assert "final mode requires --duration-sec >= 1800" in text


def test_vps_wrapper_smoke_allows_120() -> None:
    text = _text(RUN_SCRIPT)
    assert 'MODE" == "smoke"' in text
    assert "DURATION_SEC=120" in text
    assert "--run-mode" in text
    assert "vps_${MODE}" in text


def test_vps_smoke_report_does_not_apply_final_1800_second_gate() -> None:
    report = _report()
    report["duration_sec"] = 120.0
    report["capture"]["duration_sec"] = 120.0
    report["environment"]["run_mode"] = "vps_smoke"
    report["fresh_capture_required"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "FRESH_CAPTURE_DURATION_FAILURE" not in evaluated["failure_classifications"]


def test_vps_final_report_keeps_1800_second_gate() -> None:
    report = _report()
    report["duration_sec"] = 120.0
    report["capture"]["duration_sec"] = 120.0
    report["environment"]["run_mode"] = "vps_final"
    report["fresh_capture_required"] = True
    evaluated = evaluate_phase42h_report(report)
    assert "FRESH_CAPTURE_DURATION_FAILURE" in evaluated["failure_classifications"]


def test_vps_wrapper_passes_environment_args() -> None:
    text = _text(RUN_SCRIPT)
    for arg in ("--environment-name", "--environment-region", "--machine-profile", "--network-notes"):
        assert arg in text


def test_vps_wrapper_uses_phase42h_script() -> None:
    assert "scripts/run_phase42h_hotpath_environment_latency.py" in _text(RUN_SCRIPT)


def test_collect_script_declares_pass_fail_and_sha256_paths() -> None:
    text = _text(COLLECT_SCRIPT)
    assert "phase_4_2h_hotpath_environment_latency_bundle.zip" in text
    assert "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip" in text
    assert "phase_4_2h_bundle_sha256.txt" in text
    assert "sha256sum" in text


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="bash execution is not portable in this test host")
def test_vps_wrapper_dry_run_argument_behavior() -> None:
    env = {**os.environ, "PHASE42H_DRY_RUN": "1"}
    common = [
        "bash",
        str(RUN_SCRIPT),
        "--environment-name",
        "vps_singapore_do",
        "--environment-region",
        "SG",
        "--machine-profile",
        "2vCPU-4GB-Ubuntu-DO",
        "--network-notes",
        "DigitalOcean Singapore smoke",
    ]
    missing_mode = subprocess.run(common, cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    assert missing_mode.returncode != 0

    final_short = subprocess.run([*common, "--mode", "final", "--duration-sec", "120"], cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    assert final_short.returncode != 0
    assert "final mode requires" in final_short.stderr

    smoke = subprocess.run([*common, "--mode", "smoke", "--duration-sec", "120"], cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    assert smoke.returncode == 0
    assert "--duration-sec 120" in smoke.stdout
    assert "--environment-name vps_singapore_do" in smoke.stdout


def test_environment_metadata_schema() -> None:
    metadata = build_environment_metadata(
        environment_name="vps_singapore_do",
        environment_region="SG",
        machine_profile="2vCPU-4GB-Ubuntu-DO",
        network_notes="DigitalOcean Singapore final 30m",
        run_mode="vps_final",
    )
    for field in (
        "provider",
        "name",
        "region",
        "machine_profile",
        "network_notes",
        "os",
        "kernel",
        "python_version",
        "cpu_model",
        "cpu_count",
        "memory_total_mb",
        "timezone",
        "hostname_hash",
        "run_mode",
    ):
        assert field in metadata
    assert metadata["provider"] == "DigitalOcean"
    assert metadata["run_mode"] == "vps_final"


def test_environment_metadata_written(tmp_path: Path) -> None:
    report = _report()
    write_phase42h_artifacts(report, root=tmp_path, pytest_output="pytest ok", bundle_created=False)
    metadata_path = tmp_path / PHASE42H_ENVIRONMENT_METADATA
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["provider"] == "DigitalOcean"


def test_environment_metadata_in_main_report() -> None:
    report = _report()
    assert report["environment"]["provider"] == "DigitalOcean"
    assert report["environment"]["hostname_hash"]


def test_hostname_not_exposed_raw_if_hash_available() -> None:
    metadata = build_environment_metadata(
        environment_name="vps_singapore_do",
        environment_region="SG",
        machine_profile="2vCPU-4GB-Ubuntu-DO",
        run_mode="vps_final",
    )
    assert "hostname" not in metadata
    assert metadata["hostname_hash"] != socket.gethostname()


def test_preflight_report_schema(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    report = run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    assert report["schema_version"] == "phase_4_2h_vps_preflight_v1"
    assert report["phase"] == "4.2H"
    assert report["passed"] is True
    assert "checks" in report
    assert (tmp_path / PHASE42H_VPS_PREFLIGHT_REPORT).exists()


def test_preflight_fails_when_binance_time_unreachable(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    report = run_phase42h_vps_preflight(
        tmp_path,
        required_imports=("json",),
        binance_time_url="http://127.0.0.1:1/api/v3/time",
        websocket_host="127.0.0.1",
        websocket_port=1,
        check_network=True,
    )
    assert report["passed"] is False
    assert report["checks"]["binance_rest_time"]["passed"] is False


def test_preflight_fails_when_data_dir_not_writable(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    dataset_path = tmp_path / "data/dataset"
    dataset_path.parent.mkdir(parents=True)
    dataset_path.write_text("not a directory", encoding="utf-8")
    report = run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    assert report["passed"] is False
    assert report["checks"]["data_directories_writable"]["passed"] is False


def test_preflight_reports_gitignore_status(tmp_path: Path) -> None:
    _write_required_gitignore(tmp_path)
    report = run_phase42h_vps_preflight(tmp_path, required_imports=("json",), check_network=False)
    gitignore = report["checks"]["gitignore_status"]
    assert gitignore["present"] is True
    assert gitignore["validation"]["passed"] is True


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="bash execution is not portable in this test host")
def test_collect_bundle_detects_pass_bundle(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / COLLECT_SCRIPT.name
    script.write_text(_text(COLLECT_SCRIPT), encoding="utf-8")
    (tmp_path / "phase_4_2h_hotpath_environment_latency_bundle.zip").write_bytes(b"pass")
    result = subprocess.run(["bash", str(script)], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert result.returncode == 0
    assert "phase_4_2h_hotpath_environment_latency_bundle.zip" in result.stdout


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="bash execution is not portable in this test host")
def test_collect_bundle_detects_fail_bundle(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / COLLECT_SCRIPT.name
    script.write_text(_text(COLLECT_SCRIPT), encoding="utf-8")
    (tmp_path / "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip").write_bytes(b"fail")
    result = subprocess.run(["bash", str(script)], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert result.returncode == 0
    assert "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip" in result.stdout


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="bash execution is not portable in this test host")
def test_collect_bundle_fails_when_no_bundle(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / COLLECT_SCRIPT.name
    script.write_text(_text(COLLECT_SCRIPT), encoding="utf-8")
    result = subprocess.run(["bash", str(script)], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert result.returncode != 0
    assert "No Phase 4.2H bundle found" in result.stderr


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="bash execution is not portable in this test host")
def test_collect_bundle_writes_sha256(tmp_path: Path) -> None:
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / COLLECT_SCRIPT.name
    script.write_text(_text(COLLECT_SCRIPT), encoding="utf-8")
    (tmp_path / "phase_4_2h_hotpath_environment_latency_bundle.zip").write_bytes(b"pass")
    result = subprocess.run(["bash", str(script)], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert result.returncode == 0
    checksum = tmp_path / "phase_4_2h_bundle_sha256.txt"
    assert checksum.exists()
    text = checksum.read_text(encoding="utf-8")
    assert "sha256:" in text
    assert "file_size_bytes:" in text


def test_no_strategy_model_execution_or_pnl_added_to_vps_files() -> None:
    combined = "\n".join(_text(path) for path in (DOC_PATH, SETUP_SCRIPT, RUN_SCRIPT, COLLECT_SCRIPT))
    forbidden_runtime_tokens = ("class ProbabilityModel", "PaperExecutor(", "ExecutionReport(", "OrderIntent(", "profit_and_loss")
    assert not any(token in combined for token in forbidden_runtime_tokens)
