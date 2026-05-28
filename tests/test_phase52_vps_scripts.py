from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

import pytest

from app.marketdata.orderbook_phase41 import OrderbookPhase41Processor, orderbook_phase41_paths_for_root
from orderbook_phase41_test_utils import make_depth_update


ROOT = Path(__file__).resolve().parents[1]
CLEAN_SCRIPT = ROOT / "scripts/phase52_vps_clean_failed_run.sh"
START_SCRIPT = ROOT / "scripts/phase52_vps_24h_start.sh"
COLLECT_SCRIPT = ROOT / "scripts/phase52_vps_collect_artifacts.sh"
AUDIT_SCRIPT = ROOT / "scripts/phase52_vps_audit_session.sh"
RUN_AUTO_SCRIPT = ROOT / "scripts/run_phase52_auto_collection.py"
GENERATED_ARTIFACT_PATTERNS_THAT_MUST_NOT_BE_TRACKED = (
    "phase_5_2_audit_bundle_sha256.txt",
    "phase_5_2_full_dataset_bundle_sha256.txt",
    "phase_4_2h_bundle_sha256.txt",
    "phase_5_2_audit_bundle.zip",
    "phase_5_2_full_dataset_bundle.zip",
    "phase_4_2h_hotpath_environment_latency_bundle.zip",
    "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip",
    "data/phase_5_2",
    "data/dataset/*.jsonl",
    "data/debug/*.json",
    "data/reports/*.json",
    "data/reports/*.md",
)


def test_phase52_clean_failed_run_removes_active_output_dir(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(CLEAN_SCRIPT, tmp_path)
    active = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m"
    active.mkdir(parents=True)
    (active / "stale.txt").write_text("stale", encoding="utf-8")

    result = subprocess.run([bash, str(script), "--archive-active-output"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "data/phase_5_2").exists()
    assert list((tmp_path / "data/cache/phase_5_2_failed_runs").glob("phase_5_2_failed_before_cleanup_fix_*"))


def test_phase52_clean_failed_run_removes_stale_debug_files(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(CLEAN_SCRIPT, tmp_path)
    stale_files = [
        tmp_path / "data/debug/phase_5_2_auto_collection_nohup.log",
        tmp_path / "data/debug/phase_5_2_auto_collection.pid",
        tmp_path / "data/debug/phase_5_2_auto_collection_status.json",
        tmp_path / "data/debug/phase_5_2_stop_after_current_session",
    ]
    for path in stale_files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("stale", encoding="utf-8")

    result = subprocess.run([bash, str(script), "--delete-active-output"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert all(not path.exists() for path in stale_files)


def test_phase52_clean_failed_run_refuses_when_process_running(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(CLEAN_SCRIPT, tmp_path)
    active = tmp_path / "data/phase_5_2"
    active.mkdir(parents=True)
    env = {**_bash_env(), "PHASE52_FORCE_PROCESS_RUNNING": "1"}

    result = subprocess.run([bash, str(script), "--delete-active-output"], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode != 0
    assert active.exists()
    assert "Refusing to clean" in result.stderr


def test_phase52_start_refuses_stale_active_output_without_resume(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    active = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m"
    active.mkdir(parents=True)

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode != 0
    assert "data/phase_5_2 already exists" in result.stderr


def test_phase52_start_allows_clean_start_after_cleanup(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "Phase 5.2 start validation passed" in result.stdout
    assert "--fail-session-on-quality-gate" in result.stdout


def test_phase52_start_prints_bundle_mode(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "total RAM bytes:" in result.stdout
    assert "swap total bytes:" in result.stdout
    assert "memory guard decision: pass" in result.stdout
    assert "bundle mode: audit-light" in result.stdout
    assert "large datasets included in bundles: false" in result.stdout


def test_phase52_start_warns_or_refuses_known_memory_risk(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    env = {
        **_bash_env(),
        "PHASE52_MEMORY_TOTAL_BYTES": str(3 * 1024 * 1024 * 1024),
        "PHASE52_MEMORY_AVAILABLE_BYTES": str(2 * 1024 * 1024 * 1024),
        "PHASE52_SWAP_TOTAL_BYTES": "0",
    }

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode != 0
    assert "memory_total_bytes" in result.stderr
    assert "--allow-low-memory-vps" in result.stderr


def test_phase52_start_memory_guard_can_be_overridden_explicitly(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    env = {
        **_bash_env(),
        "PHASE52_MEMORY_TOTAL_BYTES": str(3 * 1024 * 1024 * 1024),
        "PHASE52_MEMORY_AVAILABLE_BYTES": str(2 * 1024 * 1024 * 1024),
        "PHASE52_SWAP_TOTAL_BYTES": "0",
    }

    result = subprocess.run([bash, str(script), "--dry-run", "--allow-low-memory-vps"], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "low-memory VPS guard overridden" in result.stderr


def test_phase52_status_reports_last_failure_oom(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(ROOT / "scripts/phase52_vps_status.sh", tmp_path)
    status = tmp_path / "data/debug/phase_5_2_auto_collection_status.json"
    status.parent.mkdir(parents=True)
    status.write_text(
        json.dumps(
            {
                "current_session": "session_002_short_1h",
                "completed_session_count": 2,
                "passed_session_count": 1,
                "failed_session_count": 1,
                "research_eligible_session_count": 1,
                "last_failure": "OOM_KILLED",
                "stopped_early": True,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run([bash, str(script)], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "last_failure: OOM_KILLED" in result.stdout


def test_phase52_audit_session_rejects_path_traversal(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(AUDIT_SCRIPT, tmp_path)

    result = subprocess.run([bash, str(script), "../session_004_medium_2h"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 2
    assert "Invalid session id" in result.stderr


def test_phase52_audit_session_missing_report_exits_nonzero(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(AUDIT_SCRIPT, tmp_path)
    status = tmp_path / "data/debug/phase_5_2_auto_collection_status.json"
    status.parent.mkdir(parents=True)
    status.write_text(
        json.dumps(
            {
                "running": False,
                "current_session": "session_004_medium_2h",
                "completed_session_count": 4,
                "passed_session_count": 3,
                "failed_session_count": 1,
                "research_eligible_session_count": 3,
                "last_failure": "REPORT_MISSING",
                "stopped_early": True,
                "stop_reason": "quality gate failed for session_004_medium_2h",
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run([bash, str(script), "session_004_medium_2h"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 1
    assert "=== Missing required reports ===" in result.stdout
    assert "quality_report" in result.stdout
    assert "metadata" in result.stdout
    assert "hotpath_report" in result.stdout
    assert "self_check" in result.stdout


def test_phase52_clean_failed_run_archive_mode_keeps_git_status_clean(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(CLEAN_SCRIPT, tmp_path)
    _init_clean_git_repo_with_script(tmp_path, script)
    active = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m"
    active.mkdir(parents=True)
    (active / "stale.txt").write_text("stale", encoding="utf-8")

    result = subprocess.run([bash, str(script), "--archive-active-output"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)
    status = subprocess.run(["git", "status", "--short", "--untracked-files=all"], cwd=tmp_path, text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    assert status.returncode == 0
    assert status.stdout == ""
    assert list((tmp_path / "data/cache/phase_5_2_failed_runs").glob("phase_5_2_failed_before_cleanup_fix_*"))


def test_cleanup_archive_mode_keeps_git_status_clean(tmp_path: Path) -> None:
    test_phase52_clean_failed_run_archive_mode_keeps_git_status_clean(tmp_path)


def test_phase52_start_refuses_dirty_git_state_including_untracked_archive(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    _init_clean_git_repo_with_script(tmp_path, script)
    legacy_archive = tmp_path / "data/phase_5_2_failed_before_cleanup_fix_legacy"
    legacy_archive.mkdir(parents=True)
    (legacy_archive / "artifact.txt").write_text("old", encoding="utf-8")

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode != 0
    assert "git working tree is dirty" in result.stderr


def test_phase52_start_guard_stays_clean_after_phase41_report_generation(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    docs_report = tmp_path / "docs/reports/phase_4_1_orderbook_quality_report.md"
    docs_report.parent.mkdir(parents=True, exist_ok=True)
    docs_report.write_text("# Static Phase 4.1 report\n", encoding="utf-8")
    _init_clean_git_repo_with_script(tmp_path, script)
    subprocess.run(["git", "add", "docs/reports/phase_4_1_orderbook_quality_report.md"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "static docs report"], cwd=tmp_path, text=True, capture_output=True, check=True)
    before = docs_report.read_text(encoding="utf-8")

    processor = OrderbookPhase41Processor(symbols=("BTCUSDT",), paths=orderbook_phase41_paths_for_root(tmp_path))
    processor.load_snapshot(
        "BTCUSDT",
        bids=[("100.00", "1.0"), ("99.00", "2.0")],
        asks=[("101.00", "1.5"), ("102.00", "2.5")],
        last_update_id=100,
        local_recv_monotonic_ns=1_000_000_000,
    )
    processor.process_depth_update(make_depth_update(first_update_id=101, final_update_id=101))
    processor.write_reports(duration_sec=1.0)

    status = subprocess.run(["git", "status", "--short", "--untracked-files=all"], cwd=tmp_path, text=True, capture_output=True, check=False)
    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert docs_report.read_text(encoding="utf-8") == before
    assert status.returncode == 0
    assert status.stdout == ""
    assert result.returncode == 0, result.stderr
    assert "Phase 5.2 start validation passed" in result.stdout


def test_phase52_start_refuses_missing_gitignore_rules(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(START_SCRIPT, tmp_path)
    _write_phase52_runner_marker(tmp_path)
    (tmp_path / ".gitignore").write_text(
        "\n".join(
            [
                "data/phase_5_2/",
                "data/cache/phase_5_2_failed_runs/",
                "data/dataset/",
                "data/debug/",
                "data/reports/",
                "*.jsonl",
                "*.zip",
                "*.log",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init"], cwd=tmp_path, text=True, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", ".gitignore", str(script.relative_to(tmp_path)).replace("\\", "/"), "bot/app/research/phase52_auto_collection.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, text=True, capture_output=True, check=True)

    result = subprocess.run([bash, str(script), "--dry-run"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode != 0
    assert ".gitignore is missing generated artifact rules" in result.stderr
    assert "phase_5_2_audit_bundle_sha256.txt" in result.stderr


def test_phase52_cli_clean_start_creates_session_001(tmp_path: Path) -> None:
    result = _run_phase52_cli(tmp_path, test_max_sessions=1)

    assert result.returncode == 0, result.stderr
    manifest = json.loads((tmp_path / "data/debug/phase_5_2_auto_collection_manifest.json").read_text(encoding="utf-8"))
    assert manifest["sessions"][0]["session_id"] == "session_001_sanity_30m"


def test_phase52_cli_does_not_start_at_session_002_after_clean(tmp_path: Path) -> None:
    result = _run_phase52_cli(tmp_path, test_max_sessions=1)

    assert result.returncode == 0, result.stderr
    status = json.loads((tmp_path / "data/debug/phase_5_2_auto_collection_status.json").read_text(encoding="utf-8"))
    assert status["current_session"] == "session_001_sanity_30m"
    assert status["completed_session_count"] == 1


def test_phase52_generated_artifacts_are_not_tracked_by_git() -> None:
    result = subprocess.run(
        ["git", "ls-files", "--", *GENERATED_ARTIFACT_PATTERNS_THAT_MUST_NOT_BE_TRACKED],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == []


def test_phase52_all_sessions_sha256_has_exact_gitignore_pattern() -> None:
    patterns = {
        line.strip()
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    assert "phase_5_2_audit_bundle_sha256.txt" in patterns
    assert "phase_5_2_full_dataset_bundle_sha256.txt" in patterns
    assert "data/cache/phase_5_2_failed_runs/" in patterns


def test_phase52_collect_artifacts_defaults_to_audit_light(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(COLLECT_SCRIPT, tmp_path)
    _seed_collect_artifacts(tmp_path)

    result = subprocess.run([bash, str(script)], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    with zipfile.ZipFile(tmp_path / "phase_5_2_vps_downloadable_artifacts.zip") as archive:
        names = archive.namelist()
    assert "file_size_manifest.json" in names
    assert not any(name.endswith(".jsonl") for name in names)
    assert not any(name.endswith(".zip") for name in names)


def test_phase52_collect_artifacts_full_mode_includes_datasets_only_when_requested(tmp_path: Path) -> None:
    bash = _require_bash()
    script = _copy_script(COLLECT_SCRIPT, tmp_path)
    _seed_collect_artifacts(tmp_path)

    default_result = subprocess.run([bash, str(script)], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)
    assert default_result.returncode == 0, default_result.stderr
    assert not (tmp_path / "phase_5_2_vps_full_dataset_artifacts.zip").exists()

    full_result = subprocess.run([bash, str(script), "--include-large-datasets"], cwd=tmp_path, env=_bash_env(), text=True, capture_output=True, check=False)
    assert full_result.returncode == 0, full_result.stderr
    with zipfile.ZipFile(tmp_path / "phase_5_2_vps_full_dataset_artifacts.zip") as archive:
        names = archive.namelist()
    assert "data/phase_5_2/sessions/session_001_sanity_30m/data/dataset/large.jsonl" in names
    assert not any(name.endswith(".zip") for name in names)


def _run_phase52_cli(tmp_path: Path, *, test_max_sessions: int) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "bot")])}
    return subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            str(RUN_AUTO_SCRIPT),
            "--root",
            str(tmp_path),
            "--plan-name",
            "phase52_test",
            "--total-budget-hours",
            "24",
            "--output-dir",
            "data/phase_5_2",
            "--strict-100ms",
            "--create-bundles",
            "--dry-run",
            "--test-max-sessions",
            str(test_max_sessions),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _copy_script(source: Path, tmp_path: Path) -> Path:
    target = tmp_path / "scripts" / source.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return target


def _write_phase52_runner_marker(tmp_path: Path) -> None:
    marker = tmp_path / "bot/app/research/phase52_auto_collection.py"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text('command = ["--clean"]\n', encoding="utf-8")


def _seed_collect_artifacts(tmp_path: Path) -> None:
    session = tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m"
    (session / "data/dataset").mkdir(parents=True, exist_ok=True)
    (session / "data/reports").mkdir(parents=True, exist_ok=True)
    (session / "data/debug").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data/debug").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data/reports").mkdir(parents=True, exist_ok=True)
    (session / "phase_5_2_session_001_sanity_30m_metadata.json").write_text("{}", encoding="utf-8")
    (session / "phase_5_2_session_001_sanity_30m_quality_report.json").write_text("{}", encoding="utf-8")
    (session / "phase_5_2_session_001_sanity_30m_console.log").write_text("console\n", encoding="utf-8")
    (session / "data/dataset/large.jsonl").write_text("{}\n", encoding="utf-8")
    (session / "data/dataset/phase_4_2h_latency_profile_datasets.zip").write_bytes(b"zip")
    (session / "data/reports/phase_4_2h_hotpath_environment_latency_report.json").write_text("{}", encoding="utf-8")
    (session / "data/debug/phase_4_2h_artifact_cleanup.json").write_text("{}", encoding="utf-8")
    (tmp_path / "data/debug/phase_5_2_auto_collection_status.json").write_text("{}", encoding="utf-8")
    (tmp_path / "data/reports/phase_5_2_auto_collection_report.json").write_text("{}", encoding="utf-8")


def _init_clean_git_repo_with_script(tmp_path: Path, script: Path) -> None:
    (tmp_path / ".gitignore").write_text(
        "\n".join(
            [
                "data/phase_5_2/",
                "data/cache/",
                "data/cache/phase_5_2_failed_runs/",
                "data/dataset/",
                "data/debug/",
                "data/reports/",
                "phase_5_2_audit_bundle_sha256.txt",
                "phase_5_2_full_dataset_bundle_sha256.txt",
                "*.zip",
                "*.log",
                "*.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init"], cwd=tmp_path, text=True, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    tracked = [".gitignore", str(script.relative_to(tmp_path)).replace("\\", "/")]
    marker = tmp_path / "bot/app/research/phase52_auto_collection.py"
    if marker.exists():
        tracked.append(str(marker.relative_to(tmp_path)).replace("\\", "/"))
    subprocess.run(["git", "add", *tracked], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, text=True, capture_output=True, check=True)


def _require_bash() -> str:
    bash = shutil.which("bash")
    candidates = [bash, r"C:\Program Files\Git\usr\bin\bash.exe", "/bin/bash"]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    pytest.skip("bash is not available on this host")


def _bash_env() -> dict[str, str]:
    env = os.environ.copy()
    git_usr_bin = Path(r"C:\Program Files\Git\usr\bin")
    if git_usr_bin.exists():
        env["PATH"] = str(git_usr_bin) + os.pathsep + env.get("PATH", "")
    env.setdefault("PHASE52_MEMORY_TOTAL_BYTES", str(8 * 1024 * 1024 * 1024))
    env.setdefault("PHASE52_MEMORY_AVAILABLE_BYTES", str(6 * 1024 * 1024 * 1024))
    env.setdefault("PHASE52_SWAP_TOTAL_BYTES", str(2 * 1024 * 1024 * 1024))
    return env
