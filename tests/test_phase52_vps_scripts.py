from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
CLEAN_SCRIPT = ROOT / "scripts/phase52_vps_clean_failed_run.sh"
START_SCRIPT = ROOT / "scripts/phase52_vps_24h_start.sh"
RUN_AUTO_SCRIPT = ROOT / "scripts/run_phase52_auto_collection.py"
GENERATED_ARTIFACT_PATTERNS_THAT_MUST_NOT_BE_TRACKED = (
    "phase_5_2_auto_collection_all_sessions_sha256.txt",
    "phase_4_2h_bundle_sha256.txt",
    "phase_5_2_auto_collection_all_sessions_bundle.zip",
    "phase_4_2h_hotpath_environment_latency_bundle.zip",
    "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip",
    "data/phase_5_2",
    "data/dataset/*.jsonl",
    "data/debug/*.json",
    "data/reports/*.json",
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
    assert "phase_5_2_auto_collection_all_sessions_sha256.txt" in patterns
    assert "data/cache/phase_5_2_failed_runs/" in patterns


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


def _init_clean_git_repo_with_script(tmp_path: Path, script: Path) -> None:
    (tmp_path / ".gitignore").write_text(
        "\n".join(
            [
                "data/phase_5_2/",
                "data/cache/",
                "data/cache/phase_5_2_failed_runs/",
                "phase_5_2_auto_collection_all_sessions_sha256.txt",
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
    return env
