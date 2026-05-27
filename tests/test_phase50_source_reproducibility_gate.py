from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys

from phase50_test_utils import ROOT, load_json


def _load_phase42h_runner():
    path = ROOT / "scripts/run_phase42h_hotpath_environment_latency.py"
    spec = importlib.util.spec_from_file_location("phase42h_runner_for_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_phase42h_runner_subprocess_pythonpath_includes_repo_root_and_bot() -> None:
    module = _load_phase42h_runner()
    parts = module._subprocess_pythonpath(ROOT).split(os.pathsep)
    assert str(ROOT) in parts
    assert str(ROOT / "bot") in parts
    assert module._subprocess_env(ROOT)["PYTHONPATH"] == os.pathsep.join([str(ROOT), str(ROOT / "bot")])


def test_tests_imports_work_in_runner_subprocess_mode() -> None:
    module = _load_phase42h_runner()
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "utf8",
            "-c",
            "import tests.test_phase42h_hotpath_environment_latency as t; print(t._report()['phase'])",
        ],
        cwd=ROOT,
        env=module._subprocess_env(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "4.2H" in result.stdout


def test_phase50_source_reproducibility_gate_report_passes() -> None:
    report = load_json("data/debug/phase_5_0_source_reproducibility_gate.json")
    assert report["status"] == "pass"
    assert report["repo_root_in_pythonpath"] is True
    assert report["bot_in_pythonpath"] is True
    assert report["tests_import_subprocess_ok"] is True
