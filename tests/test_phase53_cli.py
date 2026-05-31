from __future__ import annotations

import json
from pathlib import Path

import scripts.run_phase53_dataset_integrity_audit as cli
from tests.phase53_test_utils import write_good_session


REQUIRED_REPORTS = (
    "phase_5_3_backup_restore_inventory_report",
    "phase_5_3_artifact_integrity_report",
    "phase_5_3_session_lineage_report",
    "phase_5_3_dataset_schema_report",
    "phase_5_3_timestamp_integrity_report",
    "phase_5_3_100ms_label_coverage_report",
    "phase_5_3_orderbook_reference_consistency_report",
    "phase_5_3_runtime_data_health_report",
    "phase_5_3_research_eligibility_report",
)


def test_cli_creates_output_directories_and_writes_reports(tmp_path: Path) -> None:
    write_good_session(tmp_path)
    backup = tmp_path / "backup_meta/destroy_safety_backup_20260530T131704Z"
    backup.mkdir(parents=True)
    (backup / "SHA256SUMS.txt").write_text("", encoding="utf-8")
    (backup / "meta").mkdir()
    (backup / "meta/git_snapshot.txt").write_text("git snapshot\n", encoding="utf-8")

    code = cli.main(
        [
            "--repo-root",
            str(tmp_path),
            "--phase52-sessions",
            "data/phase_5_2/sessions",
            "--failed-runs",
            "data/cache/phase_5_2_failed_runs",
            "--preflight-sessions",
            "data/sessions",
            "--phase52f-artifacts",
            "artifacts/phase_5_2f",
            "--backup-meta",
            "backup_meta/destroy_safety_backup_20260530T131704Z",
            "--output-root",
            "data/phase_5_3",
            "--strict",
        ]
    )

    assert code == 0
    for name in ("reports", "debug", "manifests", "evidence"):
        assert (tmp_path / "data/phase_5_3" / name).is_dir()
    for stem in REQUIRED_REPORTS:
        assert (tmp_path / f"data/phase_5_3/reports/{stem}.json").exists()
        assert (tmp_path / f"data/phase_5_3/reports/{stem}.md").exists()


def test_cli_returns_zero_for_dataset_ineligibility_not_execution_failure(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    (session / "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl").write_text("", encoding="utf-8")

    code = cli.main(
        [
            "--repo-root",
            str(tmp_path),
            "--phase52-sessions",
            "data/phase_5_2/sessions",
            "--failed-runs",
            "data/cache/phase_5_2_failed_runs",
            "--preflight-sessions",
            "data/sessions",
            "--phase52f-artifacts",
            "artifacts/phase_5_2f",
            "--backup-meta",
            "backup_meta/destroy_safety_backup_20260530T131704Z",
            "--output-root",
            "data/phase_5_3",
            "--strict",
        ]
    )

    manifest = json.loads((tmp_path / "data/phase_5_3/manifests/phase_5_3_research_readiness_manifest.json").read_text(encoding="utf-8"))
    assert code == 0
    assert manifest["final_status"] == "phase_5_3_fail"


def test_strict_enforces_hard_gates_in_manifest(tmp_path: Path) -> None:
    session = write_good_session(tmp_path)
    report = session / "data/reports/phase_4_2h_hotpath_environment_latency_report.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["strict_100ms_observability_ready"] = False
    report.write_text(json.dumps(payload), encoding="utf-8")

    cli.main(
        [
            "--repo-root",
            str(tmp_path),
            "--phase52-sessions",
            "data/phase_5_2/sessions",
            "--failed-runs",
            "data/cache/phase_5_2_failed_runs",
            "--preflight-sessions",
            "data/sessions",
            "--phase52f-artifacts",
            "artifacts/phase_5_2f",
            "--backup-meta",
            "backup_meta/destroy_safety_backup_20260530T131704Z",
            "--output-root",
            "data/phase_5_3",
            "--strict",
        ]
    )

    manifest = json.loads((tmp_path / "data/phase_5_3/manifests/phase_5_3_research_readiness_manifest.json").read_text(encoding="utf-8"))
    assert manifest["allowed_phase54_sessions"] == []
    assert "strict_100ms_observability_ready_not_true" in manifest["excluded_sessions"][0]["reasons"]
