from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import (
    BACKUP_META,
    FAILED_RUN_LINEAGE,
    PHASE52F_EVIDENCE_ARTIFACT,
    PHASE53_OUTPUT,
    PREFLIGHT_SESSION,
    PRIMARY_PHASE52_SESSION,
    REPAIRED_EVAL_SESSION,
    ROOT_LEGACY_DATASET,
    ROOT_REPORT_OR_DEBUG,
    UNKNOWN,
    PathClassifier,
)


def _classifier(tmp_path: Path) -> PathClassifier:
    return PathClassifier(
        repo_root=tmp_path,
        phase52_sessions="data/phase_5_2/sessions",
        failed_runs="data/cache/phase_5_2_failed_runs",
        preflight_sessions="data/sessions",
        phase52f_artifacts="artifacts/phase_5_2f",
        backup_meta="backup_meta/destroy_safety_backup_20260530T131704Z",
        output_root="data/phase_5_3",
    )


def test_classifies_primary_phase52_sessions(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "data/phase_5_2/sessions/session_001_sanity_30m/file.jsonl") == PRIMARY_PHASE52_SESSION


def test_classifies_repaired_eval_session(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "data/phase_5_2/sessions/session_005_medium_2h_repaired_eval/file.jsonl") == REPAIRED_EVAL_SESSION


def test_classifies_failed_run_lineage(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "data/cache/phase_5_2_failed_runs/attempt/session_001/file.json") == FAILED_RUN_LINEAGE


def test_classifies_preflight_sessions(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "data/sessions/preflight_check_60s_v4/report.json") == PREFLIGHT_SESSION


def test_classifies_root_legacy_dataset(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "data/dataset/orderbook_clean_samples.jsonl") == ROOT_LEGACY_DATASET


def test_classifies_root_report_or_debug(tmp_path: Path) -> None:
    classifier = _classifier(tmp_path)
    assert classifier.classify(tmp_path / "data/debug/old.json") == ROOT_REPORT_OR_DEBUG
    assert classifier.classify(tmp_path / "data/reports/old.json") == ROOT_REPORT_OR_DEBUG


def test_classifies_phase52f_evidence_and_backup_and_phase53(tmp_path: Path) -> None:
    classifier = _classifier(tmp_path)
    assert classifier.classify(tmp_path / "artifacts/phase_5_2f/evidence.zip.sha256") == PHASE52F_EVIDENCE_ARTIFACT
    assert classifier.classify(tmp_path / "backup_meta/destroy_safety_backup_20260530T131704Z/SHA256SUMS.txt") == BACKUP_META
    assert classifier.classify(tmp_path / "data/phase_5_3/reports/report.json") == PHASE53_OUTPUT


def test_unknown_paths_become_unknown_not_ignored(tmp_path: Path) -> None:
    assert _classifier(tmp_path).classify(tmp_path / "random/file.txt") == UNKNOWN
