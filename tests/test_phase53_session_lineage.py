from __future__ import annotations

from pathlib import Path

from app.research.phase53_dataset_integrity import FAILED_RUN_LINEAGE, PREFLIGHT_SESSION, SessionLineageBuilder
from tests.phase53_test_utils import phase53_config, write_good_session, write_json


def test_session_with_metadata_quality_sha_dataset_is_complete(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    session_dir = write_good_session(tmp_path, "session_001_sanity_30m")
    (session_dir / "phase_5_2_session_001_sanity_30m_sha256.txt").write_text("sha256: " + "0" * 64 + "\n", encoding="utf-8")

    report = SessionLineageBuilder(config).build()

    row = report["sessions"][0]
    assert row["session_id"] == "session_001_sanity_30m"
    assert row["lineage_status"] == "complete"
    assert row["metadata_path"]
    assert row["quality_report_path"]
    assert row["dataset_dir"]


def test_session_missing_quality_report_is_lineage_partial(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    session_dir = write_good_session(tmp_path, "session_001_sanity_30m")
    (session_dir / "phase_5_2_session_001_sanity_30m_quality_report.json").unlink()

    row = SessionLineageBuilder(config).build()["sessions"][0]

    assert row["lineage_status"] == "partial"
    assert "missing_quality_report" in row["lineage_hard_fail_reasons"]


def test_preflight_is_not_research_scope_by_default(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    preflight = config.preflight_sessions / "preflight_check_60s_v4"
    write_json(preflight / "phase_5_2_preflight_check_60s_v4_quality_report.json", {"status": "pass"})

    rows = SessionLineageBuilder(config).build()["sessions"]

    assert rows[0]["session_class"] == PREFLIGHT_SESSION
    assert rows[0]["lineage_status"] == "not_research_scope"


def test_failed_run_lineage_is_not_research_scope_by_default(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    failed = config.failed_runs / "attempt_001/sessions/session_001_sanity_30m"
    write_json(failed / "phase_5_2_session_001_sanity_30m_quality_report.json", {"status": "fail"})

    rows = SessionLineageBuilder(config).build()["sessions"]

    assert rows[0]["session_class"] == FAILED_RUN_LINEAGE
    assert rows[0]["lineage_status"] == "not_research_scope"


def test_session_005_original_and_repaired_eval_are_not_merged(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    write_good_session(tmp_path, "session_005_medium_2h")
    write_good_session(tmp_path, "session_005_medium_2h_repaired_eval", repaired=True)

    rows = SessionLineageBuilder(config).build()["sessions"]
    ids = [row["session_id"] for row in rows]

    assert ids == ["session_005_medium_2h", "session_005_medium_2h_repaired_eval"]
    assert rows[1]["paired_original_session_id"] == "session_005_medium_2h"
