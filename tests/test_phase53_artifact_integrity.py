from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

from app.research.phase53_dataset_integrity import ArtifactIntegrityAuditor, Sha256Verifier, StreamingJsonlScanner
from tests.phase53_test_utils import phase53_config, write_json


def test_sha256_verifier_passes_valid_file(tmp_path: Path) -> None:
    path = tmp_path / "file.txt"
    path.write_text("hello\n", encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    sha = tmp_path / "file.txt.sha256"
    sha.write_text(f"{digest}  file.txt\n", encoding="utf-8")

    assert Sha256Verifier().verify_file(path, sha)["passed"] is True


def test_sha256_verifier_fails_changed_file(tmp_path: Path) -> None:
    path = tmp_path / "file.txt"
    path.write_text("hello\n", encoding="utf-8")
    sha = tmp_path / "file.txt.sha256"
    sha.write_text(f"{'0' * 64}  file.txt\n", encoding="utf-8")

    assert Sha256Verifier().verify_file(path, sha)["passed"] is False


def test_missing_sha256_reported_as_warning_not_crash(tmp_path: Path) -> None:
    path = tmp_path / "file.txt"
    path.write_text("hello\n", encoding="utf-8")

    result = Sha256Verifier().verify_file(path, tmp_path / "missing.sha256")

    assert result["status"] == "missing_sha256"
    assert result["passed"] is None


def test_zip_integrity_test_detects_corrupt_zip(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    zip_path = config.phase52f_artifacts / "bad.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"not a zip")

    result = ArtifactIntegrityAuditor(config)._test_zip(zip_path)

    assert result["status"] == "corrupt"


def test_zip_integrity_test_passes_valid_zip(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    zip_path = config.phase52f_artifacts / "good.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("ok.txt", "ok")

    assert ArtifactIntegrityAuditor(config)._test_zip(zip_path)["status"] == "pass"


def test_json_parse_test_detects_invalid_json(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    bad = config.phase52f_artifacts / "bad.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("{bad", encoding="utf-8")

    failures = ArtifactIntegrityAuditor(config)._json_parse_failures()

    assert failures
    assert failures[0]["path"].endswith("bad.json")


def test_jsonl_streaming_detects_invalid_row_and_bounds_sampling(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text("\n".join(["{bad"] * 30) + "\n", encoding="utf-8")

    result = StreamingJsonlScanner().scan(path)

    assert result["parse_error_count"] == 30
    assert len(result["sample_errors"]) == 20


def test_artifact_report_missing_sha256_is_partial_not_crash(tmp_path: Path) -> None:
    config = phase53_config(tmp_path)
    write_json(config.backup_meta / "meta/git_snapshot.txt", {"not": "json_required_but_valid"})
    report = ArtifactIntegrityAuditor(config).build()

    assert report["integrity_status"] in {"pass", "partial"}
