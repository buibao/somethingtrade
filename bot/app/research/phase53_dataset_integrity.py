from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from statistics import mean
from typing import Any, Callable, Iterable
import zipfile


PHASE = "5.3"
PHASE_NAME = "Dataset Integrity & Research Readiness"

MAX_SAMPLE_ERRORS_PER_FILE = 20
HASH_CHUNK_SIZE = 1024 * 1024 * 8
LARGE_FILE_BYTES = 100 * 1024 * 1024
REQUIRED_100MS_VALID_RATE = 0.95
REQUIRED_100MS_MAX_FUTURE_GAP_MS = 100

PRIMARY_PHASE52_SESSION = "PRIMARY_PHASE52_SESSION"
REPAIRED_EVAL_SESSION = "REPAIRED_EVAL_SESSION"
FAILED_RUN_LINEAGE = "FAILED_RUN_LINEAGE"
PREFLIGHT_SESSION = "PREFLIGHT_SESSION"
ROOT_LEGACY_DATASET = "ROOT_LEGACY_DATASET"
ROOT_REPORT_OR_DEBUG = "ROOT_REPORT_OR_DEBUG"
PHASE52F_EVIDENCE_ARTIFACT = "PHASE52F_EVIDENCE_ARTIFACT"
BACKUP_META = "BACKUP_META"
PHASE53_OUTPUT = "PHASE53_OUTPUT"
UNKNOWN = "UNKNOWN"

PATH_CLASSES = (
    PRIMARY_PHASE52_SESSION,
    REPAIRED_EVAL_SESSION,
    FAILED_RUN_LINEAGE,
    PREFLIGHT_SESSION,
    ROOT_LEGACY_DATASET,
    ROOT_REPORT_OR_DEBUG,
    PHASE52F_EVIDENCE_ARTIFACT,
    BACKUP_META,
    PHASE53_OUTPUT,
    UNKNOWN,
)

EXPECTED_PRIMARY_SESSIONS = (
    "session_001_sanity_30m",
    "session_002_short_1h",
    "session_003_short_1h",
    "session_004_medium_2h",
    "session_005_medium_2h",
    "session_005_medium_2h_repaired_eval",
)

REQUIRED_DATASET_FILES = (
    "orderbook_clean_samples.jsonl",
    "bookticker_reference_quotes.jsonl",
    "trade_reference_events.jsonl",
    "aggtrade_reference_events.jsonl",
    "orderbook_reference_benchmark_labels.jsonl",
    "orderbook_time_protocol_benchmark_labels.jsonl",
    "phase_4_2h_corrected_time_protocol_labels.jsonl",
    "phase_4_2h_latency_profile_samples.jsonl",
    "phase_4_2fg_latency_profile_samples.jsonl",
)

LABEL_DATASET_FILES = (
    "phase_4_2h_corrected_time_protocol_labels.jsonl",
    "orderbook_time_protocol_benchmark_labels.jsonl",
    "orderbook_reference_benchmark_labels.jsonl",
)

REFERENCE_DATASET_FILES = {
    "bookticker_reference_quotes.jsonl": "bookticker_reference_rows",
    "trade_reference_events.jsonl": "trade_reference_rows",
    "aggtrade_reference_events.jsonl": "aggtrade_reference_rows",
}

DEBUG_CASE_FILES = {
    "duplicate_update_cases.jsonl": "duplicate_update_cases",
    "sequence_gap_cases.jsonl": "sequence_gap_cases",
    "stale_period_cases.jsonl": "stale_period_cases",
    "book_incomplete_cases.jsonl": "book_incomplete_cases",
    "invalid_delta_cases.jsonl": "invalid_delta_cases",
    "orderbook_mismatch_cases.jsonl": "orderbook_mismatch_cases",
}

REPORT_FILENAMES = (
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

FINAL_MANIFEST_NAME = "phase_5_3_research_readiness_manifest"
FINAL_EVIDENCE_ZIP = "phase_5_3_final_evidence_bundle.zip"


@dataclass(frozen=True)
class Phase53Config:
    repo_root: Path
    phase52_sessions: Path
    failed_runs: Path
    preflight_sessions: Path
    phase52f_artifacts: Path
    backup_meta: Path
    output_root: Path
    strict: bool = False

    @classmethod
    def from_paths(
        cls,
        *,
        repo_root: str | Path,
        phase52_sessions: str | Path,
        failed_runs: str | Path,
        preflight_sessions: str | Path,
        phase52f_artifacts: str | Path,
        backup_meta: str | Path,
        output_root: str | Path,
        strict: bool = False,
    ) -> "Phase53Config":
        root = Path(repo_root).resolve()
        return cls(
            repo_root=root,
            phase52_sessions=_resolve(root, phase52_sessions),
            failed_runs=_resolve(root, failed_runs),
            preflight_sessions=_resolve(root, preflight_sessions),
            phase52f_artifacts=_resolve(root, phase52f_artifacts),
            backup_meta=_resolve(root, backup_meta),
            output_root=_resolve(root, output_root),
            strict=strict,
        )

    @property
    def reports_dir(self) -> Path:
        return self.output_root / "reports"

    @property
    def debug_dir(self) -> Path:
        return self.output_root / "debug"

    @property
    def manifests_dir(self) -> Path:
        return self.output_root / "manifests"

    @property
    def evidence_dir(self) -> Path:
        return self.output_root / "evidence"

    def input_roots(self) -> dict[str, str]:
        return {
            "phase52_sessions": _display_path(_relative_or_abs(self.repo_root, self.phase52_sessions)),
            "failed_runs": _display_path(_relative_or_abs(self.repo_root, self.failed_runs)),
            "preflight_sessions": _display_path(_relative_or_abs(self.repo_root, self.preflight_sessions)),
            "phase52f_artifacts": _display_path(_relative_or_abs(self.repo_root, self.phase52f_artifacts)),
            "backup_meta": _display_path(_relative_or_abs(self.repo_root, self.backup_meta)),
        }


def ensure_phase53_output_dirs(config: Phase53Config) -> None:
    for path in (config.reports_dir, config.debug_dir, config.manifests_dir, config.evidence_dir):
        path.mkdir(parents=True, exist_ok=True)


class PathClassifier:
    def __init__(
        self,
        *,
        repo_root: str | Path,
        phase52_sessions: str | Path,
        failed_runs: str | Path,
        preflight_sessions: str | Path,
        phase52f_artifacts: str | Path,
        backup_meta: str | Path,
        output_root: str | Path,
    ) -> None:
        root = Path(repo_root).resolve()
        self.repo_root = root
        self.phase52_sessions = _resolve(root, phase52_sessions)
        self.failed_runs = _resolve(root, failed_runs)
        self.preflight_sessions = _resolve(root, preflight_sessions)
        self.phase52f_artifacts = _resolve(root, phase52f_artifacts)
        self.backup_meta = _resolve(root, backup_meta)
        self.output_root = _resolve(root, output_root)
        self.root_dataset = root / "data" / "dataset"
        self.root_debug = root / "data" / "debug"
        self.root_reports = root / "data" / "reports"

    def classify(self, path: str | Path) -> str:
        target = _resolve(self.repo_root, path)
        if _is_relative_to(target, self.output_root):
            return PHASE53_OUTPUT
        if _is_relative_to(target, self.phase52f_artifacts):
            return PHASE52F_EVIDENCE_ARTIFACT
        if _is_relative_to(target, self.backup_meta):
            return BACKUP_META
        if _is_relative_to(target, self.failed_runs):
            return FAILED_RUN_LINEAGE
        if _is_relative_to(target, self.phase52_sessions):
            session_id = _first_relative_part(target, self.phase52_sessions)
            if "repaired_eval" in session_id.lower():
                return REPAIRED_EVAL_SESSION
            return PRIMARY_PHASE52_SESSION
        if _is_relative_to(target, self.preflight_sessions):
            first = _first_relative_part(target, self.preflight_sessions)
            if first.startswith("preflight_check_60s"):
                return PREFLIGHT_SESSION
        if _is_relative_to(target, self.root_dataset):
            return ROOT_LEGACY_DATASET
        if _is_relative_to(target, self.root_debug) or _is_relative_to(target, self.root_reports):
            return ROOT_REPORT_OR_DEBUG
        return UNKNOWN


class ArtifactInventoryBuilder:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.classifier = PathClassifier(
            repo_root=config.repo_root,
            phase52_sessions=config.phase52_sessions,
            failed_runs=config.failed_runs,
            preflight_sessions=config.preflight_sessions,
            phase52f_artifacts=config.phase52f_artifacts,
            backup_meta=config.backup_meta,
            output_root=config.output_root,
        )

    def build(self) -> dict[str, Any]:
        file_count_by_class: Counter[str] = Counter()
        byte_count_by_class: Counter[str] = Counter()
        unknown_paths: list[str] = []
        scan_roots = _dedupe_paths(
            [
                self.config.phase52_sessions,
                self.config.failed_runs,
                self.config.preflight_sessions,
                self.config.phase52f_artifacts,
                self.config.backup_meta,
                self.config.repo_root / "data" / "dataset",
                self.config.repo_root / "data" / "debug",
                self.config.repo_root / "data" / "reports",
                self.config.output_root,
            ]
        )
        for root in scan_roots:
            if not root.exists():
                continue
            for path in _iter_files_and_dirs(root):
                cls = self.classifier.classify(path)
                if path.is_file():
                    file_count_by_class[cls] += 1
                    try:
                        byte_count_by_class[cls] += path.stat().st_size
                    except OSError:
                        pass
                if cls == UNKNOWN and len(unknown_paths) < 50:
                    unknown_paths.append(_display_path(_relative_or_abs(self.config.repo_root, path)))

        session_dirs = _session_dirs(self.config.phase52_sessions)
        present = [path.name for path in session_dirs]
        report = {
            "phase": PHASE,
            "name": PHASE_NAME,
            "created_at_utc": _utc_now(),
            "repo_root": _display_path(self.config.repo_root),
            "output_root": _display_path(_relative_or_abs(self.config.repo_root, self.config.output_root)),
            "input_roots": self.config.input_roots(),
            "file_count_by_class": {cls: int(file_count_by_class.get(cls, 0)) for cls in PATH_CLASSES},
            "byte_count_by_class": {cls: int(byte_count_by_class.get(cls, 0)) for cls in PATH_CLASSES},
            "session_directory_count": len(session_dirs),
            "known_primary_sessions_present": [session for session in EXPECTED_PRIMARY_SESSIONS if session in present],
            "missing_expected_sessions": [session for session in EXPECTED_PRIMARY_SESSIONS if session not in present],
            "unknown_path_count": int(file_count_by_class.get(UNKNOWN, 0)),
            "unknown_paths_sample": unknown_paths,
        }
        return report


class Sha256Verifier:
    def compute_sha256(self, path: str | Path) -> str:
        target = Path(path)
        digest = hashlib.sha256()
        with target.open("rb") as handle:
            for chunk in iter(lambda: handle.read(HASH_CHUNK_SIZE), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def read_recorded_sha256(self, path: str | Path) -> str | None:
        try:
            text = Path(path).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return None
        match = re.search(r"\b[a-fA-F0-9]{64}\b", text)
        return match.group(0).lower() if match else None

    def verify_file(self, path: str | Path, sha256_path: str | Path | None = None) -> dict[str, Any]:
        target = Path(path)
        sha_path = Path(sha256_path) if sha256_path is not None else Path(str(target) + ".sha256")
        if not target.exists() or not target.is_file():
            return {"path": _display_path(target), "status": "missing_file", "passed": False, "reason": "file_missing"}
        if not sha_path.exists() or not sha_path.is_file():
            return {
                "path": _display_path(target),
                "sha256_path": _display_path(sha_path),
                "status": "missing_sha256",
                "passed": None,
                "warning": "sha256_file_missing",
            }
        expected = self.read_recorded_sha256(sha_path)
        actual = self.compute_sha256(target)
        passed = expected == actual
        return {
            "path": _display_path(target),
            "sha256_path": _display_path(sha_path),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "status": "pass" if passed else "fail",
            "passed": passed,
        }

    def verify_manifest(self, base_dir: str | Path, manifest_path: str | Path) -> dict[str, Any]:
        base = Path(base_dir)
        manifest = Path(manifest_path)
        if not manifest.exists():
            return {"present": False, "checked": 0, "passed": 0, "failed": 0, "skipped": 0, "failures": []}
        checked = passed = failed = skipped = 0
        failures: list[dict[str, Any]] = []
        try:
            lines = manifest.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return {"present": True, "checked": 0, "passed": 0, "failed": 1, "skipped": 0, "failures": [{"reason": "manifest_read_error"}]}
        for line in lines:
            parsed = _parse_sha256sum_line(line)
            if parsed is None:
                continue
            expected, relative = parsed
            candidate = (base / relative).resolve()
            if not _is_relative_to(candidate, base):
                skipped += 1
                continue
            if not candidate.exists() or not candidate.is_file():
                skipped += 1
                continue
            checked += 1
            actual = self.compute_sha256(candidate)
            if actual == expected:
                passed += 1
            else:
                failed += 1
                if len(failures) < MAX_SAMPLE_ERRORS_PER_FILE:
                    failures.append({"path": _display_path(candidate), "expected": expected, "actual": actual})
        return {"present": True, "checked": checked, "passed": passed, "failed": failed, "skipped": skipped, "failures": failures}


class StreamingJsonlScanner:
    def scan(
        self,
        path: str | Path,
        *,
        row_validator: Callable[[dict[str, Any], int], list[str]] | None = None,
    ) -> dict[str, Any]:
        target = Path(path)
        row_count = 0
        parse_error_count = 0
        empty_line_count = 0
        required_field_failures = 0
        type_failures = 0
        numeric_sanity_failures = 0
        sample_errors: list[dict[str, Any]] = []
        first_event_time: Any = None
        last_event_time: Any = None

        if not target.exists() or not target.is_file():
            return {
                "path": _display_path(target),
                "file_size_bytes": 0,
                "row_count": 0,
                "parse_error_count": 0,
                "empty_line_count": 0,
                "first_event_time": None,
                "last_event_time": None,
                "required_field_failures": 1,
                "type_failures": 0,
                "numeric_sanity_failures": 0,
                "sample_errors": [{"line": None, "error": "file_missing"}],
                "schema_status": "fail",
            }
        try:
            size_bytes = target.stat().st_size
        except OSError:
            size_bytes = 0
        try:
            with target.open("r", encoding="utf-8", errors="replace") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    line = raw_line.strip()
                    if not line:
                        empty_line_count += 1
                        continue
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError as exc:
                        parse_error_count += 1
                        _append_sample(sample_errors, line_number, f"json_parse_error:{exc.msg}")
                        continue
                    if not isinstance(value, dict):
                        parse_error_count += 1
                        _append_sample(sample_errors, line_number, "json_row_not_object")
                        continue
                    row_count += 1
                    event_time = _event_time(value)
                    if first_event_time is None and event_time is not None:
                        first_event_time = event_time
                    if event_time is not None:
                        last_event_time = event_time
                    if row_validator is not None:
                        for error in row_validator(value, line_number):
                            if error.startswith("required:"):
                                required_field_failures += 1
                            elif error.startswith("type:"):
                                type_failures += 1
                            else:
                                numeric_sanity_failures += 1
                            _append_sample(sample_errors, line_number, error)
        except OSError as exc:
            parse_error_count += 1
            _append_sample(sample_errors, None, f"read_error:{exc}")

        if parse_error_count or type_failures or numeric_sanity_failures or row_count == 0:
            schema_status = "fail"
        elif required_field_failures:
            schema_status = "partial"
        else:
            schema_status = "pass"
        return {
            "path": _display_path(target),
            "file_size_bytes": int(size_bytes),
            "row_count": int(row_count),
            "parse_error_count": int(parse_error_count),
            "empty_line_count": int(empty_line_count),
            "first_event_time": first_event_time,
            "last_event_time": last_event_time,
            "required_field_failures": int(required_field_failures),
            "type_failures": int(type_failures),
            "numeric_sanity_failures": int(numeric_sanity_failures),
            "sample_errors": sample_errors,
            "schema_status": schema_status,
        }

    def count_rows(self, path: str | Path) -> int:
        target = Path(path)
        if not target.exists() or not target.is_file():
            return 0
        count = 0
        with target.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.strip():
                    count += 1
        return count


class ArtifactIntegrityAuditor:
    def __init__(self, config: Phase53Config, lineages: list[dict[str, Any]] | None = None) -> None:
        self.config = config
        self.lineages = lineages or []
        self.verifier = Sha256Verifier()
        self.scanner = StreamingJsonlScanner()

    def build(self) -> dict[str, Any]:
        backup_manifest = self.config.backup_meta / "SHA256SUMS.txt"
        manifest_result = self.verifier.verify_manifest(self.config.backup_meta, backup_manifest)
        session_sha_files = sorted(self.config.phase52_sessions.rglob("*sha256*")) if self.config.phase52_sessions.exists() else []
        session_checks = [self._verify_session_sha(path) for path in session_sha_files if path.is_file()]
        zip_results = [self._test_zip(path) for path in self._zip_files()]
        json_failures = self._json_parse_failures()
        jsonl_failures = self._jsonl_smoke_failures()
        hard_fail_reasons = []
        if manifest_result["failed"]:
            hard_fail_reasons.append("backup_meta_internal_sha256_failure")
        if any(item.get("passed") is False for item in session_checks):
            hard_fail_reasons.append("per_session_sha256_failure")
        if any(item.get("status") == "corrupt" for item in zip_results):
            hard_fail_reasons.append("zip_corruption_detected")
        if json_failures:
            hard_fail_reasons.append("json_parse_failures_detected")
        if jsonl_failures:
            hard_fail_reasons.append("jsonl_parse_failures_detected")
        warnings = []
        if any(item.get("passed") is None for item in session_checks):
            warnings.append("some_session_sha256_targets_missing_or_unverifiable")
        integrity_status = "fail" if hard_fail_reasons else ("partial" if warnings else "pass")
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            "backup_meta_sha256s_present": backup_manifest.exists(),
            "internal_sha256s_checked": manifest_result["checked"],
            "internal_sha256s_passed": manifest_result["passed"],
            "internal_sha256s_failed": manifest_result["failed"],
            "internal_sha256s_skipped": manifest_result["skipped"],
            "internal_sha256_failure_samples": manifest_result["failures"],
            "per_session_sha256_files_found": len(session_sha_files),
            "per_session_sha256_files_checked": sum(1 for item in session_checks if item.get("passed") is not None),
            "per_session_sha256_failures": [item for item in session_checks if item.get("passed") is False],
            "zip_files_tested": len(zip_results),
            "zip_files_corrupt": [item for item in zip_results if item.get("status") == "corrupt"],
            "json_files_parse_failures": json_failures,
            "jsonl_files_parse_failures": jsonl_failures,
            "warnings": warnings,
            "integrity_status": integrity_status,
            "hard_fail_reasons": hard_fail_reasons,
        }

    def _verify_session_sha(self, sha_path: Path) -> dict[str, Any]:
        expected = self.verifier.read_recorded_sha256(sha_path)
        text = sha_path.read_text(encoding="utf-8", errors="ignore")
        filename_match = re.search(r"filename:\s*(?P<name>.+)", text)
        if filename_match:
            target = sha_path.parent / filename_match.group("name").strip()
        else:
            target = _candidate_target_from_sha_path(sha_path)
        if not target.exists() or not target.is_file():
            return {
                "sha256_path": _display_path(sha_path),
                "target_path": _display_path(target),
                "expected_sha256": expected,
                "passed": None,
                "status": "target_missing",
            }
        actual = self.verifier.compute_sha256(target)
        return {
            "sha256_path": _display_path(sha_path),
            "target_path": _display_path(target),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "passed": bool(expected and expected == actual),
            "status": "pass" if expected and expected == actual else "fail",
        }

    def _zip_files(self) -> list[Path]:
        roots = [self.config.phase52_sessions, self.config.phase52f_artifacts]
        result: list[Path] = []
        for root in roots:
            if root.exists():
                result.extend(path for path in root.rglob("*.zip") if path.is_file())
        return sorted(_dedupe_paths(result), key=lambda path: _display_path(path))

    def _test_zip(self, path: Path) -> dict[str, Any]:
        try:
            with zipfile.ZipFile(path, "r") as archive:
                bad_member = archive.testzip()
        except (OSError, zipfile.BadZipFile) as exc:
            return {"path": _display_path(path), "status": "corrupt", "reason": str(exc)}
        return {"path": _display_path(path), "status": "pass" if bad_member is None else "corrupt", "bad_member": bad_member}

    def _json_parse_failures(self) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        roots = [self.config.phase52_sessions, self.config.failed_runs, self.config.preflight_sessions, self.config.phase52f_artifacts, self.config.backup_meta]
        for root in roots:
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.json"), key=lambda item: _display_path(item)):
                try:
                    with path.open("r", encoding="utf-8", errors="replace") as handle:
                        json.load(handle)
                except (OSError, json.JSONDecodeError) as exc:
                    failures.append({"path": _display_path(_relative_or_abs(self.config.repo_root, path)), "error": str(exc)})
                    if len(failures) >= MAX_SAMPLE_ERRORS_PER_FILE:
                        return failures
        return failures

    def _jsonl_smoke_failures(self) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        roots = [self.config.phase52_sessions, self.config.failed_runs, self.config.preflight_sessions]
        for root in roots:
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.jsonl"), key=lambda item: _display_path(item)):
                result = _jsonl_smoke_parse(path)
                if result["parse_error_count"] > 0:
                    failures.append(
                        {
                            "path": _display_path(_relative_or_abs(self.config.repo_root, path)),
                            "parse_error_count": result["parse_error_count"],
                            "sample_errors": result["sample_errors"],
                        }
                    )
                    if len(failures) >= MAX_SAMPLE_ERRORS_PER_FILE:
                        return failures
        return failures


class SessionLineageBuilder:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.classifier = PathClassifier(
            repo_root=config.repo_root,
            phase52_sessions=config.phase52_sessions,
            failed_runs=config.failed_runs,
            preflight_sessions=config.preflight_sessions,
            phase52f_artifacts=config.phase52f_artifacts,
            backup_meta=config.backup_meta,
            output_root=config.output_root,
        )

    def build(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        rows.extend(self._phase52_session_rows())
        rows.extend(self._failed_run_rows())
        rows.extend(self._preflight_rows())
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            "session_count": len(rows),
            "sessions": rows,
        }

    def _phase52_session_rows(self) -> list[dict[str, Any]]:
        return [self._session_row(path, self.classifier.classify(path)) for path in _session_dirs(self.config.phase52_sessions)]

    def _failed_run_rows(self) -> list[dict[str, Any]]:
        if not self.config.failed_runs.exists():
            return []
        candidates: list[Path] = []
        for path in self.config.failed_runs.rglob("*"):
            if path.is_dir() and path.name.startswith("session_"):
                candidates.append(path)
        return [self._session_row(path, FAILED_RUN_LINEAGE) for path in sorted(candidates, key=lambda item: _display_path(item))]

    def _preflight_rows(self) -> list[dict[str, Any]]:
        if not self.config.preflight_sessions.exists():
            return []
        return [
            self._session_row(path, PREFLIGHT_SESSION)
            for path in sorted(self.config.preflight_sessions.iterdir(), key=lambda item: item.name)
            if path.is_dir() and path.name.startswith("preflight_check_60s")
        ]

    def _session_row(self, session_dir: Path, session_class: str) -> dict[str, Any]:
        session_id = session_dir.name
        metadata_path = _find_first(session_dir, [f"phase_5_2_{session_id}_metadata.json", "*metadata.json"])
        quality_path = _find_first(session_dir, [f"phase_5_2_{session_id}_quality_report.json", "*quality_report.json"])
        console_log_path = _find_first(session_dir, [f"phase_5_2_{session_id}_console.log", "*console.log"])
        sha256_path = _find_first(session_dir, [f"phase_5_2_{session_id}_sha256.txt", "*sha256.txt"])
        capture_bundle_path = _find_first(session_dir, [f"phase_5_2_{session_id}_capture_bundle.zip", "*capture_bundle.zip"])
        hotpath_bundle_path = _find_first(session_dir, ["phase_4_2h_hotpath_environment_latency_bundle.zip", "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip"])
        dataset_dir = session_dir / "data" / "dataset"
        debug_dir = session_dir / "data" / "debug"
        reports_dir = session_dir / "data" / "reports"
        hotpath_report_path = reports_dir / "phase_4_2h_hotpath_environment_latency_report.json"
        quality = _read_json(quality_path)
        hotpath = _read_json(hotpath_report_path)
        metadata = _read_json(metadata_path)

        failures: list[str] = []
        if session_class in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}:
            if metadata_path is None:
                failures.append("missing_metadata")
            if quality_path is None:
                failures.append("missing_quality_report")
            if not dataset_dir.exists():
                failures.append("missing_dataset_dir")
            if not hotpath_report_path.exists():
                failures.append("missing_hotpath_report")
        if session_class == REPAIRED_EVAL_SESSION:
            original_id = session_id.replace("_repaired_eval", "")
            if original_id == session_id:
                failures.append("missing_original_session_id_for_repaired_eval")
        lineage_status = "complete" if not failures else "partial"
        if session_class in {FAILED_RUN_LINEAGE, PREFLIGHT_SESSION}:
            lineage_status = "not_research_scope"
        return {
            "session_id": session_id,
            "session_class": session_class,
            "path": _display_path(_relative_or_abs(self.config.repo_root, session_dir)),
            "metadata_path": _display_optional(self.config.repo_root, metadata_path),
            "quality_report_path": _display_optional(self.config.repo_root, quality_path),
            "console_log_path": _display_optional(self.config.repo_root, console_log_path),
            "sha256_path": _display_optional(self.config.repo_root, sha256_path),
            "capture_bundle_path": _display_optional(self.config.repo_root, capture_bundle_path),
            "hotpath_bundle_path": _display_optional(self.config.repo_root, hotpath_bundle_path),
            "hotpath_report_path": _display_path(_relative_or_abs(self.config.repo_root, hotpath_report_path)) if hotpath_report_path.exists() else None,
            "dataset_dir": _display_path(_relative_or_abs(self.config.repo_root, dataset_dir)) if dataset_dir.exists() else None,
            "debug_dir": _display_path(_relative_or_abs(self.config.repo_root, debug_dir)) if debug_dir.exists() else None,
            "reports_dir": _display_path(_relative_or_abs(self.config.repo_root, reports_dir)) if reports_dir.exists() else None,
            "status_from_quality_report": quality.get("status"),
            "research_eligible_from_phase52": quality.get("research_eligible"),
            "hotpath_status": hotpath.get("status"),
            "hotpath_primary_failure": hotpath.get("primary_failure"),
            "strict_100ms_observability_ready": hotpath.get("strict_100ms_observability_ready"),
            "low_latency_ready": hotpath.get("low_latency_ready"),
            "clock_sync_status": hotpath.get("clock_sync_status") or metadata.get("clock_sync_status"),
            "evaluation_mode": hotpath.get("evaluation_mode") or _dict(hotpath.get("capture")).get("evaluation_mode"),
            "derived_artifact_mode": hotpath.get("derived_artifact_mode"),
            "rebuild_derived_artifacts": hotpath.get("rebuild_derived_artifacts"),
            "fresh_capture_performed": hotpath.get("fresh_capture_performed") or _dict(hotpath.get("capture")).get("fresh_capture_performed"),
            "skip_capture": hotpath.get("skip_capture") or _dict(hotpath.get("capture")).get("skip_capture"),
            "paired_original_session_id": session_id.replace("_repaired_eval", "") if session_class == REPAIRED_EVAL_SESSION else None,
            "lineage_status": lineage_status,
            "lineage_hard_fail_reasons": failures,
        }


class DatasetSchemaValidator:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.scanner = StreamingJsonlScanner()

    def build(self, lineages: list[dict[str, Any]]) -> dict[str, Any]:
        files: list[dict[str, Any]] = []
        for session in lineages:
            if session.get("session_class") not in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}:
                continue
            dataset_dir_text = session.get("dataset_dir")
            if not dataset_dir_text:
                continue
            dataset_dir = self.config.repo_root / str(dataset_dir_text)
            for file_name in REQUIRED_DATASET_FILES:
                path = dataset_dir / file_name
                if not path.exists():
                    continue
                result = self.validate_file(path, file_name=file_name)
                result.update({"session_id": session["session_id"], "file_name": file_name})
                files.append(result)
        status = _rollup_status(item["schema_status"] for item in files)
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            "file_count": len(files),
            "files": files,
            "dataset_schema_status": status,
            "hard_fail_reasons": ["dataset_schema_failures_detected"] if any(item["schema_status"] == "fail" for item in files) else [],
        }

    def validate_file(self, path: str | Path, *, file_name: str | None = None) -> dict[str, Any]:
        name = file_name or Path(path).name
        result = self.scanner.scan(path, row_validator=lambda row, line: _schema_errors_for_row(name, row))
        return result


class TimestampIntegrityAuditor:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config

    def build(self, lineages: list[dict[str, Any]]) -> dict[str, Any]:
        sessions: list[dict[str, Any]] = []
        for session in lineages:
            if session.get("session_class") not in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}:
                continue
            sessions.append(self.audit_session(session))
        aggregate = _sum_session_fields(
            sessions,
            [
                "monotonic_violation_count",
                "negative_latency_count",
                "future_timestamp_leak_count",
                "session_boundary_overlap_count",
            ],
        )
        status = _rollup_status(item["timestamp_status"] for item in sessions)
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            **aggregate,
            "clock_drift_status": _rollup_status(item["clock_drift_status"] for item in sessions),
            "clock_sample_quality_status": _rollup_status(item["clock_sample_quality_status"] for item in sessions),
            "horizon_100ms_timestamp_verifiable": any(item["horizon_100ms_timestamp_verifiable"] for item in sessions),
            "sample_violations": _bounded_concat(item["sample_violations"] for item in sessions),
            "timestamp_status": status,
            "sessions": sessions,
        }

    def audit_session(self, session: dict[str, Any]) -> dict[str, Any]:
        dataset_dir = self.config.repo_root / str(session.get("dataset_dir") or "")
        reports_dir = self.config.repo_root / str(session.get("reports_dir") or "")
        hotpath = _read_json(reports_dir / "phase_4_2h_hotpath_environment_latency_report.json")
        clock_summary = _dict(hotpath.get("clock_offset_summary"))
        clock_sanity = _read_json((self.config.repo_root / str(session.get("debug_dir") or "")) / "phase_4_2h_clock_sanity_report.json")
        clock_drift_valid = _first_bool(clock_summary.get("clock_offset_drift_valid"), clock_sanity.get("clock_offset_drift_valid"), _dict(hotpath.get("checks")).get("clock_offset_drift_valid"))
        clock_sample_quality_valid = _first_bool(clock_summary.get("clock_offset_sample_quality_valid"), clock_sanity.get("clock_sample_quality_valid"), _dict(hotpath.get("checks")).get("clock_offset_sample_quality_valid"))
        sample_violations: list[dict[str, Any]] = []
        monotonic_violation_count = 0
        negative_latency_count = 0
        future_timestamp_leak_count = 0
        horizon_verifiable = False
        selected_label_name = next((file_name for file_name in LABEL_DATASET_FILES if (dataset_dir / file_name).exists()), None)

        for file_name in REQUIRED_DATASET_FILES:
            if file_name in LABEL_DATASET_FILES and file_name != selected_label_name:
                continue
            path = dataset_dir / file_name
            if not path.exists() or path.suffix != ".jsonl":
                continue
            previous_receive: float | None = None
            try:
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    for line_number, raw in enumerate(handle, start=1):
                        stripped = raw.strip()
                        if not stripped:
                            continue
                        try:
                            row = json.loads(stripped)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        receive = _timestamp_ms(_first_present(row, TIMESTAMP_ALIASES["monotonic_receive_time"]))
                        if receive is not None:
                            if previous_receive is not None and receive < previous_receive:
                                monotonic_violation_count += 1
                                _append_violation(sample_violations, file_name, line_number, "monotonic_receive_time_decreased")
                            previous_receive = receive
                        exchange_ms = _timestamp_ms(_first_present(row, TIMESTAMP_ALIASES["exchange_event_time"]))
                        local_wall_ms = _timestamp_ms(_first_present(row, TIMESTAMP_ALIASES["local_receive_time"]))
                        if exchange_ms is not None and local_wall_ms is not None and local_wall_ms + 10.0 < exchange_ms:
                            negative_latency_count += 1
                            _append_violation(sample_violations, file_name, line_number, "exchange_time_after_local_receive_time")
                        leak_count, verifiable = _label_timestamp_findings(row)
                        future_timestamp_leak_count += leak_count
                        if leak_count:
                            _append_violation(sample_violations, file_name, line_number, "future_label_timestamp_leak")
                        horizon_verifiable = horizon_verifiable or verifiable
            except OSError as exc:
                _append_violation(sample_violations, file_name, None, f"read_error:{exc}")
        status_reasons: list[str] = []
        if monotonic_violation_count:
            status_reasons.append("monotonic_receive_time_violation")
        if negative_latency_count:
            status_reasons.append("negative_latency_or_mixed_clock_violation")
        if future_timestamp_leak_count:
            status_reasons.append("future_timestamp_leak_detected")
        if clock_drift_valid is False:
            status_reasons.append("clock_offset_drift_invalid")
        if clock_sample_quality_valid is False:
            status_reasons.append("clock_sample_quality_invalid")
        if not horizon_verifiable:
            status_reasons.append("horizon_100ms_timestamp_not_verifiable")
        hard_reasons = [reason for reason in status_reasons if reason != "horizon_100ms_timestamp_not_verifiable"]
        status = "fail" if hard_reasons else ("partial" if status_reasons else "pass")
        return {
            "session_id": session["session_id"],
            "session_class": session["session_class"],
            "monotonic_violation_count": monotonic_violation_count,
            "negative_latency_count": negative_latency_count,
            "future_timestamp_leak_count": future_timestamp_leak_count,
            "clock_drift_status": "pass" if clock_drift_valid is not False else "fail",
            "clock_sample_quality_status": "pass" if clock_sample_quality_valid is not False else "fail",
            "session_boundary_overlap_count": 0,
            "horizon_100ms_timestamp_verifiable": horizon_verifiable,
            "sample_violations": sample_violations,
            "timestamp_status": status,
            "hard_fail_reasons": status_reasons,
        }


class LabelCoverageAuditor:
    def __init__(self, config: Phase53Config, *, coverage_threshold: float | None = REQUIRED_100MS_VALID_RATE) -> None:
        self.config = config
        self.coverage_threshold = coverage_threshold
        self.scanner = StreamingJsonlScanner()

    def build(self, lineages: list[dict[str, Any]]) -> dict[str, Any]:
        sessions = [
            self.audit_session(session)
            for session in lineages
            if session.get("session_class") in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}
        ]
        total_observations = sum(int(item["total_observations"]) for item in sessions)
        labeled = sum(int(item["labeled_100ms_observations"]) for item in sessions)
        missing = sum(int(item["missing_100ms_label_count"]) for item in sessions)
        missing_reasons = Counter()
        horizon_distribution = Counter()
        for session in sessions:
            missing_reasons.update(session.get("missing_reason_counter") or {})
            horizon_distribution.update(session.get("horizon_distribution_ms") or {})
        status = _rollup_status(item["coverage_status"] for item in sessions)
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            "coverage_threshold_source": "app.research.reference_feed_benchmark.REQUIRED_100MS_VALID_RATE" if self.coverage_threshold is not None else "not_found",
            "coverage_threshold": self.coverage_threshold,
            "total_observations": total_observations,
            "labeled_100ms_observations": labeled,
            "missing_100ms_label_count": missing,
            "coverage_ratio": labeled / total_observations if total_observations else 0.0,
            "missing_reason_counter": dict(sorted(missing_reasons.items())),
            "horizon_distribution_ms": dict(sorted((str(key), value) for key, value in horizon_distribution.items())),
            "label_timestamp_traceable_count": sum(int(item["label_timestamp_traceable_count"]) for item in sessions),
            "label_timestamp_untraceable_count": sum(int(item["label_timestamp_untraceable_count"]) for item in sessions),
            "future_leak_count": sum(int(item["future_leak_count"]) for item in sessions),
            "research_eligible_100ms": all(item["research_eligible_100ms"] is True for item in sessions) if sessions else False,
            "coverage_status": status,
            "hard_fail_reasons": _unique([reason for item in sessions for reason in item.get("hard_fail_reasons", [])]),
            "sessions": sessions,
        }

    def audit_session(self, session: dict[str, Any]) -> dict[str, Any]:
        dataset_dir = self.config.repo_root / str(session.get("dataset_dir") or "")
        total_observations = self.scanner.count_rows(dataset_dir / "orderbook_clean_samples.jsonl")
        labeled = 0
        missing = 0
        missing_reasons: Counter[str] = Counter()
        horizon_distribution: Counter[int] = Counter()
        traceable = 0
        untraceable = 0
        future_leak = 0
        semantics_seen = False
        label_files_seen = 0

        selected_label_file = next((dataset_dir / file_name for file_name in LABEL_DATASET_FILES if (dataset_dir / file_name).exists()), None)
        if selected_label_file is not None:
            label_files_seen = 1
            try:
                with selected_label_file.open("r", encoding="utf-8", errors="replace") as handle:
                    for raw in handle:
                        stripped = raw.strip()
                        if not stripped:
                            continue
                        try:
                            row = json.loads(stripped)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        labels = _find_100ms_label_dicts(row)
                        if labels:
                            semantics_seen = True
                        row_valid = False
                        row_traceable = False
                        row_leak = False
                        reasons: list[str] = []
                        for label in labels:
                            horizon = _int_or_none(label.get("horizon_ms"))
                            if horizon is not None:
                                horizon_distribution[horizon] += 1
                            if horizon == 100 and int(_num(label.get("max_future_gap_ms"), REQUIRED_100MS_MAX_FUTURE_GAP_MS)) == 100:
                                if label.get("valid") is True:
                                    row_valid = True
                                if _label_traceable(label):
                                    row_traceable = True
                                leak_count, _verifiable = _single_label_timestamp_findings(row, label)
                                if leak_count:
                                    row_leak = True
                                reason = label.get("invalid_reason")
                                if reason:
                                    reasons.append(str(reason))
                        if row_valid:
                            labeled += 1
                        else:
                            missing += 1
                            for reason in reasons or ["no_valid_100ms_label"]:
                                missing_reasons[reason] += 1
                        if row_traceable:
                            traceable += 1
                        else:
                            untraceable += 1
                        if row_leak:
                            future_leak += 1
            except OSError:
                missing_reasons[f"{selected_label_file.name}:read_error"] += 1
        label_observations = labeled + missing
        total_observations = max(total_observations, label_observations)
        coverage_ratio = labeled / total_observations if total_observations else 0.0
        hard_reasons: list[str] = []
        warnings: list[str] = []
        if label_files_seen == 0 or not semantics_seen:
            hard_reasons.append("missing_or_unidentified_100ms_label_semantics")
        if self.coverage_threshold is None:
            hard_reasons.append("missing_explicit_100ms_coverage_threshold")
        elif coverage_ratio < self.coverage_threshold:
            hard_reasons.append("100ms_label_coverage_below_threshold")
        if future_leak:
            hard_reasons.append("future_leak_detected")
        if traceable == 0:
            hard_reasons.append("label_timestamp_untraceable")
        if session.get("strict_100ms_observability_ready") is not True:
            hard_reasons.append("strict_100ms_observability_ready_not_true")
        if self.coverage_threshold is None and hard_reasons == ["missing_explicit_100ms_coverage_threshold"]:
            status = "partial"
        elif hard_reasons:
            status = "fail"
        else:
            status = "pass"
        research_eligible_100ms: bool | str = status == "pass"
        if status == "partial":
            research_eligible_100ms = "partial"
        return {
            "session_id": session["session_id"],
            "session_class": session["session_class"],
            "total_observations": total_observations,
            "labeled_100ms_observations": labeled,
            "missing_100ms_label_count": max(0, total_observations - labeled),
            "coverage_ratio": coverage_ratio,
            "missing_reason_counter": dict(sorted(missing_reasons.items())),
            "horizon_distribution_ms": dict(sorted(horizon_distribution.items())),
            "label_timestamp_traceable_count": traceable,
            "label_timestamp_untraceable_count": untraceable,
            "future_leak_count": future_leak,
            "strict_100ms_observability_ready": session.get("strict_100ms_observability_ready"),
            "research_eligible_100ms": research_eligible_100ms,
            "coverage_status": status,
            "hard_fail_reasons": hard_reasons,
            "audit_warnings": warnings,
        }


class OrderbookReferenceAuditor:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.scanner = StreamingJsonlScanner()

    def build(self, lineages: list[dict[str, Any]]) -> dict[str, Any]:
        sessions = [
            self.audit_session(session)
            for session in lineages
            if session.get("session_class") in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}
        ]
        aggregate = _sum_session_fields(
            sessions,
            [
                "bid_ask_invalid_count",
                "bid_ask_crossed_count",
                "spread_negative_count",
                "bookticker_reference_rows",
                "trade_reference_rows",
                "aggtrade_reference_rows",
                "orderbook_label_rows",
                "stale_book_counter",
                "incomplete_book_counter",
            ],
        )
        mismatch = Counter()
        spreads: list[dict[str, Any]] = []
        for session in sessions:
            mismatch.update(session.get("mismatch_counter") or {})
            spreads.append(session.get("spread_distribution_summary") or {})
        status = _rollup_status(item["reference_consistency_status"] for item in sessions)
        hard_reasons = _unique([reason for session in sessions for reason in session.get("hard_fail_reasons", [])])
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            **aggregate,
            "spread_distribution_summary": _merge_spread_summaries(spreads),
            "mismatch_counter": dict(sorted(mismatch.items())),
            "reference_consistency_status": status,
            "hard_fail_reasons": hard_reasons,
            "sessions": sessions,
        }

    def audit_session(self, session: dict[str, Any]) -> dict[str, Any]:
        dataset_dir = self.config.repo_root / str(session.get("dataset_dir") or "")
        debug_dir = self.config.repo_root / str(session.get("debug_dir") or "")
        bid_ask_invalid = 0
        crossed = 0
        negative_spread = 0
        spread_stats = _SpreadStats()
        sample_errors: list[dict[str, Any]] = []
        for file_name in ("orderbook_clean_samples.jsonl", "bookticker_reference_quotes.jsonl"):
            path = dataset_dir / file_name
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    for line_number, raw in enumerate(handle, start=1):
                        stripped = raw.strip()
                        if not stripped:
                            continue
                        try:
                            row = json.loads(stripped)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        bid = _num_or_none(_first_present(row, BID_ALIASES))
                        ask = _num_or_none(_first_present(row, ASK_ALIASES))
                        spread = _num_or_none(_first_present(row, SPREAD_ALIASES))
                        if bid is None or ask is None:
                            bid_ask_invalid += 1
                            _append_sample(sample_errors, line_number, f"{file_name}:bid_or_ask_missing")
                            continue
                        if bid >= ask:
                            crossed += 1
                            _append_sample(sample_errors, line_number, f"{file_name}:bid_gte_ask")
                        computed_spread = ask - bid
                        spread_value = spread if spread is not None else computed_spread
                        spread_stats.add(spread_value)
                        if spread_value < 0:
                            negative_spread += 1
                            _append_sample(sample_errors, line_number, f"{file_name}:negative_spread")
            except OSError as exc:
                _append_sample(sample_errors, None, f"{file_name}:read_error:{exc}")
        reference_counts = {
            field: self.scanner.count_rows(dataset_dir / name)
            for name, field in REFERENCE_DATASET_FILES.items()
        }
        orderbook_label_rows = sum(self.scanner.count_rows(dataset_dir / name) for name in LABEL_DATASET_FILES if (dataset_dir / name).exists())
        mismatch_counter = {
            "orderbook_mismatch_cases": self.scanner.count_rows(debug_dir / "orderbook_mismatch_cases.jsonl"),
            "invalid_delta_cases": self.scanner.count_rows(debug_dir / "invalid_delta_cases.jsonl"),
            "sequence_gap_cases": self.scanner.count_rows(debug_dir / "sequence_gap_cases.jsonl"),
        }
        stale = self.scanner.count_rows(debug_dir / "stale_period_cases.jsonl")
        incomplete = self.scanner.count_rows(debug_dir / "book_incomplete_cases.jsonl")
        hard_reasons: list[str] = []
        if bid_ask_invalid:
            hard_reasons.append("bid_ask_invalid")
        if crossed:
            hard_reasons.append("bid_ask_crossed")
        if negative_spread:
            hard_reasons.append("negative_spread")
        if any(value > 0 for value in mismatch_counter.values()):
            hard_reasons.append("reference_mismatch_or_sequence_cases_present")
        if incomplete:
            hard_reasons.append("incomplete_book_cases_present")
        status = "fail" if hard_reasons else "pass"
        return {
            "session_id": session["session_id"],
            "session_class": session["session_class"],
            "bid_ask_invalid_count": bid_ask_invalid,
            "bid_ask_crossed_count": crossed,
            "spread_negative_count": negative_spread,
            "spread_distribution_summary": spread_stats.summary(),
            **reference_counts,
            "orderbook_label_rows": orderbook_label_rows,
            "mismatch_counter": mismatch_counter,
            "stale_book_counter": stale,
            "incomplete_book_counter": incomplete,
            "reference_consistency_status": status,
            "hard_fail_reasons": hard_reasons,
            "sample_errors": sample_errors,
        }


class RuntimeDataHealthAuditor:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.scanner = StreamingJsonlScanner()

    def build(self, lineages: list[dict[str, Any]]) -> dict[str, Any]:
        sessions = [
            self.audit_session(session)
            for session in lineages
            if session.get("session_class") in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}
        ]
        aggregate = _sum_session_fields(
            sessions,
            [
                "duplicate_update_cases",
                "sequence_gap_cases",
                "stale_period_cases",
                "book_incomplete_cases",
                "invalid_delta_cases",
                "orderbook_mismatch_cases",
            ],
        )
        status = _rollup_status(item["runtime_health_status"] for item in sessions)
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            **aggregate,
            "queue_backpressure_status": _rollup_status(item["queue_backpressure_status"] for item in sessions),
            "writer_batch_status": _rollup_status(item["writer_batch_status"] for item in sessions),
            "ws_lifecycle_status": _rollup_status(item["ws_lifecycle_status"] for item in sessions),
            "capture_gap_status": _rollup_status(item["capture_gap_status"] for item in sessions),
            "stopped_early_status": _rollup_status(item["stopped_early_status"] for item in sessions),
            "runtime_health_status": status,
            "hard_fail_reasons": _unique([reason for session in sessions for reason in session.get("hard_fail_reasons", [])]),
            "sessions": sessions,
        }

    def audit_session(self, session: dict[str, Any]) -> dict[str, Any]:
        debug_dir = self.config.repo_root / str(session.get("debug_dir") or "")
        reports_dir = self.config.repo_root / str(session.get("reports_dir") or "")
        counts = {
            field: self.scanner.count_rows(debug_dir / file_name)
            for file_name, field in DEBUG_CASE_FILES.items()
        }
        queue = _read_json(debug_dir / "phase_4_2h_queue_backpressure_report.json")
        writer = _read_json(debug_dir / "phase_4_2h_writer_batch_report.json")
        lifecycle = _read_json(debug_dir / "ws_lifecycle_report.json")
        hotpath = _read_json(reports_dir / "phase_4_2h_hotpath_environment_latency_report.json")
        capture = _dict(hotpath.get("capture"))
        diagnostics = _dict(capture.get("capture_diagnostics"))
        queue_status = "pass"
        if queue.get("queue_backpressure_detected") is True or int(_num(queue.get("queue_dropped_messages"))) > 0:
            queue_status = "fail"
        writer_status = "pass"
        writer_payload = writer or _dict(hotpath.get("writer_batch_report")) or _dict(diagnostics.get("reference_writer_batch_report"))
        if int(_num(writer_payload.get("writer_dropped_records"))) > 0 or int(_num(writer_payload.get("writer_error_count"))) > 0:
            writer_status = "fail"
        if writer_payload.get("writer_shutdown_flush_completed") is False:
            writer_status = "fail"
        lifecycle_status = "pass"
        if int(_num(lifecycle.get("sequence_gap_count"))) > 0 or int(_num(lifecycle.get("queue_dropped_messages"))) > 0:
            lifecycle_status = "fail"
        capture_gap_status = "pass"
        if diagnostics:
            parse_errors = sum(int(_num(value)) for value in _dict(diagnostics.get("parse_error_count_by_source")).values())
            if parse_errors:
                capture_gap_status = "fail"
        elif not capture and session.get("session_class") in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}:
            capture_gap_status = "partial"
        stopped_early_status = "fail" if hotpath.get("primary_failure") else "pass"
        hard_reasons: list[str] = []
        if any(value > 0 for value in counts.values()):
            hard_reasons.append("runtime_debug_case_files_nonempty")
        if queue_status == "fail":
            hard_reasons.append("queue_backpressure_or_drops_detected")
        if writer_status == "fail":
            hard_reasons.append("writer_batch_failure_detected")
        if lifecycle_status == "fail":
            hard_reasons.append("ws_lifecycle_failure_detected")
        if capture_gap_status == "fail":
            hard_reasons.append("capture_parse_or_gap_failure_detected")
        if stopped_early_status == "fail":
            hard_reasons.append("session_primary_failure_present")
        if hard_reasons:
            runtime_status = "fail"
        elif "partial" in {capture_gap_status}:
            runtime_status = "partial"
        else:
            runtime_status = "pass"
        return {
            "session_id": session["session_id"],
            "session_class": session["session_class"],
            **counts,
            "queue_backpressure_status": queue_status,
            "writer_batch_status": writer_status,
            "ws_lifecycle_status": lifecycle_status,
            "capture_gap_status": capture_gap_status,
            "stopped_early_status": stopped_early_status,
            "runtime_health_status": runtime_status,
            "hard_fail_reasons": hard_reasons,
        }


class ResearchEligibilityClassifier:
    def classify(
        self,
        *,
        lineages: list[dict[str, Any]],
        artifact_report: dict[str, Any],
        schema_report: dict[str, Any],
        timestamp_report: dict[str, Any],
        coverage_report: dict[str, Any],
        orderbook_report: dict[str, Any],
        runtime_report: dict[str, Any],
    ) -> dict[str, Any]:
        schema_by_session = _session_status_map(schema_report, "dataset_schema_status", nested_key="files", status_key="schema_status")
        timestamp_by_session = {item["session_id"]: item for item in timestamp_report.get("sessions", [])}
        coverage_by_session = {item["session_id"]: item for item in coverage_report.get("sessions", [])}
        orderbook_by_session = {item["session_id"]: item for item in orderbook_report.get("sessions", [])}
        runtime_by_session = {item["session_id"]: item for item in runtime_report.get("sessions", [])}
        global_artifact_block = artifact_report.get("integrity_status") == "fail"
        rows: list[dict[str, Any]] = []
        for session in lineages:
            rows.append(
                self._classify_session(
                    session,
                    schema_status=schema_by_session.get(session["session_id"], "pass"),
                    timestamp=timestamp_by_session.get(session["session_id"], {}),
                    coverage=coverage_by_session.get(session["session_id"], {}),
                    orderbook=orderbook_by_session.get(session["session_id"], {}),
                    runtime=runtime_by_session.get(session["session_id"], {}),
                    global_artifact_block=global_artifact_block,
                )
            )
        return {
            "phase": PHASE,
            "created_at_utc": _utc_now(),
            "sessions": rows,
            "allowed_phase54_sessions": [row["session_id"] for row in rows if row["allowed_for_phase54"] is True],
            "excluded_sessions": [
                {"session_id": row["session_id"], "reasons": row["blocking_reasons"] or row["reasons"]}
                for row in rows
                if row["allowed_for_phase54"] is not True
            ],
            "research_eligibility_status": self._overall_status(rows),
        }

    def _classify_session(
        self,
        session: dict[str, Any],
        *,
        schema_status: str,
        timestamp: dict[str, Any],
        coverage: dict[str, Any],
        orderbook: dict[str, Any],
        runtime: dict[str, Any],
        global_artifact_block: bool,
    ) -> dict[str, Any]:
        session_class = str(session.get("session_class"))
        reasons: list[str] = []
        blocking: list[str] = []
        warnings: list[str] = []
        followup: list[str] = []
        canonical_candidate = session_class in {PRIMARY_PHASE52_SESSION, REPAIRED_EVAL_SESSION}
        if session_class == PREFLIGHT_SESSION:
            return _eligibility_row(session, False, False, False, ["preflight_only"], ["preflight_only"], [], [])
        if session_class == FAILED_RUN_LINEAGE:
            return _eligibility_row(session, False, False, False, ["failed_run_lineage_only"], ["failed_run_lineage_only"], [], [])
        if session_class == ROOT_LEGACY_DATASET:
            return _eligibility_row(session, False, False, False, ["legacy_or_unscoped_root_artifact"], ["legacy_or_unscoped_root_artifact"], [], [])
        if not canonical_candidate:
            return _eligibility_row(session, False, False, False, ["unknown_or_not_research_scope"], ["unknown_or_not_research_scope"], [], [])

        if global_artifact_block:
            blocking.append("global_artifact_integrity_failure")
        if session.get("lineage_status") not in {"complete"}:
            blocking.extend(session.get("lineage_hard_fail_reasons") or ["lineage_not_complete"])
        hotpath_status = session.get("hotpath_status")
        if hotpath_status != "pass":
            blocking.append("hotpath_status_not_pass")
        if session.get("strict_100ms_observability_ready") is not True:
            blocking.append("strict_100ms_observability_ready_not_true")
        if session.get("low_latency_ready") is not True:
            blocking.append("low_latency_ready_not_true")
        if schema_status == "fail":
            blocking.append("dataset_schema_fail")
        elif schema_status == "partial":
            warnings.append("dataset_schema_partial")
        if timestamp.get("timestamp_status") == "fail":
            blocking.extend(timestamp.get("hard_fail_reasons") or ["timestamp_integrity_fail"])
        elif timestamp.get("timestamp_status") == "partial":
            warnings.append("timestamp_integrity_partial")
        coverage_status = coverage.get("coverage_status")
        if coverage_status == "fail":
            blocking.extend(coverage.get("hard_fail_reasons") or ["100ms_coverage_fail"])
        elif coverage_status == "partial":
            warnings.extend(coverage.get("hard_fail_reasons") or ["100ms_coverage_partial"])
            followup.append("resolve_100ms_label_coverage_partial")
        if orderbook.get("reference_consistency_status") == "fail":
            blocking.extend(orderbook.get("hard_fail_reasons") or ["orderbook_reference_consistency_fail"])
        elif orderbook.get("reference_consistency_status") == "partial":
            warnings.append("orderbook_reference_consistency_partial")
        if runtime.get("runtime_health_status") == "fail":
            blocking.extend(runtime.get("hard_fail_reasons") or ["runtime_data_health_fail"])
        elif runtime.get("runtime_health_status") == "partial":
            warnings.append("runtime_data_health_partial")

        quality_status = session.get("status_from_quality_report")
        quality_eligible = session.get("research_eligible_from_phase52")
        if session_class == PRIMARY_PHASE52_SESSION:
            if quality_status != "pass":
                blocking.append("phase52_quality_status_not_pass")
            if quality_eligible is not True:
                blocking.append("phase52_research_eligible_not_true")
        else:
            original_id = session.get("paired_original_session_id")
            reasons.append(f"repaired_eval_explicit_lineage_from_{original_id}")
            if session.get("evaluation_mode") != "existing_artifacts":
                blocking.append("repaired_eval_missing_existing_artifacts_mode")
            if session.get("derived_artifact_mode") != "reuse_existing":
                blocking.append("repaired_eval_missing_reuse_existing_mode")
            if session.get("rebuild_derived_artifacts") is not False:
                blocking.append("repaired_eval_rebuild_derived_artifacts_not_false")
            if quality_status != "pass" or quality_eligible is not True:
                warnings.append("phase52_quality_report_not_eligible_but_repaired_eval_hotpath_pass_used")
        blocking = _unique(blocking)
        warnings = _unique(warnings)
        if blocking:
            eligible: bool | str = False
            allowed = False
        elif warnings or followup:
            eligible = "partial"
            allowed = False
        else:
            eligible = True
            allowed = True
        return _eligibility_row(session, eligible, canonical_candidate, allowed, _unique(reasons), blocking, warnings, _unique(followup))

    def _overall_status(self, rows: list[dict[str, Any]]) -> str:
        if any(row["allowed_for_phase54"] is True for row in rows):
            if any(row["research_eligible"] == "partial" for row in rows):
                return "partial"
            return "pass"
        if any(row["research_eligible"] == "partial" for row in rows):
            return "partial"
        return "fail"


class ResearchReadinessManifestBuilder:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config

    def build(
        self,
        *,
        inventory_report: dict[str, Any],
        artifact_report: dict[str, Any],
        lineage_report: dict[str, Any],
        schema_report: dict[str, Any],
        timestamp_report: dict[str, Any],
        coverage_report: dict[str, Any],
        orderbook_report: dict[str, Any],
        runtime_report: dict[str, Any],
        eligibility_report: dict[str, Any],
        report_paths: dict[str, str],
    ) -> dict[str, Any]:
        allowed = list(eligibility_report.get("allowed_phase54_sessions") or [])
        excluded = list(eligibility_report.get("excluded_sessions") or [])
        hard_fail_reasons = _unique(
            [
                *artifact_report.get("hard_fail_reasons", []),
                *schema_report.get("hard_fail_reasons", []),
                *coverage_report.get("hard_fail_reasons", []),
                *timestamp_report.get("hard_fail_reasons", []),
                *orderbook_report.get("hard_fail_reasons", []),
                *runtime_report.get("hard_fail_reasons", []),
            ]
        )
        warnings = _unique(
            [
                *artifact_report.get("warnings", []),
                *[f"{item.get('session_id')}:{warning}" for item in eligibility_report.get("sessions", []) for warning in item.get("audit_warnings", [])],
            ]
        )
        final_status, research_ready = self._final_status(allowed, excluded, hard_fail_reasons, eligibility_report)
        return {
            "phase": PHASE,
            "name": PHASE_NAME,
            "created_at_utc": _utc_now(),
            "repo_root": _display_path(self.config.repo_root),
            "input_roots": self.config.input_roots(),
            "final_status": final_status,
            "research_ready": research_ready,
            "allowed_phase54_sessions": allowed,
            "excluded_sessions": excluded,
            "hard_fail_reasons": hard_fail_reasons,
            "warnings": warnings,
            "report_paths": report_paths,
            "evidence_paths": {},
            "source_backup_lineage": {
                "backup_meta": self.config.input_roots()["backup_meta"],
                "backup_meta_sha256s_present": artifact_report.get("backup_meta_sha256s_present"),
                "internal_sha256s_checked": artifact_report.get("internal_sha256s_checked"),
            },
            "artifact_integrity_summary": _pick(artifact_report, ["integrity_status", "internal_sha256s_failed", "per_session_sha256_files_checked", "zip_files_tested"]),
            "session_lineage_summary": {
                "session_count": lineage_report.get("session_count"),
                "complete_count": sum(1 for item in lineage_report.get("sessions", []) if item.get("lineage_status") == "complete"),
            },
            "dataset_schema_summary": _pick(schema_report, ["file_count", "dataset_schema_status", "hard_fail_reasons"]),
            "timestamp_integrity_summary": _pick(timestamp_report, ["timestamp_status", "future_timestamp_leak_count", "negative_latency_count", "horizon_100ms_timestamp_verifiable"]),
            "coverage_100ms_summary": _pick(coverage_report, ["coverage_status", "coverage_ratio", "coverage_threshold", "coverage_threshold_source", "future_leak_count"]),
            "orderbook_reference_summary": _pick(orderbook_report, ["reference_consistency_status", "bid_ask_crossed_count", "spread_negative_count", "mismatch_counter"]),
            "runtime_data_health_summary": _pick(runtime_report, ["runtime_health_status", "queue_backpressure_status", "writer_batch_status", "ws_lifecycle_status"]),
        }

    def _final_status(
        self,
        allowed: list[str],
        excluded: list[dict[str, Any]],
        hard_fail_reasons: list[str],
        eligibility_report: dict[str, Any],
    ) -> tuple[str, bool | str]:
        if not allowed:
            return "phase_5_3_fail", False
        if hard_fail_reasons:
            return "phase_5_3_partial", "partial"
        primary_excluded = [
            item
            for item in eligibility_report.get("sessions", [])
            if item.get("canonical_candidate") is True and item.get("allowed_for_phase54") is not True
        ]
        if primary_excluded or excluded:
            return "phase_5_3_partial", "partial"
        return "phase_5_3_pass", True


class EvidenceBundleBuilder:
    def __init__(self, config: Phase53Config) -> None:
        self.config = config
        self.verifier = Sha256Verifier()

    def build(self) -> dict[str, str]:
        bundle_path = self.config.evidence_dir / FINAL_EVIDENCE_ZIP
        sha_path = self.config.evidence_dir / f"{FINAL_EVIDENCE_ZIP}.sha256"
        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for root in (self.config.reports_dir, self.config.manifests_dir, self.config.debug_dir):
                if not root.exists():
                    continue
                for path in sorted(root.rglob("*"), key=lambda item: _display_path(item)):
                    if path.is_file():
                        archive.write(path, arcname=_display_path(path.relative_to(self.config.repo_root)))
            for relative in (
                Path("meta/git_snapshot.txt"),
                Path("SHA256SUMS.txt"),
            ):
                path = self.config.backup_meta / relative
                if path.exists() and path.is_file():
                    archive.write(path, arcname=_display_path(path.relative_to(self.config.repo_root)))
            if self.config.phase52f_artifacts.exists():
                for path in sorted(self.config.phase52f_artifacts.glob("*.sha256"), key=lambda item: item.name):
                    archive.write(path, arcname=_display_path(path.relative_to(self.config.repo_root)))
        digest = self.verifier.compute_sha256(bundle_path)
        sha_path.write_text(f"{digest}  {bundle_path.name}\n", encoding="utf-8")
        return {
            "final_evidence_bundle": _display_path(_relative_or_abs(self.config.repo_root, bundle_path)),
            "final_evidence_bundle_sha256_file": _display_path(_relative_or_abs(self.config.repo_root, sha_path)),
            "final_evidence_bundle_sha256": digest,
        }


def run_phase53_dataset_integrity_audit(config: Phase53Config) -> dict[str, Any]:
    ensure_phase53_output_dirs(config)
    inventory = ArtifactInventoryBuilder(config).build()
    lineage = SessionLineageBuilder(config).build()
    artifact = ArtifactIntegrityAuditor(config, lineage.get("sessions", [])).build()
    schema = DatasetSchemaValidator(config).build(lineage.get("sessions", []))
    timestamp = TimestampIntegrityAuditor(config).build(lineage.get("sessions", []))
    coverage = LabelCoverageAuditor(config).build(lineage.get("sessions", []))
    orderbook = OrderbookReferenceAuditor(config).build(lineage.get("sessions", []))
    runtime = RuntimeDataHealthAuditor(config).build(lineage.get("sessions", []))
    eligibility = ResearchEligibilityClassifier().classify(
        lineages=lineage.get("sessions", []),
        artifact_report=artifact,
        schema_report=schema,
        timestamp_report=timestamp,
        coverage_report=coverage,
        orderbook_report=orderbook,
        runtime_report=runtime,
    )

    reports = {
        "phase_5_3_backup_restore_inventory_report": inventory,
        "phase_5_3_artifact_integrity_report": artifact,
        "phase_5_3_session_lineage_report": lineage,
        "phase_5_3_dataset_schema_report": schema,
        "phase_5_3_timestamp_integrity_report": timestamp,
        "phase_5_3_100ms_label_coverage_report": coverage,
        "phase_5_3_orderbook_reference_consistency_report": orderbook,
        "phase_5_3_runtime_data_health_report": runtime,
        "phase_5_3_research_eligibility_report": eligibility,
    }
    report_paths: dict[str, str] = {}
    for name, payload in reports.items():
        json_path = config.reports_dir / f"{name}.json"
        md_path = config.reports_dir / f"{name}.md"
        _write_json(json_path, payload)
        _write_text(md_path, render_markdown_report(name, payload))
        report_paths[f"{name}.json"] = _display_path(_relative_or_abs(config.repo_root, json_path))
        report_paths[f"{name}.md"] = _display_path(_relative_or_abs(config.repo_root, md_path))

    manifest = ResearchReadinessManifestBuilder(config).build(
        inventory_report=inventory,
        artifact_report=artifact,
        lineage_report=lineage,
        schema_report=schema,
        timestamp_report=timestamp,
        coverage_report=coverage,
        orderbook_report=orderbook,
        runtime_report=runtime,
        eligibility_report=eligibility,
        report_paths=report_paths,
    )
    manifest_json = config.manifests_dir / f"{FINAL_MANIFEST_NAME}.json"
    manifest_md = config.reports_dir / f"{FINAL_MANIFEST_NAME}.md"
    _write_json(manifest_json, manifest)
    _write_text(manifest_md, render_manifest_markdown(manifest))
    report_paths[f"{FINAL_MANIFEST_NAME}.json"] = _display_path(_relative_or_abs(config.repo_root, manifest_json))
    report_paths[f"{FINAL_MANIFEST_NAME}.md"] = _display_path(_relative_or_abs(config.repo_root, manifest_md))

    evidence_paths = EvidenceBundleBuilder(config).build()
    manifest["report_paths"] = report_paths
    manifest["evidence_paths"] = evidence_paths
    _write_json(manifest_json, manifest)
    _write_text(manifest_md, render_manifest_markdown(manifest))
    return manifest


def render_markdown_report(name: str, report: dict[str, Any]) -> str:
    title = name.replace("_", " ").title()
    lines = [f"# {title}", ""]
    for key in ("phase", "created_at_utc", "integrity_status", "dataset_schema_status", "timestamp_status", "coverage_status", "reference_consistency_status", "runtime_health_status", "research_eligibility_status"):
        if key in report:
            lines.append(f"- {key}: `{report.get(key)}`")
    if "hard_fail_reasons" in report:
        lines.extend(["", "## Hard Fail Reasons"])
        reasons = list(report.get("hard_fail_reasons") or [])
        lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- none")
    if "warnings" in report:
        lines.extend(["", "## Warnings"])
        warnings = list(report.get("warnings") or [])
        lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- none")
    if "sessions" in report:
        lines.extend(["", "## Sessions"])
        for session in report.get("sessions", [])[:200]:
            summary = session.get("research_eligible", session.get("timestamp_status", session.get("coverage_status", session.get("runtime_health_status", session.get("lineage_status")))))
            lines.append(f"- {session.get('session_id')}: `{summary}`")
    return "\n".join(lines) + "\n"


def render_manifest_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Phase 5.3 Research Readiness Manifest",
        "",
        f"Final status: `{manifest.get('final_status')}`",
        f"Research ready: `{manifest.get('research_ready')}`",
        "",
        "## Allowed Phase 5.4 Sessions",
    ]
    allowed = list(manifest.get("allowed_phase54_sessions") or [])
    lines.extend(f"- {session}" for session in allowed) if allowed else lines.append("- none")
    lines.extend(["", "## Excluded Sessions"])
    excluded = list(manifest.get("excluded_sessions") or [])
    if excluded:
        for item in excluded:
            lines.append(f"- {item.get('session_id')}: {', '.join(item.get('reasons') or [])}")
    else:
        lines.append("- none")
    lines.extend(["", "## Hard Fail Reasons"])
    hard = list(manifest.get("hard_fail_reasons") or [])
    lines.extend(f"- {reason}" for reason in hard) if hard else lines.append("- none")
    lines.extend(["", "## Evidence"])
    evidence = _dict(manifest.get("evidence_paths"))
    for key, value in evidence.items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


BID_ALIASES = ("best_bid", "bid", "reported_best_bid", "feature_best_bid")
ASK_ALIASES = ("best_ask", "ask", "reported_best_ask", "feature_best_ask")
SPREAD_ALIASES = ("spread", "spread_ticks", "spread_bps", "feature_spread_bps")
SYMBOL_ALIASES = ("symbol", "market_id", "token", "token_id")
PRICE_ALIASES = ("price", "trade_price", "mid", "mid_price", "future_price", "future_mid", "feature_mid_price")
QUANTITY_ALIASES = ("quantity", "qty", "size", "best_bid_qty", "best_ask_qty")

TIMESTAMP_ALIASES = {
    "exchange_event_time": ("exchange_event_time", "exchange_event_ts", "exchange_ts", "event_ts", "feature_exchange_ts_ms"),
    "local_receive_time": ("local_receive_time", "local_recv_wall_ts", "receive_ts", "received_at"),
    "corrected_receive_time": ("corrected_receive_time", "corrected_receive_ts", "corrected_feature_receive_lag_ms"),
    "monotonic_receive_time": ("monotonic_receive_time", "local_recv_monotonic_ns", "local_receive_ts", "raw_ws_callback_monotonic_ns", "ws_message_received_monotonic_ns"),
    "label_time": ("future_reference_local_recv_monotonic_ns", "future_reference_exchange_ts_ms", "target_exchange_ts_ms", "target_local_recv_monotonic_ns", "horizon_time", "label_time"),
    "horizon_time": ("target_exchange_ts_ms", "target_local_recv_monotonic_ns", "horizon_time"),
}


def _schema_errors_for_row(file_name: str, row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    is_orderbook = "orderbook" in file_name or "corrected_time_protocol" in file_name
    is_reference = file_name in REFERENCE_DATASET_FILES
    is_latency = "latency_profile" in file_name
    if is_orderbook:
        if _first_present(row, SYMBOL_ALIASES) is None:
            errors.append("required:missing_symbol_or_market_id")
        bid = _num_or_none(_first_present(row, BID_ALIASES))
        ask = _num_or_none(_first_present(row, ASK_ALIASES))
        if bid is None:
            errors.append("required:missing_best_bid")
        if ask is None:
            errors.append("required:missing_best_ask")
        if bid is not None and ask is not None and bid >= ask:
            errors.append("numeric:bid_gte_ask")
        spread = _num_or_none(_first_present(row, SPREAD_ALIASES))
        if spread is not None and spread < 0:
            errors.append("numeric:negative_spread")
        if _first_present(row, TIMESTAMP_ALIASES["monotonic_receive_time"]) is None and _first_present(row, TIMESTAMP_ALIASES["local_receive_time"]) is None:
            errors.append("required:missing_receive_timestamp")
        if any(label_name in file_name for label_name in ("labels", "benchmark")):
            if not _find_100ms_label_dicts(row):
                errors.append("required:missing_100ms_label_semantics")
    if is_reference:
        if _first_present(row, SYMBOL_ALIASES) is None:
            errors.append("required:missing_symbol_or_market_id")
        price = _num_or_none(_first_present(row, PRICE_ALIASES))
        if price is None and any(alias in row for alias in PRICE_ALIASES):
            errors.append("type:price_not_numeric")
        bid = _num_or_none(_first_present(row, BID_ALIASES))
        ask = _num_or_none(_first_present(row, ASK_ALIASES))
        if bid is not None and ask is not None and bid >= ask:
            errors.append("numeric:reference_bid_gte_ask")
        for alias in QUANTITY_ALIASES:
            if alias in row:
                value = _num_or_none(row.get(alias))
                if value is None:
                    errors.append(f"type:{alias}_not_numeric")
                elif value < 0:
                    errors.append(f"numeric:{alias}_negative")
    if is_latency:
        if not row:
            errors.append("required:empty_latency_row")
    return errors


def _find_100ms_label_dicts(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(item: Any, key_hint: str = "") -> None:
        if isinstance(item, dict):
            horizon = _int_or_none(item.get("horizon_ms"))
            max_gap = _int_or_none(item.get("max_future_gap_ms"))
            if horizon == 100 or "100ms" in key_hint or max_gap == 100:
                if any(key in item for key in ("valid", "invalid_reason", "future_reference_local_recv_monotonic_ns", "future_reference_exchange_ts_ms", "target_exchange_ts_ms", "reference_source")):
                    found.append(item)
            for key, child in item.items():
                walk(child, str(key))
        elif isinstance(item, list):
            for child in item:
                walk(child, key_hint)

    walk(value)
    return found


def _label_timestamp_findings(row: dict[str, Any]) -> tuple[int, bool]:
    leak_count = 0
    verifiable = False
    for label in _find_100ms_label_dicts(row):
        leaks, label_verifiable = _single_label_timestamp_findings(row, label)
        leak_count += leaks
        verifiable = verifiable or label_verifiable
    return leak_count, verifiable


def _single_label_timestamp_findings(row: dict[str, Any], label: dict[str, Any]) -> tuple[int, bool]:
    leak_count = 0
    feature_receive = _timestamp_ms(row.get("local_recv_monotonic_ns") or label.get("feature_local_recv_monotonic_ns"))
    future_receive = _timestamp_ms(label.get("future_reference_local_recv_monotonic_ns") or label.get("target_local_recv_monotonic_ns"))
    feature_exchange = _timestamp_ms(row.get("feature_exchange_ts_ms") or row.get("exchange_event_ts") or label.get("feature_exchange_ts_ms"))
    future_exchange = _timestamp_ms(label.get("future_reference_exchange_ts_ms") or label.get("target_exchange_ts_ms"))
    verifiable = bool(future_receive is not None or future_exchange is not None)
    if feature_receive is not None and future_receive is not None and future_receive < feature_receive:
        leak_count += 1
    if feature_exchange is not None and future_exchange is not None and future_exchange < feature_exchange:
        leak_count += 1
    return leak_count, verifiable


def _label_traceable(label: dict[str, Any]) -> bool:
    return any(label.get(key) is not None for key in ("future_reference_local_recv_monotonic_ns", "future_reference_exchange_ts_ms", "target_exchange_ts_ms", "target_local_recv_monotonic_ns", "future_reference_price"))


@dataclass
class _SpreadStats:
    count: int = 0
    min: float | None = None
    max: float | None = None
    total: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)

    def summary(self) -> dict[str, Any]:
        return {"count": self.count, "min": self.min, "max": self.max, "mean": self.total / self.count if self.count else None}


def _merge_spread_summaries(items: list[dict[str, Any]]) -> dict[str, Any]:
    count = sum(int(_num(item.get("count"))) for item in items)
    totals = [float(item["mean"]) * int(item["count"]) for item in items if item.get("mean") is not None and item.get("count")]
    mins = [float(item["min"]) for item in items if item.get("min") is not None]
    maxs = [float(item["max"]) for item in items if item.get("max") is not None]
    return {
        "count": count,
        "min": min(mins) if mins else None,
        "max": max(maxs) if maxs else None,
        "mean": sum(totals) / count if count else None,
    }


def _eligibility_row(
    session: dict[str, Any],
    research_eligible: bool | str,
    canonical_candidate: bool,
    allowed: bool,
    reasons: list[str],
    blocking: list[str],
    warnings: list[str],
    followup: list[str],
) -> dict[str, Any]:
    return {
        "session_id": session.get("session_id"),
        "session_class": session.get("session_class"),
        "research_eligible": research_eligible,
        "canonical_candidate": canonical_candidate,
        "allowed_for_phase54": allowed,
        "reasons": reasons,
        "blocking_reasons": blocking,
        "audit_warnings": warnings,
        "required_followup": followup,
    }


def _session_status_map(report: dict[str, Any], default_key: str, *, nested_key: str, status_key: str) -> dict[str, str]:
    by_session: dict[str, list[str]] = {}
    for item in report.get(nested_key, []):
        session_id = item.get("session_id")
        if session_id:
            by_session.setdefault(str(session_id), []).append(str(item.get(status_key, "pass")))
    return {session: _rollup_status(statuses) for session, statuses in by_session.items()} or {}


def _rollup_status(statuses: Iterable[Any]) -> str:
    values = [str(status) for status in statuses if status is not None]
    if not values:
        return "partial"
    if any(status == "fail" for status in values):
        return "fail"
    if any(status == "partial" for status in values):
        return "partial"
    return "pass"


def _sum_session_fields(sessions: list[dict[str, Any]], fields: list[str]) -> dict[str, int]:
    return {field: sum(int(_num(session.get(field))) for session in sessions) for field in fields}


def _bounded_concat(items: Iterable[list[Any]]) -> list[Any]:
    result: list[Any] = []
    for item in items:
        for value in item:
            if len(result) >= MAX_SAMPLE_ERRORS_PER_FILE:
                return result
            result.append(value)
    return result


def _append_sample(samples: list[dict[str, Any]], line_number: int | None, error: str) -> None:
    if len(samples) < MAX_SAMPLE_ERRORS_PER_FILE:
        samples.append({"line": line_number, "error": error})


def _append_violation(samples: list[dict[str, Any]], file_name: str, line_number: int | None, reason: str) -> None:
    if len(samples) < MAX_SAMPLE_ERRORS_PER_FILE:
        samples.append({"file_name": file_name, "line": line_number, "reason": reason})


def _candidate_target_from_sha_path(path: Path) -> Path:
    stem = path.name
    for suffix in (".zip.sha256", ".sha256", "_sha256.txt", ".txt"):
        if stem.endswith(suffix):
            return path.with_name(stem[: -len(suffix)])
    return path.with_suffix("")


def _parse_sha256sum_line(line: str) -> tuple[str, Path] | None:
    match = re.match(r"^\s*(?P<sha>[a-fA-F0-9]{64})\s+\*?(?P<path>.+?)\s*$", line)
    if not match:
        return None
    relative = match.group("path").strip().lstrip("./\\")
    return match.group("sha").lower(), Path(relative)


def _jsonl_smoke_parse(path: Path, *, max_rows: int = 1000) -> dict[str, Any]:
    parse_error_count = 0
    sample_errors: list[dict[str, Any]] = []
    row_count = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw in enumerate(handle, start=1):
                stripped = raw.strip()
                if not stripped:
                    continue
                try:
                    value = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    parse_error_count += 1
                    _append_sample(sample_errors, line_number, f"json_parse_error:{exc.msg}")
                    continue
                if not isinstance(value, dict):
                    parse_error_count += 1
                    _append_sample(sample_errors, line_number, "json_row_not_object")
                row_count += 1
                if row_count >= max_rows:
                    break
    except OSError as exc:
        parse_error_count += 1
        _append_sample(sample_errors, None, f"read_error:{exc}")
    return {"parse_error_count": parse_error_count, "sample_errors": sample_errors}


def _event_time(row: dict[str, Any]) -> Any:
    for aliases in TIMESTAMP_ALIASES.values():
        value = _first_present(row, aliases)
        if value is not None:
            return value
    return None


def _timestamp_ms(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp() * 1000.0
        except ValueError:
            number = _num_or_none(value)
            if number is None:
                return None
            return _numeric_timestamp_to_ms(number)
    number = _num_or_none(value)
    if number is None:
        return None
    return _numeric_timestamp_to_ms(number)


def _numeric_timestamp_to_ms(value: float) -> float:
    absolute = abs(value)
    if absolute > 1e17:
        return value / 1_000_000.0
    if absolute > 1e14:
        return value / 1_000.0
    return value


def _first_present(row: dict[str, Any], aliases: Iterable[str]) -> Any:
    for alias in aliases:
        if alias in row and row.get(alias) is not None:
            return row.get(alias)
    return None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None


def _num_or_none(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result or result in {float("inf"), float("-inf")}:
        return None
    return result


def _num(*values: Any) -> float:
    for value in values:
        number = _num_or_none(value)
        if number is not None:
            return number
    return 0.0


def _int_or_none(value: Any) -> int | None:
    number = _num_or_none(value)
    return int(number) if number is not None else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _pick(mapping: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: mapping.get(key) for key in keys}


def _unique(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    target = Path(path)
    if not target.exists() or not target.is_file():
        return {}
    try:
        with target.open("r", encoding="utf-8", errors="replace") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: str | Path, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _iter_files_and_dirs(root: Path) -> Iterable[Path]:
    if root.is_file():
        yield root
        return
    for path in sorted(root.rglob("*"), key=lambda item: _display_path(item)):
        yield path


def _session_dirs(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted([path for path in root.iterdir() if path.is_dir()], key=lambda item: item.name)


def _looks_like_session_dir(path: Path) -> bool:
    return (
        path.name.startswith("session_")
        or (path / "data" / "dataset").exists()
        or any(path.glob("*quality_report.json"))
        or any(path.glob("*metadata.json"))
    )


def _find_first(root: Path, patterns: list[str]) -> Path | None:
    for pattern in patterns:
        direct = root / pattern
        if "*" not in pattern and direct.exists():
            return direct
        matches = sorted(root.glob(pattern), key=lambda item: item.name)
        if matches:
            return matches[0]
    return None


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _first_relative_part(path: Path, root: Path) -> str:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        return ""
    return relative.parts[0] if relative.parts else path.name


def _relative_or_abs(root: Path, path: Path) -> Path:
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError:
        return path


def _display_optional(root: Path, path: Path | None) -> str | None:
    return _display_path(_relative_or_abs(root, path)) if path is not None else None


def _display_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    result: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            result.append(resolved)
    return result


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
