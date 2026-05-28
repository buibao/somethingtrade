#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

INCLUDE_LARGE_DATASETS=0
ALLOW_NESTED_ZIP=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/phase52_vps_collect_artifacts.sh
  bash scripts/phase52_vps_collect_artifacts.sh --include-large-datasets
  bash scripts/phase52_vps_collect_artifacts.sh --include-large-datasets --allow-nested-zip

Default output is audit-light: reports, metadata, logs, debug JSON/TXT/MD, SHA files,
and file_size_manifest.json. Raw JSONL and nested ZIP files are excluded by default.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-large-datasets)
      INCLUDE_LARGE_DATASETS=1
      ;;
    --allow-nested-zip)
      ALLOW_NESTED_ZIP=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

OUT="phase_5_2_vps_downloadable_artifacts.zip"
SHA="phase_5_2_vps_downloadable_artifacts_sha256.txt"
FULL_OUT="phase_5_2_vps_full_dataset_artifacts.zip"
FULL_SHA="phase_5_2_vps_full_dataset_artifacts_sha256.txt"

rm -f "${OUT}" "${SHA}" "${FULL_OUT}" "${FULL_SHA}"

python - "${INCLUDE_LARGE_DATASETS}" "${ALLOW_NESTED_ZIP}" <<'PY'
from __future__ import annotations

from pathlib import Path
import json
import sys
import zipfile

include_large = sys.argv[1] == "1"
allow_nested_zip = sys.argv[2] == "1"
root = Path.cwd()
audit_out = Path("phase_5_2_vps_downloadable_artifacts.zip")
full_out = Path("phase_5_2_vps_full_dataset_artifacts.zip")

skip_outputs = {
    audit_out.as_posix(),
    "phase_5_2_vps_downloadable_artifacts_sha256.txt",
    full_out.as_posix(),
    "phase_5_2_vps_full_dataset_artifacts_sha256.txt",
}

def artifact_type(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return "large_dataset"
    if suffix == ".zip":
        return "archive"
    if suffix == ".log":
        return "console_log" if "console" in name else "log"
    if "metadata" in name:
        return "metadata"
    if "quality" in name:
        return "quality_report"
    if "sha256" in name:
        return "sha256"
    if "/reports/" in path.as_posix() or path.as_posix().startswith("data/reports/"):
        return "report"
    if "/debug/" in path.as_posix() or path.as_posix().startswith("data/debug/"):
        return "debug"
    return "artifact"

def is_candidate(path: Path) -> bool:
    text = path.as_posix()
    if text in skip_outputs:
        return False
    if text.startswith("data/phase_5_2/"):
        return True
    return "phase_5_2" in text or "phase52" in text

files = []
seen = set()
for base in (Path("data/phase_5_2"), Path("data/debug"), Path("data/reports")):
    if not base.exists():
        continue
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        relative = path.as_posix()
        if relative in seen or not is_candidate(path):
            continue
        is_large = path.suffix.lower() == ".jsonl"
        is_zip = path.suffix.lower() == ".zip"
        files.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "artifact_type": artifact_type(path),
                "included_in_audit_bundle": (not is_large and (not is_zip or allow_nested_zip)),
                "included_in_full_bundle": include_large and (is_large or (is_zip and allow_nested_zip)),
            }
        )
        seen.add(relative)
files.sort(key=lambda item: item["path"])

manifest = {
    "schema_version": "phase_5_2_file_size_manifest_v1",
    "bundle_policy": {
        "default_bundle": "audit_light",
        "include_large_datasets": include_large,
        "allow_nested_zip": allow_nested_zip,
        "large_jsonl_excluded_from_audit_bundle": True,
        "zip_inside_zip_excluded_by_default": not allow_nested_zip,
    },
    "files": files,
}
manifest_text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
manifest_path = Path("data/debug/phase_5_2_file_size_manifest.json")
manifest_path.parent.mkdir(parents=True, exist_ok=True)
manifest_path.write_text(manifest_text, encoding="utf-8")

with zipfile.ZipFile(audit_out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for item in files:
        if item["included_in_audit_bundle"]:
            archive.write(root / item["path"], item["path"])
    archive.write(manifest_path, manifest_path.as_posix())
    archive.writestr("file_size_manifest.json", manifest_text)

if include_large:
    with zipfile.ZipFile(full_out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in files:
            if item["included_in_full_bundle"]:
                archive.write(root / item["path"], item["path"])
        archive.write(manifest_path, manifest_path.as_posix())
        archive.writestr("file_size_manifest.json", manifest_text)
PY

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${OUT}" > "${SHA}"
  if [[ "${INCLUDE_LARGE_DATASETS}" == "1" ]]; then
    sha256sum "${FULL_OUT}" > "${FULL_SHA}"
  fi
else
  shasum -a 256 "${OUT}" > "${SHA}"
  if [[ "${INCLUDE_LARGE_DATASETS}" == "1" ]]; then
    shasum -a 256 "${FULL_OUT}" > "${FULL_SHA}"
  fi
fi

echo "Created ${OUT} (audit-light, raw JSONL excluded, nested ZIP excluded unless explicitly allowed)"
echo "Created ${SHA}"
if [[ "${INCLUDE_LARGE_DATASETS}" == "1" ]]; then
  echo "Created ${FULL_OUT} (explicit full dataset mode)"
  echo "Created ${FULL_SHA}"
fi
