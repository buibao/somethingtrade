#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

JOURNAL_SINCE="4 hours ago"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/phase52_vps_audit_session.sh session_004_medium_2h
  bash scripts/phase52_vps_audit_session.sh session_004_medium_2h --journal-since "8 hours ago"

Read-only Phase 5.2 session audit helper. It never starts, stops, deletes,
moves, compresses, or modifies runtime artifacts.
EOF
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

SESSION_ID="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --journal-since)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --journal-since" >&2
        exit 2
      fi
      JOURNAL_SINCE="$2"
      shift 2
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
done

if [[ ! "${SESSION_ID}" =~ ^session_[0-9]{3}_[A-Za-z0-9_]+$ ]]; then
  echo "Invalid session id: ${SESSION_ID}" >&2
  echo "Expected a Phase 5.2 session id like session_004_medium_2h; path separators are not allowed." >&2
  exit 2
fi

set +e
python - "${SESSION_ID}" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

session_id = sys.argv[1]
root = Path.cwd()
session_dir = root / "data/phase_5_2/sessions" / session_id

paths = {
    "phase52_status": root / "data/debug/phase_5_2_auto_collection_status.json",
    "quality_report": session_dir / f"phase_5_2_{session_id}_quality_report.json",
    "metadata": session_dir / f"phase_5_2_{session_id}_metadata.json",
    "hotpath_report": session_dir / "data/reports/phase_4_2h_hotpath_environment_latency_report.json",
    "self_check": session_dir / "data/reports/phase42h_self_check.json",
}


def read_json(name: str, path: Path, missing: list[str]) -> dict[str, Any]:
    if not path.exists():
        missing.append(f"{name}: {path.as_posix()}")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        missing.append(f"{name}: {path.as_posix()} could not be parsed: {exc}")
        return {}


def field(payload: dict[str, Any], key: str) -> Any:
    return payload.get(key)


def print_field(payload: dict[str, Any], key: str) -> None:
    value = field(payload, key)
    if isinstance(value, (list, dict)):
        text = json.dumps(value, sort_keys=True)
    else:
        text = str(value)
    print(f"{key}: {text}")


def status_failed(section: str, payload: dict[str, Any], key: str, failures: list[str]) -> None:
    if payload.get(key) == "fail":
        failures.append(f"{section}.{key}=fail")


def size_text(path: Path) -> str:
    return f"{path.stat().st_size} bytes" if path.exists() and path.is_file() else "missing"


def top_files(patterns: tuple[str, ...], limit: int = 10) -> list[Path]:
    files: list[Path] = []
    if session_dir.exists():
        for pattern in patterns:
            files.extend(path for path in session_dir.rglob(pattern) if path.is_file())
    return sorted(files, key=lambda path: path.stat().st_size, reverse=True)[:limit]


missing: list[str] = []
failures: list[str] = []
status = read_json("phase52_status", paths["phase52_status"], missing)
quality = read_json("quality_report", paths["quality_report"], missing)
metadata = read_json("metadata", paths["metadata"], missing)
hotpath = read_json("hotpath_report", paths["hotpath_report"], missing)
self_check = read_json("self_check", paths["self_check"], missing)

print("=== Phase 5.2 status summary ===")
for key in (
    "running",
    "current_session",
    "completed_session_count",
    "passed_session_count",
    "failed_session_count",
    "research_eligible_session_count",
    "last_failure",
    "stopped_early",
    "stop_reason",
):
    print_field(status, key)

print("")
print("=== Session quality report ===")
for key in ("status", "failure_reasons", "research_eligible", "bundle_sha256_valid"):
    print_field(quality, key)
status_failed("quality_report", quality, "status", failures)

print("")
print("=== Session metadata ===")
for key in (
    "runtime_status",
    "primary_failure",
    "exit_code",
    "requested_duration_sec",
    "actual_duration_sec",
    "low_latency_ready",
    "strict_100ms_observability_ready",
    "clock_sync_status",
):
    print_field(metadata, key)
if metadata.get("runtime_status") == "fail":
    failures.append("metadata.runtime_status=fail")

print("")
print("=== Hotpath report ===")
for key in (
    "status",
    "primary_failure",
    "hard_fail_reasons",
    "low_latency_ready",
    "strict_100ms_observability_ready",
    "clock_sync_status",
):
    print_field(hotpath, key)
status_failed("hotpath_report", hotpath, "status", failures)

print("")
print("=== Self-check ===")
for key in ("status", "low_latency_ready", "strict_100ms_observability_ready", "clock_sync_status"):
    print_field(self_check, key)
status_failed("self_check", self_check, "status", failures)

print("")
print("=== Memory telemetry ===")
memory = hotpath.get("memory_telemetry") if isinstance(hotpath.get("memory_telemetry"), dict) else {}
print(f"available: {memory.get('available')}")
print(f"finalization_memory_delta_bytes: {memory.get('finalization_memory_delta_bytes')}")
generated = memory.get("generated_file_sizes_bytes") if isinstance(memory.get("generated_file_sizes_bytes"), dict) else {}
print("top generated files by size:")
if generated:
    for path, size in sorted(generated.items(), key=lambda item: int(item[1] or 0), reverse=True)[:10]:
        print(f"  {size} bytes  {path}")
else:
    print("  none reported")

print("")
print("=== Bundle/data size ===")
print(f"audit bundle size: {size_text(root / 'phase_5_2_audit_bundle.zip')}")
print("session zip/jsonl largest files:")
largest = top_files(("*.zip", "*.jsonl"), limit=10)
if largest:
    for path in largest:
        print(f"  {path.stat().st_size} bytes  {path.relative_to(root).as_posix()}")
else:
    print("  none found")

if missing:
    print("")
    print("=== Missing required reports ===")
    for item in missing:
        print(f"- {item}")
if failures:
    print("")
    print("=== Failing statuses ===")
    for item in failures:
        print(f"- {item}")

sys.exit(1 if missing or failures else 0)
PY
AUDIT_STATUS=$?
set -e

echo ""
echo "=== OOM check: dmesg tail for OOM/SIGKILL ==="
if command -v dmesg >/dev/null 2>&1; then
  set +e
  DMESG_OUTPUT="$(dmesg --ctime --color=never 2>&1 | grep -Ei 'out of memory|oom-kill|oom killed|killed process|sigkill|signal 9|oom' | tail -n 40)"
  set -e
  if [[ -n "${DMESG_OUTPUT}" ]]; then
    printf '%s\n' "${DMESG_OUTPUT}"
  else
    echo "No matching dmesg lines found, or dmesg is unavailable to this user."
  fi
else
  echo "dmesg command not available."
fi

echo ""
echo "=== OOM check: journalctl since ${JOURNAL_SINCE} ==="
if command -v journalctl >/dev/null 2>&1; then
  set +e
  JOURNAL_OUTPUT="$(journalctl --since "${JOURNAL_SINCE}" -k --no-pager 2>&1 | grep -Ei 'out of memory|oom-kill|oom killed|killed process|sigkill|signal 9|oom' | tail -n 80)"
  set -e
  if [[ -n "${JOURNAL_OUTPUT}" ]]; then
    printf '%s\n' "${JOURNAL_OUTPUT}"
  else
    echo "No matching journalctl kernel lines found for the selected window, or journalctl is unavailable to this user."
  fi
else
  echo "journalctl command not available."
fi

exit "${AUDIT_STATUS}"
