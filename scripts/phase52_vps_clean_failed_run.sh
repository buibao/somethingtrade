#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="dry-run"
DRY_RUN=1

usage() {
  cat <<'EOF'
Usage:
  bash scripts/phase52_vps_clean_failed_run.sh --dry-run
  bash scripts/phase52_vps_clean_failed_run.sh --archive-active-output
  bash scripts/phase52_vps_clean_failed_run.sh --delete-active-output

Default is safe dry-run. Destructive cleanup requires an explicit archive/delete flag.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      MODE="dry-run"
      DRY_RUN=1
      ;;
    --archive-active-output)
      MODE="archive"
      DRY_RUN=0
      ;;
    --delete-active-output)
      MODE="delete"
      DRY_RUN=0
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

processes_running() {
  if [[ "${PHASE52_FORCE_PROCESS_RUNNING:-}" == "1" ]]; then
    return 0
  fi
  if [[ -f data/debug/phase_5_2_auto_collection.pid ]]; then
    local pid
    pid="$(cat data/debug/phase_5_2_auto_collection.pid 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
  fi
  if command -v pgrep >/dev/null 2>&1; then
    if pgrep -f "run_phase52_auto_collection.py|run_phase52_controlled_capture.py|run_phase42h_hotpath_environment_latency.py" >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

if processes_running; then
  echo "Refusing to clean: Phase 5.2 or Phase 4.2H process appears to be running." >&2
  exit 1
fi

echo "Phase 5.2 failed-run cleanup mode: ${MODE}"
echo "Repository: ${ROOT_DIR}"

python - "${MODE}" "${DRY_RUN}" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys

mode = sys.argv[1]
dry_run = sys.argv[2] == "1"
root = Path.cwd()

active_output = root / "data/phase_5_2"
timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
archive_target = root / "data/cache/phase_5_2_failed_runs" / f"phase_5_2_failed_before_cleanup_fix_{timestamp}"

stale_phase52 = [
    "data/debug/phase_5_2_auto_collection_nohup.log",
    "data/debug/phase_5_2_auto_collection.pid",
    "data/debug/phase_5_2_auto_collection_status.json",
    "data/debug/phase_5_2_auto_collection_manifest.json",
    "data/debug/phase_5_2_stop_after_current_session",
    "data/reports/phase_5_2_auto_collection_report.json",
    "data/reports/phase_5_2_auto_collection_report.md",
    "phase_5_2_audit_bundle.zip",
    "phase_5_2_audit_bundle_sha256.txt",
    "phase_5_2_full_dataset_bundle.zip",
    "phase_5_2_full_dataset_bundle_sha256.txt",
    "phase_5_2_auto_collection_all_sessions_bundle.zip",
    "phase_5_2_auto_collection_all_sessions_sha256.txt",
]

phase42h_files = [
    "data/dataset/orderbook_clean_samples.jsonl",
    "data/dataset/bookticker_reference_quotes.jsonl",
    "data/dataset/trade_reference_events.jsonl",
    "data/dataset/aggtrade_reference_events.jsonl",
    "data/dataset/orderbook_reference_benchmark_labels.jsonl",
    "data/dataset/orderbook_time_protocol_benchmark_labels.jsonl",
    "data/dataset/phase_4_2h_corrected_time_protocol_labels.jsonl",
    "data/dataset/phase_4_2h_latency_profile_samples.jsonl",
    "data/dataset/phase_4_2h_latency_profile_datasets.zip",
    "data/reports/phase_4_2h_hotpath_environment_latency_report.json",
    "data/reports/phase_4_2h_hotpath_environment_latency_report.md",
    "data/reports/phase42h_self_check.json",
    "data/debug/phase_4_2h_artifact_cleanup.json",
    "data/debug/phase_4_2h_clock_offset_samples.json",
    "data/debug/phase_4_2h_receive_lag_raw_vs_corrected.json",
    "data/debug/phase_4_2h_corrected_hybrid_summary.json",
    "data/debug/phase_4_2h_latency_stage_profile.json",
    "data/debug/phase_4_2h_queue_backpressure_report.json",
    "data/debug/phase_4_2h_writer_batch_report.json",
    "data/debug/phase_4_2h_clock_sanity_report.json",
    "data/debug/phase_4_2h_leakage_check.json",
    "data/debug/phase_4_2h_multifeed_capture_diagnostics.json",
    "data/debug/phase_4_2h_environment_metadata.json",
    "data/debug/phase_4_2h_vps_preflight_report.json",
    "data/debug/phase_4_2h_vps_setup_report.txt",
    "data/debug/phase_4_2h_typecheck_report.txt",
    "data/debug/phase_4_2h_pytest_output.txt",
    "data/debug/phase42h_failure_investigation.md",
    "phase_4_2h_hotpath_environment_latency_bundle.zip",
    "phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip",
    "phase_4_2h_bundle_sha256.txt",
]

summary: dict[str, object] = {
    "dry_run": dry_run,
    "mode": mode,
    "active_output_exists_before": active_output.exists(),
    "archived_to": str(archive_target.relative_to(root)).replace("\\", "/") if mode == "archive" and active_output.exists() else None,
    "deleted": [],
    "missing": [],
    "errors": [],
}

deleted: list[str] = summary["deleted"]  # type: ignore[assignment]
missing: list[str] = summary["missing"]  # type: ignore[assignment]
errors: list[str] = summary["errors"]  # type: ignore[assignment]


def display(path: Path) -> str:
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def remove_path(path: Path) -> None:
    label = display(path)
    if not path.exists():
        missing.append(label)
        return
    if dry_run:
        deleted.append(f"DRY_RUN:{label}")
        return
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        deleted.append(label)
    except OSError as exc:
        errors.append(f"{label}: {type(exc).__name__}: {exc}")


if active_output.exists():
    if mode == "archive":
        if dry_run:
            deleted.append(f"DRY_RUN:archive {display(active_output)} -> {display(archive_target)}")
        else:
            target = archive_target
            suffix = 2
            while target.exists():
                target = root / f"{archive_target.name}_{suffix}"
                suffix += 1
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(active_output), str(target))
            deleted.append(f"archived {display(active_output)} -> {display(target)}")
    elif mode == "delete":
        remove_path(active_output)
    else:
        deleted.append(f"DRY_RUN:active output would remain unless archive/delete mode is selected: {display(active_output)}")
else:
    missing.append(display(active_output))

for rel in stale_phase52 + phase42h_files:
    remove_path(root / rel)

summary["active_output_exists_after"] = active_output.exists()
print(json.dumps(summary, indent=2, sort_keys=True))
if errors:
    sys.exit(1)
PY
