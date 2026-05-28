#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RESUME=0
DRY_RUN=0
ALLOW_LOW_MEMORY_VPS=0
INCLUDE_LARGE_DATASETS=0
ALLOW_NESTED_ZIP=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/phase52_vps_24h_start.sh
  bash scripts/phase52_vps_24h_start.sh --resume
  bash scripts/phase52_vps_24h_start.sh --dry-run
  bash scripts/phase52_vps_24h_start.sh --allow-low-memory-vps
  bash scripts/phase52_vps_24h_start.sh --include-large-datasets
  bash scripts/phase52_vps_24h_start.sh --include-large-datasets --allow-nested-zip

Clean reruns must start with no active data/phase_5_2 directory. Use:
  bash scripts/phase52_vps_clean_failed_run.sh --archive-active-output
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --resume)
      RESUME=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    --allow-low-memory-vps)
      ALLOW_LOW_MEMORY_VPS=1
      ;;
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

PID_FILE="data/debug/phase_5_2_auto_collection.pid"
LOG_FILE="data/debug/phase_5_2_auto_collection_nohup.log"
STOP_FILE="data/debug/phase_5_2_stop_after_current_session"
STATUS_FILE="data/debug/phase_5_2_auto_collection_status.json"
MANIFEST_FILE="data/debug/phase_5_2_auto_collection_manifest.json"

fail() {
  echo "Refusing to start Phase 5.2: $1" >&2
  exit 1
}

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  fail "auto collection is already running with PID $(cat "${PID_FILE}")"
fi

if [[ "${RESUME}" != "1" ]]; then
  if [[ -d data/phase_5_2 ]] && find data/phase_5_2 -mindepth 1 -print -quit | grep -q .; then
    fail "data/phase_5_2 already exists. Archive/delete it with scripts/phase52_vps_clean_failed_run.sh or rerun with --resume."
  fi
  for stale in "${PID_FILE}" "${LOG_FILE}" "${STOP_FILE}" "${STATUS_FILE}" "${MANIFEST_FILE}"; do
    if [[ -e "${stale}" ]]; then
      fail "stale runtime file exists: ${stale}. Clean it before a fresh session_001 start."
    fi
  done
fi

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  dirty_state="$(git status --short --untracked-files=all)"
  tracked_generated="$(git ls-files | grep -E '(^data/(dataset|debug|cache|logs|reports)/|(^|/).+\.jsonl$|(^|/).+\.zip$|(^|/).+\.log$|^phase_5_2_.*sha256.*\.txt$|^phase_4_2h_.*sha256.*\.txt$)' || true)"
  staged_generated="$(git diff --name-only --cached | grep -E '(^data/(dataset|debug|cache|logs|reports)/|(^|/).+\.jsonl$|(^|/).+\.zip$|(^|/).+\.log$|^phase_5_2_.*sha256.*\.txt$|^phase_4_2h_.*sha256.*\.txt$)' || true)"
  deleted_runtime="$(git status --short | awk '/^ D|^D / {print $2}' | grep -E '(^data/phase_5_2/|^data/debug/phase_5_2|^data/reports/phase_5_2)' || true)"
  legacy_unignored_archives="$(find data -maxdepth 1 -type d -name 'phase_5_2_failed_before_cleanup_fix*' -print 2>/dev/null || true)"
  missing_gitignore_rules=""
  required_gitignore_rules=(
    "data/phase_5_2/"
    "data/cache/phase_5_2_failed_runs/"
    "data/dataset/"
    "data/debug/"
    "data/reports/"
    "*.jsonl"
    "*.zip"
    "*.log"
    "phase_5_2_audit_bundle_sha256.txt"
    "phase_5_2_full_dataset_bundle_sha256.txt"
  )
  for rule in "${required_gitignore_rules[@]}"; do
    if [[ ! -f .gitignore ]] || ! grep -qxF "${rule}" .gitignore; then
      missing_gitignore_rules="${missing_gitignore_rules}${rule}"$'\n'
    fi
  done
  [[ -z "${dirty_state}" ]] || fail "git working tree is dirty. Commit/stash/remove changes before starting: ${dirty_state}"
  [[ -z "${tracked_generated}" ]] || fail "generated heavy artifacts are tracked: ${tracked_generated}"
  [[ -z "${staged_generated}" ]] || fail "generated heavy artifacts are staged: ${staged_generated}"
  [[ -z "${deleted_runtime}" ]] || fail "tracked runtime artifacts were deleted by cleanup: ${deleted_runtime}"
  [[ -z "${legacy_unignored_archives}" ]] || fail "unignored legacy Phase 5.2 archive folders exist: ${legacy_unignored_archives}"
  [[ -z "${missing_gitignore_rules}" ]] || fail ".gitignore is missing generated artifact rules: ${missing_gitignore_rules}"
fi

if ! grep -q -- '"--clean"' bot/app/research/phase52_auto_collection.py; then
  fail "Phase 5.2 runner does not guarantee Phase 4.2H --clean."
fi

memory_total_bytes="${PHASE52_MEMORY_TOTAL_BYTES:-}"
memory_available_bytes="${PHASE52_MEMORY_AVAILABLE_BYTES:-}"
swap_total_bytes="${PHASE52_SWAP_TOTAL_BYTES:-}"
if [[ -z "${memory_total_bytes}" || -z "${swap_total_bytes}" ]]; then
  if [[ -r /proc/meminfo ]]; then
    memory_total_bytes="${memory_total_bytes:-$(awk '/^MemTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)}"
    memory_available_bytes="${memory_available_bytes:-$(awk '/^MemAvailable:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)}"
    swap_total_bytes="${swap_total_bytes:-$(awk '/^SwapTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)}"
  else
    memory_total_bytes="${memory_total_bytes:-0}"
    memory_available_bytes="${memory_available_bytes:-0}"
    swap_total_bytes="${swap_total_bytes:-0}"
  fi
fi

memory_total_bytes="${memory_total_bytes%.*}"
memory_available_bytes="${memory_available_bytes%.*}"
swap_total_bytes="${swap_total_bytes%.*}"
low_memory_threshold_bytes=$((4 * 1024 * 1024 * 1024))
memory_guard_decision="pass"
if [[ "${ALLOW_LOW_MEMORY_VPS}" != "1" ]]; then
  if [[ "${memory_total_bytes:-0}" -lt "${low_memory_threshold_bytes}" || "${swap_total_bytes:-0}" -le 0 ]]; then
    memory_guard_decision="refuse"
    fail "planned 1h+ Phase 5.2 sessions need streaming finalization plus a safe memory budget. Detected memory_total_bytes=${memory_total_bytes:-0}, memory_available_bytes=${memory_available_bytes:-0}, swap_total_bytes=${swap_total_bytes:-0}. Add swap or rerun with --allow-low-memory-vps."
  fi
else
  memory_guard_decision="override"
  echo "Warning: low-memory VPS guard overridden. memory_total_bytes=${memory_total_bytes:-0} memory_available_bytes=${memory_available_bytes:-0} swap_total_bytes=${swap_total_bytes:-0}" >&2
fi

command=(
  python -X utf8 scripts/run_phase52_auto_collection.py
  --plan-name phase52_24h_default
  --total-budget-hours 24
  --output-dir data/phase_5_2
  --strict-100ms
  --create-bundles
  --fail-session-on-quality-gate
  --max-session-retries 1
  --stop-after-current-session-file "${STOP_FILE}"
)

if [[ "${INCLUDE_LARGE_DATASETS}" == "1" ]]; then
  command+=(--include-large-datasets)
fi

if [[ "${ALLOW_NESTED_ZIP}" == "1" ]]; then
  command+=(--allow-nested-zip)
fi

if [[ "${RESUME}" == "1" ]]; then
  command+=(--resume)
fi

if [[ "${DRY_RUN}" == "1" ]]; then
  printf 'total RAM bytes: %s\n' "${memory_total_bytes:-0}"
  printf 'available RAM bytes: %s\n' "${memory_available_bytes:-0}"
  printf 'swap total bytes: %s\n' "${swap_total_bytes:-0}"
  printf 'memory guard decision: %s\n' "${memory_guard_decision}"
  if [[ "${INCLUDE_LARGE_DATASETS}" == "1" ]]; then
    printf 'bundle mode: audit-light plus explicit full dataset bundle\n'
    printf 'large datasets included in bundles: true\n'
  else
    printf 'bundle mode: audit-light\n'
    printf 'large datasets included in bundles: false\n'
  fi
  printf 'nested zip allowed: %s\n' "$([[ "${ALLOW_NESTED_ZIP}" == "1" ]] && echo true || echo false)"
  printf 'Phase 5.2 start validation passed. Command would be:\n'
  printf ' %q' "${command[@]}"
  printf '\n'
  exit 0
fi

mkdir -p data/debug data/phase_5_2/sessions
rm -f "${STOP_FILE}"

nohup "${command[@]}" > "${LOG_FILE}" 2>&1 &

echo "$!" > "${PID_FILE}"
echo "Phase 5.2 auto collection started with PID $(cat "${PID_FILE}")"
echo "Status: bash scripts/phase52_vps_status.sh"
echo "Graceful stop: bash scripts/phase52_vps_stop.sh"
