#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RESUME=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/phase52_vps_24h_start.sh
  bash scripts/phase52_vps_24h_start.sh --resume
  bash scripts/phase52_vps_24h_start.sh --dry-run

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
  tracked_generated="$(git ls-files | grep -E '(^data/(dataset|debug|cache|logs|reports)/|(^|/).+\.jsonl$|(^|/).+\.zip$|(^|/).+\.log$)' || true)"
  staged_generated="$(git diff --name-only --cached | grep -E '(^data/(dataset|debug|cache|logs|reports)/|(^|/).+\.jsonl$|(^|/).+\.zip$|(^|/).+\.log$)' || true)"
  deleted_runtime="$(git status --short | awk '/^ D|^D / {print $2}' | grep -E '(^data/phase_5_2/|^data/debug/phase_5_2|^data/reports/phase_5_2)' || true)"
  [[ -z "${tracked_generated}" ]] || fail "generated heavy artifacts are tracked: ${tracked_generated}"
  [[ -z "${staged_generated}" ]] || fail "generated heavy artifacts are staged: ${staged_generated}"
  [[ -z "${deleted_runtime}" ]] || fail "tracked runtime artifacts were deleted by cleanup: ${deleted_runtime}"
fi

if ! grep -q -- '"--clean"' bot/app/research/phase52_auto_collection.py; then
  fail "Phase 5.2 runner does not guarantee Phase 4.2H --clean."
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

if [[ "${RESUME}" == "1" ]]; then
  command+=(--resume)
fi

if [[ "${DRY_RUN}" == "1" ]]; then
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
