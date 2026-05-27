#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

mkdir -p data/debug data/phase_5_2/sessions

PID_FILE="data/debug/phase_5_2_auto_collection.pid"
LOG_FILE="data/debug/phase_5_2_auto_collection_nohup.log"
STOP_FILE="data/debug/phase_5_2_stop_after_current_session"

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  echo "Phase 5.2 auto collection is already running with PID $(cat "${PID_FILE}")"
  exit 0
fi

rm -f "${STOP_FILE}"

nohup python -X utf8 scripts/run_phase52_auto_collection.py \
  --plan-name phase52_24h_default \
  --total-budget-hours 24 \
  --output-dir data/phase_5_2 \
  --strict-100ms \
  --create-bundles \
  --fail-session-on-quality-gate \
  --resume \
  --max-session-retries 1 \
  --stop-after-current-session-file "${STOP_FILE}" \
  > "${LOG_FILE}" 2>&1 &

echo "$!" > "${PID_FILE}"
echo "Phase 5.2 auto collection started with PID $(cat "${PID_FILE}")"
echo "Status: bash scripts/phase52_vps_status.sh"
echo "Graceful stop: bash scripts/phase52_vps_stop.sh"

