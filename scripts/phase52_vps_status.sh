#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PID_FILE="data/debug/phase_5_2_auto_collection.pid"
LOG_FILE="data/debug/phase_5_2_auto_collection_nohup.log"
STATUS_JSON="data/debug/phase_5_2_auto_collection_status.json"

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  echo "Phase 5.2 auto collection: running (PID $(cat "${PID_FILE}"))"
else
  echo "Phase 5.2 auto collection: not running"
fi

if [[ -f "${STATUS_JSON}" ]]; then
  python - <<'PY'
import json
from pathlib import Path
status = json.loads(Path("data/debug/phase_5_2_auto_collection_status.json").read_text(encoding="utf-8"))
print(f"current_session: {status.get('current_session')}")
print(f"completed_session_count: {status.get('completed_session_count')}")
print(f"passed_session_count: {status.get('passed_session_count')}")
print(f"failed_session_count: {status.get('failed_session_count')}")
print(f"research_eligible_session_count: {status.get('research_eligible_session_count')}")
print(f"last_failure: {status.get('last_failure')}")
print(f"stopped_early: {status.get('stopped_early')}")
PY
else
  echo "No Phase 5.2 status JSON yet."
fi

if [[ -f "${LOG_FILE}" ]]; then
  echo "--- latest nohup log ---"
  tail -n 80 "${LOG_FILE}"
fi
