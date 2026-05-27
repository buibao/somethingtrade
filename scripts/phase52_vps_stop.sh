#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

STOP_FILE="data/debug/phase_5_2_stop_after_current_session"
mkdir -p "$(dirname "${STOP_FILE}")"
date -u +%Y-%m-%dT%H:%M:%SZ > "${STOP_FILE}"

echo "Created ${STOP_FILE}."
echo "Phase 5.2 auto collection will stop gracefully after the current session finishes."
echo "This script does not kill the runner by default."

