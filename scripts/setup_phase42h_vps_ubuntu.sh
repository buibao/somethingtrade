#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_PATH="${ROOT_DIR}/data/debug/phase_4_2h_vps_setup_report.txt"
INSTALL_CHRONY=0

usage() {
  cat <<'USAGE'
Usage: bash scripts/setup_phase42h_vps_ubuntu.sh [--install-chrony]

Prepares an Ubuntu VPS for the Phase 4.2H benchmark. It installs only local
OS/Python dependencies and never starts the 30-minute benchmark.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-chrony)
      INSTALL_CHRONY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$(dirname "$REPORT_PATH")"
exec > >(tee "$REPORT_PATH") 2>&1

echo "Phase 4.2H VPS setup started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Root: ${ROOT_DIR}"
cd "$ROOT_DIR"

if [[ -r /etc/os-release ]]; then
  echo "OS release:"
  cat /etc/os-release
fi

echo "Installing required Ubuntu packages"
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git curl unzip zip ca-certificates

if [[ "$INSTALL_CHRONY" == "1" ]]; then
  echo "Installing chrony as requested. No NTP server or time-source policy changes are made by this script."
  sudo apt-get install -y chrony
else
  echo "Chrony install skipped. Re-run with --install-chrony only if you want the package installed."
fi

echo "Python version:"
python3 --version
python3 - <<'PY'
import sys

if sys.version_info < (3, 12):
    raise SystemExit("Python 3.12+ is required for this source tree. Use Ubuntu 24.04 or install Python 3.12 on Ubuntu 22.04 before continuing.")
PY
echo "Git version:"
git --version
echo "Current UTC time:"
date -u +%Y-%m-%dT%H:%M:%SZ

echo "Binance /api/v3/time reachability check:"
curl --fail --silent --show-error --max-time 10 "https://api.binance.com/api/v3/time" || echo "Binance time check failed; benchmark preflight will check again."
echo

if [[ ! -d .venv ]]; then
  echo "Creating .venv"
  python3 -m venv .venv
else
  echo ".venv already exists"
fi

# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

echo "Installing project dependencies"
if python -m pip install -e ".[dev]"; then
  echo "Installed with pip install -e .[dev]"
elif [[ -f requirements.txt ]] && python -m pip install -r requirements.txt; then
  echo "Installed with requirements.txt"
else
  python -m pip install -e .
  echo "Installed with pip install -e ."
fi

echo "Running quick import check"
python -X utf8 - <<'PY'
import aiohttp
import pydantic
import websockets
from app.research.hotpath_environment_latency import build_environment_metadata

metadata = build_environment_metadata(
    environment_name="vps_setup_import_check",
    environment_region="unknown",
    machine_profile="setup",
    network_notes="setup import check",
    run_mode="vps_setup",
)
print("import check ok")
print(f"python={metadata['python_version']} os={metadata['os']}")
PY

echo "Phase 4.2H VPS setup completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Setup report written to ${REPORT_PATH}"
echo "The 30-minute benchmark was not run."
