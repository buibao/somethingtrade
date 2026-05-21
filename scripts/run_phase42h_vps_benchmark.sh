#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE=""
ENVIRONMENT_NAME=""
ENVIRONMENT_REGION=""
MACHINE_PROFILE=""
NETWORK_NOTES=""
DURATION_SEC=""

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/run_phase42h_vps_benchmark.sh \
    --mode smoke|final \
    --environment-name NAME \
    --environment-region REGION \
    --machine-profile PROFILE \
    --network-notes NOTES \
    --duration-sec SECONDS
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --environment-name)
      ENVIRONMENT_NAME="${2:-}"
      shift 2
      ;;
    --environment-region)
      ENVIRONMENT_REGION="${2:-}"
      shift 2
      ;;
    --machine-profile)
      MACHINE_PROFILE="${2:-}"
      shift 2
      ;;
    --network-notes)
      NETWORK_NOTES="${2:-}"
      shift 2
      ;;
    --duration-sec)
      DURATION_SEC="${2:-}"
      shift 2
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

if [[ "$MODE" != "smoke" && "$MODE" != "final" ]]; then
  echo "--mode is required and must be smoke or final" >&2
  usage >&2
  exit 2
fi

if [[ -z "$ENVIRONMENT_NAME" || -z "$ENVIRONMENT_REGION" || -z "$MACHINE_PROFILE" || -z "$NETWORK_NOTES" ]]; then
  echo "environment-name, environment-region, machine-profile, and network-notes are required" >&2
  usage >&2
  exit 2
fi

if [[ -z "$DURATION_SEC" ]]; then
  if [[ "$MODE" == "smoke" ]]; then
    DURATION_SEC=120
  else
    DURATION_SEC=1800
  fi
fi

if ! [[ "$DURATION_SEC" =~ ^[0-9]+$ ]]; then
  echo "--duration-sec must be a positive integer number of seconds" >&2
  exit 2
fi

if (( DURATION_SEC <= 0 )); then
  echo "--duration-sec must be greater than zero" >&2
  exit 2
fi

if [[ "$MODE" == "final" ]] && (( DURATION_SEC < 1800 )); then
  echo "final mode requires --duration-sec >= 1800" >&2
  exit 2
fi

RUN_MODE="vps_${MODE}"
PREFLIGHT_CMD=(
  python -X utf8 scripts/run_phase42h_hotpath_environment_latency.py
  --preflight-only
  --environment-name "$ENVIRONMENT_NAME"
  --environment-region "$ENVIRONMENT_REGION"
  --machine-profile "$MACHINE_PROFILE"
  --network-notes "$NETWORK_NOTES"
  --run-mode "$RUN_MODE"
)
BENCHMARK_CMD=(
  python -X utf8 scripts/run_phase42h_hotpath_environment_latency.py
  --clean
  --symbol BTCUSDT
  --duration-sec "$DURATION_SEC"
  --depth-n 20
  --environment-name "$ENVIRONMENT_NAME"
  --environment-region "$ENVIRONMENT_REGION"
  --machine-profile "$MACHINE_PROFILE"
  --network-notes "$NETWORK_NOTES"
  --run-mode "$RUN_MODE"
)

if [[ "${PHASE42H_DRY_RUN:-0}" == "1" ]]; then
  echo "Phase 4.2H VPS wrapper dry run"
  echo "Mode: ${MODE}"
  echo "Duration: ${DURATION_SEC}"
  echo "Preflight command:"
  printf ' %q' "${PREFLIGHT_CMD[@]}"
  echo
  echo "Benchmark command:"
  printf ' %q' "${BENCHMARK_CMD[@]}"
  echo
  exit 0
fi

cd "$ROOT_DIR"

if [[ ! -f .venv/bin/activate ]]; then
  echo ".venv is missing. Run bash scripts/setup_phase42h_vps_ubuntu.sh first." >&2
  exit 2
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if [[ "$MODE" == "smoke" ]]; then
  echo "Running Phase 4.2H VPS smoke benchmark. This is not final evidence."
else
  echo "Running Phase 4.2H VPS final benchmark with hard 100ms requirement."
fi

echo "Running preflight"
"${PREFLIGHT_CMD[@]}"

set +e
"${BENCHMARK_CMD[@]}"
BENCHMARK_STATUS=$?
set -e

if bash scripts/collect_phase42h_vps_bundle.sh; then
  COLLECT_STATUS=0
  echo "Phase 4.2H VPS bundle collection completed"
else
  COLLECT_STATUS=$?
  echo "Phase 4.2H VPS bundle collection did not find a bundle" >&2
fi

if (( BENCHMARK_STATUS == 0 && COLLECT_STATUS != 0 )); then
  exit "$COLLECT_STATUS"
fi

exit "$BENCHMARK_STATUS"
