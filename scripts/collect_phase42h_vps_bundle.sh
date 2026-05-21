#!/usr/bin/env bash
set -euo pipefail

DEFAULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="${PHASE42H_ROOT:-$DEFAULT_ROOT}"
PASS_BUNDLE="${ROOT_DIR}/phase_4_2h_hotpath_environment_latency_bundle.zip"
FAIL_BUNDLE="${ROOT_DIR}/phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip"
CHECKSUM_FILE="${ROOT_DIR}/phase_4_2h_bundle_sha256.txt"

existing=()
if [[ -f "$PASS_BUNDLE" ]]; then
  existing+=("$PASS_BUNDLE")
fi
if [[ -f "$FAIL_BUNDLE" ]]; then
  existing+=("$FAIL_BUNDLE")
fi

if [[ "${#existing[@]}" -eq 0 ]]; then
  echo "No Phase 4.2H bundle found." >&2
  echo "Expected one of:" >&2
  echo "  ${PASS_BUNDLE}" >&2
  echo "  ${FAIL_BUNDLE}" >&2
  exit 1
fi

selected="${existing[0]}"
if [[ "${#existing[@]}" -eq 2 ]]; then
  pass_mtime="$(stat -c %Y "$PASS_BUNDLE")"
  fail_mtime="$(stat -c %Y "$FAIL_BUNDLE")"
  if (( fail_mtime > pass_mtime )); then
    selected="$FAIL_BUNDLE"
  fi
fi

absolute_path="$(readlink -f "$selected")"
filename="$(basename "$selected")"
sha256="$(sha256sum "$selected" | awk '{print $1}')"
file_size="$(stat -c %s "$selected")"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "$CHECKSUM_FILE" <<EOF
filename: ${filename}
sha256: ${sha256}
file_size_bytes: ${file_size}
utc_timestamp: ${timestamp}
absolute_path: ${absolute_path}
EOF

echo "Phase 4.2H bundle: ${absolute_path}"
echo "Checksum file: ${CHECKSUM_FILE}"
