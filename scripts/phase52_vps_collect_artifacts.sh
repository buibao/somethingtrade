#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

OUT="phase_5_2_vps_downloadable_artifacts.zip"
SHA="phase_5_2_vps_downloadable_artifacts_sha256.txt"

rm -f "${OUT}" "${SHA}"

python - <<'PY'
from pathlib import Path
import zipfile

out = Path("phase_5_2_vps_downloadable_artifacts.zip")
with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for base in (Path("data/phase_5_2"), Path("data/debug"), Path("data/reports")):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and ("phase_5_2" in path.as_posix() or "phase52" in path.as_posix()):
                archive.write(path, path.as_posix())
    for path in (Path("phase_5_2_auto_collection_all_sessions_bundle.zip"), Path("phase_5_2_auto_collection_all_sessions_sha256.txt")):
        if path.exists():
            archive.write(path, path.as_posix())
PY

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${OUT}" > "${SHA}"
else
  shasum -a 256 "${OUT}" > "${SHA}"
fi

echo "Created ${OUT}"
echo "Created ${SHA}"

