backup_dir=/root/somethingtrade_destroy_safety_backup_20260530T131704Z
created_utc=20260530T131704Z

Included:
- source_worktree excluding .git/.venv/data/logs/cache-like dirs
- full data/ directory
- full logs/ directory if present
- root-level phase_4 / phase_5 artifacts, bundles, manifests, sha256 files
- git metadata, git diff, disk/memory snapshots, full file inventory

Important note:
This bundle is intended to preserve everything needed before destroying the VPS.
