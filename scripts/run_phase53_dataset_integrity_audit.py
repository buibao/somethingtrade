from __future__ import annotations

import argparse
from pathlib import Path
import sys


SOURCE_ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = SOURCE_ROOT / "bot"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.phase53_dataset_integrity import (  # noqa: E402
    Phase53Config,
    run_phase53_dataset_integrity_audit,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Phase 5.3 offline dataset integrity and research-readiness audit.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--phase52-sessions", required=True)
    parser.add_argument("--failed-runs", required=True)
    parser.add_argument("--preflight-sessions", required=True)
    parser.add_argument("--phase52f-artifacts", required=True)
    parser.add_argument("--backup-meta", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)

    config = Phase53Config.from_paths(
        repo_root=args.repo_root,
        phase52_sessions=args.phase52_sessions,
        failed_runs=args.failed_runs,
        preflight_sessions=args.preflight_sessions,
        phase52f_artifacts=args.phase52f_artifacts,
        backup_meta=args.backup_meta,
        output_root=args.output_root,
        strict=args.strict,
    )
    manifest = run_phase53_dataset_integrity_audit(config)
    evidence = manifest.get("evidence_paths", {})
    print(f"Phase 5.3 final status: {manifest.get('final_status')}")
    print(f"Research ready: {manifest.get('research_ready')}")
    print(f"Allowed Phase 5.4 sessions: {', '.join(manifest.get('allowed_phase54_sessions') or []) or 'none'}")
    print(f"Manifest JSON: {config.manifests_dir / 'phase_5_3_research_readiness_manifest.json'}")
    print(f"Evidence bundle: {evidence.get('final_evidence_bundle')}")
    print(f"Evidence sha256: {evidence.get('final_evidence_bundle_sha256')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
