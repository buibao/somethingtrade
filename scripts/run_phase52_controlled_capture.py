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

from app.research.phase52_auto_collection import DEFAULT_COLLECTION_ROOT, run_controlled_capture  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one controlled Phase 5.2 capture session.")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--plan-name", required=True)
    parser.add_argument("--duration-sec", type=float, required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_COLLECTION_ROOT))
    parser.add_argument("--strict-100ms", action="store_true")
    parser.add_argument("--create-bundle", action="store_true")
    parser.add_argument("--fail-session-on-quality-gate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--simulate-failure", default=None)
    parser.add_argument("--notes", default="")
    args = parser.parse_args(argv)

    result = run_controlled_capture(
        root=Path(args.root).resolve(),
        session_id=args.session_id,
        plan_name=args.plan_name,
        requested_duration_sec=args.duration_sec,
        output_dir=args.output_dir,
        strict_100ms=args.strict_100ms,
        create_bundle=args.create_bundle,
        fail_session_on_quality_gate=args.fail_session_on_quality_gate,
        dry_run=args.dry_run,
        simulate_failure=args.simulate_failure,
        notes=args.notes,
    )
    print(f"Phase 5.2 session: {result['session_id']}")
    print(f"Status: {result['status']}")
    print(f"Research eligible: {result['research_eligible']}")
    print(f"Bundle: {result['artifact_paths']['bundle']}")
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())

