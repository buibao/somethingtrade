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

from app.research.phase52_auto_collection import (  # noqa: E402
    ALL_SESSIONS_BUNDLE,
    AUDIT_BUNDLE,
    DEFAULT_COLLECTION_ROOT,
    FULL_DATASET_BUNDLE,
    MANIFEST_PATH,
    REPORT_JSON_PATH,
    STATUS_PATH,
    run_auto_collection,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Auto-orchestrate Phase 5.2 multi-session data collection.")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--plan-name", default="phase52_24h_default")
    parser.add_argument("--total-budget-hours", type=float, default=24.0)
    parser.add_argument("--output-dir", default=str(DEFAULT_COLLECTION_ROOT))
    parser.add_argument("--strict-100ms", action="store_true")
    parser.add_argument("--create-bundles", action="store_true")
    parser.add_argument("--fail-session-on-quality-gate", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-session-retries", type=int, default=1)
    parser.add_argument("--cooldown-sec", type=int, default=None)
    parser.add_argument("--session-plan-json", default=None)
    parser.add_argument("--stop-after-current-session-file", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-max-sessions", type=int, default=None)
    parser.add_argument("--include-large-datasets", action="store_true")
    parser.add_argument("--allow-nested-zip", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    report = run_auto_collection(
        root=root,
        plan_name=args.plan_name,
        total_budget_hours=args.total_budget_hours,
        output_dir=args.output_dir,
        strict_100ms=args.strict_100ms,
        create_bundles=args.create_bundles,
        fail_session_on_quality_gate=args.fail_session_on_quality_gate,
        resume=args.resume,
        max_session_retries=args.max_session_retries,
        cooldown_sec=args.cooldown_sec,
        session_plan_json=args.session_plan_json,
        stop_after_current_session_file=args.stop_after_current_session_file,
        dry_run=args.dry_run,
        test_max_sessions=args.test_max_sessions,
        include_large_datasets=args.include_large_datasets,
        allow_nested_zip=args.allow_nested_zip,
    )
    print(f"Phase 5.2 manifest: {root / MANIFEST_PATH}")
    print(f"Phase 5.2 status: {root / STATUS_PATH}")
    print(f"Phase 5.2 report: {root / REPORT_JSON_PATH}")
    print(f"Phase 5.2 audit bundle: {root / AUDIT_BUNDLE}")
    if args.include_large_datasets:
        print(f"Phase 5.2 full dataset bundle: {root / FULL_DATASET_BUNDLE}")
    print(f"Phase 5.2 all sessions bundle alias: {root / ALL_SESSIONS_BUNDLE}")
    print(f"Research eligible sessions: {report['manifest']['research_eligible_session_count']}")
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
