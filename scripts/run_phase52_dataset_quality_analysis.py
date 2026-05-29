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

from app.research.phase52_dataset_quality_analysis import (  # noqa: E402
    DEFAULT_OUTPUT_JSON,
    DEFAULT_OUTPUT_MD,
    DEFAULT_SESSIONS_ROOT,
    run_phase52_dataset_quality_analysis,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze completed Phase 5.2 session dataset quality.")
    parser.add_argument("--sessions-root", default=str(DEFAULT_SESSIONS_ROOT))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--output-md", default=str(DEFAULT_OUTPUT_MD))
    args = parser.parse_args(argv)

    report = run_phase52_dataset_quality_analysis(
        sessions_root=args.sessions_root,
        output_json=args.output_json,
        output_md=args.output_md,
    )
    print(f"Phase 5.2 dataset quality analysis JSON: {Path(args.output_json)}")
    print(f"Phase 5.2 dataset quality analysis MD: {Path(args.output_md)}")
    print(f"Status: {report.get('status')}")
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
