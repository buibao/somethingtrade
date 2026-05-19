from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "bot"
if str(BOT_PATH) not in sys.path:
    sys.path.insert(0, str(BOT_PATH))

from app.research.orderbook_labeled_dataset import run_phase42_pipeline  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Phase 4.2 labeled orderbook dataset.")
    parser.add_argument("--input", default="data/dataset/orderbook_clean_samples.jsonl")
    parser.add_argument("--output", default="data/dataset/orderbook_labeled_samples.jsonl")
    parser.add_argument(
        "--report-json",
        default="data/reports/phase_4_2_dataset_quality_report.json",
    )
    parser.add_argument(
        "--report-md",
        default="data/reports/phase_4_2_dataset_quality_report.md",
    )
    parser.add_argument("--debug-dir", default="data/debug")
    args = parser.parse_args(argv)

    result = run_phase42_pipeline(
        input_path=args.input,
        output_path=args.output,
        report_json_path=args.report_json,
        report_md_path=args.report_md,
        debug_dir=args.debug_dir,
    )
    print(f"Phase 4.2 label generation status: {result.report['status']}")
    for reason in result.report.get("hard_fail_reasons", []):
        print(f"HARD_FAIL: {reason}")
    return 0 if result.report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

