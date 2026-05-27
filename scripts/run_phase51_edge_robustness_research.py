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

from app.research.edge_robustness_research import (  # noqa: E402
    PHASE51_BUNDLE,
    PHASE51_FINAL_REPORT_JSON,
    PHASE51_FINAL_REPORT_MD,
    run_phase51,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Phase 5.1 offline edge robustness research.")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--phase50-report", default="data/reports/phase_5_0_empirical_signal_report.json")
    parser.add_argument("--phase50-bundle", default="phase_5_0_empirical_signal_research_bundle.zip")
    parser.add_argument("--input-mode", choices=["phase50_existing_dataset", "single_bundle", "multi_bundle"], default="phase50_existing_dataset")
    parser.add_argument("--bundle-manifest", default=None)
    parser.add_argument("--bundle", action="append", default=[])
    parser.add_argument("--sha256-file", action="append", default=[])
    parser.add_argument("--primary-horizon-ms", type=int, default=100)
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--create-bundle", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    report = run_phase51(
        root=root,
        phase50_report=args.phase50_report,
        phase50_bundle=args.phase50_bundle,
        input_mode=args.input_mode,
        bundle_manifest=args.bundle_manifest,
        bundle_paths=args.bundle,
        sha256_paths=args.sha256_file,
        primary_horizon_ms=args.primary_horizon_ms,
        output_dir=args.output_dir,
        create_bundle=args.create_bundle,
    )
    print(f"Phase 5.1 final report JSON: {root / PHASE51_FINAL_REPORT_JSON}")
    print(f"Phase 5.1 final report MD: {root / PHASE51_FINAL_REPORT_MD}")
    print(f"Phase 5.1 bundle: {root / PHASE51_BUNDLE}")
    print(f"Phase 5.1 decision conclusion: {report['edge_robustness_conclusion']}")
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

