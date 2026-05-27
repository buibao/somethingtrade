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

from app.research.microstructure_signal_research import (  # noqa: E402
    PHASE42H_BUNDLE_NAME,
    PHASE42H_SHA256_NAME,
    PHASE50_BUNDLE,
    PHASE50_FINAL_REPORT_JSON,
    PHASE50_FINAL_REPORT_MD,
    run_phase50,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Phase 5.0 offline microstructure empirical signal research.")
    parser.add_argument("--root", default=str(SOURCE_ROOT))
    parser.add_argument("--bundle", default=PHASE42H_BUNDLE_NAME)
    parser.add_argument("--sha256-file", default=PHASE42H_SHA256_NAME)
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    report = run_phase50(root=root, bundle_path=args.bundle, sha256_path=args.sha256_file)
    print(f"Phase 5.0 final report JSON: {root / PHASE50_FINAL_REPORT_JSON}")
    print(f"Phase 5.0 final report MD: {root / PHASE50_FINAL_REPORT_MD}")
    print(f"Phase 5.0 bundle: {root / PHASE50_BUNDLE}")
    print(f"Edge conclusion: {report['edge_conclusion']}")
    return 0 if report["edge_conclusion"] in {"EDGE_PROVEN", "EDGE_INCONCLUSIVE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

