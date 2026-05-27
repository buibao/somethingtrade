from __future__ import annotations

import json
from pathlib import Path

from app.research.edge_robustness_research import run_phase51


ROOT = Path(__file__).resolve().parents[1]
_PHASE51_RAN = False


def ensure_phase51_outputs() -> Path:
    global _PHASE51_RAN
    if not _PHASE51_RAN:
        run_phase51(ROOT, create_bundle=True)
        _PHASE51_RAN = True
    return ROOT


def load_json(relative: str) -> dict:
    root = ensure_phase51_outputs()
    return json.loads((root / relative).read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")

