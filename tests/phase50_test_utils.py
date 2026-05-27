from __future__ import annotations

import json
from pathlib import Path

from app.research.microstructure_signal_research import run_phase50


ROOT = Path(__file__).resolve().parents[1]
_PHASE50_RAN = False


def ensure_phase50_outputs() -> Path:
    global _PHASE50_RAN
    if not _PHASE50_RAN:
        run_phase50(ROOT)
        _PHASE50_RAN = True
    return ROOT


def load_json(relative: str) -> dict:
    root = ensure_phase50_outputs()
    return json.loads((root / relative).read_text(encoding="utf-8"))

