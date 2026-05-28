from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile

from app.research.microstructure_signal_research import (
    DATASET_MEMBERS,
    PHASE42H_BUNDLE_NAME,
    PHASE42H_DATASET_ZIP_MEMBER,
    PHASE42H_REPORT_MEMBER,
    PHASE42H_SHA256_NAME,
    run_phase50,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "data/cache/phase_5_0_test_fixture"
FIXTURE_BUNDLE = FIXTURE_ROOT / PHASE42H_BUNDLE_NAME
FIXTURE_SHA256 = FIXTURE_ROOT / PHASE42H_SHA256_NAME
_PHASE50_RAN = False


def ensure_phase50_outputs() -> Path:
    global _PHASE50_RAN
    if not _PHASE50_RAN:
        bundle, sha256 = phase42h_fixture_paths()
        run_phase50(ROOT, bundle_path=bundle, sha256_path=sha256)
        _PHASE50_RAN = True
    return ROOT


def load_json(relative: str) -> dict:
    root = ensure_phase50_outputs()
    return json.loads((root / relative).read_text(encoding="utf-8"))


def phase42h_fixture_paths() -> tuple[Path, Path]:
    if not FIXTURE_BUNDLE.exists() or not FIXTURE_SHA256.exists():
        create_phase42h_fixture_bundle()
    return FIXTURE_BUNDLE, FIXTURE_SHA256


def create_phase42h_fixture_bundle(*, omitted_outer_member: str | None = None) -> tuple[Path, Path]:
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)
    dataset_zip = FIXTURE_ROOT / "phase_4_2h_latency_profile_datasets_fixture.zip"
    rows = _fixture_rows()
    with zipfile.ZipFile(dataset_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for key, member in DATASET_MEMBERS.items():
            archive.writestr(member, _jsonl(rows[key]))
        archive.writestr(
            "data/debug/phase_4_2h_dataset_zip_file_manifest.json",
            json.dumps({"files": sorted(DATASET_MEMBERS.values())}, indent=2, sort_keys=True) + "\n",
        )

    runtime_report = _runtime_report(sample_count=len(rows["clean_samples"]))
    with zipfile.ZipFile(FIXTURE_BUNDLE, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if omitted_outer_member != PHASE42H_REPORT_MEMBER:
            archive.writestr(PHASE42H_REPORT_MEMBER, json.dumps(runtime_report, indent=2, sort_keys=True) + "\n")
        if omitted_outer_member != PHASE42H_DATASET_ZIP_MEMBER:
            archive.write(dataset_zip, PHASE42H_DATASET_ZIP_MEMBER)
        archive.writestr("data/reports/phase42h_self_check.json", json.dumps({"passed": True}) + "\n")
        archive.writestr(
            "data/debug/phase_4_2h_bundle_file_manifest.json",
            json.dumps(
                {
                    "files": [
                        PHASE42H_REPORT_MEMBER,
                        PHASE42H_DATASET_ZIP_MEMBER,
                        "data/reports/phase42h_self_check.json",
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )

    digest = _sha256(FIXTURE_BUNDLE)
    FIXTURE_SHA256.write_text(f"filename: {FIXTURE_BUNDLE.name}\nsha256: {digest}\n", encoding="utf-8")
    return FIXTURE_BUNDLE, FIXTURE_SHA256


def _fixture_rows(count: int = 120) -> dict[str, list[dict]]:
    base_ns = 1_000_000_000_000
    step_ns = 100_000_000
    nonflat_returns = {
        10: 3.0,
        20: -3.0,
        50: 2.5,
        80: 3.0,
        90: -2.5,
        110: 3.0,
    }
    rows: dict[str, list[dict]] = {key: [] for key in DATASET_MEMBERS}
    for index in range(count):
        ts = base_ns + index * step_ns
        mid = 100.0 + index * 0.001
        best_bid = mid - 0.005
        best_ask = mid + 0.005
        ret = nonflat_returns.get(index, 0.0)
        ref_mid = mid * 1.0001
        rows["clean_samples"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "local_recv_wall_ts": f"2026-01-01T00:00:{index % 60:02d}Z",
                "mid": mid,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "book_age_ms": 1.0,
                "bids": [[best_bid, 10.0], [best_bid - 0.01, 8.0], [best_bid - 0.02, 6.0]],
                "asks": [[best_ask, 10.0], [best_ask + 0.01, 8.0], [best_ask + 0.02, 6.0]],
            }
        )
        rows["corrected_labels"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "local_recv_wall_ts": f"2026-01-01T00:00:{index % 60:02d}Z",
                "feature_mid_price": mid,
                "feature_best_bid": best_bid,
                "feature_best_ask": best_ask,
                "feature_spread_bps": (best_ask - best_bid) / mid * 10_000.0,
                "protocol_labels": {
                    "depth_mid": {
                        "exchange_time": {
                            "valid": True,
                            "horizon_ms": 100,
                            "max_future_gap_ms": 100,
                            "exchange_future_gap_ms": 50.0,
                            "future_reference_local_recv_monotonic_ns": ts + 50_000_000,
                            "return_bps": ret,
                        }
                    }
                },
            }
        )
        rows["bookticker"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "mid_price": ref_mid,
                "spread_bps": 0.5,
            }
        )
        rows["trade"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "price": ref_mid,
                "quantity": 0.25 + (index % 5) * 0.01,
                "is_buyer_market_maker": index % 2 == 0,
            }
        )
        rows["aggtrade"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "price": ref_mid,
                "quantity": 0.20 + (index % 7) * 0.01,
                "is_buyer_market_maker": index % 3 == 0,
            }
        )
        rows["latency_profile"].append(
            {
                "symbol": "BTCUSDT",
                "local_recv_monotonic_ns": ts,
                "metrics": {
                    "end_to_end_local_hot_path_ms": 5.0,
                    "queue_wait_ms": 1.0,
                },
            }
        )
    return rows


def _runtime_report(*, sample_count: int) -> dict:
    return {
        "phase": "4.2H",
        "status": "pass",
        "primary_failure": None,
        "symbol": "BTCUSDT",
        "duration_sec": 1800.0,
        "clock_sync_status": "pass",
        "strict_100ms_observability_ready": True,
        "low_latency_ready": True,
        "phase5_ready": False,
        "phase41_runtime_report_status": "pass",
        "phase41_runtime_report": {
            "phase_4_1_status": "pass",
            "snapshot_copy_budget_met": True,
            "snapshot_copy_p99_us": 75.0,
            "snapshot_copy_budget_us": 100.0,
        },
        "clock_offset_summary": {
            "clock_offset_drift_valid": True,
            "clock_offset_sample_quality_valid": True,
            "accepted_clock_sample_count": 8,
        },
        "clock_sanity_report": {
            "clock_offset_drift_valid": True,
            "clock_offset_sample_quality_valid": True,
        },
        "hot_path_latency_summary": {
            "sample_count": sample_count,
            "metrics": {
                "end_to_end_local_hot_path_ms": {"p99": 5.0},
                "queue_wait_ms": {"p99": 1.0},
            },
        },
        "queue_backpressure_summary": {"dropped_records": 0},
        "writer_batch_report": {"writer_dropped_records": 0, "writer_error_count": 0},
        "capture": {"duration_sec": 1800.0},
    }


def _jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
