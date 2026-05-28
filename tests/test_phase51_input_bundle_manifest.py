from __future__ import annotations

from pathlib import Path

from app.research.edge_robustness_research import build_input_bundle_manifest_and_samples
from phase50_test_utils import phase42h_fixture_paths
from phase51_test_utils import ROOT, load_json, write_json


def test_single_bundle_manifest_created() -> None:
    manifest = load_json("data/debug/phase_5_1_input_bundle_manifest.json")
    assert manifest["input_mode"] == "phase50_existing_dataset"
    assert manifest["bundle_count"] == 1
    assert manifest["input_manifest_created"] is True


def test_multi_bundle_manifest_created(tmp_path: Path) -> None:
    manifest_path = tmp_path / "bundles.json"
    write_json(
        manifest_path,
        {
            "bundles": [
                {"bundle_id": "a", "bundle_path": "missing_a.zip", "sha256_path": "missing_a.txt"},
                {"bundle_id": "b", "bundle_path": "missing_b.zip", "sha256_path": "missing_b.txt"},
            ]
        },
    )
    manifest, samples = build_input_bundle_manifest_and_samples(
        root_path=tmp_path,
        phase50_bundle_path=tmp_path / "phase50.zip",
        input_mode="multi_bundle",
        bundle_manifest_path=manifest_path,
        bundle_paths=[],
        sha256_paths=[],
    )
    assert manifest["bundle_count"] == 2
    assert manifest["input_mode"] == "multi_bundle"
    assert samples == []


def test_invalid_bundle_sha256_fails_or_excludes(tmp_path: Path) -> None:
    bundle, _ = phase42h_fixture_paths()
    bad_sha = tmp_path / "bad_sha.txt"
    bad_sha.write_text("sha256: " + ("0" * 64), encoding="utf-8")
    manifest, _ = build_input_bundle_manifest_and_samples(
        root_path=ROOT,
        phase50_bundle_path=ROOT / "phase_5_0_empirical_signal_research_bundle.zip",
        input_mode="single_bundle",
        bundle_paths=[bundle],
        sha256_paths=[bad_sha],
    )
    assert manifest["status"] == "fail"
    assert manifest["bundles"][0]["sha256_valid"] is False


def test_bundle_count_recorded() -> None:
    manifest = load_json("data/debug/phase_5_1_input_bundle_manifest.json")
    assert manifest["bundle_count_recorded"] is True
    assert manifest["bundle_count"] == len(manifest["bundles"])


def test_all_bundles_valid_required_for_expanded_dataset(tmp_path: Path) -> None:
    manifest, _ = build_input_bundle_manifest_and_samples(
        root_path=tmp_path,
        phase50_bundle_path=tmp_path / "missing_phase50.zip",
        input_mode="single_bundle",
        bundle_paths=[tmp_path / "missing.zip"],
        sha256_paths=[tmp_path / "missing.txt"],
    )
    assert manifest["all_bundles_valid"] is False
    assert manifest["status"] == "fail"
