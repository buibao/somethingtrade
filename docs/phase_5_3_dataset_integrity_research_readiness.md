# Phase 5.3 Dataset Integrity & Research Readiness

Phase 5.3 is an offline audit gate for the restored Phase 5.2 dataset. It does not collect data, train models, run execution, optimize strategies, or claim alpha.

Run from the repository root:

```powershell
python scripts/run_phase53_dataset_integrity_audit.py `
  --repo-root . `
  --phase52-sessions data\phase_5_2\sessions `
  --failed-runs data\cache\phase_5_2_failed_runs `
  --preflight-sessions data\sessions `
  --phase52f-artifacts artifacts\phase_5_2f `
  --backup-meta backup_meta\destroy_safety_backup_20260530T131704Z `
  --output-root data\phase_5_3 `
  --strict
```

Outputs are written only under `data/phase_5_3`:

- `reports`: JSON and Markdown audit reports
- `manifests`: final research-readiness manifest JSON
- `debug`: compact Phase 5.3 debug outputs if produced
- `evidence`: final evidence bundle and SHA256

Interpretation:

- `phase_5_3_pass`: at least one eligible session exists and no audited gate blocks the allowed subset.
- `phase_5_3_partial`: a clearly identified subset may proceed, but exclusions or warnings remain.
- `phase_5_3_fail`: no usable session can be identified, or artifact/timestamp/label integrity blocks the dataset.

Preflight sessions, failed-run lineage, and root `data/dataset` or `data/debug` artifacts are not primary research data by default. The raw `session_005_medium_2h` and `session_005_medium_2h_repaired_eval` directories must remain separate and are classified independently.
