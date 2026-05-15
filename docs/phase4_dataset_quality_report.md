# Phase 4.0 Dataset Quality Report

Phase 4.0 is an offline dataset-quality and empirical-calibration phase for Phase 3 Binance/Polymarket repricing observations. It decides whether the data is clean, stable, and large enough for future market microstructure research.

It does not train a model, produce a trading signal, place orders, handle private keys, or add LLM calls to the realtime path.

## Run The Report

```bash
python -m app.main dataset-quality-report \
  --input data/logs/<gap_events_file>.jsonl \
  --output data/reports/dataset_quality_latest.json \
  --markdown-output data/reports/dataset_quality_latest.md \
  --csv-dir data/reports/dataset_quality_latest_csv \
  --primary-min-tier B
```

Useful options:

- `--min-quality-tier A|B|C|D`: include only rows at that tier or better in the main report.
- `--primary-min-tier A|B`: choose the clean cohort used for primary empirical buckets. The default is `B`, meaning A/B.
- `--include-diagnostic`: allow diagnostic validation rows into primary empirical buckets if they otherwise pass the tier threshold.
- `--mismatch-samples data/debug/polymarket_orderbook_mismatch_samples.jsonl`: add tick-level mismatch calibration from diagnostic samples.
- `--no-markdown`: skip markdown output.
- `--no-csv`: skip CSV output.
- `--fail-on-readiness NOT_READY|NEEDS_MORE_DATA|NEEDS_MORE_CLEANING`: exit nonzero when readiness is at or below the selected threshold.

The CLI reads JSONL safely, skips blank lines, records malformed JSON lines with line numbers, creates output directories, and writes deterministic JSON with sorted keys and indentation. It uses only the Python standard library for the Phase 4 report path.

## Output Files

- `dataset_quality_report.json`: machine-readable report with audits, health summaries, tier and validation-mode analysis, timing and edge distributions, empirical buckets, cohort sensitivity, and readiness.
- `dataset_quality_report.md`: human-readable report for review notes.
- CSV directory:
  - `cohort_summary.csv`
  - `reject_taxonomy.csv`
  - `quality_tier_summary.csv`
  - `validation_mode_summary.csv`
  - `timing_summary.csv`
  - `edge_summary.csv`
  - `empirical_buckets.csv`
  - `readiness_checks.csv`
  - `malformed_rows.csv`, when malformed JSON lines are present

## A/B/C/D Tiers

- `A`: clean validated row.
- `B`: usable research row with a tolerated one-tick mismatch or minor caveat.
- `C`: sensitivity-analysis row.
- `D`: reject or diagnostic evidence.

Primary analysis defaults to A/B. Tier C is for sensitivity checks only. Tier D rows are useful evidence for data-quality failure modes, but they are not clean empirical-bucket input.

## Success Is Not Profitability

`success_count` means measured executable repricing success under the Phase 3 measurement assumptions. It does not mean a trade was profitable, executable at size, queue-position safe, fee-adjusted, or live-tradable.

Edge fields such as `exit_edge_after_spread` and `exit_edge_ticks` are measured edge before full fee, slippage, queue, cancellation, and market-resolution modeling.

## Empirical Buckets Are Not Predictions

Empirical buckets group historical rows by features such as Binance move size, spread ticks, tradable window, executable repricing delay, validation tier, and symbol/direction cohort. Their rates are historical measured rates in this dataset.

They are not probability forecasts, model outputs, trading signals, or execution recommendations.

## Readiness Classifications

- `NOT_READY`: parsed rows are zero, primary rows are zero, or core schema fields are missing.
- `NEEDS_MORE_DATA`: primary clean rows or measured success rows are below Phase 4 thresholds.
- `NEEDS_MORE_CLEANING`: tier D share, stale quotes, book incompleteness, or cohort instability is too high.
- `READY_FOR_EMPIRICAL_RESEARCH`: the dataset is suitable for descriptive microstructure empirical research.
- `READY_FOR_BASELINE_MODEL_RESEARCH`: reserved for substantially cleaner and larger datasets with stable buckets and only minor warnings.

The Phase 4 implementation is intentionally conservative and should not mark a dataset model-ready unless it clearly passes stronger checks.

## Recommended Next Phases

The report recommends one of:

- `Collect more Phase 3 data`
- `Fix order book/data quality issues`
- `Run Phase 4.1 cleaning and label refinement`
- `Run Phase 5.0 microstructure empirical signal research`
- `Do not proceed to model prediction yet`

For most early datasets, expect the recommendation to be more collection or cleaning before any model-prediction work.

## Non-Goals Confirmed

- No model prediction was added.
- No ML training was added.
- No trading signal was added.
- No live execution was added.
- No private-key handling was added.
- No wallet copy trading or on-chain wallet logic was added.
- No LLM was added to the realtime path.
