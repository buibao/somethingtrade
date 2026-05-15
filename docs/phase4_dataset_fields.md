# Phase 4 Dataset Field Guide

This guide describes how Phase 3.10 `TradableGapObservation` JSONL rows should be interpreted for later empirical modeling. It does not define or implement a Phase 4 probability model.

## Primary Target Fields

- `reject_stage`: final stage for the observation: `none`, `pre_entry`, `window`, `exit`, `lifecycle`, or `timeout`.
- `reject_reason`: final summary reason for non-success observations.
- `quote_was_fillable`: whether the selected token was enterable at detection under Phase 3 filters.
- `mid_repricing_delay_ms`: delay until the Polymarket mid probability moved in the expected direction.
- `executable_repricing_delay_ms`: delay until the selected token had an executable exit bid above `entry_ask + GAP_MIN_EXIT_EDGE`.
- `tradable_window_ms`: measured duration of the stale enterable quote window.
- `exit_edge_after_spread`: executable exit bid minus entry ask.
- `executable_exit_bid`: first best bid that satisfied executable repricing.
- `tick_size_at_detection`: market tick size used for measurement normalization.
- `spread_ticks_at_detection`: detection spread in ticks.
- `exit_edge_ticks`: executable edge in ticks.
- `effective_reprice_threshold_ticks`: max configured repricing threshold and tick size, expressed in ticks.
- `data_quality_tier`: A/B/C/D row-quality tier.
- `data_quality_reason`: concise reason for the assigned quality tier.

## Recommended Binary Labels

- `is_fillable_at_detection = quote_was_fillable`
- `did_mid_reprice = mid_repricing_delay_ms is not None`
- `did_executable_reprice = executable_repricing_delay_ms is not None and reject_stage == "none"`
- `did_timeout = reject_stage == "timeout"`
- `did_window_fail = reject_stage == "window"`
- `did_lifecycle_fail = reject_stage == "lifecycle"`

## Recommended Regression Targets

- `executable_repricing_delay_ms`
- `tradable_window_ms`
- `exit_edge_after_spread`
- `exit_edge_ticks`

Use censoring-aware treatment for timeout, lifecycle, and window-failure rows. They are valid measurement outcomes, not missing data.

## Fields To Treat Carefully

- `estimated_edge_raw`: this is only `current_mid - before_mid`; it is not executable edge.
- `estimated_edge_ticks`: tick-normalized form of `estimated_edge_raw`; it is still not executable edge.
- `estimated_edge_after_spread`: this is set only for successful executable repricing.
- `repricing_delay_ms`: backward-compatible alias for `executable_repricing_delay_ms`; prefer `executable_repricing_delay_ms` in new code.
- `validation_mode="diagnostic"`: useful for debugging orderbook sequencing, but not clean ground truth by default.

## Data Quality Tiers

- `A`: primary clean dataset tier.
- `B`: usable for Phase 4 research; includes tolerated one-tick reported-best mismatches.
- `C`: sensitivity-analysis tier with weaker validation or diagnostic-mode caveats.
- `D`: reject/diagnostic tier, not recommended as clean training labels.

Phase 3.16 makes `validation_mode="tolerant"` with `validation_tolerance_ticks=1` the default research mode. Strict mode is an audit/safety comparison. Diagnostic mode is for debugging only.

## Dataset Quality Report

Run:

```bash
python -m app.main dataset-quality-report \
  --input data/logs/<your_gap_events_file>.jsonl \
  --output data/reports/dataset_quality_latest.json \
  --markdown-output data/reports/dataset_quality_latest.md \
  --csv-dir data/reports/dataset_quality_latest_csv \
  --primary-min-tier B
```

Before Phase 4, prefer runs with at least 1000 rows, non-zero successes, low D-tier share, low `book_incomplete`, low `quote_stale`, and mostly tolerant/strict validation modes.

See `docs/phase4_dataset_quality_report.md` for the Phase 4.0 readiness classification, CSV layout, and empirical-bucket interpretation.

## Non-Goals

This guide does not introduce online learning, paper trading, live execution, private key handling, database writes in the realtime path, or pandas in the realtime path.
