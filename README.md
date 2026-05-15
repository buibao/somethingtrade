# Polymarket/Binance Probabilistic Repricing Bot

Python 3.12 skeleton for an async-first cross-market repricing bot. The code is intentionally non-trading: it defines event contracts, module boundaries, state flow, paper execution stubs, and tests without placing real orders.

## Architecture

The runtime is organized around JSON-serializable event objects in `app.core.events`.

- `marketdata`: async websocket client stubs and raw message normalizers.
- `state`: in-memory market state updated by normalized events.
- `strategy`: probability and mispricing signal generation.
- `risk`: gates strategy signals before execution intent creation.
- `execution`: order intent models, paper execution, and Polymarket execution stubs.
- `logging`: structured event logging.
- `backtest`: replay helpers for serialized events.
- `core`: shared clocks, telemetry, and event schemas.

Strategy and execution are decoupled by event contracts. A strategy emits a `SignalEvent`; risk may convert an approved signal into an `OrderIntent`; executors consume only `OrderIntent` and produce `ExecutionReport`. This keeps the system Rust-ready because the module boundary is plain JSON rather than Python object references.

## Why No LLM In The Realtime Path

The realtime execution path must be deterministic, bounded-latency, and easy to reason about under load. LLM calls are intentionally excluded from marketdata, strategy, risk, and execution loops because they add network latency, nondeterminism, provider dependency risk, and difficult-to-audit behavior.

LLMs can still be useful outside the hot path for research, post-trade analysis, documentation, log summarization, and offline strategy review. They should not decide, approve, or route live orders.

## Quickstart

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
pytest
```

Copy `.env.example` to `.env` for local configuration. The default `MODE=paper` is the only implemented execution mode in this skeleton.

## Binance Monitor

Phase 1 includes websocket-first Binance ingestion for `aggTrade`, `bookTicker`, and `depth@100ms` combined streams. It normalizes messages into `MarketTick`, `OrderBookTop`, and `DepthUpdate`, updates in-memory state, and tracks exchange/local/parse/state timestamps plus latency.

```bash
python -m app.main binance-monitor
python -m app.main binance-monitor --symbols BTCUSDT,ETHUSDT
```

The monitor prints compact state once per second. It uses reconnect with exponential backoff and websocket heartbeat pings. No database is used in the realtime path.

## Polymarket Monitor

Phase 2 discovers active BTC/ETH short-duration up/down style markets through public Gamma market data, caches public metadata under `data/cache/`, subscribes to the public CLOB market websocket, and stores latest quotes in memory. The websocket path now maintains a local per-token CLOB book so displayed bid/ask sizes come from applied book state, not standalone `price_change` fields.

```bash
python -m app.main polymarket-monitor
```

See `docs/polymarket_notes.md` for API assumptions and cache contents. Real order execution remains unimplemented.

## Gap Monitor

Phase 3.9 measures Binance-led repricing gaps and writes completed observations as JSONL under `data/logs/`. It separates mid repricing from executable repricing, measures the true fillable stale-quote window, and records reject stage/reason taxonomy for modeling.

Phase 3.10 hardens those measurement semantics before Phase 4. It locks delayed executable repricing after mid repricing in tests, documents timeout precedence, documents the runtime invariant that `state.apply(event)` must run before `detector.on_market_event(event, state)`, and adds a Phase 4 dataset field guide. It still does not add trading, wallet, or private-key logic.

```bash
python -m app.main gap-monitor
```

See `docs/phase3_gap_measurement.md` for interpretation guidance and `docs/phase4_dataset_fields.md` for dataset-field usage. The monitor does not place orders, and a measured gap is not a live trading signal.

## Status

This project does not implement real trading. Websocket clients, Polymarket execution, and strategy logic are deliberately skeletal so real connectivity, key management, persistence, and production risk controls can be added deliberately.
