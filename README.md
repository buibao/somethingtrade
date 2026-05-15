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

## Status

This project does not implement real trading. Websocket clients, Polymarket execution, and strategy logic are deliberately skeletal so real connectivity, key management, persistence, and production risk controls can be added deliberately.
