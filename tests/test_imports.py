import importlib


MODULES = [
    "app.config",
    "app.core.clock",
    "app.core.events",
    "app.core.telemetry",
    "app.marketdata.binance_ws",
    "app.marketdata.polymarket_ws",
    "app.marketdata.normalizer",
    "app.state.market_state",
    "app.strategy.mispricing_detector",
    "app.strategy.probability_model",
    "app.risk.risk_gate",
    "app.execution.order_intent",
    "app.execution.paper_executor",
    "app.execution.polymarket_executor",
    "app.logging.event_logger",
    "app.backtest.replay",
    "app.main",
]


def test_module_imports() -> None:
    for module in MODULES:
        assert importlib.import_module(module)
