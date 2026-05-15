from functools import lru_cache
import os
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Runtime settings loaded from environment variables."""

    binance_ws_url: str = Field("wss://stream.binance.com:9443/ws", alias="BINANCE_WS_URL")
    binance_symbols_csv: str = Field("BTCUSDT,ETHUSDT", alias="BINANCE_SYMBOLS")
    polymarket_gamma_url: str = Field(
        "https://gamma-api.polymarket.com",
        alias="POLYMARKET_GAMMA_URL",
    )
    polymarket_ws_url: str = Field(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        alias="POLYMARKET_WS_URL",
    )
    polymarket_market_cache_path: str = Field(
        "data/cache/polymarket_markets.json",
        alias="POLYMARKET_MARKET_CACHE_PATH",
    )
    polymarket_max_quote_age_ms: float = Field(
        5_000.0,
        alias="POLYMARKET_MAX_QUOTE_AGE_MS",
    )
    gap_log_dir: str = Field("data/logs", alias="GAP_LOG_DIR")
    gap_min_move_pct: float = Field(0.10, alias="GAP_MIN_MOVE_PCT")
    gap_reprice_threshold: float = Field(0.005, alias="GAP_REPRICE_THRESHOLD")
    gap_max_entry_spread: float = Field(0.05, alias="GAP_MAX_ENTRY_SPREAD")
    gap_binance_stale_ms: float = Field(500.0, alias="GAP_BINANCE_STALE_MS")
    gap_polymarket_stale_ms: float = Field(1_000.0, alias="GAP_POLYMARKET_STALE_MS")
    gap_measurement_stale_ms: float = Field(5_000.0, alias="GAP_MEASUREMENT_STALE_MS")
    polymarket_api_key: str = Field("replace-me", alias="POLYMARKET_API_KEY")
    wallet_private_key: str = Field("replace-me", alias="WALLET_PRIVATE_KEY")
    mode: Literal["paper", "live"] = Field("paper", alias="MODE")

    @property
    def binance_symbols(self) -> tuple[str, ...]:
        return tuple(
            symbol.strip().upper()
            for symbol in self.binance_symbols_csv.split(",")
            if symbol.strip()
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    values = {
        key: value
        for key in (
            "BINANCE_WS_URL",
            "BINANCE_SYMBOLS",
            "POLYMARKET_GAMMA_URL",
            "POLYMARKET_WS_URL",
            "POLYMARKET_MARKET_CACHE_PATH",
            "POLYMARKET_MAX_QUOTE_AGE_MS",
            "GAP_LOG_DIR",
            "GAP_MIN_MOVE_PCT",
            "GAP_REPRICE_THRESHOLD",
            "GAP_MAX_ENTRY_SPREAD",
            "GAP_BINANCE_STALE_MS",
            "GAP_POLYMARKET_STALE_MS",
            "GAP_MEASUREMENT_STALE_MS",
            "POLYMARKET_API_KEY",
            "WALLET_PRIVATE_KEY",
            "MODE",
        )
        if (value := os.environ.get(key)) is not None
    }
    return Settings.model_validate(values)
