from functools import lru_cache
import os
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Runtime settings loaded from environment variables."""

    binance_ws_url: str = Field(..., alias="BINANCE_WS_URL")
    polymarket_ws_url: str = Field(..., alias="POLYMARKET_WS_URL")
    polymarket_api_key: str = Field(..., alias="POLYMARKET_API_KEY")
    wallet_private_key: str = Field(..., alias="WALLET_PRIVATE_KEY")
    mode: Literal["paper", "live"] = Field("paper", alias="MODE")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    return Settings.model_validate(
        {
            "BINANCE_WS_URL": os.environ.get("BINANCE_WS_URL"),
            "POLYMARKET_WS_URL": os.environ.get("POLYMARKET_WS_URL"),
            "POLYMARKET_API_KEY": os.environ.get("POLYMARKET_API_KEY"),
            "WALLET_PRIVATE_KEY": os.environ.get("WALLET_PRIVATE_KEY"),
            "MODE": os.environ.get("MODE", "paper"),
        }
    )
