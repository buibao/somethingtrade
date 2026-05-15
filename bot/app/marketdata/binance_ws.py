from collections.abc import AsyncIterator

from app.core.events import Event


class BinanceWSClient:
    """Async Binance websocket client placeholder."""

    def __init__(self, url: str) -> None:
        self.url = url

    async def stream(self) -> AsyncIterator[Event]:
        """Yield normalized events once real connectivity is implemented."""

        if False:
            yield
