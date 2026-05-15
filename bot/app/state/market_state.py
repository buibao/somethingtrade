from app.core.events import MarketTick, OrderBookTop, PolymarketQuote


class MarketState:
    """Minimal in-memory state for the latest normalized market events."""

    def __init__(self) -> None:
        self.ticks: dict[str, MarketTick] = {}
        self.book_tops: dict[str, OrderBookTop] = {}
        self.polymarket_quotes: dict[str, PolymarketQuote] = {}

    def apply(self, event: MarketTick | OrderBookTop | PolymarketQuote) -> None:
        if isinstance(event, MarketTick):
            self.ticks[event.symbol] = event
        elif isinstance(event, OrderBookTop):
            self.book_tops[event.symbol] = event
        elif isinstance(event, PolymarketQuote):
            self.polymarket_quotes[event.token_id] = event
