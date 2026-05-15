from app.core.events import MarketTick, OrderBookTop


class ProbabilityModel:
    """Placeholder probability model.

    Real implementations should be deterministic, measured, and independent of
    the execution layer.
    """

    def fair_probability(
        self,
        *,
        tick: MarketTick | None = None,
        book_top: OrderBookTop | None = None,
    ) -> float | None:
        if tick is None and book_top is None:
            return None
        if book_top is not None:
            mid = (book_top.bid_price + book_top.ask_price) / 2.0
        else:
            mid = tick.price if tick is not None else 0.0
        return max(0.0, min(1.0, mid))
