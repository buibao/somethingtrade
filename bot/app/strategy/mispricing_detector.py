from app.core.events import PolymarketQuote, Side, SignalEvent


class MispricingDetector:
    """Converts fair-value estimates and quotes into strategy signals."""

    def __init__(self, strategy_id: str = "baseline_mispricing", min_edge_bps: float = 25.0) -> None:
        self.strategy_id = strategy_id
        self.min_edge_bps = min_edge_bps

    def evaluate(
        self,
        *,
        quote: PolymarketQuote,
        fair_probability: float,
    ) -> SignalEvent | None:
        buy_edge_bps = (fair_probability - quote.ask_probability) * 10_000.0
        sell_edge_bps = (quote.bid_probability - fair_probability) * 10_000.0

        if buy_edge_bps >= self.min_edge_bps:
            return SignalEvent(
                strategy_id=self.strategy_id,
                market_id=quote.market_id,
                token_id=quote.token_id,
                side=Side.BUY,
                fair_probability=fair_probability,
                quoted_probability=quote.ask_probability,
                edge_bps=buy_edge_bps,
                confidence=0.0,
                features={"source_sequence": quote.sequence},
            )

        if sell_edge_bps >= self.min_edge_bps:
            return SignalEvent(
                strategy_id=self.strategy_id,
                market_id=quote.market_id,
                token_id=quote.token_id,
                side=Side.SELL,
                fair_probability=fair_probability,
                quoted_probability=quote.bid_probability,
                edge_bps=sell_edge_bps,
                confidence=0.0,
                features={"source_sequence": quote.sequence},
            )

        return None
