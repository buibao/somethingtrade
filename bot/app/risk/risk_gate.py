from dataclasses import dataclass

from app.core.events import OrderIntent, SignalEvent


@dataclass(frozen=True, slots=True)
class RiskGate:
    """Tiny risk gate placeholder for paper-mode intent creation."""

    max_order_size: float = 1.0
    min_confidence: float = 0.0

    def approve(self, signal: SignalEvent) -> bool:
        return signal.confidence >= self.min_confidence

    def to_order_intent(self, signal: SignalEvent, *, size: float) -> OrderIntent | None:
        if not self.approve(signal):
            return None

        capped_size = min(size, self.max_order_size)
        return OrderIntent(
            strategy_id=signal.strategy_id,
            market_id=signal.market_id,
            token_id=signal.token_id,
            side=signal.side,
            limit_price=signal.quoted_probability,
            size=capped_size,
            reason=f"edge_bps={signal.edge_bps:.2f}",
        )
