from app.core.events import ExecutionReport, ExecutionStatus, OrderIntent


class PolymarketExecutor:
    """Placeholder for live Polymarket execution.

    This skeleton deliberately rejects every order so no real trading can occur.
    """

    async def submit(self, intent: OrderIntent) -> ExecutionReport:
        return ExecutionReport(
            client_order_id=intent.client_order_id,
            status=ExecutionStatus.REJECTED,
            reject_reason="live execution is not implemented",
            raw={"mode": "live_stub"},
        )
