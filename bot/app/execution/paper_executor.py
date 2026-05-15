from app.core.events import ExecutionReport, ExecutionStatus, OrderIntent


class PaperExecutor:
    """Async paper executor that acknowledges intents without trading."""

    async def submit(self, intent: OrderIntent) -> ExecutionReport:
        return ExecutionReport(
            client_order_id=intent.client_order_id,
            status=ExecutionStatus.ACCEPTED,
            filled_size=0.0,
            raw={"mode": "paper"},
        )
