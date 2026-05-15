import structlog

from app.core.events import EventModel


def get_logger() -> structlog.BoundLogger:
    return structlog.get_logger("repricing_bot")


def log_event(event: EventModel) -> None:
    get_logger().info("event", **event.model_dump(mode="json"))
