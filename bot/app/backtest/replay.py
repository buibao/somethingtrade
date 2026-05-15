from collections.abc import AsyncIterator, Iterable

from app.core.events import EventModel


async def replay_events(events: Iterable[EventModel]) -> AsyncIterator[EventModel]:
    """Async replay adapter for serialized or in-memory event streams."""

    for event in events:
        yield event
