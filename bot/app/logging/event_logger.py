import asyncio
from datetime import UTC, datetime
from pathlib import Path

import orjson
import structlog

from app.core.events import EventModel


def get_logger() -> structlog.BoundLogger:
    return structlog.get_logger("repricing_bot")


def log_event(event: EventModel) -> None:
    get_logger().info("event", **event.model_dump(mode="json"))


class AsyncJsonlEventLogger:
    """Queue-backed JSONL event logger for realtime measurement output."""

    def __init__(self, *, log_dir: str | Path = "data/logs", prefix: str = "gap_events") -> None:
        self.log_dir = Path(log_dir)
        self.prefix = prefix
        self._queue: asyncio.Queue[EventModel | None] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "AsyncJsonlEventLogger":
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        await self.close()

    def start(self) -> None:
        if self._worker is None:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self._worker = asyncio.create_task(self._run())

    async def log(self, event: EventModel) -> None:
        if self._worker is None:
            self.start()
        await self._queue.put(event)

    def log_nowait(self, event: EventModel) -> None:
        if self._worker is None:
            self.start()
        self._queue.put_nowait(event)

    async def close(self) -> None:
        if self._worker is None:
            return
        await self._queue.put(None)
        await self._worker
        self._worker = None

    async def _run(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                if event is None:
                    return
                path = self._path_for_event(event)
                payload = orjson.dumps(event.model_dump(mode="json")) + b"\n"
                await asyncio.to_thread(self._append_bytes, path, payload)
            finally:
                self._queue.task_done()

    def _path_for_event(self, event: EventModel) -> Path:
        event_date = datetime.fromtimestamp(event.ts_ns / 1_000_000_000, UTC)
        return self.log_dir / f"{self.prefix}_{event_date:%Y%m%d}.jsonl"

    @staticmethod
    def _append_bytes(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as file:
            file.write(payload)
