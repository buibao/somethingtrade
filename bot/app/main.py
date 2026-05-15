import asyncio

from app.config.settings import get_settings
from app.execution.paper_executor import PaperExecutor


async def run() -> None:
    settings = get_settings()
    if settings.mode != "paper":
        raise RuntimeError("Only MODE=paper is supported in this skeleton.")

    _executor = PaperExecutor()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
