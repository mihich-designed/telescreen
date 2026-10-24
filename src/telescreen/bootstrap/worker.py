"""Worker process: streams every active pipeline stored in the metadata database."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dishka import make_async_container

from telescreen.application.supervisor import Supervisor
from telescreen.bootstrap.ioc import PersistenceProvider, SourceProvider, WorkerProvider
from telescreen.bootstrap.settings import Settings

logger = logging.getLogger("telescreen")


async def run(settings: Settings) -> None:
    container = make_async_container(
        PersistenceProvider(), SourceProvider(), WorkerProvider(), context={Settings: settings}
    )
    logger.info("worker %s started", settings.worker_id)
    try:
        supervisor = await container.get(Supervisor)
        await supervisor.run()
    finally:
        await container.close()


def main() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(settings))


if __name__ == "__main__":
    main()
