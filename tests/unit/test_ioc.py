"""The dependency graphs of every process resolve without touching any database."""

from dishka import make_async_container

from telescreen.application.pipelines import PipelineService
from telescreen.application.queries import PipelineQueries
from telescreen.application.supervisor import Supervisor
from telescreen.bootstrap.ioc import (
    PersistenceProvider,
    ServicesProvider,
    SourceProvider,
    WorkerProvider,
)
from telescreen.bootstrap.settings import Settings

SETTINGS = Settings(database_url="postgresql+asyncpg://user:secret@db.invalid/telescreen")


async def test_web_graph() -> None:
    container = make_async_container(
        PersistenceProvider(), ServicesProvider(), context={Settings: SETTINGS}
    )
    try:
        assert isinstance(await container.get(PipelineService), PipelineService)
        assert isinstance(await container.get(PipelineQueries), PipelineQueries)
    finally:
        await container.close()


async def test_worker_graph() -> None:
    container = make_async_container(
        PersistenceProvider(), SourceProvider(), WorkerProvider(), context={Settings: SETTINGS}
    )
    try:
        assert isinstance(await container.get(Supervisor), Supervisor)
    finally:
        await container.close()
