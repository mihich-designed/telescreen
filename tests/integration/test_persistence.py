from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from telescreen.adapters.persistence.repositories import SqlUnitOfWork
from telescreen.adapters.persistence.state import SqlCheckpointStore, SqlRuntimeStore
from telescreen.adapters.persistence.tables import metadata
from telescreen.application.ports import RuntimeState, RuntimeStatus
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import PipelineStatus
from tests.unit.test_domain import make_pipeline

DATABASE_URL = os.environ.get("TELESCREEN_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="TELESCREEN_TEST_DATABASE_URL is not set")


@pytest.fixture
async def sessions() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    assert DATABASE_URL
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
    await engine.dispose()


async def test_pipeline_round_trip(sessions: async_sessionmaker[AsyncSession]) -> None:
    pipeline = make_pipeline()
    async with SqlUnitOfWork(sessions) as uow:
        await uow.pipelines.add(pipeline)
        await uow.commit()

    async with SqlUnitOfWork(sessions) as uow:
        loaded = await uow.pipelines.get("shop")
        assert loaded == pipeline
        assert loaded is not None
        loaded.fail("boom")
        await uow.pipelines.save(loaded)
        await uow.commit()

    async with SqlUnitOfWork(sessions) as uow:
        [stored] = await uow.pipelines.list()
        assert stored.status is PipelineStatus.FAILED
        assert stored.last_error == "boom"


async def test_uncommitted_changes_are_rolled_back(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with SqlUnitOfWork(sessions) as uow:
        await uow.pipelines.add(make_pipeline())

    async with SqlUnitOfWork(sessions) as uow:
        assert await uow.pipelines.list() == []


async def test_checkpoints_and_runtime(sessions: async_sessionmaker[AsyncSession]) -> None:
    async with SqlUnitOfWork(sessions) as uow:
        await uow.pipelines.add(make_pipeline())
        await uow.commit()
    checkpoints = SqlCheckpointStore(sessions)
    runtime = SqlRuntimeStore(sessions)

    # A full 64-bit LSN must survive the round trip.
    await checkpoints.save("shop", Lsn(2**64 - 1))
    await checkpoints.save("shop", Lsn.parse("16/B374D848"))
    assert await checkpoints.load("shop") == Lsn.parse("16/B374D848")

    await runtime.report(RuntimeStatus("shop", "w1", RuntimeState.STREAMING, lag_bytes=10))
    [status] = (await runtime.all()).values()
    assert (status.worker, status.state, status.lag_bytes) == ("w1", RuntimeState.STREAMING, 10)
    assert status.updated_at is not None

    await runtime.clear("shop")
    assert await runtime.all() == {}
