from __future__ import annotations

from types import TracebackType
from typing import Any, Self

from sqlalchemy import Row, delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from telescreen.adapters.persistence.tables import pipelines
from telescreen.domain.pipeline import (
    Pipeline,
    PipelineStatus,
    SinkSpec,
    SnapshotMode,
    SourceSpec,
)
from telescreen.domain.table import TableName


def _to_row(pipeline: Pipeline) -> dict[str, Any]:
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "source_dsn": pipeline.source.dsn,
        "publication": pipeline.source.publication,
        "slot": pipeline.source.slot,
        "sink_kind": pipeline.sink.kind,
        "topic_prefix": pipeline.sink.topic_prefix,
        "tables": [str(t) for t in pipeline.tables],
        "status": pipeline.status.value,
        "last_error": pipeline.last_error,
        "snapshot_mode": pipeline.snapshot.value,
        "snapshot_completed": pipeline.snapshot_completed,
    }


def _from_row(row: Row[Any]) -> Pipeline:
    return Pipeline(
        id=row.id,
        name=row.name,
        source=SourceSpec(dsn=row.source_dsn, publication=row.publication, slot=row.slot),
        sink=SinkSpec(kind=row.sink_kind, topic_prefix=row.topic_prefix),
        tables=tuple(TableName.parse(t) for t in row.tables),
        status=PipelineStatus(row.status),
        last_error=row.last_error,
        snapshot=SnapshotMode(row.snapshot_mode),
        snapshot_completed=row.snapshot_completed,
    )


class SqlPipelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, name: str) -> Pipeline | None:
        result = await self._session.execute(select(pipelines).where(pipelines.c.name == name))
        row = result.one_or_none()
        return _from_row(row) if row is not None else None

    async def list(self) -> list[Pipeline]:
        result = await self._session.execute(select(pipelines).order_by(pipelines.c.name))
        return [_from_row(row) for row in result]

    async def add(self, pipeline: Pipeline) -> None:
        await self._session.execute(insert(pipelines).values(_to_row(pipeline)))

    async def save(self, pipeline: Pipeline) -> None:
        # Only the lifecycle is mutable; the definition of a pipeline never changes.
        await self._session.execute(
            update(pipelines)
            .where(pipelines.c.id == pipeline.id)
            .values(
                status=pipeline.status.value,
                last_error=pipeline.last_error,
                snapshot_completed=pipeline.snapshot_completed,
            )
        )

    async def remove(self, pipeline: Pipeline) -> None:
        await self._session.execute(delete(pipelines).where(pipelines.c.id == pipeline.id))


class SqlUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self.pipelines: SqlPipelineRepository

    async def __aenter__(self) -> Self:
        self._session = self._sessions()
        self.pipelines = SqlPipelineRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._session is not None
        await self._session.rollback()
        await self._session.close()
        self._session = None

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()
