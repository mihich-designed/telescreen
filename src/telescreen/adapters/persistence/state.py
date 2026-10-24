"""Frequently written worker state: checkpoints and runtime status.

Each write is a short transaction of its own, independent of any unit of work.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from telescreen.adapters.persistence.tables import checkpoints, runtime_status
from telescreen.application.ports import RuntimeState, RuntimeStatus
from telescreen.domain.lsn import Lsn


class SqlCheckpointStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def load(self, pipeline: str) -> Lsn | None:
        async with self._sessions() as session:
            value = await session.scalar(
                select(checkpoints.c.lsn).where(checkpoints.c.pipeline == pipeline)
            )
        return Lsn(int(value)) if value is not None else None

    async def save(self, pipeline: str, lsn: Lsn) -> None:
        statement = insert(checkpoints).values(pipeline=pipeline, lsn=lsn.value)
        statement = statement.on_conflict_do_update(
            index_elements=[checkpoints.c.pipeline],
            set_={"lsn": statement.excluded.lsn, "updated_at": func.now()},
        )
        async with self._sessions.begin() as session:
            await session.execute(statement)


class SqlRuntimeStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def report(self, status: RuntimeStatus) -> None:
        values = {
            "pipeline": status.pipeline,
            "worker": status.worker,
            "state": status.state.value,
            "lag_bytes": status.lag_bytes,
            "error": status.error,
        }
        statement = insert(runtime_status).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=[runtime_status.c.pipeline],
            set_={**values, "updated_at": func.now()},
        )
        async with self._sessions.begin() as session:
            await session.execute(statement)

    async def all(self) -> dict[str, RuntimeStatus]:
        async with self._sessions() as session:
            result = await session.execute(select(runtime_status))
        return {
            row.pipeline: RuntimeStatus(
                pipeline=row.pipeline,
                worker=row.worker,
                state=RuntimeState(row.state),
                lag_bytes=row.lag_bytes,
                error=row.error,
                updated_at=row.updated_at,
            )
            for row in result
        }

    async def clear(self, pipeline: str) -> None:
        async with self._sessions.begin() as session:
            await session.execute(
                delete(runtime_status).where(runtime_status.c.pipeline == pipeline)
            )
