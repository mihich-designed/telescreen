"""In-memory implementations of the ports. Used by tests and for local experiments."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from types import TracebackType
from typing import Self

from telescreen.application.ports import RuntimeStatus
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self._positions: dict[str, Lsn] = {}

    async def load(self, pipeline: str) -> Lsn | None:
        return self._positions.get(pipeline)

    async def save(self, pipeline: str, lsn: Lsn) -> None:
        self._positions[pipeline] = lsn


class MemoryRuntimeStore:
    def __init__(self) -> None:
        self.statuses: dict[str, RuntimeStatus] = {}

    async def report(self, status: RuntimeStatus) -> None:
        self.statuses[status.pipeline] = RuntimeStatus(
            pipeline=status.pipeline,
            worker=status.worker,
            state=status.state,
            lag_bytes=status.lag_bytes,
            error=status.error,
            updated_at=datetime.now(UTC),
        )

    async def all(self) -> dict[str, RuntimeStatus]:
        return dict(self.statuses)

    async def clear(self, pipeline: str) -> None:
        self.statuses.pop(pipeline, None)


class MemoryPipelineRepository:
    def __init__(self, storage: dict[str, Pipeline]) -> None:
        self._storage = storage

    async def get(self, name: str) -> Pipeline | None:
        pipeline = self._storage.get(name)
        return copy.deepcopy(pipeline) if pipeline is not None else None

    async def list(self) -> list[Pipeline]:
        return [copy.deepcopy(p) for _, p in sorted(self._storage.items())]

    async def add(self, pipeline: Pipeline) -> None:
        self._storage[pipeline.name] = copy.deepcopy(pipeline)

    async def save(self, pipeline: Pipeline) -> None:
        self._storage[pipeline.name] = copy.deepcopy(pipeline)

    async def remove(self, pipeline: Pipeline) -> None:
        self._storage.pop(pipeline.name, None)


class MemoryDatabase:
    """Committed state shared by every unit of work created from it."""

    def __init__(self) -> None:
        self.pipelines: dict[str, Pipeline] = {}

    def unit_of_work(self) -> MemoryUnitOfWork:
        return MemoryUnitOfWork(self)


class MemoryUnitOfWork:
    def __init__(self, database: MemoryDatabase) -> None:
        self._database = database
        self._staged: dict[str, Pipeline] = {}
        self.pipelines: MemoryPipelineRepository
        self.committed = False

    async def __aenter__(self) -> Self:
        self._staged = copy.deepcopy(self._database.pipelines)
        self.pipelines = MemoryPipelineRepository(self._staged)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._staged = {}

    async def commit(self) -> None:
        self._database.pipelines = copy.deepcopy(self._staged)
        self.committed = True
