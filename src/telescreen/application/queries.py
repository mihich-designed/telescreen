"""Read side: pipelines together with what workers report about them."""

from __future__ import annotations

from dataclasses import dataclass

from telescreen.application.pipelines import PipelineNotFoundError
from telescreen.application.ports import (
    CheckpointStore,
    RuntimeStatus,
    RuntimeStore,
    UnitOfWorkFactory,
)
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline


@dataclass(frozen=True, slots=True)
class PipelineView:
    pipeline: Pipeline
    runtime: RuntimeStatus | None
    checkpoint: Lsn | None


class PipelineQueries:
    def __init__(
        self, uow: UnitOfWorkFactory, runtime: RuntimeStore, checkpoints: CheckpointStore
    ) -> None:
        self._uow = uow
        self._runtime = runtime
        self._checkpoints = checkpoints

    async def list(self) -> list[PipelineView]:
        async with self._uow() as uow:
            pipelines = await uow.pipelines.list()
        runtime = await self._runtime.all()
        return [
            PipelineView(p, runtime.get(p.name), await self._checkpoints.load(p.name))
            for p in pipelines
        ]

    async def get(self, name: str) -> PipelineView:
        async with self._uow() as uow:
            pipeline = await uow.pipelines.get(name)
        if pipeline is None:
            raise PipelineNotFoundError(f"Pipeline {name!r} not found")
        runtime = await self._runtime.all()
        return PipelineView(pipeline, runtime.get(name), await self._checkpoints.load(name))
