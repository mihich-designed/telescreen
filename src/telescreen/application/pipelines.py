"""Commands that manage the lifecycle of pipelines."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from telescreen.application.ports import UnitOfWorkFactory
from telescreen.domain.errors import InvalidPipelineError
from telescreen.domain.pipeline import Pipeline, SinkSpec, SnapshotMode, SourceSpec
from telescreen.domain.table import TableName


class PipelineNotFoundError(LookupError):
    pass


class PipelineAlreadyExistsError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CreatePipeline:
    name: str
    dsn: str
    tables: Sequence[str]
    publication: str | None = None
    slot: str | None = None
    sink_kind: str = "kafka"
    topic_prefix: str | None = None
    # Copy existing rows before streaming changes.
    initial_snapshot: bool = True


def default_replication_name(pipeline: str) -> str:
    """Publication and slot name used when none is given.

    Slot names may only contain lower case letters, digits and underscores.
    """
    return "telescreen_" + pipeline.lower().replace("-", "_")


class PipelineService:
    def __init__(self, uow: UnitOfWorkFactory, sink_kinds: frozenset[str]) -> None:
        self._uow = uow
        self._sink_kinds = sink_kinds

    async def create(self, command: CreatePipeline) -> Pipeline:
        if command.sink_kind not in self._sink_kinds:
            raise InvalidPipelineError(f"Unknown sink kind {command.sink_kind!r}")
        default = default_replication_name(command.name)
        try:
            tables = tuple(TableName.parse(t.strip()) for t in command.tables)
        except ValueError as exc:
            raise InvalidPipelineError(str(exc)) from exc
        pipeline = Pipeline(
            name=command.name,
            source=SourceSpec(
                dsn=command.dsn,
                publication=command.publication or default,
                slot=command.slot or default,
            ),
            sink=SinkSpec(
                kind=command.sink_kind, topic_prefix=command.topic_prefix or command.name
            ),
            tables=tables,
            snapshot=SnapshotMode.INITIAL if command.initial_snapshot else SnapshotMode.NEVER,
        )
        async with self._uow() as uow:
            existing = await uow.pipelines.list()
            for other in existing:
                if other.name == pipeline.name:
                    raise PipelineAlreadyExistsError(f"Pipeline {pipeline.name!r} already exists")
                if other.source.slot == pipeline.source.slot:
                    raise PipelineAlreadyExistsError(
                        f"Slot {pipeline.source.slot!r} is used by pipeline {other.name!r}"
                    )
                if other.source.publication == pipeline.source.publication:
                    raise PipelineAlreadyExistsError(
                        f"Publication {pipeline.source.publication!r} is used by "
                        f"pipeline {other.name!r}"
                    )
            await uow.pipelines.add(pipeline)
            await uow.commit()
        return pipeline

    async def pause(self, name: str) -> Pipeline:
        return await self._change(name, Pipeline.pause)

    async def resume(self, name: str) -> Pipeline:
        return await self._change(name, Pipeline.resume)

    async def retry(self, name: str) -> Pipeline:
        return await self._change(name, Pipeline.retry)

    async def delete(self, name: str) -> Pipeline:
        return await self._change(name, Pipeline.delete)

    async def _change(self, name: str, action: Callable[[Pipeline], None]) -> Pipeline:
        async with self._uow() as uow:
            pipeline = await uow.pipelines.get(name)
            if pipeline is None:
                raise PipelineNotFoundError(f"Pipeline {name!r} not found")
            action(pipeline)
            await uow.pipelines.save(pipeline)
            await uow.commit()
        return pipeline
