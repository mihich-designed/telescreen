from datetime import datetime
from typing import Self

from pydantic import BaseModel, Field

from telescreen.application.pipelines import CreatePipeline
from telescreen.application.queries import PipelineView


class PipelineIn(BaseModel):
    name: str = Field(examples=["shop"])
    dsn: str = Field(description="Secret reference, e.g. env:SHOP_DSN", examples=["env:SHOP_DSN"])
    tables: list[str] = Field(min_length=1, examples=[["public.orders"]])
    publication: str | None = None
    slot: str | None = None
    sink_kind: str = "kafka"
    topic_prefix: str | None = None
    initial_snapshot: bool = Field(
        default=True, description="Copy existing rows before streaming changes"
    )

    def to_command(self) -> CreatePipeline:
        return CreatePipeline(**self.model_dump())


class SourceOut(BaseModel):
    dsn: str
    publication: str
    slot: str


class SinkOut(BaseModel):
    kind: str
    topic_prefix: str


class SnapshotOut(BaseModel):
    mode: str
    completed: bool


class RuntimeOut(BaseModel):
    state: str
    worker: str
    lag_bytes: int | None
    error: str | None
    updated_at: datetime | None


class PipelineOut(BaseModel):
    name: str
    status: str
    tables: list[str]
    source: SourceOut
    sink: SinkOut
    snapshot: SnapshotOut
    last_error: str | None
    checkpoint: str | None
    runtime: RuntimeOut | None

    @classmethod
    def from_view(cls, view: PipelineView) -> Self:
        pipeline, runtime = view.pipeline, view.runtime
        return cls(
            name=pipeline.name,
            status=pipeline.status.value,
            tables=[str(t) for t in pipeline.tables],
            source=SourceOut(
                dsn=pipeline.source.dsn,
                publication=pipeline.source.publication,
                slot=pipeline.source.slot,
            ),
            sink=SinkOut(kind=pipeline.sink.kind, topic_prefix=pipeline.sink.topic_prefix),
            snapshot=SnapshotOut(
                mode=pipeline.snapshot.value, completed=pipeline.snapshot_completed
            ),
            last_error=pipeline.last_error,
            checkpoint=str(view.checkpoint) if view.checkpoint else None,
            runtime=RuntimeOut(
                state=runtime.state.value,
                worker=runtime.worker,
                lag_bytes=runtime.lag_bytes,
                error=runtime.error,
                updated_at=runtime.updated_at,
            )
            if runtime
            else None,
        )
