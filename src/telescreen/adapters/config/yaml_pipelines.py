"""Pipeline definitions read from a YAML file, for bulk import."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from telescreen.application.pipelines import CreatePipeline


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceModel(_Model):
    dsn: str
    publication: str | None = None
    slot: str | None = None


class SinkModel(_Model):
    kind: str = "kafka"
    topic_prefix: str | None = None


class PipelineModel(_Model):
    name: str
    source: SourceModel
    tables: list[str] = Field(min_length=1)
    sink: SinkModel = SinkModel()

    def to_command(self) -> CreatePipeline:
        return CreatePipeline(
            name=self.name,
            dsn=self.source.dsn,
            tables=self.tables,
            publication=self.source.publication,
            slot=self.source.slot,
            sink_kind=self.sink.kind,
            topic_prefix=self.sink.topic_prefix,
        )


class PipelinesFile(_Model):
    pipelines: list[PipelineModel]


def load_pipelines(path: Path) -> list[CreatePipeline]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [p.to_command() for p in PipelinesFile.model_validate(data).pipelines]
