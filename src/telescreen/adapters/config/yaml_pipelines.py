"""Pipeline definitions read from a YAML file."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from telescreen.domain.pipeline import Pipeline, SinkSpec, SourceSpec
from telescreen.domain.table import TableName


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

    def to_domain(self) -> Pipeline:
        default = f"telescreen_{self.name.replace('-', '_')}"
        return Pipeline(
            name=self.name,
            source=SourceSpec(
                dsn=self.source.dsn,
                publication=self.source.publication or default,
                slot=self.source.slot or default,
            ),
            sink=SinkSpec(kind=self.sink.kind, topic_prefix=self.sink.topic_prefix or self.name),
            tables=tuple(TableName.parse(t) for t in self.tables),
        )


class PipelinesFile(_Model):
    pipelines: list[PipelineModel]


def load_pipelines(path: Path) -> list[Pipeline]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [p.to_domain() for p in PipelinesFile.model_validate(data).pipelines]
