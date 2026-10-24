from __future__ import annotations

from dataclasses import dataclass

from telescreen.domain.errors import InvalidPipelineError
from telescreen.domain.table import TableName


@dataclass(frozen=True, slots=True)
class SourceSpec:
    """Where to read changes from.

    ``dsn`` is a reference to a secret (for example ``env:SHOP_DSN``), never the
    connection string itself.
    """

    dsn: str
    publication: str
    slot: str


@dataclass(frozen=True, slots=True)
class SinkSpec:
    kind: str
    topic_prefix: str


@dataclass(slots=True)
class Pipeline:
    name: str
    source: SourceSpec
    sink: SinkSpec
    tables: tuple[TableName, ...]

    def __post_init__(self) -> None:
        if not self.name or not self.name.replace("_", "").replace("-", "").isalnum():
            raise InvalidPipelineError(f"Invalid pipeline name: {self.name!r}")
        if not self.tables:
            raise InvalidPipelineError(f"Pipeline {self.name!r} captures no tables")
        if len(set(self.tables)) != len(self.tables):
            raise InvalidPipelineError(f"Pipeline {self.name!r} lists a table twice")

    def topic_for(self, table: TableName) -> str:
        return f"{self.sink.topic_prefix}.{table.schema}.{table.name}"
