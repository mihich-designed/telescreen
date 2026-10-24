from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum

from telescreen.domain.errors import InvalidPipelineError, InvalidTransitionError
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


class PipelineStatus(StrEnum):
    """The state an operator wants the pipeline to be in.

    Whether a worker actually streams it right now is runtime information and is
    tracked separately.
    """

    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    DELETING = "deleting"


_TRANSITIONS: dict[PipelineStatus, frozenset[PipelineStatus]] = {
    PipelineStatus.ACTIVE: frozenset(
        {PipelineStatus.PAUSED, PipelineStatus.FAILED, PipelineStatus.DELETING}
    ),
    PipelineStatus.PAUSED: frozenset({PipelineStatus.ACTIVE, PipelineStatus.DELETING}),
    PipelineStatus.FAILED: frozenset({PipelineStatus.ACTIVE, PipelineStatus.DELETING}),
    PipelineStatus.DELETING: frozenset(),
}


@dataclass(slots=True)
class Pipeline:
    name: str
    source: SourceSpec
    sink: SinkSpec
    tables: tuple[TableName, ...]
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: PipelineStatus = PipelineStatus.ACTIVE
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.replace("_", "").replace("-", "").isalnum():
            raise InvalidPipelineError(f"Invalid pipeline name: {self.name!r}")
        if not self.tables:
            raise InvalidPipelineError(f"Pipeline {self.name!r} captures no tables")
        if len(set(self.tables)) != len(self.tables):
            raise InvalidPipelineError(f"Pipeline {self.name!r} lists a table twice")

    def topic_for(self, table: TableName) -> str:
        return f"{self.sink.topic_prefix}.{table.schema}.{table.name}"

    def pause(self) -> None:
        self._move_to(PipelineStatus.PAUSED)

    def resume(self) -> None:
        if self.status is not PipelineStatus.PAUSED:
            raise InvalidTransitionError(f"Pipeline {self.name!r} is {self.status}, not paused")
        self._move_to(PipelineStatus.ACTIVE)

    def fail(self, reason: str) -> None:
        self._move_to(PipelineStatus.FAILED)
        self.last_error = reason

    def retry(self) -> None:
        if self.status is not PipelineStatus.FAILED:
            raise InvalidTransitionError(f"Pipeline {self.name!r} is {self.status}, not failed")
        self._move_to(PipelineStatus.ACTIVE)
        self.last_error = None

    def delete(self) -> None:
        """Ask for removal. A worker drops the slot and the publication first."""
        self._move_to(PipelineStatus.DELETING)

    def _move_to(self, target: PipelineStatus) -> None:
        if target not in _TRANSITIONS[self.status]:
            raise InvalidTransitionError(
                f"Pipeline {self.name!r} cannot go from {self.status} to {target}"
            )
        self.status = target
