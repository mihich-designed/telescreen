from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import Protocol, Self

from telescreen.application.wal import WalMessage
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline
from telescreen.domain.table import TableName


class FatalPipelineError(Exception):
    """Retrying will not help; the pipeline needs an operator."""


@dataclass(frozen=True, slots=True)
class SourceCatalog:
    """What the source database told us about the captured tables."""

    primary_keys: Mapping[TableName, tuple[str, ...]] = field(default_factory=dict)


class ReplicationSession(Protocol):
    def messages(self) -> AsyncIterator[WalMessage]:
        """Decoded messages in WAL order. Never ends on its own."""
        ...

    def acknowledge(self, lsn: Lsn) -> None:
        """Tell the server that everything up to ``lsn`` is safely delivered."""
        ...


class ReplicationSource(Protocol):
    async def prepare(self, pipeline: Pipeline) -> SourceCatalog:
        """Make sure the publication and the replication slot exist."""
        ...

    def open(
        self, pipeline: Pipeline, start_lsn: Lsn | None
    ) -> AbstractAsyncContextManager[ReplicationSession]: ...

    async def slot_lag(self, pipeline: Pipeline) -> int | None:
        """Bytes of WAL the slot still holds back, or ``None`` if there is no slot."""
        ...

    async def teardown(self, pipeline: Pipeline) -> None:
        """Drop the slot and the publication so the source stops retaining WAL."""
        ...


@dataclass(frozen=True, slots=True)
class OutgoingRecord:
    topic: str
    key: bytes
    value: bytes
    headers: tuple[tuple[str, bytes], ...] = ()


class EventSink(Protocol):
    async def send(self, record: OutgoingRecord) -> None:
        """Queue a record. Delivery is only guaranteed after :meth:`flush` returns."""
        ...

    async def flush(self) -> None:
        """Wait until every queued record is acknowledged by the broker."""
        ...


class CheckpointStore(Protocol):
    async def load(self, pipeline: str) -> Lsn | None: ...

    async def save(self, pipeline: str, lsn: Lsn) -> None: ...


class SecretResolver(Protocol):
    def resolve(self, reference: str) -> str: ...


class PipelineRepository(Protocol):
    async def get(self, name: str) -> Pipeline | None: ...

    async def list(self) -> list[Pipeline]: ...

    async def add(self, pipeline: Pipeline) -> None: ...

    async def save(self, pipeline: Pipeline) -> None: ...

    async def remove(self, pipeline: Pipeline) -> None: ...


class UnitOfWork(Protocol):
    @property
    def pipelines(self) -> PipelineRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Roll back everything that was not committed."""
        ...

    async def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> UnitOfWork: ...


class SinkFactory(Protocol):
    def __call__(self, pipeline: Pipeline) -> AbstractAsyncContextManager[EventSink]: ...


class RuntimeState(StrEnum):
    STARTING = "starting"
    STREAMING = "streaming"
    RETRYING = "retrying"


@dataclass(frozen=True, slots=True)
class RuntimeStatus:
    """What a worker reports about a pipeline it runs."""

    pipeline: str
    worker: str
    state: RuntimeState
    lag_bytes: int | None = None
    error: str | None = None
    updated_at: datetime | None = None


class RuntimeStore(Protocol):
    async def report(self, status: RuntimeStatus) -> None: ...

    async def all(self) -> dict[str, RuntimeStatus]: ...

    async def clear(self, pipeline: str) -> None: ...
