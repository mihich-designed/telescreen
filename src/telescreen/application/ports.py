from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Protocol

from telescreen.application.wal import WalMessage
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline
from telescreen.domain.table import TableName


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
