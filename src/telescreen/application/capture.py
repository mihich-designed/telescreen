"""The core use case: turn a stream of WAL messages into delivered change events.

Delivery is at-least-once. A position in the WAL is acknowledged to PostgreSQL only
after every event up to that position has been accepted by the broker, so a crash
at any point leads to redelivery, never to loss.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from telescreen.application.envelope import to_record
from telescreen.application.ports import (
    CheckpointStore,
    EventSink,
    ReplicationSession,
    SourceCatalog,
)
from telescreen.application.wal import (
    UNCHANGED_TOAST,
    BeginMessage,
    CommitMessage,
    DeleteMessage,
    Heartbeat,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    TupleValues,
    UpdateMessage,
    WalMessage,
)
from telescreen.domain.changes import ChangeEvent, Operation, Row
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline

logger = logging.getLogger(__name__)

type RowChange = InsertMessage | UpdateMessage | DeleteMessage | TruncateMessage


class ProtocolViolationError(RuntimeError):
    """The replication stream did not follow the expected message order."""


@dataclass(frozen=True, slots=True)
class FlushPolicy:
    # Acknowledge after this many committed transactions...
    max_transactions: int = 100
    # ...or once this many events are waiting for the broker, whichever comes first.
    max_events: int = 10_000


class ChangeCapture:
    def __init__(
        self,
        pipeline: Pipeline,
        catalog: SourceCatalog,
        session: ReplicationSession,
        sink: EventSink,
        checkpoints: CheckpointStore,
        policy: FlushPolicy | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._catalog = catalog
        self._session = session
        self._sink = sink
        self._checkpoints = checkpoints
        self._policy = policy or FlushPolicy()

        self._relations: dict[int, RelationMessage] = {}
        self._transaction: BeginMessage | None = None
        self._committed_lsn: Lsn | None = None
        self._pending_transactions = 0
        self._pending_events = 0

    async def run(self) -> None:
        async for message in self._session.messages():
            await self.handle(message)

    async def handle(self, message: WalMessage) -> None:
        match message:
            case RelationMessage():
                self._relations[message.oid] = message
            case BeginMessage():
                if self._transaction is not None:
                    raise ProtocolViolationError("BEGIN inside an open transaction")
                self._transaction = message
            case CommitMessage():
                if self._transaction is None:
                    raise ProtocolViolationError("COMMIT without BEGIN")
                self._transaction = None
                self._committed_lsn = message.end_lsn
                self._pending_transactions += 1
                if self._pending_transactions >= self._policy.max_transactions:
                    await self.flush()
            case Heartbeat():
                if self._transaction is None and self._committed_lsn is not None:
                    await self.flush()
            case InsertMessage() | UpdateMessage() | DeleteMessage() | TruncateMessage():
                for event in self._to_events(message):
                    await self._sink.send(to_record(self._pipeline, event))
                    self._pending_events += 1
                if self._pending_events >= self._policy.max_events:
                    await self.flush()

    async def flush(self) -> None:
        """Wait for the broker and acknowledge the last fully delivered transaction."""
        await self._sink.flush()
        self._pending_events = 0
        lsn = self._committed_lsn
        if lsn is None:
            return
        await self._checkpoints.save(self._pipeline.name, lsn)
        self._session.acknowledge(lsn)
        self._committed_lsn = None
        self._pending_transactions = 0
        logger.debug("pipeline %s acknowledged %s", self._pipeline.name, lsn)

    def _to_events(self, message: RowChange) -> list[ChangeEvent]:
        if self._transaction is None:
            raise ProtocolViolationError(f"{type(message).__name__} outside a transaction")

        match message:
            case TruncateMessage():
                return [
                    self._event(Operation.TRUNCATE, self._relation(oid), None, None, None)
                    for oid in message.relation_oids
                ]
            case InsertMessage():
                relation = self._relation(message.relation_oid)
                after, unchanged = _row(relation, message.new)
                return [self._event(Operation.INSERT, relation, after, None, after, unchanged)]
            case UpdateMessage():
                relation = self._relation(message.relation_oid)
                before = _old_row(relation, message.old, message.old_is_key_only)
                after, unchanged = _row(relation, message.new)
                if unchanged and before is not None:
                    # Restore unchanged TOAST values from the old image when we have it.
                    after |= {c: before[c] for c in unchanged if c in before}
                    unchanged = tuple(c for c in unchanged if c not in before)
                return [self._event(Operation.UPDATE, relation, after, before, after, unchanged)]
            case DeleteMessage():
                relation = self._relation(message.relation_oid)
                old, _ = _row(relation, message.old)
                before = None if message.old_is_key_only else old
                return [self._event(Operation.DELETE, relation, old, before, None)]

    def _event(
        self,
        operation: Operation,
        relation: RelationMessage,
        key_source: Row | None,
        before: Row | None,
        after: Row | None,
        unchanged: tuple[str, ...] = (),
    ) -> ChangeEvent:
        transaction = self._transaction
        assert transaction is not None
        key = (
            {name: key_source.get(name) for name in self._key_columns(relation)}
            if key_source is not None
            else {}
        )
        return ChangeEvent(
            operation=operation,
            table=relation.table,
            key=key,
            before=before,
            after=after,
            lsn=transaction.final_lsn,
            xid=transaction.xid,
            committed_at=transaction.committed_at,
            unchanged=unchanged,
        )

    def _key_columns(self, relation: RelationMessage) -> tuple[str, ...]:
        primary_key = self._catalog.primary_keys.get(relation.table)
        if primary_key:
            return primary_key
        return tuple(c.name for c in relation.columns if c.is_identity)

    def _relation(self, oid: int) -> RelationMessage:
        try:
            return self._relations[oid]
        except KeyError:
            raise ProtocolViolationError(f"Change for unknown relation {oid}") from None


def _row(relation: RelationMessage, values: TupleValues) -> tuple[dict[str, Any], tuple[str, ...]]:
    if len(values) != len(relation.columns):
        raise ProtocolViolationError(
            f"{relation.table}: expected {len(relation.columns)} columns, got {len(values)}"
        )
    row: dict[str, Any] = {}
    unchanged: list[str] = []
    for column, value in zip(relation.columns, values, strict=True):
        if value is UNCHANGED_TOAST:
            unchanged.append(column.name)
        else:
            row[column.name] = value
    return row, tuple(unchanged)


def _old_row(
    relation: RelationMessage, values: TupleValues | None, key_only: bool
) -> dict[str, Any] | None:
    if values is None or key_only:
        return None
    row, _ = _row(relation, values)
    return row
