"""Decoded logical replication messages, as seen by the application layer.

Adapters translate the wire protocol into these types; use cases never touch bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName


class _UnchangedToast:
    """Marker for a TOASTed column whose value did not change and was not sent."""

    _instance: _UnchangedToast | None = None

    def __new__(cls) -> _UnchangedToast:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"


UNCHANGED_TOAST: Final = _UnchangedToast()

type TupleValues = tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    name: str
    type_oid: int
    is_identity: bool


@dataclass(frozen=True, slots=True)
class RelationMessage:
    oid: int
    table: TableName
    columns: tuple[ColumnInfo, ...]


@dataclass(frozen=True, slots=True)
class BeginMessage:
    final_lsn: Lsn
    committed_at: datetime
    xid: int


@dataclass(frozen=True, slots=True)
class CommitMessage:
    commit_lsn: Lsn
    end_lsn: Lsn
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class InsertMessage:
    relation_oid: int
    new: TupleValues


@dataclass(frozen=True, slots=True)
class UpdateMessage:
    relation_oid: int
    new: TupleValues
    # Old row image: absent unless the key changed or REPLICA IDENTITY FULL is set.
    old: TupleValues | None = None
    old_is_key_only: bool = False


@dataclass(frozen=True, slots=True)
class DeleteMessage:
    relation_oid: int
    old: TupleValues
    old_is_key_only: bool


@dataclass(frozen=True, slots=True)
class TruncateMessage:
    relation_oids: tuple[int, ...]
    cascade: bool
    restart_identity: bool


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """Emitted by the replication session when the stream has been idle for a while."""


type WalMessage = (
    RelationMessage
    | BeginMessage
    | CommitMessage
    | InsertMessage
    | UpdateMessage
    | DeleteMessage
    | TruncateMessage
    | Heartbeat
)
