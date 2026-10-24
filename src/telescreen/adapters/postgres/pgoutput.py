"""Decoder for the ``pgoutput`` logical replication protocol (version 1).

Message layout reference:
https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

from __future__ import annotations

import struct
from datetime import UTC, datetime, timedelta
from typing import Any

from telescreen.adapters.postgres.types import decode_text
from telescreen.application.wal import (
    UNCHANGED_TOAST,
    BeginMessage,
    ColumnInfo,
    CommitMessage,
    DeleteMessage,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    TupleValues,
    UpdateMessage,
    WalMessage,
)
from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName

POSTGRES_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)

_COLUMN_IS_KEY = 1
_TRUNCATE_CASCADE = 1
_TRUNCATE_RESTART_IDENTITY = 2


class DecodeError(ValueError):
    pass


def postgres_timestamp(microseconds: int) -> datetime:
    return POSTGRES_EPOCH + timedelta(microseconds=microseconds)


class _Reader:
    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def _unpack(self, fmt: str) -> Any:
        size = struct.calcsize(fmt)
        if self._pos + size > len(self._data):
            raise DecodeError("Unexpected end of message")
        (value,) = struct.unpack_from(fmt, self._data, self._pos)
        self._pos += size
        return value

    def byte(self) -> str:
        return chr(self.int8())

    def int8(self) -> int:
        return int(self._unpack("!B"))

    def int16(self) -> int:
        return int(self._unpack("!h"))

    def int32(self) -> int:
        return int(self._unpack("!i"))

    def uint32(self) -> int:
        return int(self._unpack("!I"))

    def int64(self) -> int:
        return int(self._unpack("!q"))

    def uint64(self) -> int:
        return int(self._unpack("!Q"))

    def string(self) -> str:
        end = self._data.find(b"\x00", self._pos)
        if end < 0:
            raise DecodeError("Unterminated string")
        value = self._data[self._pos : end].decode()
        self._pos = end + 1
        return value

    def raw(self, size: int) -> bytes:
        if self._pos + size > len(self._data):
            raise DecodeError("Unexpected end of message")
        value = self._data[self._pos : self._pos + size]
        self._pos += size
        return value


class PgOutputDecoder:
    """Turns raw ``pgoutput`` payloads into application messages.

    The decoder is stateful: it remembers relation descriptions so that column values
    can be converted to Python types according to their PostgreSQL type.
    """

    def __init__(self) -> None:
        self._relations: dict[int, RelationMessage] = {}

    def decode(self, payload: bytes) -> WalMessage | None:
        """Decode one message; returns ``None`` for messages the application ignores."""
        if not payload:
            raise DecodeError("Empty message")
        reader = _Reader(payload)
        kind = reader.byte()
        match kind:
            case "B":
                return BeginMessage(
                    final_lsn=Lsn(reader.uint64()),
                    committed_at=postgres_timestamp(reader.int64()),
                    xid=reader.uint32(),
                )
            case "C":
                reader.int8()  # flags, currently unused
                return CommitMessage(
                    commit_lsn=Lsn(reader.uint64()),
                    end_lsn=Lsn(reader.uint64()),
                    committed_at=postgres_timestamp(reader.int64()),
                )
            case "R":
                return self._relation(reader)
            case "I":
                oid = reader.uint32()
                if reader.byte() != "N":
                    raise DecodeError("INSERT without a new tuple")
                return InsertMessage(oid, self._tuple(reader, oid))
            case "U":
                return self._update(reader)
            case "D":
                oid = reader.uint32()
                marker = reader.byte()
                if marker not in ("K", "O"):
                    raise DecodeError(f"Unexpected DELETE tuple marker {marker!r}")
                return DeleteMessage(oid, self._tuple(reader, oid), old_is_key_only=marker == "K")
            case "T":
                count = reader.int32()
                options = reader.int8()
                return TruncateMessage(
                    relation_oids=tuple(reader.uint32() for _ in range(count)),
                    cascade=bool(options & _TRUNCATE_CASCADE),
                    restart_identity=bool(options & _TRUNCATE_RESTART_IDENTITY),
                )
            case "O" | "Y" | "M":
                # Origin, custom type description and logical decoding messages are
                # not needed: values arrive in text form regardless of the type.
                return None
        raise DecodeError(f"Unknown message type {kind!r}")

    def _relation(self, reader: _Reader) -> RelationMessage:
        oid = reader.uint32()
        namespace = reader.string() or "pg_catalog"
        name = reader.string()
        reader.int8()  # replica identity setting
        columns = []
        for _ in range(reader.int16()):
            flags = reader.int8()
            column_name = reader.string()
            type_oid = reader.uint32()
            reader.int32()  # type modifier
            columns.append(ColumnInfo(column_name, type_oid, bool(flags & _COLUMN_IS_KEY)))
        relation = RelationMessage(oid, TableName(namespace, name), tuple(columns))
        self._relations[oid] = relation
        return relation

    def _update(self, reader: _Reader) -> UpdateMessage:
        oid = reader.uint32()
        marker = reader.byte()
        old: TupleValues | None = None
        key_only = False
        if marker in ("K", "O"):
            key_only = marker == "K"
            old = self._tuple(reader, oid)
            marker = reader.byte()
        if marker != "N":
            raise DecodeError("UPDATE without a new tuple")
        return UpdateMessage(oid, self._tuple(reader, oid), old=old, old_is_key_only=key_only)

    def _tuple(self, reader: _Reader, oid: int) -> TupleValues:
        relation = self._relations.get(oid)
        if relation is None:
            raise DecodeError(f"Tuple for unknown relation {oid}")
        count = reader.int16()
        if count != len(relation.columns):
            raise DecodeError(
                f"{relation.table}: tuple has {count} columns, relation has {len(relation.columns)}"
            )
        values: list[Any] = []
        for column in relation.columns:
            kind = reader.byte()
            match kind:
                case "n":
                    values.append(None)
                case "u":
                    values.append(UNCHANGED_TOAST)
                case "t":
                    text = reader.raw(reader.int32()).decode()
                    values.append(decode_text(column.type_oid, text))
                case "b":
                    values.append(reader.raw(reader.int32()))
                case _:
                    raise DecodeError(f"Unknown tuple value kind {kind!r}")
        return tuple(values)
