"""Builds ``pgoutput`` payloads byte by byte, the way PostgreSQL would send them."""

from __future__ import annotations

import struct
from collections.abc import Sequence

type Value = str | bytes | None  # bytes stands for an unchanged TOAST marker


def _string(text: str) -> bytes:
    return text.encode() + b"\x00"


def begin(final_lsn: int, timestamp_us: int, xid: int) -> bytes:
    return b"B" + struct.pack("!QqI", final_lsn, timestamp_us, xid)


def commit(commit_lsn: int, end_lsn: int, timestamp_us: int) -> bytes:
    return b"C" + struct.pack("!BQQq", 0, commit_lsn, end_lsn, timestamp_us)


def relation(oid: int, schema: str, name: str, columns: Sequence[tuple[str, int, bool]]) -> bytes:
    body = struct.pack("!I", oid) + _string(schema) + _string(name) + b"f"
    body += struct.pack("!h", len(columns))
    for column_name, type_oid, is_key in columns:
        body += struct.pack("!B", int(is_key)) + _string(column_name)
        body += struct.pack("!Ii", type_oid, -1)
    return b"R" + body


UNCHANGED = b"u"


def tuple_data(values: Sequence[Value]) -> bytes:
    body = struct.pack("!h", len(values))
    for value in values:
        if value is None:
            body += b"n"
        elif value == UNCHANGED:
            body += b"u"
        else:
            assert isinstance(value, str)
            encoded = value.encode()
            body += b"t" + struct.pack("!i", len(encoded)) + encoded
    return body


def insert(oid: int, values: Sequence[Value]) -> bytes:
    return b"I" + struct.pack("!I", oid) + b"N" + tuple_data(values)


def update(
    oid: int,
    new: Sequence[Value],
    old: Sequence[Value] | None = None,
    key_only: bool = False,
) -> bytes:
    body = b"U" + struct.pack("!I", oid)
    if old is not None:
        body += (b"K" if key_only else b"O") + tuple_data(old)
    return body + b"N" + tuple_data(new)


def delete(oid: int, old: Sequence[Value], key_only: bool = False) -> bytes:
    return b"D" + struct.pack("!I", oid) + (b"K" if key_only else b"O") + tuple_data(old)


def truncate(oids: Sequence[int], cascade: bool = False, restart_identity: bool = False) -> bytes:
    options = int(cascade) | (int(restart_identity) << 1)
    return (
        b"T"
        + struct.pack("!iB", len(oids), options)
        + b"".join(struct.pack("!I", oid) for oid in oids)
    )
