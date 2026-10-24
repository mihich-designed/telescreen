"""Initial snapshot that lines up exactly with the start of a replication slot.

``CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`` creates the slot and exports the
database snapshot taken at the slot's consistent point. Reading the tables inside that
snapshot and then streaming from the slot gives every row exactly one starting state:
nothing committed before the consistent point is missed, nothing after it is read twice.

The exported snapshot only lives while the connection that created the slot stays open
and idle, so that connection is held until the copy is finished.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Any, Self

import asyncpg
import psycopg2
import psycopg2.extensions
import psycopg2.extras

from telescreen.adapters.postgres.sql import quote_ident, quote_table
from telescreen.adapters.postgres.types import decode_text
from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName

logger = logging.getLogger(__name__)

# Generated columns are not replicated by pgoutput, so they are left out of the copy too.
_COLUMNS = """
SELECT a.attname, a.atttypid::int
FROM pg_attribute a
WHERE a.attrelid = $1::text::regclass
  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY a.attnum
"""


async def drop_slot(connection: asyncpg.Connection, slot: str) -> None:
    await connection.execute(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
        slot,
    )


def _create_slot(dsn: str, slot: str) -> tuple[psycopg2.extensions.connection, Lsn, str]:
    connection = psycopg2.connect(
        dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    try:
        cursor = connection.cursor()
        # The pre-15 spelling of the option is still accepted by every supported version.
        cursor.execute(
            f"CREATE_REPLICATION_SLOT {quote_ident(slot)} LOGICAL pgoutput EXPORT_SNAPSHOT"
        )
        row = cursor.fetchone()
    except BaseException:
        connection.close()
        raise
    assert row is not None
    _, consistent_point, snapshot_name, _ = row
    return connection, Lsn.parse(consistent_point), snapshot_name


class PostgresSnapshotSession:
    def __init__(self, dsn: str, slot: str, *, fetch_size: int = 1000) -> None:
        self._dsn = dsn
        self._slot = slot
        self._fetch_size = fetch_size
        self._replication: psycopg2.extensions.connection | None = None
        self._reader: asyncpg.Connection | None = None
        self._consistent_point: Lsn | None = None

    @property
    def consistent_point(self) -> Lsn:
        assert self._consistent_point is not None, "session is not open"
        return self._consistent_point

    async def __aenter__(self) -> Self:
        admin = await asyncpg.connect(self._dsn)
        try:
            # A slot left by an interrupted snapshot has the wrong consistent point.
            await drop_slot(admin, self._slot)
        finally:
            await admin.close()

        self._replication, self._consistent_point, snapshot = await asyncio.to_thread(
            _create_slot, self._dsn, self._slot
        )
        logger.info("slot %s created at %s", self._slot, self._consistent_point)
        try:
            self._reader = await asyncpg.connect(self._dsn)
            await self._reader.execute(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;"
                f"SET TRANSACTION SNAPSHOT '{snapshot}'"
            )
        except BaseException:
            await self._close()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._close()

    async def rows(self, table: TableName) -> AsyncIterator[dict[str, Any]]:
        reader = self._reader
        assert reader is not None, "session is not open"
        columns: list[tuple[str, int]] = [
            (row[0], row[1]) for row in await reader.fetch(_COLUMNS, quote_table(table))
        ]
        # Reading every value as text makes the snapshot use exactly the same
        # representation as pgoutput does while streaming.
        selection = ", ".join(f"{quote_ident(name)}::text" for name, _ in columns)
        query = f"SELECT {selection} FROM {quote_table(table)}"
        async for record in reader.cursor(query, prefetch=self._fetch_size):
            yield {
                name: decode_text(type_oid, value) if value is not None else None
                for (name, type_oid), value in zip(columns, record, strict=True)
            }

    async def _close(self) -> None:
        if self._reader is not None:
            with contextlib.suppress(Exception):
                await self._reader.execute("ROLLBACK")
            await self._reader.close()
            self._reader = None
        if self._replication is not None:
            await asyncio.to_thread(self._replication.close)
            self._replication = None
