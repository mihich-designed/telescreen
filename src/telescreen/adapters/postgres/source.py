"""Administrative side of a source database: publications, slots and catalog lookups."""

from __future__ import annotations

from collections import defaultdict
from contextlib import AbstractAsyncContextManager

import asyncpg

from telescreen.adapters.postgres.replication import PsycopgReplicationSession
from telescreen.application.ports import (
    FatalPipelineError,
    ReplicationSession,
    SecretResolver,
    SourceCatalog,
)
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline
from telescreen.domain.table import TableName


class SourceSetupError(FatalPipelineError):
    pass


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_table(table: TableName) -> str:
    return f"{quote_ident(table.schema)}.{quote_ident(table.name)}"


_PRIMARY_KEYS = """
SELECT n.nspname, c.relname, a.attname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE i.indisprimary AND n.nspname || '.' || c.relname = ANY($1::text[])
ORDER BY n.nspname, c.relname, k.position
"""

_EXISTING_TABLES = """
SELECT n.nspname || '.' || c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND n.nspname || '.' || c.relname = ANY($1::text[])
"""


class PostgresReplicationSource:
    def __init__(self, secrets: SecretResolver) -> None:
        self._secrets = secrets

    async def prepare(self, pipeline: Pipeline) -> SourceCatalog:
        connection = await asyncpg.connect(self._secrets.resolve(pipeline.source.dsn))
        try:
            names = [str(t) for t in pipeline.tables]
            existing = {row[0] for row in await connection.fetch(_EXISTING_TABLES, names)}
            if missing := sorted(set(names) - existing):
                raise SourceSetupError(f"Tables not found in the source: {', '.join(missing)}")
            await self._ensure_publication(connection, pipeline)
            await self._ensure_slot(connection, pipeline.source.slot)
            return SourceCatalog(primary_keys=await self._primary_keys(connection, names))
        finally:
            await connection.close()

    def open(
        self, pipeline: Pipeline, start_lsn: Lsn | None
    ) -> AbstractAsyncContextManager[ReplicationSession]:
        return PsycopgReplicationSession(
            dsn=self._secrets.resolve(pipeline.source.dsn),
            slot=pipeline.source.slot,
            publication=pipeline.source.publication,
            start_lsn=start_lsn,
        )

    async def slot_lag(self, pipeline: Pipeline) -> int | None:
        connection = await asyncpg.connect(self._secrets.resolve(pipeline.source.dsn))
        try:
            lag = await connection.fetchval(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
                "FROM pg_replication_slots WHERE slot_name = $1",
                pipeline.source.slot,
            )
        finally:
            await connection.close()
        return int(lag) if lag is not None else None

    async def teardown(self, pipeline: Pipeline) -> None:
        connection = await asyncpg.connect(self._secrets.resolve(pipeline.source.dsn))
        try:
            await connection.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = $1",
                pipeline.source.slot,
            )
            await connection.execute(
                f"DROP PUBLICATION IF EXISTS {quote_ident(pipeline.source.publication)}"
            )
        finally:
            await connection.close()

    @staticmethod
    async def _ensure_publication(connection: asyncpg.Connection, pipeline: Pipeline) -> None:
        publication = quote_ident(pipeline.source.publication)
        tables = ", ".join(quote_table(t) for t in pipeline.tables)
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_publication WHERE pubname = $1", pipeline.source.publication
        )
        if exists:
            # Keep the publication in sync with the configured table list.
            await connection.execute(f"ALTER PUBLICATION {publication} SET TABLE {tables}")
        else:
            await connection.execute(f"CREATE PUBLICATION {publication} FOR TABLE {tables}")

    @staticmethod
    async def _ensure_slot(connection: asyncpg.Connection, slot: str) -> None:
        row = await connection.fetchrow(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1", slot
        )
        if row is None:
            await connection.execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot
            )
        elif row["plugin"] != "pgoutput":
            raise SourceSetupError(f"Slot {slot!r} uses plugin {row['plugin']!r}, not pgoutput")

    @staticmethod
    async def _primary_keys(
        connection: asyncpg.Connection, names: list[str]
    ) -> dict[TableName, tuple[str, ...]]:
        keys: defaultdict[TableName, list[str]] = defaultdict(list)
        for schema, table, column in await connection.fetch(_PRIMARY_KEYS, names):
            keys[TableName(schema, table)].append(column)
        return {table: tuple(columns) for table, columns in keys.items()}
