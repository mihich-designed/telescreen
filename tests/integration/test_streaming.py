from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg

from telescreen.adapters.memory import MemoryCheckpointStore
from telescreen.adapters.postgres.source import PostgresReplicationSource
from telescreen.adapters.secrets import EnvSecretResolver
from telescreen.adapters.sinks.memory import MemorySink
from telescreen.application.capture import ChangeCapture, FlushPolicy
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline, SinkSpec, SourceSpec
from telescreen.domain.table import TableName


def make_pipeline(table: str) -> Pipeline:
    name = f"telescreen_test_{table.removeprefix('items_')}"
    return Pipeline(
        name=name.replace("telescreen_test_", "test-"),
        source=SourceSpec(dsn="env:SOURCE", publication=name, slot=name),
        sink=SinkSpec(kind="memory", topic_prefix="test"),
        tables=(TableName("public", table),),
    )


async def collect(
    source: PostgresReplicationSource,
    pipeline: Pipeline,
    checkpoints: MemoryCheckpointStore,
    count: int,
    start_lsn: Lsn | None = None,
) -> list[dict[str, Any]]:
    catalog = await source.prepare(pipeline)
    sink = MemorySink()
    async with source.open(pipeline, start_lsn) as session:
        capture = ChangeCapture(
            pipeline, catalog, session, sink, checkpoints, FlushPolicy(max_transactions=1)
        )
        task = asyncio.create_task(capture.run())
        async with asyncio.timeout(15):
            while len(sink.delivered) < count:  # noqa: ASYNC110 - polling a plain list
                await asyncio.sleep(0.1)
        task.cancel()
    return [json.loads(record.value) for record in sink.delivered]


async def test_streams_inserts_updates_and_deletes(source_dsn: str, scratch_table: str) -> None:
    pipeline = make_pipeline(scratch_table)
    source = PostgresReplicationSource(EnvSecretResolver({"SOURCE": source_dsn}))
    checkpoints = MemoryCheckpointStore()
    await source.prepare(pipeline)  # creates the slot, so changes below are captured

    connection = await asyncpg.connect(source_dsn)
    try:
        await connection.execute(
            f"INSERT INTO {scratch_table} VALUES (1, 'telescreen', '[\"1984\"]');"
            f"UPDATE {scratch_table} SET name = 'telescreen v2' WHERE id = 1;"
            f"DELETE FROM {scratch_table} WHERE id = 1;"
        )
    finally:
        await connection.close()

    insert, update, delete = await collect(source, pipeline, checkpoints, count=3)

    assert insert["op"] == "c"
    assert insert["key"] == {"id": 1}
    assert insert["after"] == {"id": 1, "name": "telescreen", "tags": ["1984"]}
    assert update["op"] == "u"
    assert update["before"]["name"] == "telescreen"
    assert update["after"]["name"] == "telescreen v2"
    assert delete["op"] == "d"
    assert delete["before"] == {"id": 1, "name": "telescreen v2", "tags": ["1984"]}
    assert insert["source"]["xid"] == update["source"]["xid"] == delete["source"]["xid"]


async def test_resumes_after_acknowledged_position(source_dsn: str, scratch_table: str) -> None:
    pipeline = make_pipeline(scratch_table)
    source = PostgresReplicationSource(EnvSecretResolver({"SOURCE": source_dsn}))
    checkpoints = MemoryCheckpointStore()
    await source.prepare(pipeline)

    connection = await asyncpg.connect(source_dsn)
    try:
        await connection.execute(f"INSERT INTO {scratch_table} VALUES (1, 'first', NULL)")
        [first] = await collect(source, pipeline, checkpoints, count=1)

        await connection.execute(f"INSERT INTO {scratch_table} VALUES (2, 'second', NULL)")
        # A fresh store forces the restart to rely on the slot's confirmed position.
        [second] = await collect(source, pipeline, MemoryCheckpointStore(), count=1)
    finally:
        await connection.close()

    assert first["after"]["name"] == "first"
    assert second["after"]["name"] == "second"
