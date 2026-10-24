import asyncpg

from telescreen.adapters.memory import MemoryCheckpointStore
from telescreen.adapters.postgres.source import PostgresReplicationSource
from telescreen.adapters.secrets import EnvSecretResolver
from telescreen.domain.table import TableName
from tests.integration.test_streaming import collect, make_pipeline


async def test_snapshot_hands_over_to_streaming_without_gaps(
    source_dsn: str, scratch_table: str
) -> None:
    pipeline = make_pipeline(scratch_table)
    source = PostgresReplicationSource(EnvSecretResolver({"SOURCE": source_dsn}))
    await source.prepare(pipeline)

    connection = await asyncpg.connect(source_dsn)
    try:
        await connection.execute(
            f"INSERT INTO {scratch_table} VALUES (1, 'before', '{{\"n\": 1}}')"
        )
        async with source.snapshot(pipeline) as snapshot:
            # Committed after the slot was created: must come from the stream only.
            await connection.execute(f"INSERT INTO {scratch_table} VALUES (2, 'during', NULL)")
            rows = [row async for row in snapshot.rows(TableName("public", scratch_table))]
            consistent_point = snapshot.consistent_point
    finally:
        await connection.close()

    assert rows == [{"id": 1, "name": "before", "tags": {"n": 1}}]

    [streamed] = await collect(
        source, pipeline, MemoryCheckpointStore(), count=1, start_lsn=consistent_point
    )
    assert streamed["op"] == "c"
    assert streamed["after"]["name"] == "during"


async def test_repeated_snapshot_replaces_the_slot(source_dsn: str, scratch_table: str) -> None:
    pipeline = make_pipeline(scratch_table)
    source = PostgresReplicationSource(EnvSecretResolver({"SOURCE": source_dsn}))
    await source.prepare(pipeline)

    async with source.snapshot(pipeline) as first:
        first_point = first.consistent_point
    async with source.snapshot(pipeline) as second:
        second_point = second.consistent_point

    assert second_point >= first_point
