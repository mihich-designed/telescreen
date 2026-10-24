from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest

SOURCE_DSN = os.environ.get("TELESCREEN_TEST_SOURCE_DSN")

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if SOURCE_DSN:
        return
    skip = pytest.mark.skip(reason="TELESCREEN_TEST_SOURCE_DSN is not set")
    for item in items:
        if "integration" in str(item.path):
            item.add_marker(skip)


@pytest.fixture
def source_dsn() -> str:
    assert SOURCE_DSN
    return SOURCE_DSN


@pytest.fixture
async def scratch_table(source_dsn: str) -> AsyncIterator[str]:
    """A fresh table with full replica identity, plus cleanup of slots and publications."""
    suffix = uuid.uuid4().hex[:8]
    table = f"items_{suffix}"
    connection = await asyncpg.connect(source_dsn)
    await connection.execute(
        f"CREATE TABLE {table} (id bigint PRIMARY KEY, name text NOT NULL, tags jsonb);"
        f"ALTER TABLE {table} REPLICA IDENTITY FULL"
    )
    try:
        yield table
    finally:
        slot = f"telescreen_test_{suffix}"
        await connection.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = $1",
            slot,
        )
        await connection.execute(f"DROP PUBLICATION IF EXISTS {slot}")
        await connection.execute(f"DROP TABLE {table}")
        await connection.close()
