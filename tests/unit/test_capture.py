import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest

from telescreen.adapters.memory import MemoryCheckpointStore
from telescreen.adapters.sinks.memory import MemorySink
from telescreen.application.capture import ChangeCapture, FlushPolicy, ProtocolViolationError
from telescreen.application.ports import SourceCatalog
from telescreen.application.wal import (
    UNCHANGED_TOAST,
    BeginMessage,
    ColumnInfo,
    CommitMessage,
    DeleteMessage,
    Heartbeat,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    UpdateMessage,
    WalMessage,
)
from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName
from tests.unit.test_domain import make_pipeline

ORDERS = TableName("public", "orders")
NOW = datetime(2026, 1, 1, tzinfo=UTC)
RELATION = RelationMessage(
    1,
    ORDERS,
    (
        ColumnInfo("id", 20, is_identity=True),
        ColumnInfo("status", 25, is_identity=True),
        ColumnInfo("note", 25, is_identity=True),
    ),
)


class FakeSession:
    def __init__(self, messages: list[WalMessage]) -> None:
        self._messages = messages
        self.acknowledged: list[Lsn] = []

    async def messages(self) -> AsyncIterator[WalMessage]:
        for message in self._messages:
            yield message

    def acknowledge(self, lsn: Lsn) -> None:
        self.acknowledged.append(lsn)


def transaction(xid: int, end: int, *changes: WalMessage) -> list[WalMessage]:
    return [
        BeginMessage(Lsn(end - 1), NOW, xid),
        *changes,
        CommitMessage(Lsn(end - 1), Lsn(end), NOW),
    ]


class Harness:
    def __init__(self, policy: FlushPolicy | None = None) -> None:
        self.sink = MemorySink()
        self.checkpoints = MemoryCheckpointStore()
        self.session = FakeSession([])
        self.capture = ChangeCapture(
            make_pipeline(),
            SourceCatalog(primary_keys={ORDERS: ("id",)}),
            self.session,
            self.sink,
            self.checkpoints,
            policy or FlushPolicy(max_transactions=1),
        )

    async def feed(self, *messages: WalMessage) -> None:
        for message in messages:
            await self.capture.handle(message)

    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(r.value) for r in self.sink.delivered]


@pytest.fixture
def harness() -> Harness:
    return Harness()


async def test_insert_becomes_event(harness: Harness) -> None:
    await harness.feed(RELATION, *transaction(10, 100, InsertMessage(1, (5, "new", None))))

    [record] = harness.sink.delivered
    assert record.topic == "shop.public.orders"
    assert json.loads(record.key) == {"id": 5}
    body = json.loads(record.value)
    assert body["op"] == "c"
    assert body["before"] is None
    assert body["after"] == {"id": 5, "status": "new", "note": None}
    assert body["source"] == {
        "pipeline": "shop",
        "schema": "public",
        "table": "orders",
        "lsn": "0/63",
        "xid": 10,
        "committed_at": NOW.isoformat(),
    }


async def test_acknowledges_only_after_delivery(harness: Harness) -> None:
    await harness.feed(RELATION, BeginMessage(Lsn(99), NOW, 10), InsertMessage(1, (5, "new", "")))

    assert harness.session.acknowledged == []
    assert harness.sink.delivered == []

    await harness.feed(CommitMessage(Lsn(99), Lsn(100), NOW))

    assert harness.session.acknowledged == [Lsn(100)]
    assert len(harness.sink.delivered) == 1
    assert await harness.checkpoints.load("shop") == Lsn(100)


async def test_batches_transactions_and_flushes_on_heartbeat() -> None:
    harness = Harness(FlushPolicy(max_transactions=3))
    await harness.feed(
        RELATION,
        *transaction(1, 100, InsertMessage(1, (1, "a", ""))),
        *transaction(2, 200, InsertMessage(1, (2, "b", ""))),
    )
    assert harness.session.acknowledged == []

    await harness.feed(Heartbeat())

    assert harness.session.acknowledged == [Lsn(200)]
    assert len(harness.sink.delivered) == 2


async def test_heartbeat_inside_transaction_does_not_acknowledge() -> None:
    harness = Harness(FlushPolicy(max_transactions=10))
    await harness.feed(
        RELATION,
        *transaction(1, 100, InsertMessage(1, (1, "a", ""))),
        BeginMessage(Lsn(199), NOW, 2),
        Heartbeat(),
    )

    assert harness.session.acknowledged == []


async def test_event_limit_flushes_sink_without_acknowledging_open_transaction() -> None:
    harness = Harness(FlushPolicy(max_transactions=10, max_events=2))
    await harness.feed(
        RELATION,
        BeginMessage(Lsn(99), NOW, 1),
        InsertMessage(1, (1, "a", "")),
        InsertMessage(1, (2, "b", "")),
    )

    assert len(harness.sink.delivered) == 2
    assert harness.session.acknowledged == []


async def test_update_restores_unchanged_toast_from_old_row(harness: Harness) -> None:
    update = UpdateMessage(1, new=(5, "paid", UNCHANGED_TOAST), old=(5, "new", "long text"))
    await harness.feed(RELATION, *transaction(1, 100, update))

    [body] = harness.bodies()
    assert body["op"] == "u"
    assert body["before"] == {"id": 5, "status": "new", "note": "long text"}
    assert body["after"] == {"id": 5, "status": "paid", "note": "long text"}
    assert "unchanged" not in body


async def test_update_reports_unchanged_toast_without_old_row(harness: Harness) -> None:
    await harness.feed(
        RELATION, *transaction(1, 100, UpdateMessage(1, new=(5, "paid", UNCHANGED_TOAST)))
    )

    [body] = harness.bodies()
    assert body["before"] is None
    assert body["after"] == {"id": 5, "status": "paid"}
    assert body["unchanged"] == ["note"]


async def test_delete_with_full_old_row(harness: Harness) -> None:
    await harness.feed(RELATION, *transaction(1, 100, DeleteMessage(1, (5, "new", ""), False)))

    [body] = harness.bodies()
    assert body["op"] == "d"
    assert body["key"] == {"id": 5}
    assert body["before"] == {"id": 5, "status": "new", "note": ""}
    assert body["after"] is None


async def test_delete_with_key_only(harness: Harness) -> None:
    await harness.feed(RELATION, *transaction(1, 100, DeleteMessage(1, (5, None, None), True)))

    [body] = harness.bodies()
    assert body["key"] == {"id": 5}
    assert body["before"] is None


async def test_truncate(harness: Harness) -> None:
    await harness.feed(RELATION, *transaction(1, 100, TruncateMessage((1,), False, False)))

    [body] = harness.bodies()
    assert body["op"] == "t"
    assert body["key"] == {}


async def test_falls_back_to_replica_identity_without_primary_key() -> None:
    harness = Harness()
    harness.capture = ChangeCapture(
        make_pipeline(),
        SourceCatalog(),
        harness.session,
        harness.sink,
        harness.checkpoints,
        FlushPolicy(max_transactions=1),
    )
    await harness.feed(RELATION, *transaction(1, 100, InsertMessage(1, (5, "new", "x"))))

    assert json.loads(harness.sink.delivered[0].key) == {"id": 5, "note": "x", "status": "new"}


async def test_run_consumes_session(harness: Harness) -> None:
    harness.session._messages = [
        RELATION,
        *transaction(1, 100, InsertMessage(1, (5, "new", ""))),
    ]
    await harness.capture.run()

    assert harness.session.acknowledged == [Lsn(100)]


@pytest.mark.parametrize(
    "messages",
    [
        [InsertMessage(1, (1, "a", ""))],
        [CommitMessage(Lsn(1), Lsn(2), NOW)],
        [BeginMessage(Lsn(1), NOW, 1), BeginMessage(Lsn(2), NOW, 2)],
        [BeginMessage(Lsn(1), NOW, 1), InsertMessage(99, (1,))],
        [RELATION, BeginMessage(Lsn(1), NOW, 1), InsertMessage(1, (1,))],
    ],
)
async def test_protocol_violations(harness: Harness, messages: list[WalMessage]) -> None:
    with pytest.raises(ProtocolViolationError):
        await harness.feed(*messages)
