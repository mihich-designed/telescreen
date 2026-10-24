import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from telescreen.adapters.memory import MemoryCheckpointStore, MemoryDatabase, MemoryRuntimeStore
from telescreen.adapters.sinks.memory import MemorySink
from telescreen.application.ports import (
    FatalPipelineError,
    RuntimeState,
    SourceCatalog,
)
from telescreen.application.runner import PipelineRunner
from telescreen.application.wal import WalMessage
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline, SnapshotMode
from telescreen.domain.table import TableName
from tests.unit.test_domain import make_pipeline

ORDERS = TableName("public", "orders")


class EmptySession:
    async def messages(self) -> AsyncIterator[WalMessage]:
        return
        yield

    def acknowledge(self, lsn: Lsn) -> None:
        pass


class FakeSnapshot:
    consistent_point = Lsn(0x500)

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def rows(self, table: TableName) -> AsyncIterator[dict[str, Any]]:
        for row in self._rows:
            yield row


class ScriptedSource:
    """``prepare`` raises the planted errors one by one, then succeeds."""

    def __init__(
        self, errors: list[Exception], snapshot_rows: list[dict[str, Any]] | None = None
    ) -> None:
        self.errors = errors
        self.snapshot_rows = snapshot_rows or []
        self.calls: list[str] = []
        self.opened_at: list[Lsn | None] = []

    async def prepare(self, pipeline: Pipeline) -> SourceCatalog:
        self.calls.append("prepare")
        if self.errors:
            raise self.errors.pop(0)
        return SourceCatalog(primary_keys={ORDERS: ("id",)})

    async def ensure_slot(self, pipeline: Pipeline) -> None:
        self.calls.append("ensure_slot")

    @asynccontextmanager
    async def snapshot(self, pipeline: Pipeline) -> AsyncIterator[FakeSnapshot]:
        self.calls.append("snapshot")
        yield FakeSnapshot(self.snapshot_rows)

    @asynccontextmanager
    async def open(self, pipeline: Pipeline, start_lsn: Lsn | None) -> AsyncIterator[Any]:
        self.calls.append("open")
        self.opened_at.append(start_lsn)
        yield EmptySession()

    async def slot_lag(self, pipeline: Pipeline) -> int | None:
        return 42


class Harness:
    def __init__(self, source: ScriptedSource) -> None:
        self.source = source
        self.runtime = MemoryRuntimeStore()
        self.database = MemoryDatabase()
        self.checkpoints = MemoryCheckpointStore()
        self.sink = MemorySink()

        @asynccontextmanager
        async def sinks(pipeline: Pipeline) -> AsyncIterator[MemorySink]:
            yield self.sink

        source_any: Any = source
        self.runner = PipelineRunner(
            source=source_any,
            sinks=sinks,
            checkpoints=self.checkpoints,
            runtime=self.runtime,
            uow=self.database.unit_of_work,
            worker_id="w1",
            initial_backoff=0,
        )

    def add(self, pipeline: Pipeline) -> Pipeline:
        self.database.pipelines[pipeline.name] = pipeline
        return pipeline


def streaming_only() -> Pipeline:
    pipeline = make_pipeline()
    pipeline.snapshot = SnapshotMode.NEVER
    return pipeline


async def test_run_once_without_snapshot_just_streams() -> None:
    harness = Harness(ScriptedSource([]))

    await harness.runner.run_once(harness.add(streaming_only()))

    assert harness.source.calls == ["prepare", "ensure_slot", "open"]
    status = harness.runtime.statuses["shop"]
    assert status.worker == "w1"
    assert status.state in {RuntimeState.STARTING, RuntimeState.STREAMING}


async def test_snapshot_then_stream_from_consistent_point() -> None:
    rows = [{"id": 1, "status": "paid"}, {"id": 2, "status": "new"}]
    harness = Harness(ScriptedSource([], snapshot_rows=rows))
    pipeline = harness.add(make_pipeline())

    await harness.runner.run_once(pipeline)

    assert harness.source.calls == ["prepare", "snapshot", "open"]
    assert harness.source.opened_at == [Lsn(0x500)]
    bodies = [json.loads(r.value) for r in harness.sink.delivered]
    assert [b["op"] for b in bodies] == ["r", "r"]
    assert [b["key"] for b in bodies] == [{"id": 1}, {"id": 2}]
    assert bodies[0]["after"] == rows[0]
    assert bodies[0]["source"]["lsn"] == "0/500"
    assert bodies[0]["source"]["xid"] is None
    assert harness.database.pipelines["shop"].snapshot_completed
    assert not pipeline.needs_snapshot


async def test_snapshot_is_taken_only_once() -> None:
    harness = Harness(ScriptedSource([]))
    pipeline = harness.add(make_pipeline())

    await harness.runner.run_once(pipeline)
    await harness.runner.run_once(pipeline)

    assert harness.source.calls == ["prepare", "snapshot", "open", "prepare", "ensure_slot", "open"]


async def test_transient_errors_are_retried_and_reported() -> None:
    harness = Harness(ScriptedSource([ConnectionError("refused"), FatalPipelineError("no table")]))

    with pytest.raises(FatalPipelineError, match="no table"):
        await harness.runner.run_forever(harness.add(streaming_only()))

    assert harness.source.calls == ["prepare", "prepare"]
    # The last report before the fatal error came from the retry.
    assert harness.runtime.statuses["shop"].state is RuntimeState.STARTING


async def test_retry_report_contains_error() -> None:
    harness = Harness(ScriptedSource([ConnectionError("refused"), FatalPipelineError("stop")]))
    reports: list[Any] = []
    original = harness.runtime.report

    async def spy(status: Any) -> None:
        reports.append(status)
        await original(status)

    harness.runtime.report = spy  # type: ignore[method-assign]

    with pytest.raises(FatalPipelineError):
        await harness.runner.run_forever(harness.add(streaming_only()))

    retrying = [r for r in reports if r.state is RuntimeState.RETRYING]
    assert len(retrying) == 1
    assert "refused" in retrying[0].error
