from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from telescreen.adapters.memory import MemoryCheckpointStore, MemoryRuntimeStore
from telescreen.adapters.sinks.memory import MemorySink
from telescreen.application.ports import (
    FatalPipelineError,
    RuntimeState,
    SourceCatalog,
)
from telescreen.application.runner import PipelineRunner
from telescreen.application.wal import WalMessage
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline
from tests.unit.test_domain import make_pipeline


class EmptySession:
    async def messages(self) -> AsyncIterator[WalMessage]:
        return
        yield

    def acknowledge(self, lsn: Lsn) -> None:
        pass


class ScriptedSource:
    """``prepare`` raises the planted errors one by one, then succeeds."""

    def __init__(self, errors: list[Exception]) -> None:
        self.errors = errors
        self.prepared = 0

    async def prepare(self, pipeline: Pipeline) -> SourceCatalog:
        self.prepared += 1
        if self.errors:
            raise self.errors.pop(0)
        return SourceCatalog()

    @asynccontextmanager
    async def open(self, pipeline: Pipeline, start_lsn: Lsn | None) -> AsyncIterator[Any]:
        yield EmptySession()

    async def slot_lag(self, pipeline: Pipeline) -> int | None:
        return 42


def make_runner(source: ScriptedSource, runtime: MemoryRuntimeStore) -> PipelineRunner:
    @asynccontextmanager
    async def sinks(pipeline: Pipeline) -> AsyncIterator[MemorySink]:
        yield MemorySink()

    source_any: Any = source
    return PipelineRunner(
        source=source_any,
        sinks=sinks,
        checkpoints=MemoryCheckpointStore(),
        runtime=runtime,
        worker_id="w1",
        initial_backoff=0,
    )


async def test_run_once_reports_starting_then_streaming() -> None:
    runtime = MemoryRuntimeStore()
    source = ScriptedSource([])

    await make_runner(source, runtime).run_once(make_pipeline())

    status = runtime.statuses["shop"]
    assert status.worker == "w1"
    assert status.state in {RuntimeState.STARTING, RuntimeState.STREAMING}


async def test_transient_errors_are_retried_and_reported() -> None:
    runtime = MemoryRuntimeStore()
    source = ScriptedSource([ConnectionError("refused"), FatalPipelineError("no table")])

    with pytest.raises(FatalPipelineError, match="no table"):
        await make_runner(source, runtime).run_forever(make_pipeline())

    assert source.prepared == 2
    # The last report before the fatal error came from the retry.
    assert runtime.statuses["shop"].state is RuntimeState.STARTING


async def test_retry_report_contains_error() -> None:
    runtime = MemoryRuntimeStore()
    reports: list[Any] = []
    original = runtime.report

    async def spy(status: Any) -> None:
        reports.append(status)
        await original(status)

    runtime.report = spy  # type: ignore[method-assign]
    source = ScriptedSource([ConnectionError("refused"), FatalPipelineError("stop")])

    with pytest.raises(FatalPipelineError):
        await make_runner(source, runtime).run_forever(make_pipeline())

    retrying = [r for r in reports if r.state is RuntimeState.RETRYING]
    assert len(retrying) == 1
    assert "refused" in retrying[0].error
