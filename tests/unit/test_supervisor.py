import asyncio
from typing import Any

import pytest

from telescreen.adapters.memory import MemoryDatabase, MemoryRuntimeStore
from telescreen.application.ports import FatalPipelineError, RuntimeState, RuntimeStatus
from telescreen.application.supervisor import Supervisor
from telescreen.domain.pipeline import Pipeline, PipelineStatus
from tests.unit.test_domain import make_pipeline


class FakeRunner:
    """Runs until cancelled, or fails with the error planted for a pipeline."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.errors: dict[str, BaseException] = {}

    async def run_forever(self, pipeline: Pipeline) -> None:
        self.started.append(pipeline.name)
        if error := self.errors.get(pipeline.name):
            raise error
        await asyncio.Event().wait()


class FakeSource:
    def __init__(self) -> None:
        self.torn_down: list[str] = []

    async def teardown(self, pipeline: Pipeline) -> None:
        self.torn_down.append(pipeline.name)


class Harness:
    def __init__(self) -> None:
        self.database = MemoryDatabase()
        self.runner = FakeRunner()
        self.source = FakeSource()
        self.runtime = MemoryRuntimeStore()
        runner: Any = self.runner
        source: Any = self.source
        self.supervisor = Supervisor(self.database.unit_of_work, runner, source, self.runtime)

    def put(self, pipeline: Pipeline) -> None:
        self.database.pipelines[pipeline.name] = pipeline

    async def reconcile(self) -> None:
        await self.supervisor.reconcile()
        await asyncio.sleep(0)  # let freshly started tasks run


@pytest.fixture
async def harness() -> Any:
    harness = Harness()
    yield harness
    for name in list(harness.supervisor.running):
        await harness.supervisor._stop(name)


async def test_starts_active_pipelines_once(harness: Harness) -> None:
    harness.put(make_pipeline("a"))
    paused = make_pipeline("b")
    paused.pause()
    harness.put(paused)

    await harness.reconcile()
    await harness.reconcile()

    assert harness.runner.started == ["a"]
    assert harness.supervisor.running == {"a"}


async def test_stops_paused_pipeline_and_clears_runtime(harness: Harness) -> None:
    pipeline = make_pipeline("a")
    harness.put(pipeline)
    await harness.reconcile()
    await harness.runtime.report(RuntimeStatus("a", "w1", RuntimeState.STREAMING))

    pipeline.pause()
    harness.put(pipeline)
    await harness.reconcile()

    assert harness.supervisor.running == frozenset()
    assert await harness.runtime.all() == {}


async def test_fatal_error_marks_pipeline_failed(harness: Harness) -> None:
    harness.runner.errors["a"] = FatalPipelineError("table is gone")
    harness.put(make_pipeline("a"))

    await harness.reconcile()
    await harness.reconcile()

    pipeline = harness.database.pipelines["a"]
    assert pipeline.status is PipelineStatus.FAILED
    assert pipeline.last_error == "table is gone"
    assert harness.supervisor.running == frozenset()


async def test_unexpected_exit_is_restarted(harness: Harness) -> None:
    harness.runner.errors["a"] = RuntimeError("bug")
    harness.put(make_pipeline("a"))

    await harness.reconcile()
    del harness.runner.errors["a"]
    await harness.reconcile()

    assert harness.runner.started == ["a", "a"]
    assert harness.database.pipelines["a"].status is PipelineStatus.ACTIVE


async def test_deleting_pipeline_is_stopped_and_torn_down(harness: Harness) -> None:
    pipeline = make_pipeline("a")
    harness.put(pipeline)
    await harness.reconcile()

    pipeline.delete()
    harness.put(pipeline)
    await harness.reconcile()

    assert harness.source.torn_down == ["a"]
    assert harness.database.pipelines == {}
    assert harness.supervisor.running == frozenset()
