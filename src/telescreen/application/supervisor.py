"""Reconciles what runs in this worker with what is stored in the metadata database.

The API only changes the desired state of a pipeline. The supervisor notices the
change on its next pass and starts, stops or tears down streams accordingly.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from telescreen.application.ports import (
    FatalPipelineError,
    ReplicationSource,
    RuntimeStore,
    UnitOfWorkFactory,
)
from telescreen.application.runner import PipelineRunner
from telescreen.domain.pipeline import Pipeline, PipelineStatus

logger = logging.getLogger(__name__)


class Supervisor:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        runner: PipelineRunner,
        source: ReplicationSource,
        runtime: RuntimeStore,
        poll_interval: float = 2.0,
    ) -> None:
        self._uow = uow
        self._runner = runner
        self._source = source
        self._runtime = runtime
        self._poll_interval = poll_interval
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def running(self) -> frozenset[str]:
        return frozenset(self._tasks)

    async def run(self) -> None:
        try:
            while True:
                try:
                    await self.reconcile()
                except Exception:
                    logger.exception("reconciliation failed")
                await asyncio.sleep(self._poll_interval)
        finally:
            for name in list(self._tasks):
                await self._stop(name)

    async def reconcile(self) -> None:
        await self._collect_finished()
        async with self._uow() as uow:
            pipelines = {p.name: p for p in await uow.pipelines.list()}

        for name in list(self._tasks):
            pipeline = pipelines.get(name)
            if pipeline is None or pipeline.status is not PipelineStatus.ACTIVE:
                await self._stop(name)

        for pipeline in pipelines.values():
            if pipeline.status is PipelineStatus.ACTIVE and pipeline.name not in self._tasks:
                self._start(pipeline)
            elif pipeline.status is PipelineStatus.DELETING:
                await self._teardown(pipeline)

    def _start(self, pipeline: Pipeline) -> None:
        logger.info("starting pipeline %s", pipeline.name)
        self._tasks[pipeline.name] = asyncio.create_task(
            self._runner.run_forever(pipeline), name=f"pipeline-{pipeline.name}"
        )

    async def _stop(self, name: str) -> None:
        logger.info("stopping pipeline %s", name)
        task = self._tasks.pop(name)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        await self._runtime.clear(name)

    async def _collect_finished(self) -> None:
        for name, task in list(self._tasks.items()):
            if not task.done():
                continue
            del self._tasks[name]
            error = task.exception()
            if isinstance(error, FatalPipelineError):
                logger.error("pipeline %s failed permanently: %s", name, error)
                await self._mark_failed(name, str(error))
            elif error is not None:
                logger.error("pipeline %s stopped unexpectedly", name, exc_info=error)

    async def _mark_failed(self, name: str, reason: str) -> None:
        async with self._uow() as uow:
            pipeline = await uow.pipelines.get(name)
            if pipeline is None or pipeline.status is not PipelineStatus.ACTIVE:
                return
            pipeline.fail(reason)
            await uow.pipelines.save(pipeline)
            await uow.commit()
        await self._runtime.clear(name)

    async def _teardown(self, pipeline: Pipeline) -> None:
        logger.info("tearing down pipeline %s", pipeline.name)
        await self._source.teardown(pipeline)
        async with self._uow() as uow:
            await uow.pipelines.remove(pipeline)
            await uow.commit()
