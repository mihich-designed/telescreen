"""Keeps a pipeline running: prepares the source, streams, restarts on failure."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from telescreen.application.capture import ChangeCapture, FlushPolicy
from telescreen.application.ports import (
    CheckpointStore,
    FatalPipelineError,
    ReplicationSource,
    RuntimeState,
    RuntimeStatus,
    RuntimeStore,
    SinkFactory,
)
from telescreen.domain.pipeline import Pipeline

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(
        self,
        source: ReplicationSource,
        sinks: SinkFactory,
        checkpoints: CheckpointStore,
        runtime: RuntimeStore,
        worker_id: str,
        policy: FlushPolicy | None = None,
        *,
        status_interval: float = 5.0,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ) -> None:
        self._source = source
        self._sinks = sinks
        self._checkpoints = checkpoints
        self._runtime = runtime
        self._worker_id = worker_id
        self._policy = policy
        self._status_interval = status_interval
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff

    async def run_once(self, pipeline: Pipeline) -> None:
        await self._report(pipeline, RuntimeState.STARTING)
        catalog = await self._source.prepare(pipeline)
        start_lsn = await self._checkpoints.load(pipeline.name)
        async with (
            self._sinks(pipeline) as sink,
            self._source.open(pipeline, start_lsn) as session,
        ):
            capture = ChangeCapture(
                pipeline, catalog, session, sink, self._checkpoints, self._policy
            )
            status = asyncio.create_task(self._report_streaming(pipeline))
            try:
                await capture.run()
            finally:
                status.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await status

    async def run_forever(self, pipeline: Pipeline) -> None:
        """Run until cancelled. Fatal errors are not retried and propagate."""
        backoff = self._initial_backoff
        while True:
            started = time.monotonic()
            try:
                await self.run_once(pipeline)
            except (asyncio.CancelledError, FatalPipelineError):
                raise
            except Exception as exc:
                if time.monotonic() - started > self._max_backoff:
                    backoff = self._initial_backoff  # it ran fine for a while
                logger.exception("pipeline %s failed, retrying in %.0fs", pipeline.name, backoff)
                await self._report(pipeline, RuntimeState.RETRYING, error=f"{exc!r}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)

    async def _report_streaming(self, pipeline: Pipeline) -> None:
        while True:
            try:
                lag = await self._source.slot_lag(pipeline)
                await self._report(pipeline, RuntimeState.STREAMING, lag_bytes=lag)
            except Exception:
                # Status reporting must never take the stream down.
                logger.warning("could not report status of %s", pipeline.name, exc_info=True)
            await asyncio.sleep(self._status_interval)

    async def _report(
        self,
        pipeline: Pipeline,
        state: RuntimeState,
        *,
        lag_bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        await self._runtime.report(
            RuntimeStatus(
                pipeline=pipeline.name,
                worker=self._worker_id,
                state=state,
                lag_bytes=lag_bytes,
                error=error,
            )
        )
