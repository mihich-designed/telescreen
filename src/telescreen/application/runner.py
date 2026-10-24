"""Keeps a pipeline running: prepares the source, streams, restarts on failure."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from telescreen.application.capture import ChangeCapture, FlushPolicy
from telescreen.application.ports import CheckpointStore, EventSink, ReplicationSource
from telescreen.domain.pipeline import Pipeline

logger = logging.getLogger(__name__)

type SinkFactory = Callable[[Pipeline], AbstractAsyncContextManager[EventSink]]


class PipelineRunner:
    def __init__(
        self,
        source: ReplicationSource,
        sinks: SinkFactory,
        checkpoints: CheckpointStore,
        policy: FlushPolicy | None = None,
        *,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ) -> None:
        self._source = source
        self._sinks = sinks
        self._checkpoints = checkpoints
        self._policy = policy
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff

    async def run_once(self, pipeline: Pipeline) -> None:
        catalog = await self._source.prepare(pipeline)
        start_lsn = await self._checkpoints.load(pipeline.name)
        async with (
            self._sinks(pipeline) as sink,
            self._source.open(pipeline, start_lsn) as session,
        ):
            capture = ChangeCapture(
                pipeline, catalog, session, sink, self._checkpoints, self._policy
            )
            await capture.run()

    async def run_forever(self, pipeline: Pipeline) -> None:
        backoff = self._initial_backoff
        while True:
            try:
                await self.run_once(pipeline)
                backoff = self._initial_backoff
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("pipeline %s failed, restarting in %.0fs", pipeline.name, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)
