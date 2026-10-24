"""Worker process: streams every configured pipeline until interrupted."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import AbstractAsyncContextManager

from telescreen.adapters.config.yaml_pipelines import load_pipelines
from telescreen.adapters.memory import MemoryCheckpointStore
from telescreen.adapters.postgres.source import PostgresReplicationSource
from telescreen.adapters.secrets import EnvSecretResolver
from telescreen.adapters.sinks.kafka import KafkaSink
from telescreen.application.ports import EventSink
from telescreen.application.runner import PipelineRunner, SinkFactory
from telescreen.bootstrap.settings import Settings
from telescreen.domain.pipeline import Pipeline

logger = logging.getLogger("telescreen")


class UnknownSinkError(ValueError):
    pass


def build_sink_factory(settings: Settings) -> SinkFactory:
    def factory(pipeline: Pipeline) -> AbstractAsyncContextManager[EventSink]:
        if pipeline.sink.kind == "kafka":
            return KafkaSink(
                settings.kafka_bootstrap_servers, client_id=f"telescreen-{pipeline.name}"
            )
        raise UnknownSinkError(f"Unknown sink kind {pipeline.sink.kind!r}")

    return factory


async def run(settings: Settings) -> None:
    pipelines = load_pipelines(settings.pipelines_file)
    runner = PipelineRunner(
        source=PostgresReplicationSource(EnvSecretResolver()),
        sinks=build_sink_factory(settings),
        checkpoints=MemoryCheckpointStore(),
    )
    logger.info("starting %d pipeline(s)", len(pipelines))
    async with asyncio.TaskGroup() as group:
        for pipeline in pipelines:
            group.create_task(runner.run_forever(pipeline), name=f"pipeline-{pipeline.name}")


def main() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(settings))


if __name__ == "__main__":
    main()
