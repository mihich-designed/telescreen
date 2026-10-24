from __future__ import annotations

from contextlib import AbstractAsyncContextManager

from telescreen.adapters.sinks.kafka import KafkaSink
from telescreen.application.ports import EventSink, FatalPipelineError
from telescreen.domain.pipeline import Pipeline

SINK_KINDS = frozenset({"kafka"})


class UnknownSinkError(FatalPipelineError):
    pass


class BrokerSinkFactory:
    """Creates a dedicated sink for every running pipeline."""

    def __init__(self, kafka_bootstrap_servers: str) -> None:
        self._kafka_bootstrap_servers = kafka_bootstrap_servers

    def __call__(self, pipeline: Pipeline) -> AbstractAsyncContextManager[EventSink]:
        if pipeline.sink.kind == "kafka":
            return KafkaSink(self._kafka_bootstrap_servers, client_id=f"telescreen-{pipeline.name}")
        raise UnknownSinkError(f"Unknown sink kind {pipeline.sink.kind!r}")
