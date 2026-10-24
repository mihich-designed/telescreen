from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Self

from aiokafka import AIOKafkaProducer

from telescreen.application.ports import OutgoingRecord


class KafkaSink:
    """Publishes records to Kafka with idempotent, fully acknowledged writes."""

    def __init__(self, bootstrap_servers: str, client_id: str = "telescreen") -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
        )
        self._pending: list[asyncio.Future[object]] = []

    async def __aenter__(self) -> Self:
        await self._producer.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._producer.stop()

    async def send(self, record: OutgoingRecord) -> None:
        # ``send`` only enqueues into the producer's batch; the returned future
        # resolves once the broker has acknowledged the write.
        future = await self._producer.send(
            record.topic, value=record.value, key=record.key, headers=list(record.headers)
        )
        self._pending.append(future)

    async def flush(self) -> None:
        pending, self._pending = self._pending, []
        if pending:
            await asyncio.gather(*pending)
