"""Logical replication session on top of psycopg2.

psycopg2 is the only mainstream Python driver that speaks the streaming replication
protocol, and it is synchronous. The session therefore runs the protocol loop in a
dedicated thread and hands payloads over to asyncio through a bounded queue, which
also gives us backpressure: when the application falls behind, the thread stops
reading from the socket.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import select
import threading
import time
from collections.abc import AsyncIterator
from concurrent.futures import TimeoutError as FutureTimeoutError
from types import TracebackType
from typing import Self

import psycopg2
import psycopg2.extras

from telescreen.adapters.postgres.pgoutput import PgOutputDecoder
from telescreen.application.wal import Heartbeat, WalMessage
from telescreen.domain.lsn import Lsn

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.5
_STOP = object()

type _Item = bytes | Heartbeat | BaseException | object


class PsycopgReplicationSession:
    def __init__(
        self,
        dsn: str,
        slot: str,
        publication: str,
        start_lsn: Lsn | None = None,
        *,
        heartbeat_interval: float = 1.0,
        feedback_interval: float = 10.0,
        queue_size: int = 1000,
    ) -> None:
        self._dsn = dsn
        self._slot = slot
        self._publication = publication
        self._start_lsn = start_lsn
        self._heartbeat_interval = heartbeat_interval
        self._feedback_interval = feedback_interval
        self._queue: asyncio.Queue[_Item] = asyncio.Queue(maxsize=queue_size)
        self._decoder = PgOutputDecoder()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Written by the event loop, read by the replication thread. A plain int
        # assignment is atomic, so no lock is needed.
        self._flush_lsn = 0

    async def __aenter__(self) -> Self:
        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(
            target=self._run, name=f"replication-{self._slot}", daemon=True
        )
        self._thread.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop.set()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join)

    async def messages(self) -> AsyncIterator[WalMessage]:
        while True:
            item = await self._queue.get()
            if item is _STOP:
                return
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, Heartbeat):
                yield item
            elif isinstance(item, bytes):
                message = self._decoder.decode(item)
                if message is not None:
                    yield message

    def acknowledge(self, lsn: Lsn) -> None:
        self._flush_lsn = max(self._flush_lsn, lsn.value)

    # --- replication thread -------------------------------------------------------

    def _run(self) -> None:
        try:
            self._stream()
        except BaseException as exc:
            if not self._stop.is_set():
                logger.exception("replication slot %s failed", self._slot)
                self._put(exc)
                return
        self._put(_STOP)

    def _stream(self) -> None:
        connection = psycopg2.connect(
            self._dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection
        )
        try:
            cursor = connection.cursor()
            cursor.start_replication(
                slot_name=self._slot,
                decode=False,
                start_lsn=self._start_lsn.value if self._start_lsn else 0,
                options={"proto_version": "1", "publication_names": self._publication},
            )
            logger.info("streaming from slot %s", self._slot)
            sent_lsn = 0
            last_feedback = last_activity = time.monotonic()
            while not self._stop.is_set():
                message = cursor.read_message()
                now = time.monotonic()
                flush_lsn = self._flush_lsn
                if flush_lsn > sent_lsn or now - last_feedback >= self._feedback_interval:
                    cursor.send_feedback(flush_lsn=flush_lsn)  # type: ignore[no-untyped-call]
                    sent_lsn, last_feedback = flush_lsn, now
                if message is not None:
                    self._put(bytes(message.payload))
                    last_activity = now
                    continue
                if now - last_activity >= self._heartbeat_interval:
                    self._put(Heartbeat())
                    last_activity = now
                select.select([connection], [], [], _POLL_INTERVAL)
            # Report the final position so a restart does not replay delivered changes.
            if self._flush_lsn > sent_lsn:
                cursor.send_feedback(flush_lsn=self._flush_lsn, force=True)  # type: ignore[no-untyped-call]
        finally:
            with contextlib.suppress(Exception):
                connection.close()

    def _put(self, item: _Item) -> None:
        """Hand an item to the event loop, blocking while the queue is full."""
        assert self._loop is not None
        if self._loop.is_closed():
            return
        future = asyncio.run_coroutine_threadsafe(self._queue.put(item), self._loop)
        while True:
            try:
                future.result(timeout=_POLL_INTERVAL)
                return
            except FutureTimeoutError:
                if self._stop.is_set():
                    future.cancel()
                    return
