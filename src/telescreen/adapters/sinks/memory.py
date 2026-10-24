from __future__ import annotations

from telescreen.application.ports import OutgoingRecord


class MemorySink:
    """Keeps records in memory. Useful for tests and dry runs."""

    def __init__(self) -> None:
        self.pending: list[OutgoingRecord] = []
        self.delivered: list[OutgoingRecord] = []

    async def send(self, record: OutgoingRecord) -> None:
        self.pending.append(record)

    async def flush(self) -> None:
        self.delivered.extend(self.pending)
        self.pending.clear()
