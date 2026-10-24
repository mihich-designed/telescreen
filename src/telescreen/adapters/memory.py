from __future__ import annotations

from telescreen.domain.lsn import Lsn


class MemoryCheckpointStore:
    """Checkpoints that live as long as the process.

    Enough on its own, because the replication slot remembers the confirmed position
    on the server side.
    """

    def __init__(self) -> None:
        self._positions: dict[str, Lsn] = {}

    async def load(self, pipeline: str) -> Lsn | None:
        return self._positions.get(pipeline)

    async def save(self, pipeline: str, lsn: Lsn) -> None:
        self._positions[pipeline] = lsn
