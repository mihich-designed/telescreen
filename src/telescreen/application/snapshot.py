"""Copies the rows that existed before streaming started."""

import logging
from dataclasses import dataclass, field

from telescreen.application.envelope import to_record
from telescreen.application.ports import EventSink, SnapshotSession, SourceCatalog
from telescreen.domain.changes import ChangeEvent, Operation
from telescreen.domain.pipeline import Pipeline
from telescreen.domain.table import TableName

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    rows: dict[TableName, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return sum(self.rows.values())


class SnapshotCapture:
    def __init__(
        self,
        pipeline: Pipeline,
        catalog: SourceCatalog,
        sink: EventSink,
        *,
        batch_size: int = 1000,
    ) -> None:
        self._pipeline = pipeline
        self._catalog = catalog
        self._sink = sink
        self._batch_size = batch_size

    async def run(self, session: SnapshotSession) -> SnapshotResult:
        """Send every row as a ``r`` event and wait until the broker has all of them."""
        counts: dict[TableName, int] = {}
        for table in self._pipeline.tables:
            key_columns = self._catalog.primary_keys.get(table, ())
            count = 0
            async for row in session.rows(table):
                event = ChangeEvent(
                    operation=Operation.SNAPSHOT,
                    table=table,
                    key={name: row.get(name) for name in key_columns},
                    before=None,
                    after=row,
                    lsn=session.consistent_point,
                    xid=None,
                    committed_at=None,
                )
                await self._sink.send(to_record(self._pipeline, event))
                count += 1
                if count % self._batch_size == 0:
                    await self._sink.flush()
            await self._sink.flush()
            counts[table] = count
            logger.info("pipeline %s: copied %d rows of %s", self._pipeline.name, count, table)
        return SnapshotResult(counts)
