from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName

type Row = Mapping[str, Any]


class Operation(StrEnum):
    INSERT = "c"
    UPDATE = "u"
    DELETE = "d"
    TRUNCATE = "t"
    SNAPSHOT = "r"


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """A single row-level change captured from the source database."""

    operation: Operation
    table: TableName
    key: Row
    before: Row | None
    after: Row | None
    lsn: Lsn
    xid: int | None
    committed_at: datetime | None
    # Columns whose new value was not sent by PostgreSQL (unchanged TOAST data)
    # and could not be restored from the old row image.
    unchanged: tuple[str, ...] = field(default=())
