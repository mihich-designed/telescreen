"""Serialization of change events into broker records."""

from __future__ import annotations

import json
from typing import Any

from telescreen.application.ports import OutgoingRecord
from telescreen.domain.changes import ChangeEvent
from telescreen.domain.pipeline import Pipeline


def _dumps(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str
    ).encode()


def to_record(pipeline: Pipeline, event: ChangeEvent) -> OutgoingRecord:
    body: dict[str, Any] = {
        "op": event.operation.value,
        "source": {
            "pipeline": pipeline.name,
            "schema": event.table.schema,
            "table": event.table.name,
            "lsn": str(event.lsn),
            "xid": event.xid,
            "committed_at": event.committed_at.isoformat() if event.committed_at else None,
        },
        "key": dict(event.key),
        "before": dict(event.before) if event.before is not None else None,
        "after": dict(event.after) if event.after is not None else None,
    }
    if event.unchanged:
        body["unchanged"] = list(event.unchanged)
    return OutgoingRecord(
        topic=pipeline.topic_for(event.table),
        key=_dumps(dict(event.key)),
        value=_dumps(body),
        headers=(
            ("telescreen-op", event.operation.value.encode()),
            ("telescreen-pipeline", pipeline.name.encode()),
        ),
    )
