from __future__ import annotations

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Uuid,
    func,
)

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

# PostgreSQL identifiers (slot and publication names) are limited to 63 bytes.
IDENTIFIER = String(63)

pipelines = Table(
    "pipelines",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("name", IDENTIFIER, nullable=False, unique=True),
    Column("source_dsn", Text, nullable=False),
    Column("publication", IDENTIFIER, nullable=False, unique=True),
    Column("slot", IDENTIFIER, nullable=False, unique=True),
    Column("sink_kind", String(32), nullable=False),
    Column("topic_prefix", Text, nullable=False),
    Column("tables", ARRAY(Text), nullable=False),
    Column("status", String(16), nullable=False),
    Column("last_error", Text),
    Column("snapshot_mode", String(16), nullable=False),
    Column("snapshot_completed", Boolean, nullable=False, server_default="false"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    ),
    CheckConstraint("status IN ('active', 'paused', 'failed', 'deleting')", name="status"),
    CheckConstraint("snapshot_mode IN ('initial', 'never')", name="snapshot_mode"),
)

checkpoints = Table(
    "checkpoints",
    metadata,
    Column(
        "pipeline",
        IDENTIFIER,
        ForeignKey("pipelines.name", ondelete="CASCADE"),
        primary_key=True,
    ),
    # An LSN is an unsigned 64-bit integer, which does not fit into BIGINT.
    Column("lsn", Numeric(20, 0), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

runtime_status = Table(
    "runtime_status",
    metadata,
    Column(
        "pipeline",
        IDENTIFIER,
        ForeignKey("pipelines.name", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("worker", Text, nullable=False),
    Column("state", String(16), nullable=False),
    Column("lag_bytes", BigInteger),
    Column("error", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
