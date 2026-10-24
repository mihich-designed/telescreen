"""Pipelines, checkpoints and runtime status.

Revision ID: 0001
Revises:
Create Date: 2026-10-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(63), nullable=False),
        sa.Column("source_dsn", sa.Text(), nullable=False),
        sa.Column("publication", sa.String(63), nullable=False),
        sa.Column("slot", sa.String(63), nullable=False),
        sa.Column("sink_kind", sa.String(32), nullable=False),
        sa.Column("topic_prefix", sa.Text(), nullable=False),
        sa.Column("tables", sa.ARRAY(sa.Text()), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("status IN ('active', 'paused', 'failed', 'deleting')", name="status"),
        sa.PrimaryKeyConstraint("id", name="pk_pipelines"),
        sa.UniqueConstraint("name", name="uq_pipelines_name"),
        sa.UniqueConstraint("publication", name="uq_pipelines_publication"),
        sa.UniqueConstraint("slot", name="uq_pipelines_slot"),
    )
    op.create_table(
        "checkpoints",
        sa.Column("pipeline", sa.String(63), nullable=False),
        sa.Column("lsn", sa.Numeric(20, 0), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["pipeline"],
            ["pipelines.name"],
            name="fk_checkpoints_pipeline_pipelines",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("pipeline", name="pk_checkpoints"),
    )
    op.create_table(
        "runtime_status",
        sa.Column("pipeline", sa.String(63), nullable=False),
        sa.Column("worker", sa.Text(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("lag_bytes", sa.BigInteger(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["pipeline"],
            ["pipelines.name"],
            name="fk_runtime_status_pipeline_pipelines",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("pipeline", name="pk_runtime_status"),
    )


def downgrade() -> None:
    op.drop_table("runtime_status")
    op.drop_table("checkpoints")
    op.drop_table("pipelines")
