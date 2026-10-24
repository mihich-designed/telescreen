"""Snapshot mode and progress of pipelines.

Pipelines created before this revision already stream, so they get ``never``.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "pipelines",
        sa.Column("snapshot_mode", sa.String(16), nullable=False, server_default="never"),
    )
    op.alter_column("pipelines", "snapshot_mode", server_default=None)
    op.add_column(
        "pipelines",
        sa.Column("snapshot_completed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_check_constraint(
        "snapshot_mode", "pipelines", "snapshot_mode IN ('initial', 'never')"
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_pipelines_snapshot_mode"), "pipelines", type_="check")
    op.drop_column("pipelines", "snapshot_completed")
    op.drop_column("pipelines", "snapshot_mode")
