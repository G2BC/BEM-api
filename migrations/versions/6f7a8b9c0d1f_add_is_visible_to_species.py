"""add is_visible to species

Revision ID: 6f7a8b9c0d1f
Revises: 507ac7f3984b
Create Date: 2026-03-27 21:30:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6f7a8b9c0d1f"
down_revision = "507ac7f3984b"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.add_column(sa.Column("is_visible", sa.Boolean(), nullable=True))

    op.execute("UPDATE species SET is_visible = TRUE")

    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.alter_column(
            "is_visible",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        )


def downgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.drop_column("is_visible")
