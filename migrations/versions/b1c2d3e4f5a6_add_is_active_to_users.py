"""add is_active to users

Revision ID: b1c2d3e4f5a6
Revises: 9f2a1c4e7b11
Create Date: 2026-02-26 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "9f2a1c4e7b11"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_active", sa.Boolean(), server_default=sa.false(), nullable=False)
        )

    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.alter_column("is_active", server_default=None)


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_column("is_active")
