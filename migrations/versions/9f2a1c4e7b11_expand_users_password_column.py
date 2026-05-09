"""expand users password column

Revision ID: 9f2a1c4e7b11
Revises: d27a9f70d1bf
Create Date: 2026-02-24 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9f2a1c4e7b11"
down_revision = "d27a9f70d1bf"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.alter_column(
            "password",
            existing_type=sa.String(length=150),
            type_=sa.String(length=255),
            existing_nullable=False,
        )


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.alter_column(
            "password",
            existing_type=sa.String(length=255),
            type_=sa.String(length=150),
            existing_nullable=False,
        )
