"""add lumm flag to species photo requests

Revision ID: c4d5e6f7a8b9
Revises: 1a2b3c4d5e6f
Create Date: 2026-03-15 16:20:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c4d5e6f7a8b9"
down_revision = "1a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("lumm", sa.Boolean(), nullable=False, server_default=sa.true())
        )


def downgrade():
    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.drop_column("lumm")
