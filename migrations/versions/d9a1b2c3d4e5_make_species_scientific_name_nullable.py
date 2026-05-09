"""make species scientific_name nullable

Revision ID: d9a1b2c3d4e5
Revises: 6f7a8b9c0d1f
Create Date: 2026-03-29 12:20:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d9a1b2c3d4e5"
down_revision = "6f7a8b9c0d1f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.alter_column(
            "scientific_name",
            existing_type=sa.Text(),
            nullable=True,
        )


def downgrade():
    op.execute(
        "UPDATE species "
        "SET scientific_name = '__unnamed_species_' || id::text "
        "WHERE scientific_name IS NULL"
    )

    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.alter_column(
            "scientific_name",
            existing_type=sa.Text(),
            nullable=False,
        )
