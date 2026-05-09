"""add legal metadata to species photo tables

Revision ID: a3d92e4c1f7b
Revises: f7a8b9c0d1e2
Create Date: 2026-02-27 10:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a3d92e4c1f7b"
down_revision = "f7a8b9c0d1e2"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.add_column(sa.Column("rights_holder", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("source_url", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("declaration_accepted_at", sa.DateTime(timezone=True), nullable=True))

    with op.batch_alter_table("species_photos", schema=None) as batch_op:
        batch_op.add_column(sa.Column("rights_holder", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("source_url", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("declaration_accepted_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    with op.batch_alter_table("species_photos", schema=None) as batch_op:
        batch_op.drop_column("declaration_accepted_at")
        batch_op.drop_column("source_url")
        batch_op.drop_column("rights_holder")

    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.drop_column("declaration_accepted_at")
        batch_op.drop_column("source_url")
        batch_op.drop_column("rights_holder")
