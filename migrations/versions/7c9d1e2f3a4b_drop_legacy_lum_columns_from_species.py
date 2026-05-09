"""drop legacy luminescence columns from species

Revision ID: 7c9d1e2f3a4b
Revises: 2b4c6d8e9f10
Create Date: 2026-03-02 22:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7c9d1e2f3a4b"
down_revision = "2b4c6d8e9f10"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.drop_column("lum_mycelium")
        batch_op.drop_column("lum_basidiome")
        batch_op.drop_column("lum_stipe")
        batch_op.drop_column("lum_pileus")
        batch_op.drop_column("lum_lamellae")
        batch_op.drop_column("lum_spores")


def downgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.add_column(sa.Column("lum_spores", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("lum_lamellae", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("lum_pileus", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("lum_stipe", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("lum_basidiome", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("lum_mycelium", sa.Boolean(), nullable=True))
