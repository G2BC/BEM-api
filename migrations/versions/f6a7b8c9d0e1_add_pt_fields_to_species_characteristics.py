"""add pt fields to species characteristics

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-03-05 00:40:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("cultivation_pt", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("finding_tips_pt", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("nearby_trees_pt", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("curiosities_pt", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("general_description_pt", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("colors_pt", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_column("colors_pt")
        batch_op.drop_column("general_description_pt")
        batch_op.drop_column("curiosities_pt")
        batch_op.drop_column("nearby_trees_pt")
        batch_op.drop_column("finding_tips_pt")
        batch_op.drop_column("cultivation_pt")
