"""create species similarities relation

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-03-03 01:45:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "species_similarities",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("similar_species_id", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("species_id <> similar_species_id", name="ck_species_similarity_not_self"),
        sa.ForeignKeyConstraint(["species_id"], ["species.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["similar_species_id"], ["species.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("species_id", "similar_species_id"),
    )

    with op.batch_alter_table("species_similarities", schema=None) as batch_op:
        batch_op.create_index("idx_species_similarities_similar_species", ["similar_species_id"], unique=False)


def downgrade():
    with op.batch_alter_table("species_similarities", schema=None) as batch_op:
        batch_op.drop_index("idx_species_similarities_similar_species")

    op.drop_table("species_similarities")
