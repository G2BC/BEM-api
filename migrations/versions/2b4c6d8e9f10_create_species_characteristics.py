"""create species characteristics table and backfill luminescence fields

Revision ID: 2b4c6d8e9f10
Revises: 1a2b3c4d5e6f
Create Date: 2026-03-02 21:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2b4c6d8e9f10"
down_revision = "1a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "species_characteristics",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("lum_mycelium", sa.Boolean(), nullable=True),
        sa.Column("lum_basidiome", sa.Boolean(), nullable=True),
        sa.Column("lum_stipe", sa.Boolean(), nullable=True),
        sa.Column("lum_pileus", sa.Boolean(), nullable=True),
        sa.Column("lum_lamellae", sa.Boolean(), nullable=True),
        sa.Column("lum_spores", sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(["species_id"], ["species.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("species_id"),
    )

    op.execute(
        """
        INSERT INTO species_characteristics (
            species_id,
            lum_mycelium,
            lum_basidiome,
            lum_stipe,
            lum_pileus,
            lum_lamellae,
            lum_spores
        )
        SELECT
            id,
            lum_mycelium,
            lum_basidiome,
            lum_stipe,
            lum_pileus,
            lum_lamellae,
            lum_spores
        FROM species
        """
    )


def downgrade():
    op.drop_table("species_characteristics")
