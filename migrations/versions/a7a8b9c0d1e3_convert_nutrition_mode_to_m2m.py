"""convert nutrition mode to many-to-many

Revision ID: a7a8b9c0d1e3
Revises: f6a7b8c9d0e1
Create Date: 2026-03-05 01:20:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a7a8b9c0d1e3"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "species_characteristics_nutrition_modes",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("nutrition_mode_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["species_id"],
            ["species_characteristics.species_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["nutrition_mode_id"],
            ["nutrition_modes.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("species_id", "nutrition_mode_id"),
    )
    op.create_index(
        "idx_scnm_nutrition_mode_id",
        "species_characteristics_nutrition_modes",
        ["nutrition_mode_id"],
        unique=False,
    )

    op.execute(
        """
        INSERT INTO species_characteristics_nutrition_modes (species_id, nutrition_mode_id)
        SELECT species_id, nutrition_mode_id
        FROM species_characteristics
        WHERE nutrition_mode_id IS NOT NULL
        """
    )

    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_constraint("fk_species_characteristics_nutrition_mode", type_="foreignkey")
        batch_op.drop_column("nutrition_mode_id")


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("nutrition_mode_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_species_characteristics_nutrition_mode",
            "nutrition_modes",
            ["nutrition_mode_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.execute(
        """
        UPDATE species_characteristics sc
        SET nutrition_mode_id = src.nutrition_mode_id
        FROM (
            SELECT species_id, MIN(nutrition_mode_id) AS nutrition_mode_id
            FROM species_characteristics_nutrition_modes
            GROUP BY species_id
        ) AS src
        WHERE sc.species_id = src.species_id
        """
    )

    op.drop_index("idx_scnm_nutrition_mode_id", table_name="species_characteristics_nutrition_modes")
    op.drop_table("species_characteristics_nutrition_modes")
