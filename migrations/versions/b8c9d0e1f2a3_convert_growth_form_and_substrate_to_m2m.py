"""convert growth form and substrate to many-to-many

Revision ID: b8c9d0e1f2a3
Revises: a7a8b9c0d1e3
Create Date: 2026-03-05 02:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b8c9d0e1f2a3"
down_revision = "a7a8b9c0d1e3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "species_characteristics_growth_forms",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("growth_form_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["species_id"],
            ["species_characteristics.species_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["growth_form_id"],
            ["growth_forms.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("species_id", "growth_form_id"),
    )
    op.create_index(
        "idx_scgf_growth_form_id",
        "species_characteristics_growth_forms",
        ["growth_form_id"],
        unique=False,
    )

    op.create_table(
        "species_characteristics_substrates",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("substrate_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["species_id"],
            ["species_characteristics.species_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["substrate_id"],
            ["substrates.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("species_id", "substrate_id"),
    )
    op.create_index(
        "idx_scs_substrate_id",
        "species_characteristics_substrates",
        ["substrate_id"],
        unique=False,
    )

    op.execute(
        """
        INSERT INTO species_characteristics_growth_forms (species_id, growth_form_id)
        SELECT species_id, growth_form_id
        FROM species_characteristics
        WHERE growth_form_id IS NOT NULL
        """
    )
    op.execute(
        """
        INSERT INTO species_characteristics_substrates (species_id, substrate_id)
        SELECT species_id, substrate_id
        FROM species_characteristics
        WHERE substrate_id IS NOT NULL
        """
    )

    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_constraint("fk_species_characteristics_growth_form", type_="foreignkey")
        batch_op.drop_constraint("fk_species_characteristics_substrate", type_="foreignkey")
        batch_op.drop_column("growth_form_id")
        batch_op.drop_column("substrate_id")


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("growth_form_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("substrate_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_species_characteristics_growth_form",
            "growth_forms",
            ["growth_form_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_foreign_key(
            "fk_species_characteristics_substrate",
            "substrates",
            ["substrate_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.execute(
        """
        UPDATE species_characteristics sc
        SET growth_form_id = src.growth_form_id
        FROM (
            SELECT species_id, MIN(growth_form_id) AS growth_form_id
            FROM species_characteristics_growth_forms
            GROUP BY species_id
        ) AS src
        WHERE sc.species_id = src.species_id
        """
    )
    op.execute(
        """
        UPDATE species_characteristics sc
        SET substrate_id = src.substrate_id
        FROM (
            SELECT species_id, MIN(substrate_id) AS substrate_id
            FROM species_characteristics_substrates
            GROUP BY species_id
        ) AS src
        WHERE sc.species_id = src.species_id
        """
    )

    op.drop_index("idx_scs_substrate_id", table_name="species_characteristics_substrates")
    op.drop_table("species_characteristics_substrates")
    op.drop_index("idx_scgf_growth_form_id", table_name="species_characteristics_growth_forms")
    op.drop_table("species_characteristics_growth_forms")
