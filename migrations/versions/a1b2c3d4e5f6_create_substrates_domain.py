"""create substrates domain and link to species characteristics

Revision ID: a1b2c3d4e5f6
Revises: 9b1c2d3e4f50
Create Date: 2026-03-03 00:40:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "9b1c2d3e4f50"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "substrates",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_substrates_slug"),
    )

    op.execute(
        """
        INSERT INTO substrates (slug, label_pt, label_en, is_active)
        VALUES
            ('lignicolous', 'Lignícola', 'Lignicolous', true),
            ('terricolous', 'Terrícola', 'Terricolous', true),
            ('humicolous', 'Humícola', 'Humicolous', true),
            ('folicolous', 'Folícola', 'Folicolous', true),
            ('muscicolous', 'Muscícola', 'Muscicolous', true),
            ('grassland', 'Campestre', 'Grassland', true),
            ('silvicolous', 'Silvícola', 'Silvicolous', true),
            ('psammophilous', 'Psamófilo', 'Psammophilous', true),
            ('ripicolous', 'Ripícola', 'Ripicolous', true),
            ('carbonicolous', 'Carbonícola', 'Carbonicolous', true)
        """
    )

    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("substrate_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_species_characteristics_substrate",
            "substrates",
            ["substrate_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_constraint("fk_species_characteristics_substrate", type_="foreignkey")
        batch_op.drop_column("substrate_id")

    op.drop_table("substrates")
