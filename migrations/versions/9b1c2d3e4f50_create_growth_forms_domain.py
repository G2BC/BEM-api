"""create growth forms domain and link to species characteristics

Revision ID: 9b1c2d3e4f50
Revises: 8d2e4f6a7b8c
Create Date: 2026-03-03 00:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9b1c2d3e4f50"
down_revision = "8d2e4f6a7b8c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "growth_forms",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_growth_forms_slug"),
    )

    op.execute(
        """
        INSERT INTO growth_forms (slug, label_pt, label_en, is_active)
        VALUES
            ('solitary', 'Solitário', 'Solitary', true),
            ('dispersed', 'Disperso', 'Dispersed', true),
            ('gregarious', 'Gregário', 'Gregarious', true),
            ('cespitose', 'Cespiteoso', 'Cespitose', true),
            ('fasciculate', 'Fasciculado', 'Fasciculate', true),
            ('imbricate', 'Imbricado', 'Imbricate', true),
            ('effuse', 'Efuso', 'Effuse', true),
            ('annular', 'Anelar', 'Annular', true),
            ('subcespitose', 'Subcespiteoso', 'Subcespitose', true)
        """
    )

    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("growth_form_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_species_characteristics_growth_form",
            "growth_forms",
            ["growth_form_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_constraint("fk_species_characteristics_growth_form", type_="foreignkey")
        batch_op.drop_column("growth_form_id")

    op.drop_table("growth_forms")
