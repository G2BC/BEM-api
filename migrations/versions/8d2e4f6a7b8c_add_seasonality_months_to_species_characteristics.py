"""add seasonality month range to species characteristics

Revision ID: 8d2e4f6a7b8c
Revises: 7c9d1e2f3a4b
Create Date: 2026-03-02 23:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8d2e4f6a7b8c"
down_revision = "7c9d1e2f3a4b"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "nutrition_modes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_nutrition_modes_slug"),
    )

    op.execute(
        """
        INSERT INTO nutrition_modes (slug, label_pt, label_en, is_active)
        VALUES
            ('saprotrophic', 'Saprófito', 'Saprotrophic', true),
            ('mycorrhizal', 'Micorrízico', 'Mycorrhizal', true),
            ('parasitic', 'Parasita', 'Parasitic', true),
            ('hemibiotrophic', 'Hemibiotrófico', 'Hemibiotrophic', true),
            ('non_mycorrhizal_mutualist', 'Mutualista não micorrízico', 'Non-mycorrhizal mutualist', true),
            ('coprophilous', 'Coprófilo', 'Coprophilous', true),
            ('endophytic', 'Endofítico', 'Endophytic', true)
        """
    )

    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.add_column(sa.Column("cultivation", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("finding_tips", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("nearby_trees", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("curiosities", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("general_description", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("colors", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("size_cm", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("nutrition_mode_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("season_start_month", sa.SmallInteger(), nullable=True))
        batch_op.add_column(sa.Column("season_end_month", sa.SmallInteger(), nullable=True))
        batch_op.create_foreign_key(
            "fk_species_characteristics_nutrition_mode",
            "nutrition_modes",
            ["nutrition_mode_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_check_constraint(
            "ck_species_characteristics_season_start_month_range",
            "season_start_month IS NULL OR season_start_month BETWEEN 1 AND 12",
        )
        batch_op.create_check_constraint(
            "ck_species_characteristics_season_end_month_range",
            "season_end_month IS NULL OR season_end_month BETWEEN 1 AND 12",
        )
        batch_op.create_check_constraint(
            "ck_species_characteristics_season_months_pair",
            "(season_start_month IS NULL) = (season_end_month IS NULL)",
        )


def downgrade():
    with op.batch_alter_table("species_characteristics", schema=None) as batch_op:
        batch_op.drop_constraint("fk_species_characteristics_nutrition_mode", type_="foreignkey")
        batch_op.drop_constraint("ck_species_characteristics_season_months_pair", type_="check")
        batch_op.drop_constraint(
            "ck_species_characteristics_season_end_month_range", type_="check"
        )
        batch_op.drop_constraint(
            "ck_species_characteristics_season_start_month_range", type_="check"
        )
        batch_op.drop_column("season_end_month")
        batch_op.drop_column("season_start_month")
        batch_op.drop_column("nutrition_mode_id")
        batch_op.drop_column("general_description")
        batch_op.drop_column("curiosities")
        batch_op.drop_column("nearby_trees")
        batch_op.drop_column("finding_tips")
        batch_op.drop_column("colors")
        batch_op.drop_column("size_cm")
        batch_op.drop_column("cultivation")

    op.drop_table("nutrition_modes")
