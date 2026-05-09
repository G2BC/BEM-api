"""create habitats domain and many-to-many with species characteristics

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-03 01:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "habitats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_habitats_slug"),
    )

    op.execute(
        """
        INSERT INTO habitats (slug, label_pt, label_en, is_active)
        VALUES
            ('tropical_forest', 'Floresta tropical', 'Tropical forest', true),
            ('subtropical_forest', 'Floresta subtropical', 'Subtropical forest', true),
            ('temperate_forest', 'Floresta temperada', 'Temperate forest', true),
            ('boreal_forest_taiga', 'Floresta boreal (taiga)', 'Boreal forest (taiga)', true),
            ('coniferous_forest', 'Floresta de coníferas', 'Coniferous forest', true),
            ('riparian_forest', 'Mata ciliar (ripária)', 'Riparian forest', true),
            ('mangrove', 'Manguezal', 'Mangrove', true),
            ('restinga', 'Restinga', 'Restinga', true),
            ('natural_grasslands', 'Campos naturais', 'Natural grasslands', true),
            ('pasture', 'Pastagem', 'Pasture', true),
            ('cerrado_savanna', 'Cerrado / Savana', 'Cerrado / Savanna', true),
            ('caatinga', 'Caatinga', 'Caatinga', true),
            ('dunes_sandy_area', 'Dunas / área arenosa', 'Dunes / sandy area', true),
            ('mountainous_high_altitude', 'Área montanhosa / altitude elevada', 'Mountainous area / high altitude', true),
            ('alpine_environment', 'Ambiente alpino', 'Alpine environment', true),
            ('subalpine_region', 'Região subalpina', 'Subalpine region', true),
            ('wetland_swamp', 'Área alagada / brejo', 'Wetland / swamp', true),
            ('river_lake_margin', 'Margem de rio / lago', 'River / lake margin', true),
            ('soil_terricolous', 'Solo (terrícola)', 'Soil (terricolous)', true),
            ('leaf_litter', 'Serrapilheira', 'Leaf litter', true),
            ('dead_wood_lignicolous', 'Madeira morta (lignícola)', 'Dead wood (lignicolous)', true),
            ('plant_root_mycorrhizal', 'Raiz de planta (micorrízico)', 'Plant root (mycorrhizal)', true),
            ('living_trunk_parasitism', 'Tronco vivo (parasitismo)', 'Living trunk (parasitism)', true),
            ('dung_coprophilous', 'Esterco (coprófilo)', 'Dung (coprophilous)', true),
            ('agricultural_residues', 'Resíduos agrícolas', 'Agricultural residues', true),
            ('composting', 'Compostagem', 'Composting', true),
            ('burned_area', 'Área queimada', 'Burned area', true),
            ('termite_mound', 'Cupinzeiro', 'Termite mound', true),
            ('ant_nest', 'Formigueiro', 'Ant nest', true),
            ('urban_area_gardens_squares', 'Área urbana (jardins, praças)', 'Urban area (gardens, squares)', true)
        """
    )

    op.create_table(
        "species_characteristics_habitats",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("habitat_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["species_id"],
            ["species_characteristics.species_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["habitat_id"], ["habitats.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("species_id", "habitat_id"),
    )

    with op.batch_alter_table("species_characteristics_habitats", schema=None) as batch_op:
        batch_op.create_index("idx_sch_habitat_id", ["habitat_id"], unique=False)


def downgrade():
    with op.batch_alter_table("species_characteristics_habitats", schema=None) as batch_op:
        batch_op.drop_index("idx_sch_habitat_id")

    op.drop_table("species_characteristics_habitats")
    op.drop_table("habitats")
