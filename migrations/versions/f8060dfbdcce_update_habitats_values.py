"""update habitats values

Revision ID: f8060dfbdcce
Revises: c03188550cb7
Create Date: 2026-04-20 00:09:06.037755

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f8060dfbdcce'
down_revision = 'c03188550cb7'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DELETE FROM habitats;

        INSERT INTO habitats (slug, label_en, label_pt)
        VALUES
            ('tropical_subtropical_moist_broadleaf_forest', 'Tropical & subtropical moist broadleaf forest', 'Floresta latifoliada úmida tropical e subtropical'),
            ('tropical_subtropical_dry_broadleaf_forest', 'Tropical & subtropical dry broadleaf forest', 'Floresta latifoliada seca tropical e subtropical'),
            ('tropical_subtropical_coniferous_forest', 'Tropical & subtropical coniferous forest', 'Floresta de coníferas tropical e subtropical'),
            ('temperate_broadleaf_mixed_forest', 'Temperate broadleaf & mixed forest', 'Floresta temperada latifoliada e mista'),
            ('temperate_conifer_forest', 'Temperate conifer forest', 'Floresta temperada de coníferas'),
            ('boreal_forest_taiga', 'Boreal forest & taiga', 'Floresta boreal e taiga'),
            ('mediterranean_forest_woodland_scrub', 'Mediterranean forest, woodland & scrub', 'Floresta, bosque e vegetação arbustiva mediterrânea'),
            ('tropical_grassland_savanna', 'Tropical grassland & savanna', 'Campos tropicais e savana'),
            ('temperate_grassland_steppe', 'Temperate grassland & steppe', 'Campos temperados e estepe'),
            ('montane_grassland_shrubland', 'Montane grassland & shrubland', 'Campos e arbustais montanos'),
            ('flooded_grassland_savanna', 'Flooded grassland & savanna', 'Campos inundáveis e savana'),
            ('tundra', 'Tundra', 'Tundra'),
            ('desert_xeric_shrubland', 'Desert & xeric shrubland', 'Deserto e arbustal xérico'),
            ('mangrove', 'Mangrove', 'Manguezal'),
            ('inland_freshwater_bodies', 'Inland freshwater bodies', 'Corpos de água doce continentais'),
            ('coastal_marine', 'Coastal & marine', 'Costeiro e marinho'),
            ('artificial_urban_suburban', 'Artificial — urban & suburban', 'Artificial — urbano e suburbano'),
            ('artificial_agricultural', 'Artificial — agricultural', 'Artificial — agrícola');
    """)


def downgrade():
    pass
