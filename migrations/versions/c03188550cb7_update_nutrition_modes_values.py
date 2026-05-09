"""update nutrition modes values

Revision ID: c03188550cb7
Revises: cb9fadb82e45
Create Date: 2026-04-20 00:05:20.291516

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c03188550cb7'
down_revision = 'cb9fadb82e45'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DELETE FROM nutrition_modes;

        INSERT INTO nutrition_modes (slug, label_en, label_pt)
        VALUES
        ('wood_saprotroph', 'Wood saprotroph', 'Saprófito de madeira'),
        ('litter_saprotroph', 'Litter saprotroph', 'Saprófito de serapilheira'),
        ('soil_saprotroph', 'Soil saprotroph', 'Saprófito de solo'),
        ('dung_saprotroph', 'Dung saprotroph', 'Saprófito de esterco'),
        ('nectar_sap_saprotroph', 'Nectar & sap saprotroph', 'Saprófito de néctar e seiva'),
        ('pollen_saprotroph', 'Pollen saprotroph', 'Saprófito de pólen'),
        ('unspecified_saprotroph', 'Unspecified saprotroph', 'Saprófito não especificado'),
        ('plant_pathogen', 'Plant pathogen', 'Patógeno de plantas'),
        ('ectomycorrhizal', 'Ectomycorrhizal', 'Ectomicorrízico'),
        ('arbuscular_mycorrhizal', 'Arbuscular mycorrhizal', 'Micorrízico arbuscular'),
        ('ericoid_mycorrhizal', 'Ericoid mycorrhizal', 'Micorrízico ericoide'),
        ('orchid_mycorrhizal', 'Orchid mycorrhizal', 'Micorrízico de orquídeas'),
        ('foliar_endophyte', 'Foliar endophyte', 'Endófito foliar'),
        ('root_endophyte', 'Root endophyte', 'Endófito de raiz'),
        ('epiphyte', 'Epiphyte', 'Epífito'),
        ('lichenised', 'Lichenised', 'Liquenizado'),
        ('mycoparasite', 'Mycoparasite', 'Micoparasita'),
        ('lichen_parasite', 'Lichen parasite', 'Parasita de líquen'),
        ('animal_parasite', 'Animal parasite', 'Parasita de animais'),
        ('animal_endosymbiont', 'Animal endosymbiont', 'Endossimbionte animal'),
        ('unknown', 'Unknown', 'Desconhecido');
    """)


def downgrade():
    pass
