"""update growth forms values

Revision ID: e469c4fe58cf
Revises: 94de0c72afcc
Create Date: 2026-04-19 23:32:00.527386

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e469c4fe58cf'
down_revision = '94de0c72afcc'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        UPDATE growth_forms
        SET slug = 'caespitose', label_en = 'Caespitose', label_pt = 'Cespitosas'
        WHERE slug = 'cespitose';

        UPDATE growth_forms
        SET slug = 'subcaespitose', label_en = 'Subcaespitose', label_pt = 'Subcaespitosa'
        WHERE slug = 'subcespitose';

        INSERT INTO growth_forms (slug, label_en, label_pt)
        VALUES 
        ('scattered', 'Scattered', 'Espalhados'),
        ('trooping', 'Trooping', 'Tropa'),
        ('fairy_ring', 'Fairy ring', 'Círculo de fadas'),
        ('resupinate', 'Resupinate', 'Resupinar');

        DELETE FROM growth_forms
        WHERE slug IN ('dispersed', 'gregarious', 'fasciculate', 'imbricate', 'annular');
    """)


def downgrade():
    pass
