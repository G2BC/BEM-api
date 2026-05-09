"""update substrates values

Revision ID: cb9fadb82e45
Revises: e469c4fe58cf
Create Date: 2026-04-19 23:52:03.950394

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cb9fadb82e45'
down_revision = 'e469c4fe58cf'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        INSERT INTO substrates (slug, label_en, label_pt)
        VALUES 
        ('campestral', 'Campestral', 'Campestre'),
        ('unknown', 'Unknown', 'Desconhecido');

        DELETE FROM substrates
        WHERE slug IN ('terricolous', 'grassland', 'silvicolous');
    """)


def downgrade():
    pass
