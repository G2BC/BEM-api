"""create_distribution_table

Revision ID: e0383ee632a3
Revises: affe8fe40000
Create Date: 2026-04-01 21:16:39.632275
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e0383ee632a3"
down_revision = "affe8fe40000"
branch_labels = None
depends_on = None


distribution_table = sa.table(
    "distributions",
    sa.column("id", sa.Integer),
    sa.column("slug", sa.Text),
    sa.column("label_en", sa.Text),
    sa.column("label_pt", sa.Text),
)


def upgrade():
    op.create_table(
        "distributions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )

    op.bulk_insert(
        distribution_table,
        [
            {"id": 1, "slug": "EU", "label_en": "Europe", "label_pt": "Europa"},
            {"id": 2, "slug": "NA", "label_en": "North America", "label_pt": "América do Norte"},
            {"id": 3, "slug": "SA", "label_en": "South America", "label_pt": "América do Sul"},
            {"id": 4, "slug": "CA", "label_en": "Central America and the Caribbean region", "label_pt": "América Central e Caribe"},
            {"id": 5, "slug": "PA", "label_en": "Pacific islands", "label_pt": "Ilhas do Pacífico"},
            {"id": 6, "slug": "CH", "label_en": "China", "label_pt": "China"},
            {"id": 7, "slug": "JP", "label_en": "Japan", "label_pt": "Japão"},
            {"id": 8, "slug": "MS", "label_en": "Malesia, South Asia, and Southeastern Asia", "label_pt": "Malésia, Sul da Ásia e Sudeste Asiático"},
            {"id": 9, "slug": "AU", "label_en": "Australasia including Papua New Guinea and New Caledonia", "label_pt": "Australásia, incluindo Papua-Nova Guiné e Nova Caledônia"},
            {"id": 10, "slug": "AF", "label_en": "Africa", "label_pt": "África"},
        ],
    )


def downgrade():
    op.drop_table("distributions")