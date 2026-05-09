"""create decay_types domain and link to species characteristics

Revision ID: d1e2f3a4b5c6
Revises: cb9fadb82e45
Create Date: 2026-04-23 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d1e2f3a4b5c6"
down_revision = "f8060dfbdcce"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "decay_types",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("label_pt", sa.Text(), nullable=False),
        sa.Column("label_en", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_decay_types_slug"),
    )

    op.execute(
        """
        INSERT INTO decay_types (slug, label_pt, label_en, is_active)
        VALUES
            ('white_rot', 'Podridão branca (lignina + celulose degradadas)', 'White rot (lignin + cellulose degraded)', true),
            ('brown_rot', 'Podridão parda (celulose degradada, lignina preservada)', 'Brown rot (cellulose degraded, lignin remains)', true),
            ('soft_rot', 'Podridão mole (celulose degradada por ascomicetos)', 'Soft rot (cellulose degraded by ascomycetes)', true),
            ('litter_decomposition', 'Decomposição de serapilheira', 'Litter decomposition', true),
            ('humus_formation', 'Formação de húmus', 'Humus formation', true),
            ('not_applicable', 'Não aplicável — não saprotrófico', 'Not applicable — non-saprotrophic', true)
        """
    )

    op.create_table(
        "species_characteristics_decay_types",
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("decay_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["species_id"],
            ["species_characteristics.species_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["decay_type_id"],
            ["decay_types.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("species_id", "decay_type_id"),
    )
    op.create_index(
        "idx_scdt_decay_type_id",
        "species_characteristics_decay_types",
        ["decay_type_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("idx_scdt_decay_type_id", table_name="species_characteristics_decay_types")
    op.drop_table("species_characteristics_decay_types")
    op.drop_table("decay_types")
