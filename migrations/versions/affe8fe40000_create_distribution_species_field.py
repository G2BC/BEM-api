"""create_distribution_species_field

Revision ID: affe8fe40000
Revises: f2c8684f0de7
Create Date: 2026-04-01 21:03:11.042036
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "affe8fe40000"
down_revision = "f2c8684f0de7"
branch_labels = None
depends_on = None

distribution_enum = postgresql.ENUM(
    "EU",
    "NA",
    "SA",
    "CA",
    "PA",
    "CH",
    "JP",
    "MS",
    "AU",
    "AF",
    name="distribution_enum",
)

def upgrade():
    bind = op.get_bind()
    distribution_enum.create(bind, checkfirst=True)

    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "distribution",
                postgresql.ARRAY(distribution_enum),
                server_default=sa.text("'{}'::distribution_enum[]"),
                nullable=False,
            )
        )
        batch_op.drop_index("idx_species_distribution", postgresql_using="gin")
        batch_op.create_index(
            "idx_species_distribution",
            ["distribution"],
            unique=False,
            postgresql_using="gin",
        )
        batch_op.drop_column("distribution_regions")


def downgrade():
    with op.batch_alter_table("species", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "distribution_regions",
                postgresql.ARRAY(sa.Text()),
                server_default=sa.text("'{}'::text[]"),
                nullable=False,
            )
        )
        batch_op.drop_index("idx_species_distribution", postgresql_using="gin")
        batch_op.create_index(
            "idx_species_distribution",
            ["distribution_regions"],
            unique=False,
            postgresql_using="gin",
        )
        batch_op.drop_column("distribution")

    bind = op.get_bind()
    distribution_enum.drop(bind, checkfirst=True)