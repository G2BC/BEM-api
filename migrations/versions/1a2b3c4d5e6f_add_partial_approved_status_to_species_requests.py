"""add partial approved status to species change requests

Revision ID: 1a2b3c4d5e6f
Revises: a3d92e4c1f7b
Create Date: 2026-02-27 13:30:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "1a2b3c4d5e6f"
down_revision = "a3d92e4c1f7b"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("species_change_requests", schema=None) as batch_op:
        batch_op.drop_constraint("ck_species_change_requests_status_valid", type_="check")
        batch_op.create_check_constraint(
            "ck_species_change_requests_status_valid",
            "status IN ('pending', 'approved', 'partial_approved', 'rejected')",
        )


def downgrade():
    with op.batch_alter_table("species_change_requests", schema=None) as batch_op:
        batch_op.drop_constraint("ck_species_change_requests_status_valid", type_="check")
        batch_op.create_check_constraint(
            "ck_species_change_requests_status_valid",
            "status IN ('pending', 'approved', 'rejected')",
        )
