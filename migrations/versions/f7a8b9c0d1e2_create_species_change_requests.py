"""create species change request workflow tables

Revision ID: f7a8b9c0d1e2
Revises: e1f2a3b4c5d6
Create Date: 2026-02-26 21:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f7a8b9c0d1e2"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "species_change_requests",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("species_id", sa.BigInteger(), nullable=False),
        sa.Column("requested_by_user_id", sa.BigInteger(), nullable=True),
        sa.Column("requester_name", sa.String(length=120), nullable=True),
        sa.Column("requester_email", sa.String(length=150), nullable=True),
        sa.Column("requester_institution", sa.String(length=150), nullable=True),
        sa.Column("request_note", sa.Text(), nullable=True),
        sa.Column("proposed_data", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("review_note", sa.Text(), nullable=True),
        sa.Column("reviewed_by_user_id", sa.BigInteger(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected')",
            name="ck_species_change_requests_status_valid",
        ),
        sa.ForeignKeyConstraint(["requested_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["reviewed_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["species_id"], ["species.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    with op.batch_alter_table("species_change_requests", schema=None) as batch_op:
        batch_op.create_index("idx_species_change_requests_species", ["species_id"], unique=False)
        batch_op.create_index("idx_species_change_requests_status", ["status"], unique=False)

    op.create_table(
        "species_photo_requests",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("request_id", sa.BigInteger(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("bucket_name", sa.String(length=100), nullable=True),
        sa.Column("original_filename", sa.String(length=255), nullable=True),
        sa.Column("mime_type", sa.String(length=100), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("caption", sa.Text(), nullable=True),
        sa.Column("license_code", sa.Text(), nullable=True),
        sa.Column("attribution", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected')",
            name="ck_species_photo_requests_status_valid",
        ),
        sa.ForeignKeyConstraint(["request_id"], ["species_change_requests.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.create_index("idx_species_photo_requests_request", ["request_id"], unique=False)


def downgrade():
    with op.batch_alter_table("species_photo_requests", schema=None) as batch_op:
        batch_op.drop_index("idx_species_photo_requests_request")

    op.drop_table("species_photo_requests")

    with op.batch_alter_table("species_change_requests", schema=None) as batch_op:
        batch_op.drop_index("idx_species_change_requests_status")
        batch_op.drop_index("idx_species_change_requests_species")

    op.drop_table("species_change_requests")
