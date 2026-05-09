"""add role to users with consistency checks

Revision ID: e1f2a3b4c5d6
Revises: c7d8e9f0a1b2
Create Date: 2026-02-26 20:45:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e1f2a3b4c5d6"
down_revision = "c7d8e9f0a1b2"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "role",
                sa.String(length=20),
                nullable=False,
                server_default="researcher",
            )
        )

    op.execute("UPDATE users SET role = 'admin' WHERE is_admin = true")

    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.create_check_constraint(
            "ck_users_role_valid",
            "role IN ('researcher', 'curator', 'admin')",
        )
        batch_op.create_check_constraint(
            "ck_users_role_is_admin_consistent",
            "((role = 'admin') = is_admin)",
        )


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_constraint("ck_users_role_is_admin_consistent", type_="check")
        batch_op.drop_constraint("ck_users_role_valid", type_="check")
        batch_op.drop_column("role")
