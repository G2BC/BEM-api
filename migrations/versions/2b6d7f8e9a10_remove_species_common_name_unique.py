"""remove species common_name unique

Revision ID: 2b6d7f8e9a10
Revises: 151dca54bb38
Create Date: 2026-05-08 23:45:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '2b6d7f8e9a10'
down_revision = '151dca54bb38'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('species', schema=None) as batch_op:
        batch_op.drop_constraint('uq_species_common_name', type_='unique')


def downgrade():
    with op.batch_alter_table('species', schema=None) as batch_op:
        batch_op.create_unique_constraint('uq_species_common_name', ['common_name'])
