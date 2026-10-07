###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Owned data containers: owner reference, membership, element-type metadata.

SQLite counterpart of the ``psql_dos`` ``main_0004`` revision, per
``orm-container-spec.md`` sections 2-3. Adds ``db_dbnode.owner_id``,
``db_dbnode.container_element_type``, and the ``db_dbmembership`` table
with the same logical constraints. Membership rows are containment edges,
never provenance links.

Revision ID: main_0004
Revises: main_0003
Create Date: 2026-10-07
"""

import sqlalchemy as sa
from alembic import op

revision = 'main_0004'
down_revision = 'main_0003'
branch_labels = None
depends_on = None


def upgrade():
    """Migrations for the upgrade."""
    # SQLite does not support ALTER TABLE ... ADD CONSTRAINT, so the existing table is migrated in batch
    # (copy-and-move) mode, which recreates it with the new columns, indexes, and foreign key.
    with op.batch_alter_table('db_dbnode') as batch_op:
        batch_op.add_column(sa.Column('owner_id', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('container_element_type', sa.String(length=255), nullable=True))
        batch_op.create_index('ix_db_dbnode_db_dbnode_owner_id', ['owner_id'])
        batch_op.create_index('ix_db_dbnode_db_dbnode_container_element_type', ['container_element_type'])
        batch_op.create_foreign_key(
            'fk_db_dbnode_owner_id_db_dbnode',
            'db_dbnode',
            ['owner_id'],
            ['id'],
            deferrable=True,
            initially='DEFERRED',
        )
    op.create_table(
        'db_dbmembership',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('owner_id', sa.Integer(), nullable=False, index=True),
        sa.Column('child_id', sa.Integer(), nullable=False, index=True),
        sa.Column('position', sa.Integer(), nullable=False),
        sa.Column('key', sa.String(length=255), nullable=True, index=True),
        sa.ForeignKeyConstraint(['owner_id'], ['db_dbnode.id'], deferrable=True, initially='DEFERRED'),
        sa.ForeignKeyConstraint(['child_id'], ['db_dbnode.id'], deferrable=True, initially='DEFERRED'),
        sa.UniqueConstraint('child_id', name='uq_db_dbmembership_child_id'),
        sa.UniqueConstraint('owner_id', 'position', name='uq_db_dbmembership_owner_id_position'),
        sa.UniqueConstraint('owner_id', 'key', name='uq_db_dbmembership_owner_id_key'),
        sa.CheckConstraint('position >= 0', name='ck_db_dbmembership_position_nonnegative'),
        sa.CheckConstraint('owner_id != child_id', name='ck_db_dbmembership_no_self_ownership'),
    )


def downgrade():
    """Migrations for the downgrade."""
    op.drop_table('db_dbmembership')
    with op.batch_alter_table('db_dbnode') as batch_op:
        batch_op.drop_constraint('fk_db_dbnode_owner_id_db_dbnode', type_='foreignkey')
        batch_op.drop_index('ix_db_dbnode_db_dbnode_container_element_type')
        batch_op.drop_index('ix_db_dbnode_db_dbnode_owner_id')
        batch_op.drop_column('container_element_type')
        batch_op.drop_column('owner_id')
