###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Owned data containers: owner reference, membership, element-type metadata.

Adds, per ``orm-container-spec.md`` sections 2-3:

- ``db_dbnode.owner_id``: indexed, nullable FK to ``db_dbnode.id``
  (NULL = standalone, non-null = exclusively owned; default NO ACTION,
  deferred, so deletion planning expands ownership units explicitly).
  RESTRICT is deliberately not used: it cannot be deferred (SQLite enforces
  it immediately even for deferred constraints; PostgreSQL forbids deferring
  it), which would make whole-subtree deletes impossible.
- ``db_dbnode.container_element_type``: nullable stored ``node_type``
  identifier (NULL = unset/locked after first element).
- ``db_dbmembership``: dedicated membership table (``owner_id``, ``child_id``,
  ``position >= 0``, nullable string ``key``) with one-row-per-child,
  per-owner position, per-owner key, and no-self-ownership constraints.

Membership rows are containment edges, never provenance links.

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
    op.add_column('db_dbnode', sa.Column('owner_id', sa.Integer(), nullable=True))
    op.add_column('db_dbnode', sa.Column('container_element_type', sa.String(length=255), nullable=True))
    op.create_index('ix_db_dbnode_db_dbnode_owner_id', 'db_dbnode', ['owner_id'], unique=False)
    op.create_index(
        'ix_db_dbnode_db_dbnode_container_element_type', 'db_dbnode', ['container_element_type'], unique=False
    )
    op.create_foreign_key(
        'fk_db_dbnode_owner_id_db_dbnode',
        'db_dbnode',
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
    op.drop_constraint('fk_db_dbnode_owner_id_db_dbnode', 'db_dbnode', type_='foreignkey')
    op.drop_index('ix_db_dbnode_db_dbnode_container_element_type', table_name='db_dbnode')
    op.drop_index('ix_db_dbnode_db_dbnode_owner_id', table_name='db_dbnode')
    op.drop_column('db_dbnode', 'container_element_type')
    op.drop_column('db_dbnode', 'owner_id')
