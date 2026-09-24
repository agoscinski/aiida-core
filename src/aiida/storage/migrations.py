"""Module with common resources related to storage migrations."""

from __future__ import annotations

import typing as t
from uuid import uuid4

TEMPLATE_INVALID_SCHEMA_VERSION = """
Database schema version `{schema_version_database}` is incompatible with the required schema version `{schema_version_code}`.
To migrate the database schema version to the current one, run the following command:

    verdi -p {profile_name} storage migrate
"""  # noqa: E501

OWNED_TABLES: tuple[str, ...] = ('db_dbnode', 'db_dbgroup', 'db_dbcomment')


def require_single_legacy_user(bind: t.Any) -> None:
    """Reject a multi-user storage before running any destructive migrations."""
    import sqlalchemy as sa
    from sqlalchemy import text

    if sa.inspect(bind).has_table('db_dbuser'):
        count = bind.execute(text('SELECT count(*) FROM db_dbuser')).scalar_one()
        if count > 1:
            msg = 'Cannot migrate storage with multiple users: split them into profiles before migrating.'
            raise RuntimeError(msg)


def migrate_users_to_profile(op: t.Any, profile_uuid: str) -> None:
    """Migrate a single legacy user to a persistent profile without losing contact details.

    Archives have no configured profile UUID; assign one to the archived identity instead.
    Multiple users are rejected before any schema alteration until splitting their configured
    profiles and authentication information is supported.
    """
    import sqlalchemy as sa
    from sqlalchemy import text

    bind = op.get_bind()
    is_sqlite = bind.dialect.name == 'sqlite'
    inspector = sa.inspect(bind)
    require_single_legacy_user(bind)
    users = bind.execute(text('SELECT id, email, first_name, last_name, institution FROM db_dbuser')).mappings().all()

    user_id = users[0]['id'] if users else None
    ownership_columns = [(table, 'user_id') for table in OWNED_TABLES]
    ownership_columns.append(('db_dbauthinfo', 'aiidauser_id'))
    for table, column in ownership_columns:
        if user_id is None:
            invalid_owner = bind.execute(text(f'SELECT 1 FROM {table} LIMIT 1')).first()
        else:
            query = text(f'SELECT 1 FROM {table} WHERE {column} != :id OR {column} IS NULL LIMIT 1')
            invalid_owner = bind.execute(query, {'id': user_id}).first()
        if invalid_owner:
            msg = f'Cannot migrate {table}: its owner does not match the only legacy user.'
            raise RuntimeError(msg)

    # The old user ID is retained as the profile PK, so migrating ownership does not
    # change the identity of existing nodes, groups, comments or authinfos.
    profile_uuid = profile_uuid or str(uuid4())
    op.create_table(
        'db_dbprofile',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('uuid', sa.String(36), nullable=False, unique=True),
        sa.Column('email', sa.String(254), nullable=False),
        sa.Column('first_name', sa.String(254), nullable=False),
        sa.Column('last_name', sa.String(254), nullable=False),
        sa.Column('institution', sa.String(254), nullable=False),
    )
    user = users[0] if users else None
    bind.execute(
        text("""INSERT INTO db_dbprofile (id, uuid, email, first_name, last_name, institution)
                VALUES (:id, :uuid, :email, :first_name, :last_name, :institution)"""),
        {
            'id': user['id'] if user else 1,
            'uuid': profile_uuid,
            'email': user['email'] if user else '',
            'first_name': user['first_name'] if user else '',
            'last_name': user['last_name'] if user else '',
            'institution': user['institution'] if user else '',
        },
    )
    # PostgreSQL sequences do not advance when an explicit ID is inserted.
    if not is_sqlite:
        bind.execute(
            text("SELECT setval(pg_get_serial_sequence('db_dbprofile', 'id'), (SELECT max(id) FROM db_dbprofile))")
        )

    def drop_column_indexes(table: str, column: str) -> None:
        for index in inspector.get_indexes(table):
            if index['column_names'] == [column] and index['name']:
                op.drop_index(index['name'], table_name=table)

    for table in OWNED_TABLES:
        op.add_column(table, sa.Column('profile_uuid', sa.String(36), nullable=True))
        bind.execute(text(f'UPDATE {table} SET profile_uuid = :uuid'), {'uuid': profile_uuid})
        op.create_index(f'ix_{table}_{table}_profile_uuid', table, ['profile_uuid'])
        if is_sqlite:
            drop_column_indexes(table, 'user_id')
            with op.batch_alter_table(table) as batch_op:
                batch_op.alter_column('profile_uuid', existing_type=sa.String(36), nullable=False)
                batch_op.drop_column('user_id')
        else:
            op.alter_column(table, 'profile_uuid', existing_type=sa.String(36), nullable=False)
            _drop_user_fk(op, bind, table, 'user_id')
            op.drop_column(table, 'user_id')

    op.add_column('db_dbauthinfo', sa.Column('profile_uuid', sa.String(36), nullable=True))
    bind.execute(text('UPDATE db_dbauthinfo SET profile_uuid = :uuid'), {'uuid': profile_uuid})
    if is_sqlite:
        for constraint in inspector.get_unique_constraints('db_dbauthinfo'):
            if 'aiidauser_id' in constraint['column_names'] and constraint['name']:
                with op.batch_alter_table('db_dbauthinfo') as batch_op:
                    batch_op.drop_constraint(constraint['name'], type_='unique')
                break
        drop_column_indexes('db_dbauthinfo', 'aiidauser_id')
        with op.batch_alter_table('db_dbauthinfo') as batch_op:
            batch_op.alter_column('profile_uuid', existing_type=sa.String(36), nullable=False)
            batch_op.drop_column('aiidauser_id')
            batch_op.create_unique_constraint(
                'uq_db_dbauthinfo_profile_uuid_dbcomputer_id', ['profile_uuid', 'dbcomputer_id']
            )
    else:
        op.alter_column('db_dbauthinfo', 'profile_uuid', existing_type=sa.String(36), nullable=False)
        _drop_user_fk(op, bind, 'db_dbauthinfo', 'aiidauser_id')
        for constraint in sa.inspect(bind).get_unique_constraints('db_dbauthinfo'):
            if 'aiidauser_id' in constraint['column_names']:
                op.drop_constraint(constraint['name'], 'db_dbauthinfo', type_='unique')
        op.drop_column('db_dbauthinfo', 'aiidauser_id')
        op.create_unique_constraint(
            'uq_db_dbauthinfo_profile_uuid_dbcomputer_id', 'db_dbauthinfo', ['profile_uuid', 'dbcomputer_id']
        )
    op.create_index('ix_db_dbauthinfo_db_dbauthinfo_profile_uuid', 'db_dbauthinfo', ['profile_uuid'])
    op.drop_table('db_dbuser')


def _drop_user_fk(op: t.Any, bind: t.Any, table: str, column: str) -> None:
    """Drop the foreign key on ``column`` referencing ``db_dbuser``, if present."""
    import sqlalchemy as sa

    for foreign_key in sa.inspect(bind).get_foreign_keys(table):
        if foreign_key['constrained_columns'] == [column] and foreign_key['referred_table'] == 'db_dbuser':
            op.drop_constraint(foreign_key['name'], table, type_='foreignkey')
