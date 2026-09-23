"""Module with common resources related to storage migrations."""

from __future__ import annotations

import typing as t

TEMPLATE_INVALID_SCHEMA_VERSION = """
Database schema version `{schema_version_database}` is incompatible with the required schema version `{schema_version_code}`.
To migrate the database schema version to the current one, run the following command:

    verdi -p {profile_name} storage migrate
"""  # noqa: E501

#: Tables whose rows carry a per-row owner that is migrated to a profile UUID label.
OWNED_TABLES: tuple[str, ...] = ('db_dbnode', 'db_dbgroup', 'db_dbcomment')


def migrate_users_to_profile(op: t.Any, profile_uuid: str) -> None:
    """Replace per-row user ownership with a profile UUID label.

    This implements the ``orm.User`` → ``profile_uuid`` migration agreed for
    `#7553 <https://github.com/aiidateam/aiida-core/issues/7553>`_: every row
    in the database is assumed to belong to the profile being migrated (the
    single-owner model), so all rows are stamped with its UUID. Email and other
    contact strings are discarded by design; the UUID is the only identity.

    The transformation, applied in order:

    1. On ``db_dbnode``, ``db_dbgroup`` and ``db_dbcomment``: add a
       ``profile_uuid`` column, backfill it, make it non-nullable, then drop
       the ``user_id`` foreign key column.
    2. On ``db_dbauthinfo``: drop the ``aiidauser_id`` column (authentication
       becomes per-computer) and replace its unique constraint with one on
       ``dbcomputer_id`` alone.
    3. Drop the ``db_dbuser`` table.

    ``profile_uuid`` is a plain string with no foreign key: the configuration
    file stays the source of truth for profiles, so renames and UUID rotations
    never require database writes.

    .. note::

        On SQLite the table rebuilds below DROP and recreate parent tables.
        With foreign key enforcement on, SQLite turns the DROP into an implicit
        ``DELETE FROM`` which cascades into child tables and silently destroys
        data (see https://alembic.sqlalchemy.org/en/latest/batch.html#dealing-with-referencing-foreign-keys).
        The migration connection must therefore run with enforcement off; the
        ``sqlite_dos`` migrator and the archive migrator both create their
        engines with ``enforce_foreign_keys=False`` for this reason.

    :param op: the Alembic operations object of the calling revision.
    :param profile_uuid: the UUID of the profile being migrated. Callers
        without a meaningful profile (e.g. profile-agnostic archives) pass an
        empty string to mark unknown origin; the importing profile keeps the
        stored values as-is.
    """
    import sqlalchemy as sa
    from sqlalchemy import text

    bind = op.get_bind()
    is_sqlite = bind.dialect.name == 'sqlite'
    inspector = sa.inspect(bind)

    def _drop_column_indexes(table: str, column: str) -> None:
        """Drop single-column indexes on ``column`` ahead of a column drop.

        SQLite recreates the whole table on alteration and would otherwise try
        to recreate indexes of the removed column.
        """
        for index in inspector.get_indexes(table):
            if index['column_names'] == [column] and index['name']:
                op.drop_index(index['name'], table_name=table)

    for table in OWNED_TABLES:
        op.add_column(table, sa.Column('profile_uuid', sa.String(36), nullable=True))
        op.execute(text(f"UPDATE {table} SET profile_uuid = '{profile_uuid}'"))
        # Match the model naming convention (``ix_<table>_<table>_<column>``): migrated databases must be
        # indistinguishable from freshly initialised ones.
        op.create_index(f'ix_{table}_{table}_profile_uuid', table, ['profile_uuid'])
        if is_sqlite:
            _drop_column_indexes(table, 'user_id')
            with op.batch_alter_table(table) as batch_op:
                batch_op.alter_column('profile_uuid', existing_type=sa.String(36), nullable=False)
                batch_op.drop_column('user_id')
        else:
            op.alter_column(table, 'profile_uuid', existing_type=sa.String(36), nullable=False)
            _drop_user_fk(op, bind, table, 'user_id')
            op.drop_column(table, 'user_id')

    if is_sqlite:
        for constraint in inspector.get_unique_constraints('db_dbauthinfo'):
            if 'aiidauser_id' not in constraint['column_names']:
                continue
            # Legacy databases (e.g. old archives) may carry the constraint without a name; dropping the column
            # in the rebuild below removes it regardless, so only drop by name when there is one to address.
            if constraint['name']:
                with op.batch_alter_table('db_dbauthinfo') as batch_op:
                    batch_op.drop_constraint(constraint['name'], type_='unique')
            break
        _drop_column_indexes('db_dbauthinfo', 'aiidauser_id')
        with op.batch_alter_table('db_dbauthinfo') as batch_op:
            batch_op.drop_column('aiidauser_id')
            batch_op.create_unique_constraint('uq_db_dbauthinfo_dbcomputer_id', ['dbcomputer_id'])
    else:
        _drop_user_fk(op, bind, 'db_dbauthinfo', 'aiidauser_id')
        for constraint in sa.inspect(bind).get_unique_constraints('db_dbauthinfo'):
            if 'aiidauser_id' in constraint['column_names']:
                op.drop_constraint(constraint['name'], 'db_dbauthinfo', type_='unique')
        op.drop_column('db_dbauthinfo', 'aiidauser_id')
        op.create_unique_constraint('uq_db_dbauthinfo_dbcomputer_id', 'db_dbauthinfo', ['dbcomputer_id'])

    op.drop_table('db_dbuser')


def _drop_user_fk(op: t.Any, bind: t.Any, table: str, column: str) -> None:
    """Drop the foreign key on ``column`` referencing ``db_dbuser``, if present.

    Constraint names are discovered by inspection instead of assumed, since
    databases created by different schema generations name them differently
    (naming-convention names vs PostgreSQL defaults).
    """
    import sqlalchemy as sa

    for foreign_key in sa.inspect(bind).get_foreign_keys(table):
        if foreign_key['constrained_columns'] == [column] and foreign_key['referred_table'] == 'db_dbuser':
            op.drop_constraint(foreign_key['name'], table, type_='foreignkey')
