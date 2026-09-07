###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Shared SQLAlchemy model definitions for SQL storage backends."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql.sqltypes import TypeEngine

from aiida.common import timezone
from aiida.common.utils import get_new_uuid


@dataclass(frozen=True)
class ModelClasses:
    """The ORM classes created for a database dialect."""

    authinfo: type
    comment: type
    computer: type
    group: type
    group_node: type
    link: type
    log: type
    node: type
    setting: type
    user: type


def create_models(
    base: type,
    *,
    datetime_type: TypeEngine[Any],
    json_type: TypeEngine[Any],
    uuid_type: TypeEngine[Any],
    create_pattern_index: Callable[[str, Any, str], Index | None],
) -> ModelClasses:
    """Create ORM models for a SQL dialect.

    :param base: Declarative base to which the models are mapped.
    :param datetime_type: Dialect-specific timestamp type.
    :param json_type: Dialect-specific JSON type.
    :param uuid_type: Dialect-specific UUID type.
    :param create_pattern_index: Create an optional index optimized for prefix matching.
    """

    def pattern_indexes(*indexes: Index | None) -> tuple[Index, ...]:
        """Return the dialect-supported indexes."""
        return tuple(index for index in indexes if index is not None)

    class DbUser(base):
        __tablename__ = 'db_dbuser'

        id = Column(Integer, primary_key=True)
        email = Column(String(254), nullable=False, unique=True)
        first_name = Column(String(254), default='', nullable=False)
        last_name = Column(String(254), default='', nullable=False)
        institution = Column(String(254), default='', nullable=False)

        __table_args__ = pattern_indexes(create_pattern_index('ix_pat_db_dbuser_email', email, 'email'))

        def __str__(self):
            return self.email

    class DbComputer(base):
        __tablename__ = 'db_dbcomputer'

        id = Column(Integer, primary_key=True)
        uuid = Column(uuid_type, default=get_new_uuid, nullable=False, unique=True)
        label = Column(String(255), nullable=False, unique=True)
        hostname = Column(String(255), default='', nullable=False)
        description = Column(Text, default='', nullable=False)
        scheduler_type = Column(String(255), default='', nullable=False)
        transport_type = Column(String(255), default='', nullable=False)
        _metadata = Column('metadata', json_type, default=dict, nullable=False)

        __table_args__ = pattern_indexes(create_pattern_index('ix_pat_db_dbcomputer_label', label, 'label'))

        @property
        def pk(self):
            return self.id

        def __str__(self):
            return f'{self.label} ({self.hostname})'

    class DbGroupNode(base):
        __tablename__ = 'db_dbgroup_dbnodes'

        id = Column(Integer, primary_key=True)
        dbnode_id = Column(
            Integer, ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True
        )
        dbgroup_id = Column(
            Integer, ForeignKey('db_dbgroup.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True
        )

        __table_args__ = (UniqueConstraint('dbgroup_id', 'dbnode_id'),)

    class DbGroup(base):
        __tablename__ = 'db_dbgroup'

        id = Column(Integer, primary_key=True)
        uuid = Column(uuid_type, default=get_new_uuid, nullable=False, unique=True)
        label = Column(String(255), nullable=False, index=True)
        type_string = Column(String(255), default='', nullable=False, index=True)
        time = Column(datetime_type, default=timezone.now, nullable=False)
        description = Column(Text, default='', nullable=False)
        extras = Column(json_type, default=dict, nullable=False)
        user_id = Column(
            Integer,
            ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )

        user = relationship('DbUser', backref=backref('dbgroups', cascade='merge'))
        dbnodes = relationship('DbNode', secondary=DbGroupNode.__table__, backref='dbgroups', lazy='dynamic')

        __table_args__ = (
            UniqueConstraint('label', 'type_string'),
            *pattern_indexes(
                create_pattern_index('ix_pat_db_dbgroup_label', label, 'label'),
                create_pattern_index('ix_pat_db_dbgroup_type_string', type_string, 'type_string'),
            ),
        )

        @property
        def pk(self):
            return self.id

        def __str__(self):
            return f'<DbGroup [type: {self.type_string}] "{self.label}">'

    class DbNode(base):
        __tablename__ = 'db_dbnode'

        id = Column(Integer, primary_key=True)
        uuid = Column(uuid_type, default=get_new_uuid, nullable=False, unique=True)
        node_type = Column(String(255), default='', nullable=False, index=True)
        process_type = Column(String(255), index=True)
        label = Column(String(255), nullable=False, default='', index=True)
        description = Column(Text(), nullable=False, default='')
        ctime = Column(datetime_type, default=timezone.now, nullable=False, index=True)
        mtime = Column(datetime_type, default=timezone.now, onupdate=timezone.now, nullable=False, index=True)
        attributes = Column(json_type, default=dict)
        extras = Column(json_type, default=dict)
        repository_metadata = Column(json_type, nullable=False, default=dict)
        dbcomputer_id = Column(
            Integer,
            ForeignKey('db_dbcomputer.id', deferrable=True, initially='DEFERRED', ondelete='RESTRICT'),
            nullable=True,
            index=True,
        )
        user_id = Column(
            Integer,
            ForeignKey('db_dbuser.id', deferrable=True, initially='DEFERRED', ondelete='RESTRICT'),
            nullable=False,
            index=True,
        )

        dbcomputer = relationship('DbComputer', backref=backref('dbnodes', passive_deletes='all', cascade='merge'))
        user = relationship('DbUser', backref=backref('dbnodes', passive_deletes='all', cascade='merge'))
        outputs_q = relationship(
            'DbNode',
            secondary='db_dblink',
            primaryjoin='DbNode.id == DbLink.input_id',
            secondaryjoin='DbNode.id == DbLink.output_id',
            backref=backref('inputs_q', passive_deletes=True, lazy='dynamic'),
            lazy='dynamic',
            passive_deletes=True,
        )

        __table_args__ = pattern_indexes(
            create_pattern_index('ix_pat_db_dbnode_label', label, 'label'),
            create_pattern_index('ix_pat_db_dbnode_node_type', node_type, 'node_type'),
            create_pattern_index('ix_pat_db_dbnode_process_type', process_type, 'process_type'),
        )

        @property
        def outputs(self):
            return self.outputs_q.all()

        @property
        def inputs(self):
            return self.inputs_q.all()

        def get_simple_name(self, invalid_result=None):
            thistype = str(self.node_type)
            if thistype == '':
                thistype = 'node.Node.'
            if not thistype.endswith('.'):
                return invalid_result
            return thistype[:-1].rpartition('.')[2]

        @property
        def pk(self):
            return self.id

        def __str__(self):
            simplename = self.get_simple_name(invalid_result='Unknown')
            if self.label:
                return f'{simplename} node [{self.pk}]: {self.label}'
            return f'{simplename} node [{self.pk}]'

    class DbLink(base):
        __tablename__ = 'db_dblink'

        id = Column(Integer, primary_key=True)
        input_id = Column(
            Integer, ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True
        )
        output_id = Column(
            Integer,
            ForeignKey('db_dbnode.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )
        input = relationship('DbNode', primaryjoin='DbLink.input_id == DbNode.id', overlaps='inputs_q,outputs_q')
        output = relationship('DbNode', primaryjoin='DbLink.output_id == DbNode.id', overlaps='inputs_q,outputs_q')
        label = Column(String(255), nullable=False, index=True)
        type = Column(String(255), nullable=False, index=True)

        __table_args__ = pattern_indexes(
            create_pattern_index('ix_pat_db_dblink_label', label, 'label'),
            create_pattern_index('ix_pat_db_dblink_type', type, 'type'),
        )

        def __str__(self):
            return '{} ({}) --> {} ({})'.format(
                self.input.get_simple_name(invalid_result='Unknown node'),
                self.input.pk,
                self.output.get_simple_name(invalid_result='Unknown node'),
                self.output.pk,
            )

    class DbAuthInfo(base):
        __tablename__ = 'db_dbauthinfo'

        id = Column(Integer, primary_key=True)
        aiidauser_id = Column(
            Integer,
            ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )
        dbcomputer_id = Column(
            Integer,
            ForeignKey('db_dbcomputer.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )
        _metadata = Column('metadata', json_type, default=dict, nullable=False)
        auth_params = Column(json_type, default=dict, nullable=False)
        enabled = Column(Boolean, default=True, nullable=False)

        aiidauser = relationship('DbUser', backref=backref('authinfos', passive_deletes=True, cascade='all, delete'))
        dbcomputer = relationship(
            'DbComputer', backref=backref('authinfos', passive_deletes=True, cascade='all, delete')
        )

        __table_args__ = (UniqueConstraint('aiidauser_id', 'dbcomputer_id'),)

        def __str__(self):
            if self.enabled:
                return f'DB authorization info for {self.aiidauser.email} on {self.dbcomputer.label}'
            return f'DB authorization info for {self.aiidauser.email} on {self.dbcomputer.label} [DISABLED]'

    class DbComment(base):
        __tablename__ = 'db_dbcomment'

        id = Column(Integer, primary_key=True)
        uuid = Column(uuid_type, default=get_new_uuid, nullable=False, unique=True)
        dbnode_id = Column(
            Integer,
            ForeignKey('db_dbnode.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )
        ctime = Column(datetime_type, default=timezone.now, nullable=False)
        mtime = Column(datetime_type, default=timezone.now, onupdate=timezone.now, nullable=False)
        user_id = Column(
            Integer,
            ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
            nullable=False,
            index=True,
        )
        content = Column(Text, default='', nullable=False)

        dbnode = relationship('DbNode', backref='dbcomments')
        user = relationship('DbUser')

        def __str__(self):
            return 'DbComment for [{} {}] on {}'.format(
                self.dbnode.get_simple_name(),
                self.dbnode.id,
                timezone.localtime(cast(datetime, self.ctime)).strftime('%Y-%m-%d'),
            )

    class DbLog(base):
        __tablename__ = 'db_dblog'

        id = Column(Integer, primary_key=True)
        uuid = Column(uuid_type, default=get_new_uuid, nullable=False, unique=True)
        time = Column(datetime_type, default=timezone.now, nullable=False)
        loggername = Column(String(255), nullable=False, index=True, doc='What process recorded the message')
        levelname = Column(String(50), nullable=False, index=True, doc='How critical the message is')
        dbnode_id = Column(
            Integer,
            ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED', ondelete='CASCADE'),
            nullable=False,
            index=True,
        )
        message = Column(Text(), default='', nullable=False)
        _metadata = Column('metadata', json_type, default=dict, nullable=False)

        dbnode = relationship('DbNode', backref=backref('dblogs', passive_deletes='all', cascade='merge'))

        __table_args__ = pattern_indexes(
            create_pattern_index('ix_pat_db_dblog_loggername', loggername, 'loggername'),
            create_pattern_index('ix_pat_db_dblog_levelname', levelname, 'levelname'),
        )

        def __str__(self):
            return f'DbLog: {self.levelname} for node {self.dbnode.id}: {self.message}'

    class DbSetting(base):
        __tablename__ = 'db_dbsetting'

        id = Column(Integer, primary_key=True)
        key = Column(String(1024), nullable=False, unique=True)
        val = Column(json_type, default={})
        description = Column(Text, default='', nullable=False)
        time = Column(datetime_type, default=timezone.now, onupdate=timezone.now, nullable=False)

        __table_args__ = pattern_indexes(create_pattern_index('ix_pat_db_dbsetting_key', key, 'key'))

        def __str__(self):
            return f"'{self.key}'={self.val}"

    return ModelClasses(
        authinfo=DbAuthInfo,
        comment=DbComment,
        computer=DbComputer,
        group=DbGroup,
        group_node=DbGroupNode,
        link=DbLink,
        log=DbLog,
        node=DbNode,
        setting=DbSetting,
        user=DbUser,
    )
