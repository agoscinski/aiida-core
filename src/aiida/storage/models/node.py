"""Unified Node and Link models for AiiDA - works with all database backends.

This is the most complex and critical model in AiiDA, representing the core
of the provenance graph.
"""

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Index
from sqlalchemy.orm import relationship, backref

from .base import Base
from .types import GUID, TZDateTime, JSONType
from aiida.common import timezone
from aiida.common.utils import get_new_uuid


class DbNode(Base):
    """Database model to store data for Node entities.

    Nodes are the fundamental entities in AiiDA's provenance graph. They represent:
    - Data nodes: Inputs and outputs of calculations
    - Process nodes: Calculations, workflows, and functions
    - Code nodes: Executable codes used in calculations

    Each node can be categorized by its node_type (e.g., 'data.core.int.Int.')
    and optionally by process_type for process nodes.

    This unified model works with both PostgreSQL and SQLite backends,
    using database-agnostic types (GUID, TZDateTime, JSONType) that automatically
    adapt to the dialect.

    Replaces:
    - aiida.storage.psql_dos.models.node.DbNode
    - aiida.storage.sqlite_zip.models.DbNode
    """

    __tablename__ = 'db_dbnode'

    # Primary key and identifiers
    id = Column(Integer, primary_key=True)
    uuid = Column(GUID, default=get_new_uuid, nullable=False, unique=True)

    # Node classification
    node_type = Column(String(255), default='', nullable=False, index=True)
    process_type = Column(String(255), index=True)

    # Metadata
    label = Column(String(255), nullable=False, default='', index=True)
    description = Column(Text, nullable=False, default='')

    # Timestamps (automatically timezone-aware on both PostgreSQL and SQLite)
    ctime = Column(TZDateTime, default=timezone.now, nullable=False, index=True)
    mtime = Column(
        TZDateTime,
        default=timezone.now,
        onupdate=timezone.now,
        nullable=False,
        index=True
    )

    # JSON columns (JSONB on PostgreSQL, JSON on SQLite)
    # - attributes: Node-specific data that cannot be changed after storage
    # - extras: User-defined metadata that can be modified after storage
    # - repository_metadata: Metadata about files stored in the repository
    attributes = Column(JSONType, default=dict, nullable=False)
    extras = Column(JSONType, default=dict, nullable=False)
    repository_metadata = Column(JSONType, nullable=False, default=dict)

    # Foreign keys
    dbcomputer_id = Column(
        Integer,
        ForeignKey('db_dbcomputer.id', deferrable=True, initially='DEFERRED', ondelete='RESTRICT'),
        nullable=True,
        index=True
    )
    user_id = Column(
        Integer,
        ForeignKey('db_dbuser.id', deferrable=True, initially='DEFERRED', ondelete='RESTRICT'),
        nullable=False,
        index=True
    )

    # Relationships to other tables
    dbcomputer = relationship(
        'DbComputer',
        backref=backref('dbnodes', passive_deletes='all', cascade='merge')
    )
    user = relationship(
        'DbUser',
        backref=backref('dbnodes', passive_deletes='all', cascade='merge')
    )

    # Graph relationships via DbLink table
    # These create the provenance graph structure
    outputs_q = relationship(
        'DbNode',
        secondary='db_dblink',
        primaryjoin='DbNode.id == DbLink.input_id',
        secondaryjoin='DbNode.id == DbLink.output_id',
        backref=backref('inputs_q', passive_deletes=True, lazy='dynamic'),
        lazy='dynamic',
        passive_deletes=True
    )

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations

    @property
    def outputs(self):
        """Get all output nodes (eager loading).

        Returns a list of nodes that are outputs of this node.
        """
        return self.outputs_q.all()

    @property
    def inputs(self):
        """Get all input nodes (eager loading).

        Returns a list of nodes that are inputs to this node.
        """
        return self.inputs_q.all()

    @property
    def pk(self):
        """Alias for id (primary key)."""
        return self.id

    def get_simple_name(self, invalid_result=None):
        """Return the last part of the node type string.

        Extracts the simple class name from the full node type string.
        For example: 'data.core.int.Int.' -> 'Int'

        :param invalid_result: Value to return if node type is invalid
        :return: Simple name string
        """
        thistype = self.node_type
        # Fix for base class
        if thistype == '':
            thistype = 'node.Node.'
        if not thistype.endswith('.'):
            return invalid_result
        thistype = thistype[:-1]  # Strip final dot
        return thistype.rpartition('.')[2]

    def __str__(self):
        """String representation."""
        simplename = self.get_simple_name(invalid_result='Unknown')
        if self.label:
            return f'{simplename} node [{self.pk}]: {self.label}'
        return f'{simplename} node [{self.pk}]'

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbNode(id={self.id}, uuid={self.uuid}, "
            f"node_type='{self.node_type}', label='{self.label}')>"
        )


class DbLink(Base):
    """Database model to store links between Node entities.

    Links represent the edges in AiiDA's provenance graph, connecting:
    - Data inputs to calculations
    - Calculation outputs to data
    - Workflow calls to sub-processes
    - Return values from functions

    Each link has:
    - type: The kind of link (input_calc, create, return, call_calc, call_work)
    - label: A descriptive label (e.g., 'structure', 'parameters', 'result')

    This unified model works with both PostgreSQL and SQLite backends.

    Replaces:
    - aiida.storage.psql_dos.models.node.DbLink
    - aiida.storage.sqlite_zip.models.DbLink
    """

    __tablename__ = 'db_dblink'

    id = Column(Integer, primary_key=True)

    # The two nodes being connected
    input_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True
    )
    output_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', ondelete='CASCADE', deferrable=True, initially='DEFERRED'),
        nullable=False,
        index=True
    )

    # Relationships to nodes
    input = relationship(
        'DbNode',
        primaryjoin='DbLink.input_id == DbNode.id',
        overlaps='inputs_q,outputs_q'
    )
    output = relationship(
        'DbNode',
        primaryjoin='DbLink.output_id == DbNode.id',
        overlaps='inputs_q,outputs_q'
    )

    # Link metadata
    label = Column(String(255), nullable=False, index=True)
    type = Column(String(255), nullable=False, index=True)

    # Note: PostgreSQL-specific pattern matching indexes (ix_pat_*) are created by migrations

    def __str__(self):
        """String representation."""
        return (
            f'{self.input.get_simple_name(invalid_result="Unknown node")} '
            f'({self.input.pk}) --> '
            f'{self.output.get_simple_name(invalid_result="Unknown node")} '
            f'({self.output.pk})'
        )

    def __repr__(self):
        """Developer-friendly representation."""
        return (
            f"<DbLink(id={self.id}, input_id={self.input_id}, "
            f"output_id={self.output_id}, type='{self.type}', label='{self.label}')>"
        )
