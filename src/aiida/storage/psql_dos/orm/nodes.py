###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""SqlAlchemy implementation of the `BackendNode` and `BackendNodeCollection` classes."""

import typing as t
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.exc import NoResultFound

from aiida.common import exceptions
from aiida.common.lang import type_check
from aiida.orm.implementation import BackendNode, BackendNodeCollection
from aiida.orm.implementation.utils import clean_value, validate_attribute_extra_key
from aiida.storage.psql_dos.models import node as models
from aiida.storage.psql_dos.orm import entities
from aiida.storage.psql_dos.orm import utils as sqla_utils
from aiida.storage.psql_dos.orm.computers import SqlaComputer
from aiida.storage.psql_dos.orm.extras_mixin import ExtrasMixin
from aiida.storage.psql_dos.orm.users import SqlaUser


class SqlaNode(entities.SqlaModelEntity[models.DbNode], ExtrasMixin, BackendNode):
    """SQLA Node backend entity"""

    MODEL_CLASS = models.DbNode
    USER_CLASS = SqlaUser
    COMPUTER_CLASS = SqlaComputer
    LINK_CLASS = models.DbLink

    def __init__(
        self,
        backend,
        node_type,
        user,
        computer=None,
        process_type=None,
        label='',
        description='',
        ctime=None,
        mtime=None,
    ):
        """Construct a new `BackendNode` instance wrapping a new `DbNode` instance.

        :param backend: the backend
        :param node_type: the node type string
        :param user: associated `BackendUser`
        :param computer: associated `BackendComputer`
        :param label: string label
        :param description: string description
        :param ctime: The creation time as datetime object
        :param mtime: The modification time as datetime object
        """
        super().__init__(backend)

        arguments = {
            'node_type': node_type,
            'process_type': process_type,
            'user': user.bare_model,
            'label': label,
            'description': description,
        }

        type_check(user, self.USER_CLASS)

        if computer:
            type_check(computer, self.COMPUTER_CLASS, f'computer is of type {type(computer)}')
            arguments['dbcomputer'] = computer.bare_model

        if ctime:
            type_check(ctime, datetime, f'the given ctime is of type {type(ctime)}')
            arguments['ctime'] = ctime

        if mtime:
            type_check(mtime, datetime, f'the given mtime is of type {type(mtime)}')
            arguments['mtime'] = mtime

        self._model = sqla_utils.ModelWrapper(self.MODEL_CLASS(**arguments), backend)

    def clone(self):
        """Return an unstored clone of ourselves.

        :return: an unstored `BackendNode` with the exact same attributes and extras as self
        """
        arguments = {
            'node_type': self.model.node_type,
            'process_type': self.model.process_type,
            'user': self.model.user,
            'dbcomputer': self.model.dbcomputer,
            'label': self.model.label,
            'description': self.model.description,
            'attributes': self.model.attributes,
            'extras': self.model.extras,
        }

        clone = self.__class__.__new__(self.__class__)
        clone.__init__(self.backend, self.node_type, self.user)
        clone._model = sqla_utils.ModelWrapper(self.MODEL_CLASS(**arguments), self.backend)
        return clone

    @contextmanager
    def store_scope(self) -> Iterator[None]:
        """Restore the original unstored model after an enclosing transaction fails."""
        if self.is_stored:
            yield
            return

        model = self.bare_model
        values = {column.key: deepcopy(getattr(model, column.key)) for column in model.__table__.columns}
        snapshot = self.MODEL_CLASS(**values, user=model.user, dbcomputer=model.dbcomputer)
        try:
            yield
        except BaseException:
            # SQLAlchemy rollback makes inserted models transient but leaves their
            # generated primary keys behind. Keep the entity (and its ORM adapters)
            # while replacing that model with its pre-store state.
            self._model = sqla_utils.ModelWrapper(snapshot, self.backend)
            raise

    @property
    def ctime(self):
        return self.model.ctime

    @property
    def mtime(self):
        return self.model.mtime

    @property
    def process_state_change_timestamp(self) -> datetime | None:
        return self.model.process_state_change_timestamp

    @process_state_change_timestamp.setter
    def process_state_change_timestamp(self, value: datetime | None) -> None:
        self.model.process_state_change_timestamp = value

    @property
    def uuid(self):
        return str(self.model.uuid)

    @property
    def node_type(self):
        return self.model.node_type

    @property
    def process_type(self):
        return self.model.process_type

    @process_type.setter
    def process_type(self, value):
        self.model.process_type = value

    @property
    def label(self):
        return self.model.label

    @label.setter
    def label(self, value):
        self.model.label = value

    @property
    def description(self):
        return self.model.description

    @description.setter
    def description(self, value):
        self.model.description = value

    @property
    def repository_metadata(self):
        return self.model.repository_metadata or {}

    @repository_metadata.setter
    def repository_metadata(self, value):
        self.model.repository_metadata = value

    @property
    def computer(self):
        try:
            return self.backend.computers.ENTITY_CLASS.from_dbmodel(self.model.dbcomputer, self.backend)
        except TypeError:
            return None

    @computer.setter
    def computer(self, computer):
        type_check(computer, self.COMPUTER_CLASS, allow_none=True)

        if computer is not None:
            computer = computer.bare_model

        self.model.dbcomputer = computer

    @property
    def user(self):
        return self.backend.users.ENTITY_CLASS.from_dbmodel(self.model.user, self.backend)

    @user.setter
    def user(self, user):
        type_check(user, self.USER_CLASS)
        self.model.user = user.bare_model

    def add_incoming(self, source, link_type, link_label):
        session = self.backend.get_session()

        type_check(source, self.__class__)

        if not self.is_stored:
            raise exceptions.ModificationNotAllowed('node has to be stored when adding an incoming link')

        if not source.is_stored:
            raise exceptions.ModificationNotAllowed('source node has to be stored when adding a link from it')

        self._add_link(source, link_type, link_label)
        if not session.in_nested_transaction():
            self.backend._commit()

    def _add_link(self, source, link_type, link_label):
        """Add a single link"""
        session = self.backend.get_session()

        try:
            with session.begin_nested():
                link = self.LINK_CLASS(input_id=source.pk, output_id=self.pk, label=link_label, type=link_type.value)
                session.add(link)
        except SQLAlchemyError as exception:
            msg = f'failed to create the link: {exception}'
            raise exceptions.UniquenessError(msg) from exception

    def clean_values(self):
        self.model.attributes = clean_value(self.model.attributes)
        self.model.extras = clean_value(self.model.extras)

    def store(self, links=None, clean=True):
        session = self.backend.get_session()

        if clean:
            self.clean_values()

        session.add(self.model)

        if links:
            for link_triple in links:
                self._add_link(*link_triple)

        if not session.in_nested_transaction():
            try:
                self.backend._commit()
            except SQLAlchemyError:
                session.rollback()
                raise
        else:
            # Make sure the new addition is flushed to, e.g., populate automatic primary keys.
            session.flush()

        return self

    @property
    def attributes(self):
        return self.model.attributes

    def get_attribute(self, key: str) -> t.Any:
        try:
            return self.model.attributes[key]
        except KeyError as exception:
            msg = f'attribute `{exception}` does not exist'
            raise AttributeError(msg) from exception

    def set_attribute(self, key: str, value: t.Any) -> None:
        validate_attribute_extra_key(key)

        if self.is_stored:
            value = clean_value(value)

        attributes = self.model.attributes
        attributes[key] = value
        self.model.attributes = attributes

    def set_attribute_many(self, attributes: dict[str, t.Any]) -> None:
        for key in attributes:
            validate_attribute_extra_key(key)

        if self.is_stored:
            attributes = {key: clean_value(value) for key, value in attributes.items()}

        values = self.model.attributes
        values.update(attributes)
        self.model.attributes = values

    def reset_attributes(self, attributes: dict[str, t.Any]) -> None:
        for key in attributes:
            validate_attribute_extra_key(key)

        if self.is_stored:
            attributes = clean_value(attributes)

        self.model.attributes = attributes

    def delete_attribute(self, key: str) -> None:
        attributes = self.model.attributes
        try:
            attributes.pop(key)
        except KeyError as exception:
            msg = f'attribute `{exception}` does not exist'
            raise AttributeError(msg) from exception
        self.model.attributes = attributes

    def delete_attribute_many(self, keys: Iterable[str]) -> None:
        non_existing_keys = [key for key in keys if key not in self.model.attributes]

        if non_existing_keys:
            msg = f'attributes `{", ".join(non_existing_keys)}` do not exist'
            raise AttributeError(msg)

        values = self.model.attributes
        for key in keys:
            values.pop(key)
        self.model.attributes = values

    def clear_attributes(self):
        self.model.attributes = {}

    def attributes_items(self) -> Iterable[tuple[str, t.Any]]:
        yield from self.model.attributes.items()

    def attributes_keys(self) -> Iterable[str]:
        yield from self.model.attributes.keys()


class SqlaNodeCollection(BackendNodeCollection):
    """The collection of Node entries."""

    ENTITY_CLASS: type[SqlaNode] = SqlaNode

    def get(self, pk):
        session = self.backend.get_session()

        try:
            return self.ENTITY_CLASS.from_dbmodel(
                session.query(self.ENTITY_CLASS.MODEL_CLASS).filter_by(id=pk).one(), self.backend
            )
        except NoResultFound:
            msg = f"Node with pk '{pk}' not found"
            raise exceptions.NotExistent(msg) from NoResultFound

    def delete(self, pk):
        session = self.backend.get_session()

        try:
            row = session.query(self.ENTITY_CLASS.MODEL_CLASS).filter_by(id=pk).one()
            session.delete(row)
            self.backend._commit()
        except NoResultFound:
            msg = f"Node with pk '{pk}' not found"
            raise exceptions.NotExistent(msg) from NoResultFound
