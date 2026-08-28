###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Module for all common top level AiiDA entity classes and methods"""

from __future__ import annotations

import abc
import inspect
from collections.abc import Mapping, Sequence
from enum import Enum
from functools import cached_property, lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    NoReturn,
    TypeVar,
)

from plumpy.base.utils import call_with_super_check, super_check
from typing_extensions import Self

from aiida.common import exceptions, log
from aiida.common.exceptions import InvalidOperation
from aiida.common.lang import classproperty, type_check
from aiida.common.warnings import warn_deprecation
from aiida.manage import get_manager

from .columns import EntityColumns
from .fields import BaseField, ColumnField
from .qb_fields import QbField, QbFields

if TYPE_CHECKING:
    from aiida.common.pydantic import AiiDABaseModel
    from aiida.orm.implementation import BackendEntity, StorageBackend
    from aiida.orm.querybuilder import FilterType, OrderByType, QueryBuilder

__all__ = ('Collection', 'Entity', 'EntityTypes')

CollectionType = TypeVar('CollectionType', bound='Collection[Any]')
EntityType = TypeVar('EntityType', bound='Entity[Any,Any]')
BackendEntityType = TypeVar('BackendEntityType', bound='BackendEntity')


class EntityTypes(Enum):
    """Enum for referring to ORM entities in a backend-agnostic manner."""

    AUTHINFO = 'authinfo'
    COMMENT = 'comment'
    COMPUTER = 'computer'
    GROUP = 'group'
    LOG = 'log'
    NODE = 'node'
    USER = 'user'
    LINK = 'link'
    GROUP_NODE = 'group_node'


class Collection(abc.ABC, Generic[EntityType]):
    """Container class that represents the collection of objects of a particular entity type."""

    collection_type: ClassVar[str] = 'entities'

    @staticmethod
    @abc.abstractmethod
    def _entity_base_cls() -> type[EntityType]:
        """The allowed entity class or subclasses thereof."""

    @classmethod
    @lru_cache(maxsize=100)
    def get_cached(cls, entity_class: type[EntityType], backend: StorageBackend) -> Self:
        """Get the cached collection instance for the given entity class and backend.

        :param backend: the backend instance to get the collection for
        """
        from aiida.orm.implementation import StorageBackend

        type_check(backend, StorageBackend)
        return cls(entity_class, backend=backend)

    def __init__(self, entity_class: type[EntityType], backend: StorageBackend | None = None) -> None:
        """Construct a new entity collection.

        :param entity_class: the entity type e.g. User, Computer, etc
        :param backend: the backend instance to get the collection for, or use the default
        """
        from aiida.orm.implementation import StorageBackend

        type_check(backend, StorageBackend, allow_none=True)
        assert issubclass(entity_class, self._entity_base_cls())
        self._backend = backend or get_manager().get_profile_storage()
        self._entity_type = entity_class

    def __call__(self, backend: StorageBackend) -> Self:
        """Get or create a cached collection using a new backend."""
        if backend is self._backend:
            return self
        return self.get_cached(self.entity_type, backend=backend)

    @property
    def entity_type(self) -> type[EntityType]:
        """The entity type for this instance."""
        return self._entity_type

    @property
    def backend(self) -> StorageBackend:
        """Return the backend."""
        return self._backend

    def query(
        self,
        filters: FilterType | None = None,
        order_by: OrderByType | None = None,
        project: list[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        subclassing: bool = True,
    ) -> QueryBuilder:
        """Get a query builder for the objects of this collection.

        :param filters: the keyword value pair filters to match
        :param order_by: a list of (key, direction) pairs specifying the sort order
        :param project: Optional projections.
        :param limit: the maximum number of results to return
        :param offset: number of initial results to be skipped
        :param subclassing: whether to match subclasses of the type as well.
        """
        from . import querybuilder

        filters = filters or {}
        order_by = {self.entity_type: order_by} if order_by else {}

        query = querybuilder.QueryBuilder(backend=self._backend, limit=limit, offset=offset)
        query.append(self.entity_type, project=project, filters=filters, subclassing=subclassing)
        query.order_by([order_by])
        return query

    def get(self, **filters: Any) -> EntityType:
        """Get a single collection entry that matches the filter criteria.

        :param filters: the filters identifying the object to get

        :return: the entry
        """
        res = self.query(filters=filters)
        return res.one()[0]

    def find(
        self,
        filters: FilterType | None = None,
        order_by: OrderByType | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[EntityType]:
        """Find collection entries matching the filter criteria.

        :param filters: the keyword value pair filters to match
        :param order_by: a list of (key, direction) pairs specifying the sort order
        :param limit: the maximum number of results to return
        :param offset: number of initial results to be skipped

        :return: a list of resulting matches
        """
        query = self.query(filters=filters, order_by=order_by, limit=limit, offset=offset)
        return query.all(flat=True)

    def all(self) -> list[EntityType]:
        """Get all entities in this collection.

        :return: A list of all entities
        """
        return self.query().all(flat=True)

    def count(self, filters: FilterType | None = None) -> int:
        """Count entities in this collection according to criteria.

        :param filters: the keyword value pair filters to match

        :return: The number of entities found using the supplied criteria
        """
        return self.query(filters=filters).count()


class EntityBase:
    """The namespace of an entity's sub-managers, reached as ``entity.base``."""

    def __init__(self, entity: Entity) -> None:
        self._entity = entity

    @cached_property
    def columns(self) -> EntityColumns:
        """Return an interface to interact with the fixed columns of this entity."""
        return EntityColumns(self._entity)


class Entity(abc.ABC, Generic[BackendEntityType, CollectionType]):
    """An AiiDA entity"""

    _CLS_COLLECTION: type[CollectionType] = Collection  # type: ignore[assignment]
    _logger = log.AIIDA_LOGGER.getChild('orm.entities')

    identity_field = 'pk'

    #: Fixed-schema declarations introduced by this class. Only ``aiida-core`` declares these: the
    #: columns are owned by the storage schema and its migrations.
    _column_fields: ClassVar[Sequence[BaseField]] = (
        ColumnField('pk', int, 'The primary key of the entity', rest_api_read_only=True, cli_exclude=True),
    )

    #: Open-namespace declarations introduced by this class. This is the protected subclass API for
    #: data plugins; ordinary users should use ``fields`` for query introspection instead.
    _attribute_fields: ClassVar[Sequence[BaseField]] = ()

    #: The complete declaration mapping, collected across the MRO. Internal: a declaration is not
    #: installed under its own name, so ``fields`` is the way to reach one.
    _field_declarations: ClassVar[Mapping[str, BaseField]] = {}

    #: Whether a subclass inherits the declarations this class introduces. Set to ``False`` by a
    #: deprecated class whose stored keys its successors do not have -- the legacy ``Code``, whose
    #: subclasses are the modern code classes and store none of its attributes.
    _inheritable_fields: ClassVar[bool] = True

    #: The query view of the collected declarations, one ``QbField`` per declaration.
    fields: ClassVar[QbFields] = QbFields()

    def __init__(self, backend_entity: BackendEntityType) -> None:
        """:param backend_entity: the backend model supporting this entity"""
        self._backend_entity = backend_entity
        call_with_super_check(self.initialize)

    @cached_property
    def base(self) -> EntityBase:
        """Return the namespace of this entity's sub-managers."""
        return EntityBase(self)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._collect_fields()

    @classmethod
    def _collect_fields(cls) -> None:
        """Collect the declarations of this class and its bases.

        Base classes first, so a subclass narrows what it inherits. The two channels are checked
        against the kind they accept when the class is defined, rather than when a query
        eventually touches the field.

        :raises TypeError: if a declaration is in the wrong channel.
        """
        declarations: dict[str, BaseField] = {}

        for klass in reversed(cls.__mro__):
            if klass is not cls and not vars(klass).get('_inheritable_fields', True):
                continue
            for channel, is_attribute in (('_column_fields', False), ('_attribute_fields', True)):
                for declaration in vars(klass).get(channel, ()):
                    if not isinstance(declaration, BaseField) or declaration.is_attribute is not is_attribute:
                        kind = 'AttributeField' if is_attribute else 'ColumnField'
                        raise TypeError(f'`{klass.__name__}.{channel}` accepts only {kind} declarations')
                    declarations[declaration.name] = declaration

        cls._field_declarations = declarations
        cls.fields = QbFields(cls._build_query_fields(declarations))

    @classmethod
    def _build_query_fields(cls, declarations: Mapping[str, BaseField]) -> dict[str, QbField]:
        """Build the query view of the declarations.

        A hook, so that :class:`~aiida.orm.nodes.node.Node` can wire up its attributes namespace.
        """
        return {name: QbField.from_declaration(declaration) for name, declaration in declarations.items()}

    @classmethod
    def from_model(cls, model: AiiDABaseModel) -> Self:
        """Return an entity built from a validated model of any layer.

        **Which arguments ``__init__`` takes is a fact about this class, not about the layer that
        produced the model**, so it is answered here once rather than in every consuming layer.

        All this does is hand those values to the constructor. It deliberately does no more: there
        is no general recipe for building an entity -- a constructor may take a ``Computer`` and
        store a primary key, or a directory that becomes repository content, or arguments that are
        no field at all. Whatever a class needs beyond "pass the values through" is that class's
        to write, by overriding this or by taking the arguments in ``__init__``.
        """
        accepted = inspect.signature(cls.__init__).parameters
        takes_kwargs = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in accepted.values())
        published = type(model).model_fields
        return cls(
            **{
                name: cls._value_from_model(name, getattr(model, name))
                for name in published
                if takes_kwargs or name in accepted
            }
        )

    @classmethod
    def _value_from_model(cls, name: str, value: Any) -> Any:
        """Return a validated model value in the form the constructor takes.

        A relationship crosses the wire as a primary key, so it is resolved back into the entity
        the constructor expects. The lookup is eager: a pk that names nothing fails here, with the
        entity type in the message, rather than as a foreign key error at commit.

        :raises NotExistent: if no entity of the declared type has that primary key.
        """
        declaration = cls._field_declarations.get(name)
        if declaration is None or not declaration.is_entity_reference or not isinstance(value, int):
            return value

        entity_class = declaration.entity_class
        try:
            return entity_class.collection.get(id=value)
        except exceptions.NotExistent as exception:
            raise exceptions.NotExistent(f'no `{entity_class.__name__}` found with pk={value}') from exception

    @classproperty
    def objects(cls) -> CollectionType:  # noqa: N805
        """Get a collection for objects of this type, with the default backend.

        .. deprecated:: This will be removed in v3, use ``collection`` instead.

        :return: an object that can be used to access entities of this type
        """
        warn_deprecation('`objects` property is deprecated, use `collection` instead.', version=3, stacklevel=4)
        return cls.collection

    @classproperty
    def collection(cls) -> CollectionType:  # noqa: N805
        """Get a collection for objects of this type, with the default backend.

        :return: an object that can be used to access entities of this type
        """
        return cls._CLS_COLLECTION.get_cached(cls, get_manager().get_profile_storage())

    @classmethod
    def get_collection(cls, backend: StorageBackend) -> CollectionType:
        """Get a collection for objects of this type for a given backend.

        .. note:: Use the ``collection`` class property instead if the currently loaded backend or backend of the
            default profile should be used.

        :param backend: The backend of the collection to use.
        :return: A collection object that can be used to access entities of this type.
        """
        return cls._CLS_COLLECTION.get_cached(cls, backend)

    @classmethod
    def get(cls, **kwargs: Any) -> Self:
        """Get an entity of the collection matching the given filters.

        .. deprecated: Will be removed in v3, use `Entity.collection.get` instead.

        """
        warn_deprecation(
            f'`{cls.__name__}.get` method is deprecated, use `{cls.__name__}.collection.get` instead.',
            version=3,
            stacklevel=2,
        )
        return cls.collection.get(**kwargs)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False

        if hasattr(self, 'uuid'):
            return self.uuid == other.uuid  # type: ignore[attr-defined]

        return super().__eq__(other)

    def __getstate__(self) -> NoReturn:
        """Prevent an ORM entity instance from being pickled."""
        raise InvalidOperation('pickling of AiiDA ORM instances is not supported.')

    @super_check
    def initialize(self) -> None:
        """Initialize instance attributes.

        This will be called after the constructor is called or an entity is created from an existing backend entity.
        """

    @property
    def logger(self) -> log.AiidaLoggerType:
        """Return the internal logger."""
        try:
            return self._logger
        except AttributeError:
            raise exceptions.InternalError('No self._logger configured for {}!')

    @property
    def id(self) -> int | None:
        """Return the id for this entity.

        This identifier is guaranteed to be unique amongst entities of the same type for a single backend instance.

        .. deprecated: Will be removed in v3, use `pk` instead.

        :return: the entity's id
        """
        warn_deprecation('`id` property is deprecated, use `pk` instead.', version=3, stacklevel=2)
        return self._backend_entity.id

    @property
    def pk(self) -> int | None:
        """Return the primary key for this entity.

        This identifier is guaranteed to be unique amongst entities of the same type for a single backend instance.

        :return: the entity's principal key
        """
        return self._backend_entity.id

    def store(self) -> Self:
        """Store the entity."""
        self._backend_entity.store()
        return self

    @property
    def is_stored(self) -> bool:
        """Return whether the entity is stored."""
        return self._backend_entity.is_stored

    @property
    def backend(self) -> StorageBackend:
        """Get the backend for this entity"""
        return self._backend_entity.backend

    @property
    def backend_entity(self) -> BackendEntityType:
        """Get the implementing class for this object"""
        return self._backend_entity


def from_backend_entity(cls: type[EntityType], backend_entity: BackendEntity) -> EntityType:
    """Construct an entity from a backend entity instance

    :param backend_entity: the backend entity

    :return: an AiiDA entity instance
    """
    from .implementation.entities import BackendEntity

    type_check(backend_entity, BackendEntity)
    entity = cls.__new__(cls)
    entity._backend_entity = backend_entity
    call_with_super_check(entity.initialize)
    return entity
