###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Declarations of the fields an ORM entity persists.

A declaration is a **value**: it states facts about what the backend stores -- the type, the
documentation, the default, and the key it is held under -- and it reads that value back. It has
no opinion about querying, and no opinion about what any layer may do with the value. Because it
is a plain value, it compares and hashes like one, which matters: two declarations from different
classes must not be interchangeable as dictionary keys.

There are two kinds, because the backend holds the two kinds of value differently:

* :class:`AttributeField` -- kept in the entity's ``attributes``, whose keys are open. This is what
  a data plugin declares, and the only kind it can declare, since the attributes are the only part
  of a row a plugin may add to. It is therefore the **public** kind, and is kept to what a plugin
  author must state.
* :class:`ColumnField` -- a column of the table backing the entity. The set of columns is fixed by
  the storage schema and its migrations, so only ``aiida-core`` declares these. A column also
  carries what each consuming layer does with it, under a ``cli_`` or ``rest_api_`` prefix.

Declarations are collected onto a class through the two channels
:attr:`~aiida.orm.entities.Entity._column_fields` and ``_attribute_fields``; see
:mod:`aiida.orm.entities`. A declaration is deliberately *not* installed under its own name, which
leaves that name free for a property where the stored value and the Python value differ.

The query-side view of a declaration is :class:`~aiida.orm.qb_fields.QbField`, reached through
``Entity.fields``.
"""

from __future__ import annotations

import abc
import datetime
import types
import typing as t
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence

from aiida.common.fields import MISSING, CliField, RestApiField, _Missing
from aiida.common.lang import isidentifier

# Re-exported for backwards compatibility: the query classes used to live in this module, so
# `from aiida.orm.fields import QbField` keeps working.
from aiida.orm.qb_fields import QbAttributesField as QbAttributesField
from aiida.orm.qb_fields import QbField as QbField
from aiida.orm.qb_fields import QbFieldFilters as QbFieldFilters
from aiida.orm.qb_fields import QbFields as QbFields

#: Only the kind a plugin can declare is exported. ``BaseField`` and ``ColumnField`` stay
#: reachable here for the entity classes that need them, but exporting them from ``aiida.orm``
#: would promise a stability guarantee for two classes a plugin has no business declaring. The
#: ``Qb`` names are re-exported above for backwards compatibility, and are listed in the
#: ``__all__`` of :mod:`aiida.orm.qb_fields`, which is where they now live.
__all__ = ('AttributeField',)

#: What a backend can hold. Containers are the covariant ``Mapping``/``Sequence`` rather than
#: ``dict``/``list``, or invariance would reject ``list[str]``.
Storable: t.TypeAlias = 'Mapping[t.Any, t.Any] | Sequence[t.Any] | str | bool | int | float | None | datetime.datetime'

#: What ``default`` may be: a storable value that is *immutable*, so stating it once on the
#: declaration is safe. Anything mutable belongs in ``default_factory`` instead, and the two
#: annotations together make that choice a type error rather than a convention.
ImmutableStorable: t.TypeAlias = (
    'str | bool | int | float | None | datetime.datetime | tuple[t.Any, ...] | frozenset[t.Any]'
)

#: What ``default_factory`` must produce: a storable value that is *mutable*, and so unsafe to
#: share between entities.
MutableStorable: t.TypeAlias = 'MutableMapping[t.Any, t.Any] | MutableSequence[t.Any]'

#: The annotation for ``dtype``: a type a backend can store, or a union of them. ``types.UnionType``
#: is there because ``str | None`` is a union object rather than a ``type``.
StorableType: t.TypeAlias = 'type[Storable] | types.UnionType | t.Any'


#: What a layer does with a field it has said nothing about: publish it under its own name.
_CLI_AS_IS: t.Final = CliField()
_REST_AS_IS: t.Final = RestApiField()


def _names_an_entity(dtype: t.Any) -> bool:
    """Return whether a declared type refers to another ORM entity, optionally or not.

    Answered without resolving an entry point name to its class: the query fields are built while
    the class is being defined, and loading an entry point that early closes an import cycle.
    """
    from aiida.orm.entities import Entity

    if isinstance(dtype, str):
        return True
    if t.get_origin(dtype) in (t.Union, types.UnionType):
        return any(_names_an_entity(arg) for arg in t.get_args(dtype))
    return isinstance(dtype, type) and issubclass(dtype, Entity)


def _resolve_entity_class(dtype: t.Any) -> type[t.Any] | None:
    """Return the ORM entity class a declared type refers to, or ``None``.

    A declaration may name the class directly, or -- where importing it would close a cycle, as
    ``Comment`` referring to ``Node`` does -- by the entry point name of one. Resolving the latter
    is deferred to the first write, by which time the entry points are loadable.
    """
    if isinstance(dtype, str):
        from aiida.plugins.factories import BaseFactory

        return t.cast('type[t.Any]', BaseFactory('aiida.orm', dtype))

    if t.get_origin(dtype) in (t.Union, types.UnionType):
        for arg in t.get_args(dtype):
            resolved = _resolve_entity_class(arg)
            if resolved is not None:
                return resolved
        return None

    return dtype if _names_an_entity(dtype) else None


class BaseField(abc.ABC):
    """Declaration of a persisted field.

    Declare an :class:`AttributeField` or a :class:`ColumnField`; this class cannot be
    instantiated, so there is no neutral default to fall into.
    """

    __slots__ = (
        '_alias',
        '_cli_exclude',
        '_cli_name',
        '_default',
        '_default_factory',
        '_doc',
        '_dtype',
        '_entity_class',
        '_examples',
        '_name',
        '_cli_option_cls',
        '_cli_priority',
        '_cli_prompt',
        '_cli_short_name',
        '_layer_options',
        '_rest_api_exclude',
        '_rest_api_name',
        '_rest_api_read_only',
        '_validator',
    )

    def __init__(
        self,
        name: str,
        dtype: StorableType,
        doc: str = '',
        *,
        default: ImmutableStorable | _Missing = MISSING,
        default_factory: t.Callable[[], MutableStorable] | None = None,
        alias: str | None = None,
        examples: Sequence[t.Any] | None = None,
        validator: t.Callable[[t.Any], t.Any] | None = None,
        cli_name: str | None = None,
        cli_exclude: bool = False,
        cli_prompt: str | None = None,
        cli_short_name: str | None = None,
        cli_priority: int = 0,
        cli_option_cls: t.Any | None = None,
        rest_api_name: str | None = None,
        rest_api_exclude: bool = False,
        rest_api_read_only: bool = False,
        **layer_options: t.Any,
    ) -> None:
        """Declare a persisted field.

        :param name: The name of the field on the ORM entity.
        :param dtype: The type of the stored value.
        :param doc: Description of the field.
        :param default: The value read back when the backend holds none, where it is immutable.
            Left at ``MISSING``, an absent value raises instead, and the field is required in any
            model a layer builds from this declaration.
        :param default_factory: The callable producing the default, for a default that is mutable
            and so unsafe to share between entities.
        :param alias: The key the storage backend holds the value under, where it differs from the
            field name. Only an attribute may be aliased, and only because the attribute keys are
            not restricted to Python identifiers -- ``JsonableData`` stores under ``@module``.
        :param examples: Example values, carried through to the schema a layer generates.
        :param validator: A callable the stored value must pass, raising if it does not.
        :param cli_name: The name the CLI publishes this field under.
        :param cli_exclude: Whether the CLI leaves this field out entirely.
        :param cli_prompt: The text an interactive command prompts for this field with.
        :param cli_short_name: The short option name, such as ``-L``.
        :param cli_priority: Orders the CLI options; larger comes first.
        :param cli_option_cls: The ``click.Option`` subclass to build the option from.
        :param rest_api_name: The name the REST API publishes this field under.
        :param rest_api_exclude: Whether the REST API leaves this field out entirely.
        :param rest_api_read_only: Whether a client may not write this field; AiiDA sets it.
        :param layer_options: Further per-layer options, under a ``cli_`` or ``rest_api_`` prefix:
            ``dtype``, ``serialize`` and ``deserialize``, for a layer that publishes the value in
            an encoding of its own.
        """
        if not isidentifier(name):
            raise ValueError(f'`{name}` is not a valid python identifier')
        if default is not MISSING and default_factory is not None:
            raise ValueError('cannot specify both `default` and `default_factory`')
        self._name = name
        self._dtype = dtype
        self._doc = doc
        self._default = default
        self._default_factory = default_factory
        self._alias = alias if alias is not None else name
        self._examples = list(examples) if examples is not None else None
        self._validator = validator
        self._cli_name = cli_name
        self._cli_exclude = cli_exclude
        self._rest_api_name = rest_api_name
        self._rest_api_exclude = rest_api_exclude
        self._rest_api_read_only = rest_api_read_only
        self._cli_prompt = cli_prompt
        self._cli_short_name = cli_short_name
        self._cli_priority = cli_priority
        self._cli_option_cls = cli_option_cls
        unknown = [key for key in layer_options if not key.startswith(('cli_', 'rest_api_'))]
        if unknown:
            raise TypeError(f'unexpected keyword argument(s): {", ".join(sorted(unknown))}')
        self._layer_options = layer_options
        # Resolved on first use: an entry-point string cannot be resolved at import time, and
        # most declarations never refer to an entity at all.
        self._entity_class: type[t.Any] | None | _Missing = MISSING

    # -- a value ----------------------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        """Ordinary equality. The query-building ``==`` lives on ``QbField``, not here."""
        if not isinstance(other, BaseField):
            return NotImplemented
        return (type(self), self._name, self._alias, self._dtype) == (
            type(other),
            other._name,
            other._alias,
            other._dtype,
        )

    def __hash__(self) -> int:
        return hash((type(self), self._name, self._alias, self._dtype))

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.backend_key!r}, dtype={self._dtype!r})'

    # -- the facts --------------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Return the name of the field on the ORM entity.

        Stated on the declaration rather than by the key of the mapping that holds it: one of the
        two would otherwise have to be kept in step with the other, and nothing would notice if
        they drifted.
        """
        return self._name

    @property
    def dtype(self) -> t.Any:
        """Return the declared type of the stored value.

        Where the value is another ORM entity, this is the entity class -- or the entry point name
        of one, for a reference that cannot be imported without a cycle. What the *backend* stores
        in that case is the primary key; see :attr:`wire_dtype`.
        """
        return self._dtype

    @property
    def entity_class(self) -> type[t.Any] | None:
        """Return the ORM entity this field refers to, or ``None`` if it holds a plain value.

        A relationship is stored as a foreign key, so the fact the backend holds is a primary key
        and the entity instance an accessor hands back is the ORM's own convenience. Naming the
        class here is what lets that conversion happen in both directions without a callable per
        field: reading takes the ``pk`` off the instance, and constructing resolves a ``pk`` back
        into an instance.
        """
        if self._entity_class is MISSING:
            self._entity_class = _resolve_entity_class(self._dtype)
        return t.cast('type[t.Any] | None', self._entity_class)

    @property
    def is_entity_reference(self) -> bool:
        """Return whether this field refers to another ORM entity, and so is stored as a ``pk``."""
        return _names_an_entity(self._dtype)

    @property
    def wire_dtype(self) -> t.Any:
        """Return the type of the value as it is stored and published: a ``pk`` for a relationship.

        An optional relationship stays optional: a node need not have a computer, and the primary
        key standing in for one has to be able to say so.
        """
        if not self.is_entity_reference:
            return self._dtype
        if t.get_origin(self._dtype) in (t.Union, types.UnionType) and type(None) in t.get_args(self._dtype):
            return int | None
        return int | None if self._default is None else int

    @property
    def doc(self) -> str:
        """Return the field description."""
        return self._doc

    @property
    def examples(self) -> list[t.Any] | None:
        """Return example values, for the schema a consuming layer generates."""
        return self._examples

    @property
    def storage_key(self) -> str:
        """Return the unqualified key the storage backend holds the value under."""
        return self._alias

    @property
    def validator(self) -> t.Callable[[t.Any], t.Any] | None:
        """Return the check a value of this field must pass, if it has one.

        Optional, and the one piece of *logic* a declaration carries. It earns the exception by
        having no other home: a REST ``PUT`` replaces a stored value without going through
        ``__init__``, so a check written there would never run, and writing it once per layer
        would mean the same rule stated twice and able to drift.

        It raises rather than returning a verdict, and it runs wherever a value enters: every
        layer model wires it in, and so do ``base.attributes.set`` and ``base.columns.set``.
        """
        return self._validator

    @property
    def cli(self) -> CliField:
        """Return how the CLI publishes this field.

        Assembled on access rather than in ``__init__``. A declaration holds the options as the
        plain values they were given, and only the layer that is building a model turns them into
        a view -- so importing the ORM does not construct one of these per field per layer for
        something most callers never look at.
        """
        return CliField(
            name=self._cli_name,
            exclude=self._cli_exclude,
            prompt=self._cli_prompt,
            short_name=self._cli_short_name,
            priority=self._cli_priority,
            option_cls=self._cli_option_cls,
            **self._options_for('cli_'),
        )

    @property
    def rest_api(self) -> RestApiField:
        """Return how the REST API publishes this field. Assembled on access, as ``cli`` is."""
        return RestApiField(
            name=self._rest_api_name,
            exclude=self._rest_api_exclude,
            read_only=self._rest_api_read_only,
            **self._options_for('rest_api_'),
        )

    def _options_for(self, prefix: str) -> dict[str, t.Any]:
        """Return the encoding options stated for one layer, with the prefix stripped."""
        return {key[len(prefix) :]: value for key, value in self._layer_options.items() if key.startswith(prefix)}

    @property
    def default(self) -> ImmutableStorable | _Missing:
        """Return the value read back when the backend holds none, where it is immutable."""
        return self._default

    @property
    def default_factory(self) -> t.Callable[[], MutableStorable] | None:
        """Return the factory a mutable default is built by.

        Paired with ``default`` rather than folded into it: the declaration itself would otherwise
        hold one list forever and hand every reader an alias into it, and a mutable default in a
        class body reads as a bug whether or not it is one.
        """
        return self._default_factory

    @property
    @abc.abstractmethod
    def is_attribute(self) -> bool:
        """Return whether the backend holds the value in the open attributes namespace."""

    @property
    @abc.abstractmethod
    def backend_key(self) -> str:
        """Return the fully qualified key the backend holds the value under."""

    # -- read access ------------------------------------------------------------------------

    def read(self, instance: t.Any) -> t.Any:
        """Return the value the backend holds for this field on ``instance``.

        Declarations are values held in the internal declaration mapping, never under their own
        names, so ``getattr`` does not reach them and every reader goes through here.

        A relationship reads back as its primary key: that is the value the backend holds, and the
        entity instance the accessor returns is a dereference of it.
        """
        return self._as_stored(self.read_raw(instance))

    @abc.abstractmethod
    def read_raw(self, instance: t.Any) -> t.Any:
        """Return the value this field's accessor hands back, before it is put in stored form.

        A relationship hands back the entity here; :meth:`read` turns that into its ``pk``. A layer
        that publishes the entity in some other encoding -- the CLI names a computer by its label
        -- reads through this instead.
        """

    def _as_stored(self, value: t.Any) -> t.Any:
        """Return the stored form of a value read through an accessor."""
        if self.is_entity_reference and value is not None and not isinstance(value, int):
            return value.pk
        return value


class AttributeField(BaseField):
    """A field the backend holds in the entity's open ``attributes``.

    This is what a data plugin declares, and the only kind it can: the attributes are the only part
    of the row a plugin may add to. It is therefore **the user-facing kind**, and it is kept to
    what a plugin author must state -- the type, and what the value means.

    The attributes namespace is open, so a declared key may legitimately be absent from a node
    stored before the declaration existed. A declaration may therefore state a ``default`` for what
    an absent value reads as; without one, reading an absent attribute raises.
    """

    __slots__ = ()

    @property
    def is_attribute(self) -> bool:
        return True

    @property
    def backend_key(self) -> str:
        return f'attributes.{self._alias}'

    def read_raw(self, instance: t.Any) -> t.Any:
        """Return the stored value, falling back to the declared default where there is one.

        :raises AttributeError: if the key is not set and the declaration states no default.
        """
        if self._default_factory is not None:
            return instance.base.attributes.get(self._alias, self._default_factory())
        if self._default is not MISSING:
            return instance.base.attributes.get(self._alias, self._default)
        return instance.base.attributes.get(self._alias)


class ColumnField(BaseField):
    """A field the backend holds in a column of the table backing the entity.

    The columns are fixed by the storage schema and its migrations, so these belong to the core
    entity classes; a plugin has no column to declare.

    Unlike an attribute, a column has a schema behind it, so its default is a *fact about what the
    backend stores* rather than a decision about what to write -- which is why the two default
    options live here and not on the base.
    """

    __slots__ = ()

    def __init__(self, name: str, dtype: StorableType, doc: str = '', **kwargs: t.Any) -> None:
        """Declare a column of the table backing the entity.

        :raises ValueError: if a backend key differing from the field name is declared. A column is
            named by the schema, so there is nothing for an alias to resolve.
        """
        if kwargs.get('alias') not in (None, name):
            raise ValueError(f'a column field may not be aliased, but `{name}` aliases `{kwargs["alias"]}`')
        super().__init__(name, dtype, doc, **kwargs)

    @property
    def is_attribute(self) -> bool:
        return False

    @property
    def backend_key(self) -> str:
        return self._alias

    def read_raw(self, instance: t.Any) -> t.Any:
        """Return the stored value, through the entity's accessor where it has one.

        Preferred over reading the row directly because a backend spells some columns as
        ``get_<name>()`` rather than as an attribute, and the property is where that difference is
        already resolved. Where the entity exposes no accessor of that name -- ``Group.extras``
        lives behind ``base.extras`` -- the row itself is the only place left to read.
        """
        if hasattr(type(instance), self._name):
            return getattr(instance, self._name)
        return getattr(instance.backend_entity, self._alias)


def add_field(
    key: str,
    alias: str | None = None,
    *,
    dtype: t.Any | None = None,
    doc: str = '',
    is_attribute: bool = False,
    is_subscriptable: bool = False,
) -> QbField:
    """Return a ``QbField`` for a key.

    .. deprecated:: Declare an :class:`AttributeField` or a ``ColumnField`` in the entity's
        ``_attribute_fields`` / ``_column_fields`` instead; ``Entity`` builds the query fields.
    """
    from aiida.common.warnings import warn_deprecation

    warn_deprecation(
        '`add_field` is deprecated, declare an `AttributeField` in the `_attribute_fields` of the entity instead.',
        version=3,
        stacklevel=2,
    )
    if not isidentifier(key):
        raise ValueError(f'{key} is not a valid python identifier')
    if not is_attribute and alias:
        raise ValueError('only attribute fields may be aliased')
    cls = QbAttributesField if key == 'attributes' else QbField
    return cls(key, alias, dtype=dtype, doc=doc, is_attribute=is_attribute)
