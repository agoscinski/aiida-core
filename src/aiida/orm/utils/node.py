###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Utilities to operate on `Node` classes."""

import logging
import warnings
from abc import ABCMeta

from aiida.common import exceptions

__all__ = (
    'AbstractNodeMeta',
    'get_query_type_from_type_string',
    'get_type_string_from_class',
    'load_node_class',
)


def load_node_class(type_string):
    """Return the `Node` sub class that corresponds to the given type string.

    :param type_string: the `type` string of the node
    :return: a sub class of `Node`
    """
    from aiida.orm import Data, Node, ProcessNode
    from aiida.plugins.entry_point import load_entry_point

    if type_string == '':
        return Node

    if type_string == 'data.Data.':
        return Data

    if not type_string.endswith('.'):
        raise exceptions.DbContentError(f'The type string `{type_string}` is invalid')

    base_path = type_string.rsplit('.', 2)[0]

    # This exception needs to be there to make migrations work that rely on the old type string starting with `node.`
    # Since now the type strings no longer have that prefix, we simply strip it and continue with the normal logic.
    if base_path.startswith('node.'):
        base_path = base_path.removeprefix('node.')

    # If the Data plugin is not available we fall back on the base Data class
    if base_path.startswith('data.'):
        entry_point_name = base_path.removeprefix('data.')
        try:
            return load_entry_point('aiida.data', entry_point_name)
        except exceptions.MissingEntryPointError:
            return Data

    # If the Process plugin is not available we fall back on the base ProcessNode class
    if base_path.startswith('process'):
        try:
            return load_entry_point('aiida.node', base_path)
        except exceptions.MissingEntryPointError:
            return ProcessNode

    # At this point we really have an anomalous type string. At some point, storing nodes with unresolvable type strings
    # was allowed, for example by creating a sub class in a shell and then storing an instance. Attempting to load the
    # node then would fail miserably. This is now no longer allowed, but we need a fallback for existing cases, which
    # should be rare. We fallback on `Data` and not `Node` because bare node instances are also not storable and so the
    # logic of the ORM is not well defined for a loaded instance of the base `Node` class.
    warnings.warn(f'unknown type string `{type_string}`, falling back onto `Data` class')

    return Data


def get_type_string_from_class(class_module, class_name):
    """Given the module and name of a class, determine the orm_class_type string, which codifies the
    orm class that is to be used. The returned string will always have a terminating period, which
    is required to query for the string in the database

    :param class_module: module of the class
    :param class_name: name of the class
    """
    from aiida.plugins.entry_point import ENTRY_POINT_GROUP_TO_MODULE_PATH_MAP, get_entry_point_from_class

    group, entry_point = get_entry_point_from_class(class_module, class_name)

    # If we can reverse engineer an entry point group and name, we're dealing with an external class
    if group and entry_point:
        module_base_path = ENTRY_POINT_GROUP_TO_MODULE_PATH_MAP[group]
        type_string = f'{module_base_path}.{entry_point.name}.{class_name}.'

    # Otherwise we are dealing with an internal class
    else:
        type_string = f'{class_module}.{class_name}.'

    prefixes = ('aiida.orm.nodes.', 'aiida.orm.core.')

    # Sequentially and **in order** strip the prefixes if present
    for prefix in prefixes:
        type_string = type_string.removeprefix(prefix)

    # This needs to be here as long as `aiida.orm.nodes.data` does not live in `aiida.orm.nodes.data` because all the
    # `Data` instances will have a type string that starts with `data.` instead of `nodes.`, so in order to match any
    # `Node` we have to look for any type string essentially.
    if type_string == 'node.Node.':
        type_string = ''

    return type_string


def is_valid_node_type_string(type_string, raise_on_false=False):
    """Checks whether type string of a Node is valid.

    :param type_string: the plugin_type_string attribute of a Node
    :return: True if type string is valid, else false
    """
    # Currently the type string for the top-level node is empty.
    # Change this when a consistent type string hierarchy is introduced.
    if type_string == '':
        return True

    # Note: this allows for the user-defined type strings like 'group' in the QueryBuilder
    # as well as the usual type strings like 'data.parameter.ParameterData.'
    if type_string.count('.') == 1 or not type_string.endswith('.'):
        if raise_on_false:
            raise exceptions.DbContentError(f'The type string {type_string} is invalid')
        return False

    return True


def get_query_type_from_type_string(type_string):
    """Take the type string of a Node and create the queryable type string

    :param type_string: the plugin_type_string attribute of a Node
    :return: the type string that can be used to query for
    """
    is_valid_node_type_string(type_string, raise_on_false=True)

    # Currently the type string for the top-level node is empty.
    # Change this when a consistent type string hierarchy is introduced.
    if type_string == '':
        return ''

    type_path = type_string.rsplit('.', 2)[0]
    type_string = f'{type_path}.'

    return type_string


#: Node model/fields attribute names governed by `_LazyNodeModelAttribute`. Of these, only the first
#: three (`AttributesModel`, `ReadModel`, `WriteModel`) can ever appear as an author-declared template
#: in a class body (`fields` is always fully derived, never hand-written).
_LAZY_NODE_MODEL_ATTRIBUTES = ('AttributesModel', 'ReadModel', 'WriteModel', 'fields')


class _LazyNodeModelAttribute:
    """Descriptor exposing a node model/fields attribute that is built lazily on first access.

    The *same* four instances of this class (one per name) are installed in two places for every
    ``Node`` subclass:

    - as **data descriptors on the metaclass** (:class:`AbstractNodeMeta`), which is what makes
      ``SomeNodeClass.ReadModel`` (class-level access) resolve through here even though a subclass
      may declare its own ``class ReadModel(Parent.ReadModel): ...`` in its body: a data descriptor
      on the metaclass always wins over anything in ``SomeNodeClass.__dict__``/its MRO, since
      ``type.__getattribute__`` checks the metaclass first;
    - as a **plain class attribute on every node class itself** (`AbstractNodeMeta.__new__` installs
      it directly into each class' own namespace), which is what makes ``some_node.ReadModel``
      (instance-level access, e.g. inside ``self.ReadModel``) resolve through here too: instance
      attribute lookup (``object.__getattribute__``) only ever consults ``type(instance).__mro__``,
      never the metaclass, so without this second installation instance access would silently fall
      through to whatever plain value happens to sit in some ancestor's ``__dict__``.

    Because a class' own ``__dict__`` entry for these names would otherwise collide with a subclass'
    declared template, `AbstractNodeMeta.__new__` relocates any such template (before installing this
    descriptor) into a private ``_<name>_template`` slot; `Node._ensure_models_built` reads it back
    from there. The resolved/cached model lives in a further separate slot, ``_lazy_<name>``.
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._cache_attr = f'_lazy_{name}'

    def __get__(self, obj, owner=None):
        if obj is None:
            return self
        cls = obj if isinstance(obj, type) else type(obj)
        if self._cache_attr not in cls.__dict__:
            cls._ensure_models_built()
        return cls.__dict__[self._cache_attr]

    def __set__(self, obj, value) -> None:
        cls = obj if isinstance(obj, type) else type(obj)
        setattr(cls, self._cache_attr, value)


#: Shared descriptor instances, reused verbatim as both the metaclass-level and the per-class
#: instance-level accessor for each name (see `_LazyNodeModelAttribute`); there are only ever these 4
#: objects, regardless of how many `Node` subclasses (built-in or plugin-provided) exist.
_lazy_node_model_descriptors = {name: _LazyNodeModelAttribute(name) for name in _LAZY_NODE_MODEL_ATTRIBUTES}


class AbstractNodeMeta(ABCMeta):
    """Some python black magic to set correctly the logger also in subclasses."""

    AttributesModel = _lazy_node_model_descriptors['AttributesModel']
    ReadModel = _lazy_node_model_descriptors['ReadModel']
    WriteModel = _lazy_node_model_descriptors['WriteModel']
    fields = _lazy_node_model_descriptors['fields']

    def __new__(mcs, name, bases, namespace, **kwargs):
        for attr_name in ('AttributesModel', 'ReadModel', 'WriteModel'):
            if attr_name in namespace:
                # An author-declared template (e.g. `class AttributesModel(Parent.AttributesModel): ...`)
                # would otherwise collide with the descriptor installed below; relocate it so
                # `Node._ensure_models_built` can still read it back as the template.
                namespace[f'_{attr_name}_template'] = namespace.pop(attr_name)
            namespace[attr_name] = _lazy_node_model_descriptors[attr_name]
        namespace['fields'] = _lazy_node_model_descriptors['fields']

        newcls = super().__new__(mcs, name, bases, namespace, **kwargs)
        newcls._logger = logging.getLogger(f'{namespace["__module__"]}.{name}')
        return newcls
