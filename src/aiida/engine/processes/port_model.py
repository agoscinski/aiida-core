###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Declare port namespaces using PortModel fields."""

from __future__ import annotations

import dataclasses
import typing as t

from typing_extensions import Self, dataclass_transform

__all__ = (
    'Field',
    'PortField',
    'PortModel',
    'as_dict',
    'build',
    'fields_of',
    'is_structured',
    'is_typeddict_annotation',
    'namespace_fields_of',
    'typeddict_fields_of',
    'without_marks',
    'without_optional',
)

UNSPECIFIED = object()


@dataclasses.dataclass(frozen=True, kw_only=True)
class PortField:
    """Metadata for a port, written as ``Annotated[T, PortField(help='...')]``.

    :param help: the help displayed for the generated port or namespace.
    """

    help: str | None = None


@dataclass_transform(frozen_default=True, kw_only_default=True, field_specifiers=(dataclasses.field,))
class PortModel:
    """Declare a namespace using annotated fields and optional defaults.

    Subclasses are frozen, keyword-only dataclasses. AiiDA ports validate the
    values; the model itself does not coerce them. Task functions receive
    attribute-accessible namespace mappings, not reconstructed model instances.
    Model instances are optional input/output conveniences. Opaque inputs use ORM nodes.
    """

    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__init_subclass__(**kwargs)
        dataclasses.dataclass(cls, frozen=True, kw_only=True)

    def as_dict(self) -> dict[str, t.Any]:
        """Return the declared fields as a namespace mapping."""
        return {field.name: _held(getattr(self, field.name)) for field in fields_of(type(self)) or ()}

    @classmethod
    def from_dict(cls, values: t.Mapping[str, t.Any]) -> Self:
        """Reconstruct a model, including nested namespaces, from a mapping.

        :param values: supplied field values.
        :return: the reconstructed model.
        """
        fields = {field.name: field for field in fields_of(cls) or ()}
        held = {
            name: build(fields[name].annotation, value)
            if name in fields and isinstance(value, t.Mapping) and is_structured(fields[name].annotation)
            else value
            for name, value in values.items()
        }
        return cls(**held)


@dataclasses.dataclass(frozen=True)
class Field:
    """One model field in the words a port is declared with."""

    name: str
    annotation: t.Any
    default: t.Any = UNSPECIFIED
    help: str | None = None

    @property
    def required(self) -> bool:
        """Return whether a value has to be given for this field."""
        return self.default is UNSPECIFIED


def is_a_plain_class(annotation: t.Any) -> bool:
    """Return whether an annotation is a class without generic arguments."""
    return isinstance(annotation, type) and t.get_origin(annotation) is None


def without_optional(annotation: t.Any) -> t.Any:
    """Return the non-``None`` member of an optional union, or the annotation itself.

    Only a union of one other type and ``None`` is unwrapped, which is what ``T | None`` and
    ``Optional[T]`` both spell. Anything else is returned unchanged, so genuine unions keep
    their leaf contract instead of silently becoming a namespace.
    """
    from types import UnionType

    annotation = without_marks(annotation)
    if t.get_origin(annotation) in (t.Union, UnionType):
        members = [member for member in t.get_args(annotation) if member is not type(None)]
        if len(members) == 1 and len(t.get_args(annotation)) == 2:
            return without_marks(members[0])
    return annotation


def is_structured(annotation: t.Any) -> bool:
    """Return whether an annotation declares a PortModel namespace."""
    return is_a_plain_class(without_optional(annotation)) and issubclass(without_optional(annotation), PortModel)


def fields_of(annotation: t.Any) -> tuple[Field, ...] | None:
    """Read a PortModel declaration, or return None for other annotations.

    :param annotation: the parameter or return annotation.
    """
    annotation = without_optional(annotation)
    if not is_structured(annotation):
        return None
    hints = t.get_type_hints(annotation, include_extras=True)
    fields = []
    for field in dataclasses.fields(annotation):
        if not field.init:
            continue
        if field.default is not dataclasses.MISSING:
            default = field.default
        elif field.default_factory is not dataclasses.MISSING:
            default = field.default_factory()
        else:
            default = UNSPECIFIED
        hint = hints.get(field.name)
        fields.append(Field(name=field.name, annotation=without_marks(hint), default=default, help=_port_help(hint)))
    return tuple(fields)


def is_typeddict_annotation(annotation: t.Any) -> bool:
    """Return whether an annotation is a ``TypedDict``, which declares no ports."""
    candidate = without_optional(annotation) if annotation is not None else None
    is_typeddict = getattr(t, 'is_typeddict', None)
    if callable(is_typeddict):
        try:
            return bool(is_typeddict(candidate))
        except TypeError:
            return False
    return isinstance(candidate, type) and issubclass(candidate, dict) and hasattr(candidate, '__required_keys__')


def typeddict_fields_of(annotation: t.Any) -> tuple[Field, ...] | None:
    """Read a ``TypedDict`` declaration as namespace fields, or return None for other annotations.

    Requiredness comes from the ``TypedDict`` itself: fields in ``__required_keys__`` are required,
    the rest default to ``None`` like an optional ``PortModel`` field. ``Required``/``NotRequired``
    wrappers name the inner type. A ``total=False`` mapping is therefore an all-optional namespace.

    :param annotation: the parameter or return annotation.
    """
    candidate = without_optional(annotation)
    if not is_typeddict_annotation(candidate):
        return None
    try:
        hints = t.get_type_hints(candidate, include_extras=True)
    except Exception:
        hints = dict(getattr(candidate, '__annotations__', {}))
    required_keys = frozenset(getattr(candidate, '__required_keys__', ()))
    markers = {
        marker for marker in (getattr(t, 'Required', None), getattr(t, 'NotRequired', None)) if marker is not None
    }
    fields = []
    for name, declared in hints.items():
        unmarked = without_marks(declared)
        if markers and t.get_origin(unmarked) in markers and t.get_args(unmarked):
            unmarked = without_marks(t.get_args(unmarked)[0])
        required = name in required_keys
        fields.append(
            Field(
                name=name,
                annotation=unmarked,
                default=UNSPECIFIED if required else None,
                help=_port_help(unmarked),
            )
        )
    return tuple(fields)


def namespace_fields_of(annotation: t.Any) -> tuple[Field, ...] | None:
    """Read a ``PortModel`` or ``TypedDict`` declaration as namespace fields.

    A ``TypedDict`` is the plain-mapping spelling of the same contract: fixed string keys with
    declared value types. Either one declares a namespace of ports; anything else returns ``None``.

    :param annotation: the parameter or return annotation.
    """
    fields = fields_of(annotation)
    return fields if fields is not None else typeddict_fields_of(annotation)


def as_dict(value: t.Any) -> dict[str, t.Any] | None:
    """Return a model's namespace values, or None for other values.

    :param value: a model instance or any other value.
    """
    return value.as_dict() if isinstance(value, PortModel) else None


def _held(value: t.Any) -> t.Any:
    return value.as_dict() if isinstance(value, PortModel) else value


def build(container: type[PortModel], values: t.Mapping[str, t.Any]) -> PortModel:
    """Reconstruct a model from namespace values.

    :param container: the model class.
    :param values: supplied field values.
    """
    return container.from_dict(values)


def _port_help(annotation: t.Any) -> str | None:
    while t.get_origin(annotation) is t.Annotated:
        args = t.get_args(annotation)
        for mark in args[1:]:
            if isinstance(mark, PortField):
                return mark.help
        annotation = args[0]
    return None


def without_marks(annotation: t.Any) -> t.Any:
    """Return an annotation without its metadata."""
    while t.get_origin(annotation) is t.Annotated:
        annotation = t.get_args(annotation)[0]
    return annotation
