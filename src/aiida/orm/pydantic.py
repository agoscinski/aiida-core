from __future__ import annotations

import typing as t
from types import SimpleNamespace

from aiida.common.exceptions import EntryPointError, NotExistent

if t.TYPE_CHECKING:
    from aiida.orm import Entity

__all__ = ('OrmModel',)

_UNDEFINED = object()


def ConfigDict(**kwargs: t.Any) -> dict[str, t.Any]:  # noqa: N802
    """Return a plain dictionary instead of a Pydantic config."""
    return kwargs


def WithJsonSchema(schema: dict[str, t.Any]) -> dict[str, t.Any]:  # noqa: N802
    """Return schema metadata unchanged instead of a Pydantic marker."""
    return schema


class AliasChoices(tuple):
    """Minimal stand-in for Pydantic alias choices."""

    def __new__(cls, *choices: str) -> AliasChoices:
        return super().__new__(cls, choices)


def field_validator(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
    """No-op replacement for Pydantic field validators."""

    def decorator(function: t.Any) -> t.Any:
        return function

    return decorator


def get_metadata(field_info: t.Any, key: str, default: t.Any | None = None) -> t.Any:
    """Return dummy field metadata for the benchmark pydantic bypass."""
    for element in getattr(field_info, 'metadata', []):
        if isinstance(element, dict) and key in element:
            return element[key]
    return default


class _BenchmarkField:
    """Minimal replacement for pydantic ``FieldInfo`` for import benchmarks."""

    def __init__(self, default: t.Any = _UNDEFINED, **kwargs: t.Any) -> None:
        self.default = default
        self.default_factory = kwargs.get('default_factory')
        self.annotation: t.Any = None
        self.alias = kwargs.get('alias')
        self.description = kwargs.get('description')
        self.examples = kwargs.get('examples')
        self.metadata: list[dict[str, t.Any]] = []

    def is_required(self) -> bool:
        return self.default is _UNDEFINED and self.default_factory is None


class OrmModel:
    """Benchmark-only stand-in that avoids constructing Pydantic models."""

    model_config: dict[str, t.Any] = {}
    model_fields: dict[str, _BenchmarkField] = {}
    __pydantic_decorators__ = SimpleNamespace(field_serializers={}, field_validators={})

    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__init_subclass__(**kwargs)
        fields = {}
        for base in reversed(cls.__mro__[1:]):
            fields.update(getattr(base, 'model_fields', {}))
        for key, annotation in getattr(cls, '__annotations__', {}).items():
            default = getattr(cls, key, _UNDEFINED)
            field = default if isinstance(default, _BenchmarkField) else _BenchmarkField(default)
            field.annotation = annotation
            fields[key] = field
        cls.model_fields = fields
        cls.__pydantic_decorators__ = SimpleNamespace(field_serializers={}, field_validators={})

    def __init__(self, **kwargs: t.Any) -> None:
        for key, field in self.__class__.model_fields.items():
            field_name = field.alias or key
            if field_name in kwargs:
                value = kwargs[field_name]
            elif key in kwargs:
                value = kwargs[key]
            elif field.default_factory is not None:
                value = field.default_factory()
            elif field.default is not _UNDEFINED:
                value = field.default
            else:
                value = None
            setattr(self, key, value)

    def model_dump(self, *args: t.Any, **kwargs: t.Any) -> dict[str, t.Any]:
        return {key: getattr(self, key) for key in self.__class__.model_fields}

    @classmethod
    def model_json_schema(cls, *args: t.Any, **kwargs: t.Any) -> dict[str, t.Any]:
        return {'title': cls.__qualname__.replace('.', '')}

    @classmethod
    def model_rebuild(cls, *args: t.Any, **kwargs: t.Any) -> bool:
        return True

    def _to_orm_field_values(self) -> dict[str, t.Any]:
        """Return the field values for ORM instantiation."""
        from aiida.plugins.factories import BaseFactory

        fields: dict[str, t.Any] = {}
        for key, field in self.__class__.model_fields.items():
            field_name = field.alias or key
            field_value = getattr(self, key, field.default)

            if field_value is None:
                continue

            if isinstance(field_value, OrmModel):
                fields[field_name] = field_value._to_orm_field_values()
            elif orm_class := get_metadata(field, 'orm_class'):
                if isinstance(orm_class, str):
                    try:
                        orm_class = BaseFactory('aiida.orm', orm_class)
                    except EntryPointError as exception:
                        raise EntryPointError(f'invalid `orm_class` on `{key}`: {exception}') from exception
                try:
                    fields[field_name] = orm_class.collection.get(id=field_value)
                except NotExistent as exception:
                    raise NotExistent(f'no `{orm_class}` found with pk={field_value}') from exception
            elif model_to_orm := get_metadata(field, 'model_to_orm'):
                fields[field_name] = model_to_orm(self)
            else:
                fields[field_name] = field_value

        return fields

    @classmethod
    def _as_minimal_model(cls: type[OrmModel]) -> type[OrmModel]:
        return cls


class OrmFieldsAsModelDump(OrmModel):
    """Mixin to override the default ORM field value extraction with a simple call to `model_dump`."""

    def _to_orm_field_values(self) -> dict[str, t.Any]:
        """Return the field values for ORM instantiation as a simple call to `model_dump`."""
        return self.model_dump()


def OrmMetadataField(  # noqa: N802
    default: t.Any = _UNDEFINED,
    *,
    orm_class: type[Entity[t.Any, t.Any]] | str | None = None,
    orm_to_model: (
        t.Callable[[Entity[t.Any, t.Any]], t.Any] | t.Callable[[Entity[t.Any, t.Any], dict[str, t.Any]], t.Any] | None
    ) = None,
    model_to_orm: t.Callable[[OrmModel], t.Any] | None = None,
    **kwargs: t.Any,
) -> t.Any:
    """Return a benchmark field without importing Pydantic."""
    field_info = _BenchmarkField(default, **kwargs)

    for key, value in (
        ('priority', kwargs.get('priority')),
        ('short_name', kwargs.get('short_name')),
        ('option_cls', kwargs.get('option_cls')),
        ('read_only', kwargs.get('read_only', False)),
        ('write_only', kwargs.get('write_only', False)),
        ('may_be_large', kwargs.get('may_be_large', False)),
        ('orm_class', orm_class),
        ('orm_to_model', orm_to_model),
        ('model_to_orm', model_to_orm),
    ):
        if value is not None:
            field_info.metadata.append({key: value})

    return field_info
