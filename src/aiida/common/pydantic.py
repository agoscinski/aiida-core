"""Utilities related to ``pydantic``."""

from __future__ import annotations

import functools
import typing as t
from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, Field, create_model
from pydantic.fields import FieldInfo
from pydantic.functional_validators import AfterValidator
from pydantic_core import PydanticUndefined

from aiida.common.fields import MISSING, LayerField

if t.TYPE_CHECKING:
    from aiida.orm.fields import BaseField


def get_metadata(field_info: FieldInfo, key: str, default: t.Any | None = None) -> t.Any:
    """Return a the metadata of the given field for a particular key.

    :param field_info: The field from which to retrieve the metadata.
    :param key: The metadata name.
    :param default: Optional default value to return in case the metadata is not defined on the field.
    :returns: The metadata if defined, otherwise the default.
    """
    for element in field_info.metadata:
        if isinstance(element, dict) and key in element:
            return element[key]
    return default


class AiiDABaseModel(BaseModel, defer_build=True):
    """Base class for all AiiDA pydantic models."""

    model_config = ConfigDict(
        serialize_by_alias=True,
        validate_by_alias=True,
        validate_by_name=True,
    )

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        cls._set_json_schema_title()

    @classmethod
    def __pydantic_on_complete__(cls, **kwargs: t.Any) -> None:
        # For Node subclasses, `__pydantic_init_subclass__` is run too early due to patching.
        # However, we can't only use `__pydantic_on_complete__` due to `defer_build=True`.
        # Therefore, we set the JSON schema title in both to guarantee the title is set correctly.
        super().__pydantic_on_complete__(**kwargs)
        cls._set_json_schema_title()

    @classmethod
    def _set_json_schema_title(cls) -> None:
        """Derive the JSON schema title.

        The title is derived from the `__qualname__`, for example, `Int.Model` -> `IntModel`
        """
        cls.model_config['title'] = cls.__qualname__.replace('.', '')


def MetadataField(  # noqa: N802
    default: t.Any = PydanticUndefined,
    *,
    priority: int = 0,
    short_name: str | None = None,
    option_cls: t.Any | None = None,
    read_only: bool = False,
    write_only: bool = False,
    may_be_large: bool = False,
    **kwargs: t.Any,
) -> t.Any:
    """Return a :class:`pydantic.fields.Field` instance with additional metadata.

    .. code-block:: python

        class Model(AiiDABaseModel):

            field: MetadataField('default', priority=1000, short_name='-A')

    This is a utility function that constructs a ``Field`` instance with an easy interface to add additional metadata.
    It is possible to add metadata using ``Annotated``::

        class Model(AiiDABaseModel):

            field: Annotated[str, {'metadata': 'value'}] = Field(...)

    However, when requiring multiple metadata, this notation can make the model difficult to read. Since this utility
    is only used to automatically build command line interfaces from the model definition, it is possible to restrict
    which metadata are accepted.

    :param priority: Used to order the list of all fields in the model. Ordering is done from small to large priority.
    :param short_name: Optional short name to use for an option on a command line interface.
    :param option_cls: The :class:`click.Option` class to use to construct the option.
    :param read_only: When set to ``True``, this field value will not be passed to the ORM entity constructor
        through ``Entity.from_model``.
    :param write_only: When set to ``True``, this field value will not be populated when constructing the model from an
        ORM entity through ``Entity.to_model``.
    :param may_be_large: Whether the field value may be large. This is used to determine whether to include the field
        when serializing the entity for various purposes, such as exporting or logging.
    """

    extra = kwargs.pop('json_schema_extra', {})

    if read_only and write_only:
        raise ValueError('A field cannot be both read-only and write-only.')

    if read_only:
        extra.update({'readOnly': True})

    if write_only:
        extra.update({'writeOnly': True})

    kwargs['json_schema_extra'] = extra

    field_info = Field(default, **kwargs)

    for key, value in (
        ('priority', priority),
        ('short_name', short_name),
        ('option_cls', option_cls),
        ('read_only', read_only),
        ('write_only', write_only),
        ('may_be_large', may_be_large),
    ):
        if value is not None:
            field_info.metadata.append({key: value})

    return field_info


#: What a declaration with no layer entry gets: published under its own name, value untouched.
_PUBLISH_AS_IS: t.Final = LayerField()


def _annotation(declaration: BaseField, options: LayerField) -> t.Any:
    """Return the annotation for a field, carrying whatever the declaration says it must satisfy.

    Rendering and parsing are ``pydantic``'s, not ours: every declared type is one it already
    handles, and a value has to be database-serialisable before it gets here. A layer that
    publishes the value in an encoding of its own states the type it publishes as, and its own
    validation belongs with it rather than with the stored value.
    """
    if options.dtype is not None:
        return options.dtype
    if declaration.validator is None:
        return declaration.wire_dtype
    return t.Annotated[declaration.wire_dtype, AfterValidator(_as_value_error(declaration.validator))]


def _as_value_error(validator: t.Callable[[t.Any], t.Any]) -> t.Callable[[t.Any], t.Any]:
    """Return the validator with AiiDA's own validation error restated as one pydantic collects.

    ``pydantic`` gathers a ``ValueError`` into its own ``ValidationError``, reporting which field
    failed and why alongside every other failure in the payload; anything else propagates raw and
    aborts the request at the first bad field. The exception type a direct write raises is
    unchanged -- this wrapping is the model boundary's business only.
    """

    @functools.wraps(validator)
    def _validate(value: t.Any) -> t.Any:
        from aiida.common.exceptions import ValidationError

        try:
            return validator(value)
        except ValidationError as exception:
            raise ValueError(str(exception)) from exception

    return _validate


def build_model(
    name: str,
    declarations: Mapping[str, BaseField],
    layer_options: Mapping[str, LayerField],
    *,
    base: type[AiiDABaseModel] = AiiDABaseModel,
) -> type[AiiDABaseModel]:
    """Build a ``pydantic`` model from ORM field declarations and one layer's view of them.

    This is the piece with a genuine claim on a shared home: the CLI and the REST API project the
    same declarations, and each was hand-rolling a dict literal over the same field names. What
    they share is the *builder*; what stays theirs is which fields, under which names.

    :param name: The name of the generated model class.
    :param declarations: What the backend stores, from ``Entity._field_declarations``.
    :param layer_options: What this layer does with each declaration, keyed by declared name. A
        declaration with no entry is published as it stands.
    :param base: The base class of the generated model.
    """
    definitions: dict[str, t.Any] = {}

    for key, declaration in declarations.items():
        options = layer_options.get(key, _PUBLISH_AS_IS)
        if options.exclude:
            continue

        # `Field()` is typed to return the default's own type, so pin this or the first branch
        # decides what the other two may assign.
        info: t.Any
        shared: dict[str, t.Any] = {'description': declaration.doc}
        if declaration.examples is not None:
            shared['examples'] = declaration.examples
        if options.name is not None:
            shared['alias'] = options.name

        if declaration.default_factory is not None:
            info = Field(default_factory=declaration.default_factory, **shared)
        elif declaration.default is MISSING:
            info = Field(**shared)
        else:
            info = Field(declaration.default, **shared)

        definitions[key] = (_annotation(declaration, options), info)

    return t.cast('type[AiiDABaseModel]', create_model(name, __base__=base, **definitions))


def published_values(entity: t.Any, layer_options: Mapping[str, LayerField]) -> dict[str, t.Any]:
    """Return the stored values a layer publishes, ready to hand to its model.

    Just the stored values of the fields the layer keeps; rendering them is ``pydantic``'s job.
    """
    declarations = type(entity)._field_declarations
    values = {}

    for name, declaration in declarations.items():
        options = layer_options.get(name, _PUBLISH_AS_IS)
        if options.exclude:
            continue
        # A layer serialiser is given the accessor value rather than the stored form, so that a
        # layer wanting the entity rather than its `pk` can still reach it.
        values[name] = (
            options.serialize(declaration.read_raw(entity)) if options.serialize else declaration.read(entity)
        )

    return values
