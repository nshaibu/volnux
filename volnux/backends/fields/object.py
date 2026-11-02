import dataclasses
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, ForwardRef, Optional, get_origin

from formax import Attrib, MiniAnnotated
from formax.typing import MISSING

from volnux.import_utils import import_string
from volnux.utils import get_obj_klass_import_str

from .utils import UNSET, formax_null_validator, unwrap_optional

logger = logging.getLogger(__name__)

_PRIMITIVES = (str, int, float, bool, bytes)


@dataclass(frozen=True)
class ObjectConfig:
    """Configuration for an ObjectField.

    Attributes:
        serializer: ``value -> JSON-safe payload``. Defaults to the built-in
            codec (see ``ObjectField``).
        deserializer: ``payload -> value``. Defaults to the built-in codec.
        default: Value used when none is given. ``ObjectField[Optional[T]]``
            defaults to None.
        default_factory: Callable producing the default (for mutable defaults).
        nullable: Allow None. Implied by ``Optional[T]``.
        coerce: Applied on assignment before validation, e.g. to normalise a
            single item into the collection the field holds.
    """

    serializer: Optional[Callable[[Any], Any]] = None
    deserializer: Optional[Callable[[Any], Any]] = None
    default: Any = UNSET
    default_factory: Any = UNSET
    nullable: bool = False
    coerce: Optional[Callable[[Any], Any]] = None


class _FactoryDefault:
    """Default that builds a new value for every model.

    Formax's fast ``__init__`` evaluates ``Attrib(default_factory=...)`` once and
    shares the result between instances, so the factory is carried by this
    marker and called by the pre-formatter, which runs for defaults too.
    """

    __slots__ = ("factory",)

    def __init__(self, factory: Callable[[], Any]) -> None:
        self.factory = factory

    def __repr__(self) -> str:
        return f"<default: {getattr(self.factory, '__name__', self.factory)}()>"


def _has_custom_state(klass: type) -> bool:
    """True when ``klass`` defines its own __getstate__ and __setstate__."""
    default_getstate = getattr(object, "__getstate__", None)
    getstate = getattr(klass, "__getstate__", None)
    return (
        getstate is not None
        and getstate is not default_getstate
        and hasattr(klass, "__setstate__")
    )


def default_serialize(value: Any) -> Any:
    """Built-in serializer: primitives, enums, dataclasses, containers and any
    class with its own ``__getstate__``/``__setstate__``."""
    if value is None or isinstance(value, _PRIMITIVES):
        return value
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: default_serialize(getattr(value, f.name))
            for f in dataclasses.fields(value)
        }
    if isinstance(value, dict):
        return {key: default_serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)) or hasattr(value, "maxlen"):
        return [default_serialize(item) for item in value]
    if _has_custom_state(type(value)):
        return {"__class__": get_obj_klass_import_str(value), "state": value.__getstate__()}
    raise TypeError(
        f"Cannot serialize {type(value).__name__} automatically. Give it "
        f"__getstate__/__setstate__ or use ObjectConfig(serializer=..., deserializer=...)."
    )


def default_deserialize(expected: Any, payload: Any) -> Any:
    """Inverse of ``default_serialize`` for the field's declared type."""
    if payload is None:
        return None
    if isinstance(payload, dict) and "__class__" in payload and "state" in payload:
        klass = import_string(payload["__class__"])
        instance = klass.__new__(klass)
        instance.__setstate__(payload["state"])
        return instance
    if isinstance(expected, type):
        if issubclass(expected, Enum):
            return payload if isinstance(payload, expected) else expected(payload)
        if dataclasses.is_dataclass(expected) and isinstance(payload, dict):
            return expected(**payload)
    return payload


def _resolve(annotation: Any) -> Any:
    """Resolve a forward reference to a class when possible, else None."""
    if isinstance(annotation, str):
        annotation = ForwardRef(annotation)
    if isinstance(annotation, ForwardRef):
        try:
            from volnux.backends.db_utils import ModelResolverMixin

            return ModelResolverMixin._resolve_target_model(annotation)
        except Exception:
            return None
    return annotation


def _runtime_class(annotation: Any) -> Optional[type]:
    """The class values must be instances of, or None if it cannot be checked
    (typing.Any, unions, unresolved forward references)."""
    annotation = _resolve(annotation)
    origin = get_origin(annotation)
    if origin is not None:
        annotation = origin
    return annotation if isinstance(annotation, type) else None


class ObjectField:
    """Field definition for an arbitrary object that is kept live in memory and
    serialized only when the model is persisted.

    Without a config the value is stored with a built-in codec: primitives,
    enums (by value), dataclasses (as a dict of fields) and containers; a class
    that defines ``__getstate__`` and ``__setstate__`` is stored as its class
    path plus that state. Anything else needs ``ObjectConfig(serializer=...,
    deserializer=...)``.

    Example:
        >>> class Job(BaseModel):
        ...     status: ObjectField[Status, ObjectConfig(default=Status.NEW)]
        ...     metrics: ObjectField[Metrics, ObjectConfig(default_factory=Metrics)]
        ...     parent: ObjectField[Optional[Job]]
        ...     engine: ObjectField[Engine, ObjectConfig(serializer=ref, deserializer=load)]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError("Cannot subclass ObjectField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ObjectField cannot be instantiated")

    @classmethod
    def __class_getitem__(cls, params: Any) -> MiniAnnotated:
        if not isinstance(params, tuple):
            params = (params,)
        if len(params) == 1:
            annotation, config = params[0], ObjectConfig()
        elif len(params) == 2:
            annotation, config = params
            if not isinstance(config, ObjectConfig):
                raise TypeError(
                    f"Second argument must be ObjectConfig, got {type(config).__name__}"
                )
        else:
            raise TypeError(
                f"ObjectField[...] expects 1 or 2 arguments, got {len(params)}"
            )

        annotation, optional = unwrap_optional(annotation)
        nullable = config.nullable or optional

        default, default_factory = config.default, config.default_factory
        if default is UNSET and default_factory is UNSET and optional:
            default = None
        default = MISSING if default is UNSET else default
        if default_factory is not UNSET:
            default, default_factory = _FactoryDefault(default_factory), MISSING
        else:
            default_factory = MISSING

        def pre_fmt(instance: Any, value: Any) -> Any:
            if isinstance(value, _FactoryDefault):
                value = value.factory()
            if config.coerce is not None:
                value = config.coerce(value)
            if value is None:
                if not nullable:
                    raise ValueError("ObjectField cannot be None")
                return None
            expected = _runtime_class(annotation)
            if expected is not None and not isinstance(value, expected):
                raise TypeError(
                    f"Expected {expected.__name__}, got {type(value).__name__}"
                )
            return value

        def to_state(instance: Any, value: Any) -> Any:
            if value is None:
                return None
            if config.serializer is not None:
                return config.serializer(value)
            return default_serialize(value)

        def from_state(instance: Any, payload: Any) -> Any:
            if payload is None:
                return None
            if config.deserializer is not None:
                return config.deserializer(payload)
            return default_deserialize(_runtime_class(annotation), payload)

        validators = [] if nullable else [formax_null_validator]
        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                validators=validators,
                default=default,
                default_factory=default_factory,
                metadata={
                    "type": "object",
                    "object_type": annotation,
                    "nullable": nullable,
                    "to_state": to_state,
                    "from_state": from_state,
                },
            ),
        ]
