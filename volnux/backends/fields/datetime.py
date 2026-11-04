"""Date and datetime fields for formax models.

A field declared as ``DateTimeField[DTConfig(...)]`` or ``DateField[DTConfig(...)]``
holds a real :class:`datetime.datetime` / :class:`datetime.date` on the model, so
values can be compared, subtracted and formatted directly. Like ``ObjectField`` and
``ListField`` it carries ``to_state`` / ``from_state`` codecs in ``Attrib.metadata``:
the store sees ISO-8601 strings, and loading a record restores the real values.

What a field accepts when a value is assigned (or loaded):

* ``DateTimeField``: a ``datetime``, an ISO-8601 string, or epoch *seconds*
  (``int`` / ``float``).
* ``DateField``: a ``date``, a ``datetime``, an ISO-8601 date or datetime string, or
  epoch seconds (read as the UTC date).

Anything else (``bool``, NaN/infinity, an out-of-range epoch such as milliseconds,
a blank string, other types) is rejected with a message naming the field and value.

Time zones: with ``tz_aware=True`` (the default) a naive datetime is taken as UTC and
an aware one is converted to UTC, so every stored instant is directly comparable.
With ``tz_aware=False`` the value is kept exactly as given. ``DateField`` has no time
zone of its own: an aware datetime is converted to UTC first, then reduced to its date.

``auto_now`` sets the field to the current time whenever the record is saved;
``auto_now_add`` sets it once, when the record is created.
"""

import datetime
import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from formax import MiniAnnotated, Attrib

from .utils import formax_null_validator

_UTC = datetime.timezone.utc

#: Formats ``fromisoformat`` cannot read on older Pythons (compact offsets).
_FALLBACK_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S%z",
)


@dataclass
class DTConfig:
    """Configuration for DateTimeField and DateField.

    Args:
        field_name: Name of the attribute. Optional and rarely needed: the field finds
            the attribute it is declared on by itself. It is only a fallback when that
            lookup fails.
        auto_now: Set the field to the current time every time the record is saved
            (UTC; ``DateField`` uses the UTC date). A value assigned in between is
            respected until the next save.
        auto_now_add: Set the field to the current time when the record is first
            created, and keep it from then on.
        tz_aware: ``DateTimeField`` only. If true (default), values are normalized to
            UTC (naive values are taken as UTC). If false, values are kept as given.
        nullable: Whether the field can be None. A nullable field may be left out when
            the record is created.
    """

    field_name: Optional[str] = None
    auto_now: bool = False
    auto_now_add: bool = False
    tz_aware: bool = True
    nullable: bool = False


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(_UTC)


def _utc_today() -> datetime.date:
    return _utc_now().date()


# --- parsing -----------------------------------------------------------------


def _describe(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 60 else text[:57] + "..."


def _parse_iso_datetime(text: str) -> datetime.datetime:
    stripped = text.strip()
    if not stripped:
        raise ValueError("a blank string is not a date or time")
    if stripped[-1] in "zZ":
        stripped = stripped[:-1] + "+00:00"
    try:
        return datetime.datetime.fromisoformat(stripped)
    except ValueError:
        pass
    for fmt in _FALLBACK_DATETIME_FORMATS:
        try:
            return datetime.datetime.strptime(stripped, fmt)
        except ValueError:
            continue
    raise ValueError(f"{text!r} is not an ISO-8601 date/time")


def _from_epoch(value: float) -> datetime.datetime:
    if not math.isfinite(value):
        raise ValueError(f"{value!r} is not a valid epoch time")
    try:
        return datetime.datetime.fromtimestamp(value, tz=_UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(
            f"{value!r} is out of range as epoch seconds (milliseconds?)"
        ) from exc


def _to_datetime(value: Any, *, tz_aware: bool, kind: str) -> datetime.datetime:
    """Normalize any accepted input to a ``datetime``."""
    if isinstance(value, bool):
        raise TypeError(f"{kind} cannot hold a bool ({_describe(value)})")
    if isinstance(value, datetime.datetime):
        result = value
        given_tz = value.tzinfo is not None
    elif isinstance(value, str):
        try:
            result = _parse_iso_datetime(value)
        except ValueError as exc:
            raise ValueError(f"{kind} cannot read {_describe(value)}: {exc}") from exc
        given_tz = result.tzinfo is not None
    elif isinstance(value, (int, float)):
        try:
            result = _from_epoch(value)
        except ValueError as exc:
            raise ValueError(f"{kind} cannot read {_describe(value)}: {exc}") from exc
        given_tz = False  # epoch seconds are UTC; "as given" means naive UTC below
        if not tz_aware:
            return result.replace(tzinfo=None)
    else:
        raise TypeError(
            f"{kind} expects a datetime, an ISO-8601 string or epoch seconds, "
            f"got {type(value).__name__}"
        )

    if tz_aware:
        return result.astimezone(_UTC) if given_tz else result.replace(tzinfo=_UTC)
    return result


def _to_date(value: Any, *, kind: str) -> datetime.date:
    """Normalize any accepted input to a ``date``."""
    if isinstance(value, bool):
        raise TypeError(f"{kind} cannot hold a bool ({_describe(value)})")
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(_UTC)
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return datetime.date.fromisoformat(text)
        except ValueError:
            pass
        try:
            return _to_date(_parse_iso_datetime(text), kind=kind)
        except ValueError as exc:
            raise ValueError(
                f"{kind} cannot read {_describe(value)}: expected ISO-8601 "
                f"(YYYY-MM-DD)"
            ) from exc
    if isinstance(value, (int, float)):
        try:
            return _from_epoch(value).date()
        except ValueError as exc:
            raise ValueError(f"{kind} cannot read {_describe(value)}: {exc}") from exc
    raise TypeError(
        f"{kind} expects a date, a datetime, an ISO-8601 string or epoch seconds, "
        f"got {type(value).__name__}"
    )


def coerce_temporal_like(reference: Any, value: Any) -> Any:
    """Bring ``value`` to the type of ``reference`` so the two can be compared.

    Used when filtering: the stored value is a ``datetime``/``date`` and the caller may
    pass an ISO string, an epoch number, or a datetime with the other time-zone
    convention. Returns ``value`` unchanged when it cannot be read, so the comparison
    simply does not match.
    """
    try:
        if isinstance(reference, datetime.datetime):
            converted = _to_datetime(value, tz_aware=False, kind="filter")
            if reference.tzinfo is not None:
                return (
                    converted.astimezone(_UTC)
                    if converted.tzinfo is not None
                    else converted.replace(tzinfo=_UTC)
                )
            if converted.tzinfo is not None:
                return converted.astimezone(_UTC).replace(tzinfo=None)
            return converted
        if isinstance(reference, datetime.date):
            return _to_date(value, kind="filter")
    except (TypeError, ValueError):
        pass
    return value


# --- declaration helpers -------------------------------------------------------


def _find_field_name(model_class: type, attrib: Attrib) -> Optional[str]:
    """Find which annotated field on ``model_class`` is bound to ``attrib``."""
    for klass in model_class.__mro__:
        for fname, annotation in getattr(klass, "__annotations__", {}).items():
            if any(meta is attrib for meta in getattr(annotation, "__metadata__", ())):
                return fname
    return None


def _make_current_value_getter(
    config: DTConfig,
) -> Tuple[Callable[[Any], Any], Callable[[Attrib], None]]:
    """Build a reader for the value currently stored on an instance.

    Returns ``(get_current, bind)``; ``bind`` must be called with the field's Attrib.
    The attribute is found by the Attrib it is bound to; ``config.field_name`` is only
    a fallback, so a wrong name there cannot silently break ``auto_now_add``.
    """
    bound: Dict[str, Attrib] = {}
    discovered: Dict[type, Optional[str]] = {}

    def bind(attrib: Attrib) -> None:
        bound["attrib"] = attrib

    def get_current(instance: Any) -> Any:
        if instance is None:
            return None
        cls = type(instance)
        if cls not in discovered:
            discovered[cls] = _find_field_name(cls, bound["attrib"])
        field_name = discovered[cls] or config.field_name
        if field_name is None:
            return None
        try:
            return getattr(instance, field_name)
        except AttributeError:
            # Field not assigned yet (first assignment during __init__)
            return None

    return get_current, bind


def parse_params(params: Optional[Tuple[Any]], *, field_type) -> DTConfig:
    """Process parameters for DateTimeField / DateField."""
    if params is None:
        raise TypeError(f"{field_type}[...] expects a DTConfig instance")
    elif isinstance(params, DTConfig):
        return params
    elif (
        isinstance(params, tuple)
        and len(params) == 1
        and isinstance(params[0], DTConfig)
    ):
        return params[0]
    else:
        raise TypeError(
            f"{field_type}[...] expects a DTConfig instance, got {type(params).__name__}"
        )


def _validators(config: DTConfig) -> list:
    if config.auto_now or config.auto_now_add or config.nullable:
        return []
    return [formax_null_validator]


def _optional_kwargs(config: DTConfig) -> Dict[str, Any]:
    """Auto and nullable fields may be left out when a record is created. The
    pre-formatter still runs on the default, which is how auto fields get stamped."""
    if config.auto_now or config.auto_now_add or config.nullable:
        return {"default": None}
    return {}


class DateTimeField:
    """Create a MiniAnnotated field definition for a datetime.

    The attribute holds a ``datetime``; it is stored as an ISO-8601 string.

    Args:
        config: A DTConfig describing the field's behaviour.

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Workflow(BaseModel):
        ...     created_at: DateTimeField[DTConfig(auto_now_add=True)]
        ...     updated_at: DateTimeField[DTConfig(auto_now=True)]
        ...     scheduled_for: DateTimeField[DTConfig(tz_aware=False, nullable=True)]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError(f"Cannot subclass DateTimeField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("DateTimeField cannot be instantiated")

    def __class_getitem__(cls, params) -> MiniAnnotated:
        config = parse_params(params, field_type=cls.__name__)

        if config.auto_now and config.auto_now_add:
            raise ValueError(
                "A DateTimeField cannot be both auto_now and auto_now_add."
            )

        get_current, bind = _make_current_value_getter(config)

        def convert(value: Any) -> datetime.datetime:
            return _to_datetime(value, tz_aware=config.tz_aware, kind="DateTimeField")

        def pre_fmt(instance, value):
            """Normalize an assigned value to a ``datetime`` (or None)."""
            if value is None:
                if config.auto_now:
                    value = _utc_now()
                elif config.auto_now_add:
                    # Keep the value from creation; only generate one if unset
                    value = get_current(instance)
                    if value is None:
                        value = _utc_now()
                else:
                    return None
            return convert(value)

        def to_state(instance, value):
            """The datetime as an ISO-8601 string, for the store."""
            if value is None:
                return None
            return convert(value).isoformat()

        def from_state(instance, raw):
            """Read a stored value back to a ``datetime``. Lenient on purpose: a
            store may hand back ISO strings, epoch numbers or datetimes."""
            if raw is None:
                return None
            return convert(raw)

        attrib = Attrib(
            pre_formatter=pre_fmt,
            # formax does not call this; kept as the "stored -> value" reader for
            # callers that use the formatter directly.
            post_formatter=from_state,
            validators=_validators(config),
            metadata={
                "type": "datetime",
                "auto_now": config.auto_now,
                "auto_now_add": config.auto_now_add,
                "tz_aware": config.tz_aware,
                "nullable": config.nullable,
                "to_state": to_state,
                "from_state": from_state,
            },
            **_optional_kwargs(config),
        )
        bind(attrib)
        return MiniAnnotated[Any, attrib]


class DateField:
    """Create a MiniAnnotated field definition for a strict date (no time).

    The attribute holds a ``date``; it is stored as an ISO-8601 date string.

    Args:
        config: A DTConfig describing the field's behaviour.

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Invoice(BaseModel):
        ...     issue_date: DateField[DTConfig(auto_now_add=True)]
        ...     due_date: DateField[DTConfig(nullable=True)]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError(f"Cannot subclass DateField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("DateField cannot be instantiated")

    def __class_getitem__(cls, params) -> MiniAnnotated:
        config = parse_params(params, field_type=cls.__name__)

        if config.auto_now and config.auto_now_add:
            raise ValueError("A DateField cannot be both auto_now and auto_now_add.")

        get_current, bind = _make_current_value_getter(config)

        def convert(value: Any) -> datetime.date:
            return _to_date(value, kind="DateField")

        def pre_fmt(instance, value):
            """Normalize an assigned value to a ``date`` (or None)."""
            if value is None:
                if config.auto_now:
                    value = _utc_today()
                elif config.auto_now_add:
                    value = get_current(instance)
                    if value is None:
                        value = _utc_today()
                else:
                    return None
            return convert(value)

        def to_state(instance, value):
            if value is None:
                return None
            return convert(value).isoformat()

        def from_state(instance, raw):
            if raw is None:
                return None
            return convert(raw)

        attrib = Attrib(
            pre_formatter=pre_fmt,
            post_formatter=from_state,
            validators=_validators(config),
            metadata={
                "type": "date",
                "auto_now": config.auto_now,
                "auto_now_add": config.auto_now_add,
                "nullable": config.nullable,
                "to_state": to_state,
                "from_state": from_state,
            },
            **_optional_kwargs(config),
        )
        bind(attrib)
        return MiniAnnotated[Any, attrib]
