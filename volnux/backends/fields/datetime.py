import datetime
from dataclasses import dataclass
from typing import Any, Tuple, Optional
from formax import MiniAnnotated, Attrib

from .utils import formax_null_validator


@dataclass
class DTConfig:
    """Configuration for DateTimeField.

    Args:
        field_name: Name of the field. Must match the name of the attribute in the model.
        auto_now: Automatically set the field to now every time the object is saved.
        auto_now_add: Automatically set the field to now when the object is first created.
        tz_aware: Enforce timezone-aware datetime objects (defaults to UTC if naive).
        nullable: Whether the datetime field can be None.
    """

    field_name: str
    auto_now: bool = False
    auto_now_add: bool = False
    tz_aware: bool = True
    nullable: bool = False


def parse_params(params: Optional[Tuple[Any]], *, field_type) -> DTConfig:
    """Process parameters for DateTimeField.

    This function can be used to customize the behavior of DateTimeField.
    """
    if params is None:
        # return DTConfig()
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


class DateTimeField:
    """Create a MiniAnnotated field definition for a datetime.

    Args:
        config: Optional DTConfig for datetime behavior.

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Workflow(BaseModel):
        ...     created_at: DateTimeField[DTConfig("created_at", auto_now_add=True)]
        ...     updated_at: DateTimeField[DTConfig("updated_at", auto_now=True)]
        ...     scheduled_for: DateTimeField[DTConfig("scheduled_for", tz_aware=False, nullable=True)]
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

        validators = []
        if not (config.auto_now or config.auto_now_add):
            if not config.nullable:
                validators.append(formax_null_validator)

        def pre_fmt(instance, value):
            """Serializes datetime to strict JSON primitive (ISO 8601 string)."""
            if value is None:
                if config.auto_now:
                    value = datetime.datetime.now()
                elif config.auto_now_add:
                    # check that the field is set on the instance if not set generate a new date
                    value: Optional[datetime.datetime] = getattr(
                        instance, config.field_name
                    )
                    if value is None:
                        value = datetime.datetime.now()
                    elif value.tzinfo is not None:
                        value = value.replace(tzinfo=None)
                else:
                    return None

            if not isinstance(value, datetime.datetime):
                raise TypeError(
                    f"Expected datetime.datetime instance, got {type(value).__name__}"
                )

            if config.tz_aware and value.tzinfo is None:
                # Auto-assign UTC if tz_aware is True, but a naive datetime is provided
                value = value.replace(tzinfo=datetime.timezone.utc)

            # convert to ISO 8601 string
            return value.isoformat()

        def post_fmt(instance, value):
            """Deserializes JSON primitive (ISO string or Unix timestamp) back to datetime."""
            if value is None:
                return None

            if isinstance(value, str):
                try:
                    dt = datetime.datetime.fromisoformat(value)
                except ValueError:
                    # Fallback for specific legacy formats if necessary
                    dt = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            elif isinstance(value, (int, float)):
                # Support Unix timestamps
                dt = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
            else:
                raise TypeError(
                    f"Cannot deserialize datetime from {type(value).__name__}"
                )

            # Ensure timezone consistency upon rehydration
            if config.tz_aware and dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)

            return dt

        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                post_formatter=post_fmt,
                validators=validators,
                metadata={
                    "type": "datetime",
                    "auto_now": config.auto_now,
                    "auto_now_add": config.auto_now_add,
                    "tz_aware": config.tz_aware,
                    "nullable": config.nullable,
                },
            ),
        ]


class DateField:
    """Create a MiniAnnotated field definition for a strict date (no time).

    Args:
        config: Optional DateConfig for date behavior.

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Invoice(BaseModel):
        ...     issue_date: DateField[DTConfig("issue_date", auto_now_add=True)]
        ...     due_date: DateField[DTConfig("due_date", nullable=True)]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError(f"Cannot subclass DateField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("DateField cannot be instantiated")

    def __class_getitem__(cls, params) -> MiniAnnotated:
        config = parse_params(params, field_type=cls.__name__)

        if config.auto_now and config.auto_now_add:
            raise ValueError("A DateField cannot be both auto_now and auto_now_add.")

        validators = []

        if not (config.auto_now or config.auto_now_add):
            if not config.nullable:
                validators.append(formax_null_validator)

        def pre_fmt(instance, value):
            """Serializes date to strict JSON primitive (ISO 8601 date string)."""
            if value is None:
                if config.auto_now:
                    value = datetime.date.today()
                elif config.auto_now_add:
                    # check that the field is set on the instance if not set generate a new date
                    value = getattr(instance, config.field_name)
                    if value is None:
                        value = datetime.date.today()
                else:
                    return None

            if isinstance(value, datetime.datetime):
                value = value.date()

            if not isinstance(value, datetime.date):
                raise TypeError(
                    f"Expected datetime.date instance, got {type(value).__name__}"
                )

            # convert to YYYY-MM-DD string
            return value.isoformat()

        def post_fmt(instance, value):
            """Deserializes JSON primitive (ISO date string) back to datetime.date."""
            if value is None:
                return None

            if isinstance(value, str):
                try:
                    # Strict ISO 8601 date parsing (YYYY-MM-DD)
                    return datetime.date.fromisoformat(value)
                except ValueError as e:
                    raise ValueError(
                        f"Invalid date format '{value}'. Expected ISO 8601 (YYYY-MM-DD)."
                    ) from e
            else:
                raise TypeError(
                    f"Cannot deserialize date from {type(value).__name__}. Expected string."
                )

        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                post_formatter=post_fmt,
                validators=validators,
                metadata={
                    "type": "date",
                    "auto_now": config.auto_now,
                    "auto_now_add": config.auto_now_add,
                    "nullable": config.nullable,
                },
            ),
        ]
