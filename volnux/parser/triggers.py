"""
What a ``@trigger:`` declaration may say.

One table, used twice: semantic analysis checks the literal parts of a declaration
at parse time, and the engine builds the trigger objects from the same names at run
time. Pure data and functions: nothing here imports the engine.
"""

import re
import typing
from dataclasses import dataclass, field

__all__ = [
    "CONDITION_FUNCTIONS",
    "TRIGGER_SPECS",
    "TriggerSpec",
    "check_cron",
    "check_timezone",
    "split_cron",
]

_CRON_FIELD = re.compile(r"^[0-9A-Za-z*/,\-?]+$")
_CRON_FIELD_NAMES = ("minute", "hour", "day_of_month", "month_of_year", "day_of_week")


@dataclass(frozen=True)
class TriggerSpec:
    """Shape of one trigger kind.

    :ivar positional: Types of the positional arguments, in order (all required).
    :ivar variadic: Name and type of further positional arguments after those (at
        least one is required), e.g. the event types of ``event("a", "b")``.
    :ivar keywords: Keyword name -> accepted Python types of its literal value.
    """

    positional: typing.Tuple[typing.Tuple[str, type], ...] = ()
    variadic: typing.Optional[typing.Tuple[str, type]] = None
    keywords: typing.Mapping[str, typing.Tuple[type, ...]] = field(default_factory=dict)


TRIGGER_SPECS: typing.Dict[str, TriggerSpec] = {
    "schedule": TriggerSpec(positional=(("cron", str),), keywords={"timezone": (str,)}),
    "webhook": TriggerSpec(
        positional=(("path", str),),
        keywords={
            "secret": (str,),
            "token_header": (str,),
            "timestamp_header": (str,),
            "tolerance_seconds": (int, float),
        },
    ),
    # ``event("order.created", "order.updated", pattern={"status": "paid"})``;
    # an event type of "*" accepts every type.
    "event": TriggerSpec(
        variadic=("event_type", str),
        keywords={"pattern": (dict,), "name": (str,)},
    ),
    # ``when <condition>``: the condition is not an argument.
    "when": TriggerSpec(),
}

#: Condition functions usable as ``when name(args)``: name -> number of arguments.
#: (The engine supplies the implementations; more can be registered there.)
CONDITION_FUNCTIONS: typing.Dict[str, int] = {"file_exists": 1}


def split_cron(expression: str) -> typing.Dict[str, str]:
    """``"0 6 * * MON-FRI"`` -> the five ``crontab`` keyword arguments.

    :raises ValueError: not five fields, or a field with characters a cron field
        never has.
    """
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"a cron expression has 5 fields (minute hour day-of-month month "
            f"day-of-week), got {len(fields)}: {expression!r}"
        )
    for name, value in zip(_CRON_FIELD_NAMES, fields):
        if not _CRON_FIELD.match(value):
            raise ValueError(f"invalid cron {name} field {value!r}")
    return dict(zip(_CRON_FIELD_NAMES, fields))


def check_cron(expression: str) -> typing.Optional[str]:
    """Why ``expression`` is not a cron expression, or None if it is."""
    try:
        split_cron(expression)
    except ValueError as exc:
        return str(exc)
    return None


def check_timezone(name: str) -> typing.Optional[str]:
    """Why ``name`` is not a known IANA time zone, or None if it is."""
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(name)
    except Exception:
        return f"unknown time zone {name!r}"
    return None
