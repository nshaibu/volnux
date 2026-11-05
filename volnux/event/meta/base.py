"""Meta events: stream transformers that apply a template to every item of the input.

``MAP<Double>``, ``FILTER<IsBig>``, ``REDUCE<Add>`` ... are mini-orchestrators. Each is
a *streaming* event (its ``process`` is an async generator, see ``StreamingMixin``): it
reads its input as a stream, applies the template to the items with bounded
concurrency, and writes the results to a stream of its own. Nothing is held in memory
beyond the items in flight, so they suit large data, and everything the streaming layer
gives (ordering, backpressure, resume, ``fail_fast``) applies.

Input
-----
* After ``|->``: the upstream stream, as it is produced.
* After ``->`` (or first in a chain): the ``collection`` option, else the contents of
  ``previous_result``, an iterable content being taken item by item.

Output
------
A stream. A ``|->`` successor reads the items live; a ``->`` successor receives the
usual stream reference (with a summary).

Template
--------
An event class, written ``MAP<Double>`` in pointy (it is given the input as
``previous_result``), or, from Python, a plain or async function. A subclass may name
one with ``template_class = ...``; ``options.extras["template_class"]`` (what pointy
produces) wins.
"""

import asyncio
import inspect
import logging
import typing

from volnux.constants import EMPTY
from volnux.event.utils import resolve_event_ref_to_class
from volnux.exceptions import (
    MetaEventConfigurationError,
    NestedMetaEventError,
    StopProcessingError,
)
from volnux.mixins.stream.mixin import StreamPolicy
from volnux.signal.handlers.event_initialiser import EventInitKwargs
from volnux.typing import TypeAlias
from volnux.result import EventResult
from ..base import EventBase, EventType
from .runner import Outcome, TemplateCaller, run_concurrently

logger = logging.getLogger(__name__)

AttributesKwargs: TypeAlias = EventInitKwargs


class MetaAttributes(typing.TypedDict, total=False):
    """
    Typed dictionary for meta-attributes configuration.
    """

    collection: typing.List[typing.Any]
    keep_on_error: bool
    max_workers: int
    count: int
    batch_size: int
    flatten_depth: int
    direction: str
    skip_errors: bool
    initial_value: object
    continue_on_error: bool
    concurrent: bool
    concurrency_mode: typing.Literal["thread", "process", "async"]
    timeout: typing.Optional[float]
    partial_success: bool
    ordered: bool
    buffer: typing.Optional[int]

    # filter
    invert: bool


def is_control_flow_event(
    event_class: typing.Union[EventBase, typing.Type[EventBase]],
) -> bool:
    """
    Check if an event class is a ControlFlowEvent (Meta Event)
    """
    return (
        event_class.__name__ == "ControlFlowEvent"
        or event_class.event_type == EventType.META
    )


def validate_meta_event(
    nested_event_class: typing.Union[str, typing.Type[EventBase]],
    field_name: str,
    meta_event: "ControlFlowEvent",
) -> None:
    """
    Reject a meta event used as a template.

    Raises:
        NestedMetaEventError: If template is a meta event
        ValueError: If template event doesn't exist
    """
    resolved_event_class = resolve_event_ref_to_class(nested_event_class)

    if is_control_flow_event(resolved_event_class):
        raise NestedMetaEventError(
            f"Meta Events cannot be used as Template Events. "
            f"Found: {meta_event.name}<{nested_event_class.__name__}> "
            f"Template Event '{field_name}' is a Meta Event, which is not allowed. "
            f"Use explicit sequential composition instead."
        )


def _is_collection(content: typing.Any) -> bool:
    """An iterable that is a collection of items (a string or mapping is one item)."""
    if isinstance(content, (str, bytes, bytearray, dict)):
        return False
    return hasattr(content, "__iter__") or hasattr(content, "__aiter__")


class ControlFlowEvent(EventBase):
    """
    Base class for Meta-Events: reserved system components that apply a template to
    the items of a stream (see the module docstring).

    A subclass declares ``name`` and ``attributes`` and implements
    :meth:`transform`, an async generator of ``(success, data)`` pairs.
    """

    # Name of the Meta Event
    name: str = "CONTROL_FLOW"

    #: The template: an event class, an event name, or a callable (see module docs).
    template_class: typing.Any = None

    #: A meta event writes many small chunks; keep the default policy, but let the
    #: producer run ahead of a slow consumer by a bounded amount.
    stream_policy = StreamPolicy(max_in_flight=1000)

    # Attributes for the Meta Events. This will be used to fetch data
    # from the Options specific to Meta Events
    attributes: typing.Dict[str, AttributesKwargs] = {
        "concurrent": {
            "type": bool,
            "default": True,
            "description": "Apply the template to several items at once",
        },
        "concurrency_mode": {
            "type": str,
            "default": "thread",
            "description": (
                "Where the template runs: 'thread' / 'process' (shared pools) or "
                "'async' (on the event loop, for non-blocking templates)"
            ),
            "choices": ["thread", "process", "async"],
        },
        "max_workers": {
            "type": int,
            "default": 4,
            "description": "Max parallel executions",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
        "ordered": {
            "type": bool,
            "default": True,
            "description": "Emit results in input order (False: as they finish)",
        },
        "buffer": {
            "type": int,
            "default": None,
            "description": (
                "Items in flight beyond max_workers (default: max_workers). "
                "Bounds memory and how far the input is read ahead."
            ),
            "validators": [lambda x: isinstance(x, int) and x >= 0],
        },
        "timeout": {
            "type": (int, float),
            "default": None,
            "description": "Seconds one template call may take",
            "validators": [lambda x: x > 0],
        },
        "collection": {
            "type": (list, tuple, range),
            "default": None,
            "description": "Input items, when they do not arrive from a predecessor",
        },
    }

    event_type = EventType.META

    # --- configuration ----------------------------------------------------

    def _extras(self) -> typing.Dict[str, typing.Any]:
        options = getattr(self, "options", None)
        return getattr(options, "extras", None) or {}

    def _validate_attributes(self) -> MetaAttributes:
        """The meta attributes from the options, defaults filled in and checked.

        Raises:
            TypeError: if a value has the wrong type, is not an allowed choice, fails
                its validator, or a required one is missing.
        """
        extras = self._extras()
        name = self.__class__.__name__
        validated: MetaAttributes = {}
        for param_name, config in self.attributes.items():
            default = config.get("default", EMPTY)
            value = extras.get(param_name, default)

            if value is EMPTY:
                if config.get("required", False):
                    raise TypeError(
                        f"Event '{name}' requires the option '{param_name}'."
                    )
                continue
            if value is None:
                validated[param_name] = None  # type: ignore[literal-required]
                continue

            data_type = config.get("type", object)
            if not isinstance(value, data_type):
                raise TypeError(
                    f"Event '{name}' requires type '{data_type}' for '{param_name}', "
                    f"but received type '{type(value)}' (value: {value})."
                )
            choices = config.get("choices")
            if choices is not None and value not in choices:
                raise TypeError(
                    f"Invalid value '{value}' for '{param_name}' in event '{name}'. "
                    f"Allowed values: {choices}"
                )
            for validator in config.get("validators", []):
                if not validator(value):
                    raise TypeError(
                        f"Validation failed for parameter '{param_name}' "
                        f"in event '{name}' with value: {value}"
                    )
            validated[param_name] = value  # type: ignore[literal-required]
        return validated

    def get_template_class(self) -> typing.Any:
        """The template as configured (not yet resolved)."""
        extras = self._extras()
        template = extras.get("template_class")
        if template is None:
            # not ``self.template_class``: a plain function there would be bound as a method
            declared = inspect.getattr_static(type(self), "template_class", None)
            return declared.__func__ if isinstance(declared, staticmethod) else declared
        namespace = extras.get("template_namespace")
        if isinstance(template, str) and namespace and "::" not in template:
            return f"{namespace}::{template}"
        return template

    def resolve_template(self) -> typing.Any:
        """The template as something callable: an event class or a function."""
        template = self.get_template_class()
        if template is None:
            raise StopProcessingError(
                f"{self.name}: no template. Write {self.name}<Event> in pointy, or "
                "set template_class."
            )
        if isinstance(template, str) or (
            inspect.isclass(template) and issubclass(template, EventBase)
        ):
            try:
                template = resolve_event_ref_to_class(template)
            except (ValueError, TypeError) as exc:
                raise StopProcessingError(
                    f"{self.name}: template '{template}' could not be resolved: {exc}"
                ) from exc
            if is_control_flow_event(template):
                raise NestedMetaEventError(
                    f"Meta Events cannot be used as Template Events: {self.name}"
                    f"<{template.__name__}>. Use explicit sequential composition."
                )
            return template
        if callable(template):
            return template
        raise MetaEventConfigurationError(
            f"{self.name}: the template must be an event class, an event name or a "
            f"callable, got {type(template).__name__}"
        )

    # --- input ---------------------------------------------------------------

    def _reads_a_stream(self) -> bool:
        return bool(
            self._upstream_stream_ids
            or self._upstream_stream_id
            or self._upstream_refs()
        )

    async def items(
        self, collection: typing.Optional[typing.Iterable[typing.Any]] = None
    ) -> typing.AsyncIterator[typing.Any]:
        """The input, one item at a time, without materialising it."""
        if self._reads_a_stream():
            async for item in self.input_data():
                yield item
            return
        if collection:
            for item in collection:
                yield item
            return
        previous = getattr(self, "previous_result", None)
        if previous is None or previous is EMPTY:
            return
        # one result, or a collection of results (a list, a ResultSet ...)
        results = [previous] if isinstance(previous, EventResult) else previous
        if not hasattr(results, "__iter__"):
            results = [results]
        for result in results:
            content = getattr(result, "content", result)
            if hasattr(content, "__aiter__"):
                async for item in content:
                    yield item
            elif _is_collection(content):
                for item in content:
                    yield item
            else:
                yield content

    @staticmethod
    async def batched(
        items: typing.AsyncIterator[typing.Any], size: int
    ) -> typing.AsyncIterator[typing.Any]:
        """Items as lists of ``size`` (the last may be shorter); unchanged for 0/1."""
        if size <= 1:
            async for item in items:
                yield item
            return
        batch: typing.List[typing.Any] = []
        async for item in items:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    # --- the work ------------------------------------------------------------

    def outcomes(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
        *,
        workers: typing.Optional[int] = None,
    ) -> typing.AsyncIterator[Outcome]:
        """Apply the template to ``items`` with the configured concurrency."""
        if workers is None:
            workers = (
                attributes.get("max_workers", 4)
                if attributes.get("concurrent", True)
                else 1
            )
        return run_concurrently(
            items,
            caller.call,
            workers=workers,
            ordered=attributes.get("ordered", True),
            buffer=attributes.get("buffer"),
        )

    async def process(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.AsyncGenerator[typing.Tuple[bool, typing.Any], None]:
        """Stream the transformed items. Subclasses implement :meth:`transform`."""
        attributes = self._validate_attributes()
        template = self.resolve_template()
        caller = TemplateCaller(
            template,
            owner=self,
            mode=attributes.get("concurrency_mode", "thread"),
            timeout=attributes.get("timeout"),
        )
        items = self.items(attributes.get("collection"))
        async for out in self.transform(items, caller, attributes):
            yield out

    def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        """Async generator of ``(success, data)``: the operator's logic."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement transform()"
        )
