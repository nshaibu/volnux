"""Fixtures for the EventBase / mixin tests.

Nothing here patches global state: events get a mock execution context, a mock
checkpoint manager and an in-memory command channel, and signal emissions are
recorded through ordinary listeners.
"""

import itertools
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux import EventBase
from volnux.signal import signals

_names = itertools.count()


def unique_name(prefix: str = "TestEvent") -> str:
    # EventMeta registers every subclass; unique names avoid duplicate-registration warnings
    return "%s%d" % (prefix, next(_names))


async def _ok(self, *args, **kwargs):
    return True, "ok"


def make_event_class(base: type = EventBase, **attrs: Any) -> type:
    """Build a concrete event class. ``process`` defaults to ``(True, "ok")``."""
    attrs.setdefault("process", _ok)
    return type(unique_name(), (base,), attrs)


class FakeChannel:
    """In-memory stand-in for a CommandChannelBase."""

    poll_interval = 0.001

    def __init__(self) -> None:
        self.sent: List[Any] = []
        self.commands: List[Any] = []
        self.send_message = AsyncMock(side_effect=self._record)
        self.receive_command = AsyncMock(side_effect=self._next_command)

    async def _record(self, message: Any) -> None:
        self.sent.append(message)

    async def _next_command(self, timeout: Optional[float] = None) -> Any:
        if self.commands:
            return self.commands.pop(0)
        import asyncio

        await asyncio.sleep(0.001)
        raise asyncio.TimeoutError()

    @property
    def states(self) -> List[Any]:
        return [m.payload.get("state") for m in self.sent if m.payload]


@pytest.fixture
def make_event() -> Callable[..., type]:
    return make_event_class


@pytest.fixture
def ctx() -> MagicMock:
    context = MagicMock(name="execution_context")
    context.state_id = "ctx-1"
    return context


@pytest.fixture
def checkpoint_manager() -> MagicMock:
    return MagicMock(name="checkpoint_manager")


@pytest.fixture
def channel() -> FakeChannel:
    return FakeChannel()


@pytest.fixture
def build(ctx: MagicMock) -> Callable[..., EventBase]:
    """``build(EventClass_or_attrs, **init_kwargs)`` -> event instance."""

    def factory(event_class: Any = None, **init_kwargs: Any) -> EventBase:
        if event_class is None:
            event_class = make_event_class()
        elif isinstance(event_class, dict):
            event_class = make_event_class(**event_class)
        init_kwargs.setdefault("execution_context", ctx)
        init_kwargs.setdefault("task_id", "task-1")
        return event_class(**init_kwargs)

    return factory


@pytest.fixture
def recorded() -> Dict[str, List[Dict[str, Any]]]:
    """Record emissions of the lifecycle signals the event system fires."""
    names = [
        "event_phase_changed",
        "event_called",
        "event_execution_retry",
        "event_execution_retry_done",
        "event_init",
    ]
    log: Dict[str, List[Dict[str, Any]]] = {n: [] for n in names}
    listeners = []

    for name in names:

        def make(name: str) -> Callable[..., None]:
            def listener(sender: Any = None, signal: Any = None, **kwargs: Any) -> None:
                log[name].append(kwargs)

            return listener

        fn = make(name)
        listeners.append(fn)  # keep alive: signals hold listeners weakly
        getattr(signals, name).connect(None, fn)

    yield log

    for name, fn in zip(names, listeners):
        getattr(signals, name).disconnect(None, fn)
