"""Events used by the process-pool tests.

They live in an importable module (not inside a test function) because a worker
process must be able to import the event class to unpickle the event.
"""

from volnux import EventBase


class ProcessEcho(EventBase):
    executor = "process"

    async def process(self, *args, **kwargs):
        import os

        return True, f"ran-in-pid-{os.getpid()}"


class ProcessSyncEcho(EventBase):
    executor = "process"

    def process(self, *args, **kwargs):
        import os

        return True, f"sync-in-pid-{os.getpid()}"


class ParA(EventBase):
    async def process(self, *args, **kwargs):
        return True, "a"


class ParB(EventBase):
    async def process(self, *args, **kwargs):
        return True, "b"


class ParC(EventBase):
    async def process(self, *args, **kwargs):
        return True, "c"


class AfterParallel(EventBase):
    async def process(self, *args, **kwargs):
        return True, "after"


class ProcessReportsChannel(EventBase):
    executor = "process"

    async def process(self, *args, **kwargs):
        channel = self._command_channel
        return True, f"channel={type(channel).__name__ if channel else None}"


# --- events for the Redis-backed control-channel tests -------------------------
import asyncio as _asyncio
import os as _os
from pathlib import Path as _Path


def _marker(name):
    directory = _os.environ.get("VOLNUX_TEST_MARKER_DIR")
    if directory:
        (_Path(directory) / name).write_text("x")


class ProcessSlow(EventBase):
    """Runs far longer than any test waits, unless it is cancelled."""

    executor = "process"

    async def process(self, *args, **kwargs):
        _marker("slow-started")
        await _asyncio.sleep(60)
        _marker("slow-finished")
        return True, "slept"


class ProcessPausable(EventBase):
    executor = "process"

    async def communicate(self, *args, **kwargs):
        _marker("pausable-communicating")
        await _asyncio.sleep(0.8)  # steps remain after this: the pause gate

    async def process(self, *args, **kwargs):
        _marker("pausable-processed")
        return True, "done"


class ProcessTalker(EventBase):
    executor = "process"

    async def process(self, *args, **kwargs):
        channel = self._command_channel
        return True, f"channel={type(channel).__name__ if channel else None}"


class CeleryTalker(EventBase):
    executor = "celery-test"

    async def process(self, *args, **kwargs):
        channel = self._command_channel
        return True, f"channel={type(channel).__name__ if channel else None}"


class CelerySlow(EventBase):
    executor = "celery-test"

    async def process(self, *args, **kwargs):
        _marker("celery-slow-started")
        await _asyncio.sleep(60)
        _marker("celery-slow-finished")
        return True, "slept"
