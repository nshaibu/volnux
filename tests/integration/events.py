"""Event classes at module level: a process pool pickles an event by reference."""

import os

from volnux import EventBase


class IntegrationProcessEcho(EventBase):
    executor = "process"

    async def process(self, *args, **kwargs):
        return True, os.getpid()


class ParallelLeft(EventBase):
    async def process(self, *args, **kwargs):
        return True, ("left", os.getpid())


class ParallelRight(EventBase):
    async def process(self, *args, **kwargs):
        return True, ("right", os.getpid())


class ParallelJoin(EventBase):
    async def process(self, *args, **kwargs):
        return True, sorted(str(r.content[0]) for r in self.previous_result)
