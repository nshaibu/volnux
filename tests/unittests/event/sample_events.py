"""Importable event classes (a checkpoint stores the class by dotted path)."""

from volnux import EventBase
from volnux.mixins.event.retry import RetryPolicy


class RoundTripEvent(EventBase):
    retry_policy = RetryPolicy(max_attempts=3)

    async def process(self, *args, **kwargs):
        return True, "ok"


class SeenArgsEvent(EventBase):
    async def process(self, *args, **kwargs):
        return True, "ok"
