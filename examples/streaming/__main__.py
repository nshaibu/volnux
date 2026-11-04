"""Streaming with ``|->``: a producer, a transform stage and a sink run together.

    python -m streaming

Uses the in-memory backend (the default), so everything runs in one process. With
Redis or Kafka configured the same workflow streams across processes and machines.
"""

import asyncio

from volnux import EventBase, Pipeline


class Numbers(EventBase):
    """A generator ``process()`` streams: each ``yield`` is one chunk."""

    async def process(self):
        for i in range(1, 6):
            await asyncio.sleep(0.1)
            print(f"produced {i}")
            yield True, i


class Double(EventBase):
    """Reads the upstream stream and streams its own output."""

    async def process(self):
        async for chunk in self.input_stream():
            yield True, chunk.content * 2


class Collect(EventBase):
    """The end of the stream: consumes everything, returns one result."""

    async def process(self):
        values = []
        async for chunk in self.input_stream():
            print(f"  collected {chunk.content}")
            values.append(chunk.content)
        return True, values


class Evens(EventBase):
    async def process(self):
        for i in (2, 4, 6):
            await asyncio.sleep(0.12)
            yield True, f"even {i}"


class Odds(EventBase):
    async def process(self):
        for i in (1, 3, 5):
            await asyncio.sleep(0.1)
            yield True, f"odd {i}"


class Merge(EventBase):
    """Fan-in: reads one merged stream from every producer."""

    async def process(self):
        async for chunk in self.input_stream():
            print(f"  merged {chunk.content} (from {chunk.event_name})")
        return True, "merged"


class StreamingPipeline(Pipeline):
    class Meta:
        pointy = "Numbers |-> Double |-> Collect"


class FanInPipeline(Pipeline):
    class Meta:
        # two producers feed one consumer
        pointy = "(Evens || Odds) |-> Merge"


class CooperativePipeline(Pipeline):
    class Meta:
        # cooperative group: its chains run together, and Merge reads their tails
        pointy = "{ Evens, Odds } |-> Merge"


if __name__ == "__main__":
    print("-- a stream chain")
    asyncio.run(StreamingPipeline().start(force_rerun=True))
    print("-- fan-in: (Evens || Odds) |-> Merge")
    asyncio.run(FanInPipeline().start(force_rerun=True))
    print("-- a cooperative group: { Evens, Odds } |-> Merge")
    asyncio.run(CooperativePipeline().start(force_rerun=True))
