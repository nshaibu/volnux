import asyncio
from typing import Optional, TYPE_CHECKING, cast, Type

from .base import ResourceProvider
from volnux.import_utils import import_string as import_class

if TYPE_CHECKING:
    from volnux.event import EventBase


class ResourceMonitor:
    """
    Periodically captures the state of registered resources and updates
    the event's in-memory _external_resources dict.

    Does NOT persist to storage — that's the Checkpoint Manager's job
    when it drains the checkpoint queue. This only keeps the in-memory
    state current so that when a checkpoint is taken, the latest
    resource state is included.
    """

    def __init__(self, interval: float = 5.0):
        self.interval = interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def _monitor_loop(self, event: "EventBase") -> None:
        while self._running:
            for name, entry in event._external_resources.items():
                resource = event._resource_instances.get(name)
                if resource is None:
                    continue

                provider_path = entry["provider_path"]
                try:
                    provider = cast(Type[ResourceProvider], import_class(provider_path))
                except ImportError:
                    continue

                event._external_resources[name] = {
                    "resource_name": name,
                    "data": provider.save_state(resource),
                    "provider_path": provider_path,
                }
            await asyncio.sleep(self.interval)

    def is_started(self) -> bool:
        return self._running

    async def start(self, event: "EventBase") -> None:
        self._running = True
        self._task = asyncio.create_task(
            self._monitor_loop(event), name="ResourceMonitor"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
