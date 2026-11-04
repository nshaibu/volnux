import asyncio
import dataclasses
import inspect
from typing import Any, Dict, Iterable, List, Optional

from .deserializer import StateDeserializer
from .event_result_serializer import EventResultRef
from .snapshot import EventCheckpointSnapshot
from volnux.import_utils import import_string as import_class
from volnux.result import EventResult, ResultSet


class EventRehydrator:
    """Reconstructs an event from an EventCheckpointSnapshot.

    Uses StateDeserializer for all deserialization. The rehydrator
    orchestrates the restoration process but delegates data conversion
    to the deserializer.
    """

    def __init__(self, deserializer: StateDeserializer = StateDeserializer):
        self.deserializer = deserializer

    async def rehydrate(
        self,
        snapshot: EventCheckpointSnapshot,
        checkpoint_manager: Optional[Any] = None,
        command_channel: Optional[Any] = None,
        previous_results: Optional[Iterable[EventResult]] = None,
    ) -> "EventBase":
        """Reconstruct an event from its checkpoint snapshot.

        ``checkpoint_manager`` and ``command_channel`` are live collaborators
        that are never part of a snapshot; pass them to give the rehydrated
        event the same checkpointing and control-channel wiring as the original.

        ``previous_results`` are added to the event's ``previous_result`` set.
        This is how the outcome of what a suspended task was waiting for (a human
        decision, an external event) reaches it: ``communicate()`` suspends
        rather than returns, and the response arrives here.
        """

        event_class = import_class(snapshot.class_path)

        init_kwargs = self.deserializer.deserialize_init_args(snapshot.init_args)
        init_kwargs = await self._resolve_awaitables(init_kwargs)

        results = await self._load_results(init_kwargs.pop("previous_result", None))
        results.extend(previous_results or ())
        if results:
            init_kwargs["previous_result"] = ResultSet(results)

        # The serialised form carries more keys (e.g. free-form ``kwargs``) than
        # ``EventBase.__init__`` accepts.
        accepted = inspect.signature(event_class.__init__).parameters
        init_kwargs = {k: v for k, v in init_kwargs.items() if k in accepted}
        if checkpoint_manager is not None:
            init_kwargs["checkpoint_manager"] = checkpoint_manager
        if command_channel is not None:
            init_kwargs["command_channel"] = command_channel
        event = event_class(**init_kwargs)

        event._phase = snapshot.phase
        event._retry_count = snapshot.retry_count
        event.exec_status = snapshot.exec_status

        if snapshot.exec_result is not None:
            event.exec_result = await self._resolve_references(
                self.deserializer.deserialize_exec_result(snapshot.exec_result)
            )

        if snapshot.external_resources:
            # Kept as saved ``ResourceState`` entries: ``acquire_resource()``
            # restores each live resource on demand from its saved state.
            event._external_resources = dict(snapshot.external_resources)

        call_args = await self._resolve_references(
            self.deserializer.deserialize_call_args(snapshot.call_args)
        )
        # The live shape: positional arguments apart, keyword arguments as a dict
        event._call_positional = tuple(call_args["args"])
        event._call_args = call_args["kwargs"]

        if getattr(event, "retry_policy", None):
            # Copy: ``retry_policy`` may be the class-level policy shared by
            # every instance of this event.
            event.retry_policy = dataclasses.replace(
                event.retry_policy, max_attempts=snapshot.max_retry_attempts
            )

        # Restore user-bound serializable attributes
        if snapshot.attribs:
            for key, value in snapshot.attribs.items():
                setattr(event, key, value)

        return event

    @staticmethod
    async def _resolve_awaitables(values: Dict[str, Any]) -> Dict[str, Any]:
        """Await values the sync deserializer could only hand back as awaitables
        (``ExecutionContext.get`` is a coroutine)."""
        resolved = {}
        for key, value in values.items():
            resolved[key] = await value if inspect.isawaitable(value) else value
        return resolved

    @staticmethod
    async def _load_results(result_ids: Optional[List[str]]) -> List[EventResult]:
        """Load the persisted results a snapshot references by id."""
        if not result_ids:
            return []
        return list(await asyncio.gather(*(EventResult.get(i) for i in result_ids)))

    @classmethod
    async def _resolve_references(cls, value: Any) -> Any:
        """Replace every ``EventResultRef`` in ``value`` by the stored result."""
        if isinstance(value, EventResultRef):
            return await import_class(value.class_path).get(value.id)
        if isinstance(value, dict):
            return {k: await cls._resolve_references(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            items = [await cls._resolve_references(v) for v in value]
            return type(value)(items)
        return value
