from typing import TYPE_CHECKING, Any, Dict, List

from .snapshot import EventCheckpointSnapshot
from .serializer import StateSerializer
from volnux.utils import get_obj_klass_import_str
from volnux.constants import MAX_RETRIES
from volnux.result import EventResult, ResultSet

if TYPE_CHECKING:
    from volnux import EventBase


class SnapshotBuilder:
    """
    Builds a snapshot of an event for checkpointing and rehydration.

    This class is responsible for serializing event data into a format suitable
    for checkpointing and rehydration. It uses a StateSerializer to handle
    the serialization of event attributes.
    """

    def __init__(self, serializer=StateSerializer):
        self.serializer = serializer

    async def build(self, event: "EventBase") -> EventCheckpointSnapshot:
        max_attempts: int = (
            event.retry_policy.max_attempts if event.retry_policy else MAX_RETRIES
        )

        # A snapshot references results by id, so every result it mentions
        # must already be in the store.
        await self._persist_referenced_results(event)

        return EventCheckpointSnapshot(
            task_id=event._task_id,
            class_path=get_obj_klass_import_str(event),
            phase=event.get_phase(),
            init_args=self.serializer.serialize_init_args(event.get_init_args()),
            call_args=self.serializer.serialize_call_args(self._call_arguments(event)),
            retry_count=event._retry_count,
            max_retry_attempts=max_attempts,
            exec_result=self.serializer.serialize_exec_result(event.exec_result),
            exec_status=event.exec_status,
            external_resources=event._external_resources,
            attribs=self.serializer.serialize_attribs(event),
        )

    @staticmethod
    def _call_arguments(event: "EventBase") -> Dict[str, Any]:
        """The arguments the event was called with, in the snapshot's shape.

        ``get_call_args()`` holds the keyword arguments (``EMPTY`` before the
        first call); the positional ones are kept separately.
        """
        kwargs = event.get_call_args()
        positional = getattr(event, "_call_positional", ())
        return {
            "args": list(positional) if isinstance(positional, (tuple, list)) else [],
            "kwargs": dict(kwargs) if isinstance(kwargs, dict) else {},
        }

    @classmethod
    def _collect_results(cls, value: Any, found: List[EventResult]) -> None:
        if isinstance(value, EventResult):
            found.append(value)
        elif isinstance(value, dict):
            for item in value.values():
                cls._collect_results(item, found)
        elif isinstance(value, (list, tuple, set, frozenset, ResultSet)):
            for item in value:
                cls._collect_results(item, found)

    async def _persist_referenced_results(self, event: "EventBase") -> None:
        found: List[EventResult] = []
        self._collect_results(getattr(event, "previous_result", None), found)
        self._collect_results(getattr(event, "exec_result", None), found)
        self._collect_results(event.get_call_args(), found)
        self._collect_results(getattr(event, "_call_positional", None), found)

        for result in found:
            if not getattr(result, "is_persisted", False):
                await result.save()

    # async def build(self, event: "EventBase") -> EventCheckpointSnapshot:
    #
    #     return EventCheckpointSnapshot(
    #         task_id=event._task_id,
    #         class_path=get_obj_klass_import_str(event),
    #         phase=event.get_phase(),
    #         init_args=self.serializer.serialize_init_args(event.get_init_args()),
    #         call_args=self.serializer.serialize_call_args(event.get_call_args()),
    #         retry_count=event._retry_count,
    #         max_attempts=(
    #             event.retry_policy.max_attempts if event.retry_policy else MAX_RETRIES
    #         ),
    #         exec_result=self.serializer.serialize_exec_result(event._exec_result),
    #         exec_status=event.exec_status,
    #         external_resources=event._external_resources,
    #     )
