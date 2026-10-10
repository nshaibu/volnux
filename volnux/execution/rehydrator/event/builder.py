from typing import TYPE_CHECKING

from .snapshot import EventCheckpointSnapshot
from .serializer import StateSerializer
from volnux.utils import get_obj_klass_import_str
from volnux.constants import MAX_RETRIES

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

        return EventCheckpointSnapshot(
            task_id=event._task_id,
            class_path=get_obj_klass_import_str(event),
            phase=event.get_phase(),
            init_args=self.serializer.serialize_init_args(event.get_init_args()),
            call_args=self.serializer.serialize_call_args(event.get_call_args()),
            retry_count=event._retry_count,
            max_retry_attempts=max_attempts,
            exec_result=self.serializer.serialize_exec_result(event.exec_result),
            exec_status=event.exec_status,
            external_resources=event._external_resources,
            attribs=self.serializer.serialize_attribs(event),
        )

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
