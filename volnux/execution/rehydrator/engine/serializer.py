import dataclasses
import enum
import typing
import traceback
from typing import Any, Dict, Optional, Union

from .snapshot import QueueTaskTemplate

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType
    from volnux.engine.base import TaskNode


class StateSerializer:
    """
    Handles serialization of complex Volnux objects to KeyValue-compatible formats.
    """

    @classmethod
    def serialize_task(cls, task: "TaskType") -> Dict[str, Any]:
        """Serialize a task or task grouping to a dict (see ``TaskSnapshot``)."""
        # TODO: reference the note in the task snapshot for how we will handle
        #  checkpoint of groupings (chains and strategy are not persisted yet).
        return {
            **cls._serialize_identity(task),
            **cls._serialize_event(task),
            **cls._serialize_sink(task),
            **cls._serialize_descriptor(task),
            **cls._serialize_group(task),
            "options": cls._serialize_options(task),
            "condition_node": task.condition_node.as_dict(),
            "sequence_number": task.sequence_number,
        }

    @staticmethod
    def _enum_value(member: Optional[Any]) -> Optional[Any]:
        """Value of an enum member, or None when it is unset."""
        return member.value if member else None

    @staticmethod
    def _serialize_identity(task: "TaskType") -> Dict[str, Any]:
        return {
            "task_id": task.get_id(),
            "task_type": "group" if getattr(task, "is_grouping", False) else "normal",
            "namespace": task.namespace,
        }

    @staticmethod
    def _serialize_event(task: "TaskType") -> Dict[str, Any]:
        # A grouping runs chains of tasks and has no event class of its own.
        event_class_path = None
        if not getattr(task, "is_grouping", False):
            event_class = task.get_event_class()
            event_class_path = f"{event_class.__module__}.{event_class.__name__}"
        return {
            "event_name": task.get_event_name(),
            "event_class_import_path": event_class_path,
        }

    @classmethod
    def _serialize_sink(cls, task: "TaskType") -> Dict[str, Any]:
        return {
            "sink_task_id": task.sink_node.get_id() if task.sink_node else None,
            "sink_task_pipe": cls._enum_value(task.sink_pipe),
            # the whole sink subtree, so a persisted graph can be rebuilt
            "sink_task": cls.serialize_task(task.sink_node) if task.sink_node else None,
        }

    @classmethod
    def _serialize_group(cls, task: "TaskType") -> Dict[str, Any]:
        if not getattr(task, "is_grouping", False):
            return {"chains": None, "strategy": None}
        strategy = getattr(task, "strategy", None)
        return {
            "chains": [cls.serialize_task(chain) for chain in task.chains],
            "strategy": strategy.name if strategy is not None else None,
        }

    @classmethod
    def _serialize_descriptor(cls, task: "TaskType") -> Dict[str, Any]:
        return {
            "descriptor": task.descriptor,
            "descriptor_pipe_type": cls._enum_value(task.descriptor_pipe),
            # label -> value map copied from the program, for 2-way translation
            "descriptors": dict(task.descriptors),
        }

    @classmethod
    def _serialize_options(cls, task: "TaskType") -> Dict[str, Any]:
        if task.options is None:
            return {}
        return cls._jsonable(task.options.as_dict())

    @classmethod
    def _jsonable(cls, value: Any) -> Any:
        """Reduce enums and dataclasses to plain values so options survive JSON."""
        if isinstance(value, enum.Enum):
            return value.value
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return cls._jsonable(dataclasses.asdict(value))
        if isinstance(value, dict):
            return {key: cls._jsonable(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            return [cls._jsonable(item) for item in value]
        return value

    @staticmethod
    def serialise_queue_task(
        task_position: int, task_node: "TaskNode"
    ) -> QueueTaskTemplate:
        return {
            "position_in_queue": task_position,
            "task_id": task_node.task.get_id(),
            "previous_context_id": task_node.previous_context.state_id,
        }
