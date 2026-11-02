import logging
import typing
from typing import Any, Dict

from volnux.import_utils import import_string as import_class

from volnux.parser.options import Options
from .snapshot import EngineStateSnapshot, TaskSnapshot

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType

logger = logging.getLogger(__name__)


class StateDeserializer:
    """
    Handles deserialization of raw dict payloads back into snapshot objects, and
    of serialized task graphs back into live tasks.

    This is the inverse of ``StateSerializer``: each method takes a plain
    ``dict`` (as produced by ``get_state`` or loaded from the key-value store)
    and reconstructs the corresponding object. ``ExecutionContext`` itself needs
    no deserializer; its fields restore themselves.
    """

    @staticmethod
    def deserialize_engine_state(data: Dict[str, Any]) -> EngineStateSnapshot:
        """Reconstruct an EngineStateSnapshot from a raw dict.

        The queue entries remain ``QueueTaskTemplate`` dicts, matching how they
        were serialized.
        """
        return EngineStateSnapshot(**data)

    @staticmethod
    def deserialize_task(data: Dict[str, Any]) -> TaskSnapshot:
        """Reconstruct a TaskSnapshot from a raw dict.

        This is the inverse of SnapshotBuilder.build_pipeline_task, which wraps
        the StateSerializer.serialize_task output in a TaskSnapshot.
        """
        return TaskSnapshot(**data)

    @classmethod
    def rebuild_task(cls, data: Dict[str, Any]) -> "TaskType":
        """Rebuild a live task (or grouping) graph from ``serialize_task`` output.

        Restores the task itself, its options, namespace and descriptor names,
        every descriptor branch (recursively) and the sink subtree, keeping the
        original task ids. Event classes are imported from their recorded path.
        """
        from volnux.parser.operator import PipeType
        from volnux.task import PipelineTask, PipelineTaskGrouping

        if data.get("task_type") == "group":
            task: "TaskType" = PipelineTaskGrouping(
                [cls.rebuild_task(chain) for chain in data.get("chains") or []]
            )
        else:
            path = data.get("event_class_import_path")
            task = PipelineTask(event=import_class(path) if path else data["event_name"])

        task.change_object_id(data["task_id"])
        task.namespace = data.get("namespace") or "local"
        task.descriptors = dict(data.get("descriptors") or {})
        task.sequence_number = data.get("sequence_number")
        if data.get("options"):
            task.options = cls._rebuild_options(data["options"])

        if data.get("descriptor") is not None:
            task.descriptor = data["descriptor"]
        if data.get("descriptor_pipe_type"):
            task.descriptor_pipe = PipeType(data["descriptor_pipe_type"])

        branches = (data.get("condition_node") or {}).get("_descriptors") or {}
        for config in branches.values():
            child_data = config.get("task")
            child = cls.rebuild_task(child_data) if child_data else None
            if child is not None:
                child.parent_node = task
            pipe = PipeType(config["pipe"]) if config.get("pipe") else None
            task.condition_node.add_descriptor(config["descriptor"], pipe, child)

        if data.get("sink_task"):
            sink = cls.rebuild_task(data["sink_task"])
            sink.parent_node = task
            task.sink_node = sink
            if data.get("sink_task_pipe"):
                task.sink_pipe = PipeType(data["sink_task_pipe"])
        return task

    @staticmethod
    def _rebuild_options(data: Dict[str, Any]) -> Options:
        values = dict(data)
        # unset executor settings are the default, so they are not restored
        executor_config = values.get("executor_config") or {}
        values["executor_config"] = {
            key: value for key, value in executor_config.items() if value != "unset"
        }
        return Options.from_dict(values)
