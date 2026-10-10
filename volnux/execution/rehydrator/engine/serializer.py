import typing
import traceback
from typing import Union, Dict, Any

from volnux.result import EventResult
from volnux.execution.pipeline import Pipeline
from .snapshot import QueueTaskTemplate

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType
    from volnux.engine.base import TaskNode


class StateSerializer:
    """
    Handles serialization of complex Volnux objects to KeyValue-compatible formats.
    """

    @staticmethod
    def serialize_task(task: "TaskType") -> Dict[str, Any]:
        """Serialize PipelineTask to dict."""
        event_class = task.get_event_class()

        payload = {
            "task_id": task.get_id(),
            "task_type": "group" if getattr(task, "is_grouping", False) else "normal",
            "event_name": task.get_event_name(),
            "event_class_import_path": (
                f"{event_class.__module__}.{event_class.__name__}"
            ),
            "sink_task_id": task.sink_node.get_id() if task.sink_node else None,
            "sink_task_pipe": task.sink_pipe.value if task.sink_pipe else None,
            "options": task.options.as_dict(),
            "sequence_number": task.sequence_number,
            "descriptor": task.descriptor,
            "descriptor_pipe_type": (
                task.descriptor_pipe.value if task.descriptor_pipe else None
            ),
            "condition_node": task.condition_node.as_dict(),
        }

        # TODO: reference the note in the task snapshot
        #  for how we will handle checkpoint of groupings
        # chain = getattr(task, "chains", None)
        # if chain:
        #     payload["chains"] = [
        #         StateSerializer.serialize_task(child) for child in chain
        #     ]
        #
        # strategy = getattr(task, "strategy", None)
        # if strategy is not None:
        #     payload["strategy"] = str(strategy)

        return payload

    @staticmethod
    def serialize_result(result: "EventResult") -> Union[Dict[str, typing.Any], str]:
        """Serialize EventResult."""
        if result.should_persist():
            return result.as_dict()
        return result.id

    @staticmethod
    def serialise_queue_task(
        task_position: int, task_node: "TaskNode"
    ) -> QueueTaskTemplate:
        return {
            "position_in_queue": task_position,
            "task_id": task_node.task.get_id(),
            "previous_context_id": task_node.previous_context.state_id,
        }

    @staticmethod
    def serialize_exception(exc: Exception) -> typing.Dict[str, typing.Any]:
        """Serialize exception to structured data."""
        return {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ),
        }

    @staticmethod
    def serialize_pipeline_ref(pipeline: "Pipeline") -> typing.Tuple[str, str]:
        """Extract pipeline identity and class path."""
        class_path = f"{pipeline.__class__.__module__}.{pipeline.__class__.__name__}"
        return pipeline.id, class_path
