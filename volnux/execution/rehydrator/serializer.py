import typing
import traceback
from enum import Enum


if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType


class SerializationStrategy(Enum):
    """Strategy for serializing complex objects"""

    JSON = "json"
    PICKLE = "pickle"
    HYBRID = "hybrid"  # JSON metadata + Pickle binary


class StateSerializer:
    """
    Handles serialization of complex Volnux objects to Redis-compatible formats.
    """

    @staticmethod
    def serialize_task(task: "TaskType") -> dict:
        """Serialize PipelineTask to dict"""
        return {
            "task_id": getattr(task, "id", None),
            "event_name": task.get_event_name(),
            "event_class_path": f"{task.get_event_class().__module__}.{task.get_event_class().__name__}",
            "options": task.options.to_dict() if task.options else None,
            "condition_node": StateSerializer._serialize_condition_node(
                task.condition_node
            ),
        }

    @staticmethod
    def _serialize_condition_node(node) -> dict:
        """Serialize condition node configuration"""
        return {
            "on_success_pipe": (
                node.on_success_pipe.value if node.on_success_pipe else None
            ),
            "on_failure_pipe": (
                node.on_failure_pipe.value if node.on_failure_pipe else None
            ),
        }

    @staticmethod
    def serialize_result(result: "EventResult") -> dict:
        """Serialize EventResult"""
        return {
            "data": result.data,
            "metadata": result.metadata if hasattr(result, "metadata") else {},
            "timestamp": getattr(result, "timestamp", None),
        }

    @staticmethod
    def serialize_exception(exc: Exception) -> str:
        """Serialize exception to string with traceback"""
        return f"{exc.__class__.__name__}: {str(exc)}\n{''.join(traceback.format_tb(exc.__traceback__))}"

    @staticmethod
    def serialize_pipeline_ref(pipeline: "Pipeline") -> tuple:
        """Extract pipeline class path and initialization data"""
        class_path = f"{pipeline.__class__.__module__}.{pipeline.__class__.__name__}"
        # Store pipeline ID or configuration needed to reconstruct
        pipeline_id = getattr(pipeline, "id", pipeline.__class__.__name__)
        return pipeline_id, class_path
