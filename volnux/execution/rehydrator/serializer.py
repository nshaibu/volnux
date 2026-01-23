import typing
import traceback
from enum import Enum

from volnux.result import EventResult
from volnux.pipeline import Pipeline

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType


class SerializationStrategy(Enum):
    JSON = "json"
    PICKLE = "pickle"
    HYBRID = "hybrid"  # JSON metadata + Pickle binary


class StateSerializer:
    """
    Handles serialization of complex Volnux objects to KeyValue-compatible formats.
    """

    @staticmethod
    def serialize_task(task: "TaskType") -> dict:
        """Serialize PipelineTask to dict"""
        return {
            "task_id": task.get_id(),
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
    def serialize_result(result: EventResult) -> typing.Dict[str, typing.Any]:
        """Serialize EventResult"""
        return result.as_dict()

    @staticmethod
    def serialize_exception(exc: Exception) -> str:
        """Serialize exception to string with traceback"""
        return f"{exc.__class__.__name__}: {str(exc)}\n{''.join(traceback.format_tb(exc.__traceback__))}"

    @staticmethod
    def serialize_pipeline_ref(pipeline: Pipeline) -> typing.Tuple[str, str]:
        """Extract pipeline class path and initialization data"""
        class_path = f"{pipeline.__class__.__module__}.{pipeline.__class__.__name__}"
        return pipeline.id, class_path
