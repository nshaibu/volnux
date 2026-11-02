"""
Serializers for the parts of ``ExecutionContext`` that are not plain data.

They are plugged into the context's ``ObjectField`` declarations through
``ObjectConfig(serializer=..., deserializer=...)``: the context keeps live
tasks, a live pipeline and live exceptions in memory, and these functions turn
them into JSON-safe payloads (and back) only when the context is persisted.
"""

import traceback
import typing
from collections import deque
from dataclasses import dataclass

from volnux.utils import get_obj_klass_import_str


def serialize_tasks(tasks: typing.Iterable[typing.Any]) -> typing.List[dict]:
    """Task graphs as ``StateSerializer.serialize_task`` payloads."""
    from volnux.execution.rehydrator.engine.serializer import StateSerializer

    return [StateSerializer.serialize_task(task) for task in tasks]


def deserialize_tasks(payload: typing.Iterable[dict]) -> typing.Deque[typing.Any]:
    """Rebuild live task graphs from their payloads."""
    from volnux.execution.rehydrator.engine.deserializer import StateDeserializer

    return deque(StateDeserializer.rebuild_task(task) for task in payload)


@dataclass
class PipelineReference:
    """Where to find a pipeline again: a pipeline is code and configuration, so
    only its identity is stored and the rehydrator supplies the object."""

    id: str
    class_path: str


def serialize_pipeline(pipeline: typing.Any) -> typing.Dict[str, str]:
    return {"id": pipeline.id, "class_path": get_obj_klass_import_str(pipeline)}


def deserialize_pipeline(payload: typing.Dict[str, str]) -> PipelineReference:
    return PipelineReference(id=payload["id"], class_path=payload["class_path"])


def serialize_error(
    error: typing.Union[Exception, typing.Dict[str, typing.Any]],
) -> typing.Dict[str, typing.Any]:
    """Flatten an exception; already-flattened errors pass through unchanged."""
    if isinstance(error, dict):
        return error
    return {
        "type": error.__class__.__name__,
        "message": str(error),
        "traceback": "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        ),
    }


def serialize_errors(errors: typing.Iterable[typing.Any]) -> typing.List[dict]:
    return [serialize_error(error) for error in errors]


def deserialize_errors(payload: typing.Iterable[dict]) -> typing.List[dict]:
    """Reloaded errors stay flattened: they are history, not control flow."""
    return list(payload)
