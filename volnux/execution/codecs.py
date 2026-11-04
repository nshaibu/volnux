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
from dataclasses import dataclass, field

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
    #: The values of the pipeline's input fields. They live in the process that ran
    #: the pipeline, so a resume elsewhere needs them to rebuild it; only JSON-safe
    #: values can be carried.
    fields: typing.Dict[str, typing.Any] = field(default_factory=dict)


def _pipeline_field_values(pipeline: typing.Any) -> typing.Dict[str, typing.Any]:
    from volnux.execution.utils import is_json_serializable

    get_fields = getattr(pipeline, "get_fields", None)
    if get_fields is None:
        return {}
    values = {}
    for name, _ in get_fields():
        if name in vars(pipeline) and is_json_serializable(vars(pipeline)[name]):
            values[name] = vars(pipeline)[name]
    return values


def serialize_pipeline(pipeline: typing.Any) -> typing.Dict[str, typing.Any]:
    return {
        "id": pipeline.id,
        "class_path": get_obj_klass_import_str(pipeline),
        "fields": _pipeline_field_values(pipeline),
    }


def deserialize_pipeline(payload: typing.Dict[str, typing.Any]) -> PipelineReference:
    return PipelineReference(
        id=payload["id"],
        class_path=payload["class_path"],
        fields=dict(payload.get("fields") or {}),
    )


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
