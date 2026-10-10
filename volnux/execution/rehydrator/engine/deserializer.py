import logging
import typing
from typing import Any, Dict

from .snapshot import ContextSnapshot, TraversalSnapshot, TaskSnapshot

logger = logging.getLogger(__name__)


class StateDeserializer:
    """
    Handles deserialization of raw dict payloads back into snapshot objects.

    This is the inverse of the snapshot building performed by SnapshotBuilder
    (which relies on StateSerializer). Each ``deserialize_*`` method takes a
    plain ``dict`` (as produced by ``get_state``/``to_dict`` or loaded from the
    key-value store) and reconstructs the corresponding snapshot object defined
    in ``snapshot.py``.
    """

    @staticmethod
    def deserialize_context(data: Dict[str, Any]) -> ContextSnapshot:
        """Reconstruct a ContextSnapshot from a raw dict.

        The nested ``traversal`` payload is rebuilt into a TraversalSnapshot
        first, then the full ContextSnapshot is assembled.
        """
        payload = dict(data)

        traversal = payload.get("traversal")
        if isinstance(traversal, dict):
            payload["traversal"] = StateDeserializer.deserialize_traversal(traversal)

        return ContextSnapshot(**payload)

    @staticmethod
    def deserialize_traversal(data: Dict[str, Any]) -> TraversalSnapshot:
        """Reconstruct a TraversalSnapshot from a raw dict.

        ``TraversalSnapshot`` is a plain dataclass; the queue entries remain as
        ``QueueTaskTemplate`` dicts, matching how they were serialized.
        """
        return TraversalSnapshot(**data)

    @staticmethod
    def deserialize_task(data: Dict[str, Any]) -> TaskSnapshot:
        """Reconstruct a TaskSnapshot from a raw dict.

        This is the inverse of SnapshotBuilder.build_pipeline_task, which wraps
        the StateSerializer.serialize_task output in a TaskSnapshot.
        """
        return TaskSnapshot(**data)
