"""
OpenTelemetry instrumentation of the workflow engine.

``DefaultWorkflowEngine.execute`` gets an ``engine.execute`` span (its own engine
id, the workflow's tasks processed and outcome as attributes) and the engine
metrics. Run through ``run_workflow``, it nests under the ``workflow.*`` span of
``Pipeline.start``.
"""

import logging
import typing
from typing import Any, Dict

from ._common import traced
from ._patching import patch_attr

if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _attributes(
    self, root_task=None, pipeline=None, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
    return {
        "engine.id": getattr(self, "_engine_id", None),
        "engine.class": self.__class__.__name__,
        "engine.strict_mode": getattr(self, "strict_mode", None),
        "pipeline.class": pipeline.__class__.__name__ if pipeline is not None else None,
    }


def _result_attributes(result: Any) -> Dict[str, Any]:
    status = getattr(result, "status", None)
    error = getattr(result, "error", None)
    return {
        "engine.tasks_processed": getattr(result, "tasks_processed", None),
        "engine.status": getattr(status, "value", status),
        "engine.error": type(error).__name__ if error is not None else None,
    }


def instrument_engine_execute(original_execute):
    """Instrument ``DefaultWorkflowEngine.execute`` (a coroutine function)."""
    return traced(
        "engine.execute",
        attributes=_attributes,
        result_attributes=_result_attributes,
    )(original_execute)


def patch_engine() -> bool:
    """Instrument ``DefaultWorkflowEngine.execute`` (once)."""
    from volnux.engine.default_engine import DefaultWorkflowEngine

    patched = patch_attr(DefaultWorkflowEngine, "execute", instrument_engine_execute)
    if patched:
        logger.info("DefaultWorkflowEngine instrumented with OpenTelemetry")
    return patched


__all__ = ["instrument_engine_execute", "patch_engine"]
