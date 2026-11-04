"""Bring a suspended workflow back to life.

A task that asked the outside world for something was checkpointed and released
(see ``ExecutionCoordinator``). When the answer arrives, ``resume_suspended_workflow``
rebuilds what was lost and carries on:

1. the suspended execution context and the engine's position (``LazyRehydrator``),
2. the suspended event, from its checkpoint, with the answer injected into its
   ``previous_result`` (``EventRehydrator``),
3. the run itself, from that task onward (``DefaultWorkflowEngine.resume``).

The answer reaches the task through ``previous_result`` and not as a return value:
``communicate()`` suspends, so it has no value to return. When the task runs
``communicate()`` again it finds the answer there.
"""

import logging
import typing

from volnux.engine.base import EngineResult, WorkflowEngine
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.execution.context import ExecutionContext
from volnux.execution.rehydrator.engine.rehydrator import LazyRehydrator
from volnux.execution.rehydrator.event.rehydrator import EventRehydrator
from volnux.execution.rehydrator.event.snapshot import EventCheckpointSnapshot
from volnux.mixins.event.communication.datastructures import (
    RESPONSE_EVENT_NAMES,
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
)
from volnux.result import EventResult
from volnux.signal.signals import event_execution_resumed

logger = logging.getLogger(__name__)


def build_response_result(
    entry: ExternalCommunicationQueueEntry,
    response: ExternalCommunicationResponse,
) -> EventResult:
    """The answer as an ``EventResult``, ready to be injected into ``previous_result``.

    A timeout is delivered the same way, with ``decision == "timeout"`` and
    ``source == "timeout"``, so the task decides what a missing answer means.
    """
    return EventResult(
        error=False,
        event_name=RESPONSE_EVENT_NAMES.get(entry.request_type, "external_response"),
        content={
            "type": RESPONSE_EVENT_NAMES.get(entry.request_type, "external_response"),
            "request_id": entry.request_id,
            "decision": response.decision,
            "data": response.data,
            "reviewer": response.reviewer,
            "source": response.source,
            "responded_at": response.responded_at,
        },
        task_id=entry.task_id,
        workflow_id=entry.workflow_id,
    )


async def resume_suspended_workflow(
    entry: ExternalCommunicationQueueEntry,
    response: ExternalCommunicationResponse,
    *,
    engine_class: typing.Type[WorkflowEngine] = DefaultWorkflowEngine,
) -> EngineResult:
    """Resume the workflow ``entry`` is waiting on, with ``response`` as its answer.

    Returns the result of the resumed run: it may complete, fail, or suspend again.
    Raises if the saved state cannot be loaded (the caller decides about retrying).
    """
    rehydrator = LazyRehydrator()
    suspended, engine = await rehydrator.resume_from(entry.context_id, engine_class)
    if engine is None:  # resume_from only returns no engine without an engine class
        raise RuntimeError("Could not rebuild the engine of the suspended workflow")

    snapshot = await EventCheckpointSnapshot.get(entry.checkpoint_key)
    answer = build_response_result(entry, response)
    await answer.save()

    event = await EventRehydrator().rehydrate(snapshot, previous_results=[answer])
    engine.stage_resumed_event(entry.task_id, event)

    pipeline = suspended.pipeline
    await event_execution_resumed.emit_async(
        sender=ExecutionContext,
        task_profiles=suspended.get_task_profiles().copy(),
        execution_context=suspended,
        state=suspended.status,
    )

    result = await engine.resume(pipeline)
    await pipeline.report_outcome()
    return result
