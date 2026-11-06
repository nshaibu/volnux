"""Helpers shared by the hydration tests."""

from volnux.engine.base import EngineExecutionResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
)


async def suspend(workflow_class, **pipeline_kwargs):
    """Run ``workflow_class`` until it suspends; return (pipeline, queue entry)."""
    pipeline = workflow_class(**pipeline_kwargs)
    engine = DefaultWorkflowEngine()
    result = await engine.execute(pipeline.get_pipeline_state().start, pipeline)
    assert result.status == EngineExecutionResult.SUSPENDED
    entries = await (await ExternalCommunicationQueueEntry.all()).to_list()
    return pipeline, entries[-1]


def answer(entry, decision="approve", **kw):
    return ExternalCommunicationResponse(
        request_id=entry.request_id, decision=decision, data={}, reviewer="ann", **kw
    )
