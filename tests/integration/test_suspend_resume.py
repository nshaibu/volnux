"""A run that suspends is saved and resumed by another engine (in-memory stores)."""

import pytest

from tests.unittests.hydration.conftest import (  # noqa: F401  (fixtures)
    generated_module,
    make_event,
    make_pipeline,
    seen,
    serialized_store,
    workflow,
    workflow_factory,
)
from tests.unittests.hydration.helpers import answer, suspend
from volnux.engine.base import EngineExecutionResult
from volnux.engine.resume import resume_suspended_workflow
from volnux.execution.status import ExecutionStatus


@pytest.mark.asyncio
async def test_a_task_that_asks_a_human_suspends_the_whole_run(
    serialized_store, workflow, seen
):
    pipeline, entry = await suspend(workflow)

    assert seen["ran"] == ["A"]  # C is not reached
    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.SUSPENDED
    )


@pytest.mark.asyncio
async def test_resuming_with_the_answer_finishes_the_run(
    serialized_store, workflow, seen
):
    _, entry = await suspend(workflow)

    result = await resume_suspended_workflow(entry, answer(entry, "approve"))

    assert result.status == EngineExecutionResult.COMPLETED
    assert seen["ran"] == ["A", "B", "C"] and seen["decision"] == "approve"
