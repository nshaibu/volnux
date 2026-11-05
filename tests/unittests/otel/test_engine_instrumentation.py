"""DefaultWorkflowEngine.execute: the engine.execute span."""

from types import SimpleNamespace

import pytest
from opentelemetry.trace import StatusCode

from volnux.engine.base import EngineExecutionResult, EngineResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.otel.instrumentations import engine as engine_module
from volnux.otel.instrumentations._patching import is_patched, unpatch_all

pytestmark = pytest.mark.asyncio


class FakeEngine:
    _engine_id = "eng-1"
    strict_mode = True

    def __init__(self, outcome):
        self.outcome = outcome

    async def execute(self, root_task, pipeline):
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class Pipe:
    pass


def engine_class():
    cls = type("FakeEngine2", (FakeEngine,), {})
    cls.execute = engine_module.instrument_engine_execute(cls.execute)
    return cls


async def test_a_span_with_the_engine_and_result_attributes(otel):
    result = EngineResult(status=EngineExecutionResult.COMPLETED, tasks_processed=3)
    assert await engine_class()(result).execute("root", Pipe()) is result
    span = otel.span("engine.execute")
    assert span.attributes["engine.id"] == "eng-1"
    assert span.attributes["engine.class"] == "FakeEngine2"
    assert span.attributes["engine.strict_mode"] is True
    assert span.attributes["pipeline.class"] == "Pipe"
    assert span.attributes["engine.tasks_processed"] == 3
    assert span.attributes["engine.status"] == "completed"
    assert "engine.error" not in span.attributes


async def test_a_result_that_carries_an_error(otel):
    result = EngineResult(
        status=EngineExecutionResult.FAILED, error=KeyError("k"), tasks_processed=1
    )
    await engine_class()(result).execute("root", Pipe())
    assert otel.span("engine.execute").attributes["engine.error"] == "KeyError"


async def test_an_exception_is_recorded_and_raised(otel):
    with pytest.raises(RuntimeError, match="engine broke"):
        await engine_class()(RuntimeError("engine broke")).execute("root", Pipe())
    assert otel.span("engine.execute").status.status_code is StatusCode.ERROR


async def test_without_a_tracer(no_otel):
    result = EngineResult(status=EngineExecutionResult.COMPLETED)
    assert await engine_class()(result).execute("root", Pipe()) is result


async def test_patch_engine_patches_the_real_engine_once(otel):
    assert engine_module.patch_engine() is True
    assert engine_module.patch_engine() is False
    assert is_patched(DefaultWorkflowEngine, "execute")
    import inspect

    assert inspect.iscoroutinefunction(DefaultWorkflowEngine.execute)
    unpatch_all()
    assert not is_patched(DefaultWorkflowEngine, "execute")


async def test_the_old_helpers_are_gone():
    # (InstrumentedDefaultWorkflowEngine called helpers the engine does not have and
    # awaited nothing; nothing ever used the wrapper it made)
    assert not hasattr(engine_module, "InstrumentedDefaultWorkflowEngine")
    assert not hasattr(engine_module, "instrument_engine")
